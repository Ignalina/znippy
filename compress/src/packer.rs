use std::{
    fs::File,
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    os::raw::c_void,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
};

use anyhow::Result;
use blake3::Hasher;
use crossbeam_channel::bounded;
use log::debug;
use sysinfo::{MemoryRefreshKind, RefreshKind, System};
use walkdir::WalkDir;
use zstd_sys::*;

use snippy_common::{common_config::CONFIG, CompressionReport, FileEntry, should_skip_compression};

pub fn compress_dir(input_dir: &Path, output_file: &Path, skip_compression: bool) -> Result<CompressionReport> {
    debug!("[compress_dir] Startar komprimering från {:?} till {:?}", input_dir, output_file);

    let mut all_paths = Vec::new();
    let mut total_dirs = 0;

    for entry in WalkDir::new(input_dir) {
        let entry = entry?;
        if entry.file_type().is_dir() {
            total_dirs += 1;
        } else if entry.file_type().is_file() {
            all_paths.push(entry.into_path());
        }
    }

    let entries: Vec<FileEntry> = all_paths
        .iter()
        .map(|path| FileEntry {
            relative_path: path.strip_prefix(input_dir).unwrap().to_path_buf(),
            offset: 0,
            length: 0,
            checksum: [0u8; 32],
            compressed: false,
            uncompressed_size: 0,
        })
        .collect();

    let writer = BufWriter::new(File::create(output_file)?);
    let refresh = RefreshKind::everything().with_memory(MemoryRefreshKind::everything());
    let sys = Arc::new(parking_lot::Mutex::new(System::new_with_specifics(refresh)));
    let total_memory = sys.lock().total_memory();

    let (tx_writer, rx_writer) = bounded(CONFIG.max_core_in_flight);
    let (tx_compress, rx_compress) = bounded::<(usize, PathBuf)>(CONFIG.max_core_in_flight);

    let uncompressed_bytes = Arc::new(AtomicU64::new(0));
    let compressed_bytes = Arc::new(AtomicU64::new(0));
    let num_compressed = Arc::new(AtomicU64::new(0));
    let num_uncompressed = Arc::new(AtomicU64::new(0));

    let compressor_handles: Vec<_> = (0..CONFIG.max_core_in_compress)
        .map(|thread_id| {
            let rx = rx_compress.clone();
            let tx = tx_writer.clone();
            let uncompressed_bytes = Arc::clone(&uncompressed_bytes);
            let compressed_bytes = Arc::clone(&compressed_bytes);
            let num_compressed = Arc::clone(&num_compressed);
            let num_uncompressed = Arc::clone(&num_uncompressed);

            thread::spawn(move || {
                for (i, pathbuf) in rx.iter() {
                    let should_compress = !skip_compression && !should_skip_compression(&pathbuf);
                    debug!("[compressor-{thread_id}] Behandlar fil {:?} (index {}), skip_compression = {}, should_skip = {}", pathbuf, i, skip_compression, !should_compress);

                    let mut file = match File::open(&pathbuf) {
                        Ok(f) => f,
                        Err(e) => {
                            debug!("[compressor-{thread_id}] Kunde inte öppna {}: {}", pathbuf.display(), e);
                            continue;
                        }
                    };

                    if should_compress {
                        let mut input_data = Vec::new();
                        if let Err(e) = file.read_to_end(&mut input_data) {
                            debug!("[compressor-{thread_id}] Fel vid läsning: {}", e);
                            continue;
                        }

                        let input_len = input_data.len();
                        uncompressed_bytes.fetch_add(input_len as u64, Ordering::SeqCst);

                        let cctx = unsafe { ZSTD_createCCtx() };
                        let cctx_params = unsafe { ZSTD_createCCtxParams() };

                        unsafe {
                            ZSTD_CCtxParams_init(cctx_params, 0);
                            ZSTD_CCtxParams_setParameter(cctx_params, ZSTD_cParameter::ZSTD_c_nbWorkers, 4);
                            ZSTD_CCtx_setParametersUsingCCtxParams(cctx, cctx_params);
                        }

                        let dst_capacity = unsafe { ZSTD_compressBound(input_len) };
                        let mut dst = vec![0u8; dst_capacity];

                        let mut input_buffer = ZSTD_inBuffer {
                            src: input_data.as_ptr() as *const c_void,
                            size: input_len,
                            pos: 0,
                        };
                        let mut output_buffer = ZSTD_outBuffer {
                            dst: dst.as_mut_ptr() as *mut c_void,
                            size: dst_capacity,
                            pos: 0,
                        };

                        let rc = unsafe {
                            ZSTD_compressStream2(
                                cctx,
                                &mut output_buffer,
                                &mut input_buffer,
                                ZSTD_EndDirective::ZSTD_e_end,
                            )
                        };
                        if unsafe { ZSTD_isError(rc) } != 0 {
                            debug!("[compressor-{thread_id}] Komprimering misslyckades: {}", rc);
                            continue;
                        }

                        unsafe {
                            ZSTD_freeCCtxParams(cctx_params);
                            ZSTD_freeCCtx(cctx);
                        }

                        dst.truncate(output_buffer.pos);
                        let dst_len = dst.len() as u64;
                        compressed_bytes.fetch_add(dst_len, Ordering::SeqCst);
                        num_compressed.fetch_add(1, Ordering::SeqCst);

                        debug!("[compressor-{thread_id}] Skickar komprimerad fil {} ({} bytes)", pathbuf.display(), dst_len);
                        tx.send((i, dst, true, pathbuf, input_len as u64, dst_len)).unwrap();
                    } else {
                        let mut total = 0u64;
                        let mut buf = vec![0u8; 1024 * 1024];
                        while let Ok(n) = file.read(&mut buf) {
                            if n == 0 {
                                break;
                            }
                            total += n as u64;
                        }
                        uncompressed_bytes.fetch_add(total, Ordering::SeqCst);
                        num_uncompressed.fetch_add(1, Ordering::SeqCst);

                        debug!("[compressor-{thread_id}] Skickar okomprimerad fil {} ({} bytes)", pathbuf.display(), total);
                        tx.send((i, Vec::new(), false, pathbuf, total, total)).unwrap();
                    }
                }
            })
        })
        .collect();

    let writer_handle = thread::spawn({
        let mut writer = writer;
        let mut entries = entries;
        move || -> Result<(Vec<FileEntry>, u64)> {
            writer.seek(SeekFrom::Start(8))?;
            for (i, data, compressed, path, in_bytes, out_bytes) in rx_writer.iter() {
                let entry = &mut entries[i];
                entry.offset = writer.seek(SeekFrom::Current(0))?;
                entry.compressed = compressed;
                entry.uncompressed_size = in_bytes;
                entry.length = out_bytes;

                if compressed {
                    entry.checksum = *blake3::hash(&data).as_bytes();
                    writer.write_all(&data)?;
                } else {
                    let mut file = File::open(&path)?;
                    let mut hasher = Hasher::new();
                    let mut buffer = [0u8; 1024 * 1024];
                    let mut written = 0;

                    loop {
                        let n = file.read(&mut buffer)?;
                        if n == 0 {
                            break;
                        }
                        writer.write_all(&buffer[..n])?;
                        hasher.update(&buffer[..n]);
                        written += n;
                    }

                    entry.checksum = *hasher.finalize().as_bytes();
                }
            }

            let index_bytes = bincode::encode_to_vec(&entries, bincode::config::standard())?;
            let index_len = index_bytes.len() as u64;
            writer.write_all(&index_bytes)?;
            writer.seek(SeekFrom::Start(0))?;
            writer.write_all(&index_len.to_le_bytes())?;
            writer.flush()?;
            Ok((entries, index_len))
        }
    });

    for (i, pathbuf) in all_paths.into_iter().enumerate() {
        while rx_compress.len() >= CONFIG.max_core_in_flight {
            let used = {
                let mut sys = sys.lock();
                sys.refresh_memory();
                sys.used_memory()
            };
            let free_ratio = (total_memory - used) as f32 / total_memory as f32;
            if free_ratio > CONFIG.min_free_memory_ratio {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        tx_compress.send((i, pathbuf))?;
    }

    drop(tx_compress);
    drop(tx_writer);

    for h in compressor_handles {
        h.join().expect("kompressortråd kraschade");
    }

    let (entries, _index_len) = writer_handle.join().unwrap()?;

    let total_in = uncompressed_bytes.load(Ordering::SeqCst);
    let total_out = compressed_bytes.load(Ordering::SeqCst)
        + entries.iter().filter(|e| !e.compressed).map(|e| e.length).sum::<u64>();

    let ratio = if total_in > 0 {
        (compressed_bytes.load(Ordering::SeqCst) as f32 / total_in as f32) * 100.0
    } else {
        0.0
    };

    Ok(CompressionReport {
        total_files: entries.len(),
        total_dirs,
        compressed_files: num_compressed.load(Ordering::SeqCst) as usize,
        uncompressed_files: num_uncompressed.load(Ordering::SeqCst) as usize,
        total_bytes_in: total_in,
        total_bytes_out: total_out,
        compressed_bytes: compressed_bytes.load(Ordering::SeqCst),
        uncompressed_bytes: total_out - compressed_bytes.load(Ordering::SeqCst),
        compression_ratio: ratio,
    })
}