use zstd_sys::{
    ZSTD_CCtxParams_init, ZSTD_CCtxParams_setParameter, ZSTD_CCtx_setParametersUsingCCtxParams,
    ZSTD_compressBound, ZSTD_compressStream2, ZSTD_createCCtx, ZSTD_createCCtxParams,
    ZSTD_freeCCtx, ZSTD_freeCCtxParams, ZSTD_inBuffer, ZSTD_isError, ZSTD_outBuffer,
    ZSTD_EndDirective, ZSTD_cParameter,
};
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    os::raw::c_int,
    path::{Path, PathBuf},
    sync::{atomic::{AtomicU64, Ordering}, Arc},
    thread,
};

use anyhow::Result;
use crossbeam_channel::bounded;
use log::debug;
use sysinfo::{MemoryRefreshKind, RefreshKind, System};
use snippy_common::{common_config::CONFIG, CompressionReport, FileEntry, should_skip_compression};

pub fn compress_dir(input_dir: &Path, output_file: &Path, _skip_compression: bool) -> Result<CompressionReport> {
    debug!("[compress_dir] Börjar komprimering från {:?} till {:?}", input_dir, output_file);

    let mut entries = Vec::new();
    let mut writer = BufWriter::new(File::create(output_file)?);

    let paths: Vec<_> = walkdir::WalkDir::new(input_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .collect();

    debug!("[compress_dir] Totalt {} filer att komprimera", paths.len());

    for entry in &paths {
        let rel_path = entry.path().strip_prefix(input_dir)?.to_path_buf();
        entries.push(FileEntry {
            relative_path: rel_path,
            offset: 0,
            length: 0,
            checksum: [0u8; 32],
            compressed: false,
            uncompressed_size: 0,
        });
    }

    let index_bytes = bincode::encode_to_vec(&entries, bincode::config::standard())?;
    writer.write_all(&(index_bytes.len() as u64).to_le_bytes())?;
    writer.write_all(&index_bytes)?;
    debug!("[compress_dir] Skrev index på {} bytes", index_bytes.len());

    let refresh = RefreshKind::everything().with_memory(MemoryRefreshKind::everything());
    let sys = Arc::new(parking_lot::Mutex::new(System::new_with_specifics(refresh)));
    let total_memory = sys.lock().total_memory();

    let (tx_writer, rx_writer) = bounded::<(usize, Vec<u8>, bool, PathBuf, u64, u64)>(CONFIG.max_core_in_flight);
    let (tx_compress, rx_compress) = bounded::<(usize, PathBuf)>(CONFIG.max_core_in_flight);

    let uncompressed_bytes = Arc::new(AtomicU64::new(0));
    let compressed_bytes = Arc::new(AtomicU64::new(0));
    let num_compressed = Arc::new(AtomicU64::new(0));
    let num_uncompressed = Arc::new(AtomicU64::new(0));

    debug!("[compress_dir] Startar {} kompressortrådar", CONFIG.max_core_in_compress);

    let compressor_handles: Vec<_> = (0..CONFIG.max_core_in_compress)
        .map(|thread_id| {
            let rx = rx_compress.clone();
            let tx = tx_writer.clone();
            let uncompressed_bytes = Arc::clone(&uncompressed_bytes);
            let compressed_bytes = Arc::clone(&compressed_bytes);
            let num_compressed = Arc::clone(&num_compressed);
            let num_uncompressed = Arc::clone(&num_uncompressed);

            thread::spawn(move || {
                for (i, path) in rx.iter() {
                    let should_compress = !should_skip_compression(&path);
                    debug!("[compressor-{thread_id}] Behandlar fil {:?} (index {})", path, i);

                    let mut file = match File::open(&path) {
                        Ok(f) => f,
                        Err(e) => {
                            debug!("[compressor-{thread_id}] Kunde inte öppna {}: {}", path.display(), e);
                            continue;
                        }
                    };

                    if should_compress {
                        let mut input_data = Vec::new();
                        if let Err(e) = file.read_to_end(&mut input_data) {
                            debug!("[compressor-{thread_id}] Fel vid läsning: {}", e);
                            continue;
                        }

                        uncompressed_bytes.fetch_add(input_data.len() as u64, Ordering::SeqCst);

                        let cctx = unsafe { ZSTD_createCCtx() };
                        assert!(!cctx.is_null());
                        let cctx_params = unsafe { ZSTD_createCCtxParams() };

                        unsafe {
                            ZSTD_CCtxParams_init(cctx_params, 0);
                            ZSTD_CCtxParams_setParameter(cctx_params, ZSTD_cParameter::ZSTD_c_nbWorkers, 4 as c_int);
                            ZSTD_CCtx_setParametersUsingCCtxParams(cctx, cctx_params);
                        }

                        let mut dst = vec![0u8; unsafe { ZSTD_compressBound(input_data.len()) }];
                        let mut input_buffer = ZSTD_inBuffer {
                            src: input_data.as_ptr() as *const _,
                            size: input_data.len(),
                            pos: 0,
                        };
                        let mut output_buffer = ZSTD_outBuffer {
                            dst: dst.as_mut_ptr() as *mut _,
                            size: dst.len(),
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
                        compressed_bytes.fetch_add(dst.len() as u64, Ordering::SeqCst);
                        num_compressed.fetch_add(1, Ordering::SeqCst);
                        
                        let dst_len = dst.len() as u64;
                        tx.send((i, dst, true, path, input_data.len() as u64, dst_len)).unwrap();
                    } else {
                        let mut total = 0u64;
                        let mut buf = [0u8; 8192];
                        while let Ok(n) = file.read(&mut buf) {
                            if n == 0 { break; }
                            total += n as u64;
                        }
                        uncompressed_bytes.fetch_add(total, Ordering::SeqCst);
                        num_uncompressed.fetch_add(1, Ordering::SeqCst);
                        tx.send((i, Vec::new(), false, path, total, total)).unwrap();
                    }
                }
            })
        })
        .collect();

    let writer_handle = thread::spawn({
        let mut writer = writer;
        let mut entries = entries;
        move || -> Result<Vec<FileEntry>> {
            for (i, data, compressed, path, in_bytes, out_bytes) in rx_writer.iter() {
                let entry = &mut entries[i];
                entry.offset = writer.seek(SeekFrom::Current(0))?;
                entry.compressed = compressed;
                entry.uncompressed_size = in_bytes;

                if compressed {
                    entry.length = out_bytes;
                    entry.checksum = *blake3::hash(&data).as_bytes();
                    writer.write_all(&data)?;
                } else {
                    let mut file = File::open(&path)?;
                    let mut hasher = blake3::Hasher::new();
                    let mut buffer = [0u8; 8192];
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

                    entry.length = written as u64;
                    entry.checksum = *hasher.finalize().as_bytes();
                }
            }

            Ok(entries)
        }
    });

    for (i, path) in paths.iter().enumerate() {
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

        tx_compress.send((i, path.path().to_path_buf()))?;
    }

    drop(tx_compress);
    drop(tx_writer);

    for h in compressor_handles {
        h.join().expect("kompressortråd kraschade");
    }

    let entries = writer_handle.join().unwrap()?;

    let total_dirs = walkdir::WalkDir::new(input_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_dir())
        .count();

    let total_in = uncompressed_bytes.load(Ordering::SeqCst);
    let total_out = compressed_bytes.load(Ordering::SeqCst) +
        entries.iter().filter(|e| !e.compressed).map(|e| e.length).sum::<u64>();

    let ratio = if total_in > 0 {
        (compressed_bytes.load(Ordering::SeqCst) as f32 / total_in as f32) * 100.0
    } else {
        0.0
    };
    Ok(CompressionReport {
        total_files: entries.len() ,
        total_dirs: total_dirs ,
        compressed_files: num_compressed.load(Ordering::SeqCst) as usize,
        uncompressed_files: num_uncompressed.load(Ordering::SeqCst) as usize,
        total_bytes_in: total_in,
        total_bytes_out: total_out,
        compressed_bytes: compressed_bytes.load(Ordering::SeqCst),
        uncompressed_bytes: total_out - compressed_bytes.load(Ordering::SeqCst),
        compression_ratio: ratio,
    })
}
