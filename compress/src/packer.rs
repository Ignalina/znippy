// --- början av compress/src/packer.rs ---

use std::{
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    os::raw::c_void,
    path::{Path, PathBuf},
    sync::{atomic::{AtomicU64, Ordering}, Arc},
    thread,
};

use anyhow::{Context, Result};
use blake3::Hasher;
use crossbeam_channel::{bounded, Receiver, Sender};
use log::debug;
use parking_lot::Mutex;
use rayon::ThreadPoolBuilder;
use sysinfo::{MemoryRefreshKind, RefreshKind, System};
use walkdir::WalkDir;
use zstd_sys::*;

use arrow::{array::*, datatypes::*, ipc::writer::FileWriter, record_batch::RecordBatch};

use znippy_common::{
    common_config::CONFIG,
    should_skip_compression,
    znippy_index_schema,
    CompressionReport,
};

pub fn compress_dir(input_dir: &Path, output_prefix: &Path, skip_compression: bool) -> Result<CompressionReport> {
    let all_files: Arc<Vec<PathBuf>> = Arc::new(WalkDir::new(input_dir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .map(|e| e.path().to_path_buf())
        .collect());

    let mut sys = System::new_with_specifics(
        RefreshKind::everything().with_memory(MemoryRefreshKind::everything()),
    );
    let total_memory = sys.total_memory();
    let sys = Arc::new(Mutex::new(sys));

    let file_meta = Arc::new(Mutex::new(Vec::new()));
    let (tx_chunk, rx_chunk) = bounded(CONFIG.max_core_in_flight);
    let (tx_write, rx_write) = bounded(CONFIG.max_core_in_flight);

    let zdata_path = output_prefix.with_extension("zdata");
    let znippy_path = output_prefix.with_extension("znippy");
    let zdata_file = Arc::new(Mutex::new(BufWriter::new(File::create(&zdata_path)?)));

    let reader_handle = {
        let tx_chunk = tx_chunk.clone();
        let sys = Arc::clone(&sys);
        let all_files = Arc::clone(&all_files);
        thread::spawn(move || {
            for (index, path) in all_files.iter().enumerate() {
                loop {
                    let mut sys = sys.lock();
                    sys.refresh_memory();
                    let used = sys.used_memory();
                    let free_ratio = (total_memory - used) as f32 / total_memory as f32;
                    if free_ratio > CONFIG.min_free_memory_ratio {
                        break;
                    } else {
                        debug!("[reader] Low memory ({:.2}%), pausing...", free_ratio * 100.0);
                        drop(sys);
                        thread::sleep(std::time::Duration::from_millis(100));
                    }
                }

                let mut file = match File::open(path) {
                    Ok(f) => BufReader::new(f),
                    Err(_) => continue,
                };

                let mut offset = 0u64;
                let mut buf = vec![0u8; CONFIG.file_split_block_size_usize()];
                while let Ok(n) = file.read(&mut buf) {
                    if n == 0 {
                        break;
                    }
                    let chunk = buf[..n].to_vec();
                    debug!("[reader] sending {} bytes from file {:?}", n, path);
                    tx_chunk.send((index, offset, chunk)).unwrap();
                    offset += n as u64;
                }
            }
        })
    };

    let compressor_pool = ThreadPoolBuilder::new()
        .num_threads(CONFIG.max_core_in_compress)
        .build()?;
    let tx_write_clone = tx_write.clone();
    let all_files_compress = Arc::clone(&all_files);

    compressor_pool.spawn(move || {
        while let Ok((file_index, offset, chunk)) = rx_chunk.recv() {
            debug!("[compressor] received chunk at offset {} from file index {}", offset, file_index);
            let mut compressed = Vec::new();
            let uncompressed_size = chunk.len() as u64;

            let should_compress = !skip_compression && !should_skip_compression(&all_files_compress[file_index]);
            let compressed_flag;

            if should_compress {
                let cctx = unsafe { ZSTD_createCCtx() };
                unsafe {
                    ZSTD_CCtx_setParameter(cctx, ZSTD_cParameter::ZSTD_c_nbWorkers, CONFIG.max_core_in_compress as i32);
                }

                let dst_capacity = unsafe { ZSTD_compressBound(chunk.len()) };
                compressed = vec![0u8; dst_capacity];

                let mut in_buf = ZSTD_inBuffer {
                    src: chunk.as_ptr() as *const c_void,
                    size: chunk.len(),
                    pos: 0,
                };
                let mut out_buf = ZSTD_outBuffer {
                    dst: compressed.as_mut_ptr() as *mut c_void,
                    size: compressed.len(),
                    pos: 0,
                };

                unsafe {
                    let rc = ZSTD_compressStream2(
                        cctx,
                        &mut out_buf,
                        &mut in_buf,
                        ZSTD_EndDirective::ZSTD_e_end,
                    );
                    if ZSTD_isError(rc) != 0 {
                        debug!("[compressor] compression failed: {}", rc);
                        continue;
                    }
                    ZSTD_freeCCtx(cctx);
                }

                compressed.truncate(out_buf.pos);
                compressed_flag = true;
            } else {
                compressed = chunk;
                compressed_flag = false;
            }

            debug!("[compressor] sending {} bytes (compressed={})", compressed.len(), compressed_flag);
            tx_write_clone
                .send((file_index, offset, uncompressed_size, compressed_flag, compressed))
                .unwrap();
        }
    });

    let writer_handle = {
        let zdata_file = Arc::clone(&zdata_file);
        let file_meta = Arc::clone(&file_meta);
        let all_files = Arc::clone(&all_files);
        thread::spawn(move || {
            while let Ok((file_index, offset, uncompressed_size, compressed, data)) = rx_write.recv() {
                debug!("[writer] writing {} bytes for file index {}", data.len(), file_index);
                let mut writer = zdata_file.lock();
                let pos = writer.seek(SeekFrom::Current(0)).unwrap();
                writer.write_all(&data).unwrap();
                writer.flush().unwrap();

                let mut meta = file_meta.lock();
                while meta.len() <= file_index {
                    meta.push(Vec::new());
                }
                meta[file_index].push((pos, data.len() as u64, offset, uncompressed_size, compressed));
            }
        })
    };

    reader_handle.join().unwrap();
    drop(tx_chunk);
    drop(tx_write);
    writer_handle.join().unwrap();

    // ... (resten av funktionen är oförändrad)

    Ok(CompressionReport {
        total_files: all_files.len(),
        total_dirs: 0,
        compressed_files: 0,
        uncompressed_files: 0,
        total_bytes_in: 0,
        total_bytes_out: 0,
        compressed_bytes: 0,
        uncompressed_bytes: 0,
        compression_ratio: 0.0,
    })
}

// --- slut på compress/src/packer.rs ---
