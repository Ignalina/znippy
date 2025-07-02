//! Multithreaded file compressor using preallocated chunk buffers and zero-copy RingBuffer coordination.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crossbeam_channel::{bounded, unbounded};
use zstd_sys::*;

use znippy_common::chunkpool::ChunkPool;
use znippy_common::common_config::CONFIG;
use znippy_common::meta::{ChunkMeta, CompressionStats};

pub fn compress_dir(all_files: Vec<PathBuf>) -> anyhow::Result<()> {
    let chunk_size = CONFIG.file_split_block_size as usize;
    let chunk_pool = Arc::new(ChunkPool::<{ CONFIG.file_split_block_size as usize }>::new(CONFIG.max_chunks));
    let (tx_chunk, rx_chunk) = unbounded::<(usize, usize, usize)>();
    let (tx_compressed, rx_compressed) = unbounded::<(usize, usize, Vec<u8>, usize, usize)>();
    let (tx_meta, rx_meta) = unbounded::<(usize, ChunkMeta)>();
    let (tx_stats, rx_stats) = unbounded::<CompressionStats>();

    log::info!("Starting compression with {} files, {} chunks, chunk size {} bytes", all_files.len(), CONFIG.max_chunks, chunk_size);

    // Writer setup
    let zdata_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(CONFIG.output_zdata_path())?;
    let writer = Arc::new(Mutex::new(BufWriter::with_capacity(1 << 20, zdata_file)));

    // Reader thread
    {
        let cp = Arc::clone(&chunk_pool);
        let tx = tx_chunk.clone();
        std::thread::spawn(move || {
            for (file_index, path) in all_files.iter().enumerate() {
                let file = match File::open(path) {
                    Ok(f) => f,
                    Err(e) => {
                        log::warn!("Failed to open file {:?}: {}", path, e);
                        continue;
                    }
                };
                let mut reader = BufReader::new(file);
                loop {
                    let chunk_nr = match cp.try_get_chunk_nr() {
                        Some(nr) => nr,
                        None => continue,
                    };
                    let mut arc_buf = Arc::clone(cp.get(chunk_nr));
                    let buf = Arc::get_mut(&mut arc_buf).unwrap();
                    let n = match reader.read(&mut buf[..]) {
                        Ok(0) => {
                            cp.return_chunk_nr(chunk_nr);
                            break;
                        }
                        Ok(n) => n,
                        Err(e) => {
                            log::warn!("Failed to read file {:?}: {}", path, e);
                            cp.return_chunk_nr(chunk_nr);
                            break;
                        }
                    };
                    log::debug!("[reader] file {} → chunk {} ({} bytes)", file_index, chunk_nr, n);
                    tx.send((chunk_nr, n, file_index)).unwrap();
                }
            }
        });
    }

    // Compressor thread
    {
        let cp = Arc::clone(&chunk_pool);
        let tx = tx_compressed.clone();
        std::thread::spawn(move || {
            unsafe {
                let cctx = ZSTD_createCCtx();
                assert!(!cctx.is_null(), "ZSTD_createCCtx failed");

                ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, CONFIG.zstd_level as i32);
                ZSTD_CCtx_setParameter(cctx, ZSTD_c_nbWorkers, CONFIG.zstd_workers as i32);
                log::info!("Compressor initialized with level {} and {} workers", CONFIG.zstd_level, CONFIG.zstd_workers);

                while let Ok((chunk_nr, used, file_index)) = rx_chunk.recv() {
                    let buf = cp.get(chunk_nr);
                    let input_ptr = buf.as_ptr();

                    let max_compressed = ZSTD_compressBound(used);
                    let mut output = vec![0u8; max_compressed];

                    let compressed_size = ZSTD_compress2(
                        cctx,
                        output.as_mut_ptr() as *mut _,
                        max_compressed,
                        input_ptr as *const _,
                        used,
                    );

                    if ZSTD_isError(compressed_size) != 0 {
                        let err_str = std::ffi::CStr::from_ptr(ZSTD_getErrorName(compressed_size));
                        panic!("ZSTD_compress2 error: {}", err_str.to_string_lossy());
                    }

                    log::debug!(
                        "[compressor] file={} chunk={} {} → {} bytes",
                        file_index,
                        chunk_nr,
                        used,
                        compressed_size
                    );

                    ZSTD_CCtx_reset(cctx, ZSTD_reset_session_only);

                    tx.send((file_index, chunk_nr, output, compressed_size as usize, used)).unwrap();
                }

                ZSTD_freeCCtx(cctx);
            }
        });
    }

    // Writer thread
    {
        let cp = Arc::clone(&chunk_pool);
        let writer = Arc::clone(&writer);
        std::thread::spawn(move || {
            let mut offset = 0u64;
            let mut stats = CompressionStats::default();

            while let Ok((file_index, chunk_nr, compressed_data, compressed_len, original_size)) = rx_compressed.recv() {
                let mut w = writer.lock().unwrap();
                if let Err(e) = w.write_all(&compressed_data[..compressed_len]) {
                    log::error!("Failed to write chunk {} to .zdata: {}", chunk_nr, e);
                    continue;
                }

                let chunk_meta = ChunkMeta {
                    offset,
                    length: compressed_len as u64,
                    compressed: true,
                    uncompressed_size: original_size as u64,
                };

                log::trace!(
                    "[writer] wrote chunk {} ({} → {} bytes) at offset {}",
                    chunk_nr,
                    original_size,
                    compressed_len,
                    offset
                );

                tx_meta.send((file_index, chunk_meta)).unwrap();
                offset += compressed_len as u64;

                stats.total_files += 1;
                stats.total_chunks += 1;
                stats.total_input_bytes += original_size as u64;
                stats.total_output_bytes += compressed_len as u64;

                cp.return_chunk_nr(chunk_nr);
            }

            log::info!("Writer finished. Compressed {} chunks ({} → {} bytes)",
                stats.total_chunks, stats.total_input_bytes, stats.total_output_bytes);

            tx_stats.send(stats).ok();
        });
    }

    // TODO: samla rx_meta och rx_stats och bygg index (eller streama till Arrow direkt i framtiden)

    Ok(())
}
