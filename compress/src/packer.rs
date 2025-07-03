//! Multithreaded file compressor using preallocated chunk buffers and zero-copy RingBuffer coordination.

use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use walkdir::WalkDir;
use zstd_sys::*;

use znippy_common::chunkpool::ChunkPool;
use znippy_common::common_config::CONFIG;
use znippy_common::CompressionReport;
use znippy_common::meta::{ChunkMeta, CompressionStats};
use zstd_sys::ZSTD_cParameter::{ZSTD_c_compressionLevel, ZSTD_c_nbWorkers};
use zstd_sys::ZSTD_ResetDirective::ZSTD_reset_session_only;

pub fn compress_dir(input_dir: &PathBuf, output: &PathBuf) -> anyhow::Result<CompressionReport> {
    log::debug!("Reading directory: {:?}", input_dir);
    let all_files: Vec<PathBuf> = WalkDir::new(input_dir)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter(|e| e.file_type().is_file())
        .map(|e| e.into_path())
        .collect();
    log::debug!("Found {} files to compress", all_files.len());
    let total_files = all_files.len();

    let (tx_chunk, rx_chunk): (Sender<(usize, u32, Arc<[u8]>, usize)>, Receiver<_>) = bounded(CONFIG.max_chunks as usize);
    let (tx_compressed, rx_compressed): (Sender<(usize,  ChunkMeta, Vec<u8>)>, Receiver<_>) = unbounded();
    let (tx_return, rx_return): (Sender<u32>, Receiver<u32>) = unbounded();

    let output_zdata_path = output.with_extension("zdata");
    log::debug!("Creating zdata file at: {:?}", output_zdata_path);
    let zdata_file = OpenOptions::new().create(true).write(true).truncate(true).open(&output_zdata_path)?;
    let mut writer = BufWriter::with_capacity((CONFIG.file_split_block_size / 2) as usize, zdata_file);

    let reader_thread = {
        let tx_chunk = tx_chunk.clone();
        let rx_done = rx_return.clone();
        let all_files = all_files.clone();
        thread::spawn(move || {
            let mut chunk_pool = ChunkPool::new();
            for (file_index, path) in all_files.iter().enumerate() {
                log::debug!("[reader] Handling file index {}: {:?}", file_index, path);
                let file = match File::open(path) {
                    Ok(f) => f,
                    Err(e) => {
                        log::warn!("Failed to open file {:?}: {}", path, e);
                        continue;
                    }
                };
                let mut reader = BufReader::new(file);
                let mut has_read_any_data = false;

                loop {
                    log::debug!("[reader] Attempting to get chunk index");
                    let chunk_nr = chunk_pool.get_index();
                    log::debug!("[reader] Got chunk index: {}", chunk_nr);
                    let buf = chunk_pool.get_buffer_mut(chunk_nr);
                    let n = match reader.read(&mut buf[..]) {
                        Ok(0) => {
                            if !has_read_any_data {
                                log::debug!("[reader] Detected zero-length file {}", file_index);
                                // Optional: count as skipped, or send empty compressed data
                            } else {
                                log::debug!("[reader] EOF after data for file {}", file_index);
                            }

                            chunk_pool.return_index(chunk_nr);
                            break;
                        }
                        Ok(n) => {
                            has_read_any_data = true;
                            n
                        }
                        Err(e) => {
                            log::warn!("Error reading {:?}: {}", path, e);
                            chunk_pool.return_index(chunk_nr);
                            break;
                        }
                    };
                    let buf_arc = chunk_pool.get_buffer(chunk_nr);
                    log::debug!("[reader] file={} read {} bytes into chunk {}", file_index, n, chunk_nr);
                    tx_chunk.send((file_index, chunk_nr, buf_arc, n)).unwrap();

                    if let Ok(returned) = rx_done.try_recv() {
                        log::debug!("[reader] Returned chunk {} to pool", returned);
                        chunk_pool.return_index(returned);
                    }
                }
            }
            log::debug!("Reader thread done");
        })
    };

    let compressor_thread = {
        let tx_ret = tx_return.clone();
        thread::spawn(move || {
            unsafe {
                let cctx = ZSTD_createCCtx();
                assert!(!cctx.is_null(), "ZSTD_createCCtx failed");

                ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, CONFIG.compression_level);
                ZSTD_CCtx_setParameter(cctx, ZSTD_c_nbWorkers, CONFIG.max_core_in_compress as i32);
                log::info!("Compressor ready with level {} and {} workers", CONFIG.compression_level, CONFIG.max_core_in_compress);

                while let Ok((file_index, chunk_nr, arc_buf, used)) = rx_chunk.recv() {
                    log::debug!("[compressor] Received chunk {} from file {} ({} bytes)", chunk_nr, file_index, used);
                    let input_ptr = arc_buf.as_ptr();

                    let mut output = vec![0u8; CONFIG.zstd_output_buffer_size];

                    ZSTD_CCtx_reset(cctx, ZSTD_reset_session_only);

                    let mut input = ZSTD_inBuffer {
                        src: input_ptr as *const _,
                        size: used,
                        pos: 0,
                    };

                    let mut output_buffer = ZSTD_outBuffer {
                        dst: output.as_mut_ptr() as *mut _,
                        size: output.len(),
                        pos: 0,
                    };

                    let code = ZSTD_compressStream2(
                        cctx,
                        &mut output_buffer,
                        &mut input,
                        ZSTD_EndDirective::ZSTD_e_end,
                    );

                    if ZSTD_isError(code) != 0 {
                        let err_str = std::ffi::CStr::from_ptr(ZSTD_getErrorName(code));
                        panic!("ZSTD error: {}", err_str.to_string_lossy());
                    }

                    let total_written = output_buffer.pos;
                    let chunk_meta = ChunkMeta {
                        offset: 0,
                        length: total_written as u64,
                        compressed: true,
                        uncompressed_size: used as u64,
                    };

                    log::debug!("[compressor] file={} chunk={} {} → {} bytes", file_index, chunk_nr, used, total_written);

                    output.truncate(total_written);
                    tx_compressed.send((file_index, chunk_meta, output)).unwrap();
                    log::debug!("[compressor] Done with chunk {} → returning to reader", chunk_nr);
                    tx_ret.send(chunk_nr).unwrap();
                }

                ZSTD_freeCCtx(cctx);
                log::debug!("Compressor thread done");
            }
        })
    };

    let writer_thread = thread::spawn(move || {
        let mut offset = 0u64;
        let mut stats = CompressionStats::default();

        while let Ok((file_index,  mut meta, compressed_data)) = rx_compressed.recv() {
            log::debug!("[writer] Compressed block for file {}: {} bytes", file_index, meta.length);
            if let Err(e) = writer.write_all(&compressed_data[..]) {
                log::error!("Write error: {}", e);
                continue;
            }

            meta.offset = offset;
            offset += meta.length;

            stats.total_chunks += 1;
            stats.total_input_bytes += meta.uncompressed_size;
            stats.total_output_bytes += meta.length;

            log::trace!("[writer] file={} wrote {} → {} bytes at offset {}", file_index, meta.uncompressed_size, meta.length, meta.offset);
        }

        log::info!("Writer done. Compressed {} chunks ({} → {} bytes)", stats.total_chunks, stats.total_input_bytes, stats.total_output_bytes);
        stats
    });

    log::debug!("Waiting for reader thread to finish...");
    reader_thread.join().unwrap();
    log::debug!("Reader thread joined");

    log::debug!("Waiting for compressor thread to finish...");
    compressor_thread.join().unwrap();
    log::debug!("Compressor thread joined");

    log::debug!("Waiting for writer thread to finish...");
    let stats = writer_thread.join().unwrap();
    log::debug!("Writer thread joined");

    let compressed_files = stats.total_chunks;

    let report = CompressionReport {
        total_files,
        compressed_files,
        uncompressed_files: 0,
        total_dirs: 0,
        total_bytes_in: stats.total_input_bytes,
        total_bytes_out: stats.total_output_bytes,
        compressed_bytes: stats.total_output_bytes,
        uncompressed_bytes: 0,
        compression_ratio: if stats.total_input_bytes > 0 {
            (stats.total_output_bytes as f32 / stats.total_input_bytes as f32) * 100.0
        } else {
            0.0
        },
    };

    log::debug!("Compression report: {:?}", report);
    Ok(report)
}
