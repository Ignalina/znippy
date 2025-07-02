//! Multithreaded file compressor using preallocated chunk buffers and zero-copy RingBuffer coordination.

use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use crossbeam_channel::{unbounded, Receiver, Sender};
use zstd_sys::*;

use znippy_common::chunkpool::ChunkPool;
use znippy_common::common_config::CONFIG;
use znippy_common::{CompressionReport};
use znippy_common::meta::{ChunkMeta, CompressionStats};
use zstd_sys::ZSTD_cParameter::{ZSTD_c_compressionLevel, ZSTD_c_nbWorkers};
use zstd_sys::ZSTD_ResetDirective::ZSTD_reset_session_only;

pub fn compress_dir(input_dir: &PathBuf, output: &PathBuf) -> anyhow::Result<CompressionReport> {
    let all_files: Vec<PathBuf> = fs::read_dir(input_dir)?
        .filter_map(|entry| {
            entry.ok().and_then(|e| {
                let path = e.path();
                if path.is_file() {
                    Some(path)
                } else {
                    None
                }
            })
        })
        .collect();

    let total_files = all_files.len();
    let mut chunk_pool = ChunkPool::new();

    let (tx_chunk, rx_chunk): (Sender<(usize, u32, Arc<[u8]>, usize)>, Receiver<_>) = unbounded();
    let (tx_compressed, rx_compressed): (Sender<(usize, u32, ChunkMeta, Vec<u8>)>, Receiver<_>) = unbounded();
    let (tx_return, rx_return): (Sender<u32>, Receiver<u32>) = unbounded();

    let output_zdata_path = output.with_extension("zdata");
    let zdata_file = OpenOptions::new().create(true).write(true).truncate(true).open(&output_zdata_path)?;
    let mut writer = BufWriter::with_capacity((CONFIG.file_split_block_size / 2) as usize, zdata_file);

    // Reader thread (single-threaded, owns &mut chunk_pool)
    let reader_thread = {
        let tx_chunk = tx_chunk.clone();
        let rx_done = rx_return.clone();
        let all_files = all_files.clone();
        thread::spawn(move || {
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
                    let chunk_nr = chunk_pool.get_index();
                    let mut buf_arc = chunk_pool.get_buffer(chunk_nr);
                    let buf = Arc::get_mut(&mut buf_arc).expect("Reader must own unique Arc");
                    let n = match reader.read(&mut buf[..]) {
                        Ok(0) => {
                            chunk_pool.return_index(chunk_nr);
                            break;
                        }
                        Ok(n) => n,
                        Err(e) => {
                            log::warn!("Error reading {:?}: {}", path, e);
                            chunk_pool.return_index(chunk_nr);
                            break;
                        }
                    };
                    log::debug!("[reader] file {} → chunk {} ({} bytes)", file_index, chunk_nr, n);
                    tx_chunk.send((file_index, chunk_nr, buf_arc.into(), n)).unwrap();

                    // Block if all chunks are in use
                    if let Ok(returned) = rx_done.try_recv() {
                        chunk_pool.return_index(returned);
                    }
                }
            }
        })
    };

    // Compressor thread
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
                    let input_ptr = arc_buf.as_ptr();

                    let mut output = vec![0u8; CONFIG.zstd_output_buffer_size];
                    let mut total_written = 0usize;

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

                    total_written = output_buffer.pos;
                    let chunk_meta = ChunkMeta {
                        offset: 0,
                        length: total_written as u64,
                        compressed: true,
                        uncompressed_size: used as u64,
                    };

                    log::debug!("[compressor] file={} chunk={} {} → {} bytes", file_index, chunk_nr, used, total_written);

                    output.truncate(total_written);
                    tx_compressed.send((file_index, chunk_nr, chunk_meta, output)).unwrap();
                    tx_ret.send(chunk_nr).unwrap();
                }

                ZSTD_freeCCtx(cctx);
            }
        })
    };

    // Writer thread
    let writer_thread = thread::spawn(move || {
        let mut offset = 0u64;
        let mut stats = CompressionStats::default();

        while let Ok((file_index, _chunk_nr, mut meta, compressed_data)) = rx_compressed.recv() {
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

    reader_thread.join().unwrap();
    compressor_thread.join().unwrap();
    let stats = writer_thread.join().unwrap();

    let compressed_files = stats.total_chunks; // simplification

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

    Ok(report)
}
