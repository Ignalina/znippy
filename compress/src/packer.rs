//! Multithreaded file compressor using preallocated chunk buffers and zero-copy RingBuffer coordination.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;

use crossbeam_channel::{unbounded};
use zstd_sys::*;

use znippy_common::chunkpool::ChunkPool;
use znippy_common::common_config::CONFIG;
use znippy_common::meta::{ChunkMeta, CompressionStats};
use zstd_sys::ZSTD_cParameter::{ZSTD_c_compressionLevel, ZSTD_c_nbWorkers};
use zstd_sys::ZSTD_ResetDirective::ZSTD_reset_session_only;

pub fn compress_dir(all_files: Vec<PathBuf>, output: PathBuf) -> anyhow::Result<()>  {
    let chunk_pool = Arc::new(ChunkPool::new());

    let (tx_chunk, rx_chunk) = unbounded::<(u32, usize, usize)>(); // (chunk_nr, size, file_index)
    let (tx_compressed, rx_compressed) = unbounded::<(usize, u32, Vec<u8>, usize, usize)>(); // (file_index, chunk_nr, compressed_data, out_len, in_len)
    let (tx_return, rx_return) = unbounded::<u32>(); // chunk_nr
    let (tx_meta, rx_meta) = unbounded::<(usize, ChunkMeta)>();
    let (tx_stats, rx_stats) = unbounded::<CompressionStats>();

    log::info!(
        "Starting compression: {} files, {} chunks, chunk size {} bytes",
        all_files.len(),
        CONFIG.max_chunks,
        CONFIG.file_split_block_size
    );

    // Writer setup
    let output_zdata_path = output.with_extension("zdata");
    let zdata_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&output_zdata_path)?;
    
    let writer_buf_size = CONFIG.file_split_block_size/2;
    let mut writer = BufWriter::with_capacity(writer_buf_size as usize, zdata_file);

    // Reader thread
    {
        let cp = Arc::clone(&chunk_pool);
        let tx = tx_chunk.clone();
        let rx_done = rx_return.clone();
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
                    let chunk_nr = cp.get_index(); // blockerar om slut på chunkar
                    let mut arc_buf = cp.get_buffer(chunk_nr);
                    let buf = Arc::get_mut(&mut arc_buf).expect("Unique Arc required");
                    let n = match reader.read(&mut buf[..]) {
                        Ok(0) => {
                            cp.return_index(chunk_nr);
                            break;
                        }
                        Ok(n) => n,
                        Err(e) => {
                            log::warn!("Error reading {:?}: {}", path, e);
                            cp.return_index(chunk_nr);
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
        let tx_ret = tx_return.clone();
        std::thread::spawn(move || {
            unsafe {
                let cctx = ZSTD_createCCtx();
                assert!(!cctx.is_null(), "ZSTD_createCCtx failed");

                ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, CONFIG.compression_level);
                ZSTD_CCtx_setParameter(cctx, ZSTD_c_nbWorkers, CONFIG.max_core_in_compress as i32);
                log::info!("Compressor ready with level {} and {} workers",
                    CONFIG.compression_level, CONFIG.max_core_in_compress);

                while let Ok((chunk_nr, used, file_index)) = rx_chunk.recv() {
                    let buf = cp.get_buffer(chunk_nr);
                    let input_ptr = buf.as_ptr();

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

                    log::debug!(
                        "[compressor] file={} chunk={} {} → {} bytes",
                        file_index,
                        chunk_nr,
                        used,
                        total_written
                    );

                    tx.send((file_index, chunk_nr, output, total_written, used)).unwrap();
                    tx_ret.send(chunk_nr).unwrap();
                }

                ZSTD_freeCCtx(cctx);
            }
        });
    }

    // Writer thread
    {
        let cp = Arc::clone(&chunk_pool);
        std::thread::spawn(move || {
            let mut offset = 0u64;
            let mut stats = CompressionStats::default();

            while let Ok((file_index, chunk_nr, compressed_data, compressed_len, original_size)) = rx_compressed.recv() {
                if let Err(e) = writer.write_all(&compressed_data[..compressed_len]) {
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

                stats.total_chunks += 1;
                stats.total_input_bytes += original_size as u64;
                stats.total_output_bytes += compressed_len as u64;

                // Note: pool return already handled via tx_return above
            }

            log::info!("Writer done. Compressed {} chunks ({} → {} bytes)",
                stats.total_chunks, stats.total_input_bytes, stats.total_output_bytes);

            tx_stats.send(stats).ok();
        });
    }

    // TODO: samla rx_meta + rx_stats → .znippy

    Ok(())
}
