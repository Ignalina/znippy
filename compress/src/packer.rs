// compress/src/packer.rs

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::thread;

use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use walkdir::WalkDir;
use zstd_sys::*;

use znippy_common::chunkrevolver::{ChunkRevolver, RevolverChunk, SendPtr, get_chunk_slice};
use znippy_common::common_config::CONFIG;
use znippy_common::CompressionReport;
use znippy_common::meta::{ChunkMeta, CompressionStats};
use znippy_common::index::should_skip_compression;
use zstd_sys::ZSTD_cParameter::{ZSTD_c_compressionLevel, ZSTD_c_nbWorkers};
use zstd_sys::ZSTD_ResetDirective::ZSTD_reset_session_only;

pub fn compress_dir(input_dir: &PathBuf, output: &PathBuf, no_skip: bool) -> anyhow::Result<CompressionReport> {
    log::debug!("Reading directory: {:?}", input_dir);
    let mut total_dirs = 0;
    let all_files: Vec<PathBuf> = WalkDir::new(input_dir)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter_map(|e| {
            if e.file_type().is_dir() {
                total_dirs += 1;
                None
            } else if e.file_type().is_file() {
                Some(e.into_path())
            } else {
                None
            }
        })
        .collect();
    log::info!("Found {} files to compress in {} directories", all_files.len(), total_dirs);
    let total_files = all_files.len();

    let (tx_chunk, rx_chunk): (Sender<(usize, u32, usize, bool)>, Receiver<_>) = bounded(CONFIG.max_chunks as usize);
    let (tx_compressed, rx_compressed): (Sender<(usize, ChunkMeta, Vec<u8>)>, Receiver<_>) = unbounded();
    let (tx_return, rx_return): (Sender<u32>, Receiver<u32>) = unbounded();

    let output_zdata_path = output.with_extension("zdata");
    log::info!("Creating zdata file at: {:?}", output_zdata_path);
    let zdata_file = OpenOptions::new().create(true).write(true).truncate(true).open(&output_zdata_path)?;
    let mut writer = BufWriter::with_capacity((CONFIG.file_split_block_size / 2) as usize, zdata_file);

    let mut revolver = ChunkRevolver::new();
    let base_ptr = SendPtr::new(revolver.base_ptr());
    let chunk_size = revolver.chunk_size();

    let reader_thread = {
        let tx_chunk = tx_chunk.clone();
        let rx_done = rx_return.clone();
        let all_files = all_files.clone();
        let mut revolver = revolver; // move into thread
        thread::spawn(move || {
            for (file_index, path) in all_files.iter().enumerate() {
                log::info!("[reader] Processing file {}: {:?}", file_index, path);

                let skip = !no_skip && should_skip_compression(path);
                if skip {
                    log::info!("[reader] Skipping compression for file {}", path.display());
                }

                let file = match File::open(path) {
                    Ok(f) => f,
                    Err(e) => {
                        log::warn!("Failed to open file {:?}: {}", path, e);
                        continue;
                    }
                };
                let mut reader = BufReader::new(file);
                let mut has_read_any_data = false;
                let mut return_later = None;

                loop {
                    let chunk_index;
                    let n;
                    {
                        let mut chunk = revolver.get_chunk();
                        chunk_index = chunk.index;
                        match reader.read(&mut *chunk) {
                            Ok(0) => {
                                if !has_read_any_data {
                                    log::info!("[reader] Detected zero-length file {}", file_index);
                                } else {
                                    log::info!("[reader] EOF after data for file {}", file_index);
                                }
                                return_later = Some(chunk.index);
                                break;
                            }
                            Ok(bytes_read) => {
                                has_read_any_data = true;
                                n = bytes_read;
                                log::debug!("[reader] Read {} bytes from file {} into chunk {}", n, file_index, chunk_index);
                            }
                            Err(e) => {
                                log::warn!("Error reading {:?}: {}", path, e);
                                return_later = Some(chunk.index);
                                break;
                            }
                        }
                    }

                    tx_chunk.send((file_index, chunk_index, n, skip)).unwrap();

                    if let Ok(returned) = rx_done.try_recv() {
                        log::debug!("[reader] Returned chunk {} to pool", returned);
                        revolver.return_chunk(returned);
                    }
                }

                if let Some(index) = return_later {
                    revolver.return_chunk(index);
                }
            }
            log::info!("Reader thread done");
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

                while let Ok((file_index, chunk_index, used, skip)) = rx_chunk.recv() {
                    let input = get_chunk_slice(base_ptr.as_ptr(), chunk_size, chunk_index, used);
                    let chunk_meta;
                    let output;

                    if skip {
                        log::debug!("[compressor] Skipping compression for chunk {} of file {}", chunk_index, file_index);
                        output = input.to_vec();
                        chunk_meta = ChunkMeta {
                            offset: 0,
                            length: used as u64,
                            compressed: false,
                            uncompressed_size: used as u64,
                        };
                    } else {
                        log::debug!("[compressor] Compressing chunk {} from file {} ({} bytes)", chunk_index, file_index, used);

                        output = {
                            let input_ptr = input.as_ptr();
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

                            output.truncate(output_buffer.pos);
                            chunk_meta = ChunkMeta {
                                offset: 0,
                                length: output_buffer.pos as u64,
                                compressed: true,
                                uncompressed_size: used as u64,
                            };

                            output
                        };
                    }

                    tx_compressed.send((file_index, chunk_meta, output)).unwrap();
                    tx_ret.send(chunk_index).unwrap();
                }

                ZSTD_freeCCtx(cctx);
                log::info!("Compressor thread done");
            }
        })
    };

    let writer_thread = thread::spawn(move || {
        let mut offset = 0u64;
        let mut stats = CompressionStats::default();

        while let Ok((file_index, mut meta, compressed_data)) = rx_compressed.recv() {
            if let Err(e) = writer.write_all(&compressed_data[..]) {
                log::error!("Write error: {}", e);
                continue;
            }

            log::debug!("[writer] Writing file {} chunk at offset {} ({} bytes)", file_index, offset, meta.length);

            meta.offset = offset;
            offset += meta.length;

            stats.total_chunks += 1;
            stats.total_input_bytes += meta.uncompressed_size;
            stats.total_output_bytes += meta.length;
        }

        log::info!("Writer done. Compressed {} chunks ({} → {} bytes)", stats.total_chunks, stats.total_input_bytes, stats.total_output_bytes);
        stats
    });

    reader_thread.join().unwrap();
    compressor_thread.join().unwrap();
    let stats = writer_thread.join().unwrap();

    let report = CompressionReport {
        total_files,
        compressed_files: stats.total_chunks,
        uncompressed_files: 0,
        total_dirs,
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

    log::info!("Compression completed: {:?}", report);
    Ok(report)
}
