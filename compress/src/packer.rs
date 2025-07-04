use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use walkdir::WalkDir;
use zstd_sys::*;
use blake3::Hasher; // Blake3 import
use znippy_common::chunkrevolver::{ChunkRevolver, RevolverChunk, SendPtr, get_chunk_slice};
use znippy_common::common_config::CONFIG;
use znippy_common::{build_arrow_batch, CompressionReport};
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
    log::debug!("Found {} files to compress in {} directories", all_files.len(), total_dirs);
    let total_files = all_files.len();
    let mut file_paths = Vec::<PathBuf>::with_capacity(total_files);

    let (tx_chunk, rx_chunk): (Sender<(usize, u32, usize, bool, u16)>, Receiver<_>) = bounded(CONFIG.max_core_in_flight);
//    let (tx_compressed, rx_compressed): (Sender<(usize, ChunkMeta)>, Receiver<_>) = unbounded(); // Send metadata

    let (tx_compressed, rx_compressed): (Sender<(usize,u16, Arc<[u8]>)>, Receiver<(usize,u16, Arc<[u8]>)>) = unbounded();
    let (tx_return, rx_return): (Sender<(u32,ChunkMeta)>, Receiver<(u32, ChunkMeta)>) = unbounded();

    let output_zdata_path = output.with_extension("zdata");
    log::debug!("Creating zdata file at: {:?}", output_zdata_path);
    let zdata_file = OpenOptions::new().create(true).write(true).truncate(true).open(&output_zdata_path)?;
    let mut writer = BufWriter::with_capacity((CONFIG.file_split_block_size / 2) as usize, zdata_file);

    let mut revolver = ChunkRevolver::new();
    let base_ptr = SendPtr::new(revolver.base_ptr());
    let chunk_size = revolver.chunk_size();

    // Reader Thread with inflight counter
    let reader_thread = {
        let tx_chunk = tx_chunk.clone();
        let rx_done = rx_return.clone();
        let all_files = all_files.clone();
        let output = output.clone(); // Clone the output path here
        let mut revolver = revolver; // move into thread
        thread::spawn(move || {
            let mut inflight_chunks = 0usize;
//            let mut file_chunks_metas: Vec<Vec<ChunkMeta>> = Vec::with_capacity(all_files.len());
            let mut uncompressed_files = 0; // Track uncompressed files
            let mut uncompressed_bytes = 0; // Track uncompressed bytes

            for (file_index, path) in all_files.iter().enumerate() {
                log::debug!("[reader] Handling file index {}: {:?}", file_index, path);
                file_paths.push(path.clone());
  //              file_chunks_metas.push(Vec::new());

                let skip = !no_skip && should_skip_compression(path);
                if skip {
                    log::debug!("[reader] Skipping compression for file {}", path.display());
                    uncompressed_files += 1; // Increment uncompressed files count
                    uncompressed_bytes += path.metadata().unwrap().len(); // Add to uncompressed bytes
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
                let mut chunk_seq:u16=0;
                loop {
                    // Handle returned chunk numbers to prevent reader from blocking
                    while let Ok(returned) = rx_done.try_recv() {
                        log::debug!("[reader] Returned chunk {} to pool", returned.0);
                        revolver.return_chunk(returned.0);
                        inflight_chunks = inflight_chunks.checked_sub(1).expect("inflight_chunks underflow");
                    }

                    let chunk_index;
                    let n;
                    {
                        let mut chunk = revolver.get_chunk();
                        chunk_index = chunk.index;
                        match reader.read(&mut *chunk) {
                            Ok(0) => {
                                if !has_read_any_data {
                                    log::debug!("[reader] Detected zero-length file {}", file_index);
                                } else {
                                    log::debug!("[reader] EOF after data for file {}", file_index);
                                }
                                return_later = Some(chunk.index);
                                break;
                            }
                            Ok(bytes_read) => {
                                has_read_any_data = true;
                                n = bytes_read;
                            }
                            Err(e) => {
                                log::warn!("Error reading {:?}: {}", path, e);
                                return_later = Some(chunk.index);
                                break;
                            }
                        };
                    }

                    // Send chunk index, size, and metadata via tx_chunk
                    log::debug!("[reader] Sending chunk {} from file {} to compressor", chunk_index, file_index);
                    tx_chunk.send((file_index, chunk_index, n, skip, chunk_seq)).unwrap();
// file_chunks_metas[file_index].clone()
                    inflight_chunks += 1;
                }

                if let Some(index) = return_later {
                    revolver.return_chunk(index);
                }
            }

            // Wait for all inflight chunks to return before finishing
            while inflight_chunks > 0 {
                match rx_done.recv() {
                    Ok(returned) => {
                        log::debug!("[reader] Returned chunk {} to pool during draining", returned.0);
                        revolver.return_chunk(returned.0);
                        inflight_chunks -= 1;
                    }
                    Err(_) => {
                        log::debug!("[reader] rx_done channel closed, exiting draining loop");
                        break;
                    }
                }
            }
/*
            // After reading all data, prepare and write the index
            // Kanske flyttar denna till main då måste file_paths skickas ut tuppeln nedan

            log::info!("[reader] Writing index to file...");
            let batch = build_arrow_batch(&file_paths, &file_chunks_metas).unwrap();
            let index_path = output.with_extension("znippy");
            let index_file = File::create(&index_path).unwrap();
            let mut writer = arrow::ipc::writer::FileWriter::try_new(index_file, &batch.schema()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
            log::info!("[reader] Index written to {:?}", index_path);
*/
            drop(tx_chunk);
            log::debug!("[reader] Reader thread done, tx_chunk dropped and index written");

            // Return the statistics to the main thread
            (uncompressed_files, uncompressed_bytes)
        })
    };

    // Compressor Threads pool
    let mut compressor_threads = Vec::with_capacity(CONFIG.max_core_in_flight as usize);
    for _ in 0..CONFIG.max_core_in_flight {
        let rx_chunk = rx_chunk.clone();
        let tx_compressed = tx_compressed.clone();
        let tx_ret = tx_return.clone();
        let base_ptr = SendPtr::new(base_ptr.as_ptr()); // create new SendPtr for each thread
        let chunk_size = chunk_size;

        let handle = thread::spawn(move || {
            unsafe {
                let cctx = ZSTD_createCCtx();
                assert!(!cctx.is_null(), "ZSTD_createCCtx failed");

                ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, CONFIG.compression_level);
                ZSTD_CCtx_setParameter(cctx, ZSTD_c_nbWorkers, CONFIG.max_core_in_compress as i32);
                log::info!("[compressor] Compressor thread started with level {} and {} workers", CONFIG.compression_level, CONFIG.max_core_in_compress);

                while let Ok((file_index, chunk_index, used, skip, chunk_seq)) = rx_chunk.recv() {
                    log::debug!("[compressor] Processing chunk {} from file {}: {} bytes", chunk_index, file_index, used);
                    let input = get_chunk_slice(base_ptr.as_ptr(), chunk_size, chunk_index, used);
                    let chunk_meta;
                    let output:Arc<[u8]>;

                    let mut hasher = Hasher::new(); // Blake3 hash initialization

                    if skip {
                        log::debug!("[compressor] Skipping compression for chunk {} of file {}", chunk_index, file_index);
                        output =  Arc::from(input);
                        hasher.update(&output);  // Update hash for uncompressed data
                        chunk_meta = ChunkMeta {
                            offset: 0,
                            length: used as u64,
                            compressed: false,
                            uncompressed_size: used as u64,
                            checksum: Some(hasher.finalize().as_bytes().to_vec()),
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

                            // Truncate the output to the actual size of the compressed data
                            output.truncate(output_buffer.pos);

                            // Hash the compressed data
                            hasher.update(&output);

                            // Create an Arc directly from a boxed slice of the compressed data
                            let output_arc: Arc<[u8]> = Arc::from(output.into_boxed_slice());

                            chunk_meta = ChunkMeta {
                                offset: 0,
                                length: output_buffer.pos as u64,
                                compressed: true,
                                uncompressed_size: used as u64,
                                checksum: Some(hasher.finalize().as_bytes().to_vec()), // Store checksum
                            };

                            output_arc // Return the Arc<[u8]>
                        };
                    }

                    // Send compressed metadata to the next thread (writer)
                    log::debug!("[compressor] Sending chunk {} of file {} to writer", chunk_index, file_index);
                    tx_compressed.send((file_index,chunk_seq, output)).unwrap();
//                    tx_ret.send(chunk_index).unwrap();
                    // Send **metadata** (with checksum) to the reader for aggregation and indexing
                    log::debug!("[compressor] Sending metadata for chunk {} of file {} to reader", chunk_index, file_index);
                    tx_ret.send((chunk_index, chunk_meta)).unwrap();  // Send chunk metadata to reader (including checksum)
                }

                ZSTD_freeCCtx(cctx);
                drop(tx_compressed);
                drop(tx_ret);
                log::debug!("[compressor] Compressor thread finished processing.");
            }
        });
        compressor_threads.push(handle);
    }

    // Writer thread
    let writer_thread = thread::spawn(move || {
        let mut offset = 0u64;
        let mut stats = CompressionStats::default();

        while let Ok((file_index,chunk_seq ,compressed_data)) = rx_compressed.recv() {
            // Log when the writer receives a chunk
            log::debug!("[writer] Received compressed block from file {}: offset {}, length {}", file_index, offset, compressed_data.len());

//            let compressed_data = vec![0u8; compressed_data.len() as usize]; // placeholder

            if let Err(e) = writer.write_all(&compressed_data) {
                log::error!("[writer] Write error: {}", e);
                continue;
            }

//            chunk_meta.offset = offset;
            offset += compressed_data.len() as u64;

            stats.total_chunks += 1;
//            stats.total_input_bytes += chunk_meta.uncompressed_size;
//            stats.total_output_bytes += chunk_meta.length;

            log::debug!("[writer] Writing compressed chunk at offset {} ({} bytes)", offset, compressed_data.len());
        }

        log::info!("[writer] Writer done. Compressed {} chunks ({} → {} bytes)", stats.total_chunks, stats.total_input_bytes, stats.total_output_bytes);
        stats
    });

    // Wait for threads to finish
    let (uncompressed_files, uncompressed_bytes) = reader_thread.join().unwrap();

    for handle in compressor_threads {
        handle.join().unwrap();
    }

    let stats = writer_thread.join().unwrap();

    let report = CompressionReport {
        total_files,
        compressed_files: stats.total_chunks,
        uncompressed_files,
        total_dirs,
        total_bytes_in: uncompressed_bytes,  // Adjusted to the uncompressed bytes
        total_bytes_out: stats.total_output_bytes,
        compressed_bytes: stats.total_output_bytes,
        uncompressed_bytes,
        compression_ratio: if uncompressed_bytes > 0 {
            (stats.total_output_bytes as f32 / uncompressed_bytes as f32) * 100.0
        } else {
            0.0
        },
    };

    Ok(report)
}
