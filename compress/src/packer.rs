use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
<<<<<<< Updated upstream
use std::net::Shutdown::Write as OtherWrite;
=======
use std::net::Shutdown::Write;
>>>>>>> Stashed changes
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
use znippy_common::meta::{ChunkMeta, WriterStats};
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
    let total_files:u64 = all_files.len() as u64;
    let mut file_paths = Vec::<PathBuf>::with_capacity(total_files as usize);

    let (tx_chunk, rx_chunk): (Sender<(u64, u64, u64, bool, u64)>, Receiver<_>) = bounded(CONFIG.max_core_in_flight);

    let (tx_compressed, rx_compressed): (Sender<(u64,u64, Arc<[u8]>)>, Receiver<(u64,u64, Arc<[u8]>)>) = unbounded();
    let (tx_return, rx_return): (Sender<u64>, Receiver<u64>) = unbounded();

    let output_zdata_path = output.with_extension("zdata");
    log::debug!("Creating zdata file at: {:?}", output_zdata_path);
    let zdata_file = OpenOptions::new().create(true).write(true).truncate(true).open(&output_zdata_path)?;
    let mut writer = BufWriter::with_capacity((CONFIG.file_split_block_size / 2) as usize, zdata_file);

    let revolver = ChunkRevolver::new();
    let base_ptr = SendPtr::new(revolver.base_ptr());
    let chunk_size = revolver.chunk_size();

    // Reader Thread with inflight counter
    let reader_thread = {
        let mut local_file_paths = Vec::with_capacity(all_files.len());
        let tx_chunk = tx_chunk.clone();
        let rx_done = rx_return.clone();
        let all_files = all_files.clone();
        let output = output.clone(); // Clone the output path here
        let mut revolver = revolver; // move into thread
        thread::spawn(move || {
            let mut inflight_chunks = 0usize;
//            let mut file_chunks_metas: Vec<Vec<ChunkMeta>> = Vec::with_capacity(all_files.len());
            let mut uncompressed_files:u64 = 0;
            let mut uncompressed_bytes:u64 = 0;
            let mut compressed_files:u64=0;
            let mut compressed_bytes:u64=0;

            for (file_index, path) in all_files.iter().enumerate() {
                log::debug!("[reader] Handling file index {}: {:?}", file_index, path);
                file_paths.push(path.clone());
  //              file_chunks_metas.push(Vec::new());

                let skip = !no_skip && should_skip_compression(path);
                if skip {
                    log::debug!("[reader] Skipping compression for file {}", path.display());
                    uncompressed_files += 1; // Increment uncompressed files count
                    uncompressed_bytes += path.metadata().unwrap().len(); // Add to uncompressed bytes
                } else {
                    compressed_files += 1;
                    compressed_bytes += path.metadata().unwrap().len();
                }

                let file = match File::open(path) {
                    Ok(f) => f,
                    Err(e) => {
                        panic!("Problem opening the file: {:?}: {}", path, e)
                    }
                };
                let mut reader = BufReader::new(file);
                let mut has_read_any_data = false;
                let mut return_later = None;
                let chunk_seq:u64=0;
                loop {
                    // Handle returned chunk numbers to prevent reader from blocking
                    while let Ok(returned) = rx_done.try_recv() {
                        log::debug!("[reader] Returned chunk {} to pool", returned);
                        revolver.return_chunk(returned);
                        inflight_chunks = inflight_chunks.checked_sub(1).expect("inflight_chunks underflow");
                    }

                    let chunk_index:u64;
                    let n:u64;
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
                                n = bytes_read as u64;
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
                    tx_chunk.send((file_index as u64, chunk_index, n, skip, chunk_seq)).unwrap();
// file_chunks_metas[file_index].clone()
                    inflight_chunks += 1;
                }

                if let Some(index) = return_later {
                    revolver.return_chunk(index);
                }
            }
            // Reader thread cleanup
            log::debug!("[reader] Reader thread done, tx_chunk dropped");

            // Wait for all inflight chunks to return before finishing
            while inflight_chunks > 0 {
                match rx_done.recv() {
                    Ok(returned) => {
                        log::debug!("[reader] Returned chunk {} to pool during draining", returned);
                        revolver.return_chunk(returned);
                        inflight_chunks -= 1;
                    }
                    Err(_) => {
                        log::debug!("[reader] rx_done channel closed, exiting draining loop");
                        break;
                    }
                }
            }

            // Drop the sender side of the channel `tx_chunk` to signal that no more data will be sent
            drop(tx_chunk); // This ensures that tx_chunk is no longer available and allows the receiver to detect closure
            log::debug!("[reader] tx_chunk dropped after finishing all chunk sends");

            // Drop other resources after processing is complete
            drop(rx_done);  // Drop rx_done after we're done draining inflight chunks
            log::debug!("[reader] rx_done dropped after processing all chunks");

            // Optionally, drop other references (Rust will drop them at thread exit, but this makes it explicit)
  //          drop(file_paths);  // Drop file_paths if needed
            drop(revolver);    // Drop revolver if needed (Rust will also drop it when the thread finishes)

            // Return the statistics to the main thread
<<<<<<< Updated upstream
            (uncompressed_files, uncompressed_bytes,compressed_files,compressed_bytes,local_file_paths)})
=======
            (uncompressed_files, uncompressed_bytes,compressed_files,compressed_bytes)})
>>>>>>> Stashed changes
    };

    // Compressor Threads pool
    // chunk_nr is index in the shared chunk array
    // chunk_index is a sequence within the file.


    let mut compressor_threads = Vec::with_capacity(CONFIG.max_core_in_flight as usize);
    for _ in 0..CONFIG.max_core_in_flight {
        let rx_chunk = rx_chunk.clone();
        let tx_compressed = tx_compressed.clone();
        let tx_ret = tx_return.clone();
        let base_ptr = SendPtr::new(base_ptr.as_ptr()); // create new SendPtr for each thread
        let chunk_size = chunk_size;

        let handle = thread::spawn(move || {
            let mut local_chunkmeta: Vec<ChunkMeta> = Vec::new();

            unsafe {
                let cctx = ZSTD_createCCtx();
                assert!(!cctx.is_null(), "ZSTD_createCCtx failed");

                ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, CONFIG.compression_level);
                ZSTD_CCtx_setParameter(cctx, ZSTD_c_nbWorkers, CONFIG.max_core_in_compress as i32);
                log::info!("[compressor] Compressor thread started with level {} and {} workers", CONFIG.compression_level, CONFIG.max_core_in_compress);

                loop {
                    match rx_chunk.recv() {
                        Ok((file_index, chunk_nr, used, skip, chunk_index)) => {
                            log::debug!("[compressor] Processing chunk {} from file {}: {} bytes", chunk_nr, file_index, used);
                            let input = get_chunk_slice(base_ptr.as_ptr(), chunk_size, chunk_nr as u32, used as usize);
                            let chunk_meta;
                            let output: Arc<[u8]>;

                            let mut hasher = Hasher::new(); // Blake3 hash initialization

                            if skip {
                                log::debug!("[compressor] Skipping compression for chunk {} of file {}", chunk_index, file_index);
                                output = Arc::from(input);
                                hasher.update(&output);  // Update hash for uncompressed data
                                chunk_meta = ChunkMeta {
                                    file_index: file_index as u64,
                                    chunk_index: chunk_index ,
                                    offset: 0,
                                    length: used as u64,
                                    compressed: false,
                                    uncompressed_size: used as u64,
                                    checksum: *hasher.finalize().as_bytes(),
                                };
                            } else {
                                log::debug!("[compressor] Compressing chunk {} from file {} ({} bytes)", chunk_index, file_index, used);

                                output = {
                                    let input_ptr = input.as_ptr();
                                    let mut output = vec![0u8; CONFIG.zstd_output_buffer_size];

                                    ZSTD_CCtx_reset(cctx, ZSTD_reset_session_only);

                                    let mut input = ZSTD_inBuffer {
                                        src: input_ptr as *const _,
                                        size: used as usize,
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
                                        file_index,
                                        chunk_index,
                                        offset: 0,
                                        length: output_buffer.pos as u64,
                                        compressed: true,
                                        uncompressed_size: used as u64,
                                        checksum: *hasher.finalize().as_bytes(), // Store checksum
                                    };

                                    output_arc // Return the Arc<[u8]>
                                };
                            }
                            // Send compressed metadata to the next thread (writer)
                            log::debug!("[compressor] Sending chunk {} of file {} to writer", chunk_index, file_index);
                            tx_compressed.send((file_index, chunk_index, output)).unwrap();
                            //                    tx_ret.send(chunk_index).unwrap();
                            // Send **metadata** (with checksum) to the reader for aggregation and indexing
                            log::debug!("[compressor] Sending metadata for chunk {} of file {} to reader", chunk_index, file_index);
                            tx_ret.send(chunk_nr);  // Send chunk metadata to reader (including checksum)
                        }
                        Err(_) => {
                            // Channel is closed, gracefully exit the loop
                            log::debug!("[compressor] rx_chunk channel closed, compressor exiting");
                            break;
                        }
                    }
                }

                ZSTD_freeCCtx(cctx);

                // Drop sender-side channels after the receiver finishes
                drop(tx_compressed);
                drop(tx_ret);

                // Finally, drop the receiver channel
                drop(rx_chunk);

                log::debug!("[compressor] Compressor thread finished processing.");

            }
            local_chunkmeta
        });
        compressor_threads.push(handle);
    }

    // Writer thread
    let writer_thread = thread::spawn(move || {
        let mut writerstats=WriterStats {
            offset: 0,
            total_chunks: 0,
            total_written_bytes: 0,
        };
 

        while let Ok((file_index,chunk_seq ,compressed_data)) = rx_compressed.recv() {
<<<<<<< Updated upstream
            log::debug!("[writer] Received compressed block from file {}: offset {}, length {}", file_index, writerstats.offset, compressed_data.len());
=======
            log::debug!("[writer] Received compressed block from file {}: offset {}, length {}", file_index, offset, compressed_data.len());
>>>>>>> Stashed changes


            if let Err(e) = writer.write_all(&compressed_data) {
                log::error!("[writer] Write error: {}", e);
                continue;
            }
            writerstats.offset += compressed_data.len() as u64;
<<<<<<< Updated upstream
            writerstats.total_chunks += 1;
            writerstats.total_written_bytes += compressed_data.len() as u64; // Update the total output bytes

=======

            writerstats.total_chunks += 1;
            writerstats.total_written_bytes += compressed_data.len() as u64; // Update the total output bytes

>>>>>>> Stashed changes
            log::debug!("[writer] Writing compressed chunk at offset {} ({} bytes)", writerstats.offset, compressed_data.len());
        }

        log::info!("[writer] Writer done. Compressed {} chunks , total written {} bytes)", writerstats.total_chunks,  writerstats.total_written_bytes);
        writerstats
    });


    // Wait for reader thread to finish
<<<<<<< Updated upstream
    let (uncompressed_files, uncompressed_bytes,compressed_files,compressed_bytes,file_paths) = reader_thread.join().unwrap();
=======
    let (uncompressed_files, uncompressed_bytes,compressed_files,compressed_bytes) = reader_thread.join().unwrap();
>>>>>>> Stashed changes
    log::debug!("[reader] reader_thread joined");

    // Drop the sender-side channels after the reader and compressor threads finish
    drop(tx_chunk); // Ensure no more chunks are sent
    log::debug!("[reader] tx_chunk dropped after reader thread finished");

    //for handle in compressor_threads {
     //   handle.join().unwrap();
     //   log::debug!("[compressor] compressor thread joined");
   // }


    let mut all_chunkmeta: Vec<Vec<ChunkMeta>> = Vec::with_capacity(CONFIG.max_core_in_compress as usize);

    for handle in compressor_threads {
        let metas = handle.join().unwrap();
        all_chunkmeta.push(metas);
    }




    // After compressor threads are done, drop tx_compressed
    drop(tx_compressed);
    log::debug!("[compressor] tx_compressed dropped after compressors finished");

    log::debug!("[writer] Waiting for writer thread to finish");
    let writerstats = writer_thread.join().unwrap();
    log::debug!("[writer] writer_thread finished");



<<<<<<< Updated upstream
    // After reading all data, prepare and write the index
    // Kanske flyttar denna till main då måste file_paths skickas ut tuppeln nedan

    log::info!("[reader] Writing index to file...");
    let batch = build_arrow_batch(&file_paths, &all_chunkmeta)?;
    let index_path = output.with_extension("znippy");
    let index_file = File::create(&index_path)?;
    let mut writer = arrow::ipc::writer::FileWriter::try_new(index_file, &batch.schema())?;
    writer.write(&batch)?;
    writer.finish()?;
    log::info!("[reader] Index written to {:?}", index_path);

=======
>>>>>>> Stashed changes
    let report = CompressionReport {
        total_files,
        compressed_files ,
        uncompressed_files,
        total_dirs,
        total_bytes_in : compressed_bytes+uncompressed_bytes,
        total_bytes_out: writerstats.total_written_bytes,
        compressed_bytes,
        uncompressed_bytes,
        compression_ratio: if uncompressed_bytes > 0 {
            (compressed_bytes as f32 / (writerstats.total_written_bytes-uncompressed_bytes)  as f32) * 100.0
        } else {
            0.0
        },
    };

    Ok(report)
}


