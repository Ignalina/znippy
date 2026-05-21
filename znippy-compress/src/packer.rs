use anyhow::Result;
use anyhow::anyhow;
use arrow::array::Array;
use arrow::ipc::writer::FileWriter;
use blake3::Hasher; // Blake3 import
use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use walkdir::WalkDir;
use znippy_common::chunkrevolver::{Chunk, ChunkRevolver, SendPtr, get_chunk_slice};
use znippy_common::common_config::CONFIG;
use znippy_common::index::{
    build_arrow_metadata_for_checksums_and_config, should_skip_compression,
    build_batch_zero_copy,
};
use znippy_common::meta::{ChunkMeta, WriterStats};
use znippy_common::{
    CompressionReport,
    split_into_microchunks,
};
fn strip_prefix<'a>(base: &'a Path, full: &'a Path) -> PathBuf {
    full.strip_prefix(base).unwrap_or(full).to_path_buf()
}
pub fn compress_dir(
    input_dir: &PathBuf,
    output: &PathBuf,
    no_skip: bool,
) -> anyhow::Result<CompressionReport> {
    log::debug!("Reading directory: {:?}", input_dir);
    let mut total_dirs = 0;
    let mut filesTOSkip = 0;
    let mut filesTOCompress = 0;

    let all_files: Arc<Vec<PathBuf>> = Arc::new(
        WalkDir::new(input_dir)
            .into_iter()
            .filter_map(|entry| entry.ok())
            .filter_map(|e| {
                if e.file_type().is_dir() {
                    total_dirs += 1;
                    None
                } else if e.file_type().is_file() {
                    let skip = !no_skip && should_skip_compression(e.path());
                    if (skip) {
                        filesTOSkip = filesTOSkip + 1;
                    } else {
                        filesTOCompress = filesTOCompress + 1;
                    }
                    Some(e.into_path())
                } else {
                    None
                }
            })
            .collect(),
    );
    log::debug!(
        "Found {} files include in {} directories. to compressed {} will skip {}",
        all_files.len(),
        total_dirs,
        filesTOCompress,
        filesTOSkip
    );

    let total_files: u64 = all_files.len() as u64;

    let (tx_chunk_array, rx_chunk_array): (
        Vec<Sender<(u64, u64, u8, u64, u64, bool)>>,
        Vec<Receiver<(u64, u64, u8, u64, u64, bool)>>,
    ) = (0..CONFIG.max_core_in_flight)
        .map(|_| bounded(CONFIG.max_chunks as usize))
        .unzip();

    let (tx_compressed, rx_compressed): (
        Sender<(Arc<[u8]>, ChunkMeta)>,
        Receiver<(Arc<[u8]>, ChunkMeta)>,
    ) = unbounded();
    let (tx_return, rx_return): (Sender<(u8, u64)>, Receiver<(u8, u64)>) = unbounded();

    let output_path = output.with_extension("znippy");

    let mut revolver = ChunkRevolver::new(&CONFIG);
    let base_ptrs = revolver.base_ptrs();

    let chunk_size = revolver.chunk_size();

    let input_dir_cloned = input_dir.clone();

    // Reader Thread with inflight counter
    let all_files_for_reader = Arc::clone(&all_files);
    let all_files_for_writer = Arc::clone(&all_files);
    let reader_thread = {
        let tx_chunk_array = tx_chunk_array.clone();
        let rx_done = rx_return.clone();
        thread::spawn(move || {
            let mut inflight_chunks = 0usize;
            let mut uncompressed_files: u64 = 0;
            let mut uncompressed_bytes: u64 = 0;
            let mut compressed_files: u64 = 0;
            let mut compressed_bytes: u64 = 0;

            for (file_index, path) in all_files_for_reader.iter().enumerate() {
                //                log::debug!("[reader] Handling file index {}: {:?}", file_index, path);

                let skip = !no_skip && should_skip_compression(path);
                if skip {
                    //                    log::debug!("[reader] Skipping compression for file {}", path.display());
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
                let mut fdata_offset: u64 = 0;
                loop {
                    let maybe_chunk = revolver.try_get_chunk();
                    match maybe_chunk {
                        Some(mut chunk) => {
                            let ring_nr = chunk.ring_nr as usize;
                            match reader.read(&mut *chunk) {
                                Ok(0) => {
                                    if !has_read_any_data {
                                        tx_chunk_array[ring_nr]
                                            .send((
                                                file_index as u64,
                                                fdata_offset,
                                                chunk.ring_nr,
                                                chunk.index,
                                                0,
                                                skip,
                                            ))
                                            .unwrap();
                                        inflight_chunks += 1;
                                    } else {
                                        let ring_nr = chunk.ring_nr;
                                        let index = chunk.index;
                                        drop(chunk); // släpp lånet innan vi rör `revolver`
                                        revolver.return_chunk(ring_nr, index);
                                    }
                                    break;
                                }
                                Ok(bytes_read) => {
                                    has_read_any_data = true;
                                    tx_chunk_array[ring_nr]
                                        .send((
                                            file_index as u64,
                                            fdata_offset,
                                            chunk.ring_nr,
                                            chunk.index,
                                            bytes_read as u64,
                                            skip,
                                        ))
                                        .unwrap();
                                    inflight_chunks += 1;
                                    fdata_offset += bytes_read as u64;
                                }
                                Err(e) => {
                                    log::warn!(
                                        "[reader] Error reading file {}: {}",
                                        path.display(),
                                        e
                                    );
                                    let ring_nr = chunk.ring_nr;
                                    let index = chunk.index;
                                    drop(chunk);
                                    revolver.return_chunk(ring_nr, index);
                                    break;
                                }
                            }
                        }
                        None => {
                            let (ring_nr, returned) =
                                rx_done.recv().expect("rx_done channel closed unexpectedly");
                            revolver.return_chunk(ring_nr, returned);
                            inflight_chunks = inflight_chunks
                                .checked_sub(1)
                                .expect("inflight_chunks underflow");
                            continue;
                        }
                    }
                }
            }
            // Reader thread cleanup
            log::debug!("[reader] Thread done about to drain compressor returning chunks ");

            // Wait for all inflight chunks to return before finishing
            while inflight_chunks > 0 {
                match rx_done.recv() {
                    Ok((ring_nr, returned)) => {
                        log::debug!(
                            "[reader] Returned chunk {} to pool during draining",
                            returned
                        );
                        revolver.return_chunk(ring_nr, returned);
                        inflight_chunks -= 1;
                    }
                    Err(_) => {
                        log::debug!("[reader] rx_done channel closed, exiting draining loop");
                        break;
                    }
                }
            }
            log::debug!("[reader] Drain done ");

            // Drop the sender side of the channel `tx_chunk` to signal that no more data will be sent
            tx_chunk_array.into_iter().for_each(drop);
            log::debug!("[reader] tx_chunk dropped after finishing all chunk sends");

            // Drop other resources after processing is complete
            drop(rx_done); // Drop rx_done after we're done draining inflight chunks
            log::debug!("[reader] rx_done dropped after processing all chunks");

            drop(revolver); // Drop revolver if needed (Rust will also drop it when the thread finishes)

            // Return the statistics to the main thread
            (
                uncompressed_files,
                uncompressed_bytes,
                compressed_files,
                compressed_bytes,
            )
        })
    };

    // Compressor Threads pool
    // chunk_nr is index in the shared chunk array
    // chunk_index is a sequence within the file.

    let mut compressor_threads = Vec::with_capacity(CONFIG.max_core_in_flight as u8 as usize);
    for compressor_group in 0..CONFIG.max_core_in_flight as u8 {
        let rx_chunk = rx_chunk_array[compressor_group as usize].clone();

        let tx_compressed = tx_compressed.clone();
        let tx_ret = tx_return.clone();
        let base_ptr: SendPtr = base_ptrs[compressor_group as usize];

        let chunk_size = chunk_size;

        let handle = thread::spawn(move || {
            let raw_ptr = base_ptr.as_ptr();

            let mut local_chunkmeta: Vec<ChunkMeta> = Vec::new();
            let mut hasher = Hasher::new(); // Blake3 hash initialization
            let mut chunk_seq: u32 = 0;

            let mut cctx = znippy_common::codec::CompressCtx::new(CONFIG.compression_level)
                .expect("Failed to create compression context");

            log::info!(
                "[compressor] Compressor thread started with level {}",
                CONFIG.compression_level,
            );

            unsafe {
                loop {
                    match rx_chunk.recv() {
                        Ok((file_index, mut fdata_offset, ring_nr, chunk_nr, length, skip)) => {
                            log::debug!(
                                "[compressor] Processing chunk {} from file {}: {} bytes",
                                chunk_nr,
                                file_index,
                                length
                            );
                            let input = get_chunk_slice(
                                raw_ptr,
                                chunk_size,
                                chunk_nr as u32,
                                length as usize,
                            );
                            let chunk_meta;
                            let output: Arc<[u8]>;

                            // Hash the uncompressed data before compressing
                            hasher.update(&input);

                            if skip {
                                log::debug!(
                                    "[compressor] Skipping compression for chunk {} of file {}",
                                    chunk_nr,
                                    file_index
                                );
                                output = Arc::from(input);

                                chunk_meta = ChunkMeta {
                                    zdata_offset: 0,
                                    fdata_offset,
                                    file_index,
                                    chunk_seq,
                                    checksum_group: compressor_group,
                                    compressed_size: input.len() as u64,
                                    compressed: false,
                                    uncompressed_size: input.len() as u64,
                                };

                                tx_compressed.send((output, chunk_meta)).unwrap();
                                chunk_seq += 1;
                                fdata_offset += input.len() as u64;
                            } else {
                                let micro_chunks =
                                    split_into_microchunks(input, CONFIG.zstd_output_buffer_size);

                                if micro_chunks.is_empty() {
                                    // Empty file: emit one empty chunk to preserve in archive
                                    let compressed_vec = cctx.compress(&[])?;
                                    let compressed_chunk: Arc<[u8]> =
                                        Arc::from(compressed_vec.into_boxed_slice());
                                    let chunk_meta = ChunkMeta {
                                        zdata_offset: 0,
                                        fdata_offset,
                                        file_index,
                                        chunk_seq,
                                        checksum_group: compressor_group,
                                        compressed_size: compressed_chunk.len() as u64,
                                        compressed: true,
                                        uncompressed_size: 0,
                                    };
                                    tx_compressed.send((compressed_chunk, chunk_meta))?;
                                    chunk_seq += 1;
                                } else {
                                    for (micro_nr, micro) in micro_chunks.iter().enumerate() {
                                        let compressed_vec = cctx.compress(micro)?;

                                        let compressed_chunk: Arc<[u8]> =
                                            Arc::from(compressed_vec.into_boxed_slice());
                                        let chunk_meta = ChunkMeta {
                                            zdata_offset: 0, // to be set by writer
                                            fdata_offset,
                                            file_index,
                                            chunk_seq,
                                            checksum_group: compressor_group,
                                            compressed_size: compressed_chunk.len() as u64,
                                            compressed: true,
                                            uncompressed_size: micro.len() as u64,
                                        };
                                        fdata_offset += micro.len() as u64;
                                        log::debug!(
                                            "[compressor {}] did File_index {} chunk nr {} size {} micro nr {} chunk size {} out {} zdata_offset = {}",
                                            compressor_group,
                                            file_index,
                                            chunk_nr,
                                            length,
                                            micro_nr,
                                            micro.len(),
                                            chunk_meta.compressed_size,
                                            chunk_meta.zdata_offset
                                        );

                                        tx_compressed.send((compressed_chunk, chunk_meta))?;
                                        chunk_seq += 1;
                                    }
                                }
                            }
                            tx_ret.send((ring_nr, chunk_nr));
                        }
                        Err(_) => {
                            // Channel is closed, gracefully exit the loop
                            log::debug!("[compressor] rx_chunk channel closed, compressor exiting");
                            break;
                        }
                    }
                }

                // Drop sender-side channels after the receiver finishes
                drop(tx_compressed);
                drop(tx_ret);

                // Finally, drop the receiver channel
                drop(rx_chunk);

                log::debug!("[compressor] Compressor thread finished processing.");
            }
            log::info!("📦 Compressor thread/group {} returning ", compressor_group);

            Ok((compressor_group, *hasher.finalize().as_bytes()))
        });
        compressor_threads.push(handle);
    }

    // Writer thread — streams batches to Arrow IPC file incrementally
    let output_for_writer = output.clone();
    let writer_thread = thread::spawn(move || -> (WriterStats, FileWriter<std::io::BufWriter<File>>) {
        use znippy_common::index::ZNIPPY_INDEX_SCHEMA;

        const BATCH_FLUSH_BYTES: u64 = 64 * 1024 * 1024; // flush every 64MB of zdata

        let output_path = output_for_writer.with_extension("znippy");
        let file = File::create(&output_path).expect("Failed to create output file");
        let buf_writer = std::io::BufWriter::new(file);
        let schema = ZNIPPY_INDEX_SCHEMA.as_ref().clone();
        let mut arrow_writer = FileWriter::try_new(buf_writer, &schema)
            .expect("Failed to create Arrow writer");

        let mut chunk_buf: Vec<(ChunkMeta, Arc<[u8]>)> = Vec::new();
        let mut buffered_bytes: u64 = 0;
        let mut writerstats = WriterStats {
            total_chunks: 0,
            total_written_bytes: 0,
            verified_files: 0,
            corrupt_files: 0,
            verified_bytes: 0,
            corrupt_bytes: 0,
        };

        let flush = |buf: &mut Vec<(ChunkMeta, Arc<[u8]>)>, writer: &mut FileWriter<std::io::BufWriter<File>>| {
            let batch = build_batch_zero_copy(buf, |file_index| {
                let idx = file_index as usize;
                all_files_for_writer[idx]
                    .strip_prefix(&input_dir_cloned)
                    .unwrap_or(&all_files_for_writer[idx])
                    .to_string_lossy()
                    .to_string()
            }).expect("Failed to build batch");
            writer.write(&batch).expect("Failed to write batch");
            buf.clear();
        };

        while let Ok((compressed_data, chunk_meta)) = rx_compressed.recv() {
            writerstats.total_chunks += 1;
            let data_len = compressed_data.len() as u64;
            writerstats.total_written_bytes += data_len;
            buffered_bytes += data_len;
            chunk_buf.push((chunk_meta, compressed_data));

            if buffered_bytes >= BATCH_FLUSH_BYTES {
                flush(&mut chunk_buf, &mut arrow_writer);
                buffered_bytes = 0;
            }
        }

        // Flush remaining
        if !chunk_buf.is_empty() {
            flush(&mut chunk_buf, &mut arrow_writer);
        }

        log::info!(
            "[writer] Done {} chunks, total {} bytes",
            writerstats.total_chunks,
            writerstats.total_written_bytes
        );

        (writerstats, arrow_writer)
    });

    // Wait for reader thread to finish
    let (uncompressed_files, uncompressed_bytes, compressed_files, compressed_bytes) =
        reader_thread.join().unwrap();
    log::debug!("[reader] reader_thread joined");

    // Drop the sender-side channels after the reader and compressor threads finish
    tx_chunk_array.into_iter().for_each(drop);
    log::debug!("[reader] tx_chunk dropped after reader thread finished");

    let mut checksums: Vec<[u8; 32]> = Vec::with_capacity(CONFIG.max_core_in_compress as usize);

    for handle in compressor_threads {
        let Ok((compressor_group, checksum)): Result<(u8, [u8; 32]), anyhow::Error> =
            handle.join().unwrap()
        else {
            return Err(anyhow!("Compressor thread returned error"));
        };
        checksums.insert(compressor_group as usize, checksum);
    }

    log::info!(
        "📦 Compressor threads returning blake3 checksums from {} compressor threads",
        checksums.len()
    );

    // After compressor threads are done, drop tx_compressed
    drop(tx_compressed);
    log::debug!("[compressor] tx_compressed dropped after compressors finished");
    log::debug!("[writer] Waiting for writer thread to finish");
    let (writerstats, mut arrow_writer) = writer_thread.join().unwrap();

    // Write checksums and config as custom metadata in the IPC footer
    let metadata = build_arrow_metadata_for_checksums_and_config(&checksums, &CONFIG);
    for (key, value) in &metadata {
        arrow_writer.write_metadata(key, value);
    }
    arrow_writer.finish()?;

    log::info!("[main] Single-file archive written: {:?}", output_path);

    let report = CompressionReport {
        total_files,
        compressed_files,
        uncompressed_files,
        chunks: writerstats.total_chunks,
        total_dirs,
        total_bytes_in: compressed_bytes + uncompressed_bytes,
        total_bytes_out: writerstats.total_written_bytes,
        compressed_bytes,
        uncompressed_bytes,
        compression_ratio: if uncompressed_bytes > 0 {
            (compressed_bytes as f32
                / (writerstats.total_written_bytes - uncompressed_bytes) as f32)
                * 100.0
        } else {
            0.0
        },
    };

    Ok(report)
}