use anyhow::{Result, anyhow};
use blake3::Hasher;
use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use znippy_common::chunkrevolver::{ChunkRevolver, SendPtr, get_chunk_slice};
use znippy_common::common_config::CONFIG;
use znippy_common::index::{
    build_arrow_metadata_for_checksums_and_config, should_skip_compression,
    build_metadata_batch,
};
use znippy_common::meta::{ChunkMeta, WriterStats};
use znippy_common::{
    CompressionReport,
    split_into_microchunks,
};

/// An entry to be compressed into the archive.
/// Contains the relative path within the archive and the raw file bytes.
pub struct ArchiveEntry {
    pub relative_path: String,
    pub data: Vec<u8>,
}

/// A handle to the streaming compressor.
/// Send `ArchiveEntry` items through the sender, then call `finish()` to finalize the archive.
pub struct StreamCompressor {
    tx: Option<Sender<ArchiveEntry>>,
    join_handle: Option<thread::JoinHandle<Result<CompressionReport>>>,
}

impl StreamCompressor {
    /// Returns the sender channel for feeding entries into the compressor.
    pub fn sender(&self) -> &Sender<ArchiveEntry> {
        self.tx.as_ref().expect("sender already consumed")
    }

    /// Signal that no more entries will be sent and wait for compression to complete.
    /// Returns the compression report.
    pub fn finish(mut self) -> Result<CompressionReport> {
        // Drop sender to signal EOF to the pipeline
        drop(self.tx.take());
        self.join_handle
            .take()
            .expect("already finished")
            .join()
            .map_err(|e| anyhow!("Compression thread panicked: {:?}", e))?
    }
}

/// Start a streaming compression pipeline.
///
/// Returns a `StreamCompressor` handle. Feed `ArchiveEntry` items via `handle.sender().send(entry)`,
/// then call `handle.finish()` to finalize the single-file .znippy archive.
///
/// # Arguments
/// * `output` - Base path for output file (produces output.znippy)
/// * `no_skip` - If true, compress all files regardless of extension
pub fn compress_stream(output: &PathBuf, no_skip: bool) -> Result<StreamCompressor> {
    let (tx_entry, rx_entry): (Sender<ArchiveEntry>, Receiver<ArchiveEntry>) = unbounded();
    let output = output.clone();

    let join_handle = thread::spawn(move || -> Result<CompressionReport> {
        run_compression_pipeline(rx_entry, &output, no_skip)
    });

    Ok(StreamCompressor {
        tx: Some(tx_entry),
        join_handle: Some(join_handle),
    })
}

fn run_compression_pipeline(
    rx_entry: Receiver<ArchiveEntry>,
    output: &PathBuf,
    no_skip: bool,
) -> Result<CompressionReport> {
    // Collect all entries first (we need the full list for the writer thread)
    let entries: Vec<ArchiveEntry> = rx_entry.into_iter().collect();
    let total_files = entries.len() as u64;

    if entries.is_empty() {
        return Ok(CompressionReport {
            total_files: 0,
            compressed_files: 0,
            uncompressed_files: 0,
            total_dirs: 0,
            total_bytes_in: 0,
            total_bytes_out: 0,
            compressed_bytes: 0,
            uncompressed_bytes: 0,
            compression_ratio: 0.0,
            chunks: 0,
        });
    }

    let mut files_to_skip = 0u64;
    let mut files_to_compress = 0u64;

    // Determine skip status per entry
    let skip_flags: Vec<bool> = entries
        .iter()
        .map(|e| {
            let skip = !no_skip && should_skip_compression(std::path::Path::new(&e.relative_path));
            if skip {
                files_to_skip += 1;
            } else {
                files_to_compress += 1;
            }
            skip
        })
        .collect();

    log::debug!(
        "Stream compressor: {} entries, {} to compress, {} to skip",
        entries.len(),
        files_to_compress,
        files_to_skip
    );

    // Set up channels (same as compress_dir)
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

    let mut revolver = ChunkRevolver::new(&CONFIG);
    let base_ptrs = revolver.base_ptrs();
    let chunk_size = revolver.chunk_size();

    // Shared data for writer thread
    let relative_paths: Arc<Vec<String>> = Arc::new(
        entries.iter().map(|e| e.relative_path.clone()).collect(),
    );
    let relative_paths_for_writer = Arc::clone(&relative_paths);

    // Reader thread: reads from in-memory entries instead of disk
    let reader_thread = {
        let tx_chunk_array = tx_chunk_array.clone();
        let rx_done = rx_return.clone();

        thread::spawn(move || {
            let mut inflight_chunks = 0usize;
            let mut uncompressed_files: u64 = 0;
            let mut uncompressed_bytes: u64 = 0;
            let mut compressed_files: u64 = 0;
            let mut compressed_bytes: u64 = 0;

            for (file_index, entry) in entries.into_iter().enumerate() {
                let skip = skip_flags[file_index];
                let data = entry.data;
                let data_len = data.len() as u64;

                if skip {
                    uncompressed_files += 1;
                    uncompressed_bytes += data_len;
                } else {
                    compressed_files += 1;
                    compressed_bytes += data_len;
                }

                let mut cursor = std::io::Cursor::new(data);
                let mut has_read_any_data = false;
                let mut fdata_offset: u64 = 0;

                loop {
                    let maybe_chunk = revolver.try_get_chunk();
                    match maybe_chunk {
                        Some(mut chunk) => {
                            let ring_nr = chunk.ring_nr as usize;
                            match cursor.read(&mut *chunk) {
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
                                        drop(chunk);
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
                                        "[stream-reader] Error reading entry {}: {}",
                                        file_index,
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

            // Drain inflight chunks
            while inflight_chunks > 0 {
                match rx_done.recv() {
                    Ok((ring_nr, returned)) => {
                        revolver.return_chunk(ring_nr, returned);
                        inflight_chunks -= 1;
                    }
                    Err(_) => break,
                }
            }

            tx_chunk_array.into_iter().for_each(drop);
            drop(rx_done);
            drop(revolver);

            (
                uncompressed_files,
                uncompressed_bytes,
                compressed_files,
                compressed_bytes,
            )
        })
    };

    // Compressor threads (identical to compress_dir)
    let mut compressor_threads = Vec::with_capacity(CONFIG.max_core_in_flight as u8 as usize);
    for compressor_group in 0..CONFIG.max_core_in_flight as u8 {
        let rx_chunk = rx_chunk_array[compressor_group as usize].clone();
        let tx_compressed = tx_compressed.clone();
        let tx_ret = tx_return.clone();
        let base_ptr: SendPtr = base_ptrs[compressor_group as usize];
        let chunk_size = chunk_size;

        let handle = thread::spawn(move || {
            let raw_ptr = base_ptr.as_ptr();
            let mut hasher = Hasher::new();
            let mut chunk_seq: u32 = 0;

            let mut cctx = znippy_common::codec::CompressCtx::new(CONFIG.compression_level)
                .expect("Failed to create compression context");

            unsafe {
                loop {
                    match rx_chunk.recv() {
                        Ok((file_index, mut fdata_offset, ring_nr, chunk_nr, length, skip)) => {
                            let input = get_chunk_slice(
                                raw_ptr,
                                chunk_size,
                                chunk_nr as u32,
                                length as usize,
                            );

                            hasher.update(input);

                            if skip {
                                let output: Arc<[u8]> = Arc::from(input);
                                let chunk_meta = ChunkMeta {
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
                            } else {
                                let micro_chunks =
                                    split_into_microchunks(input, CONFIG.zstd_output_buffer_size);

                                if micro_chunks.is_empty() {
                                    let compressed_vec = cctx.compress(&[])?;
                                    let compressed_chunk: Arc<[u8]> =
                                        Arc::from(compressed_vec.into_boxed_slice());
                                    let chunk_meta = ChunkMeta {
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
                                    for micro in micro_chunks.iter() {
                                        let compressed_vec = cctx.compress(micro)?;
                                        let compressed_chunk: Arc<[u8]> =
                                            Arc::from(compressed_vec.into_boxed_slice());
                                        let chunk_meta = ChunkMeta {
                                            fdata_offset,
                                            file_index,
                                            chunk_seq,
                                            checksum_group: compressor_group,
                                            compressed_size: compressed_chunk.len() as u64,
                                            compressed: true,
                                            uncompressed_size: micro.len() as u64,
                                        };
                                        fdata_offset += micro.len() as u64;
                                        tx_compressed.send((compressed_chunk, chunk_meta))?;
                                        chunk_seq += 1;
                                    }
                                }
                            }
                            tx_ret.send((ring_nr, chunk_nr));
                        }
                        Err(_) => break,
                    }
                }

                drop(tx_compressed);
                drop(tx_ret);
                drop(rx_chunk);
            }

            Ok((compressor_group, *hasher.finalize().as_bytes()))
        });
        compressor_threads.push(handle);
    }

    // Channel for checksums: sent after compressor threads finish
    let (checksum_tx, checksum_rx) = bounded::<Vec<[u8; 32]>>(1);

    // Writer thread — Arrow IPC Stream format (valid Arrow file, DuckDB-readable)
    let output_for_writer = output.clone();
    let writer_thread = thread::spawn(move || -> WriterStats {
        use std::io::Write;
        use znippy_common::index::{ZNIPPY_INDEX_SCHEMA};
        use znippy_common::index::build_metadata_batch;

        let output_path = output_for_writer.with_extension("znippy");
        let file = File::create(&output_path).expect("Failed to create output file");
        let mut writer = std::io::BufWriter::new(file);

        let mut all_meta: Vec<(ChunkMeta, Arc<[u8]>)> = Vec::new();
        let mut writerstats = WriterStats {
            total_chunks: 0,
            total_written_bytes: 0,
            verified_files: 0,
            corrupt_files: 0,
            verified_bytes: 0,
            corrupt_bytes: 0,
        };

        while let Ok((compressed_data, chunk_meta)) = rx_compressed.recv() {
            writerstats.total_chunks += 1;
            let data_len = compressed_data.len() as u64;
            writerstats.total_written_bytes += data_len;

            all_meta.push((chunk_meta, compressed_data));
        }

        // Wait for checksums from main thread
        let checksums = checksum_rx.recv().expect("Failed to receive checksums");

        let meta_map = build_arrow_metadata_for_checksums_and_config(&checksums, &CONFIG);
        let schema_with_meta = arrow::datatypes::Schema::new_with_metadata(
            ZNIPPY_INDEX_SCHEMA.fields().to_vec(),
            meta_map,
        );

        let paths = &relative_paths_for_writer;
        let batch = build_metadata_batch(&all_meta, |file_index| {
            paths[file_index as usize].clone()
        }).expect("Failed to build metadata batch");

        use arrow::ipc::writer::StreamWriter;
        let mut stream_writer = StreamWriter::try_new(&mut writer, &schema_with_meta)
            .expect("Failed to create Arrow stream writer");
        stream_writer.write(&batch).expect("Failed to write batch");
        stream_writer.finish().expect("Failed to finish Arrow stream");
        writer.flush().expect("Failed to flush");

        log::info!(
            "[writer] Done {} chunks, total {} bytes",
            writerstats.total_chunks,
            writerstats.total_written_bytes,
        );

        writerstats
    });

    // Wait for reader
    let (uncompressed_files, uncompressed_bytes, compressed_files, compressed_bytes) =
        reader_thread.join().unwrap();

    tx_chunk_array.into_iter().for_each(drop);

    // Collect checksums from compressor threads
    let mut checksums: Vec<[u8; 32]> = Vec::with_capacity(CONFIG.max_core_in_compress as usize);
    for handle in compressor_threads {
        let Ok((compressor_group, checksum)): Result<(u8, [u8; 32]), anyhow::Error> =
            handle.join().unwrap()
        else {
            return Err(anyhow!("Compressor thread returned error"));
        };
        checksums.insert(compressor_group as usize, checksum);
    }

    // Signal writer: no more chunks, here are the checksums
    drop(tx_compressed);
    checksum_tx.send(checksums).expect("Failed to send checksums to writer");

    let writerstats = writer_thread.join().unwrap();

    log::info!("[stream] Arrow IPC archive written");

    let report = CompressionReport {
        total_files,
        compressed_files,
        uncompressed_files,
        chunks: writerstats.total_chunks,
        total_dirs: 0,
        total_bytes_in: compressed_bytes + uncompressed_bytes,
        total_bytes_out: writerstats.total_written_bytes,
        compressed_bytes,
        uncompressed_bytes,
        compression_ratio: if compressed_bytes > 0 {
            (compressed_bytes as f32
                / (writerstats.total_written_bytes - uncompressed_bytes) as f32)
                * 100.0
        } else {
            0.0
        },
    };

    Ok(report)
}
