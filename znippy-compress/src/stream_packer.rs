use anyhow::{Result, anyhow};
use arrow::ipc::writer::FileWriter;
use blake3::Hasher;
use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use znippy_common::chunkrevolver::{ChunkRevolver, SendPtr, get_chunk_slice};
use znippy_common::common_config::CONFIG;
use znippy_common::index::{
    build_arrow_metadata_for_checksums_and_config, should_skip_compression,
};
use znippy_common::meta::{ChunkMeta, WriterStats};
use znippy_common::{
    CompressionReport, FileMeta, attach_metadata, build_arrow_batch_from_files,
    split_into_microchunks,
};
use zstd_sys_rs::ZSTD_cParameter::{ZSTD_c_compressionLevel, ZSTD_c_nbWorkers};
use zstd_sys_rs::*;

use crate::packer::compress2_microchunk;

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
/// then call `handle.finish()` to finalize the .znippy + .zdata archive.
///
/// # Arguments
/// * `output` - Base path for output files (produces output.znippy + output.zdata)
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

    let output_zdata_path = output.with_extension("zdata");
    log::debug!("Creating zdata file at: {:?}", output_zdata_path);
    let zdata_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&output_zdata_path)?;
    let mut writer =
        BufWriter::with_capacity((CONFIG.file_split_block_size / 2) as usize, zdata_file);

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

            unsafe {
                let cctx = ZSTD_createCCtx();
                assert!(!cctx.is_null(), "ZSTD_createCCtx failed");

                ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, CONFIG.compression_level);
                ZSTD_CCtx_setParameter(cctx, ZSTD_c_nbWorkers, CONFIG.max_core_in_compress as i32);

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
                            } else {
                                let micro_chunks =
                                    split_into_microchunks(input, CONFIG.zstd_output_buffer_size);

                                for micro in micro_chunks.iter() {
                                    let compressed_vec = compress2_microchunk(cctx, micro)?;
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
                                        uncompressed_size: micro.len() as u64,
                                    };
                                    fdata_offset += micro.len() as u64;
                                    tx_compressed.send((compressed_chunk, chunk_meta))?;
                                    chunk_seq += 1;
                                }
                            }
                            tx_ret.send((ring_nr, chunk_nr));
                        }
                        Err(_) => break,
                    }
                }

                ZSTD_freeCCtx(cctx);
                drop(tx_compressed);
                drop(tx_ret);
                drop(rx_chunk);
            }

            Ok((compressor_group, *hasher.finalize().as_bytes()))
        });
        compressor_threads.push(handle);
    }

    // Writer thread (identical to compress_dir)
    let output_for_writer = output.clone();
    let writer_thread = thread::spawn(move || {
        let mut file_metadata: Vec<FileMeta> = relative_paths_for_writer
            .iter()
            .map(|path| FileMeta {
                relative_path: path.clone(),
                compressed: false,
                uncompressed_size: 0,
                chunks: Vec::new(),
            })
            .collect();

        let mut writerstats = WriterStats {
            total_chunks: 0,
            total_written_bytes: 0,
        };
        let mut zdata_offset: u64 = 0;

        while let Ok((compressed_data, mut chunk_meta)) = rx_compressed.recv() {
            let idx = chunk_meta.file_index as usize;

            if idx >= file_metadata.len() {
                log::error!(
                    "[writer] Invalid file_index {}: file_metadata.len() = {}",
                    idx,
                    file_metadata.len()
                );
                continue;
            }

            let file = &mut file_metadata[idx];
            file.compressed = chunk_meta.compressed;
            file.uncompressed_size += chunk_meta.uncompressed_size;
            chunk_meta.zdata_offset = zdata_offset;

            if let Err(e) = writer.write_all(&compressed_data) {
                log::error!("[writer] Write error: {}", e);
                continue;
            }

            file.chunks.push(chunk_meta);
            zdata_offset += compressed_data.len() as u64;
            writerstats.total_chunks += 1;
            writerstats.total_written_bytes += compressed_data.len() as u64;
        }

        log::info!(
            "[writer] Done {} chunks, total written {} bytes",
            writerstats.total_chunks,
            writerstats.total_written_bytes
        );

        // Build arrow batch using relative paths directly
        let batch = build_arrow_batch_from_files(&file_metadata, std::path::Path::new(""));
        (writerstats, batch)
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

    drop(tx_compressed);

    let (writerstats, batch) = writer_thread.join().unwrap();

    // Build and write Arrow index
    let metadata = build_arrow_metadata_for_checksums_and_config(&checksums, &CONFIG);
    let final_batch = attach_metadata(batch?, metadata)?;

    let index_path = output.with_extension("znippy");
    let index_file = File::create(&index_path)?;
    let mut arrow_writer = FileWriter::try_new(index_file, &final_batch.schema())?;
    arrow_writer.write(&final_batch)?;
    arrow_writer.finish()?;

    log::info!("[stream] Arrow index written to {:?}", index_path);

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
