use anyhow::Result;
use arrow_array::{
    BooleanArray, FixedSizeBinaryArray, StringArray, UInt32Array, UInt64Array,
};
use std::{
    collections::{HashMap, HashSet},
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
    thread,
};

use crate::{
    ChunkMeta, ChunkRevolver, common_config::CONFIG,
    index::{VerifyReport, read_znippy_index},
};

use crate::chunkrevolver::{SendPtr, get_chunk_slice};
use crate::meta::{ReaderStats, WriterStats};

/// Mirrors `Blob` on the compress side.
///
/// `Owned` — decompressed output (already a fresh allocation; no further copy needed).
/// `Revolver` — uncompressed (skip-path) chunk: raw pointer into the ring buffer slot.
/// The writer uses the slice directly for `write_all` (zero copies), then makes one
/// copy for the verify thread, then returns the slot.
enum DecompBlob {
    Owned(Vec<u8>),
    Revolver { ptr: usize, len: usize, ring_nr: u8, chunk_nr: u32 },
}

// Safety: raw pointer into ChunkRevolver's pre-allocated region, kept alive by the
// protocol: slot is returned only after write_all + verify copy in the writer thread.
unsafe impl Send for DecompBlob {}

impl DecompBlob {
    fn as_slice(&self) -> &[u8] {
        match self {
            DecompBlob::Owned(v) => v,
            DecompBlob::Revolver { ptr, len, .. } => unsafe {
                std::slice::from_raw_parts(*ptr as *const u8, *len)
            },
        }
    }
    fn len(&self) -> usize {
        match self {
            DecompBlob::Owned(v) => v.len(),
            DecompBlob::Revolver { len, .. } => *len,
        }
    }
}

use std::thread::JoinHandle;

pub fn decompress_archive(
    index_path: &Path,
    save_data: bool,
    out_dir: &Path,
) -> Result<VerifyReport> {
    let (schema, batches) = read_znippy_index(index_path)?;
    let config = &CONFIG;

    // For v0.7, read_znippy_index already concatenates sub-indexes into a single batch.
    // We still handle the rare case of multiple batches (e.g. large v0.6 archives written
    // with multiple IPC record batches) by merging them here.
    let batch = Arc::new(match batches.len() {
        0 => arrow::record_batch::RecordBatch::new_empty(Arc::new(
            crate::index::ZNIPPY_INDEX_SCHEMA.as_ref().clone(),
        )),
        1 => batches.into_iter().next().unwrap(),
        _ => arrow_select::concat::concat_batches(&schema, batches.iter())
            .map_err(|e| anyhow::anyhow!("failed to merge index batches: {}", e))?,
    });
    let batch_for_writer = Arc::clone(&batch);
    let batch_for_reader = Arc::clone(&batch);

    let total_rows = batch.num_rows();

    // Count unique files
    let paths_col = batch
        .column_by_name("relative_path")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut unique_files = HashSet::new();
    for i in 0..total_rows {
        unique_files.insert(paths_col.value(i));
    }
    let total_files = unique_files.len();

    let mut revolver = ChunkRevolver::new(config);
    let base_ptrs = revolver.base_ptrs();
    let chunk_size = revolver.chunk_size();

    let (work_tx_array, work_rx_array): (
        Vec<crossbeam_channel::Sender<(ChunkMeta, u8, u32)>>,
        Vec<crossbeam_channel::Receiver<(ChunkMeta, u8, u32)>>,
    ) = (0..CONFIG.max_core_in_flight)
        .map(|_| crossbeam_channel::bounded(CONFIG.max_chunks as usize))
        .unzip();

    let (tx_return, rx_return): (
        crossbeam_channel::Sender<(u8, u64)>,
        crossbeam_channel::Receiver<(u8, u64)>,
    ) = crossbeam_channel::unbounded();
    let (chunk_tx, chunk_rx): (
        crossbeam_channel::Sender<(ChunkMeta, DecompBlob)>,
        crossbeam_channel::Receiver<_>,
    ) = crossbeam_channel::bounded(config.max_chunks as usize);

    let out_dir = Arc::new(out_dir.to_path_buf());
    let out_dir_cloned = Arc::clone(&out_dir);

    // READER — reads blob_offset/blob_size from Arrow, seeks in archive file for each chunk
    let archive_path = index_path.to_path_buf();
    let reader_thread = {
        let done_rx = rx_return.clone();
        let work_tx_array = work_tx_array.clone();

        thread::spawn(move || -> ReaderStats {
            let mut blob_file = File::open(&archive_path)
                .expect("Failed to open archive for blob reading");

            let mut inflight_chunks = 0usize;

            let fdata_offset_col = batch_for_reader
                .column_by_name("fdata_offset").unwrap()
                .as_any().downcast_ref::<UInt64Array>().unwrap();
            let chunk_seq_col = batch_for_reader
                .column_by_name("chunk_seq").unwrap()
                .as_any().downcast_ref::<UInt32Array>().unwrap();
            let checksum_col = batch_for_reader
                .column_by_name("checksum").unwrap()
                .as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
            let compressed_col = batch_for_reader
                .column_by_name("compressed").unwrap()
                .as_any().downcast_ref::<BooleanArray>().unwrap();
            let uncompressed_size_col = batch_for_reader
                .column_by_name("uncompressed_size").unwrap()
                .as_any().downcast_ref::<UInt64Array>().unwrap();
            let blob_offset_col = batch_for_reader
                .column_by_name("blob_offset").unwrap()
                .as_any().downcast_ref::<UInt64Array>().unwrap();
            let blob_size_col = batch_for_reader
                .column_by_name("blob_size").unwrap()
                .as_any().downcast_ref::<UInt64Array>().unwrap();

            for row_idx in 0..total_rows {
                let fdata_offset = fdata_offset_col.value(row_idx);
                let chunk_seq = chunk_seq_col.value(row_idx);
                let mut checksum = [0u8; 32];
                checksum.copy_from_slice(checksum_col.value(row_idx));
                let compressed = compressed_col.value(row_idx);
                let uncompressed_size = uncompressed_size_col.value(row_idx);
                let blob_offset = blob_offset_col.value(row_idx);
                let blob_size = blob_size_col.value(row_idx) as usize;

                // Get a ring buffer slot
                let mut chunk_data = loop {
                    match revolver.try_get_chunk() {
                        Some(c) => break c,
                        None => {
                            let (thread_nr, returned) = done_rx
                                .recv()
                                .expect("rx_done channel closed unexpectedly");
                            revolver.return_chunk(thread_nr, returned);
                            inflight_chunks = inflight_chunks
                                .checked_sub(1)
                                .expect("inflight_chunks underflow");
                        }
                    }
                };

                // Read blob from archive file into ring buffer slot
                blob_file
                    .seek(SeekFrom::Start(blob_offset))
                    .expect("Failed to seek to blob");
                blob_file
                    .read_exact(&mut chunk_data[..blob_size])
                    .expect("Failed to read blob");

                let meta = ChunkMeta {
                    fdata_offset,
                    compressed_size: blob_size as u64,
                    chunk_seq,
                    checksum,
                    compressed,
                    file_index: row_idx as u64,
                    uncompressed_size,
                };

                work_tx_array[chunk_data.ring_nr as usize]
                    .send((meta, chunk_data.ring_nr, chunk_data.index as u32))
                    .unwrap();
                inflight_chunks += 1;
            }

            while inflight_chunks > 0 {
                match done_rx.recv() {
                    Ok((thread_nr, returned)) => {
                        revolver.return_chunk(thread_nr, returned);
                        inflight_chunks -= 1;
                    }
                    Err(_) => break,
                }
            }

            work_tx_array.into_iter().for_each(drop);
            drop(done_rx);
            drop(revolver);

            ReaderStats { total_files, skipped_files: 0 }
        })
    };

    // DECOMPRESSOR threads
    let mut decompressor_threads: Vec<JoinHandle<Result<()>>> =
        Vec::with_capacity(config.max_core_in_flight as usize);

    for decompressor_nr in 0..config.max_core_in_flight as u8 {
        let base_ptr: SendPtr = base_ptrs[decompressor_nr as usize];
        let rx = work_rx_array[decompressor_nr as usize].clone();
        let tx = chunk_tx.clone();
        let done_tx = tx_return.clone();
        let handle = thread::spawn(move || unsafe {
            let raw_ptr = base_ptr.as_ptr();

            loop {
                match rx.recv() {
                    Ok((chunk_meta, _ring_nr, chunk_nr)) => {
                        let data = get_chunk_slice(
                            raw_ptr,
                            chunk_size,
                            chunk_nr,
                            chunk_meta.compressed_size as usize,
                        );

                        if chunk_meta.compressed {
                            let result = std::panic::catch_unwind(|| decompress_microchunk(data));
                            match result {
                                Ok(Ok(decompressed)) => {
                                    tx.send((chunk_meta, DecompBlob::Owned(decompressed))).ok();
                                }
                                Ok(Err(e)) => {
                                    log::error!("Decompression failed: row {} chunk_seq={} error={}", chunk_meta.file_index, chunk_meta.chunk_seq, e);
                                }
                                Err(_) => {
                                    log::error!("PANIC: decompress panicked! row {} chunk_seq={}", chunk_meta.file_index, chunk_meta.chunk_seq);
                                }
                            }
                            // Ring slot returned immediately — compressed output lives in the Owned Vec.
                            done_tx.send((decompressor_nr, chunk_nr as u64)).ok();
                        } else {
                            // Zero-copy skip path: pass raw pointer into ring slot.
                            // done_tx is NOT sent here — writer owns the slot through write +
                            // verify copy, then returns it. Extra slot per thread (chunks_per_thread
                            // = max_chunks/threads + 1) ensures the reader is never starved.
                            tx.send((chunk_meta, DecompBlob::Revolver {
                                ptr: data.as_ptr() as usize,
                                len: data.len(),
                                ring_nr: decompressor_nr,
                                chunk_nr,
                            })).ok();
                        }
                    }
                    Err(_) => break,
                }
            }

            drop(tx);
            drop(done_tx);
            drop(rx);
            Ok(())
        });
        decompressor_threads.push(handle);
    }

    // WRITER thread — verifies each chunk's BLAKE3 inline (per-slice), writes
    // output, and releases Revolver ring slots after write_all. No group ordering.
    let tx_return_writer = tx_return.clone();
    let writer_thread = thread::spawn(move || -> WriterStats {
        let mut total_chunks = 0u64;
        let mut total_written_bytes = 0u64;
        let mut verified_bytes = 0u64;
        let mut corrupt_bytes = 0u64;
        let mut corrupt_file_idx: HashSet<u64> = HashSet::new();

        let paths_col = batch_for_writer
            .column_by_name("relative_path")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let mut open_files: HashMap<String, File> = HashMap::new();
        let mut created_dirs: HashSet<PathBuf> = HashSet::new();

        while let Ok((chunk_meta, blob)) = chunk_rx.recv() {
            total_chunks += 1;
            let bytes = blob.as_slice();
            let len = bytes.len() as u64;
            total_written_bytes += len;

            // Per-slice verify: hash the uncompressed bytes, compare to the row's checksum.
            let computed = blake3::hash(bytes);
            if computed.as_bytes() == &chunk_meta.checksum {
                verified_bytes += len;
            } else {
                corrupt_bytes += len;
                corrupt_file_idx.insert(chunk_meta.file_index);
                log::error!(
                    "[verify] chunk MISMATCH: row {} chunk_seq={} expected {} got {}",
                    chunk_meta.file_index,
                    chunk_meta.chunk_seq,
                    hex::encode(chunk_meta.checksum),
                    hex::encode(computed.as_bytes()),
                );
            }

            if save_data {
                let rel_path = paths_col.value(chunk_meta.file_index as usize);
                let full_path = out_dir_cloned.join(rel_path);

                if let Some(parent) = full_path.parent() {
                    if created_dirs.insert(parent.to_path_buf()) {
                        let _ = std::fs::create_dir_all(parent);
                    }
                }

                let file = open_files
                    .entry(rel_path.to_string())
                    .or_insert_with(|| {
                        OpenOptions::new()
                            .create(true)
                            .write(true)
                            .truncate(true)
                            .open(&full_path)
                            .expect("Failed to open file for writing")
                    });

                file.seek(SeekFrom::Start(chunk_meta.fdata_offset)).unwrap();
                // Zero-copy: write directly from the ring slot (skip path) or decompressed Vec.
                file.write_all(bytes).unwrap();
            }

            // Return the ring slot for the skip (Revolver) path after we're done reading it.
            if let DecompBlob::Revolver { ring_nr, chunk_nr, .. } = blob {
                tx_return_writer.send((ring_nr, chunk_nr as u64)).ok();
            }
        }

        drop(open_files);

        let corrupt_files = corrupt_file_idx.len();
        let verified_files = total_files.saturating_sub(corrupt_files);

        WriterStats {
            total_chunks,
            total_written_bytes,
            verified_files,
            corrupt_files,
            verified_bytes,
            corrupt_bytes,
        }
    });

    let reader_stats = reader_thread.join().expect("reader_thread panicked");
    work_tx_array.into_iter().for_each(drop);
    drop(tx_return);

    for handle in decompressor_threads {
        let _ = handle.join().expect("decompressor panicked");
    }
    drop(chunk_tx);
    let writer_stats = writer_thread.join().expect("writer_thread panicked");

    Ok(VerifyReport {
        total_files: reader_stats.total_files,
        verified_files: writer_stats.verified_files,
        corrupt_files: writer_stats.corrupt_files,
        total_bytes: writer_stats.total_written_bytes,
        verified_bytes: writer_stats.verified_bytes,
        corrupt_bytes: writer_stats.corrupt_bytes,
        chunks: writer_stats.total_chunks,
    })
}

pub fn decompress_microchunk(input: &[u8]) -> Result<Vec<u8>> {
    crate::codec::decompress_frame(input)
}
