use anyhow::{Context, Result};
use arrow::datatypes::SchemaRef;
use arrow_array::{
    Array, BooleanArray, StringArray,
    UInt8Array, UInt32Array, UInt64Array,
};
use std::{
    collections::{HashMap, HashSet},
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
    thread,
};

use crate::{
    ChunkMeta, ChunkRevolver, common_config::CONFIG,
    index::VerifyReport, index::read_znippy_index,
};
use blake3::Hasher;
use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use hex::FromHex;

use crate::chunkrevolver::{Chunk, SendPtr, get_chunk_slice};
use crate::meta::{ReaderStats, WriterStats};

use std::thread::JoinHandle;

pub fn decompress_archive(
    index_path: &Path,
    save_data: bool,
    out_dir: &Path,
) -> Result<VerifyReport> {
    let (schema, batches) = read_znippy_index(index_path)?;
    let file_checksums = extract_file_checksums_from_metadata(&schema);
    let config = &CONFIG;
    log::debug!(
        "read config from meta {:?}\n and checksums {:?}",
        config,
        file_checksums
    );

    let batch = Arc::new(batches[0].clone());
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
    let mut unique_files = std::collections::HashSet::new();
    for i in 0..total_rows {
        unique_files.insert(paths_col.value(i));
    }
    let total_files = unique_files.len();

    let mut revolver = ChunkRevolver::new(&config);
    let base_ptrs = revolver.base_ptrs();
    let chunk_size = revolver.chunk_size();

    let (work_tx_array, work_rx_array): (
        Vec<Sender<(ChunkMeta, u8, u32)>>,
        Vec<Receiver<(ChunkMeta, u8, u32)>>,
    ) = (0..CONFIG.max_core_in_flight)
        .map(|_| bounded(CONFIG.max_chunks as usize))
        .unzip();

    let (tx_return, rx_return): (Sender<(u8, u64)>, Receiver<(u8, u64)>) = unbounded();
    let (chunk_tx, chunk_rx): (Sender<(ChunkMeta, Vec<u8>)>, Receiver<_>) =
        bounded(config.max_core_in_flight);

    let out_dir = Arc::new(out_dir.to_path_buf());
    let out_dir_cloned = Arc::clone(&out_dir);

    // READER — iterate metadata rows, read zdata from Arrow batch
    let index_path_for_reader = index_path.to_path_buf();
    let reader_thread = {
        let done_rx = rx_return.clone();
        let work_tx_array = work_tx_array.clone();

        thread::spawn(move || -> ReaderStats {
            use arrow::array::LargeBinaryArray;

            let mut inflight_chunks = 0usize;

            let fdata_offset_col = batch_for_reader
                .column_by_name("fdata_offset")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let chunk_seq_col = batch_for_reader
                .column_by_name("chunk_seq")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();
            let checksum_group_col = batch_for_reader
                .column_by_name("checksum_group")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap();
            let compressed_col = batch_for_reader
                .column_by_name("compressed")
                .unwrap()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            let uncompressed_size_col = batch_for_reader
                .column_by_name("uncompressed_size")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let zdata_col = batch_for_reader
                .column_by_name("zdata")
                .unwrap()
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();

            for row_idx in 0..total_rows {
                let fdata_offset = fdata_offset_col.value(row_idx);
                let chunk_seq = chunk_seq_col.value(row_idx);
                let checksum_group = checksum_group_col.value(row_idx);
                let compressed = compressed_col.value(row_idx);
                let uncompressed_size = uncompressed_size_col.value(row_idx);
                let zdata = zdata_col.value(row_idx);
                let compressed_size = zdata.len() as u64;

                // Get a ring buffer slot
                let mut chunk_data: Chunk = loop {
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

                // Copy zdata bytes into ring buffer slot
                chunk_data[..zdata.len()].copy_from_slice(zdata);

                let meta = ChunkMeta {
                    fdata_offset,
                    compressed_size,
                    chunk_seq,
                    checksum_group,
                    compressed,
                    file_index: row_idx as u64,
                    uncompressed_size,
                };

                work_tx_array[chunk_data.ring_nr as usize]
                    .send((meta, chunk_data.ring_nr, chunk_data.index as u32))
                    .unwrap();
                inflight_chunks += 1;
            }

            // Drain inflight
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

            ReaderStats {
                total_files,
                skipped_files: 0,
            }
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
                    Ok((chunk_meta, ring_nr, chunk_nr)) => {
                        let data = get_chunk_slice(
                            raw_ptr,
                            chunk_size,
                            chunk_nr,
                            chunk_meta.compressed_size as usize,
                        );

                        let decompress_result = std::panic::catch_unwind(|| {
                            if chunk_meta.compressed {
                                decompress2_microchunk(&data)
                            } else {
                                Ok(data.to_vec())
                            }
                        });

                        match decompress_result {
                            Ok(Ok(decompressed)) => {
                                if let Err(e) = tx.send((chunk_meta, decompressed)) {
                                    log::error!(
                                        "[Decompressor {}] tx.send failed: {}",
                                        decompressor_nr,
                                        e
                                    );
                                }
                            }
                            Ok(Err(e)) => {
                                log::error!(
                                    "Decompression failed: row {} chunk_seq={} error={}",
                                    chunk_meta.file_index,
                                    chunk_meta.chunk_seq,
                                    e
                                );
                            }
                            Err(_) => {
                                log::error!(
                                    "PANIC: decompress panicked! row {} chunk_seq={}",
                                    chunk_meta.file_index,
                                    chunk_meta.chunk_seq,
                                );
                            }
                        }

                        if let Err(e) = done_tx.send((decompressor_nr, chunk_nr as u64)) {
                            log::warn!(
                                "[Decompressor {}] done_tx failed: {}",
                                decompressor_nr,
                                e
                            );
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

    // [VERIFY THREADS]
    let expected_checksums = file_checksums.unwrap_or_default();
    let num_groups = expected_checksums.len();

    let (verify_txs, verify_threads): (Vec<_>, Vec<_>) = (0..num_groups)
        .map(|grp_idx| {
            let (vtx, vrx): (Sender<(u32, Vec<u8>)>, Receiver<(u32, Vec<u8>)>) = bounded(64);
            let expected = expected_checksums[grp_idx];
            let handle = thread::spawn(move || -> (bool, u64) {
                let mut hasher = Hasher::new();
                let mut next_seq: u32 = 0;
                let mut pending: std::collections::BTreeMap<u32, Vec<u8>> =
                    std::collections::BTreeMap::new();
                let mut total_bytes: u64 = 0;

                while let Ok((seq, data)) = vrx.recv() {
                    if seq == next_seq {
                        hasher.update(&data);
                        total_bytes += data.len() as u64;
                        next_seq += 1;
                        while let Some(buffered) = pending.remove(&next_seq) {
                            hasher.update(&buffered);
                            total_bytes += buffered.len() as u64;
                            next_seq += 1;
                        }
                    } else {
                        pending.insert(seq, data);
                    }
                }
                while let Some((&seq, _)) = pending.iter().next() {
                    if seq == next_seq {
                        let buffered = pending.remove(&seq).unwrap();
                        hasher.update(&buffered);
                        total_bytes += buffered.len() as u64;
                        next_seq += 1;
                    } else {
                        break;
                    }
                }

                let computed = *hasher.finalize().as_bytes();
                let ok = computed == expected;
                if !ok {
                    log::error!(
                        "[verify] checksum_group {} MISMATCH: expected {}, got {}",
                        grp_idx,
                        hex::encode(expected),
                        hex::encode(computed)
                    );
                }
                (ok, total_bytes)
            });
            (vtx, handle)
        })
        .unzip();

    // WRITER thread
    let writer_thread = thread::spawn(move || -> WriterStats {
        let mut total_chunks = 0u64;
        let mut total_written_bytes = 0u64;

        let paths_col = batch_for_writer
            .column_by_name("relative_path")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let mut open_files: HashMap<String, File> = HashMap::new();
        let mut created_dirs: HashSet<PathBuf> = HashSet::new();

        while let Ok((chunk_meta, data)) = chunk_rx.recv() {
            total_chunks += 1;
            total_written_bytes += data.len() as u64;

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
                file.write_all(&data).unwrap();
            }

            // Send to verify thread
            let grp = chunk_meta.checksum_group as usize;
            if grp < num_groups {
                let _ = verify_txs[grp].send((chunk_meta.chunk_seq, data));
            }
        }

        drop(open_files);
        drop(verify_txs);

        // Collect verify results
        let mut verified_files = 0usize;
        let mut corrupt_files = 0usize;
        let mut verified_bytes = 0u64;
        let mut corrupt_bytes = 0u64;

        for handle in verify_threads {
            let (ok, bytes) = handle.join().expect("verify thread panicked");
            if ok {
                verified_bytes += bytes;
            } else {
                corrupt_bytes += bytes;
                corrupt_files += 1;
            }
        }

        if corrupt_files == 0 && num_groups > 0 {
            verified_files = total_rows;
        }

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
        handle.join().expect("decompressor panicked");
    }
    drop(chunk_tx);
    let writer_stats = writer_thread.join().expect("writer_thread panicked");

    let report = VerifyReport {
        total_files: reader_stats.total_files,
        verified_files: writer_stats.verified_files,
        corrupt_files: writer_stats.corrupt_files,
        total_bytes: writer_stats.total_written_bytes,
        verified_bytes: writer_stats.verified_bytes,
        corrupt_bytes: writer_stats.corrupt_bytes,
        chunks: writer_stats.total_chunks,
    };

    Ok(report)
}

pub fn extract_file_checksums_from_metadata(schema: &SchemaRef) -> Result<Vec<[u8; 32]>> {
    let metadata: &HashMap<String, String> = schema.metadata();
    let mut checksums: Vec<[u8; 32]> = Vec::new();

    let mut sorted_keys: Vec<_> = metadata
        .keys()
        .filter(|k| k.starts_with("checksum_group_"))
        .collect();

    sorted_keys.sort_by_key(|k| {
        k.trim_start_matches("checksum_group_")
            .parse::<usize>()
            .unwrap_or(usize::MAX)
    });

    for key in sorted_keys {
        let hexstr = metadata.get(key).context("Missing metadata entry")?;
        let bytes = <[u8; 32]>::from_hex(hexstr)
            .with_context(|| format!("Invalid hex in key {}: {}", key, hexstr))?;
        checksums.push(bytes);
    }

    Ok(checksums)
}

pub fn decompress2_microchunk(input: &[u8]) -> Result<Vec<u8>> {
    crate::codec::decompress_frame(input)
}
