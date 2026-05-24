use anyhow::Result;
use arrow_array::{BooleanArray, FixedSizeBinaryArray, StringArray, UInt32Array, UInt64Array};
use crossbeam_channel::bounded;
use std::{
    collections::{HashMap, HashSet},
    fs::{File, OpenOptions},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::Arc,
    thread,
};

use crate::{
    common_config::CONFIG,
    index::{VerifyReport, read_znippy_index},
};
use crate::slotpool::{Clip, Magazine};

/// Slot buffer size — reader stays ahead of all cores.
const SLOT_SIZE: usize = 200 * 1024 * 1024;
/// Read-ahead depth.
const NUM_SLOTS: usize = 8;

/// One blob to decompress + verify + write. Borrows bytes from a Magazine slot
/// (valid until `ejector.release_one(slot_id)` is called by the worker).
struct DecompRound {
    slot_id: u32,
    ptr: *const u8,
    len: usize,
    compressed: bool,
    file_index: u64,
    fdata_offset: u64,
    chunk_seq: u32,
    checksum: [u8; 32],
}

// Safety: ptr addresses a published Magazine slot that stays live until
// release_one is called (compressed: after decompress; skip: after pwrite).
unsafe impl Send for DecompRound {}

impl DecompRound {
    unsafe fn as_slice<'a>(&self) -> &'a [u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

struct WorkerStats {
    total_chunks: u64,
    total_written_bytes: u64,
    verified_bytes: u64,
    corrupt_bytes: u64,
    corrupt_row_indices: Vec<u64>,
}

/// Ensure `cur` holds a Clip with at least `need` bytes free. When full,
/// publish it (arming the outstanding counter) and send pending rounds before
/// claiming a fresh slot. Mirrors `ensure_room` in slot_packer.rs.
fn ensure_room_read<'p>(
    pool: &'p Magazine,
    queue_tx: &crossbeam_channel::Sender<DecompRound>,
    cur: &mut Option<Clip<'p>>,
    pending: &mut Vec<DecompRound>,
    need: usize,
) {
    loop {
        if cur.is_none() {
            *cur = pool.claim();
            if cur.is_none() {
                return;
            }
        }
        if cur.as_ref().unwrap().remaining() >= need {
            return;
        }
        let _ = cur.take().unwrap().publish(); // sets outstanding counter; Rounds unused
        for r in pending.drain(..) {
            queue_tx.send(r).ok();
        }
    }
}

pub fn decompress_archive(
    index_path: &Path,
    save_data: bool,
    out_dir: &Path,
) -> Result<VerifyReport> {
    let (schema, batches) = read_znippy_index(index_path)?;

    let batch = Arc::new(match batches.len() {
        0 => arrow::record_batch::RecordBatch::new_empty(Arc::new(
            crate::index::ZNIPPY_INDEX_SCHEMA.as_ref().clone(),
        )),
        1 => batches.into_iter().next().unwrap(),
        _ => arrow_select::concat::concat_batches(&schema, batches.iter())
            .map_err(|e| anyhow::anyhow!("failed to merge index batches: {}", e))?,
    });

    let total_rows = batch.num_rows();

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
    drop(unique_files);

    // Pre-create output files before spawning threads so workers can pwrite
    // concurrently with no locking. Indexed by row so lookup is O(1).
    let out_dir_arc = Arc::new(out_dir.to_path_buf());
    let output_files: Arc<Vec<Option<Arc<File>>>> = if save_data {
        let mut path_to_file: HashMap<String, Arc<File>> = HashMap::new();
        let mut created_dirs: HashSet<PathBuf> = HashSet::new();
        let mut files: Vec<Option<Arc<File>>> = Vec::with_capacity(total_rows);
        for i in 0..total_rows {
            let rel_path = paths_col.value(i).to_string();
            let f = path_to_file.entry(rel_path.clone()).or_insert_with(|| {
                let full = out_dir_arc.join(&rel_path);
                if let Some(parent) = full.parent() {
                    if created_dirs.insert(parent.to_path_buf()) {
                        let _ = std::fs::create_dir_all(parent);
                    }
                }
                Arc::new(
                    OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(true)
                        .open(&full)
                        .expect("failed to open output file"),
                )
            });
            files.push(Some(Arc::clone(f)));
        }
        Arc::new(files)
    } else {
        Arc::new(vec![])
    };

    let num_workers = (CONFIG.max_core_in_flight as usize).max(1);
    let pool = Magazine::new(NUM_SLOTS, SLOT_SIZE, num_workers);
    let ejector = pool.returner();

    let (queue_tx, queue_rx) = bounded::<DecompRound>(num_workers * 4);
    let (stats_tx, stats_rx) = crossbeam_channel::unbounded::<WorkerStats>();

    // ── WORKERS: decompress + verify + pwrite ──────────────────────────────────
    let mut worker_handles = Vec::with_capacity(num_workers);
    for _ in 0..num_workers {
        let queue_rx = queue_rx.clone();
        let ejector = ejector.clone();
        let output_files = Arc::clone(&output_files);
        let stats_tx = stats_tx.clone();
        worker_handles.push(thread::spawn(move || {
            let mut st = WorkerStats {
                total_chunks: 0,
                total_written_bytes: 0,
                verified_bytes: 0,
                corrupt_bytes: 0,
                corrupt_row_indices: Vec::new(),
            };

            while let Ok(round) = queue_rx.recv() {
                st.total_chunks += 1;
                // Safety: slot stays live until release_one below.
                let src = unsafe { round.as_slice() };

                if round.compressed {
                    let decompressed = match decompress_microchunk(src) {
                        Ok(v) => v,
                        Err(e) => {
                            log::error!(
                                "[decomp] row {} chunk_seq={} error={}",
                                round.file_index, round.chunk_seq, e
                            );
                            ejector.release_one(round.slot_id);
                            continue;
                        }
                    };
                    ejector.release_one(round.slot_id); // input consumed; slot freed

                    let out: &[u8] = &decompressed;
                    let len = out.len() as u64;
                    st.total_written_bytes += len;
                    let computed = blake3::hash(out);
                    if computed.as_bytes() == &round.checksum {
                        st.verified_bytes += len;
                    } else {
                        st.corrupt_bytes += len;
                        st.corrupt_row_indices.push(round.file_index);
                        log::error!(
                            "[verify] MISMATCH row={} chunk_seq={} expected={} got={}",
                            round.file_index,
                            round.chunk_seq,
                            hex::encode(round.checksum),
                            hex::encode(computed.as_bytes()),
                        );
                    }
                    if let Some(Some(file)) = output_files.get(round.file_index as usize) {
                        file.write_all_at(out, round.fdata_offset)
                            .expect("pwrite to output file failed");
                    }
                } else {
                    // Skip path: src IS the uncompressed data. Release slot AFTER pwrite.
                    let len = src.len() as u64;
                    st.total_written_bytes += len;
                    let computed = blake3::hash(src);
                    if computed.as_bytes() == &round.checksum {
                        st.verified_bytes += len;
                    } else {
                        st.corrupt_bytes += len;
                        st.corrupt_row_indices.push(round.file_index);
                        log::error!(
                            "[verify] MISMATCH row={} chunk_seq={} expected={} got={}",
                            round.file_index,
                            round.chunk_seq,
                            hex::encode(round.checksum),
                            hex::encode(computed.as_bytes()),
                        );
                    }
                    if let Some(Some(file)) = output_files.get(round.file_index as usize) {
                        file.write_all_at(src, round.fdata_offset)
                            .expect("pwrite to output file failed");
                    }
                    ejector.release_one(round.slot_id); // after write; slot was the data
                }
            }

            stats_tx.send(st).ok();
        }));
    }
    drop(queue_rx); // reader holds the only sender; workers hold the receivers
    drop(stats_tx);

    // ── READER: pread blobs from archive into Magazine slots ───────────────────
    let archive_path = index_path.to_path_buf();
    let batch_for_reader = Arc::clone(&batch);
    let reader_thread = thread::spawn(move || -> usize {
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
        let blob_offset_col = batch_for_reader
            .column_by_name("blob_offset").unwrap()
            .as_any().downcast_ref::<UInt64Array>().unwrap();
        let blob_size_col = batch_for_reader
            .column_by_name("blob_size").unwrap()
            .as_any().downcast_ref::<UInt64Array>().unwrap();

        let archive_file = File::open(&archive_path).expect("failed to open archive");

        let mut cur: Option<Clip<'_>> = None;
        let mut pending: Vec<DecompRound> = Vec::new();

        for row_idx in 0..total_rows {
            let blob_size = blob_size_col.value(row_idx) as usize;
            let blob_offset = blob_offset_col.value(row_idx);
            let compressed = compressed_col.value(row_idx);
            let fdata_offset = fdata_offset_col.value(row_idx);
            let chunk_seq = chunk_seq_col.value(row_idx);
            let mut checksum = [0u8; 32];
            checksum.copy_from_slice(checksum_col.value(row_idx));

            ensure_room_read(&pool, &queue_tx, &mut cur, &mut pending, blob_size);
            let clip = cur.as_mut().expect("pool drained unexpectedly");

            // Capture ptr before dropping the writable borrow, then pread.
            let ptr = {
                let w = clip.writable(blob_size);
                let p = w.as_ptr() as *const u8;
                if blob_size > 0 {
                    archive_file
                        .read_exact_at(w, blob_offset)
                        .expect("failed to read blob from archive");
                }
                p
            }; // writable borrow released here
            // commit_slice tracks outstanding count (the returned Round is unused).
            clip.commit_slice(blob_size, !compressed, row_idx as u64, fdata_offset, chunk_seq);
            pending.push(DecompRound {
                slot_id: clip.slot_id(),
                ptr,
                len: blob_size,
                compressed,
                file_index: row_idx as u64,
                fdata_offset,
                chunk_seq,
                checksum,
            });
        }

        // Flush the final partial slot.
        if let Some(clip) = cur.take() {
            let _ = clip.publish();
            for r in pending.drain(..) {
                queue_tx.send(r).ok();
            }
        }

        // Drain: claim all slots back, proving workers have released all slot memory.
        // This is the only backpressure / memory-safety barrier in the pipeline.
        for _ in 0..pool.num_slots() {
            if pool.claim().is_none() {
                break;
            }
        }
        drop(queue_tx); // workers exit once the queue is empty
        drop(pool); // all slot memory is now unreferenced

        total_files
    });

    // ── JOIN ───────────────────────────────────────────────────────────────────
    let reported_total_files = reader_thread.join().expect("reader panicked");
    for h in worker_handles {
        h.join().expect("worker panicked");
    }

    let mut total_chunks = 0u64;
    let mut total_written_bytes = 0u64;
    let mut verified_bytes = 0u64;
    let mut corrupt_bytes = 0u64;
    let mut corrupt_rows: HashSet<u64> = HashSet::new();
    for ws in stats_rx {
        total_chunks += ws.total_chunks;
        total_written_bytes += ws.total_written_bytes;
        verified_bytes += ws.verified_bytes;
        corrupt_bytes += ws.corrupt_bytes;
        for ri in ws.corrupt_row_indices {
            corrupt_rows.insert(ri);
        }
    }
    let corrupt_files = corrupt_rows.len();
    let verified_files = reported_total_files.saturating_sub(corrupt_files);

    Ok(VerifyReport {
        total_files: reported_total_files,
        verified_files,
        corrupt_files,
        total_bytes: total_written_bytes,
        verified_bytes,
        corrupt_bytes,
        chunks: total_chunks,
    })
}

pub fn decompress_microchunk(input: &[u8]) -> Result<Vec<u8>> {
    crate::codec::decompress_frame(input)
}
