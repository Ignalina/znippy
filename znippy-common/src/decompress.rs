use anyhow::Result;
use arrow_array::{BooleanArray, FixedSizeBinaryArray, StringArray, UInt64Array};
use std::{
    collections::{HashMap, HashSet},
    fs::{File, OpenOptions},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};

use crate::{
    common_config::CONFIG,
    index::{VerifyReport, read_znippy_index},
};

/// Per-worker accumulator, merged on join.
#[derive(Default)]
struct WorkerStats {
    total_chunks: u64,
    total_written_bytes: u64,
    verified_bytes: u64,
    corrupt_bytes: u64,
    corrupt_rows: Vec<u64>,
}

/// Decompress (and optionally extract) a znippy archive.
///
/// No-coordination read pipeline: every chunk in the index is independent work.
/// `num_workers` threads share ONE atomic row cursor — each grabs the next row,
/// `pread`s its blob straight from the archive (positioned I/O, no shared seek),
/// decompresses-or-skips into reusable buffers, blake3-verifies against the
/// per-chunk checksum, and `pwrite`s to its output file at `fdata_offset`.
/// Reads and writes are all positioned, so there is no reader/writer bottleneck
/// and no shared mutable state beyond the lock-free cursor.
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

    // Pre-create output files, indexed by row for O(1) lock-free lookup in
    // workers. Rows sharing a path share one Arc<File> (pwrite is positioned, so
    // many threads can write disjoint regions of the same file concurrently).
    let output_files: Arc<Vec<Option<Arc<File>>>> = if save_data {
        let mut path_to_file: HashMap<&str, Arc<File>> = HashMap::new();
        let mut created_dirs: HashSet<PathBuf> = HashSet::new();
        let mut files: Vec<Option<Arc<File>>> = Vec::with_capacity(total_rows);
        for i in 0..total_rows {
            let rel_path = paths_col.value(i);
            let f = path_to_file.entry(rel_path).or_insert_with(|| {
                let full = out_dir.join(rel_path);
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

    let archive = Arc::new(File::open(index_path)?);
    let cursor = Arc::new(AtomicUsize::new(0));
    let num_workers = (CONFIG.max_core_in_flight as usize).max(1);

    let mut handles = Vec::with_capacity(num_workers);
    for _ in 0..num_workers {
        let batch = Arc::clone(&batch);
        let archive = Arc::clone(&archive);
        let cursor = Arc::clone(&cursor);
        let output_files = Arc::clone(&output_files);
        handles.push(thread::spawn(move || -> WorkerStats {
            // Downcast each column once; per-row access is a cheap indexed read.
            let blob_offset_col = batch
                .column_by_name("blob_offset").unwrap()
                .as_any().downcast_ref::<UInt64Array>().unwrap();
            let blob_size_col = batch
                .column_by_name("blob_size").unwrap()
                .as_any().downcast_ref::<UInt64Array>().unwrap();
            let fdata_offset_col = batch
                .column_by_name("fdata_offset").unwrap()
                .as_any().downcast_ref::<UInt64Array>().unwrap();
            let compressed_col = batch
                .column_by_name("compressed").unwrap()
                .as_any().downcast_ref::<BooleanArray>().unwrap();
            let checksum_col = batch
                .column_by_name("checksum").unwrap()
                .as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();

            let mut st = WorkerStats::default();
            let mut read_buf: Vec<u8> = Vec::new(); // reused: holds the on-disk blob
            let mut out_buf: Vec<u8> = Vec::new(); // reused: holds decompressed bytes

            loop {
                let row = cursor.fetch_add(1, Ordering::Relaxed);
                if row >= total_rows {
                    break;
                }
                st.total_chunks += 1;

                let blob_offset = blob_offset_col.value(row);
                let blob_size = blob_size_col.value(row) as usize;
                let fdata_offset = fdata_offset_col.value(row);
                let compressed = compressed_col.value(row);

                // pread the blob into the reusable read buffer (positioned read).
                read_buf.resize(blob_size, 0);
                if blob_size > 0 {
                    archive
                        .read_exact_at(&mut read_buf, blob_offset)
                        .expect("failed to read blob from archive");
                }

                // Decompress into out_buf, or use the raw blob bytes (skip path).
                let out: &[u8] = if compressed {
                    match crate::codec::decompress_into(&read_buf, &mut out_buf) {
                        Ok(_) => &out_buf,
                        Err(e) => {
                            log::error!("[decomp] row {} error={}", row, e);
                            continue;
                        }
                    }
                } else {
                    &read_buf
                };

                let len = out.len() as u64;
                st.total_written_bytes += len;

                // Per-chunk verify: checksum is over the UNCOMPRESSED bytes.
                let computed = blake3::hash(out);
                if &computed.as_bytes()[..] == checksum_col.value(row) {
                    st.verified_bytes += len;
                } else {
                    st.corrupt_bytes += len;
                    st.corrupt_rows.push(row as u64);
                    log::error!(
                        "[verify] MISMATCH row={} expected={} got={}",
                        row,
                        hex::encode(checksum_col.value(row)),
                        hex::encode(computed.as_bytes()),
                    );
                }

                if let Some(Some(file)) = output_files.get(row) {
                    file.write_all_at(out, fdata_offset)
                        .expect("pwrite to output file failed");
                }
            }
            st
        }));
    }

    let mut total_chunks = 0u64;
    let mut total_written_bytes = 0u64;
    let mut verified_bytes = 0u64;
    let mut corrupt_bytes = 0u64;
    let mut corrupt_rows: HashSet<u64> = HashSet::new();
    for h in handles {
        let st = h.join().expect("worker panicked");
        total_chunks += st.total_chunks;
        total_written_bytes += st.total_written_bytes;
        verified_bytes += st.verified_bytes;
        corrupt_bytes += st.corrupt_bytes;
        for r in st.corrupt_rows {
            corrupt_rows.insert(r);
        }
    }
    let corrupt_files = corrupt_rows.len();
    let verified_files = total_files.saturating_sub(corrupt_files);

    Ok(VerifyReport {
        total_files,
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
