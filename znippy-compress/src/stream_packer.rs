//! Streaming compressor (`compress_stream`) on the no-barrier Gatling model.
//!
//! Entries arrive in memory via a channel. The reader (loader) splits each
//! entry's data into rounds (small entry = one whole round; big entry =
//! slice-size rounds), referencing the entry's `Arc<Vec<u8>>` — zero-copy, no
//! slot buffers needed since the data is already in RAM. Worker barrels pull
//! rounds off one queue, stamp BLAKE3 over the original bytes, compress (or
//! skip), and pwrite the payload at a reserved offset. The finalizer groups by
//! (pkg_type, repo) into a v0.7 multi-index — arrow-ipc from metadata only,
//! never the payload.

use anyhow::{Result, anyhow};
use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

use znippy_common::CompressionReport;
use znippy_common::common_config::CONFIG;
use znippy_common::index::{
    MULTI_INDEX_MAGIC, ManifestEntry, build_arrow_metadata_for_config, build_metadata_batch,
    compose_index_schema, should_skip_compression, write_manifest_bytes,
};
use znippy_common::meta::{BlobMeta, ChunkMeta};

/// Entries bigger than this are cut into slice-size rounds; smaller stay whole.
const SLICE_SIZE: usize = 8 * 1024 * 1024;

/// An entry to be compressed into the archive.
pub struct ArchiveEntry {
    pub relative_path: String,
    pub data: Vec<u8>,
    /// Package type discriminator. None means "untyped / default group".
    /// When all entries share the same (pkg_type, repo), the archive is written
    /// in v0.6 format. Multiple distinct pairs produce a v0.7 multi-index archive.
    pub pkg_type: Option<i8>,
    /// Repository label for this entry. None is treated as "".
    pub repo: Option<String>,
}

impl ArchiveEntry {
    pub fn new(relative_path: impl Into<String>, data: Vec<u8>) -> Self {
        Self { relative_path: relative_path.into(), data, pkg_type: None, repo: None }
    }
}

impl Default for ArchiveEntry {
    fn default() -> Self {
        Self { relative_path: String::new(), data: Vec::new(), pkg_type: None, repo: None }
    }
}

/// A handle to the streaming compressor.
pub struct StreamCompressor {
    tx: Option<Sender<ArchiveEntry>>,
    join_handle: Option<thread::JoinHandle<Result<CompressionReport>>>,
}

impl StreamCompressor {
    pub fn sender(&self) -> &Sender<ArchiveEntry> {
        self.tx.as_ref().expect("sender already consumed")
    }

    pub fn finish(mut self) -> Result<CompressionReport> {
        drop(self.tx.take());
        self.join_handle
            .take()
            .expect("already finished")
            .join()
            .map_err(|e| anyhow!("Compression thread panicked: {:?}", e))?
    }
}

pub fn compress_stream(output: &PathBuf, no_skip: bool) -> Result<StreamCompressor> {
    let (tx_entry, rx_entry): (Sender<ArchiveEntry>, Receiver<ArchiveEntry>) = unbounded();
    let output = output.clone();

    let join_handle = thread::spawn(move || -> Result<CompressionReport> {
        run_pipeline(rx_entry, &output, no_skip)
    });

    Ok(StreamCompressor { tx: Some(tx_entry), join_handle: Some(join_handle) })
}

/// Per-file metadata collected by the reader, consumed by the finalizer.
struct FileRegistry {
    paths: Vec<String>,
    pkg_types: Vec<Option<i8>>,
    repos: Vec<Option<String>>,
}

/// One round of work referencing a slice of an entry's in-memory data.
/// The `Arc` keeps the entry alive until every round over it is processed.
struct Round {
    data: Arc<Vec<u8>>,
    start: usize,
    len: usize,
    skip: bool,
    file_index: u64,
    fdata_offset: u64,
    chunk_seq: u32,
}

/// A fired round handed from a barrel to the writer.
enum Payload {
    /// Compressed output; recycled to the buffer pool after pwrite.
    Buf(Vec<u8>),
    /// Skip path: zero-copy from the entry's data (the Arc keeps it alive).
    Skip { data: Arc<Vec<u8>>, start: usize, len: usize },
}

struct WriteJob {
    payload: Payload,
    on_disk_len: usize,
    file_index: u64,
    fdata_offset: u64,
    chunk_seq: u32,
    checksum: [u8; 32],
    compressed: bool,
    uncompressed_size: u64,
}

fn run_pipeline(
    rx_entry: Receiver<ArchiveEntry>,
    output: &PathBuf,
    no_skip: bool,
) -> Result<CompressionReport> {
    let output_path = output.with_extension("znippy");
    let file = Arc::new(File::create(&output_path)?);
    let out_cursor = Arc::new(AtomicU64::new(0)); // blob region starts at 0

    let num_workers = CONFIG.max_core_in_flight.max(1);
    let (tx_round, rx_round) = bounded::<Round>(num_workers * 4);
    let (tx_write, rx_write) = bounded::<WriteJob>(num_workers * 4);
    // Recycled compressed-output buffers; the writer returns each after pwrite.
    let (free_bufs_tx, free_bufs_rx) = bounded::<Vec<u8>>(num_workers * 4);
    for _ in 0..num_workers * 4 {
        free_bufs_tx.send(Vec::new()).ok();
    }

    // ── READER (the loader): feed rounds onto the belt ───────────────────────
    let reader_thread = thread::spawn(move || -> (u64, u64, u64, u64, FileRegistry) {
        let mut paths = Vec::new();
        let mut pkg_types = Vec::new();
        let mut repos = Vec::new();
        let (mut uf, mut ub, mut cf, mut cb) = (0u64, 0u64, 0u64, 0u64);

        for entry in rx_entry {
            let file_index = paths.len() as u64;
            let skip = !no_skip && should_skip_compression(Path::new(&entry.relative_path));
            let data_len = entry.data.len() as u64;
            if skip {
                uf += 1;
                ub += data_len;
            } else {
                cf += 1;
                cb += data_len;
            }
            pkg_types.push(entry.pkg_type);
            repos.push(entry.repo);
            paths.push(entry.relative_path);

            let data = Arc::new(entry.data);
            let total = data.len();
            if total == 0 {
                // Empty entry → one zero-length round so it appears in the index.
                tx_round
                    .send(Round {
                        data: Arc::clone(&data),
                        start: 0,
                        len: 0,
                        skip,
                        file_index,
                        fdata_offset: 0,
                        chunk_seq: 0,
                    })
                    .ok();
                continue;
            }
            let small = total <= SLICE_SIZE;
            let mut off = 0usize;
            let mut seq = 0u32;
            while off < total {
                let len = if small { total } else { SLICE_SIZE.min(total - off) };
                tx_round
                    .send(Round {
                        data: Arc::clone(&data),
                        start: off,
                        len,
                        skip,
                        file_index,
                        fdata_offset: off as u64,
                        chunk_seq: seq,
                    })
                    .ok();
                off += len;
                seq += 1;
            }
        }
        drop(tx_round); // lets the barrels stop once the belt runs out
        (uf, ub, cf, cb, FileRegistry { paths, pkg_types, repos })
    });

    // ── BARRELS: compress (or skip) and toss to the writer — never wait on I/O ─
    let mut workers = Vec::with_capacity(num_workers);
    for _ in 0..num_workers {
        let rx_round = rx_round.clone();
        let tx_write = tx_write.clone();
        let free_bufs_rx = free_bufs_rx.clone();
        let level = CONFIG.compression_level;
        workers.push(thread::spawn(move || -> Result<()> {
            let mut cctx = znippy_common::codec::CompressCtx::new(level)?;
            while let Ok(r) = rx_round.recv() {
                let src = &r.data[r.start..r.start + r.len];
                let checksum = *blake3::hash(src).as_bytes(); // ORIGINAL bytes, pre-compression
                let uncompressed_size = r.len as u64;

                let (payload, on_disk_len, compressed) = if r.skip {
                    (
                        Payload::Skip { data: Arc::clone(&r.data), start: r.start, len: r.len },
                        r.len,
                        false,
                    )
                } else {
                    let mut buf = free_bufs_rx.recv().unwrap_or_default();
                    let n = cctx.compress_into(src, &mut buf)?;
                    (Payload::Buf(buf), n, true)
                };

                tx_write
                    .send(WriteJob {
                        payload,
                        on_disk_len,
                        file_index: r.file_index,
                        fdata_offset: r.fdata_offset,
                        chunk_seq: r.chunk_seq,
                        checksum,
                        compressed,
                        uncompressed_size,
                    })
                    .ok();
            }
            Ok(())
        }));
    }

    // ── WRITER: one worker that only fires bytes at the disk ────────────────────
    let writer_thread = {
        let file = Arc::clone(&file);
        let out_cursor = Arc::clone(&out_cursor);
        thread::spawn(move || -> Result<Vec<BlobMeta>> {
            let mut all_blobs: Vec<BlobMeta> = Vec::new();
            while let Ok(job) = rx_write.recv() {
                let off = out_cursor.fetch_add(job.on_disk_len as u64, Ordering::Relaxed);
                match job.payload {
                    Payload::Buf(mut buf) => {
                        file.write_all_at(&buf[..job.on_disk_len], off)?;
                        buf.clear();
                        free_bufs_tx.send(buf).ok();
                    }
                    Payload::Skip { data, start, len } => {
                        file.write_all_at(&data[start..start + len], off)?;
                    }
                }
                all_blobs.push(BlobMeta {
                    chunk_meta: ChunkMeta {
                        fdata_offset: job.fdata_offset,
                        file_index: job.file_index,
                        chunk_seq: job.chunk_seq,
                        checksum: job.checksum,
                        compressed: job.compressed,
                        uncompressed_size: job.uncompressed_size,
                        compressed_size: job.on_disk_len as u64,
                    },
                    blob_offset: off,
                    blob_size: job.on_disk_len as u64,
                });
            }
            Ok(all_blobs)
        })
    };

    // Main holds none of the producing handles, so the belts close cleanly.
    drop(tx_write);
    drop(rx_round);
    drop(free_bufs_rx);

    // ── FINALIZER (main): join, then build the multi-index ──────────────────────
    let (uf, ub, cf, cb, reg) =
        reader_thread.join().map_err(|_| anyhow!("reader panicked"))?;
    for w in workers {
        w.join().map_err(|_| anyhow!("barrel panicked"))??;
    }
    let mut all_blobs = writer_thread.join().map_err(|_| anyhow!("writer panicked"))??;

    all_blobs.sort_by_key(|b| (b.chunk_meta.file_index, b.chunk_meta.chunk_seq));

    let blob_bytes = out_cursor.load(Ordering::Relaxed);
    let total_chunks = all_blobs.len() as u64;

    use arrow::ipc::writer::StreamWriter;

    // Group blob indices by (pkg_type, repo). BTreeMap keeps groups stable.
    let file_keys: Vec<(i8, String)> = reg
        .pkg_types
        .iter()
        .zip(reg.repos.iter())
        .map(|(p, r)| (p.unwrap_or(0), r.clone().unwrap_or_default()))
        .collect();
    let mut groups: std::collections::BTreeMap<(i8, String), Vec<usize>> =
        std::collections::BTreeMap::new();
    for (i, blob) in all_blobs.iter().enumerate() {
        let key = file_keys[blob.chunk_meta.file_index as usize].clone();
        groups.entry(key).or_default().push(i);
    }

    let meta_map = build_arrow_metadata_for_config(&CONFIG);
    let mut manifest_entries: Vec<ManifestEntry> = Vec::new();
    let mut cursor = blob_bytes; // index region begins right after the blobs

    for ((pkg_type, repo), blob_indices) in &groups {
        let sub_start = cursor;
        let group_blobs: Vec<_> = blob_indices.iter().map(|&i| all_blobs[i].clone()).collect();
        let row_count = group_blobs.len() as u64;

        let batch = build_metadata_batch(&group_blobs, |fi| reg.paths[fi as usize].clone(), &[], &[])
            .map_err(|e| anyhow!("build sub-index batch: {e}"))?;
        let schema_with_meta = arrow::datatypes::Schema::new_with_metadata(
            compose_index_schema(&[]).fields().to_vec(),
            meta_map.clone(),
        );
        let mut sub_bytes: Vec<u8> = Vec::new();
        let mut sw = StreamWriter::try_new(&mut sub_bytes, &schema_with_meta)
            .map_err(|e| anyhow!("sub-index writer: {e}"))?;
        sw.write(&batch).map_err(|e| anyhow!("sub-index write: {e}"))?;
        sw.finish().map_err(|e| anyhow!("sub-index finish: {e}"))?;

        let sub_len = sub_bytes.len() as u64;
        file.write_all_at(&sub_bytes, sub_start)?;
        cursor += sub_len;

        manifest_entries.push(ManifestEntry {
            pkg_type: *pkg_type,
            repo: repo.clone(),
            module_name: String::new(),
            index_offset: sub_start,
            index_len: sub_len,
            row_count,
        });
    }

    let manifest_offset = cursor;
    let manifest_bytes = write_manifest_bytes(&manifest_entries).map_err(|e| anyhow!("manifest: {e}"))?;
    file.write_all_at(&manifest_bytes, manifest_offset)?;
    let after = manifest_offset + manifest_bytes.len() as u64;
    file.write_all_at(&MULTI_INDEX_MAGIC, after)?;
    file.write_all_at(&manifest_offset.to_le_bytes(), after + MULTI_INDEX_MAGIC.len() as u64)?;
    file.sync_all()?;

    let total_bytes_out = after + MULTI_INDEX_MAGIC.len() as u64 + 8;
    let total_files = uf + cf;

    log::info!(
        "[stream] gatling archive: {} group(s), {} blob bytes, manifest at {}",
        manifest_entries.len(),
        blob_bytes,
        manifest_offset
    );

    Ok(CompressionReport {
        total_files,
        compressed_files: cf,
        uncompressed_files: uf,
        chunks: total_chunks,
        total_dirs: 0,
        total_bytes_in: cb + ub,
        total_bytes_out,
        compressed_bytes: cb,
        uncompressed_bytes: ub,
        compression_ratio: if cb > 0 && total_bytes_out > ub {
            (cb as f32 / (total_bytes_out - ub) as f32) * 100.0
        } else {
            0.0
        },
    })
}
