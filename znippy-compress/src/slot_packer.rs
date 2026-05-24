//! No-barrier slice pipeline (TODO_NOW.md "THE FINAL SOLUTION 2026").
//!
//! River model: one reader pours files into shared 200 MB slots; small files
//! coalesce bow-to-stern (1 file = 1 slice), big files are cut into slice-size
//! logs spilling across slots. All workers pull slices off ONE global queue and
//! never wait for a slot to finish. Each worker compresses (if needed), stamps
//! BLAKE3 over the ORIGINAL bytes, pwrites the payload at a reserved offset, and
//! releases the slot. The finalizer builds the arrow-ipc index from metadata
//! only — no payload ever crosses a channel.
//!
//! This is the directory-compression entry point (`compress_dir`).

use anyhow::{Result, anyhow};
use crossbeam_channel::bounded;
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use walkdir::WalkDir;

use znippy_common::common_config::CONFIG;
use znippy_common::index::{
    FileExtMeta, MULTI_INDEX_MAGIC, ManifestEntry, build_arrow_metadata_for_config,
    build_metadata_batch, compose_index_schema, should_skip_compression, write_manifest_bytes,
};
use znippy_common::meta::{BlobMeta, ChunkMeta};
use znippy_common::slotpool::Magazine;
use znippy_common::CompressionReport;

/// Slot buffer size. Big enough that the single reader stays ahead of the cores.
const SLOT_SIZE: usize = 200 * 1024 * 1024;
/// Read-ahead depth: number of slots the reader can fill before the cores drain.
const NUM_SLOTS: usize = 8;

/// A fired round handed from a barrel to the writer. Carries either the
/// compressed output buffer (recycled after pwrite) or, for the skip path, a
/// pointer into the slot (the writer pwrites it zero-copy, then frees the slot).
struct WriteJob {
    buf: Option<Vec<u8>>,
    ptr: *const u8,
    on_disk_len: usize,
    slot_id: u32,
    release_slot: bool,
    file_index: u64,
    fdata_offset: u64,
    chunk_seq: u32,
    checksum: [u8; 32],
    compressed: bool,
    uncompressed_size: u64,
}
// Safety: `ptr` (skip path) addresses slot memory that stays live until the
// writer calls `release_one` after the pwrite — the reader's reclaim-all barrier
// guarantees no job outlives its slot.
unsafe impl Send for WriteJob {}

/// Read until `buf` is full or EOF; returns bytes read (< buf.len() only at EOF).
fn read_fully<R: Read>(r: &mut R, buf: &mut [u8]) -> io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match r.read(&mut buf[n..])? {
            0 => break,
            k => n += k,
        }
    }
    Ok(n)
}

pub fn compress_dir(
    input_dir: &PathBuf,
    output: &PathBuf,
    no_skip: bool,
    plugin: Option<&znippy_common::plugin::PluginRegistry>,
    repo: Option<&str>,
) -> Result<CompressionReport> {
    let mut total_dirs = 0u64;
    let all_files: Arc<Vec<PathBuf>> = Arc::new(
        WalkDir::new(input_dir)
            .into_iter()
            .filter_map(|e| e.ok())
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
            .collect(),
    );
    let total_files = all_files.len() as u64;

    // Plugin metadata is now extracted INLINE during the reader loop (zero-copy
    // from the slot buffer). No separate pre-pass, no double read.
    // For files larger than slice_size (rare), we fall back to a separate fs::read.
    let ext_fields: Vec<znippy_common::arrow::datatypes::Field> =
        plugin.map(|r| r.schema_fields()).unwrap_or_default();

    // Channel for inline metadata extraction results from the reader thread.
    // The reader extracts metadata while data is in the slot buffer (no double read).
    let (tx_meta, rx_meta) = bounded::<(usize, FileExtMeta)>(256);

    let output_path = output.with_extension("znippy");
    let file = Arc::new(File::create(&output_path)?);
    let out_cursor = Arc::new(AtomicU64::new(0)); // blob region starts at 0

    let num_workers = CONFIG.max_core_in_flight.max(1);
    let pool = Magazine::new(NUM_SLOTS, SLOT_SIZE, num_workers);
    let slice_size = pool.slice_size();
    let returner = pool.returner();

    // The current (reader→barrels) and the write belt (barrels→writer).
    let (tx_slice, rx_slice) = bounded(NUM_SLOTS * 4);
    let (tx_write, rx_write) = bounded::<WriteJob>(num_workers * 4);
    // Recycled compressed-output buffers: the writer returns each after pwrite,
    // so the compress path never allocates per round.
    let (free_bufs_tx, free_bufs_rx) = bounded::<Vec<u8>>(num_workers * 4);
    for _ in 0..num_workers * 4 {
        free_bufs_tx.send(Vec::new()).ok();
    }

    // ── READER: the single source ───────────────────────────────────────────
    // SAFETY: plugin outlives reader_thread (we join it before returning from compress_dir).
    // We pass the plugin as a usize (pointer cast) to satisfy Send requirements.
    let plugin_addr: usize = plugin.map(|p| p as *const _ as usize).unwrap_or(0);

    let reader_thread = {
        let all_files = Arc::clone(&all_files);
        let input_dir = input_dir.clone();
        thread::spawn(move || -> (u64, u64, u64, u64) {
            let plugin_ref: Option<&znippy_common::plugin::PluginRegistry> = if plugin_addr != 0 {
                Some(unsafe { &*(plugin_addr as *const znippy_common::plugin::PluginRegistry) })
            } else {
                None
            };
            let mut uncompressed_files = 0u64;
            let mut uncompressed_bytes = 0u64;
            let mut compressed_files = 0u64;
            let mut compressed_bytes = 0u64;

            let mut cur = None;

            for (file_index, path) in all_files.iter().enumerate() {
                let skip = !no_skip && should_skip_compression(path);
                let file_size = path.metadata().map(|m| m.len()).unwrap_or(0);
                if skip {
                    uncompressed_files += 1;
                    uncompressed_bytes += file_size;
                } else {
                    compressed_files += 1;
                    compressed_bytes += file_size;
                }

                let f = match File::open(path) {
                    Ok(f) => f,
                    Err(e) => {
                        log::warn!("[reader] open {} failed: {}", path.display(), e);
                        continue;
                    }
                };
                let mut rdr = BufReader::new(f);
                let mut fdata_offset = 0u64;
                let mut chunk_seq = 0u32;

                // Check if plugin wants this file (before reading data).
                let wants_meta = plugin_ref
                    .map(|r| r.matches(&path.strip_prefix(&input_dir)
                        .unwrap_or(path).to_string_lossy()))
                    .unwrap_or(false);

                // Empty file → one zero-length slice so it appears in the index.
                if file_size == 0 {
                    ensure_room(&pool, &tx_slice, &mut cur, 0);
                    let fill = cur.as_mut().unwrap();
                    fill.commit_slice(0, skip, file_index as u64, 0, 0);
                    continue;
                }

                let mut remaining = file_size;
                // Small file (≤ slice_size) is NEVER split: read it whole as one slice.
                // Big file is cut into slice_size logs (may spill across slots).
                let small = file_size <= slice_size as u64;
                while remaining > 0 {
                    let want = if small {
                        file_size as usize
                    } else {
                        slice_size.min(remaining as usize)
                    };
                    ensure_room(&pool, &tx_slice, &mut cur, want);
                    let fill = cur.as_mut().unwrap();
                    let buf = fill.writable(want);
                    let got = match read_fully(&mut rdr, buf) {
                        Ok(g) => g,
                        Err(e) => {
                            log::warn!("[reader] read {} failed: {}", path.display(), e);
                            break;
                        }
                    };
                    if got == 0 {
                        break;
                    }

                    // Inline plugin extraction: for small files the entire content is
                    // in this single slice — extract metadata directly from slot memory.
                    // No double-read, no extra allocation.
                    if wants_meta && small && chunk_seq == 0 {
                        let rel = path.strip_prefix(&input_dir)
                            .unwrap_or(path).to_string_lossy();
                        if let Some(reg) = plugin_ref {
                            if let Some(row) = reg.extract(&rel, &buf[..got]) {
                                tx_meta.send((file_index, Some((reg.type_id().unwrap_or(0), row)))).ok();
                            }
                        }
                    }

                    fill.commit_slice(got, skip, file_index as u64, fdata_offset, chunk_seq);
                    fdata_offset += got as u64;
                    chunk_seq += 1;
                    remaining = remaining.saturating_sub(got as u64);
                    if got < want {
                        break; // short read = EOF
                    }
                }

                // Fallback: big files that were sliced — re-read for metadata (rare).
                if wants_meta && !small {
                    if let Some(reg) = plugin_ref {
                        let rel = path.strip_prefix(&input_dir)
                            .unwrap_or(path).to_string_lossy();
                        if let Ok(data) = std::fs::read(path) {
                            if let Some(row) = reg.extract(&rel, &data) {
                                tx_meta.send((file_index, Some((reg.type_id().unwrap_or(0), row)))).ok();
                            }
                        }
                    }
                }
            }

            // Publish the last partial slot.
            if let Some(fill) = cur.take() {
                for s in fill.publish() {
                    tx_slice.send(s).ok();
                }
            }

            // Wait for the pipeline to fully drain: reclaiming every slot proves all
            // slices were processed AND released, so the pool memory is safe to drop.
            for _ in 0..pool.num_slots() {
                if pool.claim().is_none() {
                    break;
                }
            }

            // Dropping tx_slice here lets the workers exit once the queue empties.
            drop(tx_slice);
            drop(tx_meta);
            drop(pool);

            (uncompressed_files, uncompressed_bytes, compressed_files, compressed_bytes)
        })
    };

    // ── BARRELS: compress (or skip) and toss to the writer — never wait on I/O ─
    let mut workers = Vec::with_capacity(num_workers);
    for _ in 0..num_workers {
        let rx_slice = rx_slice.clone();
        let tx_write = tx_write.clone();
        let free_bufs_rx = free_bufs_rx.clone();
        let returner = returner.clone();
        let level = CONFIG.compression_level;
        workers.push(thread::spawn(move || -> Result<()> {
            let mut cctx = znippy_common::codec::CompressCtx::new(level)?;
            while let Ok(slice) = rx_slice.recv() {
                // Safety: slot stays live until release_one (compress: here; skip: writer).
                let src = unsafe { slice.as_slice() };
                let checksum = *blake3::hash(src).as_bytes(); // ORIGINAL bytes, pre-compression
                if slice.skip {
                    // Hand the slot bytes to the writer; it frees the slot after pwrite.
                    tx_write
                        .send(WriteJob {
                            buf: None,
                            ptr: src.as_ptr(),
                            on_disk_len: src.len(),
                            slot_id: slice.slot_id,
                            release_slot: true,
                            file_index: slice.file_index,
                            fdata_offset: slice.fdata_offset,
                            chunk_seq: slice.chunk_seq,
                            checksum,
                            compressed: false,
                            uncompressed_size: src.len() as u64,
                        })
                        .ok();
                } else {
                    let mut buf = free_bufs_rx.recv().unwrap_or_default();
                    let n = cctx.compress_into(src, &mut buf)?;
                    let uncompressed_size = src.len() as u64;
                    returner.release_one(slice.slot_id); // input consumed → free slot now
                    tx_write
                        .send(WriteJob {
                            buf: Some(buf),
                            ptr: std::ptr::null(),
                            on_disk_len: n,
                            slot_id: 0,
                            release_slot: false,
                            file_index: slice.file_index,
                            fdata_offset: slice.fdata_offset,
                            chunk_seq: slice.chunk_seq,
                            checksum,
                            compressed: true,
                            uncompressed_size,
                        })
                        .ok();
                }
            }
            Ok(())
        }));
    }

    // ── WRITER: one worker that only fires bytes at the disk ────────────────────
    let writer_thread = {
        let file = Arc::clone(&file);
        let out_cursor = Arc::clone(&out_cursor);
        let returner = returner.clone();
        thread::spawn(move || -> Result<Vec<BlobMeta>> {
            let mut all_blobs: Vec<BlobMeta> = Vec::new();
            while let Ok(job) = rx_write.recv() {
                let off = out_cursor.fetch_add(job.on_disk_len as u64, Ordering::Relaxed);
                match job.buf {
                    Some(mut buf) => {
                        file.write_all_at(&buf[..job.on_disk_len], off)?;
                        buf.clear();
                        free_bufs_tx.send(buf).ok(); // recycle for a barrel
                    }
                    None => {
                        // Safety: slot is live until we release it just below.
                        let bytes =
                            unsafe { std::slice::from_raw_parts(job.ptr, job.on_disk_len) };
                        file.write_all_at(bytes, off)?;
                        if job.release_slot {
                            returner.release_one(job.slot_id);
                        }
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

    // Drop main's handles so the belts close when reader/barrels/writer finish.
    // (tx_slice moved into the reader; free_bufs_tx moved into the writer.)
    drop(tx_write);
    drop(rx_slice);
    drop(free_bufs_rx);

    // ── FINALIZER (main): join, then build the index from metadata only ─────────
    let (uncompressed_files, uncompressed_bytes, compressed_files, compressed_bytes) =
        reader_thread.join().map_err(|_| anyhow!("reader panicked"))?;
    for w in workers {
        w.join().map_err(|_| anyhow!("barrel panicked"))??;
    }
    let mut all_blobs = writer_thread.join().map_err(|_| anyhow!("writer panicked"))??;

    // Collect inline-extracted metadata from the reader thread channel.
    let mut ext_meta: Vec<FileExtMeta> = vec![None; all_files.len()];
    while let Ok((idx, meta)) = rx_meta.try_recv() {
        if idx < ext_meta.len() {
            ext_meta[idx] = meta;
        }
    }

    // Stable index order (out-of-order completion is fine on disk; index is sorted).
    all_blobs.sort_by_key(|b| (b.chunk_meta.file_index, b.chunk_meta.chunk_seq));

    let total_chunks = all_blobs.len() as u64;
    let index_offset = out_cursor.load(Ordering::Relaxed); // end of blob region
    let blob_bytes = index_offset;

    let input_dir_for_paths = input_dir.clone();
    let all_files_for_paths = Arc::clone(&all_files);
    let path_resolver = |file_index: u64| {
        let idx = file_index as usize;
        all_files_for_paths[idx]
            .strip_prefix(&input_dir_for_paths)
            .unwrap_or(&all_files_for_paths[idx])
            .to_string_lossy()
            .to_string()
    };

    use arrow::ipc::writer::StreamWriter;
    let row_count = all_blobs.len() as u64;
    let batch = build_metadata_batch(&all_blobs, path_resolver, &ext_meta, &ext_fields)
        .map_err(|e| anyhow!("build index batch: {e}"))?;
    let meta_map = build_arrow_metadata_for_config(&CONFIG);
    let composed = compose_index_schema(&ext_fields);
    let schema_with_meta =
        arrow::datatypes::Schema::new_with_metadata(composed.fields().to_vec(), meta_map);
    let mut sub_bytes: Vec<u8> = Vec::new();
    let mut sw = StreamWriter::try_new(&mut sub_bytes, &schema_with_meta)
        .map_err(|e| anyhow!("index writer: {e}"))?;
    sw.write(&batch).map_err(|e| anyhow!("index write: {e}"))?;
    sw.finish().map_err(|e| anyhow!("index finish: {e}"))?;

    let sub_len = sub_bytes.len() as u64;
    file.write_all_at(&sub_bytes, index_offset)?;

    let manifest_offset = index_offset + sub_len;
    let pkg_type_val: i8 = plugin.and_then(|r| r.type_id()).unwrap_or(0);
    let manifest_entries = vec![ManifestEntry {
        pkg_type: pkg_type_val,
        repo: repo.unwrap_or("").to_string(),
        module_name: String::new(),
        index_offset,
        index_len: sub_len,
        row_count,
    }];
    let manifest_bytes = write_manifest_bytes(&manifest_entries)
        .map_err(|e| anyhow!("manifest: {e}"))?;
    file.write_all_at(&manifest_bytes, manifest_offset)?;

    let after_manifest = manifest_offset + manifest_bytes.len() as u64;
    file.write_all_at(&MULTI_INDEX_MAGIC, after_manifest)?;
    file.write_all_at(
        &manifest_offset.to_le_bytes(),
        after_manifest + MULTI_INDEX_MAGIC.len() as u64,
    )?;
    file.sync_all()?;

    let total_bytes_out =
        after_manifest + MULTI_INDEX_MAGIC.len() as u64 + 8;

    log::info!(
        "[slot_packer] {} chunks, {} blob bytes, manifest at {}",
        total_chunks, blob_bytes, manifest_offset
    );

    Ok(CompressionReport {
        total_files,
        compressed_files,
        uncompressed_files,
        chunks: total_chunks,
        total_dirs,
        total_bytes_in: compressed_bytes + uncompressed_bytes,
        total_bytes_out,
        compressed_bytes,
        uncompressed_bytes,
        compression_ratio: if uncompressed_bytes > 0 {
            (compressed_bytes as f32 / blob_bytes.max(1) as f32) * 100.0
        } else {
            0.0
        },
    })
}

/// Ensure `cur` is a claimed slot with at least `need` bytes free; publish &
/// re-claim when full. `need` is always ≤ SLOT_SIZE, so a fresh slot fits it.
fn ensure_room<'p>(
    pool: &'p Magazine,
    tx_slice: &crossbeam_channel::Sender<znippy_common::slotpool::Round>,
    cur: &mut Option<znippy_common::slotpool::Clip<'p>>,
    need: usize,
) {
    loop {
        if cur.is_none() {
            *cur = pool.claim();
            if cur.is_none() {
                return; // pool shut down
            }
        }
        if cur.as_ref().unwrap().remaining() >= need {
            return;
        }
        let slices = cur.take().unwrap().publish();
        for s in slices {
            tx_slice.send(s).ok();
        }
    }
}
