//! Two-pass directory compression.
//!
//! Pass 1 — BIG files (> slice_size): sequential chunked reads, metadata by re-reading file.
//! Pass 2 — SMALL files (≤ slice_size): read into slot, metadata from in-memory data.
//!
//! Arrow IPC index is written incrementally — each pass writes its batch as soon as it finishes.
//! No accumulation, no merge.

use anyhow::{Result, anyhow};
use crossbeam_channel::{bounded, unbounded};
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
    FileExtMeta, build_arrow_metadata_for_config,
    build_metadata_batch, compose_index_schema, should_skip_compression,
};
use znippy_common::meta::{BlobMeta, ChunkMeta};
use znippy_common::slotpool::Magazine;
use znippy_common::CompressionReport;
use znippy_common::{ArchiveMetaSink, ArrowIpcSink, GroupKey};

const SLOT_SIZE: usize = 200 * 1024 * 1024;
const NUM_SLOTS: usize = 8;

struct WriteJob {
    buf: Vec<u8>,
    on_disk_len: usize,
    file_index: u64,
    fdata_offset: u64,
    chunk_seq: u32,
    checksum: [u8; 32],
    compressed: bool,
    uncompressed_size: u64,
}

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

    let ext_fields: Vec<znippy_common::arrow::datatypes::Field> =
        plugin.map(|r| r.schema_fields()).unwrap_or_default();

    let output_path = output.with_extension("znippy");
    let file = Arc::new(File::create(&output_path)?);
    let out_cursor = Arc::new(AtomicU64::new(0));

    let num_workers = CONFIG.max_core_in_flight.max(1);
    let slice_size = SLOT_SIZE / num_workers.max(1);

    // ── PARTITION ────────────────────────────────────────────────────────────
    let mut big_indices: Vec<usize> = Vec::new();
    let mut small_indices: Vec<usize> = Vec::new();
    for (i, path) in all_files.iter().enumerate() {
        let size = path.metadata().map(|m| m.len()).unwrap_or(0);
        if size > slice_size as u64 || size == 0 {
            big_indices.push(i);
        } else {
            small_indices.push(i);
        }
    }

    let mut ext_meta: Vec<FileExtMeta> = vec![None; all_files.len()];
    let mut uncompressed_files = 0u64;
    let mut uncompressed_bytes = 0u64;
    let mut compressed_files = 0u64;
    let mut compressed_bytes = 0u64;
    let mut total_chunks = 0u64;

    // Metadata index schema (shared across both passes). The batches are
    // collected and handed to the metadata sink as one sub-index below.
    let meta_map = build_arrow_metadata_for_config(&CONFIG);
    let composed = compose_index_schema(&ext_fields);
    let schema_with_meta =
        arrow::datatypes::Schema::new_with_metadata(composed.fields().to_vec(), meta_map);
    let mut index_batches: Vec<arrow::record_batch::RecordBatch> = Vec::new();

    let input_dir_for_paths = input_dir.clone();
    let all_files_for_paths = Arc::clone(&all_files);

    // ══════════════════════════════════════════════════════════════════════════
    // PASS 1: BIG FILES
    // ══════════════════════════════════════════════════════════════════════════
    if !big_indices.is_empty() {
        let (uf, ub, cf, cb, blobs, meta) = run_big_pass(
            &all_files, input_dir, &big_indices, no_skip, plugin,
            &file, &out_cursor, num_workers,
        )?;
        uncompressed_files += uf; uncompressed_bytes += ub;
        compressed_files += cf; compressed_bytes += cb;
        for (idx, m) in meta { if idx < ext_meta.len() { ext_meta[idx] = m; } }

        total_chunks += blobs.len() as u64;
        let all_f = Arc::clone(&all_files_for_paths);
        let inp = input_dir_for_paths.clone();
        let resolver = |file_index: u64| {
            let idx = file_index as usize;
            all_f[idx].strip_prefix(&inp).unwrap_or(&all_f[idx])
                .to_string_lossy().to_string()
        };
        let batch = build_metadata_batch(&blobs, resolver, &ext_meta, &ext_fields)
            .map_err(|e| anyhow!("big index batch: {e}"))?;
        index_batches.push(batch);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // PASS 2: SMALL FILES
    // ══════════════════════════════════════════════════════════════════════════
    if !small_indices.is_empty() {
        let (uf, ub, cf, cb, blobs, meta) = run_small_pass(
            &all_files, input_dir, &small_indices, no_skip, plugin,
            &file, &out_cursor, num_workers, slice_size,
        )?;
        uncompressed_files += uf; uncompressed_bytes += ub;
        compressed_files += cf; compressed_bytes += cb;
        for (idx, m) in meta { if idx < ext_meta.len() { ext_meta[idx] = m; } }

        total_chunks += blobs.len() as u64;
        let all_f = Arc::clone(&all_files_for_paths);
        let inp = input_dir_for_paths.clone();
        let resolver = |file_index: u64| {
            let idx = file_index as usize;
            all_f[idx].strip_prefix(&inp).unwrap_or(&all_f[idx])
                .to_string_lossy().to_string()
        };
        let batch = build_metadata_batch(&blobs, resolver, &ext_meta, &ext_fields)
            .map_err(|e| anyhow!("small index batch: {e}"))?;
        index_batches.push(batch);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // FINALIZE: write the metadata layer (one sub-index of all batches) via the sink
    // ══════════════════════════════════════════════════════════════════════════
    let index_offset = out_cursor.load(Ordering::Relaxed);
    let blob_bytes = index_offset;
    let pkg_type_val: i8 = plugin.and_then(|r| r.type_id()).unwrap_or(0);

    let mut sink: Box<dyn ArchiveMetaSink> =
        Box::new(ArrowIpcSink::new(Arc::clone(&file), blob_bytes));
    sink.push_subindex(
        &schema_with_meta,
        &index_batches,
        GroupKey {
            pkg_type: pkg_type_val,
            repo: repo.unwrap_or("").to_string(),
            module_name: String::new(),
        },
    )?;
    let total_bytes_out = sink.finish()?;

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

// ─────────────────────────────────────────────────────────────────────────────
// PASS 1: big files — sequential chunked reads, re-read for metadata
// ─────────────────────────────────────────────────────────────────────────────
fn run_big_pass(
    all_files: &Arc<Vec<PathBuf>>,
    input_dir: &PathBuf,
    big_indices: &[usize],
    no_skip: bool,
    plugin: Option<&znippy_common::plugin::PluginRegistry>,
    file: &Arc<File>,
    out_cursor: &Arc<AtomicU64>,
    num_workers: usize,
) -> Result<(u64, u64, u64, u64, Vec<BlobMeta>, Vec<(usize, FileExtMeta)>)> {
    let pool = Magazine::new(NUM_SLOTS, SLOT_SIZE, num_workers);
    let returner = pool.returner();
    let (tx_slice, rx_slice) = bounded(NUM_SLOTS * 4);
    let (tx_write, rx_write) = unbounded::<WriteJob>();
    let (tx_meta, rx_meta) = unbounded::<(usize, FileExtMeta)>();

    let plugin_addr: usize = plugin.map(|p| p as *const _ as usize).unwrap_or(0);

    let reader = {
        let all_files = Arc::clone(all_files);
        let input_dir = input_dir.clone();
        let big_indices = big_indices.to_vec();
        let tx_meta = tx_meta.clone();
        thread::spawn(move || -> (u64, u64, u64, u64) {
            let plugin_ref: Option<&znippy_common::plugin::PluginRegistry> =
                if plugin_addr != 0 { Some(unsafe { &*(plugin_addr as *const _) }) } else { None };

            let mut uf = 0u64; let mut ub = 0u64;
            let mut cf = 0u64; let mut cb = 0u64;
            let mut cur = None;
            let ss = pool.slice_size();

            for &file_index in &big_indices {
                let path = &all_files[file_index];
                let file_size = path.metadata().map(|m| m.len()).unwrap_or(0);
                let skip = !no_skip && should_skip_compression(path);
                if skip { uf += 1; ub += file_size; } else { cf += 1; cb += file_size; }

                if file_size == 0 {
                    ensure_room(&pool, &tx_slice, &mut cur, 0);
                    cur.as_mut().unwrap().commit_slice(0, skip, file_index as u64, 0, 0);
                    continue;
                }

                let f = match File::open(path) {
                    Ok(f) => f,
                    Err(e) => { log::warn!("[big] open {}: {}", path.display(), e); continue; }
                };
                let mut rdr = BufReader::new(f);
                let mut fdata_offset = 0u64;
                let mut chunk_seq = 0u32;
                let mut remaining = file_size;

                while remaining > 0 {
                    let want = ss.min(remaining as usize);
                    ensure_room(&pool, &tx_slice, &mut cur, want);
                    let fill = cur.as_mut().unwrap();
                    let buf = fill.writable(want);
                    let got = match read_fully(&mut rdr, buf) {
                        Ok(g) => g,
                        Err(e) => { log::warn!("[big] read {}: {}", path.display(), e); break; }
                    };
                    if got == 0 { break; }
                    fill.commit_slice(got, skip, file_index as u64, fdata_offset, chunk_seq);
                    fdata_offset += got as u64;
                    chunk_seq += 1;
                    remaining = remaining.saturating_sub(got as u64);
                    if got < want { break; }
                }

                // Big file metadata: re-read the file (acceptable for large files).
                if let Some(reg) = plugin_ref {
                    let rel = path.strip_prefix(&input_dir).unwrap_or(path).to_string_lossy();
                    if reg.matches(&rel) {
                        if let Ok(data) = std::fs::read(path) {
                            if let Some(row) = reg.extract(&rel, &data) {
                                tx_meta.send((file_index, Some((reg.type_id().unwrap_or(0), row)))).ok();
                            }
                        }
                    }
                }
            }

            if let Some(fill) = cur.take() {
                for s in fill.publish() { tx_slice.send(s).ok(); }
            }

            // Drain: reclaim all slots to prove workers/writer are done with slot memory.
            for _ in 0..NUM_SLOTS {
                if pool.claim().is_none() { break; }
            }

            drop(tx_slice);
            drop(tx_meta);
            drop(pool);
            (uf, ub, cf, cb)
        })
    };

    let workers = spawn_workers(num_workers, rx_slice.clone(), tx_write.clone(), returner.clone());
    let writer = spawn_writer(Arc::clone(file), Arc::clone(out_cursor), returner.clone(), rx_write);

    drop(tx_write); drop(rx_slice); drop(tx_meta);

    let (uf, ub, cf, cb) = reader.join().map_err(|_| anyhow!("big reader panicked"))?;
    for w in workers { w.join().map_err(|_| anyhow!("big worker panicked"))??; }
    let blobs = writer.join().map_err(|_| anyhow!("big writer panicked"))??;

    let mut meta = Vec::new();
    while let Ok(m) = rx_meta.try_recv() { meta.push(m); }

    Ok((uf, ub, cf, cb, blobs, meta))
}

// ─────────────────────────────────────────────────────────────────────────────
// PASS 2: small files — read into slot, metadata from in-memory data
// ─────────────────────────────────────────────────────────────────────────────
fn run_small_pass(
    all_files: &Arc<Vec<PathBuf>>,
    input_dir: &PathBuf,
    small_indices: &[usize],
    no_skip: bool,
    plugin: Option<&znippy_common::plugin::PluginRegistry>,
    file: &Arc<File>,
    out_cursor: &Arc<AtomicU64>,
    num_workers: usize,
    _slice_size: usize,
) -> Result<(u64, u64, u64, u64, Vec<BlobMeta>, Vec<(usize, FileExtMeta)>)> {
    let pool = Magazine::new(NUM_SLOTS, SLOT_SIZE, num_workers);
    let returner = pool.returner();
    let (tx_slice, rx_slice) = bounded(NUM_SLOTS * 4);
    let (tx_write, rx_write) = unbounded::<WriteJob>();
    let (tx_meta, rx_meta) = unbounded::<(usize, FileExtMeta)>();

    let plugin_addr: usize = plugin.map(|p| p as *const _ as usize).unwrap_or(0);

    let reader = {
        let all_files = Arc::clone(all_files);
        let input_dir = input_dir.clone();
        let small_indices = small_indices.to_vec();
        let tx_meta = tx_meta.clone();
        thread::spawn(move || -> (u64, u64, u64, u64) {
            let plugin_ref: Option<&znippy_common::plugin::PluginRegistry> =
                if plugin_addr != 0 { Some(unsafe { &*(plugin_addr as *const _) }) } else { None };

            let mut uf = 0u64; let mut ub = 0u64;
            let mut cf = 0u64; let mut cb = 0u64;
            let mut cur = None;

            let mut ring = io_uring::IoUring::new(256).expect("io_uring init");
            let mut idx = 0usize;
            let n = small_indices.len();

            while idx < n {
                // Collect a batch that fits in current slot
                if cur.is_none() { cur = pool.claim(); }
                let batch_start = idx;
                let mut batch: Vec<(usize, usize, bool)> = Vec::new(); // (file_index, size, skip)
                let mut batch_total = 0usize;

                while idx < n && batch.len() < 128 {
                    let file_index = small_indices[idx];
                    let path = &all_files[file_index];
                    let file_size = path.metadata().map(|m| m.len()).unwrap_or(0) as usize;
                    let skip = !no_skip && should_skip_compression(path);

                    let remaining = cur.as_ref().unwrap().remaining();
                    if batch_total + file_size > remaining {
                        if batch_total == 0 {
                            // Slot is too full for even one file — publish and get new slot
                            if let Some(fill) = cur.take() {
                                for s in fill.publish() { tx_slice.send(s).ok(); }
                            }
                            cur = pool.claim();
                            continue; // retry with new slot
                        }
                        break; // process what we have
                    }

                    if skip { uf += 1; ub += file_size as u64; }
                    else { cf += 1; cb += file_size as u64; }
                    batch.push((file_index, file_size, skip));
                    batch_total += file_size;
                    idx += 1;
                }

                if batch.is_empty() { continue; }

                // Phase 1: batch open
                let mut cstrings: Vec<std::ffi::CString> = Vec::with_capacity(batch.len());
                for &(fi, _, _) in &batch {
                    let p = all_files[fi].as_os_str().as_encoded_bytes();
                    cstrings.push(unsafe { std::ffi::CString::from_vec_unchecked(p.to_vec()) });
                }

                let mut fds: Vec<i32> = vec![-1; batch.len()];
                for chunk in (0..batch.len()).collect::<Vec<_>>().chunks(256) {
                    for &i in chunk {
                        let open_e = io_uring::opcode::OpenAt::new(
                            io_uring::types::Fd(libc::AT_FDCWD),
                            cstrings[i].as_ptr(),
                        )
                        .flags(libc::O_RDONLY | libc::O_CLOEXEC)
                        .build()
                        .user_data(i as u64);
                        unsafe { ring.submission().push(&open_e).ok(); }
                    }
                    ring.submit_and_wait(chunk.len()).ok();
                    let mut got = 0;
                    while got < chunk.len() {
                        if let Some(cqe) = ring.completion().next() {
                            fds[cqe.user_data() as usize] = cqe.result();
                            got += 1;
                        }
                    }
                }

                // Phase 2: batch read directly into slot via writable()
                // writable() gives a slice at cursor without advancing — use it for
                // the entire batch, then commit each file.
                let fill = cur.as_mut().unwrap();
                let slot_buf = fill.writable(batch_total);
                let slot_ptr = slot_buf.as_mut_ptr();

                let mut offsets: Vec<usize> = Vec::with_capacity(batch.len());
                let mut off = 0usize;
                for &(_, size, _) in &batch {
                    offsets.push(off);
                    off += size;
                }

                let mut read_results: Vec<usize> = vec![0; batch.len()];
                for chunk in (0..batch.len()).collect::<Vec<_>>().chunks(256) {
                    let mut to_submit = 0;
                    for &i in chunk {
                        if fds[i] < 0 { continue; }
                        let (_, size, _) = batch[i];
                        let dst = unsafe { slot_ptr.add(offsets[i]) };
                        let read_e = io_uring::opcode::Read::new(
                            io_uring::types::Fd(fds[i]),
                            dst,
                            size as u32,
                        )
                        .build()
                        .user_data(i as u64);
                        unsafe { ring.submission().push(&read_e).ok(); }
                        to_submit += 1;
                    }
                    if to_submit > 0 {
                        ring.submit_and_wait(to_submit).ok();
                        let mut got = 0;
                        while got < to_submit {
                            if let Some(cqe) = ring.completion().next() {
                                let i = cqe.user_data() as usize;
                                read_results[i] = if cqe.result() > 0 { cqe.result() as usize } else { 0 };
                                got += 1;
                            }
                        }
                    }
                }

                // Phase 3: close fds
                for &fd in &fds {
                    if fd >= 0 { unsafe { libc::close(fd); } }
                }

                // Phase 4: plugin extraction + commit each file
                let fill = cur.as_mut().unwrap();
                for i in 0..batch.len() {
                    let (file_index, _size, skip) = batch[i];
                    let got = read_results[i];

                    if let Some(reg) = plugin_ref {
                        if got > 0 {
                            let path = &all_files[file_index];
                            let rel = path.strip_prefix(&input_dir).unwrap_or(path).to_string_lossy();
                            if reg.matches(&rel) {
                                let data = unsafe {
                                    std::slice::from_raw_parts(slot_ptr.add(offsets[i]), got)
                                };
                                if let Some(row) = reg.extract(&rel, data) {
                                    tx_meta.send((file_index, Some((reg.type_id().unwrap_or(0), row)))).ok();
                                }
                            }
                        }
                    }

                    fill.commit_slice(got, skip, file_index as u64, 0, 0);
                }
            }

            if let Some(fill) = cur.take() {
                for s in fill.publish() { tx_slice.send(s).ok(); }
            }

            for _ in 0..NUM_SLOTS {
                if pool.claim().is_none() { break; }
            }

            drop(tx_slice);
            drop(tx_meta);
            drop(pool);
            (uf, ub, cf, cb)
        })
    };

    let workers = spawn_workers(num_workers, rx_slice.clone(), tx_write.clone(), returner.clone());
    let writer = spawn_writer(Arc::clone(file), Arc::clone(out_cursor), returner.clone(), rx_write);

    drop(tx_write); drop(rx_slice); drop(tx_meta);

    let (uf, ub, cf, cb) = reader.join().map_err(|_| anyhow!("small reader panicked"))?;
    for w in workers { w.join().map_err(|_| anyhow!("small worker panicked"))??; }
    let blobs = writer.join().map_err(|_| anyhow!("small writer panicked"))??;

    let mut meta = Vec::new();
    while let Ok(m) = rx_meta.try_recv() { meta.push(m); }

    Ok((uf, ub, cf, cb, blobs, meta))
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared helpers
// ─────────────────────────────────────────────────────────────────────────────

fn spawn_workers(
    num_workers: usize,
    rx_slice: crossbeam_channel::Receiver<znippy_common::slotpool::Round>,
    tx_write: crossbeam_channel::Sender<WriteJob>,
    returner: znippy_common::slotpool::Ejector,
) -> Vec<thread::JoinHandle<Result<()>>> {
    let level = CONFIG.compression_level;
    (0..num_workers).map(|_| {
        let rx = rx_slice.clone();
        let tw = tx_write.clone();
        let ret = returner.clone();
        thread::spawn(move || -> Result<()> {
            let mut cctx = znippy_common::codec::CompressCtx::new(level)?;
            let mut reuse_buf: Vec<u8> = Vec::new();
            while let Ok(slice) = rx.recv() {
                let src = unsafe { slice.as_slice() };
                let checksum = *blake3::hash(src).as_bytes();
                if slice.skip {
                    // Copy to owned buf and release slot immediately.
                    let mut buf = std::mem::take(&mut reuse_buf);
                    let len = src.len();
                    if buf.capacity() < len { buf.reserve(len - buf.len()); }
                    buf.resize(len, 0);
                    buf[..len].copy_from_slice(src);
                    ret.release_one(slice.slot_id);
                    tw.send(WriteJob {
                        buf, on_disk_len: len,
                        file_index: slice.file_index, fdata_offset: slice.fdata_offset,
                        chunk_seq: slice.chunk_seq, checksum, compressed: false,
                        uncompressed_size: len as u64,
                    }).ok();
                } else {
                    let mut buf = std::mem::take(&mut reuse_buf);
                    let n = cctx.compress_into(src, &mut buf)?;
                    let usz = src.len() as u64;
                    ret.release_one(slice.slot_id);
                    tw.send(WriteJob {
                        buf, on_disk_len: n,
                        file_index: slice.file_index, fdata_offset: slice.fdata_offset,
                        chunk_seq: slice.chunk_seq, checksum, compressed: true,
                        uncompressed_size: usz,
                    }).ok();
                }
            }
            Ok(())
        })
    }).collect()
}

fn spawn_writer(
    file: Arc<File>,
    out_cursor: Arc<AtomicU64>,
    _returner: znippy_common::slotpool::Ejector,
    rx_write: crossbeam_channel::Receiver<WriteJob>,
) -> thread::JoinHandle<Result<Vec<BlobMeta>>> {
    thread::spawn(move || -> Result<Vec<BlobMeta>> {
        let mut blobs = Vec::new();
        while let Ok(job) = rx_write.recv() {
            let off = out_cursor.fetch_add(job.on_disk_len as u64, Ordering::Relaxed);
            file.write_all_at(&job.buf[..job.on_disk_len], off)?;
            blobs.push(BlobMeta {
                chunk_meta: ChunkMeta {
                    fdata_offset: job.fdata_offset, file_index: job.file_index,
                    chunk_seq: job.chunk_seq, checksum: job.checksum,
                    compressed: job.compressed, uncompressed_size: job.uncompressed_size,
                    compressed_size: job.on_disk_len as u64,
                },
                blob_offset: off, blob_size: job.on_disk_len as u64,
            });
        }
        Ok(blobs)
    })
}

fn ensure_room<'p>(
    pool: &'p Magazine,
    tx_slice: &crossbeam_channel::Sender<znippy_common::slotpool::Round>,
    cur: &mut Option<znippy_common::slotpool::Clip<'p>>,
    need: usize,
) {
    loop {
        if cur.is_none() {
            *cur = pool.claim();
            if cur.is_none() { return; }
        }
        if cur.as_ref().unwrap().remaining() >= need { return; }
        let slices = cur.take().unwrap().publish();
        for s in slices { tx_slice.send(s).ok(); }
    }
}
