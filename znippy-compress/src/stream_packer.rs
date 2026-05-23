use anyhow::{Result, anyhow};
use blake3::Hasher;
use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::thread;
use znippy_common::chunkrevolver::{ChunkRevolver, SendPtr, get_chunk_slice};
use znippy_common::common_config::CONFIG;
use znippy_common::index::{
    build_arrow_metadata_for_config, build_metadata_batch, compose_index_schema,
    should_skip_compression, write_manifest_bytes, ManifestEntry, MULTI_INDEX_MAGIC,
};
use znippy_common::meta::{BlobMeta, ChunkMeta, WriterStats};
use znippy_common::{CompressionReport, split_into_microchunks};
use crate::Blob;

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
        run_compression_pipeline(rx_entry, &output, no_skip)
    });

    Ok(StreamCompressor {
        tx: Some(tx_entry),
        join_handle: Some(join_handle),
    })
}

/// Per-file metadata collected by the reader thread and consumed by the writer.
struct FileRegistry {
    paths: Vec<String>,
    pkg_types: Vec<Option<i8>>,
    repos: Vec<Option<String>>,
}

fn run_compression_pipeline(
    rx_entry: Receiver<ArchiveEntry>,
    output: &PathBuf,
    no_skip: bool,
) -> Result<CompressionReport> {
    let (tx_chunk_array, rx_chunk_array): (
        Vec<Sender<(u64, u64, u8, u64, u64, bool)>>,
        Vec<Receiver<(u64, u64, u8, u64, u64, bool)>>,
    ) = (0..CONFIG.max_core_in_flight)
        .map(|_| bounded(CONFIG.max_chunks as usize))
        .unzip();

    let (tx_compressed, rx_compressed): (
        Sender<(Blob, ChunkMeta)>,
        Receiver<(Blob, ChunkMeta)>,
    ) = unbounded();
    let (tx_return, rx_return): (Sender<(u8, u64)>, Receiver<(u8, u64)>) = unbounded();
    let (paths_tx, paths_rx) = bounded::<FileRegistry>(1);
    let (checksum_tx, checksum_rx) = bounded::<Vec<[u8; 32]>>(1);

    let mut revolver = ChunkRevolver::new(&CONFIG);
    let base_ptrs = revolver.base_ptrs();
    let chunk_size = revolver.chunk_size();

    // Reader thread: streams entries from rx_entry, no collect()
    let reader_thread = {
        let tx_chunk_array = tx_chunk_array.clone();
        let rx_done = rx_return.clone();

        thread::spawn(move || {
            let mut paths: Vec<String> = Vec::new();
            let mut pkg_types: Vec<Option<i8>> = Vec::new();
            let mut repos: Vec<Option<String>> = Vec::new();
            let mut inflight_chunks = 0usize;
            let mut uncompressed_files: u64 = 0;
            let mut uncompressed_bytes: u64 = 0;
            let mut compressed_files: u64 = 0;
            let mut compressed_bytes: u64 = 0;

            for entry in rx_entry {
                let file_index = paths.len() as u64;
                let skip = !no_skip
                    && should_skip_compression(std::path::Path::new(&entry.relative_path));
                let data_len = entry.data.len() as u64;

                if skip {
                    uncompressed_files += 1;
                    uncompressed_bytes += data_len;
                } else {
                    compressed_files += 1;
                    compressed_bytes += data_len;
                }

                pkg_types.push(entry.pkg_type);
                repos.push(entry.repo);
                paths.push(entry.relative_path);

                let mut cursor = std::io::Cursor::new(entry.data);
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
                                                file_index,
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
                                            file_index,
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
                            inflight_chunks =
                                inflight_chunks.checked_sub(1).expect("inflight_chunks underflow");
                        }
                    }
                }
            }

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
                FileRegistry { paths, pkg_types, repos },
            )
        })
    };

    // Compressor threads
    let mut compressor_threads = Vec::with_capacity(CONFIG.max_core_in_flight as usize);
    for compressor_group in 0..CONFIG.max_core_in_flight as u8 {
        let rx_chunk = rx_chunk_array[compressor_group as usize].clone();
        let tx_compressed = tx_compressed.clone();
        let tx_ret = tx_return.clone();
        let base_ptr: SendPtr = base_ptrs[compressor_group as usize];

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
                                // Zero-copy: pass a raw pointer into the revolver slot.
                                // tx_ret is NOT called here — the writer returns the chunk
                                // after write_all, keeping the memory valid throughout.
                                let chunk_meta = ChunkMeta {
                                    fdata_offset,
                                    file_index,
                                    chunk_seq,
                                    checksum_group: compressor_group,
                                    compressed_size: input.len() as u64,
                                    compressed: false,
                                    uncompressed_size: input.len() as u64,
                                };
                                let blob = Blob::Revolver {
                                    ptr: input.as_ptr() as usize,
                                    len: input.len(),
                                    ring_nr,
                                    chunk_nr,
                                };
                                tx_compressed.send((blob, chunk_meta)).unwrap();
                                chunk_seq += 1;
                            } else {
                                let micro_chunks =
                                    split_into_microchunks(input, CONFIG.zstd_output_buffer_size);

                                if micro_chunks.is_empty() {
                                    let compressed_vec = cctx.compress(&[])?;
                                    let chunk_meta = ChunkMeta {
                                        fdata_offset,
                                        file_index,
                                        chunk_seq,
                                        checksum_group: compressor_group,
                                        compressed_size: compressed_vec.len() as u64,
                                        compressed: true,
                                        uncompressed_size: 0,
                                    };
                                    tx_compressed.send((Blob::Owned(compressed_vec), chunk_meta))?;
                                    chunk_seq += 1;
                                } else {
                                    for micro in micro_chunks.iter() {
                                        let compressed_vec = cctx.compress(micro)?;
                                        let chunk_meta = ChunkMeta {
                                            fdata_offset,
                                            file_index,
                                            chunk_seq,
                                            checksum_group: compressor_group,
                                            compressed_size: compressed_vec.len() as u64,
                                            compressed: true,
                                            uncompressed_size: micro.len() as u64,
                                        };
                                        fdata_offset += micro.len() as u64;
                                        tx_compressed.send((Blob::Owned(compressed_vec), chunk_meta))?;
                                        chunk_seq += 1;
                                    }
                                }
                                tx_ret.send((ring_nr, chunk_nr)).ok();
                            }
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

    // Writer thread: streams blobs to disk immediately, writes Arrow index at end.
    // It holds a tx_return clone so it can release Revolver chunks after write_all.
    let tx_ret_writer = tx_return.clone();
    let output_for_writer = output.clone();
    let writer_thread = thread::spawn(move || -> WriterStats {
        let output_path = output_for_writer.with_extension("znippy");
        let file = File::create(&output_path).expect("Failed to create output file");
        let mut writer = std::io::BufWriter::new(file);

        let mut all_blobs: Vec<BlobMeta> = Vec::new();
        let mut current_offset: u64 = 0;
        let mut writerstats = WriterStats {
            total_chunks: 0,
            total_written_bytes: 0,
            verified_files: 0,
            corrupt_files: 0,
            verified_bytes: 0,
            corrupt_bytes: 0,
        };

        // Write each blob immediately as it arrives
        while let Ok((blob, chunk_meta)) = rx_compressed.recv() {
            writerstats.total_chunks += 1;
            let blob_size = blob.len() as u64;

            writer.write_all(blob.as_slice()).expect("Failed to write blob");
            if let Blob::Revolver { ring_nr, chunk_nr, .. } = blob {
                tx_ret_writer.send((ring_nr, chunk_nr)).ok();
            }

            all_blobs.push(BlobMeta {
                chunk_meta,
                blob_offset: current_offset,
                blob_size,
            });

            current_offset += blob_size;
            writerstats.total_written_bytes += blob_size;
        }

        // Receive file registry and checksums from main thread
        let reg = paths_rx.recv().expect("Failed to receive file registry");
        let checksums = checksum_rx.recv().expect("Failed to receive checksums");

        use arrow::ipc::writer::StreamWriter;

        // ── v0.7 writer ────────────────────────────────────────────────────
        // Group blob indices by (pkg_type, repo). BTreeMap keeps groups in stable order.
        let file_keys: Vec<(i8, String)> = reg.pkg_types.iter()
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

        for ((pkg_type, repo), blob_indices) in &groups {
            let sub_start = current_offset;
            let group_blobs: Vec<_> = blob_indices.iter()
                .map(|&i| all_blobs[i].clone())
                .collect();
            let row_count = group_blobs.len() as u64;

            let batch = build_metadata_batch(&group_blobs, &checksums, |fi| {
                reg.paths[fi as usize].clone()
            }, &[], &[])
            .expect("Failed to build sub-index batch");

            let schema_with_meta = arrow::datatypes::Schema::new_with_metadata(
                compose_index_schema(&[]).fields().to_vec(),
                meta_map.clone(),
            );
            let mut sub_bytes: Vec<u8> = Vec::new();
            let mut sw = StreamWriter::try_new(&mut sub_bytes, &schema_with_meta)
                .expect("Failed to create sub-index writer");
            sw.write(&batch).expect("Failed to write sub-index batch");
            sw.finish().expect("Failed to finish sub-index stream");

            let sub_len = sub_bytes.len() as u64;
            writer.write_all(&sub_bytes).expect("Failed to write sub-index");
            current_offset += sub_len;
            writerstats.total_written_bytes += sub_len;

            manifest_entries.push(ManifestEntry {
                pkg_type: *pkg_type,
                repo: repo.clone(),
                module_name: String::new(),
                index_offset: sub_start,
                index_len: sub_len,
                row_count,
            });
        }

        // Write manifest + 16-byte v0.7 footer (MAGIC + manifest_offset).
        let manifest_offset = current_offset;
        let manifest_bytes = write_manifest_bytes(&manifest_entries)
            .expect("Failed to serialize manifest");
        writer.write_all(&manifest_bytes).expect("Failed to write manifest");
        writer.write_all(&MULTI_INDEX_MAGIC).expect("Failed to write magic");
        writer.write_all(&manifest_offset.to_le_bytes()).expect("Failed to write manifest offset");
        writer.flush().expect("Failed to flush");

        log::info!("[writer] v0.7 archive: {} group(s), {} blob bytes, manifest at {}",
            manifest_entries.len(), writerstats.total_written_bytes, manifest_offset);

        writerstats
    });

    // Wait for reader to finish; it returns file registry (paths + pkg_types + repos)
    let (uncompressed_files, uncompressed_bytes, compressed_files, compressed_bytes, reg) =
        reader_thread.join().unwrap();

    let total_files = (uncompressed_files + compressed_files) as u64;

    // Send file registry to writer before compressors finish (writer is still consuming rx_compressed)
    paths_tx.send(reg).expect("Failed to send file registry to writer");

    tx_chunk_array.into_iter().for_each(drop);

    // Collect checksums from compressor threads
    let mut checksums: Vec<[u8; 32]> =
        vec![[0u8; 32]; CONFIG.max_core_in_flight as usize];
    for handle in compressor_threads {
        let Ok((compressor_group, checksum)): Result<(u8, [u8; 32]), anyhow::Error> =
            handle.join().unwrap()
        else {
            return Err(anyhow!("Compressor thread returned error"));
        };
        checksums[compressor_group as usize] = checksum;
    }

    drop(tx_compressed);
    checksum_tx
        .send(checksums)
        .expect("Failed to send checksums to writer");

    let writerstats = writer_thread.join().unwrap();

    log::info!("[stream] archive written");

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
