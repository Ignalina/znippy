use anyhow::Result;
use anyhow::anyhow;
use blake3::Hasher;
use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use std::fs::{File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{PathBuf};
use std::sync::Arc;
use std::thread;
use walkdir::WalkDir;
use znippy_common::chunkrevolver::{ChunkRevolver, SendPtr, get_chunk_slice};
use znippy_common::common_config::CONFIG;
use znippy_common::index::{
    ZNIPPY_INDEX_SCHEMA, build_arrow_metadata_for_config, build_metadata_batch,
    should_skip_compression,
};
use znippy_common::meta::{BlobMeta, ChunkMeta, WriterStats};
use znippy_common::{CompressionReport, split_into_microchunks};
use crate::Blob;

pub fn compress_dir(
    input_dir: &PathBuf,
    output: &PathBuf,
    no_skip: bool,
) -> anyhow::Result<CompressionReport> {
    log::debug!("Reading directory: {:?}", input_dir);
    let mut total_dirs = 0;
    let mut files_to_skip = 0u64;
    let mut files_to_compress = 0u64;

    let all_files: Arc<Vec<PathBuf>> = Arc::new(
        WalkDir::new(input_dir)
            .into_iter()
            .filter_map(|entry| entry.ok())
            .filter_map(|e| {
                if e.file_type().is_dir() {
                    total_dirs += 1;
                    None
                } else if e.file_type().is_file() {
                    let skip = !no_skip && should_skip_compression(e.path());
                    if skip {
                        files_to_skip += 1;
                    } else {
                        files_to_compress += 1;
                    }
                    Some(e.into_path())
                } else {
                    None
                }
            })
            .collect(),
    );

    let total_files = all_files.len() as u64;

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
    let (checksum_tx, checksum_rx) = bounded::<Vec<[u8; 32]>>(1);

    let output_path = output.with_extension("znippy");

    let mut revolver = ChunkRevolver::new(&CONFIG);
    let base_ptrs = revolver.base_ptrs();
    let chunk_size = revolver.chunk_size();

    let input_dir_cloned = input_dir.clone();
    let all_files_for_reader = Arc::clone(&all_files);
    let all_files_for_writer = Arc::clone(&all_files);

    let reader_thread = {
        let tx_chunk_array = tx_chunk_array.clone();
        let rx_done = rx_return.clone();
        thread::spawn(move || {
            let mut inflight_chunks = 0usize;
            let mut uncompressed_files: u64 = 0;
            let mut uncompressed_bytes: u64 = 0;
            let mut compressed_files: u64 = 0;
            let mut compressed_bytes: u64 = 0;

            for (file_index, path) in all_files_for_reader.iter().enumerate() {
                let skip = !no_skip && should_skip_compression(path);
                if skip {
                    uncompressed_files += 1;
                    uncompressed_bytes += path.metadata().unwrap().len();
                } else {
                    compressed_files += 1;
                    compressed_bytes += path.metadata().unwrap().len();
                }

                let file = match File::open(path) {
                    Ok(f) => f,
                    Err(e) => panic!("Problem opening the file: {:?}: {}", path, e),
                };
                let mut reader = BufReader::new(file);
                let mut has_read_any_data = false;
                let mut fdata_offset: u64 = 0;

                loop {
                    let maybe_chunk = revolver.try_get_chunk();
                    match maybe_chunk {
                        Some(mut chunk) => {
                            let ring_nr = chunk.ring_nr as usize;
                            match reader.read(&mut *chunk) {
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
                                    log::warn!("[reader] Error reading file {}: {}", path.display(), e);
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
            )
        })
    };

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
    let writer_thread = thread::spawn(move || -> WriterStats {
        let file = File::create(&output_path).expect("Failed to create output file");
        let mut writer = BufWriter::new(file);

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

        let checksums = checksum_rx.recv().expect("Failed to receive checksums");

        let index_offset = current_offset;

        let batch = build_metadata_batch(&all_blobs, &checksums, |file_index| {
            let idx = file_index as usize;
            all_files_for_writer[idx]
                .strip_prefix(&input_dir_cloned)
                .unwrap_or(&all_files_for_writer[idx])
                .to_string_lossy()
                .to_string()
        })
        .expect("Failed to build metadata batch");

        let meta_map = build_arrow_metadata_for_config(&CONFIG);
        let schema_with_meta = arrow::datatypes::Schema::new_with_metadata(
            ZNIPPY_INDEX_SCHEMA.fields().to_vec(),
            meta_map,
        );

        use arrow::ipc::writer::StreamWriter;
        let mut stream_writer = StreamWriter::try_new(&mut writer, &schema_with_meta)
            .expect("Failed to create Arrow stream writer");
        stream_writer.write(&batch).expect("Failed to write batch");
        stream_writer.finish().expect("Failed to finish Arrow stream");

        writer
            .write_all(&index_offset.to_le_bytes())
            .expect("Failed to write footer");
        writer.flush().expect("Failed to flush");

        log::info!(
            "[writer] Done: {} chunks, {} blob bytes, Arrow index at offset {}",
            writerstats.total_chunks,
            writerstats.total_written_bytes,
            index_offset,
        );

        writerstats
    });

    let (uncompressed_files, uncompressed_bytes, compressed_files, compressed_bytes) =
        reader_thread.join().unwrap();

    tx_chunk_array.into_iter().for_each(drop);

    let mut checksums: Vec<[u8; 32]> = vec![[0u8; 32]; CONFIG.max_core_in_flight as usize];
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

    log::info!("[main] v0.6 archive written");

    let report = CompressionReport {
        total_files,
        compressed_files,
        uncompressed_files,
        chunks: writerstats.total_chunks,
        total_dirs,
        total_bytes_in: compressed_bytes + uncompressed_bytes,
        total_bytes_out: writerstats.total_written_bytes,
        compressed_bytes,
        uncompressed_bytes,
        compression_ratio: if uncompressed_bytes > 0 {
            (compressed_bytes as f32
                / (writerstats.total_written_bytes - uncompressed_bytes) as f32)
                * 100.0
        } else {
            0.0
        },
    };

    Ok(report)
}
