use std::ptr;
use zstd_sys::*;

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
};

use anyhow::{Context, Result};
use arrow::datatypes::SchemaRef;
use arrow::ipc::RecordBatch;
use arrow_array::{Array, BinaryArray, Datum, ListArray, StringArray, StructArray, UInt32Array, UInt64Array, UInt8Array};

use blake3::Hasher;
use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use hex::FromHex;
use crate::{common_config::CONFIG, extract_config_from_arrow_metadata, index::read_znippy_index, index::VerifyReport, ChunkMeta, ChunkRevolver};

use crate::chunkrevolver::{get_chunk_slice, SendPtr,Chunk};

pub fn decompress_archive(index_path: &Path, save_data: bool, out_dir: &Path) -> Result<VerifyReport> {
    let zdata_path = index_path.with_extension("zdata");

    let (schema, batches) = read_znippy_index(index_path)?;
    let file_checksums = extract_file_checksums_from_metadata(&schema);
    let config = extract_config_from_arrow_metadata(schema.metadata())?;
    log::debug!("read config from meta {:?}\n and checksums {:?}", config,file_checksums);


    let file_checksums = Arc::new(file_checksums);


    let revolver = ChunkRevolver::new(&config);
    let base_ptr = SendPtr::new(revolver.base_ptr());
    let chunk_size = revolver.chunk_size();



    let (work_tx, work_rx): (Sender<(ChunkMeta,u32)>, Receiver<(ChunkMeta,u32 )>) = bounded(config.max_core_in_flight);

    let (chunk_tx, chunk_rx): (Sender<(ChunkMeta, Vec<u8>)>, Receiver<_>) = bounded(config.max_core_in_flight);

    let (done_tx, done_rx): (Sender<u64>, Receiver<u64>) = unbounded();

//    let chunk_counts = Arc::new(Mutex::new(HashMap::new()));

    let out_dir = Arc::new(out_dir.to_path_buf());
    let  report = VerifyReport::default();

    let rx = chunk_rx.clone();
    let out_dir_cloned = Arc::clone(&out_dir);
    let done_tx_cloned = done_tx.clone();



// READER

        let tx = work_tx.clone();
        let done_rx = done_rx.clone();
    let mut reader_thread = thread::spawn(move || {
        let mut inflight_chunks = 0usize;

        let mut zdata_file = File::open(&zdata_path).expect("Failed to open .zdata file");
        let mut revolver = revolver; // move into thread

        let Some(batch) = batches.get(0) else {
            eprintln!("❌ No batch found in index");
            return;
        };

        let file_count = batch.num_rows();


        let paths = batch
            .column_by_name("relative_path").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();

        let chunks_array = batch
            .column_by_name("chunks").unwrap()
            .as_any().downcast_ref::<ListArray>().unwrap();

        let struct_array = chunks_array.values().as_any().downcast_ref::<StructArray>().unwrap();

        let zdata_offsets = struct_array.column_by_name("zdata_offset").unwrap()
            .as_any().downcast_ref::<UInt64Array>().unwrap();

        let fdata_offsets = struct_array.column_by_name("fdata_offset").unwrap()
            .as_any().downcast_ref::<UInt64Array>().unwrap();

        let lengths = struct_array.column_by_name("length").unwrap()
            .as_any().downcast_ref::<UInt64Array>().unwrap();

        let chunk_seqs = struct_array.column_by_name("chunk_seq").unwrap()
            .as_any().downcast_ref::<UInt32Array>().unwrap();

        let checksum_groups = struct_array.column_by_name("checksum_group").unwrap()
            .as_any().downcast_ref::<UInt8Array>().unwrap();

        let uncompressed_size_arr = batch
            .column_by_name("uncompressed_size")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let fdata_offsets = struct_array
            .column_by_name("fdata_offset")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();


        let chunk_offsets = chunks_array.value_offsets();

        log::debug!(
    "Batch schema stats → files: {}, paths: {}, chunks: {}, zdata_offsets: {}, fdata_offsets: {}, lengths: {}, chunk_seqs: {}, checksum_groups: {}, uncompressed_sizes: {}",
    file_count,
    paths.len(),
    chunks_array.len(),
    zdata_offsets.len(),
    fdata_offsets.len(),
    lengths.len(),
    chunk_seqs.len(),
    checksum_groups.len(),
    uncompressed_size_arr.len()
);



        for file_index in 0..file_count as u64 - 1 {

            // Use the idiomatic way to navigate chunks in Arrow 55+
            let chunks_array = batch
                .column_by_name("chunks")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();

            let struct_array = chunks_array.values().as_any().downcast_ref::<StructArray>().unwrap();
            let chunk_offsets = chunks_array.value_offsets();

            // For each chunk in a file, use chunk_offsets directly
            for local_idx in chunk_offsets[file_index as usize] as usize..chunk_offsets[file_index as usize + 1] as usize {

                // Log which chunk we are reading
                log::debug!("[reader] reading file {} chunk {:?}",
            paths.value(file_index as usize),
            chunks_array.value(local_idx)
        );

                // Try to get a chunk – if none are available, wait for one to be returned
                let (mut chunk, chunk_index): (Chunk, u64) = loop {
                    match revolver.try_get_chunk() {
                        Some(c) => {
                            let idx = c.index;
                            break (c, idx);
                        }
                        None => {
                            // Block until a chunk is returned
                            let returned = done_rx.recv().expect("rx_done channel closed unexpectedly");
                            log::debug!("[reader] Blocking wait — returned chunk {} to pool", returned);
                            revolver.return_chunk(returned);
                            inflight_chunks = inflight_chunks.checked_sub(1).expect("inflight_chunks underflow");
                        }
                    }
                };

                // Retrieve the metadata for the current chunk
                let zdata_offset = struct_array.column_by_name("zdata_offset").unwrap()
                    .as_any().downcast_ref::<UInt64Array>().unwrap()
                    .value(local_idx);

                let fdata_offset = struct_array.column_by_name("fdata_offset").unwrap()
                    .as_any().downcast_ref::<UInt64Array>().unwrap()
                    .value(local_idx);

                let compressed_size = struct_array.column_by_name("length").unwrap()
                    .as_any().downcast_ref::<UInt64Array>().unwrap()
                    .value(local_idx);

                let chunk_seq = struct_array.column_by_name("chunk_seq").unwrap()
                    .as_any().downcast_ref::<UInt32Array>().unwrap()
                    .value(local_idx);

                let checksum_group = struct_array.column_by_name("checksum_group").unwrap()
                    .as_any().downcast_ref::<UInt8Array>().unwrap()
                    .value(local_idx);

                let uncompressed_size = uncompressed_size_arr.value(file_index as usize);

                // Read and process the chunk data
                zdata_file.seek(SeekFrom::Start(zdata_offset)).unwrap();
                zdata_file.read_exact(&mut chunk[..compressed_size as usize]).unwrap();

                // Prepare metadata for chunk
                let meta = ChunkMeta {
                    zdata_offset,
                    fdata_offset,
                    compressed_size,
                    chunk_seq,
                    checksum_group,
                    compressed: false,
                    file_index,
                    uncompressed_size,
                };

                // Send chunk to the decompressor
                tx.send((meta, chunk_index as u32)).unwrap();  // You can switch to `ChunkWork { meta, raw_data }` if needed
                inflight_chunks += 1;
            }
        }


        // Reader thread cleanup
        log::debug!("[reader] Reader thread done about to drain writer returning chunks ");

        // Wait for all inflight chunks to return before finishing
        while inflight_chunks > 0 {
            match done_rx.recv() {
                Ok(returned) => {
                    log::debug!("[reader] Returned chunk {} to pool during draining", returned);
                    revolver.return_chunk(returned);
                    inflight_chunks -= 1;
                }
                Err(_) => {
                    log::debug!("[reader] rx_done channel closed, exiting draining loop");
                    break;
                }
            }
        }

    });



    // DECOMPRESSOR
    let mut decompressor_threads = Vec::with_capacity(config.max_core_in_compress as u8 as usize);

    for _ in 0..config.max_core_in_compress {
        let rx = work_rx.clone();
        let tx = chunk_tx.clone();
        let done_tx = done_tx.clone(); // ✅ klona in
        let base_ptr = SendPtr::new(base_ptr.as_ptr()); // create new SendPtr for each thread

        let handle = thread::spawn(move || unsafe {

            while let Ok((chunk_meta, chunk_nr)) = rx.recv() {
                log::debug!("[Decompressor] got chunk_nr {}",chunk_nr);
                let data = get_chunk_slice(
                    base_ptr.as_ptr(),
                    chunk_size,
                    chunk_nr as u32,
                    chunk_meta.compressed_size as usize,
                );

                match decompress_chunk_stream(&data) {
                    Ok(decompressed) => {
                        let mut hasher = Hasher::new();
                        hasher.update(&decompressed);
                        tx.send((chunk_meta, decompressed)).unwrap();
                        done_tx.send(chunk_nr as u64).unwrap(); // ✅ viktigt
                    }
                    Err(e) => {
                        eprintln!("Decompression failed: {}", e);
                        done_tx.send(chunk_nr as u64).unwrap(); // ✅ viktigt
                    }
                }
            }
        });
        decompressor_threads.push(handle);
    }


    // [WRITER]
    let writer_thread=thread::spawn(move || {
        while let Ok((chunk_meta, data)) = rx.recv() {
            let path = out_dir_cloned.join(format!("file_{}", chunk_meta.file_index));

            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }

            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .open(&path)
                .unwrap();

            f.seek(SeekFrom::Start(chunk_meta.fdata_offset)).unwrap();
            f.write_all(&data).unwrap();

        }
    });

    reader_thread.join();

    for handle in  decompressor_threads  {
         handle.join();
    }

    writer_thread.join();

    Ok(report)
}

pub fn extract_file_checksums_from_metadata(schema: &SchemaRef) -> Result<Vec<[u8; 32]>> {
    let metadata: &HashMap<String, String> = schema.metadata();
    let mut checksums: Vec<[u8; 32]> = Vec::new();

    let mut sorted_keys: Vec<_> = metadata.keys()
        .filter(|k| k.starts_with("checksum_"))
        .collect();

    // Sort by numerical suffix to preserve order
    sorted_keys.sort_by_key(|k| {
        k.trim_start_matches("checksum_")
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

pub fn decompress_chunk_stream(input: &[u8]) -> Result<Vec<u8>> {
    unsafe {
        let dctx = ZSTD_createDCtx();
        if dctx.is_null() {
            return Err(anyhow::anyhow!("Failed to create decompression context"));
        }

        let mut dst_buf = vec![0u8; CONFIG.zstd_output_buffer_size];
        let mut output = Vec::new();

        let mut input_buffer = ZSTD_inBuffer {
            src: input.as_ptr() as *const _,
            size: input.len(),
            pos: 0,
        };

        let mut output_buffer = ZSTD_outBuffer {
            dst: dst_buf.as_mut_ptr() as *mut _,
            size: dst_buf.len(),
            pos: 0,
        };

        while input_buffer.pos < input_buffer.size {
            output_buffer.pos = 0;

            let remaining = ZSTD_decompressStream(dctx, &mut output_buffer, &mut input_buffer);
            if ZSTD_isError(remaining) != 0 {
                let err = ZSTD_getErrorName(remaining);
                let err_str = std::ffi::CStr::from_ptr(err).to_string_lossy().into_owned();
                ZSTD_freeDCtx(dctx);
                return Err(anyhow::anyhow!("ZSTD_decompressStream error: {}", err_str));
            }

            output.extend_from_slice(&dst_buf[..output_buffer.pos]);
        }

        ZSTD_freeDCtx(dctx);
        Ok(output)
    }
}
