use std::ffi::CStr;
use zstd_sys_rs::*;
use anyhow::{anyhow};

use std::ffi::c_void;
use std::ptr;
use zstd_sys_rs::*;

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
};
use std::any::{type_name, Any};
use anyhow::{Context, Result};
use arrow::datatypes::SchemaRef;
use arrow::ipc::RecordBatch;
use arrow_array::{Array, BinaryArray, BooleanArray, Datum, ListArray, StringArray, StructArray, UInt32Array, UInt64Array, UInt8Array};

use blake3::Hasher;
use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use hex::FromHex;
use crate::{common_config::CONFIG, extract_config_from_arrow_metadata, index::read_znippy_index, index::VerifyReport, ChunkMeta, ChunkRevolver};

use crate::chunkrevolver::{get_chunk_slice, SendPtr,Chunk};
use arrow_array::{ArrayRef};

pub fn decompress_archive(index_path: &Path, save_data: bool, out_dir: &Path) -> Result<VerifyReport> {
    let zdata_path = index_path.with_extension("zdata");

    let (schema, batches) = read_znippy_index(index_path)?;
    let file_checksums = extract_file_checksums_from_metadata(&schema);
    let config = &CONFIG;
    //extract_config_from_arrow_metadata(schema.metadata())?;
    log::debug!("read config from meta {:?}\n and checksums {:?}", config,file_checksums);

    let batch = Arc::new(batches[0].clone()); // ✅ clone är shallow – delar data internt
    let batch_cloned_for_writer = Arc::clone(&batch);


    let mut revolver = ChunkRevolver::new(&config);
    let base_ptrs = revolver.base_ptrs();
    let chunk_size = revolver.chunk_size();


    //    let (work_tx, work_rx): (Sender<(ChunkMeta,u8,u32)>, Receiver<(ChunkMeta,u8,u32 )>) = bounded(config.max_core_in_flight);


    // work: skickas från reader → decompressors
    let (work_tx_array, work_rx_array): (
        Vec<Sender<(ChunkMeta, u8, u32)>>,
        Vec<Receiver<(ChunkMeta, u8, u32)>>
    ) = (0..CONFIG.max_core_in_flight)
        .map(|_| bounded(CONFIG.max_chunks as usize))
        .unzip();


    let (tx_return, rx_return): (Sender<(u8, u64)>, Receiver<(u8, u64)>) = unbounded();

    // chunk: skickar decompressor til writer
    let (chunk_tx, chunk_rx): (Sender<(ChunkMeta, Vec<u8>)>, Receiver<_>) = bounded(config.max_core_in_flight);


    let out_dir = Arc::new(out_dir.to_path_buf());

    let chunk_rx_cloned = chunk_rx.clone();
    let out_dir_cloned = Arc::clone(&out_dir);


    // READER
     let reader_thread = {
         let done_rx = rx_return.clone();

         let work_tx_array = work_tx_array.clone();

    thread::spawn(move || -> ReaderStats  {
        let mut inflight_chunks = 0usize;

        let mut zdata_file = File::open(&zdata_path).expect("Failed to open .zdata file");

        let Some(batch) = batches.get(0) else {
            eprintln!("❌ No batch found in index");
            return ReaderStats { total_files: 0, skipped_files: 0 };
        };

        let total_files = batch.num_rows(); // Get file_count here

        // Fetch columns only once for later use
        let paths = batch
            .column_by_name("relative_path").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();

        let chunks_array = batch
            .column_by_name("chunks").unwrap()
            .as_any().downcast_ref::<ListArray>().unwrap();

        let struct_array = chunks_array.values().as_any().downcast_ref::<StructArray>().unwrap();

        let uncompressed_size_arr = batch
            .column_by_name("uncompressed_size")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let chunk_offsets = chunks_array.value_offsets(); // Get chunk offsets once before the loop

        // Iterate over files, using Arrow's ListArray to access chunks directly
        for file_index in 0..total_files as u64 {

            // Ensure we access the correct chunk array for the current file
            let chunks_array_for_file = chunks_array.value(file_index as usize); // This gives us the StructArray for the current file

            // Check if we are dealing with a StructArray
            if let Some(struct_array) = chunks_array_for_file.as_any().downcast_ref::<StructArray>() {
                // Define the number of chunks
                let n_chunks = struct_array.len() as u16; // Number of chunks in this file

                for local_idx in 0..n_chunks {
                    // Access the individual child arrays (fields) within the StructArray
                    let zdata_offset_arr = struct_array.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
                    let fdata_offset_arr = struct_array.column(1).as_any().downcast_ref::<UInt64Array>().unwrap();
                    let length_arr = struct_array.column(2).as_any().downcast_ref::<UInt64Array>().unwrap();
                    let chunk_seq_arr = struct_array.column(3).as_any().downcast_ref::<UInt32Array>().unwrap();
                    let checksum_group_arr = struct_array.column(4).as_any().downcast_ref::<UInt8Array>().unwrap();

                    // Access values for the current chunk at local_idx
                    let zdata_offset = zdata_offset_arr.value(local_idx as usize);
                    let fdata_offset = fdata_offset_arr.value(local_idx as usize);
                    let compressed_size = length_arr.value(local_idx as usize);
                    let chunk_seq = chunk_seq_arr.value(local_idx as usize);
                    let checksum_group = checksum_group_arr.value(local_idx as usize);

                    log::debug!("[reader] reading file {} chunk {:?}",paths.value(file_index as usize),(zdata_offset, fdata_offset, compressed_size, chunk_seq, checksum_group));

                    // Try to get a chunk – if none are available, wait for one to be returned
                    let mut chunk_data: Chunk = loop {
                        match revolver.try_get_chunk() {
                            Some(c) => {
                                break c
                            }
                            None => {
                                // Block until a chunk is returned
                                let (thread_nr, returned) = done_rx.recv().expect("rx_done channel closed unexpectedly");
                                log::debug!("[reader] Blocking wait — returned chunk {} from thread nr {} to pool", returned,thread_nr);
                                revolver.return_chunk(thread_nr, returned);
                                inflight_chunks = inflight_chunks.checked_sub(1).expect("inflight_chunks underflow");
                            }
                        }
                    };

                    let uncompressed_size = uncompressed_size_arr.value(file_index as usize);

                    // Extract the "compressed" status for the chunk from the file metadata
                    let compressed = batch
                        .column_by_name("compressed")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .value(file_index as usize);

                    // Read and process the chunk data from zdata file
                    zdata_file.seek(SeekFrom::Start(zdata_offset)).unwrap();
                    zdata_file.read_exact(&mut chunk_data[..compressed_size as usize]).unwrap();

                    // Prepare metadata for chunk
                    let meta = ChunkMeta {
                        zdata_offset,
                        fdata_offset,
                        compressed_size,
                        chunk_seq,
                        checksum_group,
                        compressed, // Use the value from the metadata
                        file_index,
                        uncompressed_size,
                    };

                    // Send chunk to the decompressor
                    work_tx_array[chunk_data.ring_nr as usize].send((meta, chunk_data.ring_nr, chunk_data.index as u32)).unwrap();
                    inflight_chunks += 1;
                }
            } else {
                log::debug!("❌ The chunks array is not a StructArray.");
            }
        }

        // Reader thread cleanup
        log::debug!("[reader] Thread done about to drain compressor returning chunks ");

        // Wait for all inflight chunks to return before finishing
        while inflight_chunks > 0 {
            log::debug!("[reader] draining inflight_chunks amount = {}", inflight_chunks);

            match done_rx.recv() {
                Ok((thread_nr, returned)) => {
                    log::debug!("[reader] Returned chunk {} to pool during draining", returned);
                    revolver.return_chunk(thread_nr, returned);
                    inflight_chunks -= 1;
                }
                Err(_) => {
                    log::debug!("[reader] rx_done channel closed, exiting draining loop");
                    break;
                }
            }
        }

        log::debug!("[reader] Drain done ");
        work_tx_array.into_iter().for_each(drop);
        log::debug!("[reader] tx_work dropped after finishing all chunk sends");
        drop(done_rx);
        drop(revolver);

        ReaderStats {
            total_files,
            skipped_files: 0 as usize,
        }

    })
};

// DECOMPRESSOR
    let mut decompressor_threads: Vec<JoinHandle<Result<()>>> =
        Vec::with_capacity(config.max_core_in_flight as usize);
    let rx_array = work_rx_array.clone();


    for decompressor_nr in 0..config.max_core_in_flight as u8 {
        let base_ptr: SendPtr = base_ptrs[decompressor_nr as usize];
        let rx = rx_array[decompressor_nr as usize].clone();
        let tx = chunk_tx.clone();
        let done_tx = tx_return.clone(); // ✅ klona in
        let handle = thread::spawn(move || unsafe {
            let raw_ptr = base_ptr.as_ptr();


            loop {
                match rx.recv() {
                    Ok((chunk_meta, ring_nr, chunk_nr)) => {
                        log::debug!("[Decompressor {}] got chunk_nr {}", decompressor_nr, chunk_nr);

                        let data = get_chunk_slice(
                            raw_ptr,
                            chunk_size,
                            chunk_nr,
                            chunk_meta.compressed_size as usize,
                        );

                        let chunk_org_size = chunk_meta.uncompressed_size;
                        let chunk_seq = chunk_meta.chunk_seq;
                        let file_index = chunk_meta.file_index;

                        // skydd mot panik
                        let decompress_result = std::panic::catch_unwind(|| {
                            if chunk_meta.compressed {
                                decompress2_microchunk(&data)
                            } else {
                                Ok(data.to_vec())
                            }
                        });

                        match decompress_result {
                            Ok(Ok(decompressed)) => {
                                log::debug!(
                        "Decompression successful chunk_nr {} ({} bytes)",
                        chunk_nr, decompressed.len()
                    );
                                if let Err(e) = tx.send((chunk_meta, decompressed)) {
                                    log::error!("[Decompressor {}] tx.send failed: {}", decompressor_nr, e);
                                }
                            }
                            Ok(Err(e)) => {
                                log::error!(
                        "Decompression failed: file_index {} chunk_nr {} uncompressed={} compressed={} chunk_seq={} error={}",
                        file_index, chunk_nr, chunk_org_size, data.len(), chunk_seq, e
                    );
                            }
                            Err(_) => {
                                log::error!(
                        "PANIC: decompress2_microchunk panicked! file_index {} chunk_nr {} chunk_seq={}",
                        file_index, chunk_nr, chunk_seq
                    );
                            }
                        }

                        // ✅ Alltid returnera chunk – oavsett vad som gick fel
                        if let Err(e) = done_tx.send((decompressor_nr, chunk_nr as u64)) {
                            log::warn!(
                    "[Decompressor {}] done_tx failed (chunk_nr {}): {}",
                    decompressor_nr, chunk_nr, e
                );
                        }
                    }
                    Err(_) => {
                        log::debug!(
                "[Decompressor {}] rx channel closed, exiting thread",
                decompressor_nr
            );
                        break;
                    }
                }
            }

            drop(tx);
            drop(done_tx);
            drop(rx);
            log::debug!("[compressor] Decompressor thread finished processing.");
            log::info!("📦 Decompressor thread/group {} returning ",decompressor_nr);
            Ok(())
        });
        decompressor_threads.push(handle);
    }


    // [WRITER]
    let writer_thread=thread::spawn(move || -> WriterStats  {
        let mut total_chunks = 0u64;
        let mut total_written_bytes = 0u64;

        let mut open_files: HashMap<u64, File> = HashMap::new();
        let mut current_open = 0usize;
        let mut peak_open = 0usize;

        while let Ok((chunk_meta, data)) = chunk_rx_cloned.recv() {



            let col = batch_cloned_for_writer
                .column_by_name("relative_path")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let rel_path = col.value(chunk_meta.file_index.try_into().unwrap()); // &str

            let full_path = out_dir_cloned.join(rel_path); // → PathBuf

            log::debug!("[Writer] got file_index {} index file name  {} transposed to fullpath={:?}", chunk_meta.file_index, rel_path,full_path);

            if let Some(parent) = full_path.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }




            let file = open_files.entry(chunk_meta.file_index).or_insert_with(|| {
                let rel_path = col.value(chunk_meta.file_index as usize);
                let full_path = out_dir_cloned.join(rel_path);

                if let Some(parent) = full_path.parent() {
                    std::fs::create_dir_all(parent).unwrap();
                }


                let f = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&full_path)
                    .unwrap();


                current_open += 1;
                if current_open > peak_open {
                    peak_open = current_open;
                }

                f
            });

            file.seek(SeekFrom::Start(chunk_meta.fdata_offset)).unwrap();
/*
            assert_eq!(
                data.len(),
                chunk_meta.uncompressed_size as usize,
                "[Writer] ❌ Decompressed size mismatch: expected {}, got {} (file_index {}, chunk_seq {})",
                chunk_meta.uncompressed_size,
                data.len(),
                chunk_meta.file_index,
                chunk_meta.chunk_seq,
            );
*/
            file.write_all(&data).unwrap();

            total_chunks += 1;
            total_written_bytes += data.len() as u64;
        }

        for (_, file) in open_files {
            drop(file);
            current_open -= 1;
        }
        WriterStats {
            total_chunks,
            total_written_bytes,
        }
    });
    let reader_stats = reader_thread.join().expect("reader_thread panicked");
    log::debug!("[reader] reader_thread joined");
    work_tx_array.into_iter().for_each(drop);
    log::debug!("[reader] tx_chunk dropped after reader thread finished");

    drop(tx_return);


    for handle in  decompressor_threads  {
         handle.join();
    }
    drop(chunk_tx);
    let writer_stats=writer_thread.join().expect("writher_thread panicked");

    let report = VerifyReport {
        total_files: reader_stats.total_files,
        verified_files: 0,
        corrupt_files: 0,
        total_bytes: writer_stats.total_written_bytes,
        verified_bytes: 0,
        corrupt_bytes: 0,
        chunks: writer_stats.total_chunks
    };

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
use zstd_sys_rs::*;
use std::slice;
use std::thread::JoinHandle;
use log::debug;
use crate::common_config::StrategicConfig;
use crate::meta::{ReaderStats, WriterStats};

/// Decompress a complete microchunk into the provided `dst` buffer.
/// Assumes `src` contains a *complete* ZSTD stream (as produced with `ZSTD_e_end`)
pub fn decompress_chunk_stream(
    dctx: *mut ZSTD_DCtx,
    dst: &mut [u8],
    src: &[u8],
) -> Result<usize, String> {
    unsafe {
        let mut input = ZSTD_inBuffer {
            src: src.as_ptr() as *const c_void,
            size: src.len(),
            pos: 0,
        };
        let mut output = ZSTD_outBuffer {
            dst: dst.as_mut_ptr() as *mut c_void,
            size: dst.len(),
            pos: 0,
        };

        loop {
            let remaining = ZSTD_decompressStream(dctx, &mut output, &mut input);
            if ZSTD_isError(remaining) != 0 {
                let msg = ZSTD_getErrorName(remaining);
                let cstr = std::ffi::CStr::from_ptr(msg);
                return Err(format!("ZSTD_decompressStream error: {}", cstr.to_string_lossy()));
            }

            if input.pos == input.size && remaining == 0 {
                // All input consumed and stream ended
                break;
            }

            if input.pos == input.size && output.pos == 0 {
                return Err("ZSTD_decompressStream flush error: Operation made no progress over multiple calls, due to input being empty".into());
            }
        }

        Ok(output.pos)
    }
}

pub fn decompress_chunk_stream2(input: &[u8]) -> Result<Vec<u8>> {
    unsafe {
        let dctx = ZSTD_createDCtx();
        if dctx.is_null() {
            return Err(anyhow!("Failed to create decompression context"));
        }

        let mut dst_buf = vec![0u8; CONFIG.zstd_output_buffer_size];
        let mut output = Vec::new();

        let mut input_buffer = ZSTD_inBuffer {
            src: input.as_ptr() as *const _,
            size: input.len(),
            pos: 0,
        };

        let mut output_buffer = ZSTD_outBuffer {
            dst: dst_buf.as_mut_ptr() as *mut c_void,
            size: dst_buf.len(),
            pos: 0,
        };

        const MAX_RETRIES: usize = 10;
        let mut retries = 0;

        while input_buffer.pos < input_buffer.size {
            output_buffer.pos = 0;

            let remaining = ZSTD_decompressStream(dctx, &mut output_buffer, &mut input_buffer);

            if ZSTD_isError(remaining) != 0 {
                let err = ZSTD_getErrorName(remaining);
                let err_str = CStr::from_ptr(err).to_string_lossy().into_owned();
                ZSTD_freeDCtx(dctx);
                return Err(anyhow!("ZSTD_decompressStream error: {}", err_str));
            }

            if remaining > 0 {
                // Should not happen – stream not finished
                ZSTD_freeDCtx(dctx);
                return Err(anyhow!("ZSTD_decompressStream error: Data left to decompress"));
            }

            if input_buffer.pos == input_buffer.size && output_buffer.pos == 0 {
                ZSTD_freeDCtx(dctx);
                return Err(anyhow!("ZSTD_decompressStream flush error: Operation made no progress over multiple calls, due to input being empty"));
            }

            if output_buffer.pos > 0 {
                output.extend_from_slice(&dst_buf[..output_buffer.pos]);
            }

            // Handle buffer-too-small error (should never happen)
            if retries > MAX_RETRIES {
                ZSTD_freeDCtx(dctx);
                return Err(anyhow!("Too many retries due to buffer size"));
            }
        }

        ZSTD_freeDCtx(dctx);
        Ok(output)
    }
}
pub unsafe fn decompress_chunk_once(
    dctx: *mut ZSTD_DCtx,
    src: &[u8],
    config: &StrategicConfig,
) -> Result<Vec<u8>> {
    let mut dst = Vec::with_capacity(config.zstd_output_buffer_size);
    let mut output_buf = vec![0u8; config.zstd_output_buffer_size];

    let mut input = ZSTD_inBuffer {
        src: src.as_ptr() as *const _,
        size: src.len(),
        pos: 0,
    };

    while input.pos < input.size {
        let mut output = ZSTD_outBuffer {
            dst: output_buf.as_mut_ptr() as *mut _,
            size: output_buf.len(),
            pos: 0,
        };

        let code = ZSTD_decompressStream(dctx, &mut output, &mut input);
        if ZSTD_isError(code) != 0 {
            let msg = std::ffi::CStr::from_ptr(ZSTD_getErrorName(code));
            return Err(anyhow!("ZSTD_decompressStream error: {}", msg.to_string_lossy()));
        }

        if output.pos > 0 {
            dst.extend_from_slice(&output_buf[..output.pos]);
        }

        // säkerhetsbrytare för toma svar
        if input.pos == input.size && output.pos == 0 && code > 0 {
            return Err(anyhow!("ZSTD_decompressStream flush error: no progress made"));
        }

        if code == 0 {
            break; // ✅ flushed and complete
        }
    }

    Ok(dst)
}

unsafe fn decompress_chunk_once2(dctx: *mut ZSTD_DCtx, src: &[u8], config_decompressor: &StrategicConfig) -> Result<Vec<u8>> {
    let output_capacity = config_decompressor.zstd_output_buffer_size; // t.ex. 10 MB
    let mut dst = vec![0u8; output_capacity];

    let mut input = ZSTD_inBuffer {
        src: src.as_ptr() as *const _,
        size: src.len(),
        pos: 0,
    };

    let mut output = ZSTD_outBuffer {
        dst: dst.as_mut_ptr() as *mut _,
        size: dst.len(),
        pos: 0,
    };

    let code = unsafe { ZSTD_decompressStream(dctx, &mut output, &mut input) };
    if ZSTD_isError(code) != 0 {

        return Err(anyhow!("ZSTD_decompressStream error: {:?}", ZSTD_getErrorName(code)));
    }

    if input.pos < input.size {
        return Err(anyhow!("ZSTD_decompressStream error: Data left to decompress"));
    }

    // Trunkera till faktiska decompressed storlek
    dst.truncate(output.pos);
    Ok(dst)
}


pub fn decompress2_microchunk(input: &[u8]) -> Result<Vec<u8>> {
    unsafe {
        let size = ZSTD_getFrameContentSize(input.as_ptr() as *const _, input.len());

        if size == ZSTD_CONTENTSIZE_ERROR as u64 {
            return Err(anyhow!("Not a valid zstd frame"));
        }
        if size == ZSTD_CONTENTSIZE_UNKNOWN as u64 {
            return Err(anyhow!("Unknown content size"));
        }

        let expected_size = size as usize;
        let mut output = vec![0u8; expected_size];

        let written = ZSTD_decompress(
            output.as_mut_ptr() as *mut _,
            output.len(),
            input.as_ptr() as *const _,
            input.len(),
        );

        if ZSTD_isError(written) != 0 {
            let err = ZSTD_getErrorName(written);
            let err_str = CStr::from_ptr(err).to_string_lossy();
            return Err(anyhow!("ZSTD_decompress error: {}", err_str));
        }

        output.truncate(written);
        Ok(output)
    }
}

