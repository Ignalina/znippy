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
use arrow_array::{Array, BinaryArray, ListArray, StringArray, StructArray, UInt32Array, UInt64Array, UInt8Array};

use blake3::Hasher;
use crossbeam_channel::{bounded, unbounded, Receiver, Sender};

use crate::{common_config::CONFIG, index::read_znippy_index, index::VerifyReport, ChunkMeta, ChunkRevolver};

use crate::chunkrevolver::{get_chunk_slice, SendPtr,Chunk};

pub fn decompress_archive(index_path: &Path, save_data: bool, out_dir: &Path) -> Result<VerifyReport> {
    let zdata_path = index_path.with_extension("zdata");

    let (_schema, batches) = read_znippy_index(index_path)?;
    let file_checksums = extract_file_checksums(&batches)?;
    let file_checksums = Arc::new(file_checksums);


    let revolver = ChunkRevolver::new();
    let base_ptr = SendPtr::new(revolver.base_ptr());
    let chunk_size = revolver.chunk_size();



    let (work_tx, work_rx): (Sender<(ChunkMeta,u32)>, Receiver<(ChunkMeta,u32 )>) = bounded(CONFIG.max_core_in_flight);

    let (chunk_tx, chunk_rx): (Sender<(ChunkMeta, Vec<u8>)>, Receiver<_>) = bounded(CONFIG.max_core_in_flight);

    let (done_tx, done_rx): (Sender<u64>, Receiver<u64>) = unbounded();

    let chunk_counts = Arc::new(Mutex::new(HashMap::new()));

    let out_dir = Arc::new(out_dir.to_path_buf());
    let mut report = VerifyReport::default();

    let rx = chunk_rx.clone();
    let out_dir_cloned = Arc::clone(&out_dir);
    let file_checksums_cloned = Arc::clone(&file_checksums);
    let cc_cloned = Arc::clone(&chunk_counts);
    let done_tx_cloned = done_tx.clone();



// READER

        let cc = Arc::clone(&chunk_counts);
        let tx = work_tx.clone();
        let done_rx = done_rx.clone();

    thread::spawn(move || {
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

        for file_index in 0..file_count as u64 {
            let start = chunk_offsets[file_index as usize] as usize;
            let end = chunk_offsets[file_index as usize  + 1] as usize;
            let n_chunks = (end - start) as u16;
            cc.lock().unwrap().insert(file_index, n_chunks);

            for local_idx in 0..n_chunks {



                while let Ok(returned) = done_rx.try_recv() {
                    log::debug!("[reader] Returned chunk {} to pool", returned);
                    revolver.return_chunk(returned);
                    inflight_chunks = inflight_chunks.checked_sub(1).expect("inflight_chunks underflow");
                }

                let mut chunk = revolver.get_chunk();
                let chunk_index = chunk.index;

                let global_idx = start + local_idx as usize;

                let zdata_offset = zdata_offsets.value(global_idx);
                let fdata_offset = fdata_offsets.value(global_idx);
                let compressed_size = lengths.value(global_idx);
                let chunk_seq = chunk_seqs.value(global_idx);
                let checksum_group = checksum_groups.value(global_idx);
                let fdata_offset = fdata_offsets.value(global_idx);
                let uncompressed_size = uncompressed_size_arr.value(file_index as usize);

                zdata_file.seek(SeekFrom::Start(zdata_offset)).unwrap();
//                let compressed_len = compressed_size as usize;
                zdata_file.read_exact(&mut chunk[..compressed_size as usize]).unwrap();

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

                tx.send(( meta, chunk_index as u32)).unwrap();  // du kan byta till `ChunkWork { meta, raw_data }` om du vill senare
            }

            done_rx.recv().unwrap();
        }
    });


//    match rx_chunk.recv() {
//        Ok((file_index,fdata_offset, chunk_nr, length, skip)) => {
//            log::debug!("[compressor] Processing chunk {} from file {}: {} bytes", chunk_nr, file_index, length);
//            let input = get_chunk_slice(base_ptr.as_ptr(), chunk_size, chunk_nr as u32, length as usize);

    // DECOMPRESSOR
    let mut decompressor_threads = Vec::with_capacity(CONFIG.max_core_in_compress as u8 as usize);

    for _ in 0..CONFIG.max_core_in_compress {
        let rx = work_rx.clone();
        let tx = chunk_tx.clone();
        let base_ptr = SendPtr::new(base_ptr.as_ptr()); // create new SendPtr for each thread

        let handle = thread::spawn(move || unsafe {

            while let Ok((chunk_meta, chunk_nr)) = rx.recv() {
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
                    }
                    Err(e) => {
                        eprintln!("Decompression failed: {}", e);
                    }
                }
            }
        });
        decompressor_threads.push(handle);
    }


    // [WRITER]
    thread::spawn(move || {
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

            done_tx_cloned.send(chunk_meta.file_index).unwrap();
        }
    });
    Ok(report)
}

fn extract_file_checksums(batches: &[arrow::record_batch::RecordBatch]) -> Result<Vec<[u8; 32]>> {
    let mut all = vec![];
    for batch in batches {
        let arr = batch.column_by_name("checksum")
            .context("Missing 'checksum' column")?
            .as_any().downcast_ref::<BinaryArray>()
            .context("'checksum' is not BinaryArray")?;

        for row in 0..arr.len() {
            if arr.is_null(row) {
                all.push([0u8; 32]);
            } else {
                let bytes = arr.value(row);
                let mut fixed = [0u8; 32];
                fixed[..bytes.len().min(32)].copy_from_slice(&bytes[..bytes.len().min(32)]);
                all.push(fixed);
            }
        }
    }
    Ok(all)
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
