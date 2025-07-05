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
use arrow::array::{BinaryArray, ListArray, StringArray, StructArray, UInt64Array};
use arrow_array::Array;
use blake3::Hasher;
use crossbeam_channel::{bounded, Receiver, Sender};

use crate::{
    common_config::CONFIG,
    index::read_znippy_index,
    index::VerifyReport,
};

pub fn decompress_archive(index_path: &Path, save_data: bool, out_dir: &Path) -> Result<VerifyReport> {
    let (_schema, batches) = read_znippy_index(index_path)?;
    let file_checksums = extract_file_checksums(&batches)?;
    let file_checksums = Arc::new(file_checksums);

    let zdata_path = index_path.with_extension("zdata");
    let zdata_file = Arc::new(Mutex::new(File::open(&zdata_path)?));

    let (work_tx, work_rx): (Sender<(usize, u16, Vec<u8>)>, Receiver<_>) = bounded(CONFIG.max_core_in_flight);
    let (chunk_tx, chunk_rx): (Sender<(usize, u16, Vec<u8>, [u8; 32])>, Receiver<_>) = bounded(CONFIG.max_core_in_flight);
    let (done_tx, done_rx): (Sender<usize>, Receiver<usize>) = bounded(8);
    let chunk_counts = Arc::new(Mutex::new(HashMap::new()));

    let out_dir = Arc::new(out_dir.to_path_buf());
    let mut report = VerifyReport::default();

    // 📍 Writer tråd – flyttad utanför batch-loopen
    let rx = chunk_rx.clone();
    let out_dir_cloned = Arc::clone(&out_dir);
    let file_checksums_cloned = Arc::clone(&file_checksums);
    let cc_cloned = Arc::clone(&chunk_counts);
    let done_tx_cloned = done_tx.clone();

    thread::spawn(move || {
        let mut per_file_chunks: HashMap<usize, HashMap<u16, Vec<u8>>> = HashMap::new();
        let mut per_file_partial: HashMap<usize, HashMap<u16, [u8; 32]>> = HashMap::new();

        while let Ok((file_index, chunk_index, data, checksum)) = rx.recv() {
            let entry = per_file_chunks.entry(file_index).or_default();
            entry.insert(chunk_index, data);

            let entry_chk = per_file_partial.entry(file_index).or_default();
            entry_chk.insert(chunk_index, checksum);

            let expected = cc_cloned.lock().unwrap().get(&file_index).cloned().unwrap_or(0);
            if entry.len() == expected as usize {
                let mut all_chunks = vec![];
                for i in 0..expected {
                    all_chunks.extend(entry.remove(&i).unwrap());
                }

                let mut hasher = Hasher::new();
                for i in 0..expected {
                    let part = entry_chk.remove(&i).unwrap();
                    hasher.update(&part);
                }

                let calculated = hasher.finalize();
                let expected_bytes = &file_checksums_cloned[file_index];

                if expected_bytes == calculated.as_bytes() {
                    if save_data {
                        let path = out_dir_cloned.join(format!("file_{}", file_index));
                        if let Some(parent) = path.parent() {
                            std::fs::create_dir_all(parent).unwrap();
                        }
                        let mut f = OpenOptions::new().create(true).write(true).truncate(true).open(&path).unwrap();
                        f.write_all(&all_chunks).unwrap();
                    }
                } else {
                    eprintln!("❌ Checksum mismatch for file index {}", file_index);
                    eprintln!("  expected: {:x?}", expected_bytes);
                    eprintln!("  calculated: {:x?}", calculated.as_bytes());
                }

                done_tx_cloned.send(file_index).unwrap();
            }
        }
    });

    for batch in batches {
        let file_count = batch.num_rows();
        let paths = batch.column_by_name("relative_path").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        let chunks_array = batch.column_by_name("chunks").unwrap().as_any().downcast_ref::<ListArray>().unwrap();
        let struct_array = chunks_array.values().as_any().downcast_ref::<StructArray>().unwrap();
        let offsets_arr = struct_array.column_by_name("offset").unwrap().as_any().downcast_ref::<UInt64Array>().unwrap();
        let lengths_arr = struct_array.column_by_name("length").unwrap().as_any().downcast_ref::<UInt64Array>().unwrap();
        let chunk_offsets = chunks_array.value_offsets();

        let cc = Arc::clone(&chunk_counts);
        let tx = work_tx.clone();
        let file = Arc::clone(&zdata_file);
        let done_rx = done_rx.clone();

        thread::scope(|s| {
            s.spawn(move || {
                for file_index in 0..file_count {
                    let start = chunk_offsets[file_index] as usize;
                    let end = chunk_offsets[file_index + 1] as usize;
                    let n_chunks = (end - start) as u16;
                    cc.lock().unwrap().insert(file_index, n_chunks);

                    for local_idx in 0..n_chunks {
                        let global_idx = start + local_idx as usize;
                        let offset = offsets_arr.value(global_idx);
                        let length = lengths_arr.value(global_idx);

                        let mut buf = vec![0u8; length as usize];
                        {
                            let mut f = file.lock().unwrap();
                            f.seek(SeekFrom::Start(offset)).unwrap();
                            f.read_exact(&mut buf).unwrap();
                        }

                        tx.send((file_index, local_idx, buf)).unwrap();
                    }

                    done_rx.recv().unwrap();
                }
            });

            for _ in 0..CONFIG.max_core_in_compress {
                let rx = work_rx.clone();
                let tx = chunk_tx.clone();
                s.spawn(move || {
                    while let Ok((file_index, chunk_idx, data)) = rx.recv() {
                        match decompress_chunk_stream(&data) {
                            Ok(decompressed) => {
                                let mut hasher = Hasher::new();
                                hasher.update(&decompressed);
                                let checksum = hasher.finalize().into();
                                tx.send((file_index, chunk_idx, decompressed, checksum)).unwrap();
                            }
                            Err(e) => {
                                eprintln!("Decompression failed: {}", e);
                            }
                        }
                    }
                });
            }
        });
    }

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
