use zstd_sys::*;
use crate::packer::ZSTD_EndDirective::ZSTD_e_end;
use anyhow::Result;
use anyhow::anyhow;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::Shutdown::Write as OtherWrite;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use arrow::array::Array;
use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use walkdir::WalkDir;
use zstd_sys::*;
use blake3::Hasher; // Blake3 import
use znippy_common::chunkrevolver::{ChunkRevolver, Chunk, SendPtr, get_chunk_slice};
use znippy_common::common_config::CONFIG;
use znippy_common::{attach_metadata, build_arrow_batch_from_files, split_into_microchunks, CompressionReport, FileMeta};
use znippy_common::meta::{ChunkMeta, WriterStats};
use znippy_common::index::{build_arrow_metadata_for_checksums_and_config, should_skip_compression};
use zstd_sys::ZSTD_cParameter::{ZSTD_c_compressionLevel, ZSTD_c_nbWorkers};
use zstd_sys::ZSTD_ResetDirective::ZSTD_reset_session_only;
use arrow::ipc::writer::{FileWriter, IpcWriteOptions};
use arrow::ipc::MetadataVersion;
fn strip_prefix<'a>(base: &'a Path, full: &'a Path) -> PathBuf {
    full.strip_prefix(base).unwrap_or(full).to_path_buf()
}
pub fn compress_dir(input_dir: &PathBuf, output: &PathBuf, no_skip: bool) -> anyhow::Result<CompressionReport> {
    log::debug!("Reading directory: {:?}", input_dir);
    let mut total_dirs = 0;
    let mut filesTOSkip=0;
    let mut filesTOCompress=0;


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
                if(skip) {
                    filesTOSkip=filesTOSkip+1;
                } else {
                    filesTOCompress=filesTOCompress+1;
                }
                Some(e.into_path())
            } else {
                None
            }
        })
        .collect());
    log::debug!("Found {} files include in {} directories. to compressed {} will skip {}", all_files.len(), total_dirs,filesTOCompress,filesTOSkip);

    let total_files:u64 = all_files.len() as u64;


    let (tx_chunk_array, rx_chunk_array): (
        Vec<Sender<(u64, u64, u8, u64, u64, bool)>>,
        Vec<Receiver<(u64, u64, u8, u64, u64, bool)>>
    ) = (0..CONFIG.max_core_in_flight)
        .map(|_| bounded(CONFIG.max_chunks as usize))
        .unzip();


    let (tx_compressed, rx_compressed): (Sender<(Arc<[u8]>,ChunkMeta)>, Receiver<(Arc<[u8]>,ChunkMeta)>) = unbounded();
    let (tx_return, rx_return): (Sender<(u8,u64)>, Receiver<(u8,u64)>) = unbounded();

    let output_zdata_path = output.with_extension("zdata");
    log::debug!("Creating zdata file at: {:?}", output_zdata_path);
    let zdata_file = OpenOptions::new().create(true).write(true).truncate(true).open(&output_zdata_path)?;
    let mut writer = BufWriter::with_capacity((CONFIG.file_split_block_size / 2) as usize, zdata_file);

    let mut revolver = ChunkRevolver::new(&CONFIG);
    let base_ptrs = revolver.base_ptrs();

    let chunk_size = revolver.chunk_size();

    let input_dir_cloned = input_dir.clone();

    // Reader Thread with inflight counter
    let all_files_for_reader = Arc::clone(&all_files);
    let all_files_for_writer = Arc::clone(&all_files);
    let reader_thread = {
        let tx_chunk_array = tx_chunk_array.clone();
        let rx_done = rx_return.clone();
        thread::spawn(move || {
            let mut inflight_chunks = 0usize;
            let mut uncompressed_files:u64 = 0;
            let mut uncompressed_bytes:u64 = 0;
            let mut compressed_files:u64=0;
            let mut compressed_bytes:u64=0;

            for (file_index, path) in all_files_for_reader.iter().enumerate() {
//                log::debug!("[reader] Handling file index {}: {:?}", file_index, path);

                let skip = !no_skip && should_skip_compression(path);
                if skip {
//                    log::debug!("[reader] Skipping compression for file {}", path.display());
                    uncompressed_files += 1; // Increment uncompressed files count
                    uncompressed_bytes += path.metadata().unwrap().len(); // Add to uncompressed bytes
                } else {
                    compressed_files += 1;
                    compressed_bytes += path.metadata().unwrap().len();
                }

                let file = match File::open(path) {
                    Ok(f) => f,
                    Err(e) => {
                        panic!("Problem opening the file: {:?}: {}", path, e)
                    }
                };
                let mut reader = BufReader::new(file);
                let mut has_read_any_data = false;
                let mut fdata_offset:u64=0;
                loop {
                    let maybe_chunk = revolver.try_get_chunk();

                    match maybe_chunk {
                        Some(mut chunk) => {

                            let ring_nr=chunk.ring_nr as usize;
                            match reader.read(&mut *chunk) {
                                Ok(0) => {
                                    if !has_read_any_data {
                                        tx_chunk_array[ring_nr].send((file_index as u64, fdata_offset, chunk.ring_nr,chunk.index, 0, skip)).unwrap();
                                        inflight_chunks += 1;
                                    } else {
                                        let ring_nr = chunk.ring_nr;
                                        let index = chunk.index;
                                        drop(chunk); // släpp lånet innan vi rör `revolver`
                                        revolver.return_chunk(ring_nr, index);
                                    }
                                    break;
                                }
                                Ok(bytes_read) => {
                                    has_read_any_data = true;
                                    tx_chunk_array[ring_nr].send((file_index as u64, fdata_offset, chunk.ring_nr,chunk.index, bytes_read as u64, skip)).unwrap();
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
                            let (ring_nr, returned) = rx_done.recv().expect("rx_done channel closed unexpectedly");
                            revolver.return_chunk(ring_nr, returned);
                            inflight_chunks = inflight_chunks.checked_sub(1).expect("inflight_chunks underflow");
                            continue;
                        }
                    }
                }

            }
            // Reader thread cleanup
            log::debug!("[reader] Reader thread done about to drain writer returning chunks ");

            // Wait for all inflight chunks to return before finishing
            while inflight_chunks > 0 {
                match rx_done.recv() {
                    Ok((ring_nr,returned)) => {
                        log::debug!("[reader] Returned chunk {} to pool during draining", returned);
                        revolver.return_chunk(ring_nr, returned);
                        inflight_chunks -= 1;
                    }
                    Err(_) => {
                        log::debug!("[reader] rx_done channel closed, exiting draining loop");
                        break;
                    }
                }
            }

            // Drop the sender side of the channel `tx_chunk` to signal that no more data will be sent
            tx_chunk_array.into_iter().for_each(drop);
            log::debug!("[reader] tx_chunk dropped after finishing all chunk sends");

            // Drop other resources after processing is complete
            drop(rx_done);  // Drop rx_done after we're done draining inflight chunks
            log::debug!("[reader] rx_done dropped after processing all chunks");

            drop(revolver);    // Drop revolver if needed (Rust will also drop it when the thread finishes)

            // Return the statistics to the main thread
            (uncompressed_files, uncompressed_bytes,compressed_files,compressed_bytes)})
    };

    // Compressor Threads pool
    // chunk_nr is index in the shared chunk array
    // chunk_index is a sequence within the file.


    let mut compressor_threads = Vec::with_capacity(CONFIG.max_core_in_flight as u8 as usize);
    for compressor_group in 0..CONFIG.max_core_in_flight as u8 {
        let rx_chunk = rx_chunk_array[compressor_group as usize].clone();

        let tx_compressed = tx_compressed.clone();
        let tx_ret = tx_return.clone();
        let base_ptr: SendPtr = base_ptrs[compressor_group as usize];

        let chunk_size = chunk_size;

        let handle = thread::spawn(move || {
            let raw_ptr = base_ptr.as_ptr();

            let mut local_chunkmeta: Vec<ChunkMeta> = Vec::new();
            let mut hasher = Hasher::new(); // Blake3 hash initialization
            let mut chunk_seq:u32=0;
            unsafe {
                let cctx = ZSTD_createCCtx();
                assert!(!cctx.is_null(), "ZSTD_createCCtx failed");

                ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, CONFIG.compression_level);
                ZSTD_CCtx_setParameter(cctx, ZSTD_c_nbWorkers, CONFIG.max_core_in_compress as i32);
                log::info!("[compressor] Compressor thread started with level {} and {} workers", CONFIG.compression_level, CONFIG.max_core_in_compress);

                loop {
                    match rx_chunk.recv() {
                        Ok((file_index, fdata_offset, ring_nr,chunk_nr, length, skip)) => {
                            log::debug!("[compressor] Processing chunk {} from file {}: {} bytes", chunk_nr, file_index, length);
                            let input = get_chunk_slice(raw_ptr, chunk_size, chunk_nr as u32, length as usize);
                            let input_ptr = input.as_ptr();
                            let chunk_meta;
                            let output: Arc<[u8]>;

                            // Hash the uncompressed data before compressing
                            hasher.update(&input);

                            if skip {
                                log::debug!("[compressor] Skipping compression for chunk {} of file {}", chunk_nr, file_index);
                                output = Arc::from(input);

                                chunk_meta = ChunkMeta {
                                    zdata_offset: 0,
                                    fdata_offset,
                                    file_index,
                                    chunk_seq,
                                    checksum_group: compressor_group,
                                    compressed_size: length,
                                    compressed: false,
                                    uncompressed_size: length,
                                };

                            //    log::debug!("[compressor] Sending chunk uncompressed chunk nr {} of file {} to writer", chunk_nr, file_index);
                                tx_compressed.send((output, chunk_meta)).unwrap();
                                chunk_seq += 1;
                            } else {
                                let micro_chunks=split_into_microchunks(input,CONFIG.zstd_output_buffer_size);

                                for (micro_nr, micro) in micro_chunks.iter().enumerate()  {

                                    let compressed_vec = compress2_microchunk(cctx, micro)?;
                                    let compressed_chunk: Arc<[u8]> = Arc::from(compressed_vec.into_boxed_slice());

                                    let chunk_meta = ChunkMeta {
                                        zdata_offset: 0, // to be set by writer
                                        fdata_offset,
                                        file_index,
                                        chunk_seq,
                                        checksum_group: compressor_group,
                                        compressed_size: compressed_chunk.len() as u64,
                                        compressed: true,
                                        uncompressed_size: micro.len() as u64,
                                    };
                                    log::debug!("[compressor {}] did File_index {} chunk nr {} size {} micro nr {} chunk size {} out {} ",compressor_group,file_index, chunk_nr,length,micro_nr,micro.len(),chunk_meta.compressed_size);

                                    tx_compressed.send((compressed_chunk, chunk_meta))?;
                                    chunk_seq += 1;
                                }
                            }
                      //      log::debug!("[compressor] Sending ACK on chunk_nr done  chunknr {} of file {} to reader", chunk_nr, file_index);
                            tx_ret.send((ring_nr, chunk_nr));

                        }
                        Err(_) => {
                            // Channel is closed, gracefully exit the loop
                            log::debug!("[compressor] rx_chunk channel closed, compressor exiting");
                            break;
                        }
                    }


                }

                ZSTD_freeCCtx(cctx);

                // Drop sender-side channels after the receiver finishes
                drop(tx_compressed);
                drop(tx_ret);

                // Finally, drop the receiver channel
                drop(rx_chunk);

                log::debug!("[compressor] Compressor thread finished processing.");

            }
            log::info!("📦 Compressor thread/group {} returning ",compressor_group);

            Ok(( compressor_group,*hasher.finalize().as_bytes()))
        });
        compressor_threads.push(handle);
    }

    // Writer thread
    let output = output.clone(); // klona innan writer_thread
    let writer_thread = thread::spawn(move || {


        let mut file_metadata: Vec<FileMeta> = Vec::with_capacity(all_files_for_writer.len());

        file_metadata = all_files_for_writer
            .iter()
            .map(|path| FileMeta {
                relative_path: path.to_string_lossy().to_string(),
                compressed: false,
                uncompressed_size: 0,
                chunks: Vec::new(),
            })
            .collect();

        let mut writerstats=WriterStats {
            total_chunks: 0,
            total_written_bytes: 0,
        };
        let mut zdata_offset:u64=0;


        while let Ok((compressed_data,mut chunk_meta)) = rx_compressed.recv() {
//            log::debug!("[writer] Received compressed block from file with index {}:  length {} start write at offset {}", chunk_meta.file_index, compressed_data.len(),zdata_offset);


            let idx = chunk_meta.file_index as usize;

            debug_assert_eq!(
                compressed_data.len() as u64,
                chunk_meta.compressed_size,
                "Mismatch between actual compressed data length ({}) and chunk_meta.compressed_size ({}) for file_index {}, chunk_seq {}",
                compressed_data.len(),
                chunk_meta.compressed_size,
                chunk_meta.file_index,
                chunk_meta.chunk_seq
            );

            if idx >= file_metadata.len() {
                log::error!("[writer] Invalid file_index {}: file_metadata.len() = {}", idx, file_metadata.len());
                continue;
            }

            let file = &mut file_metadata[idx];
            let path = &all_files_for_writer[idx];
            let uncompressed_size=chunk_meta.uncompressed_size;

            file.relative_path = path.to_string_lossy().to_string();
            file.compressed = chunk_meta.compressed;
            file.uncompressed_size += uncompressed_size;
            // Always push the chunk metadata
            chunk_meta.zdata_offset=zdata_offset;
            file.chunks.push(chunk_meta);


            if let Err(e) = writer.write_all(&compressed_data) {
                log::error!("[writer] Write error: {}", e);
                continue;
            }
/*
            if file.compressed {
                unsafe {
                    if let Err(e) = test_decompress_chunk(&compressed_data) {
                        log::error!("Test decompression failed in WRITER THREAD for  {} error {}",file.relative_path,e);
                    }
                }
            }

 */
            zdata_offset += compressed_data.len() as u64;
            writerstats.total_chunks += 1;
            writerstats.total_written_bytes += compressed_data.len() as u64; // Update the total output bytes

        }

        log::info!("[writer] Done {} chunks , total written {} bytes)", writerstats.total_chunks,  writerstats.total_written_bytes);

        let batch = build_arrow_batch_from_files(&file_metadata,&input_dir_cloned);
        (writerstats,batch)
    });


    // Wait for reader thread to finish
    let (uncompressed_files, uncompressed_bytes,compressed_files,compressed_bytes) = reader_thread.join().unwrap();
    log::debug!("[reader] reader_thread joined");

    // Drop the sender-side channels after the reader and compressor threads finish
    tx_chunk_array.into_iter().for_each(drop);
    log::debug!("[reader] tx_chunk dropped after reader thread finished");




    let mut checksums: Vec<[u8;32]>  = Vec::with_capacity(CONFIG.max_core_in_compress as usize);

    let mut  i=0;
    for handle in compressor_threads {
        let Ok((compressor_group, checksum)): Result<(u8, [u8; 32]), anyhow::Error> = handle.join().unwrap() else {
            return Err(anyhow!("Compressor thread returned error"));
        };
        checksums.insert(compressor_group as usize,checksum);
    }





    log::info!("📦 Compressor threads returning blake3 checksums from {} compressor threads", checksums.len());


    // After compressor threads are done, drop tx_compressed
    drop(tx_compressed);
    log::debug!("[compressor] tx_compressed dropped after compressors finished");
    log::debug!("[writer] Waiting for writer thread to finish");
    let (writerstats, batch) = writer_thread.join().unwrap();

    // Build metadata
    let metadata = build_arrow_metadata_for_checksums_and_config(&checksums, &CONFIG);

    // Attach metadata to batch
    let final_batch = attach_metadata(batch?, metadata)?;

    log::debug!("[writer] writer_thread finished");

    // Create index file
    let index_path = output.with_extension("znippy");
    let index_file = File::create(&index_path)?;

    // Create FileWriter
    let mut writer = FileWriter::try_new(index_file, &final_batch.schema())?;

    // Write batch
    writer.write(&final_batch)?;
    writer.finish()?; // ← Ensure writer is properly finished

    log::info!("[main] Arrow index written and finished.");


    let report = CompressionReport {
        total_files,
        compressed_files ,
        uncompressed_files,
        chunks: writerstats.total_chunks,
        total_dirs,
        total_bytes_in : compressed_bytes+uncompressed_bytes,
        total_bytes_out: writerstats.total_written_bytes,
        compressed_bytes,
        uncompressed_bytes,
        compression_ratio: if uncompressed_bytes > 0 {
            (compressed_bytes as f32 / (writerstats.total_written_bytes-uncompressed_bytes)  as f32) * 100.0
        } else {
            0.0
        },
    };

    Ok(report)
}


use std::ffi::CStr;
use zstd_sys::*;

pub fn compress_microchunk(
    cctx: *mut ZSTD_CCtx,
    input: &[u8],
    zstd_buf: &mut [u8],
) -> Result<Vec<u8>> {
    let mut in_buf = ZSTD_inBuffer {
        src: input.as_ptr() as *const _,
        size: input.len(),
        pos: 0,
    };

    let mut full_output = Vec::with_capacity(zstd_buf.len() * 2);

    loop {
        let mut out_buf = ZSTD_outBuffer {
            dst: zstd_buf.as_mut_ptr() as *mut _,
            size: zstd_buf.len(),
            pos: 0,
        };

        let code = unsafe {
            ZSTD_compressStream2(cctx, &mut out_buf, &mut in_buf, ZSTD_EndDirective::ZSTD_e_end)
        };

        if unsafe { ZSTD_isError(code) } != 0 {
            let msg = unsafe { CStr::from_ptr(ZSTD_getErrorName(code)) };
            return Err(anyhow!(
                "ZSTD_compressStream2 failed: {}",
                msg.to_string_lossy()
            ));
        }

        if out_buf.pos > 0 {
            full_output.extend_from_slice(&zstd_buf[..out_buf.pos]);
        }

        if code == 0 {
            break;
        }
    }

    Ok(full_output)
}

use std::slice;
use zstd_sys::*;
use std::ptr;

unsafe fn test_decompress_chunk(data: &[u8]) -> Result<usize> {
    let ctx = unsafe { ZSTD_createDCtx() };
    if ctx.is_null() {
        return Err(anyhow!("Failed to create ZSTD decompression context"));
    }

    let mut out_buf = vec![0u8; 10_000_000]; // Big enough
    let mut out_pos = 0;

    let mut input = ZSTD_inBuffer {
        src: data.as_ptr() as *const _,
        size: data.len(),
        pos: 0,
    };

    let mut output = ZSTD_outBuffer {
        dst: out_buf.as_mut_ptr() as *mut _,
        size: out_buf.len(),
        pos: 0,
    };

    let code = unsafe { ZSTD_decompressStream(ctx, &mut output, &mut input) };
    unsafe { ZSTD_freeDCtx(ctx) };

    if ZSTD_isError(code) != 0 {
        let msg = unsafe { std::ffi::CStr::from_ptr(ZSTD_getErrorName(code)) };
        return Err(anyhow!("Decompression error: {:?}", msg));
    }

    Ok(output.pos)
}


/// Compresses a microchunk using `ZSTD_compress2` (non-streaming API).
/// Returns a standalone frame that is self-decompressible.
pub fn compress2_microchunk(
    cctx: *mut ZSTD_CCtx,
    input: &[u8],
) -> Result<Vec<u8>> {
    unsafe {
        let bound = ZSTD_compressBound(input.len());
        let mut output = vec![0u8; bound];

        let compressed_size = ZSTD_compress2(
            cctx,
            output.as_mut_ptr() as *mut _,
            bound,
            input.as_ptr() as *const _,
            input.len(),
        );

        if ZSTD_isError(compressed_size) != 0 {
            let msg = CStr::from_ptr(ZSTD_getErrorName(compressed_size));
            return Err(anyhow!(
                "ZSTD_compress2 failed: {}",
                msg.to_string_lossy()
            ));
        }

        output.truncate(compressed_size);
        Ok(output)
    }
}
