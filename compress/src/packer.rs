use zstd_sys::{ZSTD_CCtxParams_init, ZSTD_CCtxParams_setParameter, ZSTD_CCtx_setParametersUsingCCtxParams, ZSTD_compressBound, ZSTD_compressStream2, ZSTD_createCCtx, ZSTD_createCCtxParams, ZSTD_freeCCtx, ZSTD_freeCCtxParams, ZSTD_inBuffer, ZSTD_isError, ZSTD_outBuffer};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use anyhow::{Result, Context};
use crossbeam_channel::{bounded, Sender};
use rayon::prelude::*;
use sysinfo::{System, RefreshKind, MemoryRefreshKind};
use zstd_sys::ZSTD_EndDirective;
use snippy_common::{FileEntry, should_skip_compression};
use std::os::raw::c_int;
use zstd_sys::ZSTD_cParameter;
use snippy_common::common_config::CONFIG;

pub fn compress_dir(input_dir: &Path, output_file: &Path, skip_compression: bool) -> Result<()> {
    let mut entries = Vec::new();
    let mut writer = BufWriter::new(File::create(output_file)?);

    let paths: Vec<_> = walkdir::WalkDir::new(input_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .collect();

    for entry in &paths {
        let rel_path = entry.path().strip_prefix(input_dir)?.to_path_buf();
        entries.push(FileEntry {
            relative_path: rel_path,
            offset: 0,
            length: 0,
            checksum: [0u8; 32],
            compressed: false,
            uncompressed_size: 0,
        });
    }

    let index_bytes = bincode::encode_to_vec(&entries, bincode::config::standard())?;

    writer.write_all(&(index_bytes.len() as u64).to_le_bytes())?;
    writer.write_all(&index_bytes)?;

    let refresh = RefreshKind::everything().with_memory(MemoryRefreshKind::everything());
    let sys = Arc::new(parking_lot::Mutex::new(System::new_with_specifics(refresh)));
    let total_memory = sys.lock().total_memory();
    let cpus = sys.lock().cpus().len();

    let (tx, rx) = bounded::<(usize, PathBuf, Vec<u8>)>(CONFIG.max_core_in_flight);
    let results = Arc::new(parking_lot::Mutex::new(vec![None; entries.len()]));

    let writer_handle = thread::spawn({
        let results = Arc::clone(&results);
        move || -> Result<()> {
            eprintln!("[writer] started");
            for (i, path, data) in rx.iter() {
                eprintln!("[writer] got file {} (idx {})", path.display(), i);
                let should_compress = !should_skip_compression(&path);
                let (output, compressed) = if should_compress {
                    eprintln!("[writer] compressing {} ({} bytes)", path.display(), data.len());
                    let cctx = unsafe { ZSTD_createCCtx() };
                    assert!(!cctx.is_null());

                    let cctx_params = unsafe { ZSTD_createCCtxParams() };
                    unsafe {
                        ZSTD_CCtxParams_init(cctx_params, 0);
                        ZSTD_CCtxParams_setParameter(
                            cctx_params,
                            ZSTD_cParameter::ZSTD_c_nbWorkers,
                            4 as c_int,
                        );
                        ZSTD_CCtx_setParametersUsingCCtxParams(cctx, cctx_params);
                    };

                    let mut dst = vec![0u8; unsafe { ZSTD_compressBound(data.len()) }];
                    let mut input_buffer = ZSTD_inBuffer {
                        src: data.as_ptr() as *const _,
                        size: data.len(),
                        pos: 0,
                    };
                    let mut output_buffer = ZSTD_outBuffer {
                        dst: dst.as_mut_ptr() as *mut _,
                        size: dst.len(),
                        pos: 0,
                    };

                    let rc = unsafe {
                        ZSTD_compressStream2(
                            cctx,
                            &mut output_buffer,
                            &mut input_buffer,
                            ZSTD_EndDirective::ZSTD_e_end,
                        )
                    };
                    if unsafe { ZSTD_isError(rc) } != 0 {
                        anyhow::bail!("Compression failed: {}", rc);
                    }

                    unsafe {
                        ZSTD_freeCCtxParams(cctx_params);
                        ZSTD_freeCCtx(cctx);
                    }

                    dst.truncate(output_buffer.pos);
                    eprintln!("[writer] compressed to {} bytes", output_buffer.pos);
                    (dst, true)
                } else {
                    (data, false)
                };

                let mut results = results.lock();
                results[i] = Some((output, compressed));
            }
            Ok(())
        }
    });

    for (i, path) in paths.iter().enumerate() {
        while results.lock().iter().filter(|r| r.is_none()).count() < entries.len() {
            let used = {
                let mut sys = sys.lock();
                sys.refresh_memory();
                sys.used_memory()
            };
            let free_ratio = (total_memory - used) as f32 / total_memory as f32;
            if free_ratio > CONFIG.min_free_memory_ratio && tx.len() < CONFIG.max_core_in_flight {
                break;
            }
            eprintln!("[producer] waiting, free_ratio: {:.2}", free_ratio);
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        let mut file = File::open(path.path())?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let rel = path.path().strip_prefix(input_dir)?.to_path_buf();
        tx.send((i, rel.clone(), buffer))?;
        eprintln!("[producer] sent file {} (idx {})", rel.display(), i);
    }

    drop(tx);
    writer_handle.join().unwrap()?; // will rethrow any panic/error

    let results = results.lock();
    for (entry, result) in entries.iter_mut().zip(results.iter()) {
        if let Some((output, compressed)) = result {
            entry.offset = writer.seek(SeekFrom::Current(0))?;
            entry.length = output.len() as u64;
            entry.checksum = *blake3::hash(&output).as_bytes();
            entry.compressed = *compressed;
            writer.write_all(output)?;
        }
    }

    Ok(())
}
