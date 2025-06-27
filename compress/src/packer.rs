use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc};
use std::thread;

use anyhow::{Result, Context};
use crossbeam_channel::{bounded, Sender};
use rayon::prelude::*;
use zstd_sys::*;
use sysinfo::{System, RefreshKind, MemoryRefreshKind};

use snippy_common::{FileEntry, should_skip_compression};
use zstd_sys::{ZSTD_e_continue, ZSTD_e_flush, ZSTD_e_end};
const MAX_IN_FLIGHT: usize = 10;

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
    let sys = Arc::new(System::new_with_specifics(refresh));
    let total_memory = sys.total_memory();
    let cpus = sys.cpus().len();
    let max_in_flight = usize::min(MAX_IN_FLIGHT, cpus / 4 + 1);

    let (tx, rx) = bounded::<(usize, PathBuf, Vec<u8>)>(max_in_flight);
    let results = Arc::new(parking_lot::Mutex::new(vec![None; entries.len()]));

    let writer_handle = thread::spawn({
        let results = Arc::clone(&results);
        move || -> Result<()> {
            for (i, path, data) in rx.iter() {
                let should_compress = !should_skip_compression(&path);
                let (output, compressed) = if should_compress {
                    let cctx = unsafe { ZSTD_createCCtx() };
                    assert!(!cctx.is_null());

                    let cctx_params = unsafe { ZSTD_createCCtxParams() };
                    unsafe {
                        ZSTD_CCtxParams_init(cctx_params, 0);
                        ZSTD_CCtxParams_setParameter(cctx_params, ZSTD_cParameter_ZSTD_c_nbWorkers, 4);
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
                            ZSTD_e_end,
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
                sys.refresh_memory();
                sys.used_memory()
            };
            let free_ratio = (total_memory - used) as f32 / total_memory as f32;
            if free_ratio > 0.25 && tx.len() < max_in_flight {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        let mut file = File::open(path.path())?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let rel = path.path().strip_prefix(input_dir)?.to_path_buf();
        tx.send((i, rel, buffer))?;
    }

    drop(tx);
    writer_handle.join().unwrap()?; // will rethrow any panic/error

    let results = results.lock();
    for (entry, Some((output, compressed))) in entries.iter_mut().zip(results.iter()) {
        entry.offset = writer.seek(SeekFrom::Current(0))?;
        entry.length = output.len() as u64;
        entry.checksum = *blake3::hash(&output).as_bytes();
        entry.compressed = *compressed;
        writer.write_all(output)?;
    }

    Ok(())
}
