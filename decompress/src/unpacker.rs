use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
    thread,
};

use anyhow::Result;
use blake3::Hasher;
use crossbeam_channel::{bounded, Receiver, Sender};
use log::debug;
use rayon::prelude::*;
use zstd_sys::*;

use znippy_common::{common_config::CONFIG, read_znippy_index, FileEntry};

pub fn decompress_archive(archive_path: &Path, output_dir: &Path) -> Result<()> {
    debug!("[decompress_archive] Reading index from {:?}", archive_path);
    let archive_file = File::open(archive_path)?;
    let entries = read_znippy_index(&mut archive_file.try_clone()?)?;
    let archive_path = Arc::new(archive_path.to_path_buf());

    let (tx, rx): (Sender<usize>, Receiver<usize>) = bounded(CONFIG.max_core_in_flight);

    let handles: Vec<_> = (0..CONFIG.max_core_in_compress)
        .map(|thread_id| {
            let rx = rx.clone();
            let entries = entries.clone();
            let archive_path = Arc::clone(&archive_path);
            let output_dir = output_dir.to_path_buf();

            thread::spawn(move || {
                for i in rx.iter() {
                    let entry = &entries[i];
                    let rel_path = &entry.relative_path;
                    let out_path = output_dir.join(rel_path);

                    debug!("[decompressor-{thread_id}] Processing {:?} (index {})", rel_path, i);
                    if let Some(parent) = out_path.parent() {
                        std::fs::create_dir_all(parent).unwrap();
                    }

                    let mut archive = File::open(&*archive_path).unwrap();
                    archive.seek(SeekFrom::Start(entry.offset)).unwrap();
                    let mut reader = archive.take(entry.length);

                    if entry.compressed {
                        debug!("[decompressor-{thread_id}] Decompressing {:?}", rel_path);
                        let dctx = unsafe { ZSTD_createDCtx() };
                        assert!(!dctx.is_null());

                        let mut out_file = OpenOptions::new()
                            .create(true)
                            .write(true)
                            .truncate(true)
                            .open(&out_path)
                            .unwrap();

                        let mut in_buf = vec![0u8; 1024*1024];  // 64 KiB
                        let mut out_buf = vec![0u8; 1024*1024]; // 64 KiB
                        let mut hasher = Hasher::new();

                        let mut z_in = ZSTD_inBuffer {
                            src: std::ptr::null(),
                            size: 0,
                            pos: 0,
                        };
                        let mut z_out = ZSTD_outBuffer {
                            dst: out_buf.as_mut_ptr() as *mut _,
                            size: out_buf.len(),
                            pos: 0,
                        };

                        loop {
                            if z_in.pos >= z_in.size {
                                match reader.read(&mut in_buf) {
                                    Ok(0) => break,
                                    Ok(n) => {
                                        z_in.src = in_buf.as_ptr() as *const _;
                                        z_in.size = n;
                                        z_in.pos = 0;
                                        debug!("[decompressor-{thread_id}] Read {n} bytes from archive");
                                    }
                                    Err(e) => {
                                        debug!("[decompressor-{thread_id}] Read error: {e}");
                                        break;
                                    }
                                }
                            }

                            z_out.pos = 0;
                            let rc = unsafe { ZSTD_decompressStream(dctx, &mut z_out, &mut z_in) };

                            if unsafe { ZSTD_isError(rc) } != 0 {
                                debug!("[decompressor-{thread_id}] Decompression error for {:?}", rel_path);
                                break;
                            }

                            if z_out.pos > 0 {
                                out_file.write_all(&out_buf[..z_out.pos]).unwrap();
                                hasher.update(&out_buf[..z_out.pos]);
                                debug!("[decompressor-{thread_id}] Wrote {} bytes", z_out.pos);
                            }

                            if rc == 0 {
                                break;
                            }
                        }

                        unsafe { ZSTD_freeDCtx(dctx) };

                        let calculated = hasher.finalize();
                        if calculated.as_bytes() != &entry.checksum {
                            panic!("[decompressor-{thread_id}] Checksum mismatch for {:?}", rel_path);
                        }
                    } else {
                        debug!("[decompressor-{thread_id}] Copying raw {:?}", rel_path);
                        let mut out_file = File::create(&out_path).unwrap();
                        let mut hasher = Hasher::new();
                        let mut buffer = [0u8; 1 << 16];

                        loop {
                            match reader.read(&mut buffer) {
                                Ok(0) => break,
                                Ok(n) => {
                                    out_file.write_all(&buffer[..n]).unwrap();
                                    hasher.update(&buffer[..n]);
                                    debug!("[decompressor-{thread_id}] Copied {} bytes", n);
                                }
                                Err(e) => {
                                    debug!("[decompressor-{thread_id}] Raw read error: {e}");
                                    break;
                                }
                            }
                        }

                        let calculated = hasher.finalize();
                        if calculated.as_bytes() != &entry.checksum {
                            panic!("[decompressor-{thread_id}] Checksum mismatch for raw {:?}", rel_path);
                        }
                    }
                }
            })
        })
        .collect();

    for i in 0..entries.len() {
        tx.send(i)?;
    }
    drop(tx);

    for h in handles {
        h.join().expect("Tråd kraschade");
    }

    Ok(())
}
