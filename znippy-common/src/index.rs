// index.rs: Znippy index handling with Arrow

use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::common_config::CONFIG;

use anyhow::{Context, Result};
use blake3::Hasher;
use crossbeam_channel::{bounded, Receiver, Sender};
use sysinfo::{System, RefreshKind, MemoryRefreshKind};
use zstd_sys::*;

use arrow::{
    array::{
        BooleanArray, FixedSizeBinaryArray, ListArray, StringArray, StructArray, UInt64Array,
    },
    datatypes::Schema,
    ipc::reader::FileReader,
    record_batch::RecordBatch,
};

#[derive(Debug, Default)]
pub struct VerifyReport {
    pub total_files: usize,
    pub verified_files: usize,
    pub corrupt_files: usize,
    pub total_bytes: u64,
    pub verified_bytes: u64,
    pub corrupt_bytes: u64,
}

pub fn verify_archive_integrity(
    archive_path: &Path,
    save_data: bool,
    output_dir: Option<PathBuf>,
) -> Result<VerifyReport> {
    let (schema, batches) = read_znippy_index(archive_path)?;
    let znippy_file = File::open(archive_path.with_extension("zdata"))?;
    let znippy_file = Arc::new(znippy_file);

    let (tx, rx): (Sender<(PathBuf, Vec<u8>, bool, u64, Vec<u8>)>, Receiver<_>) =
        bounded(CONFIG.max_core_in_flight);

    let mut report = VerifyReport::default();

    let mut sys = System::new_with_specifics(
        RefreshKind::everything().with_memory(MemoryRefreshKind::everything()),
    );
    let total_memory = sys.total_memory();
    let sys = Arc::new(parking_lot::Mutex::new(sys));

    std::thread::scope(|s| {
        s.spawn({
            let znippy_file = znippy_file.clone();
            let tx = tx.clone();
            let sys = sys.clone();
            move || {
                for batch in &batches {
                    let paths = batch
                        .column_by_name("relative_path")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();

                    let compressed_flags = batch
                        .column_by_name("compressed")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap();

                    let sizes = batch
                        .column_by_name("uncompressed_size")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap();

                    let checksums = batch
                        .column_by_name("checksum")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .unwrap();

                    let chunks_array = batch
                        .column_by_name("chunks")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .unwrap();

                    for row in 0..batch.num_rows() {
                        report.total_files += 1;
                        let path = PathBuf::from(paths.value(row));
                        let compressed = compressed_flags.value(row);
                        let size = sizes.value(row);
                        let checksum_bytes = checksums.value(row).to_vec();

                        let chunk_offsets: Vec<(u64, u64)> = {
                            let list = chunks_array.value(row);
                            let struct_array = list
                                .as_any()
                                .downcast_ref::<StructArray>()
                                .unwrap();
                            let offsets = struct_array
                                .column_by_name("offset")
                                .unwrap()
                                .as_any()
                                .downcast_ref::<UInt64Array>()
                                .unwrap();
                            let lengths = struct_array
                                .column_by_name("length")
                                .unwrap()
                                .as_any()
                                .downcast_ref::<UInt64Array>()
                                .unwrap();
                            (0..offsets.len())
                                .map(|i| (offsets.value(i), lengths.value(i)))
                                .collect()
                        };

                        let result = (|| -> Result<()> {
                            loop {
                                let mut sys = sys.lock();
                                sys.refresh_memory();
                                let used = sys.used_memory();
                                let free_ratio = (total_memory - used) as f32 / total_memory as f32;
                                if free_ratio > CONFIG.min_free_memory_ratio {
                                    break;
                                } else {
                                    eprintln!(
                                        "[verify] Low memory ({:.2}%), throttling reader...",
                                        free_ratio * 100.0
                                    );
                                    std::thread::sleep(std::time::Duration::from_millis(100));
                                }
                            }

                            let mut buf = vec![0u8;
                                               chunk_offsets.iter().map(|(_, len)| len).sum::<u64>() as usize];
                            let mut file = znippy_file
                                .try_clone()
                                .context("failed to clone zdata file")?;
                            let mut pos = 0;
                            for (offset, length) in &chunk_offsets {
                                file.seek(SeekFrom::Start(*offset)).context("seek failed")?;
                                file.read_exact(&mut buf[pos..(pos + *length as usize)])
                                    .context("read failed")?;
                                pos += *length as usize;
                            }
                            tx.send((path.clone(), buf, compressed, size, checksum_bytes.clone()))
                                .context("channel send failed")?;
                            Ok(())
                        })();

                        if let Err(err) = result {
                            eprintln!(
                                "[verify_archive_integrity] error processing file {:?}: {:#}",
                                path, err
                            );
                            report.corrupt_files += 1;
                        }
                    }
                }
            }
        });

        for _ in 0..CONFIG.max_core_in_compress {
            let rx = rx.clone();
            let output_dir = output_dir.clone();
            s.spawn(move || {
                while let Ok((path, data, compressed, size, checksum_bytes)) = rx.recv() {
                    report.total_bytes += size;
                    let decompressed = if compressed {
                        let mut out = vec![0u8; size as usize];
                        unsafe {
                            ZSTD_decompress(
                                out.as_mut_ptr() as *mut _,
                                out.len(),
                                data.as_ptr() as *const _,
                                data.len(),
                            );
                        }
                        out
                    } else {
                        data
                    };

                    let mut hasher = Hasher::new();
                    hasher.update(&decompressed);
                    let hash = hasher.finalize();

                    if hash.as_bytes() == checksum_bytes.as_slice() {
                        report.verified_files += 1;
                        report.verified_bytes += size;

                        if save_data {
                            if let Some(ref outdir) = output_dir {
                                let full_path = outdir.join(&path);
                                if let Some(parent) = full_path.parent() {
                                    std::fs::create_dir_all(parent).unwrap();
                                }
                                let mut f = OpenOptions::new()
                                    .create(true)
                                    .write(true)
                                    .truncate(true)
                                    .open(&full_path)
                                    .unwrap();
                                f.write_all(&decompressed).unwrap();
                            }
                        }
                    } else {
                        report.corrupt_files += 1;
                        report.corrupt_bytes += size;
                        eprintln!("❌ Checksum mismatch for {:?}", path);
                    }
                }
            });
        }
    });

    Ok(report)
}

pub fn read_znippy_index(path: &Path) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
    let file = File::open(path)
        .with_context(|| format!("Failed to open index file: {}", path.display()))?;
    let mut reader = FileReader::try_new(file, None)?;

    let schema = reader.schema();
    let batches = reader.collect::<std::result::Result<Vec<_>, _>>()?;

    Ok((schema, batches))
}

pub fn list_archive_contents(index_path: &Path) -> Result<()> {
    let (_, batches) = read_znippy_index(index_path)?;

    for batch in batches {
        let relative_path = batch
            .column_by_name("relative_path")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let uncompressed_size = batch
            .column_by_name("uncompressed_size")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let compressed = batch
            .column_by_name("compressed")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        for i in 0..batch.num_rows() {
            let path = relative_path.value(i);
            let size = uncompressed_size.value(i);
            let is_compressed = compressed.value(i);

            println!(
                "{} ({} bytes) [{}]",
                path,
                size,
                if is_compressed { "compressed" } else { "stored" }
            );
        }
    }

    Ok(())
}
