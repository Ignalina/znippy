// index.rs – innehåller tidigare file_entry.rs samt funktioner som ska exporteras i lib.rs

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};

use anyhow::{Context, Result};
use once_cell::sync::Lazy;
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::array::{ArrayRef, BooleanBuilder, FixedSizeBinaryBuilder, ListBuilder, StringBuilder, StructBuilder, UInt64Builder, ArrayBuilder, FixedSizeBinaryArray, ListArray, StructArray, UInt64Array, StringArray, BooleanArray, make_builder};
use arrow::record_batch::RecordBatch;
use arrow::ipc::reader::FileReader;
use blake3::Hasher;
use crossbeam_channel::{bounded, Receiver, Sender};
use sysinfo::{MemoryRefreshKind, RefreshKind, System};
use zstd_sys::ZSTD_decompress;
use crate::ChunkMeta;
use crate::common_config::CONFIG;
use crate::meta::ChunkMetaCompact;
// === Arrow-schema ===

pub static ZNIPPY_INDEX_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("relative_path", DataType::Utf8, false),
        Field::new("compressed", DataType::Boolean, true),
        Field::new("uncompressed_size", DataType::UInt64, false),
        Field::new("checksum", DataType::FixedSizeBinary(32), false),
        Field::new(
            "chunks",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Field::new("offset", DataType::UInt64, false),
                    Field::new("length", DataType::UInt64, false),
                    Field::new("checksum", DataType::FixedSizeBinary(32), false),
                ])),
                true
            ))),
            true,
        ),
    ]))
});

pub fn znippy_index_schema() -> &'static Arc<Schema> {
    &ZNIPPY_INDEX_SCHEMA
}

pub fn is_probably_compressed(path: &Path) -> bool {
    if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
        let ext = ext.to_ascii_lowercase();
        matches!(
            ext.as_str(),
            "zip" | "gz" | "bz2" | "xz" | "lz" | "lzma" |
            "7z" | "rar" | "cab" | "jar" | "war" | "ear" |
            "zst" | "sz" | "lz4" | "tgz" | "txz" | "tbz" |
            "apk" | "dmg" | "deb" | "rpm" | "arrow" | "mpeg" |
            "mpg" | "jpeg"| "jpg" | "gif" | "bmp" | "png" |
            "webp" | "webm"
         )
    } else {
        false
    }
}

pub fn should_skip_compression(path: &Path) -> bool {
    is_probably_compressed(path)
}

pub fn make_chunks_builder(capacity: usize) -> ListBuilder<StructBuilder> {
    let offset_builder = UInt64Builder::with_capacity(capacity);
    let length_builder = UInt64Builder::with_capacity(capacity);
    let checksum_builder = FixedSizeBinaryBuilder::new(capacity as i32);  // Cast usize to i32 for checksum builder

    let struct_builder = StructBuilder::new(
        vec![
            Field::new("offset", DataType::UInt64, false),
            Field::new("length", DataType::UInt64, false),
            Field::new("checksum", DataType::FixedSizeBinary(32), false),  // Add the checksum field
        ],
        vec![
            Box::new(offset_builder) as Box<dyn ArrayBuilder>,
            Box::new(length_builder) as Box<dyn ArrayBuilder>,
            Box::new(checksum_builder) as Box<dyn ArrayBuilder>,  // Include checksum builder
        ],
    );

    ListBuilder::new(struct_builder)
}
pub fn convert_to_compact(
    nested: &[Vec<ChunkMeta>],
    total_files: usize,
) -> Vec<Vec<ChunkMetaCompact>> {
    let mut result: Vec<Vec<ChunkMetaCompact>> = vec![Vec::new(); total_files];

    for thread_vec in nested.iter() {
        for meta in thread_vec {
            let file_index = meta.file_index as usize;
            let compact = ChunkMetaCompact {
                offset: meta.offset,
                length: meta.length,
                checksum: [0u8; 32],
                compressed: meta.compressed,
                uncompressed_size: meta.uncompressed_size,
            };
            result[file_index].push(compact);
        }
    }

    result
}


pub fn build_arrow_batch(
    paths: &[PathBuf],
    metas: &Vec<Vec<ChunkMeta>>
) -> Result<RecordBatch> {

    let metas: Vec<Vec<ChunkMetaCompact>> =convert_to_compact(metas, paths.len());
    // create a fast lookuptable from 28 vector of ChunkMeta (in case of 28 cores)
    // that is fileindex=key , value={ChunkMeta}
    let mut path_builder = StringBuilder::new();
    let mut compressed_builder = BooleanBuilder::new();
    let mut uncompressed_size_builder = UInt64Builder::new();
    let mut checksum_builder = FixedSizeBinaryBuilder::new(32);  // Use 32 bytes for Blake3

    let mut chunks_builder = make_chunks_builder(paths.len());

    for (i, path) in paths.iter().enumerate() {
        let chunks = &metas[i];

        path_builder.append_value(path.to_string_lossy());

        if let Some(first_chunk) = chunks.get(0) {
            compressed_builder.append_value(first_chunk.compressed);
            uncompressed_size_builder.append_value(first_chunk.uncompressed_size);
            checksum_builder.append_value(first_chunk.checksum).expect("TODO: panic message");
        } else {
            compressed_builder.append_value(false);
            uncompressed_size_builder.append_value(0);
            checksum_builder.append_null();
        }

        if chunks.is_empty() {
            chunks_builder.append_null();
        } else {
            for chunk in chunks.iter() {
                chunks_builder
                    .values()
                    .field_builder::<UInt64Builder>(0)
                    .unwrap()
                    .append_value(chunk.offset);
                chunks_builder
                    .values()
                    .field_builder::<UInt64Builder>(1)
                    .unwrap()
                    .append_value(chunk.length);
                chunks_builder
                    .values()
                    .field_builder::<FixedSizeBinaryBuilder>(2)
                    .unwrap()
                    .append_value(&chunk.checksum)?;

                chunks_builder.values().append(true);
            }
            chunks_builder.append(true);
        }
    }

    let batch = RecordBatch::try_new(
        znippy_index_schema().clone(),
        vec![
            Arc::new(path_builder.finish()) as ArrayRef,
            Arc::new(compressed_builder.finish()),
            Arc::new(uncompressed_size_builder.finish()),
            Arc::new(checksum_builder.finish()),
            Arc::new(chunks_builder.finish()),
        ],
    )?;

    Ok(batch)
}

// === Återlagda funktioner ===

pub fn read_znippy_index(path: &Path) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
    let file = File::open(path)?;
    let reader = FileReader::try_new(BufReader::new(file), None)?;
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    Ok((schema, batches))
}

#[derive(Debug, Default)]
pub struct VerifyReport {
    pub total_files: usize,
    pub verified_files: usize,
    pub corrupt_files: usize,
    pub total_bytes: u64,
    pub verified_bytes: u64,
    pub corrupt_bytes: u64,
}

pub fn list_archive_contents(path: &Path) -> Result<()> {
    let (_schema, batches) = read_znippy_index(path)?;
    for batch in batches {
        println!("{:?}", batch);
    }
    Ok(())
}

pub fn verify_archive_integrity(path: &Path) -> Result<VerifyReport> {
    let out_dir = PathBuf::from("/dev/null");
    decompress_archive(path, false, &out_dir)
}

// decompress_archive är kvar intakt och orörd
pub fn decompress_archive(archive_path: &Path, save_data: bool, output_dir: &Path) -> Result<VerifyReport> {
    let (_schema, batches) = read_znippy_index(archive_path)?;
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
            let output_dir = output_dir.to_path_buf();
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
                            let full_path = output_dir.join(&path);
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