// index.rs – innehåller tidigare file_entry.rs samt funktioner som ska exporteras i lib.rs

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use arrow::array::*;

use parking_lot::Mutex;
use zstd_sys::*;



use anyhow::{Context, Result};
use once_cell::sync::Lazy;
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::array::{ArrayRef, BooleanBuilder, ListBuilder, StringBuilder, StructBuilder, UInt64Builder, ArrayBuilder, FixedSizeBinaryArray, ListArray, StructArray, UInt64Array, StringArray, BooleanArray, make_builder, Array};
use arrow::record_batch::RecordBatch;
use arrow::ipc::reader::FileReader;
use blake3::Hasher;
use crossbeam_channel::{bounded, Receiver, Sender};
use sysinfo::{MemoryRefreshKind, RefreshKind, System};
use zstd_sys::ZSTD_decompress;
use crate::{decompress_archive, ChunkMeta};
use crate::common_config::CONFIG;
// === Arrow-schema ===

pub static ZNIPPY_INDEX_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("relative_path", DataType::Utf8, false),
        Field::new("compressed", DataType::Boolean, true),
        Field::new("uncompressed_size", DataType::UInt64, false),
        Field::new(
            "chunks",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Field::new("zdata_offset", DataType::UInt64, false),
                    Field::new("length", DataType::UInt64, false),
                    Field::new("chunk_seq", DataType::UInt64, false),
                    Field::new("checksum_group", DataType::UInt16, false),
                ])),
                true
            ))),
            true,
        ),
    ]))
});
pub static ZNIPPY_INDEX_BLAKE3_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("checksum_group", DataType::UInt16, false),
        Field::new("checksum", DataType::Binary, true),
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
            "mpg" | "jpeg"| "jpg" | "gif" | "bmp" | "png" | "znippy" | "zdata" | "parquet" |
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
    let zdata_offset_builder = UInt64Builder::with_capacity(capacity);
    let length_builder = UInt64Builder::with_capacity(capacity);
    let chunk_seq_builder = UInt64Builder::with_capacity(capacity);
    let checksum_group_builder =UInt16Builder::with_capacity(capacity);

    let struct_builder = StructBuilder::new(
        vec![
            Field::new("zdata_offset", DataType::UInt64, false),
            Field::new("length", DataType::UInt64, false),
            Field::new("chunk_seq", DataType::UInt64, false),
            Field::new("checksum_group", DataType::UInt16, false),
        ],
        vec![
            Box::new(zdata_offset_builder) as Box<dyn ArrayBuilder>,
            Box::new(length_builder) as Box<dyn ArrayBuilder>,
            Box::new(chunk_seq_builder) as Box<dyn ArrayBuilder>,
            Box::new(checksum_group_builder) as Box<dyn ArrayBuilder>,  // Include checksum builder
        ],
    );

    ListBuilder::new(struct_builder)
}


pub fn build_arrow_batch(
    paths: &[PathBuf],
    metas: &Vec<Vec<ChunkMeta>>
) -> Result<RecordBatch> {
    let mut path_builder = StringBuilder::new();
    let mut compressed_builder = BooleanBuilder::new();
    let mut uncompressed_size_builder = UInt64Builder::new();
    let mut checksum_group_builder = UInt16Builder::new();
    let mut chunks_builder = make_chunks_builder(paths.len());

    for (i, path) in paths.iter().enumerate() {
        let chunks = &metas[i];

        path_builder.append_value(path.to_string_lossy());

        if chunks.is_empty() {
            compressed_builder.append_value(false);
            uncompressed_size_builder.append_value(0);
            checksum_group_builder.append_null();
            chunks_builder.append_null();
            continue;
        }

        let total_uncompressed: u64 = chunks.iter().map(|c| c.uncompressed_size).sum();
        compressed_builder.append_value(chunks[0].compressed);
        uncompressed_size_builder.append_value(total_uncompressed);
        checksum_group_builder.append_null();  // per-file checksum not written yet


        Field::new("zdata_offset", DataType::UInt64, false),
        Field::new("length", DataType::UInt64, false),
        Field::new("chunk_seq", DataType::UInt64, false),
        Field::new("checksum_group", DataType::UInt16, false),


        for chunk in chunks {
            chunks_builder
                .values()
                .field_builder::<UInt64Builder>(1).unwrap()
                .append_value(0);
            chunks_builder
                .values()
                .field_builder::<UInt64Builder>(2).unwrap()
                .append_value(chunk.length);
            chunks_builder
                .values()
                .field_builder::<UInt64Builder>(3).unwrap()
                .append_value(chunk.chunk_seq);
            chunks_builder
                .values()
                .field_builder::<UInt16Builder>(4).unwrap()
                .append_value(chunk.checksum_group);
            chunks_builder.values().append(true);
        }
        chunks_builder.append(true);
    }

    let batch = RecordBatch::try_new(
        znippy_index_schema().clone(),
        vec![
            Arc::new(path_builder.finish()) as ArrayRef,
            Arc::new(compressed_builder.finish()),
            Arc::new(uncompressed_size_builder.finish()),
            Arc::new(checksum_group_builder.finish()),
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
    eprintln!("Batch schema fields: {:?}", schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());

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





