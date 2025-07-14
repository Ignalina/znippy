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
use crate::{decompress_archive, ChunkMeta, FileMeta};
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
                    Field::new("fdata_offset", DataType::UInt64, false),
                    Field::new("length", DataType::UInt64, false),
                    Field::new("chunk_seq", DataType::UInt32, false),
                    Field::new("checksum_group", DataType::UInt8, false),
                ])),
                true
            ))),
            true,
        ),
    ]))
});
pub static ZNIPPY_INDEX_BLAKE3_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("checksum_group", DataType::UInt8, false),
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



pub fn build_arrow_batch_from_files(files: &[FileMeta]) -> arrow::error::Result<RecordBatch> {
    let mut relative_path_builder = StringBuilder::new();
    let mut compressed_builder = BooleanBuilder::new();
    let mut uncompressed_size_builder = UInt64Builder::new();

    // Create the StructBuilder for chunk data
    let chunk_struct_builder = StructBuilder::new(
        vec![
            Field::new("zdata_offset", DataType::UInt64, false),
            Field::new("fdata_offset", DataType::UInt64, false),
            Field::new("length", DataType::UInt64, false),
            Field::new("chunk_seq", DataType::UInt32, false),
            Field::new("checksum_group", DataType::UInt8, false),
        ],
        vec![
            Box::new(UInt64Builder::new()),
            Box::new(UInt64Builder::new()),
            Box::new(UInt64Builder::new()),
            Box::new(UInt32Builder::new()),
            Box::new(UInt8Builder::new()),
        ],
    );

    let mut chunks_builder = ListBuilder::new(chunk_struct_builder);

    for file in files {
        // Append each file's path, compression status, and uncompressed size
        relative_path_builder.append_value(&file.relative_path);
        compressed_builder.append_value(file.compressed);
        uncompressed_size_builder.append_value(file.uncompressed_size);

        // Access the values for appending chunk data
        let struct_builder = chunks_builder.values();

        // If there are no chunks, append null for this file's chunk list
        if file.chunks.is_empty() {
            chunks_builder.append_null();
            continue;
        }

        // Iterate over the chunks in the file and append the data to the struct
        for chunk in &file.chunks {
            // Check and append values to the struct builder
            let zdata_offset_builder = struct_builder.field_builder::<UInt64Builder>(0).unwrap();
            zdata_offset_builder.append_value(chunk.zdata_offset);

            // Check and append values to the struct builder
            let fdata_offset_builder = struct_builder.field_builder::<UInt64Builder>(1).unwrap();
            fdata_offset_builder.append_value(chunk.fdata_offset);

            let length_builder = struct_builder.field_builder::<UInt64Builder>(2).unwrap();
            length_builder.append_value(chunk.compressed_size);

            let chunk_seq_builder = struct_builder.field_builder::<UInt32Builder>(3).unwrap();
            chunk_seq_builder.append_value(chunk.chunk_seq);

            let checksum_group_builder = struct_builder.field_builder::<UInt8Builder>(4).unwrap();
            checksum_group_builder.append_value(chunk.checksum_group);

            struct_builder.append(true); // Append this chunk
        }

        chunks_builder.append(true); // Append this file's chunks to the list
    }

    RecordBatch::try_from_iter(vec![
        ("relative_path", Arc::new(relative_path_builder.finish()) as ArrayRef),
        ("compressed", Arc::new(compressed_builder.finish()) as ArrayRef),
        ("uncompressed_size", Arc::new(uncompressed_size_builder.finish()) as ArrayRef),
        ("chunks", Arc::new(chunks_builder.finish()) as ArrayRef),
    ])
}


//let mut checksums: Vec<[u8;32]>
pub fn build_arrow_batch_for_checksums(checksums: Vec<[u8;32]>) -> arrow::error::Result<RecordBatch> {
    let mut checksum_group_builder = UInt8Builder::new();
    let mut checksum_builder = BinaryBuilder::new();

    for (checksum_group, checksum) in checksums.iter().enumerate() {
    // Append the checksum group and checksum to their respective builders
        checksum_group_builder.append_value(checksum_group as u8);
        checksum_builder.append_value(checksum);
    }

    // Create a schema for the checksum record batch
    let schema = Arc::new(Schema::new(vec![
        Field::new("checksum_group", DataType::UInt8, false),
        Field::new("checksum", DataType::Binary, true),
    ]));

    // Create the RecordBatch from the builders
    RecordBatch::try_from_iter(vec![
        ("checksum" ,Arc::new(checksum_group_builder.finish()) as ArrayRef),
        ("checksum" ,   Arc::new(checksum_builder.finish()) as ArrayRef),
        ])
}
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





