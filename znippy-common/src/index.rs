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
use crate::meta::ChunkMetaCompact;
// === Arrow-schema ===

pub static ZNIPPY_INDEX_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("relative_path", DataType::Utf8, false),
        Field::new("compressed", DataType::Boolean, true),
        Field::new("uncompressed_size", DataType::UInt64, false),
        Field::new("checksum", DataType::Binary, true),
        Field::new(
            "chunks",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Field::new("offset", DataType::UInt64, false),
                    Field::new("length", DataType::UInt64, false),
                    Field::new("checksum", DataType::Binary, true),
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
    let checksum_builder = BinaryBuilder::new();
    let struct_builder = StructBuilder::new(
        vec![
            Field::new("offset", DataType::UInt64, false),
            Field::new("length", DataType::UInt64, false),
            Field::new("checksum", DataType::Binary, true),  // Add the checksum field
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
                checksum: meta.checksum,
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
    let mut path_builder = StringBuilder::new();
    let mut compressed_builder = BooleanBuilder::new();
    let mut uncompressed_size_builder = UInt64Builder::new();
    let mut checksum_builder = BinaryBuilder::with_capacity(paths.len(), 32);
    let mut chunks_builder = make_chunks_builder(paths.len());

    for (i, path) in paths.iter().enumerate() {
        let chunks = &metas[i];

        path_builder.append_value(path.to_string_lossy());

        if chunks.is_empty() {
            compressed_builder.append_value(false);
            uncompressed_size_builder.append_value(0);
            checksum_builder.append_null();
            chunks_builder.append_null();
            continue;
        }

        let total_uncompressed: u64 = chunks.iter().map(|c| c.uncompressed_size).sum();
        compressed_builder.append_value(chunks[0].compressed);
        uncompressed_size_builder.append_value(total_uncompressed);
        checksum_builder.append_null();  // per-file checksum not written yet

        for chunk in chunks {
            chunks_builder
                .values()
                .field_builder::<UInt64Builder>(0).unwrap()
                .append_value(chunk.offset);
            chunks_builder
                .values()
                .field_builder::<UInt64Builder>(1).unwrap()
                .append_value(chunk.length);
            chunks_builder
                .values()
                .field_builder::<BinaryBuilder>(2).unwrap()
                .append_value(&chunk.checksum);
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




/// Muterar en batch: lägger till kolumn med fil-checksummor beräknade från chunkar,
/// och ersätter chunk-checksum med null om `nuke_chunk_checksums` är `true`.
/// Muterar en batch: lägger till kolumn med fil-checksummor beräknade från chunkar,
/// och ersätter chunk-checksum med null om `nuke_chunk_checksums` är `true`.
use arrow::array::*;

pub fn add_file_checksums_and_cleanup(
    batch: &mut RecordBatch,
    nuke_chunk_checksums: bool,
) -> Result<()> {
    log::info!("Batch schema fields: {:?}", batch.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>());

    let chunks_array = batch
        .column_by_name("chunks")
        .context("Missing 'chunks' column")?;

    let chunks = chunks_array
        .as_any()
        .downcast_ref::<ListArray>()
        .context("'chunks' is not ListArray")?;

    let structs = chunks.values()
        .as_any()
        .downcast_ref::<StructArray>()
        .context("'chunks' inner values not StructArray")?;

    let checksum_index = structs
        .column_names()
        .iter()
        .position(|name| *name == "checksum")
        .context("No 'checksum' field in struct")?;


    let checksum_array = structs
        .column(checksum_index)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("'checksum' is not BinaryArray")?;


    let offsets = chunks.value_offsets();
//    let mut new_checksums = FixedSizeBinaryBuilder::with_capacity(batch.num_rows(), 32);
    let mut new_checksums = BinaryBuilder::new();

    for row in 0..batch.num_rows() {
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;

        if start == end {
            new_checksums.append_value(&[0u8; 32]);
        } else {
            let mut hasher = blake3::Hasher::new();
            for i in start..end {
                hasher.update(checksum_array.value(i));
            }
            let hash = hasher.finalize();
            new_checksums.append_value(hash.as_bytes());
        }
    }

    let new_checksum_array = Arc::new(new_checksums.finish()) as ArrayRef;

    let mut columns = batch.columns().to_vec();
    let checksum_col_index = batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == "checksum")
        .context("Can't find 'checksum' column")?;
    columns[checksum_col_index] = new_checksum_array;

    if nuke_chunk_checksums {
        let mut cols = structs.columns().to_vec();
        let nulls = Arc::new(BinaryArray::from(vec![None; structs.len()])) as ArrayRef;
        cols[checksum_index] = nulls;

        let new_struct = StructArray::new(
            structs.fields().clone().into(),
            cols,
            None,
        );

        let new_chunks_array = Arc::new(ListArray::new(
            Arc::new(Field::new("item", structs.data_type().clone(), true)),
            chunks.offsets().clone(),
            Arc::new(new_struct),
            chunks.nulls().cloned(),
        )) as ArrayRef;

        let chunk_col_index = batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == "chunks")
            .context("Can't find 'chunks' column")?;

        columns[chunk_col_index] = new_chunks_array;
    }


    *batch = RecordBatch::try_new(batch.schema().clone(), columns)?;
    Ok(())
}
