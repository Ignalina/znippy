// index.rs – innehåller tidigare file_entry.rs samt funktioner som ska exporteras i lib.rs

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use arrow::array::*;


use anyhow::{Context, Result};
use once_cell::sync::Lazy;
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::array::{ArrayRef, BooleanBuilder, ListBuilder, StringBuilder, StructBuilder, UInt64Builder, ArrayBuilder, FixedSizeBinaryArray, ListArray, StructArray, UInt64Array, StringArray, BooleanArray, make_builder, Array};
use arrow::record_batch::RecordBatch;
use arrow::ipc::reader::FileReader;
use crate::{decompress_archive, ChunkMeta, FileMeta};
use crate::common_config::{StrategicConfig, CONFIG};

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

pub fn build_arrow_metadata_for_checksums_and_config(
    checksums: &[ [u8; 32] ],
    config: &StrategicConfig,
) -> HashMap<String, String> {
    let mut metadata = HashMap::new();

    // Lägg in checksummor
    for (i, hash) in checksums.iter().enumerate() {
        metadata.insert(
            format!("checksum_group_{}", i),
            hex::encode(hash),
        );
    }

    // Lägg in CONFIG-parametrar
    metadata.insert("max_core_in_flight".into(), config.max_core_in_flight.to_string());
    metadata.insert("max_core_in_compress".into(), config.max_core_in_compress.to_string());
    metadata.insert("max_mem_allowed".into(), config.max_mem_allowed.to_string());
    metadata.insert("min_free_memory_ratio".into(), config.min_free_memory_ratio.to_string());
    metadata.insert("file_split_block_size".into(), config.file_split_block_size.to_string());
    metadata.insert("max_chunks".into(), config.max_chunks.to_string());
    metadata.insert("compression_level".into(), config.compression_level.to_string());
    metadata.insert("zstd_output_buffer_size".into(), config.zstd_output_buffer_size.to_string());

    metadata
}


pub fn extract_config_from_arrow_metadata(
    metadata: &std::collections::HashMap<String, String>,
) -> anyhow::Result<StrategicConfig> {
    Ok(StrategicConfig {
        max_core_allowed: 0,
        max_core_in_flight: metadata.get("max_core_in_flight")
            .ok_or_else(|| anyhow::anyhow!("Missing 'max_core_in_flight' in metadata"))?
            .parse()?,

        max_core_in_compress: metadata.get("max_core_in_compress")
            .ok_or_else(|| anyhow::anyhow!("Missing 'max_core_in_compress' in metadata"))?
            .parse()?,

        max_mem_allowed: metadata.get("max_mem_allowed")
            .ok_or_else(|| anyhow::anyhow!("Missing 'max_mem_allowed' in metadata"))?
            .parse()?,

        min_free_memory_ratio: metadata.get("min_free_memory_ratio")
            .ok_or_else(|| anyhow::anyhow!("Missing 'min_free_memory_ratio' in metadata"))?
            .parse()?,

        file_split_block_size: metadata.get("file_split_block_size")
            .ok_or_else(|| anyhow::anyhow!("Missing 'file_split_block_size' in metadata"))?
            .parse()?,

        max_chunks: metadata.get("max_chunks")
            .ok_or_else(|| anyhow::anyhow!("Missing 'max_chunks' in metadata"))?
            .parse()?,

        compression_level: metadata.get("compression_level")
            .ok_or_else(|| anyhow::anyhow!("Missing 'compression_level' in metadata"))?
            .parse()?,

        zstd_output_buffer_size: metadata.get("zstd_output_buffer_size")
            .ok_or_else(|| anyhow::anyhow!("Missing 'zstd_output_buffer_size' in metadata"))?
            .parse()?,
    })
}

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



pub fn attach_metadata(
    rb: RecordBatch,
    metadata: HashMap<String, String>,
) -> arrow::error::Result<RecordBatch> {
    let old_schema = rb.schema();

    // Clone the schema and apply new metadata
    let new_schema = Schema::new_with_metadata(
        old_schema.fields().to_vec(),
        metadata,
    );

    // Construct a new RecordBatch with the same data but updated schema
    RecordBatch::try_new(Arc::new(new_schema), rb.columns().to_vec())
}
pub fn build_arrow_batch_from_files(files: &[FileMeta],input_dir: &Path,) -> arrow::error::Result<RecordBatch> {
    let schema = ZNIPPY_INDEX_SCHEMA
        .as_ref()
        .clone();


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



        let full_path = Path::new(&file.relative_path);
        let rel_path = match full_path.strip_prefix(input_dir) {
            Ok(p) => p.to_string_lossy(),
            Err(_) => file.relative_path.as_str().into(),
        };

        relative_path_builder.append_value(&rel_path);


        compressed_builder.append_value(file.compressed);
        uncompressed_size_builder.append_value(file.uncompressed_size);

        // Access the values for appending chunk data
        let struct_builder = chunks_builder.values();

        // If there are no chunks, append null for this file's chunk list
        if file.chunks.is_empty() {
            chunks_builder.append_null();
            continue;
        }
        log::debug!("Batch schema stats → file: {} has  {} chunks",file.relative_path,file.chunks.len());

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



    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(relative_path_builder.finish()),
            Arc::new(compressed_builder.finish()),
            Arc::new(uncompressed_size_builder.finish()),
            Arc::new(chunks_builder.finish()),
        ],
    )

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
    pub chunks: u64,
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
