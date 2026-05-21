// index.rs – innehåller tidigare file_entry.rs samt funktioner som ska exporteras i lib.rs

use arrow::array::*;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::common_config::{CONFIG, StrategicConfig};
use crate::{ChunkMeta, FileMeta, decompress_archive};
use anyhow::{Context, Result};
use arrow::array::{
    Array, ArrayBuilder, ArrayRef, BooleanArray, BooleanBuilder, FixedSizeBinaryArray, ListArray,
    ListBuilder, StringArray, StringBuilder, StructArray, StructBuilder, UInt64Array,
    UInt64Builder, make_builder,
};
use arrow::datatypes::{DataType, Field, Fields, Schema, UnionFields, UnionMode};
use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use once_cell::sync::Lazy;

pub static ZNIPPY_INDEX_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("relative_path", DataType::Utf8, false),
        Field::new("compressed", DataType::Boolean, true),
        Field::new("uncompressed_size", DataType::UInt64, false),
        Field::new("group", DataType::Utf8, true),
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
                true,
            ))),
            true,
        ),
        Field::new(
            "extension",
            DataType::Union(
                extension_union_fields(),
                UnionMode::Dense,
            ),
            true,
        ),
    ]))
});

/// Extension union variants — add new extensions here.
/// Type ID 0 = holger_nexus_v1, Type ID 1 = photoalbum_v1
pub fn extension_union_fields() -> UnionFields {
    UnionFields::new(
        vec![0, 1, 2, 3],
        vec![
            Field::new(
                "generic",
                DataType::Null,
                true,
            ),
            Field::new(
                "holger_nexus_v1",
                DataType::Struct(Fields::from(vec![
                    Field::new("group", DataType::Utf8, false),
                    Field::new("artifact_type", DataType::Utf8, false),
                    Field::new("version", DataType::Utf8, true),
                ])),
                true,
            ),
            Field::new(
                "cargo_registry_v1",
                DataType::Struct(Fields::from(vec![
                    Field::new("crate_name", DataType::Utf8, false),
                    Field::new("version", DataType::Utf8, false),
                    Field::new("features", DataType::Utf8, true),
                ])),
                true,
            ),
            Field::new(
                "photoalbum_v1",
                DataType::Struct(Fields::from(vec![
                    Field::new("album", DataType::Utf8, false),
                    Field::new("sort_order", DataType::UInt32, false),
                    Field::new("thumbnail", DataType::Binary, true),
                ])),
                true,
            ),
        ],
    )
}

pub fn build_arrow_metadata_for_checksums_and_config(
    checksums: &[[u8; 32]],
    config: &StrategicConfig,
) -> HashMap<String, String> {
    let mut metadata = HashMap::new();

    // Lägg in checksummor
    for (i, hash) in checksums.iter().enumerate() {
        metadata.insert(format!("checksum_group_{}", i), hex::encode(hash));
    }

    // Lägg in CONFIG-parametrar
    metadata.insert(
        "max_core_in_flight".into(),
        config.max_core_in_flight.to_string(),
    );
    metadata.insert(
        "max_core_in_compress".into(),
        config.max_core_in_compress.to_string(),
    );
    metadata.insert("max_mem_allowed".into(), config.max_mem_allowed.to_string());
    metadata.insert(
        "min_free_memory_ratio".into(),
        config.min_free_memory_ratio.to_string(),
    );
    metadata.insert(
        "file_split_block_size".into(),
        config.file_split_block_size.to_string(),
    );
    metadata.insert("max_chunks".into(), config.max_chunks.to_string());
    metadata.insert(
        "compression_level".into(),
        config.compression_level.to_string(),
    );
    metadata.insert(
        "zstd_output_buffer_size".into(),
        config.zstd_output_buffer_size.to_string(),
    );

    metadata
}

pub fn extract_config_from_arrow_metadata(
    metadata: &std::collections::HashMap<String, String>,
) -> anyhow::Result<StrategicConfig> {
    Ok(StrategicConfig {
        max_core_allowed: 0,
        max_core_in_flight: metadata
            .get("max_core_in_flight")
            .ok_or_else(|| anyhow::anyhow!("Missing 'max_core_in_flight' in metadata"))?
            .parse()?,

        max_core_in_compress: metadata
            .get("max_core_in_compress")
            .ok_or_else(|| anyhow::anyhow!("Missing 'max_core_in_compress' in metadata"))?
            .parse()?,

        max_mem_allowed: metadata
            .get("max_mem_allowed")
            .ok_or_else(|| anyhow::anyhow!("Missing 'max_mem_allowed' in metadata"))?
            .parse()?,

        min_free_memory_ratio: metadata
            .get("min_free_memory_ratio")
            .ok_or_else(|| anyhow::anyhow!("Missing 'min_free_memory_ratio' in metadata"))?
            .parse()?,

        file_split_block_size: metadata
            .get("file_split_block_size")
            .ok_or_else(|| anyhow::anyhow!("Missing 'file_split_block_size' in metadata"))?
            .parse()?,

        max_chunks: metadata
            .get("max_chunks")
            .ok_or_else(|| anyhow::anyhow!("Missing 'max_chunks' in metadata"))?
            .parse()?,

        compression_level: metadata
            .get("compression_level")
            .ok_or_else(|| anyhow::anyhow!("Missing 'compression_level' in metadata"))?
            .parse()?,

        zstd_output_buffer_size: metadata
            .get("zstd_output_buffer_size")
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
            "zip"
                | "gz"
                | "bz2"
                | "xz"
                | "lz"
                | "lzma"
                | "7z"
                | "rar"
                | "cab"
                | "jar"
                | "war"
                | "ear"
                | "zst"
                | "sz"
                | "lz4"
                | "tgz"
                | "txz"
                | "tbz"
                | "apk"
                | "dmg"
                | "deb"
                | "rpm"
                | "arrow"
                | "mpeg"
                | "mpg"
                | "jpeg"
                | "jpg"
                | "gif"
                | "bmp"
                | "png"
                | "znippy"
                | "zdata"
                | "parquet"
                | "webp"
                | "webm"
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
    let new_schema = Schema::new_with_metadata(old_schema.fields().to_vec(), metadata);

    // Construct a new RecordBatch with the same data but updated schema
    RecordBatch::try_new(Arc::new(new_schema), rb.columns().to_vec())
}
pub fn build_arrow_batch_from_files(
    files: &[FileMeta],
    input_dir: &Path,
) -> arrow::error::Result<RecordBatch> {
    let schema = ZNIPPY_INDEX_SCHEMA.as_ref().clone();

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
        let rel_path = full_path
            .strip_prefix(input_dir)
            .expect("❌ build_arrow_batch_from_files: file path is not under input_dir")
            .to_string_lossy();

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
        log::debug!(
            "Batch schema stats → file: {} has  {} chunks",
            file.relative_path,
            file.chunks.len()
        );

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
            Arc::new(new_null_utf8_array(files.len())),  // group (NULL = default)
            Arc::new(chunks_builder.finish()),
            Arc::new(new_null_union_array(files.len())), // extension (NULL = none)
        ],
    )
}

/// Create a null Utf8 array of given length
fn new_null_utf8_array(len: usize) -> StringArray {
    let mut builder = StringBuilder::new();
    for _ in 0..len {
        builder.append_null();
    }
    builder.finish()
}

/// Create a null DenseUnion array (extension column, all nulls = generic type_id 0)
fn new_null_union_array(len: usize) -> arrow::array::UnionArray {
    use arrow::buffer::ScalarBuffer;
    let type_ids: Vec<i8> = vec![0; len]; // 0 = generic
    let offsets: Vec<i32> = (0..len as i32).collect();
    let fields = extension_union_fields();

    // Child arrays — generic is Null type, others are empty structs
    let generic_child = arrow::array::NullArray::new(len);

    let holger_fields = Fields::from(vec![
        Field::new("group", DataType::Utf8, false),
        Field::new("artifact_type", DataType::Utf8, false),
        Field::new("version", DataType::Utf8, true),
    ]);
    let cargo_fields = Fields::from(vec![
        Field::new("crate_name", DataType::Utf8, false),
        Field::new("version", DataType::Utf8, false),
        Field::new("features", DataType::Utf8, true),
    ]);
    let photo_fields = Fields::from(vec![
        Field::new("album", DataType::Utf8, false),
        Field::new("sort_order", DataType::UInt32, false),
        Field::new("thumbnail", DataType::Binary, true),
    ]);

    let children: Vec<ArrayRef> = vec![
        Arc::new(generic_child),
        Arc::new(StructArray::new_null(holger_fields, 0)),
        Arc::new(StructArray::new_null(cargo_fields, 0)),
        Arc::new(StructArray::new_null(photo_fields, 0)),
    ];

    arrow::array::UnionArray::try_new(
        fields,
        ScalarBuffer::from(type_ids),
        Some(ScalarBuffer::from(offsets)),
        children,
    ).unwrap()
}

// ─── Extension Data Types ────────────────────────────────────────────

/// Holger Nexus extension metadata (per-file)
#[derive(Debug, Clone)]
pub struct HolgerNexusExt {
    pub group: String,
    pub artifact_type: String,
    pub version: Option<String>,
}

/// Build a DenseUnion array for holger_nexus_v1 extension
pub fn build_holger_nexus_union(entries: &[Option<HolgerNexusExt>]) -> arrow::array::UnionArray {
    use arrow::buffer::ScalarBuffer;

    let fields = extension_union_fields();
    let len = entries.len();

    let mut group_builder = StringBuilder::new();
    let mut type_builder = StringBuilder::new();
    let mut version_builder = StringBuilder::new();

    let mut type_ids: Vec<i8> = Vec::with_capacity(len);
    let mut offsets: Vec<i32> = Vec::with_capacity(len);
    let mut holger_count: i32 = 0;
    let mut generic_count: i32 = 0;

    for entry in entries {
        match entry {
            Some(ext) => {
                type_ids.push(1); // holger_nexus_v1 = type_id 1
                offsets.push(holger_count);
                holger_count += 1;
                group_builder.append_value(&ext.group);
                type_builder.append_value(&ext.artifact_type);
                match &ext.version {
                    Some(v) => version_builder.append_value(v),
                    None => version_builder.append_null(),
                }
            }
            None => {
                // No extension — generic type_id 0
                type_ids.push(0);
                offsets.push(generic_count);
                generic_count += 1;
            }
        }
    }

    let generic_child = arrow::array::NullArray::new(generic_count as usize);

    let holger_struct = StructArray::from(vec![
        (
            Arc::new(Field::new("group", DataType::Utf8, false)),
            Arc::new(group_builder.finish()) as ArrayRef,
        ),
        (
            Arc::new(Field::new("artifact_type", DataType::Utf8, false)),
            Arc::new(type_builder.finish()) as ArrayRef,
        ),
        (
            Arc::new(Field::new("version", DataType::Utf8, true)),
            Arc::new(version_builder.finish()) as ArrayRef,
        ),
    ]);

    let cargo_fields = Fields::from(vec![
        Field::new("crate_name", DataType::Utf8, false),
        Field::new("version", DataType::Utf8, false),
        Field::new("features", DataType::Utf8, true),
    ]);
    let photo_fields = Fields::from(vec![
        Field::new("album", DataType::Utf8, false),
        Field::new("sort_order", DataType::UInt32, false),
        Field::new("thumbnail", DataType::Binary, true),
    ]);

    let children: Vec<ArrayRef> = vec![
        Arc::new(generic_child),
        Arc::new(holger_struct),
        Arc::new(StructArray::new_null(cargo_fields, 0)),
        Arc::new(StructArray::new_null(photo_fields, 0)),
    ];

    arrow::array::UnionArray::try_new(
        fields,
        ScalarBuffer::from(type_ids),
        Some(ScalarBuffer::from(offsets)),
        children,
    ).unwrap()
}

pub fn read_znippy_index(path: &Path) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
    let file = File::open(path)?;
    let reader = FileReader::try_new(BufReader::new(file), None)?;
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    eprintln!(
        "Batch schema fields: {:?}",
        schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
    );

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
