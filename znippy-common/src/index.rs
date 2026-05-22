// index.rs — v0.6 format: blobs stored inline, Arrow IPC is a pure metadata index.
//
// File layout:
//   [blob_0][blob_1]...[blob_N]  — compressed/raw chunk bytes, written as produced
//   [Arrow IPC stream]           — metadata index, written after all blobs
//   [8 bytes LE u64]             — byte offset where Arrow IPC starts (footer)
//
// Arrow schema columns:
//   relative_path, chunk_seq, fdata_offset, checksum_group,
//   compressed, uncompressed_size, blob_offset, blob_size, checksum

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::common_config::StrategicConfig;
use crate::meta::BlobMeta;
use crate::{decompress_archive};
use anyhow::Result;
use arrow::array::{
    BooleanBuilder, FixedSizeBinaryBuilder, StringBuilder, UInt32Builder, UInt64Builder,
    UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use once_cell::sync::Lazy;

/// v0.6 schema: Arrow IPC is a pure metadata index; blobs are stored inline before it.
pub static ZNIPPY_INDEX_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("relative_path", DataType::Utf8, false),
        Field::new("chunk_seq", DataType::UInt32, false),
        Field::new("fdata_offset", DataType::UInt64, false),
        Field::new("checksum_group", DataType::UInt8, false),
        Field::new("compressed", DataType::Boolean, false),
        Field::new("uncompressed_size", DataType::UInt64, false),
        Field::new("blob_offset", DataType::UInt64, false),
        Field::new("blob_size", DataType::UInt64, false),
        Field::new("checksum", DataType::FixedSizeBinary(32), false),
    ]))
});

pub fn znippy_index_schema() -> &'static Arc<Schema> {
    &ZNIPPY_INDEX_SCHEMA
}

/// Build Arrow schema metadata containing config (no checksum entries — those live in column).
pub fn build_arrow_metadata_for_config(config: &StrategicConfig) -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert("znippy_format_version".into(), "3".into());
    m.insert("max_core_in_flight".into(), config.max_core_in_flight.to_string());
    m.insert("max_core_in_compress".into(), config.max_core_in_compress.to_string());
    m.insert("max_mem_allowed".into(), config.max_mem_allowed.to_string());
    m.insert("min_free_memory_ratio".into(), config.min_free_memory_ratio.to_string());
    m.insert("file_split_block_size".into(), config.file_split_block_size.to_string());
    m.insert("max_chunks".into(), config.max_chunks.to_string());
    m.insert("compression_level".into(), config.compression_level.to_string());
    m.insert("zstd_output_buffer_size".into(), config.zstd_output_buffer_size.to_string());
    m
}

pub fn extract_config_from_arrow_metadata(
    metadata: &HashMap<String, String>,
) -> anyhow::Result<StrategicConfig> {
    Ok(StrategicConfig {
        max_core_allowed: 0,
        max_core_in_flight: metadata
            .get("max_core_in_flight")
            .ok_or_else(|| anyhow::anyhow!("Missing 'max_core_in_flight'"))?
            .parse()?,
        max_core_in_compress: metadata
            .get("max_core_in_compress")
            .ok_or_else(|| anyhow::anyhow!("Missing 'max_core_in_compress'"))?
            .parse()?,
        max_mem_allowed: metadata
            .get("max_mem_allowed")
            .ok_or_else(|| anyhow::anyhow!("Missing 'max_mem_allowed'"))?
            .parse()?,
        min_free_memory_ratio: metadata
            .get("min_free_memory_ratio")
            .ok_or_else(|| anyhow::anyhow!("Missing 'min_free_memory_ratio'"))?
            .parse()?,
        file_split_block_size: metadata
            .get("file_split_block_size")
            .ok_or_else(|| anyhow::anyhow!("Missing 'file_split_block_size'"))?
            .parse()?,
        max_chunks: metadata
            .get("max_chunks")
            .ok_or_else(|| anyhow::anyhow!("Missing 'max_chunks'"))?
            .parse()?,
        compression_level: metadata
            .get("compression_level")
            .ok_or_else(|| anyhow::anyhow!("Missing 'compression_level'"))?
            .parse()?,
        zstd_output_buffer_size: metadata
            .get("zstd_output_buffer_size")
            .ok_or_else(|| anyhow::anyhow!("Missing 'zstd_output_buffer_size'"))?
            .parse()?,
    })
}

/// Build the Arrow metadata index batch from blob positions + checksums.
///
/// `checksums[i]` is the BLAKE3 for compressor group i.
/// Every row carries its group's checksum in the `checksum` column.
pub fn build_metadata_batch<F>(
    blobs: &[BlobMeta],
    checksums: &[[u8; 32]],
    path_resolver: F,
) -> arrow::error::Result<RecordBatch>
where
    F: Fn(u64) -> String,
{
    let len = blobs.len();

    let mut path_builder = StringBuilder::with_capacity(len, len * 64);
    let mut seq_builder = UInt32Builder::with_capacity(len);
    let mut fdata_builder = UInt64Builder::with_capacity(len);
    let mut group_builder = UInt8Builder::with_capacity(len);
    let mut compressed_builder = BooleanBuilder::with_capacity(len);
    let mut size_builder = UInt64Builder::with_capacity(len);
    let mut blob_offset_builder = UInt64Builder::with_capacity(len);
    let mut blob_size_builder = UInt64Builder::with_capacity(len);
    let mut checksum_builder = FixedSizeBinaryBuilder::with_capacity(len, 32);

    let empty = [0u8; 32];
    for blob in blobs {
        let m = &blob.chunk_meta;
        path_builder.append_value(path_resolver(m.file_index));
        seq_builder.append_value(m.chunk_seq);
        fdata_builder.append_value(m.fdata_offset);
        group_builder.append_value(m.checksum_group);
        compressed_builder.append_value(m.compressed);
        size_builder.append_value(m.uncompressed_size);
        blob_offset_builder.append_value(blob.blob_offset);
        blob_size_builder.append_value(blob.blob_size);
        let cs = checksums.get(m.checksum_group as usize).unwrap_or(&empty);
        checksum_builder.append_value(cs)?;
    }

    RecordBatch::try_new(
        Arc::new(ZNIPPY_INDEX_SCHEMA.as_ref().clone()),
        vec![
            Arc::new(path_builder.finish()),
            Arc::new(seq_builder.finish()),
            Arc::new(fdata_builder.finish()),
            Arc::new(group_builder.finish()),
            Arc::new(compressed_builder.finish()),
            Arc::new(size_builder.finish()),
            Arc::new(blob_offset_builder.finish()),
            Arc::new(blob_size_builder.finish()),
            Arc::new(checksum_builder.finish()),
        ],
    )
}

/// Read the Arrow IPC index from a v0.6 .znippy file.
///
/// Reads the 8-byte footer to locate where the Arrow IPC section starts,
/// then reads and parses the Arrow stream from that offset.
pub fn read_znippy_index(path: &Path) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
    use arrow::ipc::reader::StreamReader;

    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();

    anyhow::ensure!(file_len >= 8, "file too small to be a znippy archive");

    // Last 8 bytes: LE u64 offset where Arrow IPC starts
    file.seek(SeekFrom::End(-8))?;
    let mut footer = [0u8; 8];
    file.read_exact(&mut footer)?;
    let index_offset = u64::from_le_bytes(footer);

    anyhow::ensure!(
        index_offset < file_len - 8,
        "corrupt footer: index_offset {} >= {}", index_offset, file_len - 8
    );

    // Read Arrow IPC bytes between index_offset and footer
    file.seek(SeekFrom::Start(index_offset))?;
    let arrow_len = (file_len - 8 - index_offset) as usize;
    let mut arrow_bytes = vec![0u8; arrow_len];
    file.read_exact(&mut arrow_bytes)?;

    let cursor = std::io::Cursor::new(arrow_bytes);
    let reader = StreamReader::try_new(cursor, None)?;
    let schema = reader.schema();
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("Failed to read Arrow stream: {}", e))?;

    Ok((schema, batches))
}

pub fn is_probably_compressed(path: &Path) -> bool {
    if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
        let ext = ext.to_ascii_lowercase();
        matches!(
            ext.as_str(),
            "zip" | "gz" | "bz2" | "xz" | "lz" | "lzma" | "7z" | "rar" | "cab"
                | "jar" | "war" | "ear" | "zst" | "sz" | "lz4" | "tgz" | "txz"
                | "tbz" | "apk" | "dmg" | "deb" | "rpm" | "arrow" | "mpeg" | "mpg"
                | "jpeg" | "jpg" | "gif" | "bmp" | "png" | "crate" | "znippy"
                | "zdata" | "parquet" | "webp" | "webm"
        )
    } else {
        false
    }
}

pub fn should_skip_compression(path: &Path) -> bool {
    is_probably_compressed(path)
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
    for batch in &batches {
        let paths = batch
            .column_by_name("relative_path")
            .and_then(|c| c.as_any().downcast_ref::<arrow::array::StringArray>())
            .ok_or_else(|| anyhow::anyhow!("missing relative_path column"))?;
        let sizes = batch
            .column_by_name("uncompressed_size")
            .and_then(|c| c.as_any().downcast_ref::<arrow::array::UInt64Array>())
            .ok_or_else(|| anyhow::anyhow!("missing uncompressed_size column"))?;
        for i in 0..batch.num_rows() {
            println!("{}\t{}", paths.value(i), sizes.value(i));
        }
    }
    Ok(())
}

pub fn verify_archive_integrity(path: &Path) -> Result<VerifyReport> {
    let out_dir = PathBuf::from("/dev/null");
    decompress_archive(path, false, &out_dir)
}
