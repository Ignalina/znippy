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
use crate::plugin::ExtensionRow;
use crate::{decompress_archive};
use anyhow::Result;
use arrow::array::{
    Array, ArrayRef, BooleanBuilder, FixedSizeBinaryBuilder, Int8Builder, StringBuilder,
    UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use once_cell::sync::Lazy;

/// Per-file extension metadata carried into the Arrow index.
/// (plugin_type_id, extracted fields) — None for files with no matching plugin.
pub type FileExtMeta = Option<(i8, ExtensionRow)>;

/// v0.6 schema: Arrow IPC is a pure metadata index; blobs are stored inline before it.
/// Base index columns — present in every archive, type-agnostic.
/// Package-type modules contribute their own columns on top via `schema_fields()`;
/// the writer composes the on-disk schema with [`compose_index_schema`].
pub static ZNIPPY_INDEX_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(base_index_fields()))
});

fn base_index_fields() -> Vec<Field> {
    vec![
        Field::new("relative_path", DataType::Utf8, false),
        Field::new("chunk_seq", DataType::UInt32, false),
        Field::new("fdata_offset", DataType::UInt64, false),
        Field::new("checksum_group", DataType::UInt8, false),
        Field::new("compressed", DataType::Boolean, false),
        Field::new("uncompressed_size", DataType::UInt64, false),
        Field::new("blob_offset", DataType::UInt64, false),
        Field::new("blob_size", DataType::UInt64, false),
        Field::new("checksum", DataType::FixedSizeBinary(32), false),
    ]
}

pub fn znippy_index_schema() -> &'static Arc<Schema> {
    &ZNIPPY_INDEX_SCHEMA
}

/// Compose the on-disk index schema: base columns, plus — when a module contributes columns —
/// a `pkg_type` discriminator followed by the module's own `ext_fields`.
/// With no module fields, this is exactly the base schema (v0.6 layout, directly DuckDB-queryable).
pub fn compose_index_schema(ext_fields: &[Field]) -> Arc<Schema> {
    let mut fields = base_index_fields();
    if !ext_fields.is_empty() {
        fields.push(Field::new("pkg_type", DataType::Int8, true));
        fields.extend(ext_fields.iter().cloned());
    }
    Arc::new(Schema::new(fields))
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
    ext_meta: &[FileExtMeta],
    ext_fields: &[Field],
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

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(path_builder.finish()),
        Arc::new(seq_builder.finish()),
        Arc::new(fdata_builder.finish()),
        Arc::new(group_builder.finish()),
        Arc::new(compressed_builder.finish()),
        Arc::new(size_builder.finish()),
        Arc::new(blob_offset_builder.finish()),
        Arc::new(blob_size_builder.finish()),
        Arc::new(checksum_builder.finish()),
    ];

    // Module-contributed columns: a pkg_type discriminator + one column per ext field.
    if !ext_fields.is_empty() {
        let mut pkg_type_builder = Int8Builder::with_capacity(len);
        for blob in blobs {
            match ext_meta.get(blob.chunk_meta.file_index as usize).and_then(|x| x.as_ref()) {
                Some((type_id, _)) => pkg_type_builder.append_value(*type_id),
                None => pkg_type_builder.append_null(),
            }
        }
        columns.push(Arc::new(pkg_type_builder.finish()));

        for field in ext_fields {
            columns.push(build_ext_column(field, blobs, ext_meta));
        }
    }

    RecordBatch::try_new(compose_index_schema(ext_fields), columns)
}

/// Build one extension column from the per-file `ExtensionRow`, keyed by the field name.
/// Supports the Arrow types modules currently declare (Utf8, UInt32); other types yield nulls.
fn build_ext_column(field: &Field, blobs: &[BlobMeta], ext_meta: &[FileExtMeta]) -> ArrayRef {
    use crate::plugin::ExtensionValue;
    let len = blobs.len();
    let value_for = |blob: &BlobMeta| -> Option<&ExtensionValue> {
        ext_meta
            .get(blob.chunk_meta.file_index as usize)
            .and_then(|x| x.as_ref())
            .and_then(|(_, row)| row.fields.get(field.name()))
    };

    match field.data_type() {
        DataType::UInt32 => {
            let mut b = UInt32Builder::with_capacity(len);
            for blob in blobs {
                match value_for(blob) {
                    Some(ExtensionValue::U32(n)) => b.append_value(*n),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        // Default to Utf8 for string-like fields (Str / OptStr).
        _ => {
            let mut b = StringBuilder::with_capacity(len, len * 16);
            for blob in blobs {
                match value_for(blob) {
                    Some(ExtensionValue::Str(s)) => b.append_value(s),
                    Some(ExtensionValue::OptStr(Some(s))) => b.append_value(s),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
    }
}

// ─── Multi-index container codec (planned v0.7, see design.md §6) ──────────────
//
// A multi-type archive holds several Arrow IPC index streams (one per (pkg_type, repo)
// sub-znippy, each with its own narrow schema), followed by a manifest stream that points
// at them, and a footer. The footer stays backward compatible with v0.6:
//
//   v0.6 single index:  [...index...] [8-byte LE u64 index_offset]
//   v0.7 multi index:   [...sub-indexes...][manifest] [8-byte MAGIC] [8-byte LE u64 manifest_offset]
//
// A reader peeks the 8 bytes preceding the trailing offset: if they equal MAGIC it's a
// multi-index archive; otherwise it's a plain v0.6 single index. v0.6 files never carry the
// magic, so old archives keep working.

/// Magic preceding the trailing offset that marks a multi-index (v0.7) archive.
pub const MULTI_INDEX_MAGIC: [u8; 8] = *b"ZNPYMIDX";

/// One entry in the multi-index manifest: a sub-znippy's identity + byte range.
#[derive(Debug, Clone, PartialEq)]
pub struct ManifestEntry {
    pub pkg_type: i8,
    pub repo: String,
    pub module_name: String,
    pub index_offset: u64,
    pub index_len: u64,
    pub row_count: u64,
}

/// What the trailing footer of an archive points at.
#[derive(Debug, Clone, PartialEq)]
pub enum IndexFooter {
    /// v0.6: a single Arrow IPC index begins at this offset.
    Single { index_offset: u64 },
    /// v0.7: the manifest stream begins at this offset.
    Multi { manifest_offset: u64 },
}

/// Interpret an archive's trailing bytes. `tail` must be the last 16 bytes of the file
/// (or last 8 for tiny v0.6 files — then it's always Single).
pub fn interpret_footer(tail: &[u8]) -> IndexFooter {
    let n = tail.len();
    let offset = u64::from_le_bytes(tail[n - 8..].try_into().unwrap());
    if n >= 16 && tail[n - 16..n - 8] == MULTI_INDEX_MAGIC {
        IndexFooter::Multi { manifest_offset: offset }
    } else {
        IndexFooter::Single { index_offset: offset }
    }
}

fn manifest_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("pkg_type", DataType::Int8, false),
        Field::new("repo", DataType::Utf8, false),
        Field::new("module_name", DataType::Utf8, false),
        Field::new("index_offset", DataType::UInt64, false),
        Field::new("index_len", DataType::UInt64, false),
        Field::new("row_count", DataType::UInt64, false),
    ]))
}

/// Serialize manifest entries to an Arrow IPC stream (itself DuckDB-readable).
pub fn write_manifest_bytes(entries: &[ManifestEntry]) -> Result<Vec<u8>> {
    use arrow::ipc::writer::StreamWriter;

    let len = entries.len();
    let mut pkg_type = Int8Builder::with_capacity(len);
    let mut repo = StringBuilder::with_capacity(len, len * 16);
    let mut module_name = StringBuilder::with_capacity(len, len * 16);
    let mut index_offset = UInt64Builder::with_capacity(len);
    let mut index_len = UInt64Builder::with_capacity(len);
    let mut row_count = UInt64Builder::with_capacity(len);
    for e in entries {
        pkg_type.append_value(e.pkg_type);
        repo.append_value(&e.repo);
        module_name.append_value(&e.module_name);
        index_offset.append_value(e.index_offset);
        index_len.append_value(e.index_len);
        row_count.append_value(e.row_count);
    }

    let schema = manifest_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(pkg_type.finish()),
            Arc::new(repo.finish()),
            Arc::new(module_name.finish()),
            Arc::new(index_offset.finish()),
            Arc::new(index_len.finish()),
            Arc::new(row_count.finish()),
        ],
    )?;

    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema)?;
        w.write(&batch)?;
        w.finish()?;
    }
    Ok(buf)
}

/// Parse a manifest Arrow IPC stream back into entries.
pub fn read_manifest_bytes(bytes: &[u8]) -> Result<Vec<ManifestEntry>> {
    use arrow::array::{Int8Array, StringArray, UInt64Array};
    use arrow::ipc::reader::StreamReader;

    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch?;
        let col = |name: &str| batch.column_by_name(name)
            .ok_or_else(|| anyhow::anyhow!("manifest missing column {name}"));
        let pkg_type = col("pkg_type")?.as_any().downcast_ref::<Int8Array>()
            .ok_or_else(|| anyhow::anyhow!("pkg_type type"))?;
        let repo = col("repo")?.as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("repo type"))?;
        let module_name = col("module_name")?.as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("module_name type"))?;
        let index_offset = col("index_offset")?.as_any().downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow::anyhow!("index_offset type"))?;
        let index_len = col("index_len")?.as_any().downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow::anyhow!("index_len type"))?;
        let row_count = col("row_count")?.as_any().downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow::anyhow!("row_count type"))?;
        for i in 0..batch.num_rows() {
            out.push(ManifestEntry {
                pkg_type: pkg_type.value(i),
                repo: repo.value(i).to_string(),
                module_name: module_name.value(i).to_string(),
                index_offset: index_offset.value(i),
                index_len: index_len.value(i),
                row_count: row_count.value(i),
            });
        }
    }
    Ok(out)
}

/// Read the Arrow IPC index from a v0.7 .znippy file.
///
/// Reads the 16-byte footer (8-byte `ZNPYMIDX` magic + 8-byte LE u64 manifest_offset),
/// parses the manifest, reads every sub-index, and merges all batches into one so callers
/// need no format-version awareness.
pub fn read_znippy_index(path: &Path) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    anyhow::ensure!(file_len >= 16, "file too small to be a v0.7 znippy archive");

    file.seek(SeekFrom::End(-16))?;
    let mut tail = [0u8; 16];
    file.read_exact(&mut tail)?;

    match interpret_footer(&tail) {
        IndexFooter::Multi { manifest_offset } => {
            read_multi_index(&mut file, file_len, manifest_offset)
        }
        IndexFooter::Single { .. } => {
            anyhow::bail!("v0.6 archives are not supported; re-compress with v0.7")
        }
    }
}

/// Read all sub-indexes from a v0.7 multi-index archive and concatenate them.
fn read_multi_index(
    file: &mut File,
    file_len: u64,
    manifest_offset: u64,
) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
    use arrow::ipc::reader::StreamReader;

    // manifest lives between manifest_offset and (file_len − 16): 8-byte magic + 8-byte offset
    let manifest_end = file_len.checked_sub(16)
        .ok_or_else(|| anyhow::anyhow!("v0.7 archive too small"))?;
    anyhow::ensure!(manifest_offset <= manifest_end, "corrupt v0.7 manifest_offset");
    let manifest_len = (manifest_end - manifest_offset) as usize;

    file.seek(SeekFrom::Start(manifest_offset))?;
    let mut manifest_bytes = vec![0u8; manifest_len];
    file.read_exact(&mut manifest_bytes)?;
    let entries = read_manifest_bytes(&manifest_bytes)?;

    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let mut schema: Option<Arc<Schema>> = None;

    for entry in &entries {
        file.seek(SeekFrom::Start(entry.index_offset))?;
        let mut sub_bytes = vec![0u8; entry.index_len as usize];
        file.read_exact(&mut sub_bytes)?;
        let cursor = std::io::Cursor::new(sub_bytes);
        let reader = StreamReader::try_new(cursor, None)?;
        if schema.is_none() {
            schema = Some(reader.schema());
        }
        for batch in reader {
            all_batches.push(batch.map_err(|e| anyhow::anyhow!("sub-index read error: {}", e))?);
        }
    }

    let schema = schema.unwrap_or_else(|| Arc::new(Schema::new(base_index_fields())));

    // Merge all sub-index batches into one so callers stay format-agnostic.
    let merged = if all_batches.len() <= 1 {
        all_batches
    } else {
        let batch = arrow_select::concat::concat_batches(&schema, all_batches.iter())
            .map_err(|e| anyhow::anyhow!("concat sub-indexes: {}", e))?;
        vec![batch]
    };

    Ok((schema, merged))
}

/// Read the manifest from a v0.7 multi-index archive.
/// Returns an error if the file is a plain v0.6 single-index archive.
pub fn read_znippy_manifest(path: &Path) -> Result<Vec<ManifestEntry>> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    anyhow::ensure!(file_len >= 16, "file too small to be a v0.7 archive");

    file.seek(SeekFrom::End(-16))?;
    let mut tail = [0u8; 16];
    file.read_exact(&mut tail)?;

    match interpret_footer(&tail) {
        IndexFooter::Single { .. } => {
            anyhow::bail!("not a v0.7 multi-index archive (no MULTI_INDEX_MAGIC)")
        }
        IndexFooter::Multi { manifest_offset } => {
            let manifest_end = file_len - 16;
            anyhow::ensure!(manifest_offset <= manifest_end, "corrupt manifest_offset");
            let manifest_len = (manifest_end - manifest_offset) as usize;
            file.seek(SeekFrom::Start(manifest_offset))?;
            let mut manifest_bytes = vec![0u8; manifest_len];
            file.read_exact(&mut manifest_bytes)?;
            read_manifest_bytes(&manifest_bytes)
        }
    }
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
        let chunk_seqs = batch
            .column_by_name("chunk_seq")
            .and_then(|c| c.as_any().downcast_ref::<arrow::array::UInt32Array>());
        let group_ids = batch
            .column_by_name("group_id")
            .and_then(|c| c.as_any().downcast_ref::<arrow::array::StringArray>());
        let artifact_ids = batch
            .column_by_name("artifact_id")
            .and_then(|c| c.as_any().downcast_ref::<arrow::array::StringArray>());
        let versions = batch
            .column_by_name("version")
            .and_then(|c| c.as_any().downcast_ref::<arrow::array::StringArray>());
        for i in 0..batch.num_rows() {
            // Only print once per file (chunk_seq == 0)
            if let Some(seqs) = chunk_seqs {
                if seqs.value(i) != 0 {
                    continue;
                }
            }
            if let (Some(g), Some(a), Some(v)) = (group_ids, artifact_ids, versions) {
                if !g.is_null(i) {
                    println!(
                        "{}\t{}\t{}:{}:{}",
                        paths.value(i),
                        sizes.value(i),
                        g.value(i),
                        a.value(i),
                        v.value(i)
                    );
                    continue;
                }
            }
            println!("{}\t{}", paths.value(i), sizes.value(i));
        }
    }
    Ok(())
}

pub fn verify_archive_integrity(path: &Path) -> Result<VerifyReport> {
    let out_dir = PathBuf::from("/dev/null");
    decompress_archive(path, false, &out_dir)
}
