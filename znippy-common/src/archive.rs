//! ZnippyArchive — trait and implementation for reading znippy archives.
//!
//! Provides selective file extraction by path (Holger's primary use case:
//! serve individual artifacts on demand from a single .znippy archive).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::array::Array;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_array::{BooleanArray, StringArray, UInt32Array, UInt64Array};

use crate::codec;
use crate::index::read_znippy_index;

/// Trait for reading from a znippy archive.
/// Implementors can provide caching, mmap, or network-backed storage.
pub trait ZnippyReader: Send + Sync {
    /// List all file paths in the archive
    fn list_files(&self) -> Result<Vec<String>>;

    /// Extract a single file by its relative path. Returns decompressed bytes.
    fn extract_file(&self, relative_path: &str) -> Result<Vec<u8>>;

    /// Check if a file exists in the archive
    fn contains(&self, relative_path: &str) -> bool;

    /// Get uncompressed size of a file (without extracting)
    fn file_size(&self, relative_path: &str) -> Option<u64>;
}

/// A znippy archive opened for random-access reads (v2.1 hybrid format).
/// Loads metadata into memory on open, reads raw chunks on demand.
pub struct ZnippyArchive {
    index_path: PathBuf,
    schema: Arc<Schema>,
    /// path → file entry with row indices for its chunks
    file_index: HashMap<String, FileEntry>,
    /// The metadata batch (no zdata — just offsets/sizes)
    batches: Vec<RecordBatch>,
}

struct FileEntry {
    compressed: bool,
    uncompressed_size: u64,
    /// Row indices in the batch for this file's chunks, sorted by chunk_seq
    chunk_rows: Vec<usize>,
}

impl ZnippyArchive {
    /// Open an archive. Reads entire Arrow IPC into memory.
    pub fn open(index_path: &Path) -> Result<Self> {
        let (schema, batches) = read_znippy_index(index_path)?;
        let file_index = Self::build_file_index(&batches)?;

        Ok(Self {
            index_path: index_path.to_path_buf(),
            schema,
            file_index,
            batches,
        })
    }

    /// Number of files in the archive
    pub fn file_count(&self) -> usize {
        self.file_index.len()
    }

    fn build_file_index(batches: &[RecordBatch]) -> Result<HashMap<String, FileEntry>> {
        let mut index: HashMap<String, FileEntry> = HashMap::new();

        for batch in batches {
            let paths = batch.column_by_name("relative_path")
                .ok_or_else(|| anyhow!("missing relative_path column"))?
                .as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("relative_path not StringArray"))?;
            let compressed_col = batch.column_by_name("compressed")
                .ok_or_else(|| anyhow!("missing compressed column"))?
                .as_any().downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow!("compressed not BooleanArray"))?;
            let sizes = batch.column_by_name("uncompressed_size")
                .ok_or_else(|| anyhow!("missing uncompressed_size column"))?
                .as_any().downcast_ref::<UInt64Array>()
                .ok_or_else(|| anyhow!("uncompressed_size not UInt64Array"))?;
            let chunk_seq_col = batch.column_by_name("chunk_seq")
                .ok_or_else(|| anyhow!("missing chunk_seq column"))?
                .as_any().downcast_ref::<UInt32Array>()
                .ok_or_else(|| anyhow!("chunk_seq not UInt32Array"))?;

            for row in 0..batch.num_rows() {
                let path = paths.value(row).to_string();
                let compressed = compressed_col.value(row);
                let uncompressed_size = sizes.value(row);

                let entry = index.entry(path).or_insert_with(|| FileEntry {
                    compressed,
                    uncompressed_size: 0,
                    chunk_rows: Vec::new(),
                });
                entry.uncompressed_size += uncompressed_size;
                entry.chunk_rows.push(row);
            }
        }

        // Sort chunk_rows by chunk_seq for each file
        for batch in batches {
            let chunk_seq_col = batch.column_by_name("chunk_seq")
                .unwrap()
                .as_any().downcast_ref::<UInt32Array>()
                .unwrap();

            for entry in index.values_mut() {
                entry.chunk_rows.sort_by_key(|&row| chunk_seq_col.value(row));
            }
        }

        Ok(index)
    }
}

impl ZnippyReader for ZnippyArchive {
    fn list_files(&self) -> Result<Vec<String>> {
        Ok(self.file_index.keys().cloned().collect())
    }

    fn extract_file(&self, relative_path: &str) -> Result<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};

        let entry = self.file_index.get(relative_path)
            .ok_or_else(|| anyhow!("file not found in archive: {}", relative_path))?;

        let batch = &self.batches[0]; // TODO: multi-batch support
        let fdata_offset_col = batch.column_by_name("fdata_offset")
            .ok_or_else(|| anyhow!("missing fdata_offset column"))?
            .as_any().downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow!("fdata_offset not UInt64Array"))?;
        let compressed_size_col = batch.column_by_name("compressed_size")
            .ok_or_else(|| anyhow!("missing compressed_size column"))?
            .as_any().downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow!("compressed_size not UInt64Array"))?;

        let mut file = std::fs::File::open(&self.index_path)?;
        let mut result = Vec::with_capacity(entry.uncompressed_size as usize);

        for &row in &entry.chunk_rows {
            let offset = fdata_offset_col.value(row);
            let size = compressed_size_col.value(row) as usize;

            file.seek(SeekFrom::Start(offset))?;
            let mut chunk_buf = vec![0u8; size];
            file.read_exact(&mut chunk_buf)?;

            if entry.compressed {
                let decompressed = codec::decompress_frame(&chunk_buf)?;
                result.extend_from_slice(&decompressed);
            } else {
                result.extend_from_slice(&chunk_buf);
            }
        }

        Ok(result)
    }

    fn contains(&self, relative_path: &str) -> bool {
        self.file_index.contains_key(relative_path)
    }

    fn file_size(&self, relative_path: &str) -> Option<u64> {
        self.file_index.get(relative_path).map(|e| e.uncompressed_size)
    }
}
