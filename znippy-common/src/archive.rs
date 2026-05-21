//! ZnippyArchive — trait and implementation for reading znippy archives.
//!
//! Provides selective file extraction by path (Holger's primary use case:
//! serve individual artifacts on demand from a single .znippy archive).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::array::{Array, AsArray, UInt64Array};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_array::{BooleanArray, StringArray, UInt32Array, LargeBinaryArray};

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

/// A znippy archive opened for random-access reads (v2 single-file format).
/// Loads the index into memory on open, then serves extract requests
/// by reading inline zdata from the Arrow batch.
pub struct ZnippyArchive {
    index_path: PathBuf,
    schema: Arc<Schema>,
    /// path → file entry with row indices for its chunks
    file_index: HashMap<String, FileEntry>,
    /// The raw batch — holds zdata column in memory
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
        let entry = self.file_index.get(relative_path)
            .ok_or_else(|| anyhow!("file not found in archive: {}", relative_path))?;

        let batch = &self.batches[0]; // TODO: multi-batch support
        let zdata_col = batch.column_by_name("zdata")
            .ok_or_else(|| anyhow!("missing zdata column"))?
            .as_any().downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| anyhow!("zdata not LargeBinaryArray"))?;

        let mut result = Vec::with_capacity(entry.uncompressed_size as usize);

        for &row in &entry.chunk_rows {
            let chunk_bytes = zdata_col.value(row);

            if entry.compressed {
                let decompressed = codec::decompress_frame(chunk_bytes)?;
                result.extend_from_slice(&decompressed);
            } else {
                result.extend_from_slice(chunk_bytes);
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
