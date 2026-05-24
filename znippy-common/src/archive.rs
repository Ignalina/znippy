//! ZnippyArchive — trait and implementation for reading znippy archives.
//!
//! Provides selective file extraction by path (serve individual artifacts
//! on demand from a single .znippy archive).

use std::collections::HashMap;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::record_batch::RecordBatch;
use arrow_array::{BooleanArray, StringArray, UInt64Array};

use crate::codec;
use crate::index::read_znippy_index;

/// Trait for reading from a znippy archive.
pub trait ZnippyReader: Send + Sync {
    fn list_files(&self) -> Result<Vec<String>>;
    fn extract_file(&self, relative_path: &str) -> Result<Vec<u8>>;
    fn contains(&self, relative_path: &str) -> bool;
    fn file_size(&self, relative_path: &str) -> Option<u64>;

    /// Batch extract multiple files. Default impl calls extract_file sequentially.
    fn extract_files(&self, paths: &[&str]) -> Vec<Result<Vec<u8>>> {
        paths.iter().map(|p| self.extract_file(p)).collect()
    }
}

struct ChunkInfo {
    blob_offset: u64,
    blob_size: u64,
    fdata_offset: u64,
    compressed: bool,
}

struct FileEntry {
    uncompressed_size: u64,
    chunks: Vec<ChunkInfo>,
}

/// A znippy archive opened for random-access reads.
/// Loads only the Arrow IPC index on open; blobs are pread on demand. The
/// archive fd is shared (`Arc<File>`) and read via positioned I/O, so
/// `extract_file` is safe to call concurrently from many threads.
pub struct ZnippyArchive {
    archive: Arc<File>,
    file_index: HashMap<String, FileEntry>,
}

impl ZnippyArchive {
    pub fn open(path: &Path) -> Result<Self> {
        let (_, batches) = read_znippy_index(path)?;
        let file_index = Self::build_file_index(&batches)?;
        let archive = Arc::new(File::open(path)?);
        Ok(Self {
            archive,
            file_index,
        })
    }

    pub fn file_count(&self) -> usize {
        self.file_index.len()
    }

    fn build_file_index(batches: &[RecordBatch]) -> Result<HashMap<String, FileEntry>> {
        let mut index: HashMap<String, FileEntry> = HashMap::new();

        for batch in batches {
            let paths = batch
                .column_by_name("relative_path")
                .ok_or_else(|| anyhow!("missing relative_path column"))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("relative_path not StringArray"))?;
            let compressed_col = batch
                .column_by_name("compressed")
                .ok_or_else(|| anyhow!("missing compressed column"))?
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow!("compressed not BooleanArray"))?;
            let sizes = batch
                .column_by_name("uncompressed_size")
                .ok_or_else(|| anyhow!("missing uncompressed_size column"))?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| anyhow!("uncompressed_size not UInt64Array"))?;
            let blob_offset_col = batch
                .column_by_name("blob_offset")
                .ok_or_else(|| anyhow!("missing blob_offset column"))?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| anyhow!("blob_offset not UInt64Array"))?;
            let blob_size_col = batch
                .column_by_name("blob_size")
                .ok_or_else(|| anyhow!("missing blob_size column"))?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| anyhow!("blob_size not UInt64Array"))?;
            let fdata_offset_col = batch
                .column_by_name("fdata_offset")
                .ok_or_else(|| anyhow!("missing fdata_offset column"))?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| anyhow!("fdata_offset not UInt64Array"))?;

            for row in 0..batch.num_rows() {
                let path = paths.value(row).to_string();
                let compressed = compressed_col.value(row);
                let uncompressed_size = sizes.value(row);
                let blob_offset = blob_offset_col.value(row);
                let blob_size = blob_size_col.value(row);
                let fdata_offset = fdata_offset_col.value(row);

                let entry = index.entry(path).or_insert_with(|| FileEntry {
                    uncompressed_size: 0,
                    chunks: Vec::new(),
                });
                entry.uncompressed_size += uncompressed_size;
                entry.chunks.push(ChunkInfo {
                    blob_offset,
                    blob_size,
                    fdata_offset,
                    compressed,
                });
            }
        }

        for entry in index.values_mut() {
            entry.chunks.sort_by_key(|c| c.fdata_offset);
        }

        Ok(index)
    }
}

impl ZnippyReader for ZnippyArchive {
    fn list_files(&self) -> Result<Vec<String>> {
        Ok(self.file_index.keys().cloned().collect())
    }

    fn extract_file(&self, relative_path: &str) -> Result<Vec<u8>> {
        let entry = self
            .file_index
            .get(relative_path)
            .ok_or_else(|| anyhow!("file not found in archive: {}", relative_path))?;

        let mut result = Vec::with_capacity(entry.uncompressed_size as usize);
        let mut blob = Vec::new(); // reused across chunks
        let mut decomp = Vec::new(); // reused across compressed chunks

        for chunk in &entry.chunks {
            blob.resize(chunk.blob_size as usize, 0);
            // Positioned read — no shared seek, safe under concurrent calls.
            self.archive.read_exact_at(&mut blob, chunk.blob_offset)?;

            if chunk.compressed {
                codec::decompress_into(&blob, &mut decomp)?;
                result.extend_from_slice(&decomp);
            } else {
                result.extend_from_slice(&blob);
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
