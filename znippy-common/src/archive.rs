//! ZnippyArchive — trait and implementation for reading znippy archives.
//!
//! Provides selective file extraction by path (Holger's primary use case:
//! serve individual artifacts on demand from a single .znippy archive).

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::array::{Array, AsArray, ListArray, StructArray, UInt64Array};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

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

/// A znippy archive opened for random-access reads.
/// Loads the index into memory on open, then serves extract requests
/// by seeking into the .zdata file.
pub struct ZnippyArchive {
    index_path: PathBuf,
    zdata_path: PathBuf,
    schema: Arc<Schema>,
    /// path → (compressed, uncompressed_size, chunks: Vec<(offset, length)>)
    file_index: HashMap<String, FileEntry>,
}

struct FileEntry {
    compressed: bool,
    uncompressed_size: u64,
    chunks: Vec<ChunkRef>,
}

struct ChunkRef {
    zdata_offset: u64,
    length: u64,
}

impl ZnippyArchive {
    /// Open an archive. Reads index into memory (~4.5MB for 53k files).
    pub fn open(index_path: &Path) -> Result<Self> {
        let zdata_path = index_path.with_extension("zdata");
        if !zdata_path.exists() {
            return Err(anyhow!("Missing .zdata file: {:?}", zdata_path));
        }

        let (schema, batches) = read_znippy_index(index_path)?;
        let file_index = Self::build_file_index(&batches)?;

        Ok(Self {
            index_path: index_path.to_path_buf(),
            zdata_path,
            schema,
            file_index,
        })
    }

    /// Number of files in the archive
    pub fn file_count(&self) -> usize {
        self.file_index.len()
    }

    fn build_file_index(batches: &[RecordBatch]) -> Result<HashMap<String, FileEntry>> {
        let mut index = HashMap::new();

        for batch in batches {
            let paths = batch.column(0).as_string::<i32>();
            let compressed_col = batch.column(1).as_boolean();
            let sizes = batch.column(2).as_primitive::<arrow::datatypes::UInt64Type>();
            let chunks_col = batch.column(4).as_any().downcast_ref::<ListArray>()
                .ok_or_else(|| anyhow!("chunks column is not a ListArray"))?;

            for row in 0..batch.num_rows() {
                let path = paths.value(row).to_string();
                let compressed = compressed_col.value(row);
                let uncompressed_size = sizes.value(row);

                let mut chunks = Vec::new();
                if !chunks_col.is_null(row) {
                    let chunk_list = chunks_col.value(row);
                    let struct_arr = chunk_list.as_any().downcast_ref::<StructArray>()
                        .ok_or_else(|| anyhow!("chunk item is not a StructArray"))?;

                    let offsets = struct_arr.column(0).as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or_else(|| anyhow!("zdata_offset not UInt64"))?;
                    let lengths = struct_arr.column(2).as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or_else(|| anyhow!("length not UInt64"))?;

                    for i in 0..struct_arr.len() {
                        chunks.push(ChunkRef {
                            zdata_offset: offsets.value(i),
                            length: lengths.value(i),
                        });
                    }
                }

                index.insert(path, FileEntry { compressed, uncompressed_size, chunks });
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

        let mut file = BufReader::new(File::open(&self.zdata_path)?);
        let mut result = Vec::with_capacity(entry.uncompressed_size as usize);

        for chunk in &entry.chunks {
            // Seek to chunk in .zdata
            file.seek(SeekFrom::Start(chunk.zdata_offset))?;

            // Read compressed chunk
            let mut compressed = vec![0u8; chunk.length as usize];
            file.read_exact(&mut compressed)?;

            if entry.compressed {
                // Decompress via codec (openzl or zstd, depending on feature)
                let decompressed = codec::decompress_frame(&compressed)?;
                result.extend_from_slice(&decompressed);
            } else {
                // Stored uncompressed
                result.extend_from_slice(&compressed);
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
