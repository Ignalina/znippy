use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMeta {
    pub fdata_offset: u64,
    pub file_index: u64,
    pub chunk_seq: u32,
    pub checksum_group: u8,
    pub compressed: bool,
    pub uncompressed_size: u64,
    pub compressed_size: u64,
}

/// Blob position written to the .znippy file; paired with ChunkMeta for the Arrow index.
#[derive(Debug, Clone)]
pub struct BlobMeta {
    pub chunk_meta: ChunkMeta,
    pub blob_offset: u64,
    pub blob_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriterStats {
    pub total_chunks: u64,
    pub total_written_bytes: u64,
    pub verified_files: usize,
    pub corrupt_files: usize,
    pub verified_bytes: u64,
    pub corrupt_bytes: u64,
}

#[derive(Debug)]
pub struct ReaderStats {
    pub total_files: usize,
    pub skipped_files: usize,
}

#[derive(Debug)]
pub struct FileMeta {
    pub relative_path: String,
    pub compressed: bool,
    pub uncompressed_size: u64,
    pub chunks: Vec<ChunkMeta>,
}
