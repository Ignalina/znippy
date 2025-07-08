use serde::{Serialize, Deserialize};

/// Metadata för en enskild chunk i arkivet (.zdata)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMeta {
    pub file_index: u64,
    pub chunk_seq: u64,
    pub checksum_group: u16, // Added checksum to ChunkMeta
    pub length: u64,
    pub compressed: bool,
    pub uncompressed_size: u64,
}



#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriterStats {
    pub offset: u64,
    pub total_chunks: u64,
    pub total_written_bytes:u64
}

/// Metadata för en hel fil (en eller flera chunks)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMeta {
    pub relative_path: String,
    pub chunks: Vec<ChunkMeta>,
}

/// Grupp av chunks tillhörande en fil, internt representation i pipelinen
#[derive(Debug)]
pub struct ChunkGroup {
    pub file_index: usize,
    pub chunks: Vec<ChunkMeta>,
}

