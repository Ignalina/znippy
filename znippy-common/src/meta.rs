use serde::{Serialize, Deserialize};

/// Metadata för en enskild chunk i arkivet (.zdata)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMeta {
    pub zdata_offset: u64,
    pub fdata_offset: u64,
    pub file_index: u64,
    pub chunk_seq: u32,
    pub checksum_group: u8, // Added checksum to ChunkMeta
    pub compressed: bool,
    pub uncompressed_size: u64,
    pub compressed_size: u64,

}



#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriterStats {
    pub total_chunks: u64,
    pub total_written_bytes:u64
}


#[derive(Debug)]
pub struct FileMeta {
    pub relative_path: String,
    pub compressed: bool,
    pub uncompressed_size: u64,
    pub chunks: Vec<ChunkMeta>,
}


