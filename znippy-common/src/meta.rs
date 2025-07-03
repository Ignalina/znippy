use serde::{Serialize, Deserialize};

/// Metadata för en enskild chunk i arkivet (.zdata)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMeta {
    pub offset: u64,
    pub length: u64,
    pub compressed: bool,
    pub uncompressed_size: u64,
    pub checksum: Option<Vec<u8>>, // Added checksum to ChunkMeta
}

/// Samlad statistik över en komprimeringskörning
#[derive(Debug, Default)]
pub struct CompressionStats {
    pub total_chunks: usize,
    pub total_input_bytes: u64,
    pub total_output_bytes: u64,
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
