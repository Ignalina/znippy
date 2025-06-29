
mod file_entry;
mod index;
pub mod common_config;
use serde::{Serialize, Deserialize};

pub use file_entry::{is_probably_compressed, should_skip_compression, FileEntry};
pub use index::{list_archive_contents, read_znippy_index, verify_archive_integrity};

#[derive(Debug, Clone)]
pub struct StrategicConfig {
    pub max_core_in_flight: usize,
    pub max_core_in_compress: usize,
    pub max_mem_allowed: u64,
}

#[derive(Debug)]
pub struct CompressionReport {
    pub total_files: usize,
    pub compressed_files: usize,
    pub uncompressed_files: usize,
    pub total_dirs: usize,
    pub total_bytes_in: u64,
    pub total_bytes_out: u64,
    pub compressed_bytes: u64,
    pub uncompressed_bytes: u64,
    pub compression_ratio: f32, // 0.0–100.0
}