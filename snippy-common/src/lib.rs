mod file_entry;
mod index;
pub mod common_config;

pub use file_entry::{is_probably_compressed, should_skip_compression, FileEntry};
pub use index::{list_archive_contents, read_snippy_index, verify_archive_integrity};

#[derive(Debug, Clone)]
pub struct StrategicConfig {
    pub max_core_in_flight: usize,
    pub max_core_in_compress: usize,
    pub max_mem_allowed: u64,
}