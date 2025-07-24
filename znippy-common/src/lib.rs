extern crate core;

pub mod index;
pub mod common_config;
mod int_ring;
pub use int_ring::{RingBuffer, ChunkQueue};

pub mod chunkrevolver;
pub use chunkrevolver::{ChunkRevolver,get_chunk_slice,split_into_microchunks};

pub mod meta;
pub use meta::{ChunkMeta,FileMeta};

use serde::{Serialize, Deserialize};

pub mod decompress;
mod skip;

pub use decompress::decompress_archive;


pub use index::{extract_config_from_arrow_metadata,attach_metadata,build_arrow_batch_from_files ,znippy_index_schema,is_probably_compressed, should_skip_compression, ZNIPPY_INDEX_SCHEMA,verify_archive_integrity,list_archive_contents,VerifyReport,read_znippy_index};

#[derive(Debug)]
pub struct CompressionReport {
    pub total_files: u64,
    pub compressed_files: u64,
    pub uncompressed_files: u64,
    pub total_dirs: u64,
    pub total_bytes_in: u64,
    pub total_bytes_out: u64,
    pub compressed_bytes: u64,
    pub uncompressed_bytes: u64,
    pub compression_ratio: f32, // 0.0–100.0
    pub chunks: u64,
}