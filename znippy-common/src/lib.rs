extern crate core;

pub mod codec;
pub mod common_config;
pub mod index;
pub mod archive;
mod int_ring;
pub use int_ring::{ChunkQueue, RingBuffer};

pub mod chunkrevolver;
pub use chunkrevolver::{ChunkRevolver, get_chunk_slice, split_into_microchunks};

pub mod meta;
pub use meta::{BlobMeta, ChunkMeta, FileMeta};

pub mod plugin;
pub mod plugins;

use serde::{Deserialize, Serialize};

pub mod decompress;
mod skip;

pub use archive::{ZnippyArchive, ZnippyReader};
pub use decompress::decompress_archive;

pub use index::{
    VerifyReport, ZNIPPY_INDEX_SCHEMA,
    build_arrow_metadata_for_config, build_metadata_batch,
    extract_config_from_arrow_metadata, is_probably_compressed, list_archive_contents,
    read_znippy_index, should_skip_compression, verify_archive_integrity, znippy_index_schema,
};

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
    pub compression_ratio: f32,
    pub chunks: u64,
}
