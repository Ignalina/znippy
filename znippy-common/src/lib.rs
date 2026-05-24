extern crate core;

/// Re-export arrow so plugins implement `schema_fields()` against the exact same arrow
/// version as the core trait, avoiding type-mismatch across crate boundaries.
pub use arrow;

pub mod codec;
pub mod common_config;
pub mod index;
pub mod archive;

pub mod slotpool;

pub mod meta;
pub use meta::{BlobMeta, ChunkMeta, FileMeta};

pub mod plugin;
pub mod plugins;

pub mod decompress;
mod skip;

pub use archive::{ZnippyArchive, ZnippyReader};
pub use decompress::decompress_archive;

pub use index::{
    VerifyReport, ZNIPPY_INDEX_SCHEMA,
    MULTI_INDEX_MAGIC, ManifestEntry, IndexFooter,
    build_arrow_metadata_for_config, build_metadata_batch,
    extract_config_from_arrow_metadata, interpret_footer,
    is_probably_compressed, list_archive_contents,
    read_manifest_bytes, read_znippy_index, read_znippy_manifest,
    should_skip_compression, verify_archive_integrity, write_manifest_bytes,
    znippy_index_schema,
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
