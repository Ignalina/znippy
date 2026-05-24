// compress/src/lib.rs

pub mod slot_packer;
pub mod stream_packer;

pub use slot_packer::compress_dir;
pub use stream_packer::{compress_stream, ArchiveEntry, StreamCompressor};
