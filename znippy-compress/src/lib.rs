// compress/src/lib.rs

pub mod packer;
pub mod stream_packer;

pub use packer::compress_dir;
pub use stream_packer::{compress_stream, ArchiveEntry, StreamCompressor};
