// decompress/src/lib.rs

pub mod unpacker;

pub use unpacker::{
    decompress_snippy,
    read_snippy_index,
    verify_archive_integrity,
};
