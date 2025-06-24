// compress/src/lib.rs
pub mod packer;
pub use packer::{
    create_snippy_archive,
    list_snippy_archive,
    export_snippy_hashlist
};
