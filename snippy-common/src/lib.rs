mod file_entry;
mod index;

pub use file_entry::{FileEntry, is_probably_compressed, should_skip_compression};
pub use index::{read_snippy_index, verify_archive_integrity, list_archive_contents};
