use std::path::{Path, PathBuf};

/// Metadata om en fil i ett `.snippy`-arkiv.
use bincode::{Encode, Decode};

#[derive(Debug, Clone, Encode, Decode)]
pub struct FileEntry {
    pub relative_path: PathBuf,
    pub offset: u64,
    pub length: u64,
    pub checksum: [u8; 32], // BLAKE3
    pub compressed: bool,
    pub uncompressed_size: u64,
}

/// Returnerar `true` om filändelsen antyder att filen redan är komprimerad.
pub fn is_probably_compressed(path: &Path) -> bool {
    if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
        let ext = ext.to_ascii_lowercase();
        matches!(
            ext.as_str(),
            "zip" | "gz" | "bz2" | "xz" | "lz" | "lzma" |
            "7z" | "rar" | "cab" | "jar" | "war" | "ear" |
            "zst" | "sz" | "lz4" | "tgz" | "txz" | "tbz" |
            "apk" | "dmg" | "deb" | "rpm"
        )
    } else {
        false
    }
}

/// Returnerar `true` om vi bör skippa komprimering för denna fil.
///
/// Default: alla "redan komprimerade" filtyper (enligt extension).
pub fn should_skip_compression(path: &Path) -> bool {
    is_probably_compressed(path)
}
