use std::path::Path;

/// Lista över vanliga filändelser som ofta redan är komprimerade
const SKIPPED_EXTENSIONS: [&str; 20] = [
    "zip", "gz", "bz2", "xz", "7z", "rar", "lz", "lz4", "zst", "tar.gz",
    "tar.bz2", "tar.xz", "tgz", "tbz", "txz", "jar", "war", "ear", "apk", "iso",
];

/// Returnerar `true` om filens ändelse matchar någon av de vanliga komprimerade typerna
pub fn should_skip_compression(path: &Path) -> bool {
    if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
        let lower = file_name.to_lowercase();

        for ext in SKIPPED_EXTENSIONS {
            if lower.ends_with(ext) {
                return true;
            }
        }
    }
    false
}
