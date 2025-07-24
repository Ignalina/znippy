use std::path::Path;

/// Lista över vanliga filändelser som ofta redan är komprimerade
const SKIPPED_EXTENSIONS: &[&str] = &[
    // Archive formats
    "zip", "gz", "bz2", "xz", "7z", "rar", "lz", "lz4", "zst", "tar",
    "tar.gz", "tar.bz2", "tar.xz", "tgz", "tbz", "txz",

    // Java / Android
    "jar", "war", "ear", "apk",

    // Disk images
    "iso", "img", "dmg",

    // Audio formats
    "mp3", "aac", "ogg", "flac", "m4a", "wma", "opus",

    // Video formats
    "mp4", "mkv", "avi", "mov", "webm", "flv", "wmv",

    // Image formats
    "jpg", "jpeg", "png", "gif", "webp", "heic", "avif",

    // Office + PDF (already zipped)
    "docx", "xlsx", "pptx", "pdf",

    // Executables & libraries
    "exe", "dll", "so", "dylib", "o", "a", "lib",

    // Fonts
    "ttf", "otf", "woff", "woff2",

    // ML/Data formats
    "npy", "npz", "onnx", "pb", "tflite", "parquet", "orc", "feather",
];

/// Returnerar `true` om filens ändelse matchar någon av de vanliga komprimerade typerna
pub fn should_skip_compression(path: &Path) -> bool {
    if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
        let lower = file_name.to_ascii_lowercase();
        return check_suffix(&lower);
    }
    false}
fn check_suffix(name: &str) -> bool {
    // Reverse suffix match tree (manually written or generated)
    name.ends_with(".zip") ||
        name.ends_with(".gz") ||
        name.ends_with(".bz2") ||
        name.ends_with(".xz") ||
        name.ends_with(".7z") ||
        name.ends_with(".rar") ||
        name.ends_with(".lz") ||
        name.ends_with(".lz4") ||
        name.ends_with(".zst") ||
        name.ends_with(".tar") ||
        name.ends_with(".tar.gz") ||
        name.ends_with(".tar.bz2") ||
        name.ends_with(".tar.xz") ||
        name.ends_with(".tgz") ||
        name.ends_with(".tbz") ||
        name.ends_with(".txz") ||
        name.ends_with(".jar") ||
        name.ends_with(".war") ||
        name.ends_with(".ear") ||
        name.ends_with(".apk") ||
        name.ends_with(".iso") ||
        name.ends_with(".img") ||
        name.ends_with(".dmg") ||
        name.ends_with(".mp3") ||
        name.ends_with(".aac") ||
        name.ends_with(".ogg") ||
        name.ends_with(".flac") ||
        name.ends_with(".m4a") ||
        name.ends_with(".wma") ||
        name.ends_with(".opus") ||
        name.ends_with(".mp4") ||
        name.ends_with(".mkv") ||
        name.ends_with(".avi") ||
        name.ends_with(".mov") ||
        name.ends_with(".webm") ||
        name.ends_with(".flv") ||
        name.ends_with(".wmv") ||
        name.ends_with(".jpg") ||
        name.ends_with(".jpeg") ||
        name.ends_with(".png") ||
        name.ends_with(".gif") ||
        name.ends_with(".webp") ||
        name.ends_with(".heic") ||
        name.ends_with(".avif") ||
        name.ends_with(".docx") ||
        name.ends_with(".xlsx") ||
        name.ends_with(".pptx") ||
        name.ends_with(".pdf") ||
        name.ends_with(".exe") ||
        name.ends_with(".dll") ||
        name.ends_with(".so") ||
        name.ends_with(".dylib") ||
        name.ends_with(".o") ||
        name.ends_with(".a") ||
        name.ends_with(".lib") ||
        name.ends_with(".ttf") ||
        name.ends_with(".otf") ||
        name.ends_with(".woff") ||
        name.ends_with(".woff2") ||
        name.ends_with(".npy") ||
        name.ends_with(".npz") ||
        name.ends_with(".onnx") ||
        name.ends_with(".pb") ||
        name.ends_with(".tflite") ||
        name.ends_with(".parquet") ||
        name.ends_with(".orc") ||
        name.ends_with(".feather")
}
