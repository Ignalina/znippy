use std::path::{Path, PathBuf};

/// Metadata om en fil i ett `.znippy`-arkiv.

use once_cell::sync::Lazy;
use arrow::datatypes::{DataType, Field, Fields, Schema};

use std::sync::Arc;


pub static ZNIPPY_INDEX_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("relative_path", DataType::Utf8, false),
        Field::new("compressed", DataType::Boolean, false),
        Field::new("uncompressed_size", DataType::UInt64, false),
        Field::new("checksum", DataType::FixedSizeBinary(32), false),
        Field::new(
            "chunks",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Field::new("offset", DataType::UInt64, false),
                    Field::new("length", DataType::UInt64, false),
                ])),
                false,
            ))),
            false,
        ),
    ]))
});

pub fn znippy_index_schema() -> &'static Arc<Schema> {
    &ZNIPPY_INDEX_SCHEMA
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
            "apk" | "dmg" | "deb" | "rpm" | "arrow"
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
