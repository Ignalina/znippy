//! Native Cargo/crate registry plugin.
//! Extracts crate name + version from .crate filenames (zero decompression cost).
//! Optionally parses Cargo.toml inside the tarball for deps (only if needed).

use crate::plugin::{ArchiveTypePlugin, ExtensionRow, ExtensionValue};
use std::collections::HashMap;

/// Native plugin that extracts crate metadata from .crate file paths.
/// Name and version are parsed from the filename (no I/O needed).
pub struct CargoPlugin {
    /// If true, also decompress and parse Cargo.toml for dependency list
    pub parse_deps: bool,
}

impl CargoPlugin {
    pub fn new() -> Self {
        Self { parse_deps: false }
    }

    pub fn with_deps() -> Self {
        Self { parse_deps: true }
    }

    /// Parse name and version from filename like "serde-1.0.200.crate"
    fn parse_filename(path: &str) -> Option<(String, String)> {
        let filename = path.rsplit('/').next()?;
        let stem = filename.strip_suffix(".crate")?;
        // Split at last hyphen followed by a digit (version start)
        let mut split_pos = None;
        for (i, c) in stem.char_indices() {
            if c == '-' {
                // Check if next char is a digit
                if let Some(next) = stem[i+1..].chars().next() {
                    if next.is_ascii_digit() {
                        split_pos = Some(i);
                    }
                }
            }
        }
        let pos = split_pos?;
        let name = &stem[..pos];
        let version = &stem[pos+1..];
        Some((name.to_string(), version.to_string()))
    }

    /// Parse deps from .crate tarball (only when parse_deps = true)
    #[cfg(feature = "ext-cargo")]
    fn parse_deps_from_tarball(data: &[u8]) -> Vec<String> {
        use flate2::read::GzDecoder;
        use tar::Archive;
        use std::io::Read;

        let gz = GzDecoder::new(data);
        let mut archive = Archive::new(gz);

        if let Ok(entries) = archive.entries() {
            for entry in entries.flatten() {
                let path = entry.path().ok().map(|p| p.to_string_lossy().to_string());
                if let Some(p) = path {
                    if p.ends_with("/Cargo.toml") || p == "Cargo.toml" {
                        let mut contents = String::new();
                        let mut entry = entry;
                        if entry.read_to_string(&mut contents).is_ok() {
                            return Self::extract_dep_names(&contents);
                        }
                    }
                }
            }
        }
        Vec::new()
    }

    #[cfg(feature = "ext-cargo")]
    fn extract_dep_names(cargo_toml: &str) -> Vec<String> {
        let mut deps = Vec::new();
        let mut in_deps = false;
        for line in cargo_toml.lines() {
            let trimmed = line.trim();
            if trimmed == "[dependencies]" {
                in_deps = true;
            } else if trimmed.starts_with('[') {
                in_deps = false;
            } else if in_deps {
                if let Some(dep_name) = trimmed.split('=').next() {
                    let dep_name = dep_name.trim();
                    if !dep_name.is_empty() && !dep_name.starts_with('#') {
                        deps.push(dep_name.to_string());
                    }
                }
            }
        }
        deps
    }
}

impl ArchiveTypePlugin for CargoPlugin {
    fn name(&self) -> &str {
        "cargo_registry_v1"
    }

    fn type_id(&self) -> i8 {
        2
    }

    fn matches_path(&self, path: &str) -> bool {
        path.ends_with(".crate")
    }

    fn extract_metadata(&self, path: &str, data: &[u8]) -> Option<ExtensionRow> {
        let (crate_name, version) = Self::parse_filename(path)?;

        let mut fields = HashMap::new();
        fields.insert("crate_name".into(), ExtensionValue::Str(crate_name));
        fields.insert("version".into(), ExtensionValue::Str(version));

        #[cfg(feature = "ext-cargo")]
        if self.parse_deps {
            let deps = Self::parse_deps_from_tarball(data);
            fields.insert("deps".into(), ExtensionValue::StrList(deps));
        }

        #[cfg(not(feature = "ext-cargo"))]
        let _ = data; // suppress unused warning

        Some(ExtensionRow { fields })
    }
}
