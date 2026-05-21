//! Native Cargo/crate registry plugin.
//! Parses .crate tarballs to extract Cargo.toml metadata.

use crate::plugin::{ArchiveTypePlugin, ExtensionRow, ExtensionValue};
use std::collections::HashMap;
use std::io::Read;

/// Native plugin that extracts crate metadata from .crate files.
/// Reads the embedded Cargo.toml to get name, version, and dependencies.
pub struct CargoPlugin;

impl CargoPlugin {
    pub fn new() -> Self {
        Self
    }

    /// Parse a .crate file (gzip tar) and extract Cargo.toml contents
    fn parse_crate_tarball(data: &[u8]) -> Option<CrateMeta> {
        use flate2::read::GzDecoder;
        use tar::Archive;

        let gz = GzDecoder::new(data);
        let mut archive = Archive::new(gz);

        for entry in archive.entries().ok()? {
            let mut entry = entry.ok()?;
            let path = entry.path().ok()?.to_string_lossy().to_string();
            if path.ends_with("/Cargo.toml") || path == "Cargo.toml" {
                let mut contents = String::new();
                entry.read_to_string(&mut contents).ok()?;
                return Self::parse_cargo_toml(&contents);
            }
        }
        None
    }

    /// Parse Cargo.toml content into CrateMeta
    fn parse_cargo_toml(contents: &str) -> Option<CrateMeta> {
        // Simple TOML parsing — extract [package] name, version, and [dependencies] keys
        let mut name = None;
        let mut version = None;
        let mut deps = Vec::new();
        let mut in_package = false;
        let mut in_deps = false;

        for line in contents.lines() {
            let trimmed = line.trim();
            if trimmed == "[package]" {
                in_package = true;
                in_deps = false;
            } else if trimmed == "[dependencies]" || trimmed == "[dev-dependencies]" || trimmed == "[build-dependencies]" {
                in_package = false;
                in_deps = trimmed == "[dependencies]";
            } else if trimmed.starts_with('[') {
                in_package = false;
                in_deps = false;
            } else if in_package {
                if let Some(val) = extract_toml_string(trimmed, "name") {
                    name = Some(val);
                } else if let Some(val) = extract_toml_string(trimmed, "version") {
                    version = Some(val);
                }
            } else if in_deps {
                // dep_name = "version" or dep_name = { version = "..." }
                if let Some(dep_name) = trimmed.split('=').next() {
                    let dep_name = dep_name.trim();
                    if !dep_name.is_empty() && !dep_name.starts_with('#') {
                        deps.push(dep_name.to_string());
                    }
                }
            }
        }

        Some(CrateMeta {
            name: name?,
            version: version?,
            deps,
        })
    }
}

/// Extract a string value from a simple TOML line like: key = "value"
fn extract_toml_string(line: &str, key: &str) -> Option<String> {
    let line = line.trim();
    if !line.starts_with(key) {
        return None;
    }
    let rest = line[key.len()..].trim();
    if !rest.starts_with('=') {
        return None;
    }
    let val = rest[1..].trim().trim_matches('"');
    Some(val.to_string())
}

struct CrateMeta {
    name: String,
    version: String,
    deps: Vec<String>,
}

impl ArchiveTypePlugin for CargoPlugin {
    fn name(&self) -> &str {
        "cargo_registry_v1"
    }

    fn type_id(&self) -> i8 {
        2 // Extension union type_id for cargo_registry_v1
    }

    fn extract_metadata(&self, path: &str, data: &[u8]) -> Option<ExtensionRow> {
        // Only process .crate files
        if !path.ends_with(".crate") {
            return None;
        }

        let meta = Self::parse_crate_tarball(data)?;

        let mut fields = HashMap::new();
        fields.insert("crate_name".into(), ExtensionValue::Str(meta.name));
        fields.insert("version".into(), ExtensionValue::Str(meta.version));
        fields.insert("deps".into(), ExtensionValue::StrList(meta.deps));

        Some(ExtensionRow { fields })
    }
}
