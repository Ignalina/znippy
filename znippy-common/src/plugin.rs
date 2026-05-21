//! Plugin trait for archive-type-aware metadata extraction.
//!
//! During ingest, znippy calls the active plugin for each file to extract
//! structured metadata (crate name, version, deps, etc.) into the extension column.

use std::collections::HashMap;

/// Metadata extracted from a single file by a plugin
#[derive(Debug, Clone)]
pub struct ExtensionRow {
    /// Key-value pairs matching the extension struct fields
    pub fields: HashMap<String, ExtensionValue>,
}

/// Typed values for extension fields
#[derive(Debug, Clone)]
pub enum ExtensionValue {
    Str(String),
    OptStr(Option<String>),
    U32(u32),
    StrList(Vec<String>),
    Bytes(Vec<u8>),
}

/// Trait implemented by each archive type plugin.
/// Called once per file during ingest — NOT in the compression hot path.
pub trait ArchiveTypePlugin: Send + Sync {
    /// Human-readable name
    fn name(&self) -> &str;

    /// DenseUnion type_id this plugin writes to
    fn type_id(&self) -> i8;

    /// Inspect file path + bytes, return extension metadata.
    /// Return None to skip (file not relevant to this plugin).
    fn extract_metadata(&self, path: &str, data: &[u8]) -> Option<ExtensionRow>;
}

/// Registry holding the active plugin (if any)
pub struct PluginRegistry {
    plugin: Option<Box<dyn ArchiveTypePlugin>>,
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self { plugin: None }
    }

    pub fn with_plugin(plugin: Box<dyn ArchiveTypePlugin>) -> Self {
        Self { plugin: Some(plugin) }
    }

    pub fn extract(&self, path: &str, data: &[u8]) -> Option<ExtensionRow> {
        self.plugin.as_ref()?.extract_metadata(path, data)
    }

    pub fn type_id(&self) -> Option<i8> {
        self.plugin.as_ref().map(|p| p.type_id())
    }

    pub fn has_plugin(&self) -> bool {
        self.plugin.is_some()
    }
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}
