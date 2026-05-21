//! Plugin trait for archive-type-aware metadata extraction.
//!
//! Two modes:
//! - **Per-file**: plugin.extract_metadata() called inline (e.g. CargoPlugin — filename only)
//! - **Batch**: files staged in IngestBatch, plugin called once before compression starts.
//!   Data is still in memory (zero-copy borrows) — no extra allocation.
//!
//! The batch runs BETWEEN read and compress:
//!   read N files → batch extract (zero-copy &[u8]) → chunk → compress → write

use std::collections::HashMap;

/// Metadata extracted from a single file by a plugin
#[derive(Debug, Clone)]
pub struct ExtensionRow {
    /// Key-value pairs matching the extension struct fields
    pub fields: HashMap<String, ExtensionValue>,
}

/// Typed values for extension fields
#[derive(Debug, Clone, PartialEq)]
pub enum ExtensionValue {
    Str(String),
    OptStr(Option<String>),
    U32(u32),
    StrList(Vec<String>),
    Bytes(Vec<u8>),
}

/// Item in a batch — borrows data already in memory
pub struct BatchItem<'a> {
    pub path: &'a str,
    pub data: &'a [u8],
}

/// Trait implemented by each archive type plugin.
pub trait ArchiveTypePlugin: Send + Sync {
    /// Human-readable name
    fn name(&self) -> &str;

    /// DenseUnion type_id this plugin writes to
    fn type_id(&self) -> i8;

    /// Per-file extraction. Return None to skip.
    fn extract_metadata(&self, path: &str, data: &[u8]) -> Option<ExtensionRow>;

    /// Whether this plugin benefits from batched processing.
    fn supports_batch(&self) -> bool { false }

    /// Byte threshold to trigger batch flush (default 200MB).
    fn batch_threshold(&self) -> usize { 200 * 1024 * 1024 }

    /// Batch extraction — process many files at once (zero-copy borrows).
    /// Returns one Option<ExtensionRow> per input item, in same order.
    fn extract_batch(&self, items: &[BatchItem<'_>]) -> Vec<Option<ExtensionRow>> {
        items.iter().map(|item| self.extract_metadata(item.path, item.data)).collect()
    }
}

// ─── IngestBatch: zero-copy staging buffer ───────────────────────────

/// A batch of files staged for ingest. Holds file data in memory;
/// plugin borrows it (zero-copy) before compression begins.
pub struct IngestBatch {
    files: Vec<StagedFile>,
    total_bytes: usize,
}

/// A file staged in the batch — owns the data until compression consumes it
pub struct StagedFile {
    pub path: String,
    pub data: Vec<u8>,
    /// Metadata extracted by plugin (populated after extract phase)
    pub metadata: Option<ExtensionRow>,
}

impl IngestBatch {
    pub fn new() -> Self {
        Self { files: Vec::new(), total_bytes: 0 }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self { files: Vec::with_capacity(cap), total_bytes: 0 }
    }

    /// Stage a file. Data is moved in (caller gives up ownership).
    /// No copy — the Vec<u8> from the file read is moved directly here.
    pub fn push(&mut self, path: String, data: Vec<u8>) {
        self.total_bytes += data.len();
        self.files.push(StagedFile { path, data, metadata: None });
    }

    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    pub fn len(&self) -> usize {
        self.files.len()
    }

    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    /// Run the plugin's batch extraction on all staged files (zero-copy).
    /// After this call, each StagedFile.metadata is populated.
    pub fn extract_metadata(&mut self, plugin: &dyn ArchiveTypePlugin) {
        if self.files.is_empty() {
            return;
        }

        if plugin.supports_batch() {
            // Batch mode: one call, plugin sees all data via borrows
            let items: Vec<BatchItem<'_>> = self.files.iter()
                .map(|f| BatchItem { path: &f.path, data: &f.data })
                .collect();

            let results = plugin.extract_batch(&items);

            for (file, meta) in self.files.iter_mut().zip(results) {
                file.metadata = meta;
            }
        } else {
            // Per-file mode: call once per file (still zero-copy — borrows &data)
            for file in &mut self.files {
                file.metadata = plugin.extract_metadata(&file.path, &file.data);
            }
        }
    }

    /// Drain files for compression. Caller gets ownership of (path, data, metadata).
    /// After this, the batch is empty and ready for reuse.
    pub fn drain(&mut self) -> impl Iterator<Item = StagedFile> + '_ {
        self.total_bytes = 0;
        self.files.drain(..)
    }

    /// Iterate staged files (for inspection before drain)
    pub fn iter(&self) -> impl Iterator<Item = &StagedFile> {
        self.files.iter()
    }
}

impl Default for IngestBatch {
    fn default() -> Self {
        Self::new()
    }
}

// ─── PluginRegistry (simplified — batch logic moves to IngestBatch) ──

/// Registry holding the active plugin
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

    /// Get batch threshold from plugin (or default 200MB if no plugin)
    pub fn batch_threshold(&self) -> usize {
        self.plugin.as_ref().map(|p| p.batch_threshold()).unwrap_or(200 * 1024 * 1024)
    }

    /// Run extraction on a batch (delegates to plugin)
    pub fn extract_batch(&self, batch: &mut IngestBatch) {
        if let Some(plugin) = &self.plugin {
            batch.extract_metadata(plugin.as_ref());
        }
    }

    /// Per-file extract (convenience for non-batched paths)
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
