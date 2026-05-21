//! Plugin trait for archive-type-aware metadata extraction.
//!
//! During ingest, znippy calls the active plugin for each file to extract
//! structured metadata (crate name, version, deps, etc.) into the extension column.
//!
//! Two modes:
//! - **Per-file**: plugin.extract_metadata() called inline (e.g. CargoPlugin — filename only)
//! - **Batch**: files queued until threshold, then plugin.extract_batch() called once
//!   (e.g. MavenPlugin — ljar processes 200MB of JARs in one multi-core pass)

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

/// Item queued for batch extraction
pub struct BatchItem<'a> {
    pub path: &'a str,
    pub data: &'a [u8],
}

/// Trait implemented by each archive type plugin.
/// Called during ingest — NOT in the compression hot path.
pub trait ArchiveTypePlugin: Send + Sync {
    /// Human-readable name
    fn name(&self) -> &str;

    /// DenseUnion type_id this plugin writes to
    fn type_id(&self) -> i8;

    /// Per-file extraction. Return None to skip.
    fn extract_metadata(&self, path: &str, data: &[u8]) -> Option<ExtensionRow>;

    /// Whether this plugin benefits from batched processing.
    /// When true, files are queued and extract_batch() called at threshold.
    fn supports_batch(&self) -> bool { false }

    /// Byte threshold to trigger batch flush (default 200MB).
    fn batch_threshold(&self) -> usize { 200 * 1024 * 1024 }

    /// Batch extraction — process many files at once for parallelism.
    /// Returns one Option<ExtensionRow> per input item, in same order.
    /// Default: falls back to per-file extract_metadata.
    fn extract_batch(&self, items: &[BatchItem<'_>]) -> Vec<Option<ExtensionRow>> {
        items.iter().map(|item| self.extract_metadata(item.path, item.data)).collect()
    }
}

/// Registry holding the active plugin + batch queue
pub struct PluginRegistry {
    plugin: Option<Box<dyn ArchiveTypePlugin>>,
    queue: Vec<QueuedFile>,
    queue_bytes: usize,
}

/// A file waiting in the batch queue
struct QueuedFile {
    path: String,
    data: Vec<u8>,
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self { plugin: None, queue: Vec::new(), queue_bytes: 0 }
    }

    pub fn with_plugin(plugin: Box<dyn ArchiveTypePlugin>) -> Self {
        Self { plugin: Some(plugin), queue: Vec::new(), queue_bytes: 0 }
    }

    /// Per-file extract (for non-batch plugins or immediate needs)
    pub fn extract(&self, path: &str, data: &[u8]) -> Option<ExtensionRow> {
        self.plugin.as_ref()?.extract_metadata(path, data)
    }

    /// Queue a file for batch extraction. Returns batch results if threshold reached.
    pub fn queue_or_extract(&mut self, path: &str, data: &[u8]) -> QueueResult {
        let plugin = match &self.plugin {
            Some(p) => p,
            None => return QueueResult::None,
        };

        if !plugin.supports_batch() {
            let row = plugin.extract_metadata(path, data);
            return QueueResult::Immediate(row);
        }

        // Batch mode: queue until threshold
        self.queue_bytes += data.len();
        self.queue.push(QueuedFile { path: path.to_string(), data: data.to_vec() });

        if self.queue_bytes >= plugin.batch_threshold() {
            QueueResult::Batch(self.flush_batch())
        } else {
            QueueResult::Queued
        }
    }

    /// Flush any remaining queued files (call at end of ingest or Arrow batch boundary)
    pub fn flush_batch(&mut self) -> Vec<(String, Option<ExtensionRow>)> {
        let plugin = match &self.plugin {
            Some(p) => p,
            None => return Vec::new(),
        };

        if self.queue.is_empty() {
            return Vec::new();
        }

        let items: Vec<BatchItem<'_>> = self.queue.iter()
            .map(|f| BatchItem { path: &f.path, data: &f.data })
            .collect();

        let results = plugin.extract_batch(&items);

        let output: Vec<(String, Option<ExtensionRow>)> = self.queue.drain(..)
            .zip(results)
            .map(|(f, row)| (f.path, row))
            .collect();

        self.queue_bytes = 0;
        output
    }

    pub fn type_id(&self) -> Option<i8> {
        self.plugin.as_ref().map(|p| p.type_id())
    }

    pub fn has_plugin(&self) -> bool {
        self.plugin.is_some()
    }

    pub fn queue_len(&self) -> usize {
        self.queue.len()
    }

    pub fn queue_bytes(&self) -> usize {
        self.queue_bytes
    }
}

/// Result of queue_or_extract
pub enum QueueResult {
    /// No plugin active
    None,
    /// Per-file result (non-batch plugin)
    Immediate(Option<ExtensionRow>),
    /// File queued, threshold not reached yet
    Queued,
    /// Threshold reached, here are all queued results: (path, metadata)
    Batch(Vec<(String, Option<ExtensionRow>)>),
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}
