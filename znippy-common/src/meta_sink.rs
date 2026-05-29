//! `ArchiveMetaSink` — abstraction over the archive's metadata layer.
//!
//! After the (unchanged) compression pipeline writes all blob bytes to disk, the
//! metadata layer — one Arrow IPC sub-index per `(pkg_type, repo)` group, a
//! manifest, and the `MULTI_INDEX_MAGIC` footer — is written through this trait.
//!
//! [`ArrowIpcSink`] reproduces the v0.7 on-disk format byte-for-byte. Future
//! backends (e.g. Iceberg) implement the same trait without touching the blob
//! pipeline.

use std::fs::File;
use std::os::unix::fs::FileExt;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use arrow::datatypes::Schema;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use crate::index::{MULTI_INDEX_MAGIC, ManifestEntry, write_manifest_bytes};

/// Identifies the logical sub-archive a sub-index belongs to.
#[derive(Debug, Clone)]
pub struct GroupKey {
    pub pkg_type: i8,
    pub repo: String,
    pub module_name: String,
}

/// Writes the archive metadata layer (sub-indexes + manifest + footer).
///
/// The blob bytes have already been written to the output by the compression
/// pipeline; implementations only decide how the metadata is materialized.
pub trait ArchiveMetaSink {
    /// Serialize one sub-index — an Arrow IPC stream of `batches` (one or more)
    /// — place it after the previously written region, and record a manifest
    /// entry for it.
    fn push_subindex(
        &mut self,
        schema: &Schema,
        batches: &[RecordBatch],
        key: GroupKey,
    ) -> Result<()>;

    /// Write the manifest + footer, fsync, and return the total file length.
    fn finish(self: Box<Self>) -> Result<u64>;
}

/// The default backend: inline Arrow IPC sub-indexes + manifest + 8-byte footer,
/// i.e. the v0.7 znippy container format. Behaviour is identical to the
/// previously-inlined writer tail in `slot_packer` / `stream_packer`.
pub struct ArrowIpcSink {
    file: Arc<File>,
    cursor: u64,
    entries: Vec<ManifestEntry>,
}

impl ArrowIpcSink {
    /// `blob_end_offset` is the byte offset just past the last blob — where the
    /// first sub-index is placed.
    pub fn new(file: Arc<File>, blob_end_offset: u64) -> Self {
        Self {
            file,
            cursor: blob_end_offset,
            entries: Vec::new(),
        }
    }
}

impl ArchiveMetaSink for ArrowIpcSink {
    fn push_subindex(
        &mut self,
        schema: &Schema,
        batches: &[RecordBatch],
        key: GroupKey,
    ) -> Result<()> {
        let sub_start = self.cursor;
        let mut sub_bytes: Vec<u8> = Vec::new();
        let mut sw = StreamWriter::try_new(&mut sub_bytes, schema)
            .map_err(|e| anyhow!("sub-index writer: {e}"))?;
        let mut row_count = 0u64;
        for batch in batches {
            row_count += batch.num_rows() as u64;
            sw.write(batch).map_err(|e| anyhow!("sub-index write: {e}"))?;
        }
        sw.finish().map_err(|e| anyhow!("sub-index finish: {e}"))?;

        let sub_len = sub_bytes.len() as u64;
        self.file.write_all_at(&sub_bytes, sub_start)?;
        self.cursor += sub_len;

        self.entries.push(ManifestEntry {
            pkg_type: key.pkg_type,
            repo: key.repo,
            module_name: key.module_name,
            index_offset: sub_start,
            index_len: sub_len,
            row_count,
        });
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<u64> {
        let manifest_offset = self.cursor;
        let manifest_bytes =
            write_manifest_bytes(&self.entries).map_err(|e| anyhow!("manifest: {e}"))?;
        self.file.write_all_at(&manifest_bytes, manifest_offset)?;

        let after = manifest_offset + manifest_bytes.len() as u64;
        self.file.write_all_at(&MULTI_INDEX_MAGIC, after)?;
        self.file.write_all_at(
            &manifest_offset.to_le_bytes(),
            after + MULTI_INDEX_MAGIC.len() as u64,
        )?;
        self.file.sync_all()?;

        Ok(after + MULTI_INDEX_MAGIC.len() as u64 + 8)
    }
}
