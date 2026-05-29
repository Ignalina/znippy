//! `IcebergSink` — an [`ArchiveMetaSink`] backend that materializes the archive
//! metadata layer as an Apache Iceberg table instead of the inline Arrow-IPC +
//! manifest + footer container.
//!
//! ## Constraints honoured
//! * The blob/compression pipeline is **untouched**: compressed blob bytes still
//!   live in the `.znippy` sidecar produced by the unchanged writer. This sink
//!   only consumes the same metadata `RecordBatch`es the Arrow-IPC sink receives
//!   and writes them into an Iceberg table; the index rows still carry
//!   `blob_offset` / `blob_size` referencing that sidecar.
//! * **tokio is confined to this crate.** The [`ArchiveMetaSink`] trait is sync;
//!   the Iceberg writer API is async, so this sink bridges async → sync with a
//!   private current-thread-free multi-thread runtime and `block_on` inside
//!   [`IcebergSink::finish`]. No other crate gains a tokio dependency.
//!
//! ## Version bridge
//! znippy uses Arrow **58**; the `iceberg` crate uses Arrow **57**. The two
//! `RecordBatch` types are not interchangeable, so [`IcebergSink::push_subindex`]
//! serializes each sub-index to a stable Arrow-IPC byte stream (Arrow 58) and
//! [`IcebergSink::finish`] re-reads it with Arrow 57 before handing it to Iceberg.
//!
//! ## Type mapping
//! Iceberg has no unsigned integer types, so unsigned/narrow columns are widened:
//! `UInt8/UInt16/UInt32/Int8/Int16 → Int32` and `UInt64 → Int64`. All other
//! column types (`Utf8`, `Boolean`, `FixedSizeBinary`, ...) map directly.
//!
//! ## Layout
//! One Iceberg namespace per archive; one table per `(pkg_type, repo, module)`
//! sub-index group (the same grouping the manifest uses).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};

use znippy_common::arrow as arrow58;
use znippy_common::{ArchiveMetaSink, GroupKey};

use arrow_array::RecordBatch as RecordBatch57;
use arrow_schema::{DataType as DataType57, Field as Field57, Schema as Schema57};

use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
use parquet::file::properties::WriterProperties;

/// One serialized sub-index: Arrow-58 IPC bytes plus the group it belongs to.
struct PendingSubindex {
    ipc_bytes: Vec<u8>,
    key: GroupKey,
}

/// [`ArchiveMetaSink`] backend that writes the metadata layer as an Iceberg table.
pub struct IcebergSink {
    warehouse: PathBuf,
    namespace: String,
    pending: Vec<PendingSubindex>,
}

impl IcebergSink {
    /// Create a sink that writes into a local-filesystem Iceberg warehouse at
    /// `warehouse`, placing each sub-index in namespace `namespace`.
    pub fn new(warehouse: impl Into<PathBuf>, namespace: impl Into<String>) -> Self {
        Self {
            warehouse: warehouse.into(),
            namespace: namespace.into(),
            pending: Vec::new(),
        }
    }
}

impl ArchiveMetaSink for IcebergSink {
    fn push_subindex(
        &mut self,
        schema: &arrow58::datatypes::Schema,
        batches: &[arrow58::record_batch::RecordBatch],
        key: GroupKey,
    ) -> Result<()> {
        let mut ipc_bytes: Vec<u8> = Vec::new();
        {
            let mut sw = arrow58::ipc::writer::StreamWriter::try_new(&mut ipc_bytes, schema)
                .map_err(|e| anyhow!("iceberg sink: ipc writer: {e}"))?;
            for batch in batches {
                sw.write(batch)
                    .map_err(|e| anyhow!("iceberg sink: ipc write: {e}"))?;
            }
            sw.finish()
                .map_err(|e| anyhow!("iceberg sink: ipc finish: {e}"))?;
        }
        self.pending.push(PendingSubindex { ipc_bytes, key });
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<u64> {
        std::fs::create_dir_all(&self.warehouse)
            .with_context(|| format!("creating warehouse dir {}", self.warehouse.display()))?;

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .context("iceberg sink: building tokio runtime")?;

        rt.block_on(self.write_all())?;

        Ok(dir_size_bytes(&self.warehouse))
    }
}

impl IcebergSink {
    async fn write_all(&self) -> Result<()> {
        let warehouse_uri = format!("file://{}", self.warehouse.display());

        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_uri)]),
            )
            .await
            .context("iceberg sink: loading memory catalog")?;

        let ns = NamespaceIdent::new(self.namespace.clone());
        if !catalog
            .namespace_exists(&ns)
            .await
            .context("iceberg sink: namespace_exists")?
        {
            catalog
                .create_namespace(&ns, HashMap::new())
                .await
                .context("iceberg sink: create_namespace")?;
        }

        for (i, sub) in self.pending.iter().enumerate() {
            let table_name = table_name_for(&sub.key, i);
            self.write_subindex(&catalog, &ns, &table_name, &sub.ipc_bytes)
                .await
                .with_context(|| format!("iceberg sink: writing sub-index '{table_name}'"))?;
        }
        Ok(())
    }

    async fn write_subindex(
        &self,
        catalog: &dyn Catalog,
        ns: &NamespaceIdent,
        table_name: &str,
        ipc_bytes: &[u8],
    ) -> Result<()> {
        let batches = read_ipc_as_arrow57(ipc_bytes)?;
        let casted: Vec<RecordBatch57> = batches
            .iter()
            .map(cast_for_iceberg)
            .collect::<Result<_>>()?;

        let arrow_schema = casted
            .first()
            .map(|b| b.schema())
            .ok_or_else(|| anyhow!("sub-index has no batches"))?;

        let ice_schema = arrow_schema_to_schema_auto_assign_ids(arrow_schema.as_ref())
            .map_err(|e| anyhow!("arrow→iceberg schema: {e}"))?;

        let creation = TableCreation::builder()
            .name(table_name.to_string())
            .schema(ice_schema)
            .build();
        let table = catalog
            .create_table(ns, creation)
            .await
            .context("create_table")?;

        // Arrow schema carrying Iceberg field-id metadata, required by the writer.
        let table_arrow_schema: Arc<Schema57> =
            Arc::new(table.metadata().current_schema().as_ref().try_into()?);

        let location_generator = DefaultLocationGenerator::new(table.metadata().clone())
            .map_err(|e| anyhow!("location generator: {e}"))?;
        let file_name_generator = DefaultFileNameGenerator::new(
            "znippy".to_string(),
            None,
            iceberg::spec::DataFileFormat::Parquet,
        );
        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::default(),
            table.metadata().current_schema().clone(),
        );
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            table.file_io().clone(),
            location_generator,
            file_name_generator,
        );
        let data_file_writer_builder = DataFileWriterBuilder::new(rolling);
        let mut data_file_writer = data_file_writer_builder
            .build(None)
            .await
            .map_err(|e| anyhow!("build data file writer: {e}"))?;

        for batch in &casted {
            let aligned =
                RecordBatch57::try_new(table_arrow_schema.clone(), batch.columns().to_vec())
                    .context("aligning batch to table schema")?;
            data_file_writer
                .write(aligned)
                .await
                .map_err(|e| anyhow!("write batch: {e}"))?;
        }
        let data_files = data_file_writer
            .close()
            .await
            .map_err(|e| anyhow!("close data file writer: {e}"))?;

        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(data_files);
        let tx = action.apply(tx).map_err(|e| anyhow!("fast_append apply: {e}"))?;
        tx.commit(catalog)
            .await
            .map_err(|e| anyhow!("commit: {e}"))?;
        Ok(())
    }
}

/// Read Arrow-IPC bytes (written by Arrow 58) back as Arrow-57 record batches.
/// The IPC stream format is stable across these adjacent major versions.
fn read_ipc_as_arrow57(bytes: &[u8]) -> Result<Vec<RecordBatch57>> {
    let reader = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|e| anyhow!("ipc read (arrow57): {e}"))?;
    let mut out = Vec::new();
    for batch in reader {
        out.push(batch.map_err(|e| anyhow!("ipc batch decode: {e}"))?);
    }
    Ok(out)
}

/// Widen unsigned/narrow integer columns to the signed types Iceberg supports.
fn cast_for_iceberg(batch: &RecordBatch57) -> Result<RecordBatch57> {
    let schema = batch.schema();
    let mut fields: Vec<Field57> = Vec::with_capacity(schema.fields().len());
    let mut columns = Vec::with_capacity(schema.fields().len());

    for (i, field) in schema.fields().iter().enumerate() {
        let col = batch.column(i);
        let target = match field.data_type() {
            DataType57::UInt8
            | DataType57::UInt16
            | DataType57::UInt32
            | DataType57::Int8
            | DataType57::Int16 => Some(DataType57::Int32),
            DataType57::UInt64 => Some(DataType57::Int64),
            _ => None,
        };
        match target {
            Some(dt) => {
                let casted = arrow_cast::cast(col, &dt)
                    .map_err(|e| anyhow!("cast column '{}': {e}", field.name()))?;
                fields.push(Field57::new(field.name(), dt, field.is_nullable()));
                columns.push(casted);
            }
            None => {
                fields.push(field.as_ref().clone());
                columns.push(col.clone());
            }
        }
    }

    let new_schema = Arc::new(Schema57::new(fields));
    RecordBatch57::try_new(new_schema, columns).map_err(|e| anyhow!("rebuild casted batch: {e}"))
}

/// Build a valid, unique Iceberg table name from a sub-index group key.
fn table_name_for(key: &GroupKey, index: usize) -> String {
    let sanitize = |s: &str| -> String {
        let cleaned: String = s
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        cleaned.trim_matches('_').to_string()
    };
    let repo = sanitize(&key.repo);
    let module = sanitize(&key.module_name);
    let mut name = format!("idx_{:03}", (key.pkg_type as i16) & 0xff);
    if !repo.is_empty() {
        name.push('_');
        name.push_str(&repo);
    }
    if !module.is_empty() {
        name.push('_');
        name.push_str(&module);
    }
    // Guarantee uniqueness even if two groups sanitize to the same name.
    format!("{name}_{index}")
}

/// Total size in bytes of all regular files under `root` (the warehouse).
fn dir_size_bytes(root: &Path) -> u64 {
    fn rec(p: &Path, acc: &mut u64) {
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                let path = e.path();
                if path.is_dir() {
                    rec(&path, acc);
                } else if let Ok(meta) = e.metadata() {
                    *acc += meta.len();
                }
            }
        }
    }
    let mut acc = 0u64;
    rec(root, &mut acc);
    acc
}
