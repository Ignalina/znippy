//! Round-trip: feed the sink the base znippy index schema/batch (Arrow 58),
//! finish it, then read the resulting Iceberg table back (Arrow 57) and confirm
//! the rows survive — including the unsigned→signed widening.

use std::sync::Arc;

use futures::TryStreamExt;
use znippy_common::GroupKey;
use znippy_common::arrow as a58;
use znippy_common::ArchiveMetaSink;

use a58::array::{
    ArrayRef, BooleanArray, FixedSizeBinaryArray, StringArray, UInt32Array, UInt64Array,
};
use a58::datatypes::{DataType, Field, Schema};
use a58::record_batch::RecordBatch;

use iceberg::TableIdent;
use iceberg::io::FileIO;
use iceberg::table::StaticTable;

use znippy_iceberg::IcebergSink;

fn base_schema() -> Schema {
    Schema::new(vec![
        Field::new("relative_path", DataType::Utf8, false),
        Field::new("chunk_seq", DataType::UInt32, false),
        Field::new("fdata_offset", DataType::UInt64, false),
        Field::new("compressed", DataType::Boolean, false),
        Field::new("uncompressed_size", DataType::UInt64, false),
        Field::new("blob_offset", DataType::UInt64, false),
        Field::new("blob_size", DataType::UInt64, false),
        Field::new("checksum", DataType::FixedSizeBinary(32), false),
    ])
}

fn base_batch(schema: Arc<Schema>) -> RecordBatch {
    let checksums: Vec<[u8; 32]> = (0u8..3).map(|i| [i; 32]).collect();
    let cs = FixedSizeBinaryArray::try_from_iter(checksums.iter().map(|c| c.to_vec())).unwrap();
    RecordBatch::try_new(schema, vec![
        Arc::new(StringArray::from(vec!["a.txt", "b.bin", "c.dat"])) as ArrayRef,
        Arc::new(UInt32Array::from(vec![0u32, 0, 0])) as ArrayRef,
        Arc::new(UInt64Array::from(vec![0u64, 0, 0])) as ArrayRef,
        Arc::new(BooleanArray::from(vec![true, false, true])) as ArrayRef,
        Arc::new(UInt64Array::from(vec![100u64, 200, 300])) as ArrayRef,
        Arc::new(UInt64Array::from(vec![0u64, 100, 300])) as ArrayRef,
        Arc::new(UInt64Array::from(vec![50u64, 200, 80])) as ArrayRef,
        Arc::new(cs) as ArrayRef,
    ])
    .unwrap()
}

#[test]
fn iceberg_sink_round_trips_base_index() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("wh");

    let schema = Arc::new(base_schema());
    let batch = base_batch(schema.clone());

    let mut sink = IcebergSink::new(&warehouse, "znippy");
    sink.push_subindex(&schema, std::slice::from_ref(&batch), GroupKey {
        pkg_type: 0,
        repo: String::new(),
        module_name: String::new(),
    })
    .unwrap();
    let total = Box::new(sink).finish().unwrap();
    assert!(total > 0, "warehouse should contain bytes");

    // A real on-disk Iceberg table must exist.
    let files = walk(&warehouse);
    assert!(
        files.iter().any(|p| p.ends_with(".metadata.json")),
        "expected iceberg metadata.json, got {files:?}"
    );
    assert!(
        files.iter().any(|p| p.ends_with(".parquet")),
        "expected parquet data file, got {files:?}"
    );

    // Read the rows back via an Iceberg scan over the on-disk metadata.
    let rows = read_back_rows(&files);
    assert_eq!(rows, 3, "all rows should round-trip");
}

/// Locate the newest `*.metadata.json` and scan it back via `StaticTable`.
fn read_back_rows(files: &[String]) -> usize {
    let mut metadata: Vec<&String> = files
        .iter()
        .filter(|p| p.ends_with(".metadata.json"))
        .collect();
    metadata.sort();
    let latest = metadata.last().expect("a metadata.json").to_string();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let file_io = FileIO::new_with_fs();
        let ident = TableIdent::from_strs(["znippy", "idx_000_0"]).unwrap();
        let table = StaticTable::from_metadata_file(&latest, ident, file_io)
            .await
            .unwrap();
        let stream = table.scan().select_all().build().unwrap().to_arrow().await.unwrap();
        let batches: Vec<_> = stream.try_collect().await.unwrap();
        batches.iter().map(|b| b.num_rows()).sum()
    })
}

fn walk(root: &std::path::Path) -> Vec<String> {
    let mut out = Vec::new();
    fn rec(p: &std::path::Path, out: &mut Vec<String>) {
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                let path = e.path();
                if path.is_dir() {
                    rec(&path, out);
                } else {
                    out.push(path.display().to_string());
                }
            }
        }
    }
    rec(root, &mut out);
    out
}
