//! Integration test: IngestBatch + CargoPlugin zero-copy pipeline.
//!
//! Simulates ingesting .crate files, extracting metadata via plugin,
//! then verifying the batch extraction populated metadata correctly.

use znippy_common::plugin::{
    ArchiveTypePlugin, IngestBatch, PluginRegistry, ExtensionValue,
};
use znippy_common::plugins::cargo_native::CargoPlugin;

#[test]
fn test_ingest_batch_cargo_plugin_extracts_metadata() {
    let plugin = CargoPlugin::new();
    let registry = PluginRegistry::with_plugin(Box::new(plugin));

    let mut batch = IngestBatch::new();

    // Simulate staging .crate files (data is irrelevant for filename-only parsing)
    let crates = vec![
        ("registry/serde-1.0.200.crate", b"fake crate data 1" as &[u8]),
        ("registry/tokio-1.38.0.crate", b"fake crate data 2"),
        ("registry/rand-0.8.5.crate", b"fake crate data 3"),
        ("registry/syn-2.0.60.crate", b"fake crate data 4"),
        ("registry/not-a-crate.tar.gz", b"this should be skipped"),
    ];

    for (path, data) in &crates {
        batch.push(path.to_string(), data.to_vec());
    }

    assert_eq!(batch.len(), 5);
    assert_eq!(batch.total_bytes(), crates.iter().map(|(_, d)| d.len()).sum::<usize>());

    // Run plugin extraction (zero-copy: borrows data in batch)
    registry.extract_batch(&mut batch);

    // Verify metadata was extracted
    let files: Vec<_> = batch.iter().collect();

    // serde-1.0.200.crate → name=serde, version=1.0.200
    let meta = files[0].metadata.as_ref().expect("serde should have metadata");
    assert_eq!(meta.fields.get("crate_name"), Some(&ExtensionValue::Str("serde".into())));
    assert_eq!(meta.fields.get("version"), Some(&ExtensionValue::Str("1.0.200".into())));

    // tokio-1.38.0.crate
    let meta = files[1].metadata.as_ref().expect("tokio should have metadata");
    assert_eq!(meta.fields.get("crate_name"), Some(&ExtensionValue::Str("tokio".into())));
    assert_eq!(meta.fields.get("version"), Some(&ExtensionValue::Str("1.38.0".into())));

    // rand-0.8.5.crate
    let meta = files[2].metadata.as_ref().expect("rand should have metadata");
    assert_eq!(meta.fields.get("crate_name"), Some(&ExtensionValue::Str("rand".into())));
    assert_eq!(meta.fields.get("version"), Some(&ExtensionValue::Str("0.8.5".into())));

    // syn-2.0.60.crate
    let meta = files[3].metadata.as_ref().expect("syn should have metadata");
    assert_eq!(meta.fields.get("crate_name"), Some(&ExtensionValue::Str("syn".into())));
    assert_eq!(meta.fields.get("version"), Some(&ExtensionValue::Str("2.0.60".into())));

    // not-a-crate.tar.gz → None (not a .crate file)
    assert!(files[4].metadata.is_none(), "non-crate should have no metadata");
}

#[test]
fn test_ingest_batch_drain_transfers_ownership() {
    let plugin = CargoPlugin::new();
    let registry = PluginRegistry::with_plugin(Box::new(plugin));

    let mut batch = IngestBatch::new();
    batch.push("crates/hyper-1.0.0.crate".into(), vec![0u8; 1024]);
    batch.push("crates/reqwest-0.12.0.crate".into(), vec![1u8; 2048]);

    registry.extract_batch(&mut batch);

    // Drain: ownership moves out, batch becomes empty
    let drained: Vec<_> = batch.drain().collect();
    assert_eq!(drained.len(), 2);
    assert_eq!(drained[0].path, "crates/hyper-1.0.0.crate");
    assert_eq!(drained[0].data.len(), 1024);
    assert!(drained[0].metadata.is_some());
    assert_eq!(drained[1].path, "crates/reqwest-0.12.0.crate");
    assert_eq!(drained[1].data.len(), 2048);

    // Batch is now empty and reusable
    assert!(batch.is_empty());
    assert_eq!(batch.total_bytes(), 0);
}

#[test]
fn test_ingest_batch_threshold_check() {
    let plugin = CargoPlugin::new();

    let mut batch = IngestBatch::new();

    // Default threshold is 200MB — our batch is small
    assert!(batch.total_bytes() < plugin.batch_threshold());

    // Push 200MB+ would trigger (simulated)
    batch.push("big.crate".into(), vec![0u8; 100 * 1024 * 1024]);
    batch.push("big2.crate".into(), vec![0u8; 101 * 1024 * 1024]);
    assert!(batch.total_bytes() > plugin.batch_threshold());
}

#[test]
fn test_cargo_plugin_hyphenated_names() {
    // Crate names with hyphens: split at LAST hyphen before digit
    let plugin = CargoPlugin::new();

    let cases = vec![
        ("x/serde-derive-1.0.200.crate", "serde-derive", "1.0.200"),
        ("x/tokio-macros-2.3.0.crate", "tokio-macros", "2.3.0"),
        ("x/proc-macro2-1.0.82.crate", "proc-macro2", "1.0.82"),
        ("x/utf8-width-0.1.7.crate", "utf8-width", "0.1.7"),
    ];

    for (path, expected_name, expected_version) in cases {
        let row = plugin.extract_metadata(path, b"").expect(&format!("should parse {}", path));
        assert_eq!(row.fields.get("crate_name"), Some(&ExtensionValue::Str(expected_name.into())),
            "failed name for {}", path);
        assert_eq!(row.fields.get("version"), Some(&ExtensionValue::Str(expected_version.into())),
            "failed version for {}", path);
    }
}
