//! Integration tests for znippy: compress → decompress round-trip.

use anyhow::Result;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use tempfile::TempDir;
use znippy_common::{ZnippyArchive, ZnippyReader};
use znippy_common::{ManifestEntry, interpret_footer, read_manifest_bytes, write_manifest_bytes};
use znippy_compress::{ArchiveEntry, compress_dir, compress_stream};

/// Helper: decompress an archive and return map of relative_path → file contents
fn decompress_to_map(archive_path: &Path) -> Result<HashMap<String, Vec<u8>>> {
    let out_dir = TempDir::new()?;
    let report = znippy_common::decompress_archive(archive_path, true, out_dir.path())?;
    assert_eq!(report.corrupt_files, 0, "Corrupt files detected during decompression");

    let mut result = HashMap::new();
    for entry in walkdir::WalkDir::new(out_dir.path())
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        let rel = entry
            .path()
            .strip_prefix(out_dir.path())
            .unwrap()
            .to_string_lossy()
            .to_string();
        let data = fs::read(entry.path())?;
        result.insert(rel, data);
    }
    Ok(result)
}

// ─── Stream compressor tests ───────────────────────────────────────────────

#[test]
fn test_stream_compress_single_small_file() -> Result<()> {
    let out_dir = TempDir::new()?;
    let archive_path = out_dir.path().join("test.znippy");

    let content = b"Hello, Znippy! This is a small test file.";
    let compressor = compress_stream(&archive_path.to_path_buf(), false)?;
    compressor.sender().send(ArchiveEntry {
        relative_path: "hello.txt".to_string(),
        data: content.to_vec(),
        pkg_type: None,
        repo: None,
    })?;
    let report = compressor.finish()?;

    assert_eq!(report.total_files, 1);
    assert!(archive_path.exists(), ".znippy file should exist");
    // v2 format: single file, no separate .zdata
    assert!(
        !archive_path.with_extension("zdata").exists(),
        ".zdata file should NOT exist in v2 format"
    );

    // Round-trip: decompress and verify
    let files = decompress_to_map(&archive_path)?;
    assert_eq!(files.len(), 1);
    assert_eq!(files.get("hello.txt").unwrap().as_slice(), content);

    Ok(())
}

#[test]
fn test_stream_compress_multiple_files() -> Result<()> {
    let out_dir = TempDir::new()?;
    let archive_path = out_dir.path().join("multi.znippy");

    let files_in: Vec<(&str, Vec<u8>)> = vec![
        ("file1.txt", b"Content of file 1".to_vec()),
        ("subdir/file2.txt", b"Content of file 2 in subdir".to_vec()),
        (
            "binary.bin",
            (0..256).cycle().take(4096).map(|x| x as u8).collect(),
        ),
    ];

    let compressor = compress_stream(&archive_path.to_path_buf(), false)?;
    for (path, data) in &files_in {
        compressor.sender().send(ArchiveEntry {
            relative_path: path.to_string(),
            data: data.clone(),
            pkg_type: None,
            repo: None,
        })?;
    }
    let report = compressor.finish()?;

    assert_eq!(report.total_files, 3);

    let files_out = decompress_to_map(&archive_path)?;
    assert_eq!(files_out.len(), 3);
    for (path, data) in &files_in {
        assert_eq!(
            files_out.get(*path).unwrap(),
            data,
            "Mismatch for file: {}",
            path
        );
    }

    Ok(())
}

#[test]
fn test_stream_compress_empty_file() -> Result<()> {
    let out_dir = TempDir::new()?;
    let archive_path = out_dir.path().join("empty.znippy");

    let compressor = compress_stream(&archive_path.to_path_buf(), false)?;
    compressor.sender().send(ArchiveEntry {
        relative_path: "empty.txt".to_string(),
        data: vec![],
        pkg_type: None,
        repo: None,
    })?;
    let report = compressor.finish()?;

    assert_eq!(report.total_files, 1);

    // v2: empty file still gets one row (chunk_seq=0 with empty compressed data)
    let (_schema, batches) = znippy_common::read_znippy_index(&archive_path)?;
    assert_eq!(batches[0].num_rows(), 1);

    Ok(())
}

#[test]
fn test_stream_compress_large_file_multi_chunk() -> Result<()> {
    let out_dir = TempDir::new()?;
    let archive_path = out_dir.path().join("large.znippy");

    // Create a file larger than file_split_block_size (10MB default) to force multiple chunks
    let size = 12 * 1024 * 1024; // 12 MB
    let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();

    let compressor = compress_stream(&archive_path.to_path_buf(), false)?;
    compressor.sender().send(ArchiveEntry {
        relative_path: "large.bin".to_string(),
        data: data.clone(),
        pkg_type: None,
        repo: None,
    })?;
    let report = compressor.finish()?;

    assert_eq!(report.total_files, 1);
    assert!(report.chunks >= 2, "Expected multiple chunks for 12MB file, got {}", report.chunks);

    let files_out = decompress_to_map(&archive_path)?;
    assert_eq!(files_out.get("large.bin").unwrap().as_slice(), data.as_slice());

    Ok(())
}

#[test]
fn test_stream_compress_already_compressed_file_skipped() -> Result<()> {
    let out_dir = TempDir::new()?;
    let archive_path = out_dir.path().join("skip.znippy");

    let data = vec![0xAA; 1024];

    let compressor = compress_stream(&archive_path.to_path_buf(), false)?;
    compressor.sender().send(ArchiveEntry {
        relative_path: "image.png".to_string(),
        data: data.clone(),
        pkg_type: None,
        repo: None,
    })?;
    let report = compressor.finish()?;

    assert_eq!(report.total_files, 1);
    // .png should be marked as uncompressed (skipped)
    assert_eq!(report.uncompressed_files, 1);

    let files_out = decompress_to_map(&archive_path)?;
    assert_eq!(files_out.get("image.png").unwrap().as_slice(), data.as_slice());

    Ok(())
}

#[test]
fn test_stream_compress_no_skip_forces_compression() -> Result<()> {
    let out_dir = TempDir::new()?;
    let archive_path = out_dir.path().join("noskip.znippy");

    let data = vec![0xBB; 2048];

    let compressor = compress_stream(&archive_path.to_path_buf(), true)?; // no_skip=true
    compressor.sender().send(ArchiveEntry {
        relative_path: "image.png".to_string(),
        data: data.clone(),
        pkg_type: None,
        repo: None,
    })?;
    let report = compressor.finish()?;

    // With no_skip=true, even .png files should be compressed
    assert_eq!(report.compressed_files, 1);
    assert_eq!(report.uncompressed_files, 0);

    let files_out = decompress_to_map(&archive_path)?;
    assert_eq!(files_out.get("image.png").unwrap().as_slice(), data.as_slice());

    Ok(())
}

#[test]
fn test_stream_compress_empty_archive() -> Result<()> {
    let out_dir = TempDir::new()?;
    let archive_path = out_dir.path().join("none.znippy");

    let compressor = compress_stream(&archive_path.to_path_buf(), false)?;
    // Send nothing
    let report = compressor.finish()?;

    assert_eq!(report.total_files, 0);
    Ok(())
}

// ─── Directory compressor tests ────────────────────────────────────────────

#[test]
fn test_compress_dir_basic() -> Result<()> {
    let input_dir = TempDir::new()?;
    let out_dir = TempDir::new()?;

    // Create test files
    fs::write(input_dir.path().join("a.txt"), "alpha")?;
    fs::create_dir_all(input_dir.path().join("sub"))?;
    fs::write(input_dir.path().join("sub/b.txt"), "beta")?;

    let archive_path = out_dir.path().join("dir_test.znippy");
    let report = compress_dir(
        &input_dir.path().to_path_buf(),
        &archive_path.to_path_buf(),
        false,
        None,
        None,
    )?;

    assert_eq!(report.total_files, 2);
    assert!(archive_path.exists());
    // v2 format: single file, no separate .zdata
    assert!(!archive_path.with_extension("zdata").exists());

    // Decompress and verify
    let decomp_dir = TempDir::new()?;
    let verify = znippy_common::decompress_archive(&archive_path, true, decomp_dir.path())?;
    assert_eq!(verify.corrupt_files, 0);
    assert_eq!(verify.total_files, 2);

    // Check file contents
    let a_content = fs::read_to_string(decomp_dir.path().join("a.txt"))?;
    assert_eq!(a_content, "alpha");
    let b_content = fs::read_to_string(decomp_dir.path().join("sub/b.txt"))?;
    assert_eq!(b_content, "beta");

    Ok(())
}

#[test]
fn test_compress_dir_with_mixed_file_types() -> Result<()> {
    let input_dir = TempDir::new()?;
    let out_dir = TempDir::new()?;

    // Normal text file (will be compressed)
    fs::write(input_dir.path().join("readme.md"), "# Hello\nThis is content")?;
    // Already-compressed extension (will be skipped by default)
    fs::write(input_dir.path().join("data.gz"), vec![0x1F, 0x8B, 0x08, 0x00])?;

    let archive_path = out_dir.path().join("mixed.znippy");
    let report = compress_dir(
        &input_dir.path().to_path_buf(),
        &archive_path.to_path_buf(),
        false,
        None,
        None,
    )?;

    assert_eq!(report.total_files, 2);
    assert_eq!(report.compressed_files, 1); // readme.md
    assert_eq!(report.uncompressed_files, 1); // data.gz

    // Round-trip
    let decomp_dir = TempDir::new()?;
    let verify = znippy_common::decompress_archive(&archive_path, true, decomp_dir.path())?;
    assert_eq!(verify.corrupt_files, 0);

    let readme = fs::read_to_string(decomp_dir.path().join("readme.md"))?;
    assert_eq!(readme, "# Hello\nThis is content");

    Ok(())
}

#[test]
fn test_compress_dir_slots_roundtrip() -> Result<()> {
    let input_dir = TempDir::new()?;
    let out_dir = TempDir::new()?;

    // Many small files (coalesce into one slot, 1 file = 1 slice).
    for i in 0..50 {
        fs::write(
            input_dir.path().join(format!("small_{i}.txt")),
            format!("content of small file number {i}\n").repeat(3),
        )?;
    }
    // Nested small file.
    fs::create_dir_all(input_dir.path().join("nested"))?;
    fs::write(input_dir.path().join("nested/deep.txt"), "deep content")?;
    // Pre-compressed extension → skip path.
    fs::write(input_dir.path().join("blob.gz"), vec![0x1F, 0x8B, 0x08, 0x00, 1, 2, 3])?;
    // Empty file → one zero-length slice.
    fs::write(input_dir.path().join("empty.txt"), "")?;
    // Large compressible file that spans many slices (> slice_size of 200MB/N is
    // unrealistic for a test, so we just make it multi-MB to exercise chunking).
    let big: Vec<u8> = (0..(8 * 1024 * 1024)).map(|i| (i % 251) as u8).collect();
    fs::write(input_dir.path().join("big.bin"), &big)?;

    let archive_path = out_dir.path().join("slots.znippy");
    let report = compress_dir(
        &input_dir.path().to_path_buf(),
        &archive_path.to_path_buf(),
        false,
        None,
        None,
    )?;
    assert_eq!(report.total_files, 54);
    assert!(archive_path.exists());

    // Decompress + per-slice verify must report zero corruption.
    let decomp_dir = TempDir::new()?;
    let verify = znippy_common::decompress_archive(&archive_path, true, decomp_dir.path())?;
    assert_eq!(verify.corrupt_files, 0, "per-slice blake3 verify found corruption");
    assert_eq!(verify.total_files, 54);

    // Empty file must round-trip to an empty file.
    let empty_out = fs::read_to_string(decomp_dir.path().join("empty.txt"))?;
    assert_eq!(empty_out, "");

    // Spot-check round-tripped contents.
    let s7 = fs::read_to_string(decomp_dir.path().join("small_7.txt"))?;
    assert_eq!(s7, "content of small file number 7\n".repeat(3));
    let deep = fs::read_to_string(decomp_dir.path().join("nested/deep.txt"))?;
    assert_eq!(deep, "deep content");
    let big_out = fs::read(decomp_dir.path().join("big.bin"))?;
    assert_eq!(big_out, big, "large multi-chunk file must round-trip exactly");

    Ok(())
}

// ─── Index/schema tests ────────────────────────────────────────────────────

#[test]
fn test_index_schema_fields() {
    let schema = znippy_common::znippy_index_schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    // znippy_index_schema() is the type-agnostic base; module columns are composed in
    // at write time via compose_index_schema(), not part of this static.
    assert_eq!(
        field_names,
        vec![
            "relative_path",
            "chunk_seq",
            "fdata_offset",
            "compressed",
            "uncompressed_size",
            "blob_offset",
            "blob_size",
            "checksum",
        ]
    );
}

#[test]
fn test_read_znippy_index_after_compress() -> Result<()> {
    let out_dir = TempDir::new()?;
    let archive_path = out_dir.path().join("idx.znippy");

    let compressor = compress_stream(&archive_path.to_path_buf(), false)?;
    compressor.sender().send(ArchiveEntry {
        relative_path: "test.txt".to_string(),
        data: b"test data for index read".to_vec(),
        pkg_type: None,
        repo: None,
    })?;
    compressor.finish()?;

    let (schema, batches) = znippy_common::read_znippy_index(&archive_path)?;
    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_rows(), 1);

    // Verify schema has config metadata (v0.6: checksums are in a column, not metadata)
    let metadata = schema.metadata();
    assert!(
        metadata.contains_key("compression_level"),
        "Expected compression_level in metadata"
    );
    // Checksums live in the 'checksum' column, not in schema metadata
    assert!(
        !metadata.contains_key("checksum_group_0"),
        "v0.6 format must not store checksums in schema metadata"
    );

    Ok(())
}

// ─── Verify/integrity tests ───────────────────────────────────────────────

#[test]
fn test_verify_via_decompress() -> Result<()> {
    let out_dir = TempDir::new()?;
    let archive_path = out_dir.path().join("verify.znippy");

    let data: Vec<u8> = (0..10000).map(|i| (i % 127) as u8).collect();

    let compressor = compress_stream(&archive_path.to_path_buf(), false)?;
    compressor.sender().send(ArchiveEntry {
        relative_path: "check.bin".to_string(),
        data: data.clone(),
        pkg_type: None,
        repo: None,
    })?;
    compressor.finish()?;

    // Use decompress with save_data=true to verify integrity
    let decomp_dir = TempDir::new()?;
    let report = znippy_common::decompress_archive(&archive_path, true, decomp_dir.path())?;
    assert_eq!(report.corrupt_files, 0);
    assert_eq!(report.total_files, 1);
    assert_eq!(report.verified_files, 1, "File should be verified via blake3 checksum");
    assert!(report.verified_bytes > 0, "Should have verified bytes");

    // Verify content matches
    let content = fs::read(decomp_dir.path().join("check.bin"))?;
    assert_eq!(content, data);

    Ok(())
}

#[test]
fn test_list_archive_contents() -> Result<()> {
    let out_dir = TempDir::new()?;
    let archive_path = out_dir.path().join("list.znippy");

    let compressor = compress_stream(&archive_path.to_path_buf(), false)?;
    compressor.sender().send(ArchiveEntry {
        relative_path: "foo.txt".to_string(),
        data: b"foo".to_vec(),
        pkg_type: None,
        repo: None,
    })?;
    compressor.sender().send(ArchiveEntry {
        relative_path: "bar.txt".to_string(),
        data: b"bar".to_vec(),
        pkg_type: None,
        repo: None,
    })?;
    compressor.finish()?;

    // list_archive_contents prints to stdout - just verify it doesn't error
    znippy_common::list_archive_contents(&archive_path)?;

    Ok(())
}

// ─── Manifest codec unit tests (v0.7) ─────────────────────────────────────

#[test]
fn test_manifest_roundtrip() -> Result<()> {
    let entries = vec![
        ManifestEntry {
            pkg_type: 1,
            repo: "central".to_string(),
            module_name: "maven".to_string(),
            index_offset: 0,
            index_len: 1024,
            row_count: 42,
        },
        ManifestEntry {
            pkg_type: 2,
            repo: "crates-io".to_string(),
            module_name: "cargo".to_string(),
            index_offset: 1024,
            index_len: 512,
            row_count: 17,
        },
    ];

    let bytes = write_manifest_bytes(&entries)?;
    let decoded = read_manifest_bytes(&bytes)?;
    assert_eq!(decoded, entries);
    Ok(())
}

#[test]
fn test_manifest_empty_roundtrip() -> Result<()> {
    let bytes = write_manifest_bytes(&[])?;
    let decoded = read_manifest_bytes(&bytes)?;
    assert!(decoded.is_empty());
    Ok(())
}

#[test]
fn test_interpret_footer_single_v06() {
    // 8-byte footer: just an offset — should be Single.
    let offset: u64 = 12345;
    let tail = offset.to_le_bytes();
    match interpret_footer(&tail) {
        znippy_common::IndexFooter::Single { index_offset } => assert_eq!(index_offset, 12345),
        other => panic!("expected Single, got {:?}", other),
    }
}

#[test]
fn test_interpret_footer_multi_v07() {
    // 16-byte footer: MAGIC + offset — should be Multi.
    let magic = znippy_common::MULTI_INDEX_MAGIC;
    let offset: u64 = 99999;
    let mut tail = [0u8; 16];
    tail[..8].copy_from_slice(&magic);
    tail[8..].copy_from_slice(&offset.to_le_bytes());
    match interpret_footer(&tail) {
        znippy_common::IndexFooter::Multi { manifest_offset } => assert_eq!(manifest_offset, 99999),
        other => panic!("expected Multi, got {:?}", other),
    }
}

#[test]
fn test_multi_index_write_read_roundtrip() -> Result<()> {
    let out_dir = TempDir::new()?;
    let archive_path = out_dir.path().join("multi.znippy");

    let compressor = compress_stream(&archive_path.to_path_buf(), false)?;

    // Two groups: (pkg_type=1, repo="maven") and (pkg_type=2, repo="cargo")
    compressor.sender().send(ArchiveEntry {
        relative_path: "pom.xml".to_string(),
        data: b"<project/>".to_vec(),
        pkg_type: Some(1),
        repo: Some("maven".to_string()),
    })?;
    compressor.sender().send(ArchiveEntry {
        relative_path: "lib.jar".to_string(),
        data: b"JAR_CONTENT".to_vec(),
        pkg_type: Some(1),
        repo: Some("maven".to_string()),
    })?;
    compressor.sender().send(ArchiveEntry {
        relative_path: "Cargo.toml".to_string(),
        data: b"[package]".to_vec(),
        pkg_type: Some(2),
        repo: Some("cargo".to_string()),
    })?;
    compressor.finish()?;

    // Verify manifest via dedicated reader
    let manifest = znippy_common::read_znippy_manifest(&archive_path)?;
    assert_eq!(manifest.len(), 2, "expected 2 sub-indexes in manifest");

    let maven = manifest.iter().find(|e| e.repo == "maven").expect("maven entry missing");
    assert_eq!(maven.pkg_type, 1);
    assert_eq!(maven.row_count, 2);

    let cargo = manifest.iter().find(|e| e.repo == "cargo").expect("cargo entry missing");
    assert_eq!(cargo.pkg_type, 2);
    assert_eq!(cargo.row_count, 1);

    // Full round-trip: decompress and verify all files
    let files = decompress_to_map(&archive_path)?;
    assert_eq!(files.len(), 3);
    assert_eq!(files["pom.xml"], b"<project/>");
    assert_eq!(files["lib.jar"], b"JAR_CONTENT");
    assert_eq!(files["Cargo.toml"], b"[package]");

    Ok(())
}

#[test]
fn test_single_group_writes_v07() -> Result<()> {
    // When all entries share the same (pkg_type, repo), the archive is still v0.7 (1-entry manifest).
    let out_dir = TempDir::new()?;
    let archive_path = out_dir.path().join("single.znippy");

    let compressor = compress_stream(&archive_path.to_path_buf(), false)?;
    compressor.sender().send(ArchiveEntry {
        relative_path: "a.txt".to_string(),
        data: b"aaa".to_vec(),
        pkg_type: Some(1),
        repo: Some("r1".to_string()),
    })?;
    compressor.sender().send(ArchiveEntry {
        relative_path: "b.txt".to_string(),
        data: b"bbb".to_vec(),
        pkg_type: Some(1),
        repo: Some("r1".to_string()),
    })?;
    compressor.finish()?;

    // read_znippy_manifest should succeed — v0.7 with 1 entry
    let manifest = znippy_common::read_znippy_manifest(&archive_path)?;
    assert_eq!(manifest.len(), 1);
    assert_eq!(manifest[0].pkg_type, 1);
    assert_eq!(manifest[0].repo, "r1");
    assert_eq!(manifest[0].row_count, 2);

    let files = decompress_to_map(&archive_path)?;
    assert_eq!(files.len(), 2);
    Ok(())
}

#[test]
fn test_znippy_archive_extract_file_multi_chunk() -> Result<()> {
    let out_dir = TempDir::new()?;
    let archive_path = out_dir.path().join("extract.znippy");

    // 12 MB forces multiple chunks; use a non-repeating pattern so any byte-order
    // swap would produce bytes that mismatch from position 0.
    let size = 12 * 1024 * 1024;
    let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();

    let compressor = compress_stream(&archive_path.to_path_buf(), false)?;
    compressor.sender().send(ArchiveEntry {
        relative_path: "big.bin".to_string(),
        data: data.clone(),
        pkg_type: None,
        repo: None,
    })?;
    let report = compressor.finish()?;
    assert!(report.chunks >= 2, "Expected multiple chunks, got {}", report.chunks);

    let archive = ZnippyArchive::open(&archive_path)?;
    let extracted = archive.extract_file("big.bin")?;
    assert_eq!(extracted.len(), data.len(), "length mismatch");
    assert_eq!(extracted, data, "content mismatch — chunks were assembled in wrong order");

    Ok(())
}
