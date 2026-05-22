//! Integration tests for znippy: compress → decompress round-trip.

use anyhow::Result;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use tempfile::TempDir;
use znippy_common::{ZnippyArchive, ZnippyReader};
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

// ─── Index/schema tests ────────────────────────────────────────────────────

#[test]
fn test_index_schema_fields() {
    let schema = znippy_common::znippy_index_schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        field_names,
        vec![
            "relative_path",
            "chunk_seq",
            "fdata_offset",
            "checksum_group",
            "compressed",
            "uncompressed_size",
            "repo",
            "extension",
            "zdata",
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
    })?;
    compressor.finish()?;

    let (schema, batches) = znippy_common::read_znippy_index(&archive_path)?;
    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_rows(), 1);

    // Verify schema has metadata (checksums, config)
    let metadata = schema.metadata();
    assert!(
        metadata.contains_key("checksum_group_0"),
        "Expected checksum in metadata"
    );
    assert!(
        metadata.contains_key("compression_level"),
        "Expected compression_level in metadata"
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
    })?;
    compressor.sender().send(ArchiveEntry {
        relative_path: "bar.txt".to_string(),
        data: b"bar".to_vec(),
    })?;
    compressor.finish()?;

    // list_archive_contents prints to stdout - just verify it doesn't error
    znippy_common::list_archive_contents(&archive_path)?;

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
    })?;
    let report = compressor.finish()?;
    assert!(report.chunks >= 2, "Expected multiple chunks, got {}", report.chunks);

    let archive = ZnippyArchive::open(&archive_path)?;
    let extracted = archive.extract_file("big.bin")?;
    assert_eq!(extracted.len(), data.len(), "length mismatch");
    assert_eq!(extracted, data, "content mismatch — chunks were assembled in wrong order");

    Ok(())
}
