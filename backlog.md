# Znippy Backlog

## Done (v0.4.0)
- [x] Streaming API (`compress_stream`, `StreamCompressor`, `ArchiveEntry`)
- [x] Wire up stream_packer in lib.rs
- [x] Integration tests (13 tests, full round-trip)
- [x] OpenZL backend (replaced zstd)
- [x] Single-file format (no .zdata, inline LargeBinary zdata column)
- [x] Multi-repo support (`repo` column, nullable)
- [x] Streaming batch writes (64MB flush threshold)
- [x] Zero-copy batch builder (`build_batch_zero_copy`)
- [x] Footer custom_metadata for checksums (reader merges transparently)
- [x] Empty file handling (emit one empty compressed chunk)

## TODO

### Performance

#### P1: mixed_repo compress regression (-68% vs v0.3)
The `extend_from_slice` concatenation in `build_batch_zero_copy` still copies each
chunk's bytes into one contiguous `Vec<u8>` for Arrow's LargeBinaryArray. The old
.zdata format just did sequential `write()` syscalls — zero userspace copies.

**Possible fixes:**
- Use Arrow's `write_all` with a custom `ArrayData` backed by multiple buffers (scatter-gather I/O)
- Bypass Arrow IPC for the zdata column entirely: write a raw data section after the Arrow
  footer (hybrid format — metadata in Arrow, data in raw appendix)
- Investigate Arrow IPC LZ4/ZSTD body compression to let Arrow handle the column natively

#### P2: decompress regression for incompressible data (-34%)
Reading LargeBinaryArray from Arrow IPC has more overhead than seeking into a flat file.
The IPC reader must parse flatbuffers, validate offsets, etc.

**Possible fixes:**
- Memory-map the file and use zero-copy Arrow IPC reading (`arrow::ipc::reader` with mmap)
- Pre-read the entire file into memory and use `FileReader` from a `Cursor<Vec<u8>>`

#### P3: Investigate Arrow IPC compression (body-level)
Arrow IPC supports LZ4 Frame and ZSTD body compression (per-batch).
This could reduce file size for the metadata columns without affecting zdata
(which is already compressed).

### Features

#### F1: Selective decompression by repo
`znippy extract archive.znippy --repo libs-release --out ./libs/`
Filter rows by `repo` column during decompression.

#### F2: Multi-batch streaming for very large archives
Current writer still buffers up to 64MB before flush. For extreme archives (>100GB),
could reduce to smaller batches or adaptive threshold.

#### F3: Refactor `compress_dir` to use `compress_stream` internally
```rust
pub fn compress_dir(input: &Path, output: &Path, no_skip: bool) -> Result<CompressionReport> {
    let handle = compress_stream(output, no_skip)?;
    for entry in WalkDir::new(input)... {
        handle.sender().send(ArchiveEntry { relative_path, data: fs::read(path)? })?;
    }
    handle.finish()
}
```

#### F4: Plugin metadata on chunk_seq=0
Extension DenseUnion column currently always null. Wire up plugin metadata
injection for the first chunk of each file.

## Performance Baseline (v0.4.0, 32-core AMD, NVMe, release)

| Test | Compress MB/s | Decompress MB/s | Ratio |
|------|--------------|----------------|-------|
| text_500mb | 1618 | 3030 | 4420x |
| binary_pattern_500mb | 2551 | 3125 | 2340x |
| random_500mb | 181 | 2110 | 1.0x |
| 100k_small_files_10kb | 3120 | 833 | 67x |
| mixed_repo_530mb | 901 | 1688 | 1.0x |
| single_file_2gb | 3537 | 3677 | 4659x |
