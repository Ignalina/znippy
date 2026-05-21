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
Two copies remain in the write path, both confirmed unavoidable within Arrow IPC:

1. `build_batch_zero_copy`: `extend_from_slice` concatenates chunks into one contiguous
   `Vec<u8>` for Arrow's LargeBinaryArray (required by Arrow columnar format).
2. Arrow IPC `write_buffer()` (arrow-ipc writer.rs:2083): unconditionally does
   `arrow_data.extend_from_slice(buffer)` to assemble all column buffers into one
   contiguous `EncodedData.arrow_data: Vec<u8>` before writing to file.

This is a known limitation: [apache/arrow-rs#9835](https://github.com/apache/arrow-rs/issues/9835).
Arrow IPC has no scatter-gather or `write_vectored` support as of v57 (2025).
Zero-copy writing requires shared memory (mmap), not supported for file serialization.

The old .zdata format did sequential `write()` syscalls — zero userspace copies.

**Possible fixes:**
- Hybrid format: Arrow IPC for metadata + raw data appendix for zdata bytes
  (write zdata after Arrow footer, store offset/length in footer metadata)
- Wait for arrow-rs scatter-gather write support (tracked in #9835)
- Accept as inherent trade-off of queryable single-file format

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
