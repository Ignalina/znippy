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

#### ~~P1: mixed_repo compress regression (-68% vs v0.3)~~ ✅ FIXED
Solved by hybrid format (v2.1): raw data section + Arrow IPC Stream for metadata.
Write path now does zero userspace copies — chunk bytes go directly to file.
mixed_repo recovered from 901 → 2775 MB/s (v0.3 was 2850).

#### ~~P2: decompress regression for incompressible data (-34%)~~ ✅ FIXED
Solved by hybrid format: reader now seeks directly in raw section (sequential I/O).
No Arrow IPC parsing overhead for data bytes.
random decompress: 3125 MB/s (was 2110, +48% improvement).

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

## Performance Baseline (v0.4.1 hybrid, 32-core AMD, NVMe, release)

| Test | Compress MB/s | Decompress MB/s | Ratio |
|------|--------------|----------------|-------|
| text_500mb | 1712 | 2959 | 4318x |
| binary_pattern_500mb | 2427 | 3106 | 2311x |
| random_500mb | 184 | 3125 | 1.0x |
| 100k_small_files_10kb | 3344 | 739 | 64x |
| mixed_repo_530mb | 2775 | 2718 | 1.0x |
| single_file_2gb | 3346 | 3431 | 4510x |
