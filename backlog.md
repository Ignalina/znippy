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

## Done (v0.5.0)
- [x] Valid Arrow IPC Stream format (DuckDB/Polars/pyarrow can query directly)
- [x] Direct buffer construction (bypass LargeBinaryBuilder copy)
- [x] Removed custom hybrid format (trailer, magic, raw section)

## Arrow IPC Serialization Bottleneck — Technical Analysis

### The numbers

| Write strategy | mixed_repo (530MB, uncompressed) | Bottleneck |
|---|---|---|
| **Direct NVMe write** (`write_all()`) | **2775 MB/s** | NVMe bandwidth |
| **Arrow IPC Stream** (current v0.5) | **777 MB/s** | `extend_from_slice` |

**Ratio: 3.6x slower due to Arrow IPC serialization copy.**

### Root cause

`arrow-ipc-57.3.0/src/writer.rs` line 2083:
```rust
fn write_buffer(arrow_data: &mut Vec<u8>, buffer: &Buffer, ...) {
    arrow_data.extend_from_slice(buffer);  // ← mandatory memcpy
}
```

Every buffer in every RecordBatch is copied into a contiguous `Vec<u8>` before
being written to disk. No scatter-gather / `write_vectored` support exists.

### What we tried (eliminated, made no difference)

1. ~~`LargeBinaryBuilder::append_value()`~~ → replaced with direct `Buffer::from(vec)` construction
2. Direct `OffsetBuffer` + `Buffer` assembly (avoids builder's internal realloc)
3. Result: 799 → 777 MB/s (noise). **The IPC writer copy dominates.**

### Why direct NVMe write is 3.6x faster

```
Direct:  ring_buffer → write_all(data) → NVMe    (1 syscall, 0 userspace copies)
Arrow:   ring_buffer → Vec values → Buffer → extend_from_slice → write() → NVMe
                        copy #1              copy #2 (IPC writer)
```

### Path to fix (upstream)

- Issue: apache/arrow-rs#9835 — "Support write_vectored / scatter-gather in IPC writer"
- **We commented on the issue with our benchmark data** (2025-05-21): 3.6x throughput loss, 777 vs 2775 MB/s
- Fix: IPC writer writes buffer slices directly instead of concatenating into Vec
- **Our code is already structured for this** — if upstream ships the fix, `cargo update` recovers full speed. Zero code changes needed on our side.

### Decision: we keep Arrow IPC (v0.5)

- 777 MB/s is still fast (saturates 6 Gbit/s network, exceeds SATA SSD)
- DuckDB/Polars queryability is worth the tradeoff
- Upstream fix will eventually land — we get the speed back for free

## TODO

### Performance

#### P4: Small-file coalescing — pack multiple small files into one chunk before compression

**Problem (observed in benchmark):**
- `rust_deps_real`: 41k `.rs`/`.toml` files averaging ~24 KB → **18 MB/s, 54 seconds**
- Root cause: each file is ~1 chunk, so the full reader→compressor→writer→tx_ret round-trip
  is paid per file. Channel overhead and thread synchronisation dominate useful work.
- `100k_small_files_10kb` decompresses at only 627 MB/s vs 2000+ MB/s for large files —
  same per-chunk overhead on the read side.

**Idea:**
Reader accumulates small files (below a threshold, e.g. `file_split_block_size / 4`) into
a single revolver chunk, recording individual `fdata_offset` boundaries in the chunk metadata.
A single chunk is sent to the compressor for the whole group. Decompressor splits it back
using the stored offsets.

**Trade-offs:**
- Complicates chunk metadata (needs a sub-file offset table or sentinel records in the index)
- Compression ratio may improve (cross-file dictionary opportunities)
- Need to decide threshold — too large means latency spike waiting to fill a chunk

**Relevant commit:** e77d9a4 (zero-copy Blob pipeline — shows current per-chunk cost baseline)

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

## Performance Baseline (v0.5.0 Arrow IPC, 8-core T14s laptop, release)

| Test | Compress MB/s | Decompress MB/s | Ratio |
|------|--------------|----------------|-------|
| text_500mb | 1707 | 3049 | 4471x |
| binary_pattern_500mb | 2370 | 2809 | 2354x |
| random_500mb | 152 | 1312 | 1.0x |
| 100k_small_files_10kb | 3150 | 751 | 67x |
| mixed_repo_530mb | 777 | 1241 | 1.0x |
| single_file_2gb | 3442 | 3089 | 4673x |

Note: mixed_repo limited by Arrow IPC serialization copy (apache/arrow-rs#9835).
Direct NVMe write achieves 2775 MB/s for same workload.

## Performance Baseline (v0.6.0, zero-copy Blob pipeline, 8-core T14s laptop, release)

| Test | In MB | Out MB | Ratio | Comp MB/s | Dec MB/s | Comp ms | Dec ms | Chunks | Skipped |
|------|-------|--------|-------|-----------|----------|---------|--------|--------|---------|
| text_500mb | 500 | 0.12 | 4092x | 2415 | 2128 | 207 | 235 | 500 | 0 |
| binary_pattern_500mb | 500 | 0.22 | 2245x | 1880 | 2304 | 266 | 217 | 500 | 0 |
| random_500mb | 500 | 500 | 1.00x | 33 | 2283 | 15322 | 219 | 500 | 0 |
| 100k_small_files_10kb | 977 | 17 | 56x | 1578 | 627 | 619 | 1557 | 100000 | 0 |
| mixed_repo_530mb | 530 | 530 | 1.00x | 2325 | 2227 | 228 | 238 | 55 | 4 |
| single_file_2gb | 2048 | 0.49 | 4141x | 2338 | 2698 | 876 | 759 | 2048 | 0 |
| rust_crates (real, 215 MB) | 215 | 215 | 1.00x | 1209 | 2562 | 178 | 84 | 1219 | 1218 |
| rust_deps_real (real, 988 MB) | 988 | 137 | 7.21x | 18 | 1642 | 54686 | 602 | 41501 | 103 |

Key changes vs v0.5.0:
- v0.6 streaming format: blobs first, Arrow index last, 8-byte footer
- Zero-copy Blob pipeline: eliminated Arc::from memcpy on both skip and compress paths
- random_500mb regressed vs v0.5 (was 152 MB/s) — incompressibility detection not yet implemented (P3/backlog)
