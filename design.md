# Znippy Design Document

## Section 1: zstd Backend (v0.2.5, tagged `v0.2.5-zstd`)

### Format

Two-file output:
- `.znippy` — Arrow IPC index containing:
  - `relative_path` (Utf8)
  - `compressed` (Boolean)
  - `uncompressed_size` (UInt64)
  - `chunks` (List of Struct: zdata_offset, fdata_offset, length, chunk_seq, checksum_group)
  - Arrow metadata: blake3 checksums per group + compression config
- `.zdata` — concatenated compressed chunks (seekable by offset)

### Compression Pipeline

```
Reader Thread
  → reads files, splits into 10MB chunks
  → assigns to ChunkRevolver ring buffer slots

Compressor Threads (N = max_core_in_flight, ~90% of cores)
  → each owns a ZSTD CCtx (level 19, nbWorkers for remainder cores)
  → hasher.update(raw_input) for blake3 per group
  → splits chunk into microchunks (1MB) for zstd framing
  → sends compressed data + ChunkMeta to writer

Writer Thread
  → receives compressed chunks (out of order)
  → writes sequentially to .zdata, records zdata_offset
  → builds Arrow RecordBatch with file metadata
  → writes .znippy index with checksums in Arrow metadata
```

### Decompression Pipeline

```
Reader Thread
  → reads .znippy index (Arrow IPC)
  → seeks into .zdata by zdata_offset per chunk
  → loads compressed data into ChunkRevolver slots

Decompressor Threads (N cores)
  → ZSTD decompress each chunk
  → sends decompressed data to writer

Writer Thread
  → writes files to disk (seek by fdata_offset for out-of-order chunks)
  → accumulates blake3 per checksum_group, sorted by chunk_seq
  → verifies against stored checksums at end
```

### Performance (32-core, release build)

| Test | In(MB) | Out(MB) | Ratio | Comp MB/s | Dec MB/s |
|------|--------|---------|-------|-----------|----------|
| text_500mb | 500.00 | 0.09 | 5404x | 1597 | 3086 |
| binary_pattern_500mb | 500.00 | 0.19 | 2609x | 2463 | 3106 |
| random_500mb (incompressible) | 500.00 | 500.04 | 1.00x | 176 | 2994 |
| 100k_small_files_10kb | 976.56 | 12.54 | 77.9x | 3140 | 863 |
| mixed_repo_530mb | 530.05 | 530.01 | 1.00x | 2199 | 2704 |
| single_file_2gb | 2048.00 | 0.35 | 5868x | 3984 | 3089 |

### Real-world data

| Dataset | Files | In(MB) | Out(MB) | Ratio | Comp MB/s | Dec MB/s |
|---------|-------|--------|---------|-------|-----------|----------|
| rust crates (airgap mirror) | 53,433 | 1,298 | 174 | 7.5x | 41 | 1,417 |
| git repos (iceberg, burn, etc) | 24,193 | 522 | 313 | 1.7x | 61 | 1,145 |

### Known Limitations (zstd)

- Small files (avg 25KB): zstd context overhead dominates → 41 MB/s compress
- `nbWorkers` on zstd adds overhead for chunks < 1MB
- No per-file-type optimization — generic compression for all content
- Two-file output requires managing .znippy + .zdata together

---

## Section 2: OpenZL Backend (v0.4, planned)

### Motivation

- **Format-aware compression**: trained codecs per file type (.rs, .toml, .json, .xml, .jar)
- **Better small-file performance**: specialized codecs avoid generic overhead
- **Universal decompressor**: single decoder handles all file types (from frame header)
- **Higher ratios on structured data** at equal or higher speed than zstd

### Format (v0.3 + OpenZL)

Single-file output — Arrow IPC with inline data:

```
Schema (one row per chunk):
  relative_path   Utf8
  chunk_seq       UInt32
  fdata_offset    UInt64
  checksum_group  UInt8
  compressed      Boolean
  repo            Utf8 (optional, for multi-repo/Nexus)
  zdata           LargeBinary (compressed chunk bytes)
```

- Multi-batch writes (flush every N chunks → bounded memory)
- No separate .zdata file
- Queryable: `SELECT * FROM 'archive.znippy' WHERE repo = 'libs-release'`
- Column pruning: read index without loading binary data

### API Mapping (zstd → OpenZL)

| Current (zstd) | OpenZL |
|---|---|
| `ZSTD_createCCtx()` | `ZL_CCtx_create()` |
| `ZSTD_CCtx_setParameter(cctx, level, 19)` | `ZL_CCtx_setParameter(cctx, ZL_CParam_compressionLevel, 6)` |
| `ZSTD_compress2(cctx, dst, src)` | `ZL_CCtx_compress(cctx, dst, dstCap, src, srcSize)` |
| `ZSTD_decompress(dst, src)` | `ZL_decompress(dst, dstCap, src, srcSize)` |
| `ZSTD_freeCCtx()` | `ZL_CCtx_free()` |

Additional: `ZL_CCtx_setCGraph()` to load file-type-specific compression graph.

### Architecture (unchanged)

```
Reader Thread → ChunkRevolver → N Compressor Threads → Writer Thread
                                (each with ZL_CCtx +
                                 file-type graph +
                                 blake3 hasher)
```

The parallel pipeline stays identical. Only the compress/decompress calls change.

### Per-file-type Strategy

| Extension | Codec strategy |
|-----------|---------------|
| .rs, .py, .java, .go | Source code graph (trained on language corpus) |
| .toml, .json, .yaml | Structured text graph |
| .xml, .html | XML/markup graph |
| .jar, .zip, .gz, .png | Skip (already compressed, store as-is) |
| .bin, unknown | Generic OpenZL (still better than zstd for structured data) |

### Rust Bindings

- Crate: `openzl-sys-rs` (BSD-3-Clause)
- Repo: `codeberg.org/mikronova/sys-openzl-rs`
- Pattern: cc + bindgen, static linking (same as `zstd-sys-rs`)
- OpenZL source as git submodule

---

## Section 3: Single-file Format Migration (v0.3)

### Current → Target

```
BEFORE (v0.2.5):
  archive.znippy  (Arrow IPC index, ~4.5MB for 53k files)
  archive.zdata   (compressed chunks, ~174MB)

AFTER (v0.3):
  archive.znippy  (Arrow IPC, one row per chunk, data inline)
```

### Benefits

- Single file to manage, copy, transfer
- Still queryable by DuckDB/Polars/DataFusion
- Column pruning: read metadata without loading chunk data
- Multi-repo support via `repo` column + row group partitioning
- Streaming write: flush batch every N chunks (bounded memory)

### Memory Model

- Each batch contains ~100 chunks × 10MB = ~1GB max
- Flush batch → start new batch → constant memory
- Arrow IPC supports unlimited batches per file
