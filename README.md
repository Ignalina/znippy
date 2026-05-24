
![znippy](https://github.com/user-attachments/assets/7db1c1c1-d577-4f87-bfe1-11af6e8c58a0)

# Znippy

High-performance archive format with per-file compression, parallel processing, and random access.
Built on **Apache Arrow IPC** + **OpenZL** (zstd+lz4 under the hood).

## Benchmarks (v0.7, 8-core T14s, release)

| Test | In | Out | Ratio | Compress | Decompress |
|------|-----|-----|-------|----------|------------|
| text 500MB | 500 MB | 0.12 MB | 4039x | 1,812 MB/s | 3,030 MB/s |
| binary pattern 500MB | 500 MB | 0.22 MB | 2229x | 2,618 MB/s | 3,205 MB/s |
| random (incompressible) 500MB | 500 MB | 500 MB | 1.0x | 184 MB/s | 3,068 MB/s |
| 100k small files (10KB) | 977 MB | 17.5 MB | 55.9x | 3,223 MB/s | 726 MB/s |
| mixed repo 530MB | 530 MB | 530 MB | 1.0x | 2,561 MB/s | 2,409 MB/s |
| single file 2GB | 2,048 MB | 0.50 MB | 4128x | 3,436 MB/s | 3,075 MB/s |
| **Rust crates (1.3k .crate)** | **197 MB** | **197 MB** | **1.0x** | **1,376 MB/s** | **2,370 MB/s** |
| **Rust deps (41k files)** | **988 MB** | **137 MB** | **7.2x** | **67 MB/s** | **1,377 MB/s** |

Already-compressed files (.crate, .jar, .gz, etc.) are stored as-is at full write speed (skip path). Random/incompressible data is measured at openzl encoding cost. Small-file throughput is bottlenecked by per-chunk channel overhead — see backlog P4.

## Architecture — v0.7 Multi-Index Format

### File format

```
[ blob_0 ][ blob_1 ] ... [ blob_N ]
[ Arrow IPC sub-index ]   ← rows: relative_path, pkg_type, repo, chunk_seq,
[ Arrow IPC sub-index ]       blob_offset, blob_size, uncompressed_size, checksum, …
...
[ Arrow IPC manifest  ]   ← rows: pkg_type, repo, sub_index_offset, sub_index_size
[ 8 bytes "ZNPYMIDX" ][ 8 bytes LE u64: manifest_offset ]
```

Blobs are written as produced (true streaming, no buffering). After all blobs finish, one Arrow IPC sub-index is written per `(pkg_type, repo)` group — each is a standard Arrow IPC stream where `pkg_type` and `repo` are ordinary columns alongside the path and offset data. The manifest Arrow IPC table records the byte offset and size of each sub-index so the reader can seek directly to the right group. The 16-byte footer gives the manifest's offset.

### Compression pipeline

```
  File bytes
      │
      ▼
  ┌─────────────────┐
  │  Split chunks   │  (ChunkRevolver ring buffer)
  └────────┬────────┘
           │
     ┌─────┼─────┐        parallel across all cores
     ▼     ▼     ▼
  ┌─────┐┌─────┐┌─────┐
  │OpenZL││OpenZL││OpenZL│  compress each chunk
  │  +  ││  +  ││  +  │
  │blake3││blake3││blake3│  hash original data
  └──┬───┘└──┬───┘└──┬───┘
     │       │       │
     ▼       ▼       ▼
  ┌────────────────────────────────────┐
  │  Writer thread (concurrent)        │  blobs written immediately
  │  Sub-indexes + manifest at end     │  ring slots freed per blob
  └────────────────────────────────────┘
```

The writer thread runs concurrently alongside compressor threads — compressors never stall waiting for index writes (Law 4 in design.md).

### Store-as-is pipeline (pre-compressed: .jar, .gz, .png, .crate…)

```
  File bytes
      │
      ├──────────────────────────────────┐
      │                                  │
      ▼                                  ▼
  ┌─────────────────┐     ┌───────────────────────────────┐
  │  Split chunks   │     │  Writer: blob written as-is   │  ZERO COPY
  └────────┬────────┘     └───────────────────────────────┘
           │
     ┌─────┼─────┐
     ▼     ▼     ▼
  ┌─────┐┌─────┐┌─────┐
  │blake3││blake3││blake3│  hash only (parallel)
  └──────┘└──────┘└──────┘
```

### Decompression pipeline

```
  archive.znippy
      │
      └── read 16-byte footer → seek to manifest → read sub-indexes
      │
      ▼
  ┌──────────────────────────┐
  │ Reader Thread            │  seeks to blob_offset for each chunk
  │ (blob_offset, blob_size) │
  └──────────┬───────────────┘
             │
       ┌─────┼─────┐        parallel across all cores
       ▼     ▼     ▼
    ┌─────┐┌─────┐┌─────┐
    │OpenZL││OpenZL││OpenZL│  decompress (or passthrough if stored raw)
    └──┬───┘└──┬───┘└──┬───┘
       │       │       │
       ▼       ▼       ▼
    ┌────────────────────────┐
    │ Writer Thread          │  write restored files to disk
    │ + Verify threads       │  BLAKE3 per checksum group
    └────────────────────────┘
```

## Features

- **Parallel compression**: fan-out to all physical cores via ChunkRevolver ring buffer
- **Concurrent writer**: blobs written to disk as produced, never blocking compressors
- **Blake3 checksums**: per-group integrity stored as Arrow column
- **Random access**: `ZnippyArchive::extract_file` seeks directly to each chunk's blob offset
- **Skip detection**: already-compressed files stored as-is at full write speed
- **Multi-index format**: one Arrow IPC sub-index per `(pkg_type, repo)` group; Arrow IPC manifest
- **Arrow IPC index**: full metadata queryable by DuckDB, Polars, pyarrow after parsing footer
- **Plugin system**: native + WASM plugins declare their own Arrow schema columns

## Usage

```bash
# Compress a directory
znippy compress --input ./mydata --output archive.znippy

# Decompress
znippy decompress --input archive.znippy --output ./restored

# Verify integrity (no file writes)
znippy verify --input archive.znippy

# List contents
znippy list --input archive.znippy
```

## Query with DuckDB / Polars

The Arrow IPC index is directly queryable once you seek to it using the manifest offset:

```python
import polars as pl
# after extracting the sub-index bytes from the archive
df = pl.read_ipc(sub_index_bytes)
df.select("relative_path", "uncompressed_size", "compressed").head(10)
```

## Host Decompressors (`host-decompressors` feature)

The `host-decompressors` feature replaces miniz_oxide with purpose-built parallel decompressors:

| Component | Throughput | What it does |
|-----------|-----------|--------------|
| **lgz** | 6,100+ MB/s multi-core | Parallel gzip decompression — splits stream at full-flush boundaries and decompresses segments in parallel |
| **linflate** | ~700 MB/s single-core | SIMD DEFLATE decoder (AVX2 match-copy) |
| **ljar** | Multi-core | Parallel JAR/ZIP extraction (per-entry parallelism) |
| miniz_oxide (replaced) | ~190 MB/s | Default single-threaded fallback |

Single-core linflate is 3.7× faster than miniz_oxide. lgz is fully parallelized and scales linearly with cores.

## Testing & Benchmarks

### Before publishing a crate — run everything

```bash
cargo xtask
```

This runs in order:
1. `cargo test --workspace` — all unit + integration tests
2. Synthetic performance suite (release build) — text, binary, random, 100k small files, mixed repo, 2 GB single file
3. Maven plugin tests — correctness + native vs fallback throughput comparison
4. Regression gate — fails if any benchmark drops >20% vs the last recorded run

To also run the real-world network benchmarks (downloads ~1 GB on first run, cached to `/tmp/znippy-bench-cache/`):

```bash
cargo xtask -- --real
```

---

### Run just the tests (no benchmarks, fast)

```bash
cargo test --workspace
```

Covers format round-trips, manifest, multi-index, Maven plugin correctness, and throughput benchmarks.

---

### Run individual suites

```bash
# Synthetic performance suite only (release)
cargo test --release -p znippy-tests --test perf_bench perf_benchmark_suite -- --nocapture

# Maven plugin tests only (correctness + throughput)
cargo test --release -p znippy-tests --test maven_bench -- --nocapture

# Real-world benchmarks (network, explicit)
cargo test --release -p znippy-tests --test perf_bench -- --ignored --nocapture
```

## Roadmap

- **v0.4.0**: Single-file format (Arrow IPC with inline zdata column), OpenZL backend, plugin system
- **v0.5.0**: Dual-pipeline architecture, DuckDB/Polars queryable, zero-copy skip path
- **v0.6.0**: Streaming format — blobs first, Arrow index last, 8-byte footer; true zero-buffer writes
- **v0.7.0** (current): Multi-index container — one Arrow IPC sub-index per `(pkg_type, repo)` group; Arrow IPC manifest; 16-byte footer; module-owned schema; WASM plugin `plugin_schema` export
- **v0.8.0** Iceberg support

🧙 May Odin watch over every bit. 🧙
## Fan arts

Iceberg 
![znippy](https://github.com/user-attachments/assets/b068d559-4339-4f9f-9623-7c6b5efe2771)
