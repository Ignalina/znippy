
![znippy](https://github.com/user-attachments/assets/7db1c1c1-d577-4f87-bfe1-11af6e8c58a0)

# Znippy

High-performance archive format built for random access, batched random access.
Built on **Apache Arrow IPC** + **OpenZL** (zstd+lz4 under the hood).

## Benchmarks (v0.7.4 Gatling pipeline + io_uring, release — 32 cores)

### Stream packer (holger — HTTP downloads into archive)

| Test | In | Out | Ratio | Compress | Decompress | Files |
|------|-----|-----|-------|----------|------------|-------|
| text 500MB | 500 MB | 0.06 MB | 9014x | 1,639 MB/s | 2,874 MB/s | 1 |
| binary pattern 500MB | 500 MB | 0.07 MB | 7338x | 2,242 MB/s | 2,976 MB/s | 1 |
| random (incompressible) 500MB | 500 MB | 500 MB | 1.0x | 105 MB/s | 2,890 MB/s | 1 |
| 100k small files (10KB) | 977 MB | 17.4 MB | 56.2x | 4,811 MB/s | 1,957 MB/s | 100,000 |
| mixed repo 530MB | 530 MB | 530 MB | 1.0x | 3,118 MB/s | 6,310 MB/s | 6 |
| single file 2GB | 2,048 MB | 0.22 MB | 9509x | 5,802 MB/s | 3,066 MB/s | 1 |

### Slot packer (CLI — directory compress with io_uring)

| Test | In | Out | Ratio | Compress | Decompress | Files |
|------|-----|-----|-------|----------|------------|-------|
| 10k small files (10KB, tmpfs) | 97.7 MB | 1.7 MB | 56x | 678 MB/s | 1,191 MB/s | 10,000 |
| real jars (RAID NVMe) | 5,124 MB | — | — | 2,527 MB/s | 9,950 MB/s | 4,730 |

Stream packer numbers are in-memory (no disk reads). Slot packer includes full disk I/O via **io_uring batched opens+reads** (128 files/batch, zero per-file syscall overhead).

Both compress and decompress run the no-barrier Gatling pipeline across all cores. Already-compressed files (.jar, .gz, .crate, .png, .parquet, etc.) match the skip-extension list and are stored/restored as-is at full I/O speed — they never touch the codec.

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
  │  Split slices   │  (shared Magazine slots, 200 MB each)
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

The read path has no reader/writer split: every chunk in the index is
independent work. N worker threads share one atomic row cursor; each grabs the
next chunk and runs it end to end with positioned I/O (`pread`/`pwrite`), so
parallel reads give the NVMe real queue depth and there is no central
bottleneck.

```
  archive.znippy
      │
      └── read 16-byte footer → seek to manifest → read sub-indexes
      │
      ▼
   one atomic row cursor  ── fetch_add ──▶  N workers (all cores)
                                              │
        ┌─────────────────────────────────────┼─────────────────────┐
        ▼                                     ▼                       ▼
  ┌───────────────┐                    ┌───────────────┐      ┌───────────────┐
  │ pread blob    │  positioned read   │ pread blob    │      │ pread blob    │
  │ OpenZL / skip │  (reused buffers)  │ OpenZL / skip │ ...  │ OpenZL / skip │
  │ blake3 verify │  per-chunk         │ blake3 verify │      │ blake3 verify │
  │ pwrite output │  @ fdata_offset    │ pwrite output │      │ pwrite output │
  └───────────────┘                    └───────────────┘      └───────────────┘
```

No channel, no Magazine slots, no drain barrier, no `unsafe` — each worker owns
its read/decompress buffers and reuses them across chunks.

## Features

- **Parallel compression**: one global slice queue over shared Magazine slots; all cores pull work with no per-slot barrier
- **Concurrent writer**: blobs written to disk as produced, never blocking compressors
- **Blake3 checksums**: per-chunk integrity stored as an Arrow column, verified on read
- **Random access**: `ZnippyArchive::extract_file` preads each chunk's blob from a shared fd (safe under concurrent calls)
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

### Release contract

`cargo xtask` (no regressions) is a **release gate**: it must pass before `cargo publish`.
`bench_history.json` (JSONL, one run per line) is the source of truth for that gate.
The sibling `katana-osm` workspace mirrors this exact `xtask` pattern (conversion
throughput instead of compress/decompress MB/s) and keeps its own `bench_history.json`;
together the two histories are the evidence base for any shared streaming-engine change.

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
- **v0.7.0**: Multi-index container — one Arrow IPC sub-index per `(pkg_type, repo)` group; Arrow IPC manifest; 16-byte footer; module-owned schema; WASM plugin `plugin_schema` export
- **v0.7.4** (current): io_uring batched open+read in slot_packer small pass; deadlock fix for large file sets; unbounded worker pipeline
- **v0.7.2**: No-barrier Gatling pipeline on both paths — write via shared Magazine slots, read via N-worker positioned I/O; per-chunk blake3; ChunkRevolver removed
- **v0.8.0** Iceberg support

🧙 May Odin watch over every bit. 🧙
## Fan arts

Iceberg 
![znippy](https://github.com/user-attachments/assets/b068d559-4339-4f9f-9623-7c6b5efe2771)
