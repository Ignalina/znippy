
![znippy](https://github.com/user-attachments/assets/7db1c1c1-d577-4f87-bfe1-11af6e8c58a0)

# Znippy

High-performance archive format with per-file compression, parallel processing, and random access.
Built on **Apache Arrow IPC** + **OpenZL** (zstd+lz4 under the hood).

## Benchmarks (32-core, OpenZL)

| Test | In | Out | Ratio | Compress | Decompress |
|------|-----|-----|-------|----------|------------|
| text 500MB | 500 MB | 0.11 MB | 4471x | 1,724 MB/s | 3,030 MB/s |
| binary pattern 500MB | 500 MB | 0.21 MB | 2354x | 2,242 MB/s | 2,941 MB/s |
| single file 2GB | 2,048 MB | 0.44 MB | 4673x | 3,684 MB/s | 3,374 MB/s |
| 100k small files (10KB) | 977 MB | 14.5 MB | 67.3x | 3,277 MB/s | 759 MB/s |
| mixed repo 530MB | 530 MB | 530 MB | 1.0x | 757 MB/s | 1,233 MB/s |
| Rust deps (41k files) | 988 MB | 136 MB | 7.3x | 66.9 MB/s | 1,259 MB/s |
| Rust crates (1.2k .crate) | 188 MB | 188 MB | 1.0x | 595 MB/s | 1,150 MB/s |

## Architecture — Dual-Pipeline (v0.5)

Single `.znippy` file = valid Arrow IPC Stream. Queryable by DuckDB/Polars/pyarrow directly:

```sql
SELECT relative_path, uncompressed_size FROM 'archive.znippy';
```

Two pipelines based on whether compression is needed:

### Pipeline A: Compress (compressible files)

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
     │     │     │
     ▼     ▼     ▼
  ┌─────────────────────────────┐
  │ Arrow IPC writer            │  Buffer::from(compressed_vec)
  │ (zdata = compressed bytes)  │  ownership transfer
  └─────────────────────────────┘
```

### Pipeline B: Store as-is (pre-compressed: .jpg, .mp4, .gz, .jar, .png)

```
  File bytes (size known upfront!)
      │
      ├──────────────────────────────────┐
      │                                  │
      ▼                                  ▼
  ┌─────────────────┐     ┌───────────────────────────────┐
  │  Split chunks   │     │  Arrow IPC writer             │
  └────────┬────────┘     │  (zdata = raw bytes)          │  ZERO COPY
           │              │  Buffer::from(vec)            │  (parallel!)
     ┌─────┼─────┐       └───────────────────────────────┘
     ▼     ▼     ▼
  ┌─────┐┌─────┐┌─────┐
  │blake3││blake3││blake3│  hash only (parallel across cores)
  └──┬───┘└──┬───┘└──┬───┘
     │     │     │
     ▼     ▼     ▼
  ┌─────────────────────┐
  │ Checksum complete   │  (data already written!)
  └─────────────────────┘
```

### Decompression

```
  archive.znippy (Arrow IPC Stream)
      │
      ▼
  ┌──────────────────────┐
  │ Reader Thread        │  read zdata column from Arrow batches
  │ (Arrow IPC reader)   │
  └──────────┬───────────┘
             │
       ┌─────┼─────┐        parallel across all cores
       ▼     ▼     ▼
    ┌─────┐┌─────┐┌─────┐
    │OpenZL││OpenZL││OpenZL│  decompress (or passthrough if stored raw)
    │  +  ││  +  ││  +  │
    │blake3││blake3││blake3│  verify checksum
    └──┬───┘└──┬───┘└──┬───┘
       │     │     │
       ▼     ▼     ▼
    ┌─────────────────────┐
    │ Writer Thread       │  write restored files to disk
    └─────────────────────┘
```

## Features

- **Parallel compression**: fan-out to all cores via ChunkRevolver
- **Blake3 checksums**: per-group integrity verification (machine-independent)
- **Arrow IPC index**: queryable by DuckDB, Polars, DataFusion
- **Skip detection**: already-compressed files (.zip, .gz, .png, etc.) stored as-is
- **Random access**: seek directly to any file's chunks via index

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

## Roadmap

- **v0.3.0**: OpenZL backend, plugin system (WASM + native), ZnippyArchive API
- **v0.4.0**: Single-file format (Arrow IPC with inline zdata column)
- **v0.5.0** (current): Dual-pipeline architecture, DuckDB/Polars queryable, zero-copy for uncompressed
- **next**: Upstream Arrow IPC scatter-gather fix → auto-recover full NVMe throughput

## Fan arts

![znippy](https://github.com/user-attachments/assets/b068d559-4339-4f9f-9623-7c6b5efe2771)
