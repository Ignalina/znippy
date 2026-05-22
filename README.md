
![znippy](https://github.com/user-attachments/assets/7db1c1c1-d577-4f87-bfe1-11af6e8c58a0)

# Znippy

High-performance archive format with per-file compression, parallel processing, and random access.
Built on **Apache Arrow IPC** + **OpenZL** (zstd+lz4 under the hood).

## Benchmarks (8-core, OpenZL, v0.6)

| Test | In | Out | Ratio | Compress | Decompress |
|------|-----|-----|-------|----------|------------|
| text 500MB | 500 MB | 0.12 MB | 4092x | 1,724 MB/s | 3,311 MB/s |
| binary pattern 500MB | 500 MB | 0.22 MB | 2245x | 2,645 MB/s | 3,378 MB/s |
| random (incompressible) 500MB | 500 MB | 500 MB | 1.0x | 55 MB/s | 3,205 MB/s |
| single file 2GB | 2,048 MB | 0.49 MB | 4141x | 3,034 MB/s | 3,556 MB/s |
| 100k small files (10KB) | 977 MB | 17.5 MB | 55.9x | 2,668 MB/s | 969 MB/s |
| mixed repo 530MB | 530 MB | 530 MB | 1.0x | 2,208 MB/s | 2,420 MB/s |

<details>
<summary>Previous results (32-core, v0.5)</summary>

| Test | In | Out | Ratio | Compress | Decompress |
|------|-----|-----|-------|----------|------------|
| text 500MB | 500 MB | 0.11 MB | 4471x | 1,724 MB/s | 3,030 MB/s |
| binary pattern 500MB | 500 MB | 0.21 MB | 2354x | 2,242 MB/s | 2,941 MB/s |
| single file 2GB | 2,048 MB | 0.44 MB | 4673x | 3,684 MB/s | 3,374 MB/s |
| 100k small files (10KB) | 977 MB | 14.5 MB | 67.3x | 3,277 MB/s | 759 MB/s |
| mixed repo 530MB | 530 MB | 530 MB | 1.0x | 757 MB/s | 1,233 MB/s |
| Rust deps (41k files) | 988 MB | 136 MB | 7.3x | 66.9 MB/s | 1,259 MB/s |
| Rust crates (1.2k .crate) | 188 MB | 188 MB | 1.0x | 595 MB/s | 1,150 MB/s |

</details>

**v0.6 highlights vs v0.5 on comparable workloads:** mixed repo (skip-heavy) improved 3× (757 → 2,208 MB/s) due to true streaming writes. Decompression throughput up across the board. Random/incompressible data correctly measured at zstd encoding cost (~55 MB/s at level 19).

## Architecture — Dual-Pipeline (v0.6)

### File format

```
[ blob_0 ][ blob_1 ] ... [ blob_N ] [ Arrow IPC index ] [ 8-byte LE u64: Arrow offset ]
```

Blobs are written as produced (true streaming, no buffering). The Arrow IPC metadata index — containing paths, chunk sequences, offsets, sizes, and BLAKE3 checksums — is appended at the end. The 8-byte footer allows a reader to seek directly to the index without scanning the file.

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
     │       │       │
     ▼       ▼       ▼
  ┌─────────────────────────────┐
  │  Writer: blob_0, blob_1...  │  written immediately to disk
  │  Arrow IPC index at end     │  checksums now a column, not metadata
  └─────────────────────────────┘
```

### Pipeline B: Store as-is (pre-compressed: .jpg, .mp4, .gz, .jar, .png…)

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
  │blake3││blake3││blake3│  hash only (parallel across cores)
  └──────┘└──────┘└──────┘
```

### Decompression

```
  archive.znippy
      │
      └── read 8-byte footer → seek to Arrow IPC index
      │
      ▼
  ┌──────────────────────────┐
  │ Reader Thread            │  seeks to blob_offset for each chunk
  │ (blob_offset, blob_size) │  reads directly from archive file
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
- **True streaming writes**: blobs written to disk as produced, no in-memory buffering
- **Blake3 checksums**: per-group integrity verification stored as Arrow column
- **Random access**: `ZnippyArchive::extract_file` seeks directly to each chunk's blob offset
- **Skip detection**: already-compressed files stored as-is at full write speed
- **Arrow IPC index**: metadata queryable by any Arrow reader after parsing the footer

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
- **v0.5.0**: Dual-pipeline architecture, DuckDB/Polars queryable, zero-copy for uncompressed
- **v0.6.0** (current): Streaming format — blobs first, Arrow index last, 8-byte footer; checksums in column; true zero-buffer writes; `ZnippyArchive` seeks to blob offsets on demand

## Fan arts

![znippy](https://github.com/user-attachments/assets/b068d559-4339-4f9f-9623-7c6b5efe2771)
