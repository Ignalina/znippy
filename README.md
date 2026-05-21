
![znippy](https://github.com/user-attachments/assets/7db1c1c1-d577-4f87-bfe1-11af6e8c58a0)

# Znippy

High-performance archive format with per-file compression, parallel processing, and random access.
Built on **Apache Arrow IPC** + **zstd** (migrating to **OpenZL**).

## Benchmarks (32-core, zstd level 19)

| Test | In | Out | Ratio | Compress | Decompress |
|------|-----|-----|-------|----------|------------|
| text 500MB | 500 MB | 0.09 MB | 5404x | 1,597 MB/s | 3,086 MB/s |
| single file 2GB | 2,048 MB | 0.35 MB | 5868x | 3,984 MB/s | 3,089 MB/s |
| 100k small files | 977 MB | 12.5 MB | 77.9x | 3,140 MB/s | 863 MB/s |
| rust crates (53k files) | 1,298 MB | 174 MB | 7.5x | 41 MB/s | 1,417 MB/s |

## Architecture

```mermaid
flowchart LR
    subgraph Input
        Files[Input Files]
    end

    subgraph "Reader Thread"
        Reader[Reader] --> Split[Split into chunks<br/>10MB default]
        Split --> Revolver((ChunkRevolver))
    end

    subgraph "Compressor Threads (1..N cores)"
        Revolver --> C0[Core 0<br/>zstd/OpenZL + blake3]
        Revolver --> C1[Core 1<br/>zstd/OpenZL + blake3]
        Revolver --> C2[Core 2<br/>...]
        Revolver --> CN[Core N<br/>zstd/OpenZL + blake3]
    end

    subgraph "Writer Thread"
        C0 --> Writer[Writer]
        C1 --> Writer
        C2 --> Writer
        CN --> Writer
    end

    subgraph Output
        Writer --> Archive[".znippy<br/>(Arrow IPC index +<br/>checksums + config)"]
        Writer --> Zdata[".zdata<br/>(compressed chunks)"]
    end

    Files --> Reader
```

## Decompression Pipeline

```mermaid
flowchart LR
    subgraph Input
        Index[".znippy index"] --> Reader
        Zdata[".zdata"] --> Reader
    end

    subgraph "Reader Thread"
        Reader[Reader<br/>seek + read chunks]
        Reader --> Revolver((ChunkRevolver))
    end

    subgraph "Decompressor Threads (1..N cores)"
        Revolver --> D0[Core 0<br/>zstd decompress]
        Revolver --> D1[Core 1<br/>zstd decompress]
        Revolver --> D2[Core 2<br/>...]
        Revolver --> DN[Core N<br/>zstd decompress]
    end

    subgraph "Writer Thread"
        D0 --> Writer[Writer<br/>+ blake3 verify]
        D1 --> Writer
        D2 --> Writer
        DN --> Writer
    end

    Writer --> Output[Restored Files]
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

- **v0.2.5** (current, tagged `v0.2.5-zstd`): zstd + Arrow IPC, 2-file format
- **v0.3**: Single-file format (Arrow IPC with inline binary column per chunk)
- **v0.4**: OpenZL backend — per-file-type specialized compression

## Fan arts

![znippy](https://github.com/user-attachments/assets/b068d559-4339-4f9f-9623-7c6b5efe2771)
