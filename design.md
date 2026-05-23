# Znippy Design Document

## Design Laws

### Law 1: Zero-Copy Channel Communication

All inter-thread communication passes **offsets/indices into shared memory**, never data copies.

- ChunkRevolver is a pre-allocated ring buffer of fixed slots
- Producer writes data into slot N, sends only the slot index (usize) through the channel
- Consumer receives the index, reads directly from the shared buffer at that offset
- No `Vec<u8>` cloning, no channel-carried payloads — only pointer-sized messages

**Rationale**: On 32 cores saturating NVMe (~7 GB/s), memcpy overhead from channel payloads
would halve throughput. Sending an 8-byte offset instead of a 10MB chunk eliminates
this entirely. The ring buffer also provides natural backpressure (producer blocks when
all slots are in-flight).

```
┌─────────────────────────────────────────────────────┐
│  Shared Ring Buffer (ChunkRevolver)                  │
│  [slot 0][slot 1][slot 2]...[slot N-1]              │
└─────────────────────────────────────────────────────┘
      ↑ write                    ↑ read
      │                          │
  Reader Thread              Compressor Thread
      │                          │
      └── channel.send(3) ──────→│  (only sends slot index = 3)
                                 │
                                 └── reads buffer[3] directly (zero copy)
```

### Law 2: No Allocation in Hot Path

Compressor threads reuse their buffers across chunks:
- ZSTD/OpenZL context is long-lived (created once per thread)
- Output buffer is pre-allocated and reused
- blake3 Hasher is `.update()`'d incrementally, never reallocated

### Law 3: Deterministic Checksums Independent of Core Count

blake3 checksum groups are bound to **chunk_seq** ordering, not thread identity.
Any machine with any number of cores reproduces the same checksums by sorting
decompressed chunks by chunk_seq before hashing.

### Law 4: Writer Is Concurrent — Compressors Never Wait for Index

The writer thread runs as a separate OS thread alongside the compressor threads.
It receives `(Blob, ChunkMeta)` from compressor threads via a channel and writes
each blob to disk **immediately** as it arrives — never buffering all blobs in memory.

Ring buffer slot release:
- `Blob::Owned` (compressed): compressor thread calls `tx_ret` *before* sending to writer —
  the slot is free the moment the compressor finishes, regardless of how fast the writer is.
- `Blob::Revolver` (skip/store path): writer calls `tx_ret` after `write_all` completes —
  the slot is held only until the raw bytes hit disk, then immediately recycled.

**Consequence:** compressor threads and the reader thread never stall waiting for index writing.
The Arrow IPC sub-indexes, manifest, and footer are written only *after* all blobs are flushed —
by which time every ring buffer slot has been returned and every compressor thread has exited.
Index writing is sequential but uncontested; no useful parallel work is delayed.

---

## Ingest Pipeline (Zero-Copy End-to-End)

### Full Data Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         INGEST PIPELINE                                  │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  Phase 1: Stage (IngestBatch)                                   │    │
│  │                                                                 │    │
│  │  read_file() ──→ IngestBatch.push(path, data)   ← move, no copy│    │
│  │  read_file() ──→ IngestBatch.push(path, data)                   │    │
│  │  ...                                                            │    │
│  │  (accumulate until batch_threshold: 200MB)                      │    │
│  └─────────────────────────────────┬───────────────────────────────┘    │
│                                    │                                    │
│  ┌─────────────────────────────────▼───────────────────────────────┐    │
│  │  Phase 2: Plugin Extract (zero-copy borrows)                    │    │
│  │                                                                 │    │
│  │  plugin.extract_batch(&[                                        │    │
│  │      BatchItem { path: &file.path, data: &file.data },  ← &ref │    │
│  │      BatchItem { path: &file.path, data: &file.data },          │    │
│  │      ...                                                        │    │
│  │  ])                                                             │    │
│  │                                                                 │    │
│  │  Per-file plugins (CargoPlugin): instant filename parse         │    │
│  │  Batch plugins (MavenPlugin):    ljar filters "pom.xml",       │    │
│  │                                  32-core parallel inflate       │    │
│  │                                                                 │    │
│  │  Result: each StagedFile.metadata populated                     │    │
│  └─────────────────────────────────┬───────────────────────────────┘    │
│                                    │                                    │
│  ┌─────────────────────────────────▼───────────────────────────────┐    │
│  │  Phase 3: Chunk + Compress (ChunkRevolver)                      │    │
│  │                                                                 │    │
│  │  for file in batch.drain():     ← ownership transfer, no copy   │    │
│  │      split file.data into 10MB chunks                           │    │
│  │      write chunk → ring buffer slot                             │    │
│  │      channel.send(slot_index)   ← 8 bytes only                  │    │
│  │                                                                 │    │
│  │  ┌───────────────────────────────────────────────────────┐      │    │
│  │  │  ChunkRevolver Ring Buffer                            │      │    │
│  │  │  [slot 0][slot 1][slot 2]...[slot N-1]               │      │    │
│  │  └───────▲──────────────────────────────▲────────────────┘      │    │
│  │          │                              │                       │    │
│  │     Reader Thread               32 Compressor Threads           │    │
│  │     (fills slots)               (each: own CCtx, own buf)      │    │
│  │          │                              │                       │    │
│  │          └── send(slot_idx) ──────────→ │ read slot, compress   │    │
│  │                                         │ openzl (or skip)      │    │
│  │                                         │ blake3 checksum       │    │
│  │                                         ▼                       │    │
│  └─────────────────────────────────────────┼───────────────────────┘    │
│                                            │                            │
│  ┌─────────────────────────────────────────▼───────────────────────┐    │
│  │  Phase 4: Write (v0.7 — blobs inline, index at end)            │    │
│  │                                                                 │    │
│  │  Writer thread runs CONCURRENTLY with compressors (Law 4):     │    │
│  │                                                                 │    │
│  │  while recv (Blob, ChunkMeta):                                  │    │
│  │    write_all(blob bytes) → NVMe   ← streaming, no buffer       │    │
│  │    if Blob::Revolver: tx_ret(slot) ← release ring buffer NOW   │    │
│  │    record BlobMeta { blob_offset, blob_size }                   │    │
│  │                                                                 │    │
│  │  [all blobs done, compressors exited, all slots returned]       │    │
│  │                                                                 │    │
│  │  for each (pkg_type, repo) group:                               │    │
│  │    write Arrow IPC sub-index  ← metadata only, no blob data    │    │
│  │  write manifest (Arrow IPC)                                     │    │
│  │  write MAGIC + manifest_offset  ← 16-byte v0.7 footer          │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

### Memory Lifecycle (one allocation per file)

```
read_file()     →  Vec<u8> allocated (the ONLY allocation for this file's data)
                       │
IngestBatch.push()     │  move (no copy)
                       │
plugin.extract_batch() │  &[u8] borrow (zero-copy)
                       │
batch.drain()          │  move to chunker (no copy)
                       │
ChunkRevolver slot     │  memcpy into ring buffer (necessary — fixed slot size)
                       │
Compressor thread      │  reads slot in-place (zero-copy)
                       │
                       ├── compressed=true:  OpenZL → Vec<u8> (new alloc)
                       │                     Buffer::from(vec) — ownership xfer
                       │
                       └── compressed=false: copy slot → Vec<u8>
                                            Buffer::from(vec) — ownership xfer
                       ▼
Arrow IPC write        zdata column row — Buffer already constructed, no extra copy
```

### Allocations Per File: Exactly 2

1. `Vec<u8>` — initial file read (unavoidable)
2. Ring buffer slot write — one memcpy into fixed-size pre-allocated slot

Everything else is moves, borrows, or reuse.

---

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
| text_500mb | 500.00 | 0.09 | 5404x | 1792 | 3145 |
| binary_pattern_500mb | 500.00 | 0.19 | 2609x | 2660 | 3145 |
| random_500mb (incompressible) | 500.00 | 500.04 | 1.00x | 176 | 3205 |
| 100k_small_files_10kb | 976.56 | 12.54 | 77.9x | 3033 | 701 |
| mixed_repo_530mb | 530.05 | 530.01 | 1.00x | 2850 | 2650 |
| single_file_2gb | 2048.00 | 0.35 | 5868x | 4047 | 3413 |

*Run: 2026-05-21, 32-core AMD, NVMe, release build, parallel verify threads*

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

## Section 2: OpenZL Backend (v0.3.0, current)

### Motivation

- **Format-aware compression**: trained codecs per file type (.rs, .toml, .json, .xml, .jar)
- **Better small-file performance**: specialized codecs avoid generic overhead
- **Universal decompressor**: single decoder handles all file types (from frame header)
- **Higher ratios on structured data** at equal or higher speed than zstd

### Format (v0.4 single-file)

Single-file output — Arrow IPC with inline data:

```
Schema (one row per chunk):
  relative_path    Utf8
  chunk_seq        UInt32
  fdata_offset     UInt64
  checksum_group   UInt8
  compressed       Boolean
  uncompressed_size UInt64
  repo             Utf8 (optional, for multi-repo/Nexus)
  extension        DenseUnion (nullable, plugin metadata on chunk_seq=0)
  zdata            LargeBinary (compressed chunk bytes)
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

## Section 3: Single-file Format (v0.5, current)

### Format

Single valid Arrow IPC Stream file — readable by DuckDB/Polars/pyarrow directly:

```
archive.znippy  (Arrow IPC Stream, one row per chunk, data inline as zdata column)
```

```sql
-- DuckDB can query the archive directly:
SELECT relative_path, uncompressed_size FROM 'archive.znippy';
SELECT * FROM 'nexus-export.znippy' WHERE repo = 'libs-release';
```

### Schema (one row per chunk)

```
relative_path     Utf8
chunk_seq         UInt32
fdata_offset      UInt64
checksum_group    UInt8
compressed        Boolean
uncompressed_size UInt64
repo              Utf8 (nullable)
extension         DenseUnion (nullable, plugin metadata on chunk_seq=0)
zdata             LargeBinary (compressed or raw chunk bytes)
```

### Dual-Pipeline Architecture (Zero-Copy)

Files are split into two pipelines based on whether compression is needed:

```
═══════════════════════════════════════════════════════════════
PIPELINE A: COMPRESS (compressed=true)
═══════════════════════════════════════════════════════════════

  File bytes
      │
      ▼
  ┌─────────────────┐
  │  Split chunks   │  (ChunkRevolver ring buffer)
  └────────┬────────┘
           │
     ┌─────┼─────┐        (parallel across cores)
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
  │ (zdata = compressed bytes)  │  ownership transfer, no extra copy
  └─────────────────────────────┘


═══════════════════════════════════════════════════════════════
PIPELINE B: STORE AS-IS (compressed=false, size known upfront)
═══════════════════════════════════════════════════════════════

  File bytes (known size!)
      │
      ├──────────────────────────────────┐
      │                                  │
      ▼                                  ▼
  ┌─────────────────┐     ┌───────────────────────────────┐
  │  Split chunks   │     │  Arrow IPC writer             │
  └────────┬────────┘     │  (zdata = raw bytes)          │  ZERO COPY
           │              │  Buffer::from(vec) — no copy  │  (parallel!)
     ┌─────┼─────┐       └───────────────────────────────┘
     ▼     ▼     ▼
  ┌─────┐┌─────┐┌─────┐
  │blake3││blake3││blake3│  hash only (parallel across cores)
  └──┬───┘└──┬───┘└──┬───┘
     │     │     │
     ▼     ▼     ▼
  ┌─────────────────────┐
  │ Checksum complete   │  (data already in Arrow IPC!)
  └─────────────────────┘
```

### Key Insight: Zero-Copy for Uncompressed Files

For files that skip compression (already-compressed: .jpg, .mp4, .gz, .jar, .png):
- Size is known upfront (= original file size)
- Data doesn't change — raw bytes go directly into Arrow `Buffer`
- `Buffer::from(vec)` transfers ownership without copying
- blake3 hashing runs in parallel on the same data — doesn't block the write
- Result: disk-speed writes for the common case (media, pre-compressed files)

For compressed files:
- OpenZL produces a `Vec<u8>` output
- Convert to Arrow `Buffer::from(compressed_vec)` — ownership transfer, one allocation
- The "extend_from_slice" copy in Arrow IPC is avoided because we construct the Buffer directly

### Benefits

- **Single file** to manage, copy, transfer
- **Valid Arrow IPC** — DuckDB/Polars/pyarrow read it directly
- **Column pruning** — read metadata without loading chunk data
- **Zero-copy writes** — uncompressed files bypass all buffer copies
- **Multi-repo support** via `repo` column + row group partitioning
- **Streaming write** — flush batch every N chunks (bounded memory)

### Multi-repo / Nexus Mode

One `.znippy` archive can contain files from multiple repositories or artifact groups.
The `repo` column acts as a partition key:

```sql
-- Export from Nexus: one archive holds multiple repos
SELECT relative_path, repo FROM 'nexus-export.znippy'
WHERE repo = 'libs-release';

-- Decompress only one repo from the archive
znippy extract nexus-export.znippy --repo libs-release --out ./libs/
```

Use cases:
- **Nexus/Artifactory export**: full mirror in one file, queryable by repo name
- **Monorepo CI cache**: each sub-project is a `repo` group, extract only what changed
- **Airgap transfer**: ship one file containing all repos, unpack selectively

The `repo` column is optional (Utf8, nullable). When absent, all files belong to the
default (unnamed) group. Checksums are still per `checksum_group` (compressor thread),
independent of `repo`.

### Memory Model

- Each batch contains ~100 chunks × 10MB = ~1GB max
- Flush batch → start new batch → constant memory
- Arrow IPC supports unlimited batches per file

---

## Section 4: Extension Metadata (DenseUnion)

### Concept

The index schema includes ONE nullable column `extension` of type `DenseUnion`.
Each archive is written by one application — the union is monomorphic per archive
(all rows use the same variant). Reader checks the union type ID to know the schema.

```
extension: DenseUnion<
  photoalbum_v1: Struct<{ album: Utf8, sort_order: UInt32, thumbnail: Binary }>,
  holger_nexus_v1: Struct<{ group: Utf8, artifact_type: Utf8, version: Utf8 }>,
  // future extensions add new variants — old readers ignore unknown type IDs
>
```

### Design Rules

1. **One column** — `extension` (DenseUnion, nullable)
2. **Monomorphic per archive** — all rows use the same variant (or NULL)
3. **Self-describing** — union type ID + schema = app knows what it reads
4. **Backward compatible** — new variants don't break old readers (unknown ID → skip)
5. **Zero-copy** — native Arrow nested type, DuckDB/Polars read it natively
6. **Column pruning** — don't read `extension` if you don't care about it

### Holger Nexus Extension (`holger_nexus_v1`)

For Holger's "dump all Nexus repos into one znippy":

```
Struct<{
  group: Utf8,           // e.g. "libs-release", "libs-snapshot", "maven-central"
  artifact_type: Utf8,   // e.g. "maven", "cargo", "npm", "docker"
  version: Utf8,         // artifact version string
}>
```

Combined with the `group` column (hierarchical path):
- `group` column: `"maven/libs-release"`, `"cargo/internal"`
- `extension` (holger_nexus_v1): rich metadata per file (type, version)

### Query Examples

```sql
-- DuckDB: query extension fields directly
SELECT relative_path, extension.group, extension.version
FROM 'nexus-dump.znippy'
WHERE extension.artifact_type = 'maven';

-- Selective extract by group prefix
znippy extract nexus-dump.znippy --group maven/ --out ./maven-mirror/
```

### What Holger Needs to Implement

1. **Writer side** (Holger → znippy):
   - Walk Nexus repos, for each artifact:
     - `relative_path`: full path within repo
     - `group`: `"{type}/{repo_name}"` (hierarchical)
     - `extension` (holger_nexus_v1): `{ group, artifact_type, version }`
   - Call `znippy-compress` stream API with `ArchiveEntry` + extension data

2. **Reader side** (znippy → Holger):
   - Read index, filter by `group` prefix or extension fields
   - Selective decompress by offset

3. **CLI flags**:
   - `znippy extract --group maven/libs-release` (prefix match)
   - `znippy list --extension holger_nexus_v1` (show extension fields)

---

## Section 5: Plugin System — Native + WASM with Host Services

### Overview

Znippy plugins extract file-type-specific metadata during compression and store it in the
Arrow index. Two plugin types share one trait (`ArchiveTypePlugin`) with identical semantics:

| Type | Example | When to use |
|------|---------|-------------|
| **Native** (compiled in) | `NativeMavenPlugin`, `CargoPlugin` | Production default — zero overhead |
| **WASM** (loaded at runtime) | `maven.wasm` | Sandboxed, hot-swappable, third-party |

CLI: native maven is used by default; pass `--plugin path.wasm` to load a WASM plugin instead.

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Host (znippy-common)                                       │
│                                                             │
│  ┌───────────┐  ┌────────────┐  ┌───────────┐              │
│  │  ljar-rs  │  │ lbzip2-rs  │  │  lgz-rs   │  Native,     │
│  │  (rayon)  │  │ (workers/  │  │ (parallel │  multi-core   │
│  │           │  │  core)     │  │  gzip)    │              │
│  └─────▲─────┘  └─────▲──────┘  └─────▲─────┘              │
│        │               │               │                    │
│  ══════╪═══════════════╪═══════════════╪════ wasmtime ═══╗  │
│  ║     │               │               │                ║  │
│  ║  ┌──┴───────────────┴───────────────┴──────────┐     ║  │
│  ║  │  Host Functions (Linker)                    │     ║  │
│  ║  │  • host_decompress(ptr,len,codec) → u64     │     ║  │
│  ║  │  • host_archive_open(ptr,len,fmt,           │     ║  │
│  ║  │      filter_ptr,filter_len) → handle        │     ║  │
│  ║  │  • host_archive_entry(hndl,name) → u64      │     ║  │
│  ║  │  • host_archive_list(hndl) → u64 (JSON)     │     ║  │
│  ║  │  • host_archive_close(hndl)                 │     ║  │
│  ║  └──▲──────────────────────────▲───────────────┘     ║  │
│  ║     │                          │                     ║  │
│  ║  ┌──┴────────┐  ┌─────────────┴──┐  ┌───────────┐   ║  │
│  ║  │  maven    │  │    image       │  │  custom   │   ║  │
│  ║  │  .wasm    │  │    .wasm       │  │  .wasm    │   ║  │
│  ║  └───────────┘  └────────────────┘  └───────────┘   ║  │
│  ╚══════════════════════════════════════════════════════╝  │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  Native Plugins (compiled in, zero overhead)          │  │
│  │  • NativeMavenPlugin — ljar parallel, GAV extraction  │  │
│  │  • CargoPlugin — filename parse, no decompression     │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Native Plugin: NativeMavenPlugin

Extracts Maven GAV (groupId, artifactId, version, packaging) from `.jar`, `.war`, `.ear`, `.pom`.

For JAR/WAR/EAR files it calls `ljar::decompress_jar_filter(data, "pom.xml")` which:
- Walks only the ZIP central directory header (O(1) per non-matching entry)
- Decompresses matching entries in parallel across all CPU cores (rayon)
- Returns empty if no `pom.xml` found — plugin returns `None`, file is stored as-is

For `.pom` files it parses XML directly without any decompression.

### WASM Plugin: maven.wasm

Same logic compiled to `wasm32-unknown-unknown`:

```
cargo build --target wasm32-unknown-unknown --release -p znippy-plugin-maven
```

The WASM module calls `host_archive_open(ptr, len, 0, filter_ptr, filter_len)` which:
- Applies the filter at the central-directory level on the host (same ljar path as native)
- Returns handle `0` if nothing matches — plugin returns `"null"` without allocating
- Decompresses only matched entries via host-side ljar (multi-core)

No CLI-specific code is needed inside the plugin — any `.wasm` that exports the standard ABI
(see below) is loadable by the CLI.

### WASM Plugin ABI

The ABI has two parts: the **logical interface** (mirrors the `ArchiveTypePlugin` trait, one
export per method) and the **transport** (how bytes cross the WASM wall, since WASM has no shared heap).

| Export | Signature | Maps to trait method | Role |
|--------|-----------|----------------------|------|
| `plugin_extensions` | `() → u32` | `matches_path` | Logical: comma-separated extension list, cached at load |
| `plugin_schema` | `() → u32` | `schema_fields` | Logical: `name:type` column list, cached at load |
| `extract` | `(path_ptr, path_len, data_ptr, data_len) → u32` | `extract_metadata` | Logical: extract; result in thread-local |
| `alloc` | `(size: u32) → u32` | — | Transport: allocate WASM memory for host writes |
| `dealloc` | `(ptr: u32, size: u32)` | — | Transport: free a previous `alloc` |
| `result_len` | `() → u32` | — | Transport: byte length of last result buffer |

`extract` writes a UTF-8 JSON object to a thread-local buffer and returns its pointer;
`result_len()` lets the host read exactly that many bytes back. Returns `"null"` (4 bytes) when
no metadata is applicable — host skips this file without allocating.

`plugin_extensions` returns e.g. `".jar,.war,.ear,.pom"`. The host calls it **once** at load
and caches the list, so `matches_path` is a cheap host-side string check — no per-file WASM call
(critical because `matches_path` runs on every file in the pre-pass). A plugin that omits this
export matches nothing.

This is the key idea: `alloc`/`dealloc`/`result_len` are **not** the interface — they're the
transport. The interface is the trait; native plugins implement it directly, WASM plugins
implement the same contract via one export per method.

### Host Function API

WASM plugins import these from `"env"`:

| Function | Signature | Description |
|----------|-----------|-------------|
| `host_decompress` | `(ptr, len, codec) → u64` | Decompress raw bytes. Result: `(out_ptr << 32 \| out_len)` |
| `host_archive_open` | `(ptr, len, format, filter_ptr, filter_len) → u32` | Open archive, keep only entries containing filter substring. Returns `0` if invalid or no match. |
| `host_archive_list` | `(handle) → u64` | JSON `["name1","name2",...]` of stored entries |
| `host_archive_entry` | `(handle, name_ptr, name_len) → u64` | Raw bytes of a named entry |
| `host_archive_close` | `(handle)` | Free handle |

**Codec IDs:** `0`=deflate, `1`=gzip (lgz), `2`=bzip2 (lbzip2), `3`=zstd

**Archive Format IDs:** `0`=jar/zip (ljar), `1`=tar.gz, `2`=tar.bz2

**Filter optimization**: the filter is applied at open time at the central-directory level.
Non-matching entries are never decompressed. `handle=0` → no matches → plugin returns `"null"` immediately.

All data crosses the WASM boundary as raw bytes via `(ptr, len)` pairs packed into a `u64`:
`ptr = result >> 32`, `len = result & 0xFFFF_FFFF`.

### CLI Plugin Loading

```sh
# Default: native NativeMavenPlugin (ljar, multi-core, no sandbox)
znippy compress -i repo/ -o archive.znippy

# WASM plugin: same metadata, sandboxed, updatable without recompile
znippy compress -i repo/ -o archive.znippy --plugin maven.wasm

# Custom WASM plugin with a different Arrow type_id
znippy compress -i photos/ -o archive.znippy --plugin exif.wasm --plugin-type-id 3
```

`--plugin` replaces the default native maven plugin. The native and WASM paths produce
identical Arrow index output — only the execution environment differs.

### Arrow Index: Maven Metadata Columns

When either plugin extracts maven metadata, five nullable columns are written for `chunk_seq=0` rows:

| Column | Arrow Type | Example |
|--------|-----------|---------|
| `pkg_type` | Int8, nullable | `1` (maven) |
| `maven_group_id` | Utf8, nullable | `"org.springframework"` |
| `maven_artifact_id` | Utf8, nullable | `"spring-core"` |
| `maven_version` | Utf8, nullable | `"6.1.8"` |
| `maven_packaging` | Utf8, nullable | `"jar"` |

DuckDB query example:
```sql
SELECT maven_group_id, maven_artifact_id, maven_version, COUNT(*) AS chunks
FROM 'archive.znippy'
WHERE pkg_type = 1
GROUP BY maven_group_id, maven_artifact_id, maven_version
ORDER BY chunks DESC;
```

### Native vs WASM Decision Matrix

| Factor | NativeMavenPlugin | maven.wasm |
|--------|------------------|------------|
| Latency | ~5μs/call | ~50μs/call |
| JAR decompression | ljar directly (rayon) | ljar via host functions (rayon) |
| Sandboxing | None (full trust) | Full (wasmtime) |
| Updatable | Recompile znippy | Drop in new .wasm |
| Use for | Default production | Benchmarking, custom, third-party |

### Backend Integration

| Archive Format | Library | Threading | Status |
|---------------|---------|-----------|--------|
| JAR/ZIP | ljar-rs | rayon thread pool | ✅ Native + WASM host function |
| Bzip2 | lbzip2-rs | workers per core | 🔲 Wire to host_decompress |
| Gzip | lgz-rs | parallel (done) | ✅ Wired via host_decompress / tar.gz |
| Zstd | znippy codec | znippy thread pool | ✅ Via codec layer |

---

## Section 6: Self-Contained Modules + Multi-Index Container (planned)

### Goal

znippy core knows **nothing** about any package type. Each module (= package handler) owns
its type's entire lifecycle:

| Responsibility | Native | WASM export | Status |
|----------------|--------|-------------|--------|
| Which files it handles | `matches_path` | `plugin_extensions` | ✅ |
| Its Arrow schema | `schema_fields` | `plugin_schema` | ✅ (native + WASM) |
| Metadata extraction | `extract_metadata` | `extract` | ✅ |
| Its CLI subcommands | `cli(args)` | `plugin_cli` | planned (F5) |

The compressor/decompressor stay **format-level and shared** — modules never reimplement them
(see backlog F5).

### Step 1 (implemented): module-owned schema, single index

- `ArchiveTypePlugin` gains `fn schema_fields(&self) -> Vec<Field>` (default empty).
- Core `ZNIPPY_INDEX_SCHEMA` is the **base 9 columns** only. The writer composes the on-disk
  schema as `base(9) + pkg_type + active module's fields` via `compose_index_schema(ext_fields)`.
- `build_metadata_batch` builds the extension columns **generically** from each file's
  `ExtensionRow`, keyed by `Field::name()` — no per-type code in core.
- The hardcoded `maven_*` columns are removed from core.
- **Column names are unprefixed** (`group_id`, `version`, not `maven_group_id`). Safe because a
  single index carries exactly one module's columns; prefixing was only needed for a hypothetical
  union table, which the multi-index design (Step 2) replaces.

### Step 2 (implemented, v0.7): multi-index container

**Problem:** an Arrow IPC stream allows exactly **one** schema. A multi-type archive
(maven + cargo + pip), where each type has its own narrow schema, cannot share one stream
without a union schema — which reintroduces null-column bloat and forces core to aggregate
every module's fields.

**Solution:** multiple Arrow IPC streams in one file, plus a manifest.

```
┌──────────────────────────────────────────────────────────────────┐
│ blob region:  [blob_0][blob_1]...[blob_N]   all chunk bytes        │
│                              (interleaved, stream-as-produced)     │
├──────────────────────────────────────────────────────────────────┤
│ index_0: Arrow IPC stream   sub-znippy (pkg_type=1, repo=maven)    │  ← narrow schema
│ index_1: Arrow IPC stream   sub-znippy (pkg_type=1, repo=libs)     │
│ index_2: Arrow IPC stream   sub-znippy (pkg_type=2, repo=cargo)    │
│ ...                                                                │
├──────────────────────────────────────────────────────────────────┤
│ manifest: Arrow IPC stream  (itself DuckDB-readable as .arrow)     │
│   pkg_type · repo · module_name · index_offset · index_len ·       │
│   row_count                                                        │
├──────────────────────────────────────────────────────────────────┤
│ [8 bytes "ZNPYMIDX"] [8 bytes LE u64] = manifest_offset  footer   │
└──────────────────────────────────────────────────────────────────┘
```

- A **sub-znippy** = rows sharing `(pkg_type, repo)`; each is an independently valid Arrow IPC
  stream with a narrow schema. Blobs referenced by existing `blob_offset`/`blob_size` columns
  point into the shared interleaved blob region.
- **Backward compatibility:** a reader sees 8 bytes before the trailing offset. If they equal
  `"ZNPYMIDX"` it is v0.7; otherwise v0.6. v0.6 files never carry the magic.
- **Blobs: interleaved** (stream-as-produced). Grouping blobs per sub-znippy would require
  buffering all data before any write, which conflicts with the streaming write model.
  Sub-znippy extraction is done via `znippy export-index --type maven` (emits a standalone
  `.arrow`), not by slicing the blob region.

**Read path:** read footer → detect magic → if v0.7: manifest_offset → manifest → list of
sub-indexes; merge all sub-index batches into a single batch for format-agnostic callers
(`decompress_archive`, `ZnippyArchive`). Per-type query (`znippy maven lookup …`) can use the
sub-index byte range directly with DuckDB/DataFusion.

**DuckDB tradeoff (important):**
- v0.6 single-index is one valid Arrow IPC stream → `SELECT * FROM 'a.znippy'` works directly.
  **Preserved** for single-type archives.
- The multi-index file is *not* a single stream — whole-file direct query is replaced by per-type
  access (module subcommand, or `znippy export-index --type maven` to emit a standalone `.arrow`).
- **Decision (implemented):** single-type archives keep the v0.6 layout; multi-type (or any
  `ArchiveEntry` with distinct `(pkg_type, repo)` pairs) uses the v0.7 manifest container.

**Settled design decisions:**
- Manifest format: Arrow IPC stream (DuckDB-readable as `.arrow`, not a hand-rolled struct).
- Blobs: interleaved (stream-as-produced, simplest; no buffering required).
- `ArchiveEntry` gains optional `pkg_type: Option<i8>` + `repo: Option<String>`; absent means
  "untyped default group" → v0.6 output.

WASM `plugin_schema` transport is settled: `name:type` pairs (`group_id:utf8,version:utf8`),
type tags `utf8`/`u32`/`i8`, parsed host-side into Arrow fields.


