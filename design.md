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

### Architecture

WASM plugins run sandboxed but get native multi-core decompression via host functions.
No threading inside WASM — all parallelism happens on the host side.

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
│  ║  │  • host_decompress(ptr, len, codec) → bytes │     ║  │
│  ║  │  • host_archive_open(ptr, len, fmt) → hndl  │     ║  │
│  ║  │  • host_archive_entry(hndl, name) → bytes   │     ║  │
│  ║  │  • host_archive_list(hndl) → JSON           │     ║  │
│  ║  │  • host_archive_close(hndl)                 │     ║  │
│  ║  └──▲──────────────────────────▲───────────────┘     ║  │
│  ║     │                          │                     ║  │
│  ║  ┌──┴────────┐  ┌─────────────┴──┐  ┌───────────┐   ║  │
│  ║  │  maven    │  │    image       │  │  custom   │   ║  │
│  ║  │  .wasm    │  │    .wasm       │  │  .wasm    │   ║  │
│  ║  │  (185KB)  │  │    (EXIF)      │  │  (user)   │   ║  │
│  ║  └───────────┘  └────────────────┘  └───────────┘   ║  │
│  ╚══════════════════════════════════════════════════════╝  │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  Native Plugins (compiled in, zero overhead)          │  │
│  │  • CargoPlugin — filename parse, no decompression     │  │
│  │  • (future: npm, docker, etc.)                        │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Host Function API

WASM plugins import these from `"env"`:

| Function | Signature | Returns |
|----------|-----------|---------|
| `host_decompress` | `(ptr, len, codec) → u64` | `(out_ptr << 32) \| out_len` |
| `host_archive_open` | `(ptr, len, format) → u32` | Handle |
| `host_archive_list` | `(handle) → u64` | JSON `["name1","name2",...]` |
| `host_archive_entry` | `(handle, name_ptr, name_len) → u64` | Raw bytes of entry |
| `host_archive_close` | `(handle)` | — |

**Codec IDs:** 0=deflate, 1=gzip(lgz), 2=bzip2(lbzip2), 3=zstd

**Archive Format IDs:** 0=jar/zip(ljar), 1=tar.gz, 2=tar.bz2

### Data Flow

All data crosses the WASM boundary as raw bytes `(ptr, len)`:
- Host calls plugin's `alloc(size)` to get writeable WASM memory
- Writes result bytes there, returns packed `(ptr << 32 | len)` as u64
- Plugin unpacks: `ptr = result >> 32`, `len = result & 0xFFFFFFFF`
- No serialization for binary data — strings are UTF-8 bytes

### Native vs WASM Decision Matrix

| Factor | Native | WASM |
|--------|--------|------|
| Latency | ~5μs/call | ~50μs/call |
| Decompression | Direct (multi-core) | Via host functions (multi-core) |
| Updatable | Recompile znippy | Drop in new .wasm |
| Sandbox | None (full trust) | Full isolation |
| Use for | Built-in types (cargo, npm) | Third-party, user plugins |

### Backend Integration Plan

| Backend | Library | Threading Model | Status |
|---------|---------|-----------------|--------|
| JAR/ZIP | ljar-rs | rayon thread pool | ✅ Minimal fallback, native TODO |
| Bzip2 | lbzip2-rs | workers-per-core (no rayon) | 🔲 Wire to host_decompress |
| Gzip | lgz-rs | parallel (planned) | 🔲 Pending lgz creation |
| Zstd | znippy codec | znippy's own thread pool | ✅ Wired via codec layer |


