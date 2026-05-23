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
- OpenZL context is long-lived (created once per thread)
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
│  │  Phase 1: Receive (ArchiveEntry stream)                         │    │
│  │                                                                 │    │
│  │  compress_stream() → sender channel                             │    │
│  │  caller sends ArchiveEntry { path, data, pkg_type, repo }       │    │
│  │  each entry: one file, any size                                 │    │
│  └─────────────────────────────────┬───────────────────────────────┘    │
│                                    │                                    │
│  ┌─────────────────────────────────▼───────────────────────────────┐    │
│  │  Phase 2: Plugin Extract (zero-copy borrows)                    │    │
│  │                                                                 │    │
│  │  plugin.extract_metadata(path, &data)  ← &ref, no copy         │    │
│  │                                                                 │    │
│  │  Per-file plugins (CargoPlugin): instant filename parse         │    │
│  │  Batch plugins (MavenPlugin):    ljar filters "pom.xml",        │    │
│  │                                  32-core parallel inflate       │    │
│  └─────────────────────────────────┬───────────────────────────────┘    │
│                                    │                                    │
│  ┌─────────────────────────────────▼───────────────────────────────┐    │
│  │  Phase 3: Chunk + Compress (ChunkRevolver)                      │    │
│  │                                                                 │    │
│  │  split file.data into 1MB chunks                                │    │
│  │  write chunk → ring buffer slot                                 │    │
│  │  channel.send(slot_index)   ← 8 bytes only                      │    │
│  │                                                                 │    │
│  │  ┌───────────────────────────────────────────────────────┐      │    │
│  │  │  ChunkRevolver Ring Buffer                            │      │    │
│  │  │  [slot 0][slot 1][slot 2]...[slot N-1]               │      │    │
│  │  └───────▲──────────────────────────────▲────────────────┘      │    │
│  │          │                              │                       │    │
│  │     Reader Thread               N Compressor Threads            │    │
│  │     (fills slots)               (each: own CCtx, own buf)      │    │
│  │          │                              │                       │    │
│  │          └── send(slot_idx) ──────────→ │ read slot, compress   │    │
│  │                                         │ openzl (or skip)      │    │
│  │                                         │ blake3 checksum       │    │
│  │                                         ▼                       │    │
│  └─────────────────────────────────────────┼───────────────────────┘    │
│                                            │                            │
│  ┌─────────────────────────────────────────▼───────────────────────┐    │
│  │  Phase 4: Write (v0.7 — blobs streaming, index at end)          │    │
│  │                                                                 │    │
│  │  Writer thread runs CONCURRENTLY with compressors (Law 4):      │    │
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
read_file() / ArchiveEntry.data   Vec<u8> allocated (the ONLY allocation for this file)
                       │
plugin.extract_metadata()         &[u8] borrow (zero-copy)
                       │
ChunkRevolver slot                memcpy into ring buffer (necessary — fixed slot size)
                       │
Compressor thread                 reads slot in-place (zero-copy)
                       │
                       ├── compressed=true:  OpenZL → Vec<u8> (new alloc)
                       │                     tx_ret(slot) ← slot freed here
                       │
                       └── compressed=false: Blob::Revolver holds slot
                                            writer calls tx_ret after write_all
                       ▼
Writer thread                     write_all(blob bytes) → NVMe
```

### Allocations Per File: Exactly 2

1. `Vec<u8>` — initial file data (unavoidable)
2. Ring buffer slot write — one memcpy into fixed-size pre-allocated slot

Everything else is moves, borrows, or reuse.

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

### WASM Plugin ABI

The ABI has two parts: the **logical interface** (mirrors the `ArchiveTypePlugin` trait) and
the **transport** (how bytes cross the WASM wall).

| Export | Signature | Maps to trait method | Role |
|--------|-----------|----------------------|------|
| `plugin_extensions` | `() → u32` | `matches_path` | Comma-separated extension list, cached at load |
| `plugin_schema` | `() → u32` | `schema_fields` | `name:type` column list, cached at load |
| `extract` | `(path_ptr, path_len, data_ptr, data_len) → u32` | `extract_metadata` | Extract; result in thread-local |
| `alloc` | `(size: u32) → u32` | — | Allocate WASM memory for host writes |
| `dealloc` | `(ptr: u32, size: u32)` | — | Free a previous `alloc` |
| `result_len` | `() → u32` | — | Byte length of last result buffer |

`extract` writes a UTF-8 JSON object to a thread-local buffer and returns its pointer;
`result_len()` lets the host read exactly that many bytes back. Returns `"null"` (4 bytes) when
no metadata is applicable — host skips this file without allocating.

`plugin_extensions` returns e.g. `".jar,.war,.ear,.pom"`. The host calls it **once** at load
and caches the list — no per-file WASM call (critical because `matches_path` runs on every file).

`plugin_schema` returns e.g. `"group_id:utf8,artifact_id:utf8,version:utf8,packaging:utf8"`.
The host parses this once at load into Arrow `Field` definitions. The writer composes the
on-disk schema as `base_columns + pkg_type + active_module_fields` via `compose_index_schema`.

### Host Function API

WASM plugins import these from `"env"`:

| Function | Signature | Description |
|----------|-----------|-------------|
| `host_decompress` | `(ptr, len, codec) → u64` | Decompress raw bytes. Result: `(out_ptr << 32 \| out_len)` |
| `host_archive_open` | `(ptr, len, format, filter_ptr, filter_len) → u32` | Open archive, keep only entries matching filter. Returns `0` if no match. |
| `host_archive_list` | `(handle) → u64` | JSON `["name1","name2",...]` of stored entries |
| `host_archive_entry` | `(handle, name_ptr, name_len) → u64` | Raw bytes of a named entry |
| `host_archive_close` | `(handle)` | Free handle |

**Codec IDs:** `0`=deflate, `1`=gzip (lgz parallel), `2`=bzip2 (lbzip2), `3`=zstd

**Archive Format IDs:** `0`=jar/zip (ljar), `1`=tar.gz, `2`=tar.bz2

### Arrow Index: Maven Metadata Columns

When the maven plugin extracts metadata, five nullable columns are written for `chunk_seq=0` rows:

| Column | Arrow Type | Example |
|--------|-----------|---------|
| `pkg_type` | Int8, nullable | `1` (maven) |
| `maven_group_id` | Utf8, nullable | `"org.springframework"` |
| `maven_artifact_id` | Utf8, nullable | `"spring-core"` |
| `maven_version` | Utf8, nullable | `"6.1.8"` |
| `maven_packaging` | Utf8, nullable | `"jar"` |

### Native vs WASM Decision Matrix

| Factor | NativeMavenPlugin | maven.wasm |
|--------|------------------|------------|
| Latency | ~5μs/call | ~50μs/call |
| JAR decompression | ljar directly (rayon) | ljar via host functions (rayon) |
| Sandboxing | None (full trust) | Full (wasmtime) |
| Updatable | Recompile znippy | Drop in new .wasm |

### Backend Integration

| Archive Format | Library | Threading | Status |
|---------------|---------|-----------|--------|
| JAR/ZIP | ljar-rs | rayon thread pool | ✅ Native + WASM host function |
| Bzip2 | lbzip2-rs | workers per core | 🔲 Wire to host_decompress |
| Gzip | lgz-rs | parallel segments | ✅ Wired via host_decompress / tar.gz |
| Zstd | znippy codec | znippy thread pool | ✅ Via codec layer |

---

## Section 6: Multi-Index Container Format (v0.7)

### Goal

One `.znippy` archive holds multiple package types (maven, cargo, pip, …) each with multiple
repos, with a narrow per-type Arrow schema — no union-schema null bloat.

| Responsibility | Native | WASM export | Status |
|----------------|--------|-------------|--------|
| Which files it handles | `matches_path` | `plugin_extensions` | ✅ |
| Its Arrow schema | `schema_fields` | `plugin_schema` | ✅ |
| Metadata extraction | `extract_metadata` | `extract` | ✅ |
| Its CLI subcommands | `cli(args)` | `plugin_cli` | planned (F5) |

### Logical view — one archive, many types and repos

```
  dump.znippy
  ┌────────────────────────────────────────────────────────────┐
  │                                                            │
  │   pkg_type = 1  (maven)        pkg_type = 2  (cargo)      │
  │   ┌─────────────────────┐      ┌─────────────────────┐    │
  │   │  repo = "libs"      │      │  repo = "stable"    │    │
  │   │  spring-core.jar    │      │  serde-1.0.crate    │    │
  │   │  guava.jar          │      │  tokio-1.47.crate   │    │
  │   │  …                  │      │  …                  │    │
  │   ├─────────────────────┤      ├─────────────────────┤    │
  │   │  repo = "release"   │      │  repo = "nightly"   │    │
  │   │  myapp-2.0.jar      │      │  rayon-1.10.crate   │    │
  │   │  …                  │      │  …                  │    │
  │   └─────────────────────┘      └─────────────────────┘    │
  │                                                            │
  │   pkg_type = 3  (pip)                                      │
  │   ┌─────────────────────┐                                  │
  │   │  repo = "main"      │                                  │
  │   │  numpy-2.0.whl      │                                  │
  │   │  requests-2.32.whl  │                                  │
  │   │  …                  │                                  │
  │   └─────────────────────┘                                  │
  └────────────────────────────────────────────────────────────┘
```

### Physical file layout

```
  dump.znippy  (on disk)
  ┌───────────────────────────────────────────────────────────────────────┐
  │  BLOB REGION  (written as produced — interleaved across all types)    │
  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐             │
  │  │ blob_0 │ │ blob_1 │ │ blob_2 │ │ blob_3 │ │ blob_N │  …          │
  │  │ maven  │ │ cargo  │ │ pip    │ │ maven  │ │ cargo  │             │
  │  │ libs   │ │ stable │ │ main   │ │release │ │nightly │             │
  │  └────────┘ └────────┘ └────────┘ └────────┘ └────────┘             │
  │                   ↑ no grouping — stream order only                   │
  ├───────────────────────────────────────────────────────────────────────┤
  │  SUB-INDEX REGION  (one Arrow IPC stream per (pkg_type, repo) pair)   │
  │                                                                       │
  │  ┌──────────────────────────────┐  ┌──────────────────────────────┐  │
  │  │ Arrow IPC sub-index           │  │ Arrow IPC sub-index           │  │
  │  │ ─────────────────────────    │  │ ─────────────────────────    │  │
  │  │ pkg_type=1  repo="libs"      │  │ pkg_type=1  repo="release"   │  │
  │  │ relative_path  blob_off  …   │  │ relative_path  blob_off  …   │  │
  │  │ spring-core.jar   0      …   │  │ myapp-2.0.jar   3072     …   │  │
  │  │ guava.jar        512     …   │  │ …                            │  │
  │  └──────────────────────────────┘  └──────────────────────────────┘  │
  │  ┌──────────────────────────────┐  ┌──────────────────────────────┐  │
  │  │ Arrow IPC sub-index           │  │ Arrow IPC sub-index           │  │
  │  │ ─────────────────────────    │  │ ─────────────────────────    │  │
  │  │ pkg_type=2  repo="stable"    │  │ pkg_type=3  repo="main"      │  │
  │  │ relative_path  blob_off  …   │  │ relative_path  blob_off  …   │  │
  │  │ serde-1.0.crate  1024    …   │  │ numpy-2.0.whl    2048    …   │  │
  │  └──────────────────────────────┘  └──────────────────────────────┘  │
  │    … one sub-index per (pkg_type, repo) pair                          │
  ├───────────────────────────────────────────────────────────────────────┤
  │  MANIFEST  (Arrow IPC stream — DuckDB/Polars readable as .arrow)      │
  │  ┌───────────┬───────────┬────────────────┬────────────┬───────────┐  │
  │  │ pkg_type  │   repo    │ sub_idx_offset │ sub_idx_sz │ row_count │  │
  │  ├───────────┼───────────┼────────────────┼────────────┼───────────┤  │
  │  │     1     │ "libs"    │    <offset_0>  │  <size_0>  │    412    │  │
  │  │     1     │ "release" │    <offset_1>  │  <size_1>  │     88    │  │
  │  │     2     │ "stable"  │    <offset_2>  │  <size_2>  │   9162    │  │
  │  │     3     │ "main"    │    <offset_3>  │  <size_3>  │    541    │  │
  │  └───────────┴───────────┴────────────────┴────────────┴───────────┘  │
  ├───────────────────────────────────────────────────────────────────────┤
  │  FOOTER  (16 bytes)                                                   │
  │  [ "ZNPYMIDX"  8 bytes magic ] [ manifest_offset  8 bytes LE u64 ]   │
  └───────────────────────────────────────────────────────────────────────┘
```

### Key properties

- **Blobs interleaved** — written as produced, no buffering. Grouping blobs per type would
  require holding all data in memory before any write — conflicts with streaming Law 4.
- **One sub-index per `(pkg_type, repo)` pair** — each is an independently valid Arrow IPC
  stream with the narrow schema for that type only. `pkg_type` and `repo` are columns in the
  rows, not file-format labels.
- **Manifest is Arrow IPC** — DuckDB/Polars can read `manifest.arrow` directly if extracted.
- **Read path**: footer → magic check → manifest_offset → manifest → sub-index list.
  `decompress_archive` and `ZnippyArchive` merge all sub-indexes transparently via
  `concat_batches`. Per-type queries use the sub-index byte range directly with DuckDB/DataFusion.

### Writing multiple types sequentially (holger workflow)

All entries must be fed into one `compress_stream` session before calling `finish()`.
Sequential per type is correct — the compressor already fans out all cores per chunk,
so adding concurrent sources only fights over ring buffer slots with no gain:

```rust
let compressor = compress_stream(&out, false)?;
let sender = compressor.sender();

// maven repos first (sequential)
for entry in nexus_maven_dump {
    sender.send(ArchiveEntry { pkg_type: Some(1), repo: Some("libs".into()), .. })?;
}
// then cargo repos
for entry in nexus_cargo_dump {
    sender.send(ArchiveEntry { pkg_type: Some(2), repo: Some("stable".into()), .. })?;
}

drop(sender);
compressor.finish()?;
// → seals with sub-index per (pkg_type, repo), manifest, 16-byte footer
```

### pkg_type registry

| Value | Format |
|-------|--------|
| 0 | raw / untyped |
| 1 | maven2 |
| 2 | cargo |
| 3 | npm |
| 4 | pypi / pip |
| 5 | docker |
| 6 | helm |
| 7 | raw (named) |
