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

## Done (v0.7.0)
- [x] Module-owned schema: `ArchiveTypePlugin::schema_fields()`, `compose_index_schema`, `build_metadata_batch` generic over ext columns (Step 1)
- [x] WASM plugin: `plugin_schema` export (`name:type` pairs), `plugin_extensions` — fully documented in design.md §5
- [x] Multi-index container format: manifest Arrow IPC + `ZNPYMIDX` magic footer (Step 2)
- [x] `ArchiveEntry` gains `pkg_type: Option<i8>` + `repo: Option<String>` for per-entry grouping
- [x] v0.7 writer (stream_packer + packer): always emits manifest; groups blobs by `(pkg_type, repo)`
- [x] v0.7 reader: `read_znippy_index` detects magic, reads manifest, merges sub-indexes into one batch
- [x] `read_znippy_manifest` for direct manifest inspection
- [x] Dropped v0.6 read support (archives must be re-compressed)
- [x] 20 integration tests (5 new v0.7: manifest roundtrip, footer detection, multi-index write/read, single-group v0.7)

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

#### F5: Self-contained package-handler modules (znippy as a thin host)

**Vision:** znippy core knows *nothing* about any specific package type. A "module"
(= package handler, native or WASM) is fully self-contained:
- It declares which files it handles (`plugin_extensions`) — already implemented.
- It declares its own CLI commands and parses its own parameters.
- znippy's only responsibility is to **enumerate the registered handlers** and **delegate**.

znippy must never hardcode a plugin's parameters. Adding a new package type (maven, cargo,
npm, docker, …) requires zero changes to znippy core — you register the module and it brings
its full CLI surface with it.

**Why:** otherwise znippy would have to know every present and future plugin's flags
(`--plugin-type-id`, resolver depth, etc.). That doesn't scale and couples the host to every
handler. Self-description inverts it: the module owns its parameters.

**Design sketch (extends the `ArchiveTypePlugin` interface):**
- Interface gains a delegated CLI entrypoint, e.g. `fn cli(&self, args: &[String]) -> Result<()>`
  (default no-op). Native modules implement it directly; WASM modules export a matching
  `plugin_cli(args_ptr, args_len)` and parse the opaque arg blob themselves.
- znippy CLI: `znippy <module> <args...>` looks up the module by name in the registry and
  forwards everything after the module name verbatim. The registry's job is discovery +
  dispatch, not argument parsing.
- `znippy modules` (or `list-handlers`) enumerates registered package handlers — the only
  plugin-aware command znippy itself owns.
- Replaces the current `--plugin <path> --plugin-type-id <n>` flags, which leak plugin
  knowledge into the host. `type_id` and `name` become module self-description (exports),
  not CLI args.

**Module owns its Arrow schema (currently violated):**
Today `pkg_type` + the 5 `maven_*` columns are hardcoded in `ZNIPPY_INDEX_SCHEMA`
(`znippy-common/src/index.rs`) and filled by maven-specific code in `build_metadata_batch`.
That's the host-knows-about-maven coupling to remove. Target:
- Trait gains `fn schema_fields(&self) -> Vec<Field>` (WASM: `plugin_schema` export).
- Writer composes the final schema = `base(9) + ⋃ registered modules' fields`.
- Each module fills only its own columns; `build_metadata_batch` has no per-type code.
- Column-name prefix convention is a hard rule (`maven_*`, `cargo_*`, `python_*`) so modules
  never collide. Schema is defined by the registered module set, so it's stable/predictable
  even though some columns are null for a given row.

**Archive model: one .znippy is a multi-type container.**
A single archive holds multiple package types, each with multiple repos:
`{ maven: [repo1, repo2], cargo: [repo1, repo2, repo3], pip: [...] }`.
- `pkg_type` (Int8) discriminates which module owns a row → which `*_` columns are populated.
- `repo` (Utf8) is the repo within that type. A "logical sub-archive" = rows sharing `(pkg_type, repo)`.
- Selective extract by `(type, repo)` — ties into F1: `znippy extract dump.znippy --type maven --repo repo1`.

**create/decompress are FORMAT-level (shared), not per-module — important.**
The container format (blobs + Arrow IPC index + footer) is universal, so modules must NOT each
reimplement the compressor/decompressor. A module plugs into:
- **write/create**: `matches_path` + `extract_metadata` + `schema_fields` hooks the core pipeline calls.
- **read/query**: package-specific subcommands (`znippy maven lookup --gav …`) that translate to a
  DuckDB/DataFusion query over the Arrow-IPC file — NOT a hand-rolled index walk. The engine does
  execution; the module owns only the schema + the ergonomic command surface.
Generic file extraction (decompress) stays type-agnostic in core.

**CLI surface:**
- `znippy create -i … -o …` / `znippy extract … -o …` — core, type-agnostic.
- `znippy <module> <subop> …` — module-provided query/ops, forwarded verbatim (module parses args).
- `znippy modules` — enumerate registered handlers + their subcommands (the only plugin-aware core cmd).

**Open questions:**
- WASM transport for an argv array (length-prefixed UTF-8 blob vs JSON).
- How a WASM module surfaces its sub-command help text to `znippy <module> --help`.
- Whether modules can register more than one command each.
- Schema = union of *all registered* modules vs only *modules used* in this archive (Arrow writes
  schema first, so "all registered" is simpler; nulls are cheap via the validity bitmap).

#### F6: Spatial archive — znippy as a geo-indexed coordinate store

**Motivation:** katana-osm needs to store ~6.8 billion node coordinates (id, lat, lon)
and look them up by ID during way geometry resolution. Current approach is a 100+ GB
flat file sorted by node ID, with Ragnar's STree for tree-routed mmap access.
Problem: nodes referenced by the same way are spatially close but may be far apart in
ID-sorted order → random 4K page faults across the entire file.

**Idea:** Use znippy as a spatial archive where each "file" is a geographic tile
(S2 cell, geohash bucket, or Hilbert curve segment) stored as a compressed Arrow IPC
chunk (columns: id, lat, lon). Ragnar's STree indexes node_id → tile, then znippy's
per-file random decompression gives you just that tile's data.

```
spatial_nodes.znippy:
  ├── tile_0x3a1f.arrow   (compressed Arrow IPC — ~1M nodes)
  ├── tile_0x3a20.arrow
  ├── ...  (~4096 tiles for planet)
  └── [znippy Arrow IPC index with STree overlay]
```

**Why this wins:**
- Ways reference nodes in 1-3 adjacent tiles → decompress only those tiles
- Each tile (~1M nodes × 16B = 16 MB uncompressed) fits in L3 cache
- Delta-encoding of spatially-local coords compresses extremely well (maybe 4-8x)
- Znippy already supports per-file random decompression — no new format work
- Arrow IPC gives zero-copy columnar access within each tile once decompressed
- Total archive size maybe ~15-20 GB instead of 100 GB (with compression)

**Two-level index:**
1. STree (Ragnar's tree): node_id → tile_index (in-RAM, ~2 GB for planet)
2. Within tile: binary search or secondary STree on the id column

**Simpler alternative — Hilbert-sorted flat file:**
Sort nodes by Hilbert curve of (lat, lon) instead of by ID. Then nodes that are
spatially close ARE file-adjacent. Same flat file + STree architecture, but the mmap
access pattern becomes nearly sequential for way lookups. No znippy compression, but
massively better page-cache locality. Could be a stepping stone.

**Open questions:**
- Tile granularity: too few tiles = large decompressions; too many = index overhead
- Hot tiles (Western Europe, Japan) vs cold tiles (ocean) — adaptive sizing?
- Can we build the spatial index incrementally during pass 1, or need a second pass?
- Hilbert vs S2 vs geohash for the partitioning scheme
- Whether to cache recently-decompressed tiles in an LRU (likely yes, ~8-16 tiles)

### F7: Transfer/resurrect VTD into znippy-zoomies as generic parallel XML parser

The VTD scanner (`xml_vtd` in katana-osm) finds XML element boundaries fast using
memchr. Combined with gatling's worker pool, it gives parallel XML parsing with
zero allocation for the index itself (streaming callback pattern). This is useful
as a generic parallel XML parser module — not OSM-specific. The element-boundary
finding + safe-split logic (`find_safe_slot_end`) is reusable for any large XML.

Transfer the core scanning primitives (`build_elem_index_slice`, `find_safe_slot_end`,
attribute parsing) into znippy-zoomies as a `vtd` module. The OSM-specific record
building stays in katana-osm; the generic XML scanner moves to znippy-zoomies alongside
gatling (which already orchestrates the parallel pipeline around it).

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
