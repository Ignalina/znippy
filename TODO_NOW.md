claude --resume 2d931445-8071-4c0e-a350-9a94e9fa5a14
# TODO_NOW — session 2026-05-24

---

## ⏯ RESUME HERE (paused 2026-05-24 session 2)

DONE session 2 (all GREEN, 21 integration + 5 common tests pass):
- decompress.rs READ path migrated off ChunkRevolver onto Magazine/Gatling model.
  Reader: pread (read_exact_at, no seek) blobs into 200 MB slots. Workers: decompress
  (or zero-copy skip), blake3 verify, pwrite to pre-created output files at fdata_offset.
  No per-thread ring routing. Drain loop in reader = memory-safety barrier (mirrors
  slot_packer.rs write path exactly).
- ChunkRevolver + int_ring DELETED from znippy-common (chunkrevolver.rs, int_ring.rs,
  tests/test.rs). LARGE_SIZE=128 inlined in common_config.rs. strategic_config_mini
  (never used) removed. Serde imports cleaned up. znippy-common now has 0 import warnings.
- NEXT: parallel file opens (tiny-file reader bottleneck), P2 bench cols, benchmarks.

DONE session 1 (all GREEN, build + 21 integration + slotpool/common tests pass):
- Gatling pipeline IS the compressor. `compress_dir` = slot_packer.rs, `compress_stream`
  = stream_packer.rs. Old packer.rs + `Blob` enum DELETED. No legacy in compressor.
- Per-slice blake3 over ORIGINAL bytes (ChunkMeta.checksum; checksum_group gone). Verify
  is per-chunk in decompress.rs.
- Write-offload: barrels compress into recycled buffer (pool, no per-round alloc) →
  hand WriteJob → ONE dedicated writer pwrites at out_cursor.fetch_add. No barrel waits on I/O.
- Gatling names in slotpool.rs: Magazine/Round/Clip/Ejector (ChunkMeta kept).
- codec::compress_into(&mut Vec, src) added. bench_history BenchRun got `version` field.

NEXT STEP (not started): migrate decompress.rs READ path off ChunkRevolver onto the
Magazine/Gatling model — it's the ONLY remaining ChunkRevolver user. Then ChunkRevolver
+ int_ring can be deleted from znippy-common. Decompress currently: reader fills
ChunkRevolver slots from disk, decompressor threads decode per-thread, writer verifies
per-chunk + extracts. Apply same Magazine + dedicated-writer shape.
Other follow-ups: parallel file opens (tiny-file reader bottleneck); P2 bench cols
(file count + cores); P3 int_ring comment cleanup.

Quick check cmd: `cargo test -p znippy-tests --test integration_test && cargo test -p znippy-common`

---

## IMPLEMENTATION PROGRESS (no-barrier pipeline) — live checkpoint

Order: (1) codec → (2) per-slice checksum data-model → (3) packer SlotPool rewrite.
Build must be GREEN at each numbered checkpoint.

- [x] **SlotPool primitive** — `znippy-common/src/slotpool.rs`, wired in lib.rs,
      2 unit tests pass. Dense-coalesce packing, per-slot outstanding counter,
      lock-free freelist (pre-filled crossbeam channel). UNWIRED into packer.
- [x] **(1) codec `compress_into(&mut Vec, src)`** — `znippy-common/src/codec.rs`.
      Reusable output buffer → no per-slice alloc. Old `compress()` kept (stream_packer still uses).
- [x] **(2) per-slice blake3 — DONE, GREEN.** All 20 integration tests pass
      (incl. test_verify_via_decompress, test_index_schema_fields, multi-chunk roundtrips).
      - meta.rs: ChunkMeta now `checksum: [u8;32]` (dropped `checksum_group`)
      - index.rs: dropped `checksum_group` column; `build_metadata_batch` dropped the
        `checksums` param, fills `checksum` from `m.checksum`
      - decompress.rs: verify is now PER-CHUNK inline in the writer (hash uncompressed
        bytes → compare to row checksum). Deleted group_checksums/group_to_idx/verify threads.
        Bonus: no longer copies bytes for verify (hashes the slice it's already holding).
      - packer.rs + stream_packer.rs: per-chunk `blake3::hash` into ChunkMeta.checksum
        (interim for packer.rs — full rewrite in (3))
      - integration_test.rs: schema assertion updated
- [x] **(3) SlotPool pipeline — DONE as new module, GREEN, TESTED.**
      `znippy-compress/src/slot_packer.rs` → `compress_dir_slots()`, compiles
      ALONGSIDE the legacy `compress_dir` (entry point NOT yet flipped).
      New test `test_compress_dir_slots_roundtrip` passes: 50 coalesced small
      files + nested + skip(.gz) + 8MB multi-slice file → decompress → per-slice
      blake3 verify reports 0 corruption, exact byte round-trip. Full suite: 21 pass.
      Implements: SlotPool(8×200MB), reader SlotFill packing (small=whole/1-slice,
      big=slice_size cuts), ONE global slice queue, workers pull→blake3(original)
      →compress_into(reused buf)→pwrite@fetch_add→release_one, finalizer = arrow-ipc
      from ChunkMeta only (no payload channel). Reader drains by reclaiming all
      slots before dropping pool (memory-safety barrier).
      REMAINING for full cutover (needs your OK — changes default behavior):
        - flip callers (CLI/znippy bin, compress_stream?) from compress_dir → compress_dir_slots
        - then delete legacy packer.rs + Blob enum + ChunkRevolver-in-compress
        - empty-file path is handled (0-len slice) but not yet covered by a test
      OLD plan steps (kept for reference):
      write as a NEW fn `compress_dir_slots` in a new module `slot_packer.rs`,
      compiling alongside the old `compress_dir`; flip the entry point only after
      it passes the integration tests. Steps:
        a. Output: `Arc<File>` + `AtomicU64 out_cursor`. Workers
           `file.write_all_at(payload, off)` (std::os::unix::fs::FileExt) at
           `off = out_cursor.fetch_add(len)`. Header/blob region starts at 0.
        b. Reader: `SlotPool::new(8, 200MB, CONFIG.max_core_in_flight)`. For each
           file, `pool.claim()`; pack via `writable()`+`commit_slice()`
           (small → coalesce; big → slice_size cuts spilling to next slot);
           `publish()` → push Slices to one `crossbeam::bounded` global queue.
        c. Workers (max_core_in_flight): pull Slice; per-worker reusable
           `Vec<u8>` out-buf + `CompressCtx`; skip → pwrite slot bytes;
           compress → `compress_into(src,&mut buf)` → pwrite; blake3 per slice
           → send ChunkMeta+blob_offset to finalizer; `returner.release_one(slot_id)`.
        d. Finalizer: collect ChunkMeta (no payload!), sort by (file_index,chunk_seq),
           `build_metadata_batch` at index_offset = out_cursor after blobs, then
           manifest + MULTI_INDEX_MAGIC + trailer (mirror current writer tail).
        e. ext_meta / plugin pre-pass: keep EXACTLY as in compress_dir (outside pool).
        f. Tests: reuse test_compress_dir_basic / _mixed against the new fn; then flip.
      Remove `Blob`/`tx_compressed` only AFTER the flip (decompress.rs DecompBlob
      is separate and stays).
- [x] **FLIP + LEGACY CLEANUP — DONE, GREEN.** `compress_dir` IS the SlotPool
      pipeline now (old packer.rs deleted). `stream_packer.rs` REWRITTEN onto the
      Gatling model (in-memory `Round`s referencing `Arc<Vec<u8>>`, pwrite workers,
      multi-index finalizer) — no ChunkRevolver, no `Blob`. `Blob` enum DELETED.
      Compressor (znippy-compress) is 100% legacy-free. All 21 integration + 11
      common tests pass (incl. all 7 stream tests). Only remaining ChunkRevolver
      user is decompress.rs (the READ path) — separate concern, not the compressor.
- [x] **bench_history.json**: added `version` field to BenchRun (reads znippy/Cargo.toml
      → "0.7.1"), `#[serde(default)]` so old lines parse. Old test data RESTORED (kept).
- [x] **WRITE-OFFLOAD (no wait on I/O) — DONE, GREEN.** Both packers: barrels
      compress into a recycled buffer (pool → no per-round alloc) or skip, then hand
      a WriteJob to ONE dedicated WRITER thread (pwrite at out_cursor.fetch_add,
      recycle buffer / release slot). Barrels never block on disk. 21 tests pass.
- [x] **NAMING — DONE.** Gatling types in slotpool.rs: SlotPool→Magazine, Slice→Round,
      SlotFill→Clip, SlotReturn→Ejector. ChunkMeta kept (fired-round logbook).
      Round = in / Chunk = fired-and-on-disk. slotpool tests + integration green.
- [ ] follow-ups: migrate decompress.rs read path off ChunkRevolver too (only
      remaining user); parallel file opens for tiny-file reader bottleneck; P2 bench cols; P3 comments

NOTE for next session: checksum is over UNCOMPRESSED bytes (compress side hashes
input slice; verify side hashes decompressed output). slice = blake3 unit = work unit.

---

## Holger ecosystem — big picture

Holger is an OSM/geospatial processing pipeline. znippy is its fast archive format.
All the `l*` libs are plugged together to make holger fast:

```
lbzip2   — parallel bzip2 decode  (OSM planet.osm.bz2, 164 GB tar)
lgz      — parallel gzip decode
lzip     — parallel lzip decode
ljar     — parallel JAR read / filter  (decompress only 1 entry: pom.xml)
linflate — raw DEFLATE decode (used inside ljar/lgz)
znippy   — archive format + typed-index builder on top of all of the above
```

znippy has two jobs:
  1. Fast file archive   — store files (compress or zero-copy skip)
  2. Typed index creator — per file-type plugin extracts metadata on ingest
                           (maven → GAV, future: iceberg, etc.)

---

## Architecture: znippy dual-path + plugin layer

```
                         INPUT FILES
                              │
            ┌─────────────────┴──────────────────┐
            │                                    │
   Compressible files                  Pre-compressed files
  (xml, class, txt …)               (JAR, gz, zip, bz2 …)
            │                                    │
            ▼  COMPRESS PATH                     ▼  SKIP PATH (zero-copy)
    ChunkRevolver slot                   ChunkRevolver slot
         (10 MB)                              (10 MB)
    split_into_microchunks              ring ptr passed as-is
    N cores → zstd compress             write_all from ring ptr
    write_all from ring ptr             slot freed after write
    slot freed after write                       │
            │                                    │
            └────────────────┬───────────────────┘
                             │
                       .znp ARCHIVE
                             │
            ┌────────────────┴────────────────────┐
            │                                     │
     Raw extraction                       Plugin / index path
    decompress → output                 (per file-type, on ingest)
                                                  │
                                    JAR / maven (NativeMavenPlugin):
                                    ljar::decompress_jar_filter(data, "pom.xml")
                                      └─ reads ZIP central dir FIRST
                                      └─ seeks to pom.xml entry only
                                      └─ decompresses ONLY that entry
                                      └─ NEVER explodes the full JAR
                                    parse MavenCoord → GAV
                                    → ExtensionRow stored in znippy index
                                    (batch path: accumulate ≥200 MB of JARs,
                                     dispatch all via ljar::thread_pool())
```

Key invariant: the filter happens BEFORE any decompression work.
Cost of a JAR in znippy ≈ ZIP central-dir read + inflate of ONE small file.

---

## THE FINAL SOLUTION 2026 — no-barrier slice pipeline (this is P1-C, decided)

The compressor and the skip path both follow ONE model. This replaces the
per-thread ring design entirely. It is the lbzip2 pattern done right (v2,
not the v1 that barriered on every slot).

### Three principles

1. **Splitting is universal.** Every path splits its input into slices.
   The slice is BOTH the unit of blake3 integrity AND the unit of parallel work.
   They are the same boundary — there is no "unsplit" path.
       skip path:     read → SPLIT → blake3(slice) → write raw from ring ptr   (zero-copy out)
       compress path: read → SPLIT → compress(slice) → blake3 → write          (owned out)

2. **No per-core barrier — ever.** There is ONE global slice queue spanning
   all in-flight slots. A worker that finishes a slice immediately grabs the
   next unclaimed slice, regardless of which slot it lives in.
   A slow slice in slot 0 NEVER blocks slot 1's slices.
   (lbzip2 v1's mistake: all cores waited for a slot to fully complete before
    touching the next slot. That barrier is the bug. We do not do that.)

3. **A slot is just a bag of slices.** Workers don't know or care whether a
   slice is a whole 50 KB JAR (coalesced) or a 6.9 MB cut of the 164 GB planet
   file (split). One lane, no special-casing. Coalescing falls out for free.

### The pipeline

```
reader (1 thread, runs continuously, always stays ahead)
   │ fills slot 0, slot 1, slot 2 …      (ring of N slots; N = read-ahead depth)
   │ per slot: defines its slices —
   │     LARGE file → split into slices across one or more slots
   │     TINY files → pack many files into one slot, 1 file = 1 slice
   ▼
global slice queue:  [s0 s1 … s31 | s32 … s63 | s64 …]
                      └─ slot 0 ─┘ └─ slot 1 ─┘ └ slot 2
   ▲    ▲    ▲    ▲
  core core core core …   each finishes → grabs next unclaimed slice → never idle
   │
   ▼ per slice — THE WORKER WRITES ITS OWN PAYLOAD TO DISK:
     off = out_cursor.fetch_add(len)         // reserve [off,off+len), lock-free
     skip     → blake3 → file.write_all_at(slot_slice, off)  (pwrite, zero-copy)
                       → release slot
     compress → zstd → release slot → file.write_all_at(out_buf, off)
     → send ChunkMeta { blob_offset: off, len, checksum, file_index, … }  (tiny)
   │
   ▼
finalizer (was "writer"): NEVER touches payload bytes. Collects ChunkMeta only,
   builds the arrow-ipc index from offsets, writes index after the blob region.
   pwrite offsets are reserved via one AtomicU64 → on-disk order is reserve-order;
   index maps logical chunk → physical offset, so out-of-order is fine.
```

### Slot lifecycle (outstanding-slice counter)

- Reader fills a slot, defines its slices, sets outstanding = num_slices, publishes.
- Slot release happens ENTIRELY in workers (the finalizer touches no slots):
    compress slice → release right after compression (input consumed)
    skip slice     → release right after its pwrite (slot IS the payload, must
                     outlive the write)
- `release_one` does `fetch_sub(1)`; on hitting 0 the slot id returns to `free`.
- This is the ONLY backpressure: if every slot is full and undrained, the reader
  blocks on `free.pop()`. Workers never wait — the reader is always ahead by N-1 slots.

### Why multiple slots exist

Purely so the single I/O reader stays ahead of the compute cores. Slot count is
read-ahead depth, nothing more. With 1 slot you re-introduce a barrier (workers
drain it, then stall for refill). 6–8 large slots is plenty to keep 32 cores fed.

### Slot / slice sizing (machine-independent)

- slot_size ≈ 200 MB  (matches maven batch_threshold; ring of 8 ≈ 1.6 GB)
- slice_size = slot_size / num_cores, computed AT RUNTIME — no baked constant
    Odin (29 in-flight): ~6.9 MB slices
    4-core laptop:       ~50 MB slices
  Both fill their cores. This is what kills the LARGE_SIZE=128 starvation for good.
- A file smaller than slice_size is never split — it becomes exactly one slice
  and gets coalesced with neighbours into a shared slot.

### LOCKED DECISIONS (2026-05-24)

- **Small-file packing: DENSE COALESCE** (not fixed cells). Pack many small files
  back-to-back into one 200 MB slot, each file = one variable-width contiguous
  slice. 4000 × 50 KB JARs → 4000 slices in one slot. (Rejected: one-file-per-cell.)
- **Return granularity: PER-SLOT** outstanding-slice counter → 0 frees the slot.
  (Dense packing means slices are variable-width and not a fixed grid, so we
  cannot free per-cell; the whole slot returns when its last slice is consumed.)
- **blake3: PER-SLICE** → `checksum: [u8;32]` per ChunkMeta; drop `checksum_group`.
  (Schema break is fine — still v0.7, no compat.)
- **Sizes:** slot = 200 MB, ring = 8 slots, slice = slot / max_core_in_flight (runtime).
- **No hot-path allocations:** slot buffers allocated ONCE at startup; each worker
  gets ONE reusable compress-output buffer (needs codec `compress_into(&mut buf, src)`
  — today `CompressCtx::compress` returns a fresh Vec per call, which must change).

### Zero-copy contract (holger ↔ znippy ↔ l* decompressors)

1. reader → slot: ONE copy (the disk read itself; unavoidable).
2. skip → disk: zero copy — the WORKER pwrites straight from the slot ptr
   (`file.write_all_at(slot_slice, off)`), then releases the slot.
3. slot → decompressor: zero copy — every l* lib takes `&[u8]` borrowed from the
   slot. `ljar::decompress_jar_filter(data,…)`, lbzip2 `decode_chunk(&[u8])`, etc.
   The slot IS the input buffer; the compressed bytes are never copied in.
4. NO PAYLOAD CROSSES A CHANNEL. The finalizer/index-builder receives only
   `ChunkMeta` (offset, len, checksum, file_index…). The `Blob` enum is gone.
5. Only unavoidable allocation: compressed OUTPUT (zstd's destination buffer),
   which goes straight compress → pwrite → drop. Everything else borrows.

Output write model: shared `Arc<File>` + one `AtomicU64` cursor. Each worker
reserves its region via `fetch_add(len)` and `write_all_at` (pwrite) — concurrent,
lock-free, zero-copy. The index is written after the blob region; arrow-ipc only
ever serializes offsets, never payloads ("zero copy to arrow-ipc").

API rule across the ecosystem: **every znippy-decompressor accepts a borrowed
`&[u8]` from the slot and never copies the input.** This is what lets a 164 GB
planet stream through with exactly one read-copy and zero further copies.

### What this obsoletes

- Per-thread rings (29 rings × 5 slots) → gone, replaced by N shared large slots
- `try_get_chunk()` thread round-robin → replaced by global slice-queue pull
- LARGE_SIZE=128 total cap and its starvation → gone (slice_size is dynamic)
- The "fast lane for small files" idea → unnecessary; coalescing is the only lane
- P4 (small-file packing) → solved by coalescing here, not a separate effort
- `Blob` enum + `tx_compressed` payload channel → gone; only ChunkMeta flows,
  workers pwrite their own payloads at reserved offsets
- Writer-thread blob writes + writer-side slot release → gone; finalizer is
  index-only, slots are released solely by workers

---

## What was done this session

### Restored NativeMavenPlugin (was lost, not checked in from other machine)
- `znippy-plugin-maven/src/native.rs` — new file, `NativeMavenPlugin` behind `host-decompressors` feature
- Uses `ljar::decompress_jar_filter(data, "pom.xml")` for fast parallel JAR pom extraction
- Implements `supports_batch() -> true`, `batch_threshold() -> 200MB`, parallel `extract_batch()` via `ljar::thread_pool().install(|| items.par_iter()...)`
- `znippy-plugin-maven/src/lib.rs` — `extract_maven_metadata()` restored (miniz_oxide fallback, no ljar dep), minimal ZIP parser `find_pom_in_jar` + `find_eocd`
- `#[cfg(feature = "resolve")]` gate added to `pub mod resolver` (was breaking non-resolve builds)

### Removed ALL #[ignore] from workspace
- `tests/tests/maven_bench.rs` — all maven tests now run by default
- `tests/tests/perf_bench.rs` — no ignored tests remain
- `znippy-plugin-maven/src/resolver.rs` — `test_resolve_guava_transitive` unignored

### Real-world benchmark corpus wired
- `tests/tests/perf_bench.rs` replaced network-download approach with `~/work/holger_tests/` on Odin's fast disk
- `holger_tests_dir()` → `~/work/holger_tests/`; tests skip gracefully if dir absent
- Three test functions: `perf_real_java_jars`, `perf_real_rust_crates`, `perf_real_python_wheels`

### README test section rewritten
- Leads with `cargo xtask` (run everything before publishing)
- `cargo xtask -- --real` for real-world benchmarks
- `cargo test --workspace` for quick check
- Individual suite commands at the bottom

### bench_history.json baseline reset
- Old baseline was from idle machine with powersave CPU governor (wrong numbers)
- Cleared; new baseline recorded on next xtask run
- CPU governor fix: `echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor`

---

## Architecture notes to remember

### ChunkRevolver slot sizing (DO NOT CHANGE without understanding P1)
- `file_split_block_size = 10MB` always (set in `common_config.rs`)
- `max_chunks = 128` total slots cap (`LARGE_SIZE` constant in `int_ring.rs`)
- On Odin (32 cores): `max_core_in_flight = 29`, `chunks_per_thread = 128/29 + 1 = 5` slots per thread
- The 200MB number is the **maven plugin batch_threshold**, NOT a chunk slot size
- ljar/lgz/lzip/lbzip2 have their own separate large-slot designs — do not confuse with ChunkRevolver

### Zero-copy paths (DO NOT BREAK)
- Compress path: `Blob::Revolver` carries raw ring ptr → writer `write_all` from ptr, slot freed after write
- Skip path (pre-compressed files): data passed through ring ptr, written as-is, zero copy
- Both paths: slot returned ONLY after the write_all completes — this is the backpressure mechanism

### NativeMavenPlugin batch flow
- Accumulate JARs until 200MB threshold (`supports_batch = true`)
- `extract_batch()` dispatches all items in parallel via `ljar::thread_pool()`
- Each item: `ljar::decompress_jar_filter(data, "pom.xml")` → find `META-INF/maven/.../pom.xml` → parse GAV
- Filter reads ZIP central directory FIRST — never decompresses the whole JAR

### lbzip2 NO-WAIT slot design (the reference pattern)
- lbzip2 tested on OSM planet.osm.bz2 (164 GB tar.bz2) — zero starvation
- Design: 6–8 large slots (~100 MB each), dedicated ring
- Reader fills one slot; ALL cores attack that slot simultaneously:
    slot → scan block boundaries → split into N segments (1 per core) → parallel decode
- While cores decode slot N, reader is already filling slot N+1
- Result: reader never stalls, cores never idle — true pipeline overlap
- The key: fewer, LARGER slots + all cores share one slot at a time (not 1 slot per thread)

---

## Remaining work (priority order)

### P1 — Ring slot starvation on Odin

**Problem**: 5 slots/thread on 29 threads = 145 total in-flight slots (29 rings × 5).
Reader fills all 145, has nowhere to put new reads → stalls.
Compressors drain slowly one chunk at a time → pipeline bubble.

**Root cause**: `LARGE_SIZE = 128` caps `max_chunks` as a TOTAL, but
`chunks_per_thread = max_chunks / threads + 1` gives slightly more total
(the +1 overshoot means 145 actual slots, not 128).
Comments in `int_ring.rs` claim these are per-thread sizes — they are not.
This is a lie that matters when diagnosing starvation.

**Three fix options** (pick one, do not combine):

Option A — Bump LARGE_SIZE to 512 total
  → On Odin: 512/29+1 = 18 slots/thread × 29 = 522 total slots × 10MB = 5.2 GB RAM
  → Simple, no formula change, survives P3 comment fix
  → Downside: RAM is fixed regardless of core count

Option B — Dynamic: max_chunks = max_core_in_flight × K (e.g. K=8)
  → On Odin: 29×8 = 232 slots × 10MB = 2.3 GB RAM
  → Scales with core count, right-sized on any machine
  → Change is inside `strategic_confi_large()` only; DO NOT touch `chunks_per_thread` formula
  → Preferred approach if we want portability

Option C — lbzip2 NO-WAIT pattern (bigger rethink, bigger payoff)
  → Replace per-thread rings with a small number of large shared slots (e.g. 8 × 200 MB)
  → Reader fills one large slot; ALL 29 cores split it into microchunks and compress in parallel
  → `split_into_microchunks()` already exists in znippy-common — the primitive is there
  → When slot done, reader has already filled next one → zero stall, same as lbzip2 on OSM planet
  → Works regardless of individual file size
  → Bigger change: ChunkRevolver needs shared-slot mode, not per-thread rings
  → This is the right long-term architecture; B is the quick fix

**DO NOT change** the `chunks_per_thread = num_chunks / safe_thread_count + 1` formula (Option A/B only).

### P2 — Add artifact count + core count to benchmark output
- Benchmark table has no "file count" or "cores" column
- `BenchResult` has `file_count` field but it's only printed in real-world tests
- Add to the synthetic suite table too; add machine core count via `num_cpus` or `sysinfo`

### P3 — LARGE_SIZE comment discrepancy
- `int_ring.rs` comments say sizes are per-thread but code uses them as total slot count
- Fix comments to match reality, or rename constants to be unambiguous
- Example: `LARGE_SIZE: 128 × 10MB × 32 threads ≈ 40 GB` — WRONG, it's 128 × 10MB = 1.28 GB total
- Fix this AFTER P1 is resolved so the new numbers are stable

### P4 — Small-file packing (backlog)
- 100k small files bench: bottlenecked by per-chunk channel overhead, not by compression
- Fix: accumulate files below e.g. 512KB into one logical slot before dispatching to compressors
- Not urgent — 100k × 10KB = 977MB at 726 MB/s decompress is still fast
- Option C from P1 (large shared slots) would partially solve this too — small files would
  pack into the same large slot and all get compressed together

### P5 — v0.8 Iceberg support
- Roadmap item, nothing started

---

## Quick reference commands

```bash
# Run everything (use before publishing a crate)
cargo xtask

# Quick check — tests only, no benchmarks
cargo test --workspace

# Synthetic perf suite only
cargo test --release -p znippy-tests --test perf_bench perf_benchmark_suite -- --nocapture

# Maven plugin tests
cargo test --release -p znippy-tests --test maven_bench -- --nocapture

# Real-world benchmarks (needs ~/work/holger_tests/)
cargo test --release -p znippy-tests --test perf_bench perf_real_java_jars perf_real_rust_crates perf_real_python_wheels -- --nocapture

# CPU governor (must be performance for valid benchmarks on Odin)
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```
