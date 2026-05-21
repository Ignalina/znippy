# Znippy Design Evolution

## Performance Baselines

### 2026-05-21 — v0.2.5 (zstd backend, Arrow IPC, 2-file format)

**Configuration:** zstd level 19, 10MB chunk size, multi-threaded via ChunkRevolver  
**System:** 32-core, release build, hardware-native

| Test | In(MB) | Out(MB) | Ratio | Comp MB/s | Dec MB/s |
|------|--------|---------|-------|-----------|----------|
| text_500mb | 500.00 | 0.09 | 5404x | 1597 | 3086 |
| binary_pattern_500mb | 500.00 | 0.19 | 2609x | 2463 | 3106 |
| random_500mb (incompressible) | 500.00 | 500.04 | 1.00x | 176 | 2994 |
| 100k_small_files_10kb | 976.56 | 12.54 | 77.9x | 3140 | 863 |
| mixed_repo_530mb | 530.05 | 530.01 | 1.00x | 2199 | 2704 |
| single_file_2gb | 2048.00 | 0.35 | 5868x | 3984 | 3089 |

**Notes:**
- Compression saturates at ~4 GB/s on highly compressible 2GB file
- Decompression consistently ~3 GB/s
- `mixed_repo_530mb` includes .jar/.tar.gz (random-like) skipped by default → ratio 1.0
- 100k small files: 3.1 GB/s compress, ~863 MB/s decompress (writer bottleneck on many small outputs)
- Total benchmark: 14.3 seconds for ~5 GB of data processed

### 2026-05-21 — Real-world data (32 cores, zstd level 19)

| Dataset | Files | In(MB) | Out(MB) | Ratio | Comp MB/s | Dec MB/s | Time |
|---------|-------|--------|---------|-------|-----------|----------|------|
| rust crates (airgap mirror) | 53,433 | 1,298 | 174 | 7.5x | 41 | 1,417 | 31.7s |
| git repos (iceberg, burn, etc) | 24,193 | 522 | 313 | 1.7x | 61 | 1,145 | 8.5s |

**Notes:**
- Compression speed bottlenecked by many small files (avg 25KB) — zstd ctx overhead dominates
- Decompression: 1.1-1.4 GB/s on real data
- git repos contain .pack files (already compressed) → low ratio
- 0 corrupt files on both datasets

---

## Format Evolution

### Current: v0.2.5 — Two-file format
- `.znippy` — Arrow IPC index (relative_path, compressed, uncompressed_size, chunks[zdata_offset, fdata_offset, length, chunk_seq, checksum_group])
- `.zdata` — concatenated zstd-compressed chunks

### Planned: v0.3 — Single-file format (Option B)
- One Arrow IPC file, one row per chunk
- Schema: `relative_path | chunk_seq | fdata_offset | checksum_group | compressed | zdata (LargeBinary)`
- Multi-batch writes (bounded memory)
- Optional `repo` column for multi-repo (Nexus export)

### Planned: v0.4 — OpenZL backend
- Replace zstd with OpenZL for per-file-type specialized compression
- Format-aware compressors (trained per file type: .jar, .xml, .pom, etc.)
- Universal decompressor (single binary, any file type)
- Arrow IPC container (no encoding overhead on blob column)
- Expected: better ratios on structured data (XML, JSON, POM) at equal or higher speed

---

## Architecture

```
Input files
    │
    ├─ split into chunks (10MB default)
    │
    ├─ N compressor threads (ChunkRevolver)
    │   └─ each has own CCtx (zstd today, OpenZL tomorrow)
    │
    ├─ writer thread: receives compressed chunks, writes output
    │
    └─ index builder: builds Arrow RecordBatch with metadata + checksums (blake3)
```
