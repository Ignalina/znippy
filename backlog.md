# Znippy Streaming API Backlog

## Context
holger-client needs to feed downloaded crate bytes directly into znippy's compression pipeline
without writing temp files to disk first. This requires a programmatic/streaming API.

## Done
- [x] Created `znippy-compress/src/stream_packer.rs` — a channel-based streaming compressor

## TODO

### 1. Wire up `stream_packer` module in lib.rs
```rust
// znippy-compress/src/lib.rs
pub mod stream_packer;
pub use stream_packer::{compress_stream, StreamCompressor, ArchiveEntry};
```

### 2. Fix `build_arrow_batch_from_files` usage
The writer thread calls `build_arrow_batch_from_files(&file_metadata, Path::new(""))`.
Currently `build_arrow_batch_from_files` strips a prefix from `relative_path`. Since we set
paths directly (no base dir prefix), passing `""` should be fine — but verify it doesn't
panic on empty-string prefix stripping.

### 3. Make `compress2_microchunk` visible to `stream_packer`
Currently `pub` in `packer.rs` — needs `pub(crate)` or re-export so `stream_packer` can use it.

### 4. Build & test
```bash
cd /run/media/rickard/T9/git/znippy
cargo build -p znippy-compress
```

### 5. Integration test
Write a test that creates a few `ArchiveEntry` items in memory, calls `compress_stream()`,
sends them, calls `finish()`, and verifies the output .znippy + .zdata files exist and can
be decompressed.

### 6. Future: true streaming (don't collect all entries first)
Current `stream_packer.rs` collects all entries into a Vec before compressing (simpler, works
for holger-client where we know all entries upfront). A future improvement would process
entries as they arrive through the channel without buffering everything in memory — useful
for very large repositories.

### 7. Future: refactor `compress_dir` to use `compress_stream` internally
Once stream_packer is stable, `compress_dir` becomes:
```rust
pub fn compress_dir(input_dir: &PathBuf, output: &PathBuf, no_skip: bool) -> Result<CompressionReport> {
    let handle = compress_stream(output, no_skip)?;
    for entry in WalkDir::new(input_dir)... {
        let data = std::fs::read(entry.path())?;
        handle.sender().send(ArchiveEntry { relative_path, data })?;
    }
    handle.finish()
}
```

## Architecture

```
holger-client                          znippy-compress
─────────────                          ───────────────
download .crate bytes                  
  │                                    compress_stream(output, no_skip)
  │  ArchiveEntry { path, data }         │
  ├──────────────────────────────────────►│ rx_entry channel
  │                                       │
  │  ArchiveEntry { path, data }          ▼
  ├──────────────────────────────────────► Reader thread (reads from Vec<ArchiveEntry>)
  │                                           │
  │                                           ▼ ChunkRevolver ring buffer
  │                                       N Compressor threads (zstd)
  │                                           │
  │                                           ▼
  │                                       Writer thread → .zdata file
  │                                           │
  finish()                                    ▼
  ◄─────────────────────────────────── Arrow index → .znippy file
  CompressionReport                        
```

## Consumer: holger-client-cli

Located in `/run/media/rickard/T9/git/holger/holger-client-cli/` (to be created).

Commands:
- `holger-client airgap --lockfile Cargo.lock --output my-archive`
  → parse Cargo.lock → download from crates.io → feed into compress_stream()
- `holger-client push --archive my-archive --nexus-url URL --repo NAME`
  → decompress_archive() → upload each .crate to Nexus REST API
