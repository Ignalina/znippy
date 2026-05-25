//! VTD-style index for OSM XML files.
//!
//! Pass 1: SIMD forward scan over a mmap'd OSM XML file.
//!         Emits one `ElemIndex` entry per top-level element (node/way/relation).
//!         Populates the node store inline — no second read of node bytes needed.
//!
//! Pass 2: filter `ElemIndex` by kind/bbox/tag_flags, seek to matching byte
//!         offsets, parse only those elements. Skips ~85% of planet file (nodes).
//!
//! The ElemIndex array is sorted by file_offset (forward scan order) and
//! written to a mmap'd output file. The OS page cache handles RAM vs NVMe
//! transparently — same code for Sweden (2 GB) and planet (1 TB).

use std::{
    fs::{File, OpenOptions},
    path::Path,
};

use anyhow::{Context as _, Result};
use memchr::memchr;
use memmap2::{Mmap, MmapMut, MmapOptions};

// ── Element kinds ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ElemKind {
    Node     = 0,
    Way      = 1,
    Relation = 2,
}

// ── Tag flag bitmask (extendable) ────────────────────────────────────────────

pub mod tag_flags {
    pub const HIGHWAY:  u32 = 1 << 0;
    pub const BUILDING: u32 = 1 << 1;
    pub const NATURAL:  u32 = 1 << 2;
    pub const LANDUSE:  u32 = 1 << 3;
    pub const WATERWAY: u32 = 1 << 4;
    pub const RAILWAY:  u32 = 1 << 5;
    pub const AMENITY:  u32 = 1 << 6;
    pub const BOUNDARY: u32 = 1 << 7;
}

// ── ElemIndex entry ───────────────────────────────────────────────────────────

/// One entry per top-level OSM element. 32 bytes, cache-line friendly in bulk.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct ElemIndex {
    /// Byte offset of the element's opening `<` in the mmap'd file.
    pub file_offset: u64,
    /// Byte length of the complete element (opening tag through closing tag).
    pub file_length: u32,
    pub kind:        ElemKind,
    pub _pad:        [u8; 3],
    pub id:          i64,
    /// Latitude in degrees × 1e7 as i32 (nodes only, else 0).
    pub lat_e7:      i32,
    /// Longitude in degrees × 1e7 as i32 (nodes only, else 0).
    pub lon_e7:      i32,
    /// Bitmask of notable tags present in this element.
    pub tag_flags:   u32,
}

// ── Mmap helpers ─────────────────────────────────────────────────────────────

/// Open a file read-only and mmap it. Returns the Mmap (keep alive alongside slice).
pub fn mmap_input(path: &Path) -> Result<Mmap> {
    let file = File::open(path)
        .with_context(|| format!("cannot open {}", path.display()))?;
    // SAFETY: we hold the File open for the lifetime of the Mmap.
    let mmap = unsafe { MmapOptions::new().map(&file) }
        .with_context(|| format!("cannot mmap {}", path.display()))?;
    Ok(mmap)
}

/// Create / truncate an output file and mmap it read-write at `capacity` bytes.
pub fn mmap_output(path: &Path, capacity: usize) -> Result<MmapMut> {
    let file = OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(path)
        .with_context(|| format!("cannot create {}", path.display()))?;
    file.set_len(capacity as u64)
        .context("cannot pre-allocate ElemIndex file")?;
    // SAFETY: we hold the File open for the lifetime of the MmapMut.
    let mmap = unsafe { MmapOptions::new().map_mut(&file) }
        .with_context(|| format!("cannot mmap output {}", path.display()))?;
    Ok(mmap)
}

// ── SIMD forward scanner ──────────────────────────────────────────────────────

/// Scan `bytes` for the next `<` at or after `pos`.
/// Uses AVX2 via the memchr crate — ~32 bytes/cycle on hot paths.
#[inline]
fn next_open(bytes: &[u8], pos: usize) -> Option<usize> {
    memchr(b'<', &bytes[pos..]).map(|rel| pos + rel)
}

/// Scan `bytes` for the next `>` at or after `pos`. Sequential — tags are short.
#[inline]
fn next_close(bytes: &[u8], pos: usize) -> Option<usize> {
    memchr(b'>', &bytes[pos..]).map(|rel| pos + rel)
}

/// Read attribute value: find the next `"..."` pair starting at `pos`.
/// Returns `(value_slice, pos_after_closing_quote)`.
#[inline]
fn attr_value<'a>(bytes: &'a [u8], pos: usize) -> Option<(&'a [u8], usize)> {
    // skip to opening quote
    let open = memchr(b'"', &bytes[pos..])?  + pos;
    let close = memchr(b'"', &bytes[open + 1..])? + open + 1;
    Some((&bytes[open + 1..close], close + 1))
}

/// Find the value of attribute `name` within a tag byte slice `tag` (the bytes
/// between `<` and `>`). Returns the raw (possibly escaped) value bytes.
pub fn find_attr<'a>(tag: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
    let mut pos = 0;
    while pos < tag.len() {
        // find next `=`
        let eq = memchr(b'=', &tag[pos..])? + pos;
        // the attribute name ends at the `=`; scan back past whitespace
        let name_end = eq;
        let mut name_start = name_end;
        while name_start > 0 && tag[name_start - 1] != b' ' && tag[name_start - 1] != b'\n' {
            name_start -= 1;
        }
        if &tag[name_start..name_end] == name {
            // found — read value
            let (val, _after) = attr_value(tag, eq + 1)?;
            return Some(val);
        }
        // skip past the `=` and its value
        if let Some((_, after)) = attr_value(tag, eq + 1) {
            pos = after;
        } else {
            break;
        }
    }
    None
}

/// Parse a decimal integer from ASCII bytes. No allocation, no UTF-8 decode.
pub fn parse_i64(bytes: &[u8]) -> i64 {
    let (neg, digits) = match bytes.first() {
        Some(&b'-') => (true,  &bytes[1..]),
        _           => (false, bytes),
    };
    let mut v: i64 = 0;
    for &b in digits {
        if b.is_ascii_digit() {
            v = v.wrapping_mul(10).wrapping_add((b - b'0') as i64);
        }
    }
    if neg { -v } else { v }
}

/// Parse a decimal float (lat/lon) × 1e7 as i32 from ASCII bytes.
/// Avoids any float parsing — works directly on the digit string.
fn parse_coord_e7(bytes: &[u8]) -> i32 {
    // e.g. b"59.3293" → 59_329_300i32
    let (neg, digits) = match bytes.first() {
        Some(&b'-') => (true,  &bytes[1..]),
        _           => (false, bytes),
    };
    let dot = memchr(b'.', digits).unwrap_or(digits.len());
    let int_part  = &digits[..dot];
    let frac_part = if dot < digits.len() { &digits[dot + 1..] } else { b"" };

    let mut val: i64 = 0;
    for &b in int_part  { if b.is_ascii_digit() { val = val * 10 + (b - b'0') as i64; } }
    val *= 10_000_000; // scale to e7

    let mut frac: i64 = 0;
    let mut scale: i64 = 1_000_000; // first decimal = 1_000_000 out of 10_000_000
    for &b in frac_part {
        if b.is_ascii_digit() && scale > 0 {
            frac += (b - b'0') as i64 * scale;
            scale /= 10;
        }
    }
    let result = (val + frac) as i32;
    if neg { -result } else { result }
}

// ── Tag flag detection ────────────────────────────────────────────────────────

fn tag_flag_for(key: &[u8]) -> u32 {
    match key {
        b"highway"  => tag_flags::HIGHWAY,
        b"building" => tag_flags::BUILDING,
        b"natural"  => tag_flags::NATURAL,
        b"landuse"  => tag_flags::LANDUSE,
        b"waterway" => tag_flags::WATERWAY,
        b"railway"  => tag_flags::RAILWAY,
        b"amenity"  => tag_flags::AMENITY,
        b"boundary" => tag_flags::BOUNDARY,
        _           => 0,
    }
}

// ── Pass 1: build ElemIndex ───────────────────────────────────────────────────

/// Scan forward from `from` to the start of the next top-level OSM element
/// (`<node`, `<way`, `<relation`). Returns the byte offset of the `<`.
/// Returns `bytes.len()` if none is found.
pub fn find_top_level_start(bytes: &[u8], from: usize) -> usize {
    let mut pos = from;
    while pos < bytes.len() {
        let Some(rel) = memchr(b'<', &bytes[pos..]) else { break };
        let lt   = pos + rel;
        let rest = bytes.get(lt + 1..).unwrap_or_default();
        if rest.starts_with(b"node ")     || rest.starts_with(b"node\t")
        || rest.starts_with(b"way ")      || rest.starts_with(b"way\t")
        || rest.starts_with(b"relation ") || rest.starts_with(b"relation\t")
        {
            return lt;
        }
        pos = lt + 1;
    }
    bytes.len()
}

/// Core scanner: emit one `ElemIndex` per top-level OSM element in `slice`.
/// `base` is added to every `file_offset` so entries carry absolute positions
/// into the original mmap'd file (not positions within the slice).
pub fn build_elem_index_slice(slice: &[u8], base: usize, on_elem: &mut impl FnMut(ElemIndex)) -> u64 {
    let mut pos   = 0usize;
    let mut count = 0u64;

    while let Some(open_pos) = next_open(slice, pos) {
        let tag_start = open_pos + 1;

        // closing-tag or comment — skip
        if slice.get(tag_start) == Some(&b'/') || slice.get(tag_start) == Some(&b'!') {
            pos = match next_close(slice, tag_start) {
                Some(p) => p + 1,
                None    => break,
            };
            continue;
        }

        let close_pos = match next_close(slice, tag_start) {
            Some(p) => p,
            None    => break,
        };

        let tag = &slice[tag_start..close_pos];
        let self_closing = tag.last() == Some(&b'/');
        let tag = if self_closing { &tag[..tag.len() - 1] } else { tag };

        let name_end = memchr(b' ', tag).unwrap_or(tag.len());
        let name = &tag[..name_end];

        let kind = match name {
            b"node"     => ElemKind::Node,
            b"way"      => ElemKind::Way,
            b"relation" => ElemKind::Relation,
            _ => { pos = close_pos + 1; continue; }
        };

        let id     = find_attr(tag, b"id").map(parse_i64).unwrap_or(0);
        let lat_e7 = find_attr(tag, b"lat").map(parse_coord_e7).unwrap_or(0);
        let lon_e7 = find_attr(tag, b"lon").map(parse_coord_e7).unwrap_or(0);

        let (elem_end, tag_flags) = if self_closing {
            (close_pos + 1, 0u32)
        } else {
            let closing = match kind {
                ElemKind::Node     => b"</node>".as_slice(),
                ElemKind::Way      => b"</way>".as_slice(),
                ElemKind::Relation => b"</relation>".as_slice(),
            };
            let mut flags = 0u32;
            let mut inner = close_pos + 1;

            loop {
                let child_open = match next_open(slice, inner) {
                    Some(p) => p,
                    None    => break,
                };
                let child_tag_start = child_open + 1;
                let child_close = match next_close(slice, child_tag_start) {
                    Some(p) => p,
                    None    => break,
                };
                let child_tag = &slice[child_tag_start..child_close];
                if child_tag.starts_with(b"/") { break; }

                let child_name_end = memchr(b' ', child_tag).unwrap_or(child_tag.len());
                if &child_tag[..child_name_end] == b"tag" {
                    if let Some(key) = find_attr(child_tag, b"k") {
                        flags |= tag_flag_for(key);
                    }
                }
                inner = child_close + 1;
            }

            let mut search = close_pos + 1;
            let end_pos = loop {
                match next_open(slice, search) {
                    None    => break slice.len(),
                    Some(p) => {
                        if slice[p..].starts_with(closing) { break p + closing.len(); }
                        search = p + 1;
                    }
                }
            };
            (end_pos, flags)
        };

        #[allow(clippy::cast_possible_truncation, reason = "file_length fits u32 for any real OSM element")]
        on_elem(ElemIndex {
            file_offset: (open_pos + base) as u64,
            file_length: (elem_end - open_pos) as u32,
            kind,
            _pad: [0; 3],
            id,
            lat_e7,
            lon_e7,
            tag_flags,
        });
        count += 1;
        pos = elem_end;
    }

    count
}

/// Single-threaded scan. Kept for tests and small files.
pub fn build_elem_index<F>(bytes: &[u8], mut on_elem: F) -> Result<u64>
where
    F: FnMut(ElemIndex),
{
    Ok(build_elem_index_slice(bytes, 0, &mut on_elem))
}

/// Parallel scan: divides the mmap'd file into `n_workers` ranges, assigns one
/// thread per range via `std::thread::scope`, resolves chunk boundaries with an
/// `AtomicU64` rendezvous per boundary.
///
/// Each thread i:
///   1. Scans forward from its nominal start to find the first top-level `<node`,
///      `<way`, or `<relation` — its **actual start**.
///   2. Posts that actual start to `slot[i-1]`, unblocking the previous thread.
///   3. Spin-waits on `slot[i]` for the next thread to post **its** actual start —
///      that becomes this thread's actual end.
///   4. Parses the clean `[actual_start, actual_end)` slice.
///
/// Results are collected per-thread into `Vec<ElemIndex>`, then merged in file
/// order and fed to `on_elem` on the calling thread.
pub fn build_elem_index_parallel<F>(
    bytes:     &[u8],
    n_workers: usize,
    mut on_elem: F,
) -> Result<u64>
where
    F: FnMut(ElemIndex),
{
    use std::sync::atomic::{AtomicU64, Ordering};

    let n = n_workers.max(1);

    // Not worth the thread overhead for tiny files.
    if n == 1 || bytes.len() < 4 * 1024 * 1024 {
        return build_elem_index(bytes, on_elem);
    }

    let chunk_size = bytes.len() / n;

    // slot[i] = actual_start of chunk i+1, written by thread i+1 upon startup.
    // u64::MAX = "not yet posted".
    let slots: Vec<AtomicU64> = (0..n).map(|_| AtomicU64::new(u64::MAX)).collect();

    let partials: Vec<Vec<ElemIndex>> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..n)
            .map(|i| {
                let bytes = bytes;
                let slots = &slots[..];
                s.spawn(move || {
                    let nominal = i * chunk_size;
                    let actual_start = find_top_level_start(bytes, nominal);

                    // Unblock the previous thread immediately.
                    if i > 0 {
                        slots[i - 1].store(actual_start as u64, Ordering::Release);
                    }

                    // Wait for the next thread to tell us where our slice ends.
                    let actual_end = if i == n - 1 {
                        bytes.len()
                    } else {
                        loop {
                            let v = slots[i].load(Ordering::Acquire);
                            if v != u64::MAX { break v as usize; }
                            std::hint::spin_loop();
                        }
                    };

                    let mut local: Vec<ElemIndex> = Vec::new();
                    build_elem_index_slice(
                        &bytes[actual_start..actual_end],
                        actual_start,
                        &mut |e| local.push(e),
                    );
                    local
                })
            })
            .collect();

        handles.into_iter().map(|h| h.join().expect("xml vtd worker panicked")).collect()
    });

    let mut total = 0u64;
    for partial in partials {
        total += partial.len() as u64;
        for e in partial { on_elem(e); }
    }
    Ok(total)
}

// ── Mmap streaming index ─────────────────────────────────────────────────────

/// Count top-level OSM elements in `slice` without full parsing.
/// Scans for `<node `, `<way `, `<relation ` — these byte sequences only
/// appear as top-level element starts in valid OSM XML (tag values containing
/// `<` must be escaped as `&lt;`). ~10× faster than a full parse.
pub fn count_elements(slice: &[u8]) -> usize {
    let mut count = 0usize;
    let mut pos   = 0;
    while pos < slice.len() {
        let Some(rel) = memchr(b'<', &slice[pos..]) else { break };
        let lt   = pos + rel;
        let rest = slice.get(lt + 1..).unwrap_or_default();
        if rest.starts_with(b"node ")     || rest.starts_with(b"node\t")
        || rest.starts_with(b"way ")      || rest.starts_with(b"way\t")
        || rest.starts_with(b"relation ") || rest.starts_with(b"relation\t")
        {
            count += 1;
        }
        pos = lt + 1;
    }
    count
}

/// Cast a mmap written by `build_elem_index_to_mmap` back to a typed slice.
/// The mmap must be page-aligned (guaranteed by the OS) and its length must
/// be a multiple of `size_of::<ElemIndex>()`.
pub fn as_elem_index(mmap: &Mmap) -> &[ElemIndex] {
    let bytes     = mmap.as_ref();
    let elem_size = std::mem::size_of::<ElemIndex>();
    assert_eq!(bytes.len() % elem_size, 0, "mmap length not aligned to ElemIndex");
    // SAFETY: ElemIndex is repr(C) + Copy. Mmap is page-aligned (4096 B) which
    // is ≥ align_of::<ElemIndex>() (8 B). Length and origin are both controlled
    // by this module.
    unsafe {
        std::slice::from_raw_parts(bytes.as_ptr() as *const ElemIndex, bytes.len() / elem_size)
    }
}

/// Planet-scale parallel scan: writes the ElemIndex directly to a mmap'd file
/// without ever accumulating the full index in RAM.
///
/// Two scopes, N threads each (N = n_workers):
///
/// **Scope 1 — rendezvous + count** (same AtomicU64 rendezvous as
/// `build_elem_index_parallel`):
///   Thread i posts its actual_start to `actual_slots[i]`.
///   Thread i spins on `actual_slots[i+1]` to learn its actual_end.
///   Then counts elements in `[actual_start, actual_end)` and posts the count.
///   All N threads run concurrently; the spin waits are ~microseconds since
///   every thread posts its start immediately after one memchr scan.
///
/// After scope 1: prefix-sum the counts → allocate the mmap at the exact size.
///
/// **Scope 2 — parse + write**:
///   Each thread knows its write offset from the prefix sum.
///   Writes each `ElemIndex` entry directly to its exclusive mmap region.
///   Zero Vec accumulation — the index lives only in the OS page cache.
///
/// Peak extra RAM: `2 × n_workers × sizeof(AtomicU64)` for the slot arrays.
pub fn build_elem_index_to_mmap(
    bytes:     &[u8],
    n_workers: usize,
    idx_path:  &Path,
) -> Result<u64> {
    use std::sync::atomic::{AtomicU64, Ordering};

    let n          = n_workers.max(1);
    let chunk_size = (bytes.len() / n).max(1);
    const SENTINEL: u64 = u64::MAX;

    // actual_slots[i] = actual_start of chunk i (thread i posts, thread i-1 reads).
    let actual_slots: Vec<AtomicU64> = (0..n).map(|_| AtomicU64::new(SENTINEL)).collect();
    // count_slots[i]  = element count of chunk i (posted after counting).
    let count_slots:  Vec<AtomicU64> = (0..n).map(|_| AtomicU64::new(SENTINEL)).collect();

    // ── Scope 1: rendezvous + count ──────────────────────────────────────────
    std::thread::scope(|s| {
        (0..n)
            .map(|i| {
                let bytes        = bytes;
                let actual_slots = &actual_slots[..];
                let count_slots  = &count_slots[..];
                s.spawn(move || {
                    // Find and immediately post own actual_start.
                    let actual_start = find_top_level_start(bytes, i * chunk_size);
                    actual_slots[i].store(actual_start as u64, Ordering::Release);

                    // Spin on the next chunk's slot to learn our actual_end.
                    // The next thread posts nearly instantly (one memchr scan).
                    let actual_end = if i + 1 == n {
                        bytes.len()
                    } else {
                        loop {
                            let v = actual_slots[i + 1].load(Ordering::Acquire);
                            if v != SENTINEL { break v as usize; }
                            std::hint::spin_loop();
                        }
                    };

                    let count = count_elements(&bytes[actual_start..actual_end]);
                    count_slots[i].store(count as u64, Ordering::Release);
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .for_each(|h| h.join().expect("rendezvous+count thread panicked"));
    });

    // All threads done — relaxed loads are fine.
    let actual_starts: Vec<usize> = actual_slots.iter()
        .map(|a| a.load(Ordering::Relaxed) as usize)
        .collect();
    let counts: Vec<usize> = count_slots.iter()
        .map(|a| a.load(Ordering::Relaxed) as usize)
        .collect();

    // Prefix-sum → per-chunk write offsets → exact mmap size.
    let mut write_offsets = vec![0usize; n];
    for i in 1..n { write_offsets[i] = write_offsets[i - 1] + counts[i - 1]; }
    let total    = write_offsets[n - 1] + counts[n - 1];
    let capacity = total * std::mem::size_of::<ElemIndex>();

    let mut idx_mmap = mmap_output(idx_path, capacity)?;
    let mmap_ptr: usize = idx_mmap.as_mut_ptr() as usize; // usize is Send

    // ── Scope 2: parse + write directly to mmap ──────────────────────────────
    std::thread::scope(|s| {
        (0..n)
            .map(|i| {
                let bytes        = bytes;
                let actual_start = actual_starts[i];
                let actual_end   = if i + 1 < n { actual_starts[i + 1] } else { bytes.len() };
                let write_offset = write_offsets[i];
                let expected     = counts[i];
                s.spawn(move || {
                    let mut idx = 0usize;
                    build_elem_index_slice(
                        &bytes[actual_start..actual_end],
                        actual_start,
                        &mut |e| {
                            // SAFETY: [write_offset, write_offset + expected) is
                            // exclusive to this thread — guaranteed by prefix sum.
                            // The mmap pointer is valid for the scope's duration.
                            unsafe {
                                std::ptr::write(
                                    (mmap_ptr as *mut ElemIndex).add(write_offset + idx),
                                    e,
                                );
                            }
                            idx += 1;
                        },
                    );
                    debug_assert_eq!(idx, expected,
                        "chunk {i}: counted {expected} but parsed {idx}");
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .for_each(|h| h.join().expect("parse+write thread panicked"));
    });

    idx_mmap.flush().context("flushing ElemIndex mmap")?;
    Ok(total as u64)
}

// ── ChunkRevolver: no-barrier parallel mmap writer ───────────────────────────

/// True revolver: threads parse freely into a local Vec, then commit in file
/// order via a `next_to_commit` counter. No barrier between chunks — a thread
/// that finishes early immediately picks up the next chunk.
///
/// Output is in file order (required by pass 2). The ordered-commit spin is
/// brief because chunks of similar size finish close together.
/// Workers parse freely and send `(chunk_index, vec)` over an mpsc channel.
/// A single committer thread drains the channel in file order using a BTreeMap
/// and writes each run directly to the mmap — writes overlap parsing.
///
/// Workers never wait on each other. The committer's BTreeMap holds at most
/// `n_workers` entries (one per in-flight chunk). Peak RAM:
/// `n_workers × chunk_bytes / compression_ratio`, where chunk_bytes ≈ file/n_chunks.
pub fn build_elem_index_revolver(
    bytes:     &[u8],
    n_workers: usize,
    idx_path:  &Path,
) -> Result<u64> {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::sync::Arc;

    let n          = n_workers.max(1);
    let n_chunks   = n * 5 / 4; // +25% over-provision keeps cores busy during commits
    let chunk_size = (bytes.len() / n_chunks).max(1);

    // Pre-compute boundaries once on the calling thread (cheap: n_chunks memchr scans).
    let mut boundaries: Vec<usize> = Vec::with_capacity(n_chunks + 1);
    for i in 0..n_chunks {
        boundaries.push(find_top_level_start(bytes, i * chunk_size));
    }
    boundaries.push(bytes.len());
    let boundaries = Arc::new(boundaries);

    // Count elements in parallel over the pre-computed chunks — no extra sequential scan.
    let counts: Vec<usize> = {
        let b = &*boundaries;
        std::thread::scope(|s| {
            let handles: Vec<_> = (0..n_chunks)
                .map(|i| s.spawn(move || count_elements(&bytes[b[i]..b[i + 1]])))
                .collect();
            handles.into_iter().map(|h| h.join().expect("count thread panicked")).collect()
        })
    };
    let total    = counts.iter().sum::<usize>();
    let capacity = (total + 1) * std::mem::size_of::<ElemIndex>();
    let mut idx_mmap = mmap_output(idx_path, capacity)?;
    let mmap_ptr: usize = idx_mmap.as_mut_ptr() as usize;

    let next_chunk = Arc::new(AtomicUsize::new(0));
    let (tx, rx)   = mpsc::channel::<(usize, Vec<ElemIndex>)>();

    std::thread::scope(|s| {
        // ── Worker threads ────────────────────────────────────────────────────
        for _ in 0..n {
            let next_chunk = Arc::clone(&next_chunk);
            let boundaries = Arc::clone(&boundaries);
            let tx         = tx.clone();
            s.spawn(move || {
                loop {
                    let ci = next_chunk.fetch_add(1, Ordering::Relaxed);
                    if ci >= n_chunks { break; }

                    let start = boundaries[ci];
                    let end   = boundaries[ci + 1];
                    if start >= end {
                        tx.send((ci, Vec::new())).ok();
                        continue;
                    }

                    let mut local: Vec<ElemIndex> = Vec::new();
                    build_elem_index_slice(&bytes[start..end], start, &mut |e| local.push(e));
                    tx.send((ci, local)).ok();
                }
            });
        }
        drop(tx); // close sender side so rx.recv() returns Err when all workers done

        // ── Committer thread (runs on this scope thread) ──────────────────────
        // Drains the channel in file order. BTreeMap buffers out-of-order arrivals.
        // The hot path (in-order arrival) commits immediately without BTreeMap churn.
        let mut pending: BTreeMap<usize, Vec<ElemIndex>> = BTreeMap::new();
        let mut next_expected = 0usize;
        let mut write_head    = 0usize;

        while let Ok((ci, vec)) = rx.recv() {
            pending.insert(ci, vec);
            while let Some(v) = pending.remove(&next_expected) {
                let slot = write_head;
                for (i, e) in v.iter().enumerate() {
                    // SAFETY: [slot, slot+v.len()) is exclusive to this thread.
                    // mmap pointer is valid for the scope duration.
                    unsafe {
                        std::ptr::write((mmap_ptr as *mut ElemIndex).add(slot + i), *e);
                    }
                }
                write_head    += v.len();
                next_expected += 1;
            }
        }

        // Flush and trim.
        let actual_bytes = write_head * std::mem::size_of::<ElemIndex>();
        idx_mmap.flush().context("flushing revolver mmap")?;
        drop(idx_mmap);
        std::fs::OpenOptions::new()
            .write(true)
            .open(idx_path)?
            .set_len(actual_bytes as u64)?;

        Ok(write_head as u64)
    })
}

// ── PipelinedReader ───────────────────────────────────────────────────────────

pub const SLOT_BYTES: usize = 100 * 1024 * 1024; // 100 MB per I/O slot
pub const RING_DEPTH: usize = 4;                  // reader prefetch depth
const MAX_ELEM_BYTES: usize = 512 * 1024;         // max OSM element size for carry-over

/// Position just past the last complete top-level OSM element in `bytes`.
/// Searches the last `MAX_ELEM_BYTES` to bound work. Returns 0 if none found.
pub fn find_safe_slot_end(bytes: &[u8]) -> usize {
    use memchr::{memrchr, memmem};
    let search_start = bytes.len().saturating_sub(MAX_ELEM_BYTES);
    let tail   = &bytes[search_start..];
    let offset = search_start;
    let mut best = 0usize;

    // Full closing tags.
    for closing in [
        b"</node>".as_slice(), b"</way>".as_slice(), b"</relation>".as_slice(),
        b"</changeset>".as_slice(),
    ] {
        if let Some(p) = memmem::rfind(tail, closing) {
            best = best.max(offset + p + closing.len());
        }
    }

    // Self-closing top-level elements: memmem rfind for "/>" then verify name.
    // Much faster than scanning every ">" — "/>" is rare relative to ">".
    let mut search_end = tail.len();
    loop {
        match memmem::rfind(&tail[..search_end], b"/>") {
            None => break,
            Some(p) => {
                if let Some(lt) = memrchr(b'<', &tail[..p]) {
                    let rest = &tail[lt + 1..];
                    if rest.starts_with(b"node ")       || rest.starts_with(b"node\t")
                    || rest.starts_with(b"way ")        || rest.starts_with(b"way\t")
                    || rest.starts_with(b"relation ")   || rest.starts_with(b"relation\t")
                    || rest.starts_with(b"changeset ")  || rest.starts_with(b"changeset\t")
                    || rest.starts_with(b"bound ")      || rest.starts_with(b"bound\t")
                    || rest.starts_with(b"note ")       || rest.starts_with(b"note\t")
                    {
                        best = best.max(offset + p + 2);
                        break;
                    }
                }
                if p == 0 { break; }
                search_end = p;
            }
        }
    }

    best
}

/// Planet-scale pipelined reader: one I/O thread reads 100 MB slots into a ring
/// buffer while all worker cores parse the current slot in parallel.
///
/// ```text
/// [Reader]   slot0  →  slot1  →  slot2  →  …   (bounded, RING_DEPTH capacity)
/// [Workers]            parse slot0  |  parse slot1  |  …
/// ```
///
/// Per slot: find N sub-chunk borders (N × ~50-byte memchr, nanoseconds), then
/// rayon-parallel parse of N sub-chunks. Slots are processed sequentially so
/// output is always in file order — no sorting, no BTreeMap.
///
/// At planet scale (100 GB XML, NVMe ~7 GB/s):
///   read time per slot ≈ 14 ms, parse time (12 cores) < 14 ms → I/O bound.
pub fn build_elem_index_pipelined(
    path:      &Path,
    n_workers: usize,
    idx_path:  &Path,
) -> Result<u64> {
    use std::io::{Read, Write};
    use std::sync::mpsc;
    use rayon::prelude::*;

    let n = n_workers.max(1);

    let out_file = OpenOptions::new()
        .create(true).write(true).truncate(true)
        .open(idx_path)
        .context("creating index file")?;
    let mut out   = std::io::BufWriter::with_capacity(8 * 1024 * 1024, out_file);
    let mut total = 0u64;

    // Bounded channel — reader stays at most RING_DEPTH slots ahead.
    // Message: (slot_id, absolute file offset of slot[0], slot bytes)
    let (tx, rx) = mpsc::sync_channel::<(usize, usize, Vec<u8>)>(RING_DEPTH);

    std::thread::scope(|s| -> Result<()> {
        // ── Reader thread ─────────────────────────────────────────────────────
        s.spawn(move || {
            let mut file     = File::open(path).expect("open input");
            let mut carry    = Vec::<u8>::new();
            let mut buf_start = 0usize; // absolute file offset of carry[0]
            let mut slot_id  = 0usize;

            loop {
                // Append up to SLOT_BYTES of new data — reserve without zeroing.
                let prev_len = carry.len();
                carry.reserve(SLOT_BYTES);
                let mut n_new = 0usize;
                while n_new < SLOT_BYTES {
                    let spare = carry.spare_capacity_mut();
                    let want  = (SLOT_BYTES - n_new).min(spare.len());
                    // SAFETY: read() fills the buffer; we only advance len by bytes actually read.
                    let dst = unsafe {
                        std::slice::from_raw_parts_mut(spare.as_mut_ptr() as *mut u8, want)
                    };
                    match file.read(dst) {
                        Ok(0)  => break,
                        Ok(k)  => {
                            unsafe { carry.set_len(prev_len + n_new + k); }
                            n_new += k;
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(e) => panic!("read: {e}"),
                    }
                }

                if n_new == 0 {
                    // EOF — flush remaining carry as final slot.
                    if !carry.is_empty() {
                        tx.send((slot_id, buf_start, carry)).ok();
                    }
                    break;
                }

                let safe = find_safe_slot_end(&carry);
                if safe == 0 { continue; } // huge element still accumulating

                // split_off copies only the small tail; parse_buf moves without copy.
                let new_carry  = carry.split_off(safe);
                let parse_buf  = carry;
                carry          = new_carry;
                let send_start = buf_start;
                buf_start     += safe;

                if tx.send((slot_id, send_start, parse_buf)).is_err() { break; }
                slot_id += 1;
            }
        });

        // ── Processor (scope thread) ──────────────────────────────────────────
        while let Ok((_slot_id, file_pos, slot)) = rx.recv() {
            // Find N sub-chunk borders inside this slot.
            let chunk_size = (slot.len() / n).max(1);
            let mut borders = Vec::with_capacity(n + 1);
            for i in 0..n { borders.push(find_top_level_start(&slot, i * chunk_size)); }
            borders.push(slot.len());

            // Parallel parse — each sub-chunk is independent.
            let sub_results: Vec<Vec<ElemIndex>> = (0..n)
                .into_par_iter()
                .map(|i| {
                    let start = borders[i];
                    let end   = borders[i + 1];
                    let mut v = Vec::new();
                    build_elem_index_slice(&slot[start..end], file_pos + start, &mut |e| v.push(e));
                    v
                })
                .collect();

            // Write in sub-chunk order (= file order). One write per sub-chunk.
            let esz = std::mem::size_of::<ElemIndex>();
            for sub in &sub_results {
                if sub.is_empty() { continue; }
                // SAFETY: ElemIndex is repr(C)+Copy; mmap cast on read uses same layout.
                let raw = unsafe {
                    std::slice::from_raw_parts(sub.as_ptr() as *const u8, sub.len() * esz)
                };
                out.write_all(raw)?;
                total += sub.len() as u64;
            }
        }

        out.flush()?;
        Ok(())
    })?;

    Ok(total)
}

// ── Pass 2: filter and seek ───────────────────────────────────────────────────

/// Filter criteria for pass 2.
#[derive(Debug, Default, Clone)]
pub struct Filter {
    /// If set, only yield elements of this kind.
    pub kind:      Option<ElemKind>,
    /// If set, only yield elements whose tag_flags intersect this mask.
    pub tag_mask:  u32,
    /// If set, only yield nodes within this lat/lon box (e7 units).
    pub bbox:      Option<[i32; 4]>, // [min_lat, min_lon, max_lat, max_lon]
}

impl Filter {
    pub fn ways() -> Self { Self { kind: Some(ElemKind::Way), ..Default::default() } }
    pub fn relations() -> Self { Self { kind: Some(ElemKind::Relation), ..Default::default() } }
    pub fn nodes() -> Self { Self { kind: Some(ElemKind::Node), ..Default::default() } }

    fn matches(&self, e: &ElemIndex) -> bool {
        if let Some(k) = self.kind { if e.kind != k { return false; } }
        if self.tag_mask != 0 && (e.tag_flags & self.tag_mask) == 0 { return false; }
        if let Some([min_lat, min_lon, max_lat, max_lon]) = self.bbox {
            if e.kind == ElemKind::Node {
                if e.lat_e7 < min_lat || e.lat_e7 > max_lat
                || e.lon_e7 < min_lon || e.lon_e7 > max_lon {
                    return false;
                }
            }
        }
        true
    }
}

/// Iterate over a pre-built ElemIndex slice, yield byte slices for matching elements.
/// `file_bytes` is the mmap of the original XML file.
/// `index` is sorted by file_offset (forward scan order).
pub fn iter_filtered<'a>(
    file_bytes: &'a [u8],
    index:      &'a [ElemIndex],
    filter:     &'a Filter,
) -> impl Iterator<Item = (ElemIndex, &'a [u8])> + 'a {
    index.iter().filter_map(move |e| {
        if !filter.matches(e) { return None; }
        let start = e.file_offset as usize;
        let end   = start + e.file_length as usize;
        file_bytes.get(start..end).map(|slice| (*e, slice))
    })
}

// ── ChunkSummary: zone-map index over ElemIndex ───────────────────────────────

/// Number of ElemIndex entries per ChunkSummary. Power of two keeps division cheap.
pub const CHUNK_SIZE: usize = 1_024;

/// Kind presence bits — one bit per ElemKind variant.
pub mod kind_bits {
    pub const NODE:     u8 = 1 << 0; // ElemKind::Node
    pub const WAY:      u8 = 1 << 1; // ElemKind::Way
    pub const RELATION: u8 = 1 << 2; // ElemKind::Relation
}

#[inline]
fn kind_bit(k: ElemKind) -> u8 { 1 << (k as u8) }

/// Zone-map summary over CHUNK_SIZE ElemIndex entries. 32 bytes, cache-line friendly.
///
/// One sequential scan of the summary array skips whole chunks:
///   - kind_mask: skip if no matching kind present (e.g. skip 88 % of planet for way-only)
///   - tag_flags: skip if no matching tag present (e.g. skip 99 % of ways for highway-only)
///   - lat/lon bbox: skip node-only queries outside the chunk's spatial extent
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct ChunkSummary {
    pub index_start: u32,
    pub index_end:   u32,
    pub kind_mask:   u8,
    pub _pad:        [u8; 3],
    pub tag_flags:   u32,
    pub lat_min:     i32,  // i32::MAX if chunk has no nodes
    pub lat_max:     i32,  // i32::MIN if chunk has no nodes
    pub lon_min:     i32,
    pub lon_max:     i32,
}

impl ChunkSummary {
    /// Returns false if the entire chunk can be skipped for `filter`.
    pub fn might_match(&self, filter: &Filter) -> bool {
        if let Some(k) = filter.kind {
            if self.kind_mask & kind_bit(k) == 0 { return false; }
        }
        if filter.tag_mask != 0 && (self.tag_flags & filter.tag_mask) == 0 { return false; }
        // Bbox skip is only safe when querying nodes specifically — ways/relations
        // have no coordinates in ElemIndex so their chunks can't be pruned by bbox.
        if filter.kind == Some(ElemKind::Node) {
            if let Some([min_lat, min_lon, max_lat, max_lon]) = filter.bbox {
                // lat_max = i32::MIN (sentinel) means no nodes → correctly fails any real bbox
                if self.lat_max < min_lat || self.lat_min > max_lat
                || self.lon_max < min_lon || self.lon_min > max_lon {
                    return false;
                }
            }
        }
        true
    }
}

/// Build a ChunkSummary array from a complete ElemIndex slice.
/// O(N) single pass, no allocation beyond the output Vec.
pub fn build_chunk_summaries(index: &[ElemIndex]) -> Vec<ChunkSummary> {
    index.chunks(CHUNK_SIZE).enumerate().map(|(ci, chunk)| {
        let mut kind_mask: u8  = 0;
        let mut tag_flags: u32 = 0;
        let mut lat_min = i32::MAX;
        let mut lat_max = i32::MIN;
        let mut lon_min = i32::MAX;
        let mut lon_max = i32::MIN;

        for e in chunk {
            kind_mask |= kind_bit(e.kind);
            tag_flags |= e.tag_flags;
            if e.kind == ElemKind::Node {
                lat_min = lat_min.min(e.lat_e7);
                lat_max = lat_max.max(e.lat_e7);
                lon_min = lon_min.min(e.lon_e7);
                lon_max = lon_max.max(e.lon_e7);
            }
        }

        ChunkSummary {
            index_start: (ci * CHUNK_SIZE) as u32,
            index_end:   (ci * CHUNK_SIZE + chunk.len()) as u32,
            kind_mask,
            _pad: [0; 3],
            tag_flags,
            lat_min,
            lat_max,
            lon_min,
            lon_max,
        }
    }).collect()
}

/// Write a ChunkSummary array to a flat binary file (same pattern as ElemIndex).
pub fn write_chunk_summaries(summaries: &[ChunkSummary], path: &Path) -> Result<()> {
    use std::io::Write as _;
    let file = OpenOptions::new()
        .create(true).write(true).truncate(true)
        .open(path)
        .with_context(|| format!("cannot create {}", path.display()))?;
    let mut w = std::io::BufWriter::with_capacity(4 * 1024 * 1024, file);
    let esz = std::mem::size_of::<ChunkSummary>();
    // SAFETY: ChunkSummary is repr(C)+Copy, no padding bytes that matter for I/O.
    let raw = unsafe {
        std::slice::from_raw_parts(summaries.as_ptr() as *const u8, summaries.len() * esz)
    };
    w.write_all(raw).context("writing chunk summaries")?;
    Ok(())
}

/// Cast a mmap written by `write_chunk_summaries` back to a typed slice.
pub fn as_chunk_summaries(mmap: &Mmap) -> &[ChunkSummary] {
    let bytes = mmap.as_ref();
    let esz = std::mem::size_of::<ChunkSummary>();
    assert_eq!(bytes.len() % esz, 0, "mmap length not aligned to ChunkSummary");
    // SAFETY: same alignment/origin guarantees as as_elem_index.
    unsafe {
        std::slice::from_raw_parts(bytes.as_ptr() as *const ChunkSummary, bytes.len() / esz)
    }
}

/// Like `iter_filtered` but skips entire CHUNK_SIZE blocks using zone-map statistics.
/// Drop-in replacement — yields identical results, faster on kind/tag/bbox filters.
pub fn iter_filtered_chunked<'a>(
    file_bytes: &'a [u8],
    index:      &'a [ElemIndex],
    summaries:  &'a [ChunkSummary],
    filter:     &'a Filter,
) -> impl Iterator<Item = (ElemIndex, &'a [u8])> + 'a {
    summaries
        .iter()
        .filter(move |s| s.might_match(filter))
        .flat_map(move |s| {
            iter_filtered(
                file_bytes,
                &index[s.index_start as usize..s.index_end as usize],
                filter,
            )
        })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // Fixture lives in the sibling katana-osm repo (not vendored into znippy).
    const LIECHTENSTEIN_OSM: &str =
        concat!(env!("CARGO_MANIFEST_DIR"), "/../../katana-osm/liechtenstein.osm");

    fn load_index() -> (Vec<u8>, Vec<ElemIndex>) {
        let bytes = std::fs::read(LIECHTENSTEIN_OSM)
            .expect("liechtenstein.osm not found — place it in the workspace root");
        let mut index = Vec::new();
        build_elem_index(&bytes, |e| index.push(e))
            .expect("build_elem_index failed");
        (bytes, index)
    }

    #[test]
    fn element_counts() {
        let (_, index) = load_index();
        let nodes = index.iter().filter(|e| e.kind == ElemKind::Node).count();
        let ways  = index.iter().filter(|e| e.kind == ElemKind::Way).count();
        let rels  = index.iter().filter(|e| e.kind == ElemKind::Relation).count();
        // grep -c '<node ' / '<way ' / '<relation ' gives ground truth
        assert_eq!(nodes, 353_167, "node count mismatch");
        assert_eq!(ways,   41_702, "way count mismatch");
        assert_eq!(rels,      938, "relation count mismatch");
    }

    #[test]
    fn first_node_coords() {
        let (_, index) = load_index();
        let first = index.iter().find(|e| e.kind == ElemKind::Node).unwrap();
        // <node id="26032978" lat="47.0472554" lon="9.4797436"/>
        assert_eq!(first.id, 26_032_978);
        assert_eq!(first.lat_e7, 470_472_554);
        assert_eq!(first.lon_e7,  94_797_436);
    }

    #[test]
    fn natural_tag_flag() {
        let (_, index) = load_index();
        // node 26863444 has natural=peak
        let e = index.iter().find(|e| e.id == 26_863_444).unwrap();
        assert_ne!(e.tag_flags & tag_flags::NATURAL, 0, "NATURAL flag not set");
    }

    #[test]
    fn highway_tag_flag() {
        let (_, index) = load_index();
        // node 30603864 has highway=crossing
        let e = index.iter().find(|e| e.id == 30_603_864).unwrap();
        assert_ne!(e.tag_flags & tag_flags::HIGHWAY, 0, "HIGHWAY flag not set");
    }

    #[test]
    fn byte_slice_roundtrip() {
        let (bytes, index) = load_index();
        // Every entry's byte slice must start with '<' and end with '>'
        for e in &index {
            let start = e.file_offset as usize;
            let end   = start + e.file_length as usize;
            let slice = &bytes[start..end];
            assert_eq!(slice.first(), Some(&b'<'), "id={} bad start", e.id);
            assert_eq!(slice.last(),  Some(&b'>'), "id={} bad end",   e.id);
        }
    }

    #[test]
    fn filter_ways_only() {
        let (bytes, index) = load_index();
        let filter = Filter::ways();
        let count = iter_filtered(&bytes, &index, &filter).count();
        assert_eq!(count, 41_702);
    }

    #[test]
    fn parallel_matches_sequential() {
        let bytes = std::fs::read(LIECHTENSTEIN_OSM)
            .expect("liechtenstein.osm not found");

        let mut seq: Vec<ElemIndex> = Vec::new();
        build_elem_index(&bytes, |e| seq.push(e)).unwrap();

        // Test with 2, 4, and 8 workers to exercise different split points.
        for n in [2, 4, 8] {
            let mut par: Vec<ElemIndex> = Vec::new();
            build_elem_index_parallel(&bytes, n, |e| par.push(e)).unwrap();

            assert_eq!(par.len(), seq.len(), "n={n}: count mismatch");
            for (i, (p, s)) in par.iter().zip(&seq).enumerate() {
                assert_eq!(p.file_offset, s.file_offset, "n={n} entry {i}: file_offset");
                assert_eq!(p.file_length, s.file_length, "n={n} entry {i}: file_length");
                assert_eq!(p.id,          s.id,          "n={n} entry {i}: id");
                assert_eq!(p.kind,        s.kind,        "n={n} entry {i}: kind");
                assert_eq!(p.tag_flags,   s.tag_flags,   "n={n} entry {i}: tag_flags");
            }
        }
    }

    // (xml_to_pbf_smoke test removed in extraction — it depended on katana-osm's
    //  xml_to_pbf encoder, which is not part of this crate.)

    #[test]
    fn filter_tag_mask() {
        let (bytes, index) = load_index();
        let filter = Filter {
            kind:     Some(ElemKind::Way),
            tag_mask: tag_flags::HIGHWAY,
            bbox:     None,
        };
        let count = iter_filtered(&bytes, &index, &filter).count();
        assert!(count > 0, "expected some highway ways");
        // all results must have HIGHWAY flag
        for (e, _) in iter_filtered(&bytes, &index, &filter) {
            assert_ne!(e.tag_flags & tag_flags::HIGHWAY, 0);
        }
    }

    // ── ChunkSummary tests ────────────────────────────────────────────────────

    #[test]
    fn chunk_summaries_cover_all() {
        let (_, index) = load_index();
        let summaries = build_chunk_summaries(&index);
        let covered: usize = summaries.iter()
            .map(|s| (s.index_end - s.index_start) as usize)
            .sum();
        assert_eq!(covered, index.len(), "summaries must cover all index entries");
        // index ranges must be contiguous and non-overlapping
        for (i, s) in summaries.iter().enumerate() {
            assert_eq!(s.index_start as usize, i * CHUNK_SIZE);
            assert!(s.index_end > s.index_start);
            assert!(s.index_end as usize <= index.len());
        }
    }

    #[test]
    fn chunk_summaries_kind_mask_correct() {
        let (_, index) = load_index();
        let summaries = build_chunk_summaries(&index);
        for s in &summaries {
            let chunk = &index[s.index_start as usize..s.index_end as usize];
            let mut expected: u8 = 0;
            for e in chunk { expected |= 1 << (e.kind as u8); }
            assert_eq!(s.kind_mask, expected, "kind_mask mismatch for chunk starting at {}", s.index_start);
        }
    }

    #[test]
    fn chunk_summaries_tag_flags_correct() {
        let (_, index) = load_index();
        let summaries = build_chunk_summaries(&index);
        for s in &summaries {
            let chunk = &index[s.index_start as usize..s.index_end as usize];
            let expected: u32 = chunk.iter().fold(0, |acc, e| acc | e.tag_flags);
            assert_eq!(s.tag_flags, expected);
        }
    }

    #[test]
    fn chunk_summaries_bbox_covers_nodes() {
        let (_, index) = load_index();
        let summaries = build_chunk_summaries(&index);
        for s in &summaries {
            let chunk = &index[s.index_start as usize..s.index_end as usize];
            for e in chunk.iter().filter(|e| e.kind == ElemKind::Node) {
                assert!(e.lat_e7 >= s.lat_min && e.lat_e7 <= s.lat_max,
                    "lat {} outside [{}, {}]", e.lat_e7, s.lat_min, s.lat_max);
                assert!(e.lon_e7 >= s.lon_min && e.lon_e7 <= s.lon_max,
                    "lon {} outside [{}, {}]", e.lon_e7, s.lon_min, s.lon_max);
            }
        }
    }

    #[test]
    fn chunked_filter_matches_simple_filter() {
        let (bytes, index) = load_index();
        let summaries = build_chunk_summaries(&index);

        let filters = [
            Filter::nodes(),
            Filter::ways(),
            Filter::relations(),
            Filter { kind: Some(ElemKind::Way), tag_mask: tag_flags::HIGHWAY, bbox: None },
        ];

        for filter in &filters {
            let simple:  Vec<ElemIndex> = iter_filtered(&bytes, &index, filter)
                .map(|(e, _)| e).collect();
            let chunked: Vec<ElemIndex> = iter_filtered_chunked(&bytes, &index, &summaries, filter)
                .map(|(e, _)| e).collect();
            assert_eq!(simple.len(), chunked.len(),
                "count mismatch for filter {:?}", filter);
            for (s, c) in simple.iter().zip(&chunked) {
                assert_eq!(s.file_offset, c.file_offset);
            }
        }
    }

    #[test]
    fn chunked_filter_skips_node_chunks_for_ways() {
        let (_, index) = load_index();
        let summaries = build_chunk_summaries(&index);
        let filter = Filter::ways();
        let skipped = summaries.iter().filter(|s| !s.might_match(&filter)).count();
        let total   = summaries.len();
        // Liechtenstein is mostly nodes — at least half the chunks should be skipped
        assert!(skipped > total / 2,
            "expected >50% chunks skipped for way-only filter, got {skipped}/{total}");
    }

    #[test]
    fn write_and_load_chunk_summaries() {
        let (_, index) = load_index();
        let summaries = build_chunk_summaries(&index);
        let path = std::path::Path::new("/tmp/liechtenstein_test.chunks.idx");
        write_chunk_summaries(&summaries, path).expect("write");
        let mmap = mmap_input(path).expect("mmap");
        let loaded = as_chunk_summaries(&mmap);
        assert_eq!(loaded.len(), summaries.len());
        for (a, b) in summaries.iter().zip(loaded.iter()) {
            assert_eq!(a.index_start, b.index_start);
            assert_eq!(a.kind_mask,   b.kind_mask);
            assert_eq!(a.tag_flags,   b.tag_flags);
            assert_eq!(a.lat_min,     b.lat_min);
        }
    }
}
