//! S-tree static search index for sorted i64 keys.
//!
//! ORIGINAL AUTHOR: **Ragnar Groot Koerkamp** — `static-search-tree`
//!   <https://github.com/RagnarGrootKoerkamp/static-search-tree>
//! This module is a derivative work; credit for the algorithm belongs to Ragnar
//! Groot Koerkamp. Licensed MIT — Copyright (c) 2025 Ragnar Groot Koerkamp.
//! The upstream MIT notice is retained in this crate's LICENSE file.
//!
//! Changes from the original (adapted for Rust 2024 edition):
//!   - Key type: u32 → i64
//!   - Node size: N=16 u32 (64 B) → N=8 i64 (64 B, one cache line)
//!   - Branching factor: B=16 → B=8
//!   - SIMD: portable_simd (nightly) → std::arch AVX2 (stable)
//!     `_mm256_cmpgt_epi64` compares 4 × i64 per 256-bit register;
//!     two calls cover all 8 elements of a node.
//!   - Return type: predecessor value → index in original array
//!     (what NodeStore actually needs).
//!
//! Build: `STree64::new(sorted_ids)` — O(n).
//! Query: `find_exact(id)` → `Option<usize>` — O(log₉ n), ≈7 cache misses for 9B nodes.
//!
//! `STree64Mmap` — same tree but WITHOUT the leaf layer.
//! The sorted mmap (16-byte records: [i64 id][f32 lat][f32 lon]) IS the leaf layer.
//! Internal nodes only: ~94 MB for Europe (94M nodes), ~9 GB for planet (9B nodes).
//! Build reads every 8th ID from the mmap (stride scan, no full copy in RAM).
//! Lookup: navigate internal nodes → final linear scan of ≤9 mmap records (144 bytes).

const B: usize = 8;            // elements per node = branching factor
const MAX64: i64 = i64::MAX;   // sentinel for unused slots

// ── Public type ────────────────────────────────────────────────────────────────

/// Immutable S-tree over a sorted `&[i64]` slice.
/// Stores a complete copy of the data internally.
pub struct STree64 {
    tree:    Vec<[i64; B]>,
    offsets: Vec<usize>,
    n:       usize,
}

impl STree64 {
    /// Build from a **sorted** slice. Panics in debug if not sorted.
    pub fn new(vals: &[i64]) -> Self {
        assert!(!vals.is_empty(), "STree64::new: empty input");
        #[cfg(debug_assertions)]
        for w in vals.windows(2) {
            assert!(w[0] <= w[1], "STree64::new: input not sorted");
        }

        let n      = vals.len();
        let height = height(n);
        let lsizes = layer_sizes(n, height);
        let n_blocks: usize = lsizes.iter().sum();

        let mut offsets = Vec::with_capacity(height);
        let mut acc = 0;
        for &ls in &lsizes {
            offsets.push(acc);
            acc += ls;
        }

        let mut tree = vec![[MAX64; B]; n_blocks];

        // ── Leaf layer ────────────────────────────────────────────────────────
        let ol = offsets[height - 1];
        for (i, &val) in vals.iter().enumerate() {
            tree[ol + i / B][i % B] = val;
        }
        if n % B != 0 {
            tree[ol + n / B][n % B..].fill(MAX64);
        }

        // ── Internal layers (root … leaf−1, built bottom-up) ──────────────────
        for h in (0..height - 1).rev() {
            let oh = offsets[h];
            tree[oh..oh + lsizes[h]].iter_mut().for_each(|nd| nd.fill(MAX64));

            for i in 0..B * lsizes[h] {
                let j = i % B;
                let mut k = (i / B) * (B + 1) + j + 1;
                for _ in h..height - 2 {
                    k *= B + 1;
                }
                tree[oh + i / B][j] = if k * B < n {
                    tree[ol + k][0]
                } else {
                    MAX64
                };
            }
        }

        Self { tree, offsets, n }
    }

    /// Find the index of `q` in the original sorted slice, or `None` if absent.
    ///
    /// Internal routing uses `count_lt` (same as the original library).  When all
    /// elements in a leaf block are < q, `idx` equals B and the search overflows
    /// into the next leaf block via `idx / B` — the same mechanism the original
    /// S-tree uses with its `self.get(o + k + idx/N, idx%N)` expression.
    #[inline]
    pub fn find_exact(&self, q: i64) -> Option<usize> {
        let height = self.offsets.len();
        let mut k  = 0usize;

        for h in 0..height - 1 {
            let o    = self.offsets[h];
            let jump = count_lt(&self.tree[o + k], q);
            k = k * (B + 1) + jump;
        }

        let o     = self.offsets[height - 1];
        let idx   = count_lt(&self.tree[o + k], q);

        // idx can be 0..=B.  When idx == B all leaf elements were < q,
        // so the target is at the start of the NEXT leaf block (overflow by 1).
        let block = k + idx / B;
        let slot  = idx % B;
        let pos   = block * B + slot;

        // Guard both self.n (logical) and tree (physical) bounds.
        if pos < self.n && self.tree[o + block][slot] == q {
            Some(pos)
        } else {
            None
        }
    }
}

// ── STree64Mmap — internal-nodes-only tree, mmap is the leaf layer ────────────
//
// Mmap record layout: [i64 le id (8 B)][f32 le lat (4 B)][f32 le lon (4 B)] = 16 B.
// Build reads the first ID of each B-record leaf block directly from the mmap
// (stride scan at 128-byte intervals — cheap and cache-friendly).
// find_exact navigates internal nodes then does a ≤9-record linear scan in the mmap.

const MMAP_RECORD: usize = 16; // bytes per record in the sorted mmap

pub struct STree64Mmap {
    tree:    Vec<[i64; B]>,
    offsets: Vec<usize>,
    pub count: usize,
}

impl STree64Mmap {
    pub fn new(mmap: &[u8], count: usize) -> Self {
        assert!(count > 0);
        let h      = height(count);
        let lsizes = layer_sizes(count, h);
        let ni     = h - 1; // number of internal layers (all except leaf)

        let n_blocks: usize = lsizes[..ni].iter().sum();
        let mut offsets = Vec::with_capacity(ni);
        let mut acc = 0usize;
        for &ls in &lsizes[..ni] { offsets.push(acc); acc += ls; }

        let mut tree = vec![[MAX64; B]; n_blocks];

        for lvl in (0..ni).rev() {
            let oh = offsets[lvl];
            tree[oh..oh + lsizes[lvl]].iter_mut().for_each(|nd| nd.fill(MAX64));
            for i in 0..B * lsizes[lvl] {
                let j = i % B;
                let mut k = (i / B) * (B + 1) + j + 1;
                for _ in lvl..h - 2 { k *= B + 1; }
                // k is the leaf block index; read first ID of that block from mmap
                tree[oh + i / B][j] = if k * B < count {
                    mmap_id(mmap, k * B)
                } else {
                    MAX64
                };
            }
        }

        Self { tree, offsets, count }
    }

    #[inline]
    pub fn find_exact(&self, q: i64, mmap: &[u8]) -> Option<usize> {
        let mut k = 0usize;
        for &o in &self.offsets {
            let jump = count_lt(&self.tree[o + k], q);
            k = k * (B + 1) + jump;
        }
        // k = leaf block; scan up to B+1 records (handles overflow when all B < q)
        for i in 0..=B {
            let pos = k * B + i;
            if pos >= self.count { break; }
            match mmap_id(mmap, pos).cmp(&q) {
                std::cmp::Ordering::Equal   => return Some(pos),
                std::cmp::Ordering::Greater => break,
                std::cmp::Ordering::Less    => {}
            }
        }
        None
    }

    /// Tree-only routing: descend internal nodes to a leaf block index.
    /// Skips the mmap leaf scan — useful when the caller wants to sort all
    /// queries by leaf-block index before touching the mmap (to convert
    /// random page faults into a sequential read pattern).
    ///
    /// Returns the leaf block index `k` such that the candidate record range
    /// is `[k*B, k*B + B + 1)` (the trailing +1 handles overflow when every
    /// element in block `k` is `< q`).
    #[inline]
    pub fn route_to_block(&self, q: i64) -> usize {
        let mut k = 0usize;
        for &o in &self.offsets {
            let jump = count_lt(&self.tree[o + k], q);
            k = k * (B + 1) + jump;
        }
        k
    }

    /// Batch lookup: resolve `ids` against the on-disk sorted record store.
    /// Returns `out[i]` = position of `ids[i]` in the mmap, or `None` if absent.
    ///
    /// Internally:
    ///   1. Tree-only routing for every id (in-RAM, ~10 cache-line reads each)
    ///   2. Sort `(leaf_block, original_index)` pairs — sequential mmap walk
    ///   3. `madvise(WILLNEED)` on the distinct page range so the kernel can
    ///      issue parallel readahead for the upcoming leaf scans
    ///   4. Linear leaf scan in sorted order; scatter results back
    ///
    /// For a slot with millions of node-refs spread across a 144 GB coord
    /// mmap, this converts millions of random page faults into a near-
    /// sequential read pattern + parallel readahead. Expected speedup on
    /// cold pages: ~4-8× vs N serial `find_exact` calls.
    pub fn lookup_batch(&self, ids: &[i64], mmap: &[u8]) -> Vec<Option<usize>> {
        let n = ids.len();
        if n == 0 { return Vec::new(); }

        // 1. Route every id through the tree → (leaf_block, original_index).
        let mut routed: Vec<(usize, u32)> = (0..n)
            .map(|i| (self.route_to_block(ids[i]), i as u32))
            .collect();

        // 2. Sort by leaf block so the mmap walk is sequential.
        routed.sort_unstable_by_key(|&(k, _)| k);

        // 3. Best-effort readahead hint over the touched range.
        //    On Linux this is a non-blocking call to advise the page cache;
        //    failures are silently ignored (this is purely a performance hint).
        if let (Some(&(lo_k, _)), Some(&(hi_k, _))) = (routed.first(), routed.last()) {
            let lo_byte = lo_k * B * MMAP_RECORD;
            let hi_byte = ((hi_k + 1) * B + 1).min(self.count) * MMAP_RECORD;
            if hi_byte > lo_byte {
                advise_willneed(&mmap[lo_byte..hi_byte]);
            }
        }

        // 4. Scan leaves in sorted order, scatter into `out` by original index.
        let mut out = vec![None; n];
        for (k, orig) in routed {
            let q = ids[orig as usize];
            for i in 0..=B {
                let pos = k * B + i;
                if pos >= self.count { break; }
                match mmap_id(mmap, pos).cmp(&q) {
                    std::cmp::Ordering::Equal   => { out[orig as usize] = Some(pos); break; }
                    std::cmp::Ordering::Greater => break,
                    std::cmp::Ordering::Less    => {}
                }
            }
        }
        out
    }
}

/// Best-effort `madvise(MADV_WILLNEED)` over a mmap slice. Linux-only; on
/// other platforms this is a no-op. Errors are silently ignored — the
/// kernel may reject the hint (e.g. unaligned, beyond mapping) but the
/// subsequent reads still succeed via normal page faults.
#[inline]
fn advise_willneed(slice: &[u8]) {
    #[cfg(target_os = "linux")]
    unsafe {
        let _ = libc::madvise(
            slice.as_ptr() as *mut libc::c_void,
            slice.len(),
            libc::MADV_WILLNEED,
        );
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = slice;
    }
}

#[inline]
fn mmap_id(mmap: &[u8], idx: usize) -> i64 {
    i64::from_le_bytes(mmap[idx * MMAP_RECORD..idx * MMAP_RECORD + 8].try_into().unwrap())
}

// ── Tree shape helpers ─────────────────────────────────────────────────────────

fn blocks(n: usize) -> usize { n.div_ceil(B) }

fn prev_keys(n: usize) -> usize { blocks(n).div_ceil(B + 1) * B }

fn height(n: usize) -> usize {
    if n <= B { 1 } else { height(prev_keys(n)) + 1 }
}

fn layer_size(mut n: usize, h: usize, height: usize) -> usize {
    for _ in h..height - 1 { n = prev_keys(n); }
    n
}

fn layer_sizes(n: usize, height: usize) -> Vec<usize> {
    (0..height)
        .map(|h| layer_size(n, h, height).div_ceil(B))
        .collect()
}

// ── SIMD node comparison ───────────────────────────────────────────────────────

/// Count elements in `node` that are strictly less than `q`.
/// Equivalently: the index of the first element >= q (predecessor step).
#[inline]
fn count_lt(node: &[i64; B], q: i64) -> usize {
    #[cfg(target_arch = "x86_64")]
    if is_x86_feature_detected!("avx2") {
        return unsafe { count_lt_avx2(node, q) };
    }
    node.iter().filter(|&&x| x < q).count()
}

/// AVX2 path: two 256-bit `_mm256_cmpgt_epi64` calls cover all 8 i64 in one cache line.
/// Each i64 comparison produces 8 identical result bytes → popcount / 8 = count of trues.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn count_lt_avx2(node: &[i64; B], q: i64) -> usize {
    use std::arch::x86_64::*;
    // Rust 2024: explicit unsafe block required even inside unsafe fn.
    unsafe {
        let q_v  = _mm256_set1_epi64x(q);
        let lo   = _mm256_loadu_si256(node.as_ptr()        as *const __m256i);
        let hi   = _mm256_loadu_si256(node.as_ptr().add(4) as *const __m256i);
        let c_lo = _mm256_cmpgt_epi64(q_v, lo);
        let c_hi = _mm256_cmpgt_epi64(q_v, hi);
        let m_lo = _mm256_movemask_epi8(c_lo) as u32;
        let m_hi = _mm256_movemask_epi8(c_hi) as u32;
        ((m_lo.count_ones() + m_hi.count_ones()) / 8) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_small() {
        let vals: Vec<i64> = vec![1, 3, 5, 7, 9, 11, 13, 15, 100, 200];
        let tree = STree64::new(&vals);
        for (i, &v) in vals.iter().enumerate() {
            assert_eq!(tree.find_exact(v), Some(i), "failed for val {v}");
        }
        assert_eq!(tree.find_exact(2), None);
        assert_eq!(tree.find_exact(0), None);
        assert_eq!(tree.find_exact(201), None);
    }

    #[test]
    fn round_trip_large() {
        let vals: Vec<i64> = (0..100_000).map(|i| i * 3).collect();
        let tree = STree64::new(&vals);
        for (i, &v) in vals.iter().enumerate() {
            assert_eq!(tree.find_exact(v), Some(i));
        }
        assert_eq!(tree.find_exact(1), None);
        assert_eq!(tree.find_exact(299_999), None);
    }

    #[test]
    fn planet_scale_ids() {
        // OSM planet node IDs: sparse, up to ~12B
        let vals: Vec<i64> = (0..1000).map(|i| i as i64 * 12_000_000).collect();
        let tree = STree64::new(&vals);
        for (i, &v) in vals.iter().enumerate() {
            assert_eq!(tree.find_exact(v), Some(i));
        }
    }
}
