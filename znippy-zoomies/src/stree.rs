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

    /// Pipelined batched lookup using software-pipelined internal traversal.
    /// Const generic P sets the number of in-flight queries to overlap memory
    /// latency. This is Ragnar's key trick to increase memory-level parallelism.
    pub fn lookup_batch_pipeline<const P: usize>(&self, ids: &[i64], mmap: &[u8]) -> Vec<Option<usize>> {
        let n = ids.len();
        if n == 0 { return Vec::new(); }

        // 1. Route queries in batches of P through internal nodes with prefetch.
        let mut routed: Vec<(usize, u32)> = Vec::with_capacity(n);
        let mut i = 0usize;
        let offs = &self.offsets;
        let n_levels = offs.len();
        while i + P <= n {
            let mut q = [0i64; P];
            for j in 0..P { q[j] = ids[i + j]; }

            let mut kk = [0usize; P];
            // Traverse all internal layers except the last with prefetch
            for h in 0..n_levels.saturating_sub(1) {
                let o = offs[h];
                let o2 = offs[h + 1];
                for j in 0..P {
                    let jump = count_lt(&self.tree[o + kk[j]], q[j]);
                    kk[j] = kk[j] * (B + 1) + jump;
                    prefetch_index(&self.tree, o2 + kk[j]);
                }
            }
            // Final internal level (no next tree level to prefetch)
            if n_levels > 0 {
                let o = offs[n_levels - 1];
                for j in 0..P {
                    let jump = count_lt(&self.tree[o + kk[j]], q[j]);
                    kk[j] = kk[j] * (B + 1) + jump;
                }
            }
            for j in 0..P {
                routed.push((kk[j], (i + j) as u32));
            }
            i += P;
        }
        // Remainder
        while i < n {
            routed.push((self.route_to_block(ids[i]), i as u32));
            i += 1;
        }

        // 2. Sort & madvise as before
        routed.sort_unstable_by_key(|&(k, _)| k);
        if let (Some(&(lo_k, _)), Some(&(hi_k, _))) = (routed.first(), routed.last()) {
            let lo_byte = lo_k * B * MMAP_RECORD;
            let hi_byte = ((hi_k + 1) * B + 1).min(self.count) * MMAP_RECORD;
            if hi_byte > lo_byte {
                advise_willneed(&mmap[lo_byte..hi_byte]);
            }
        }

        // 3. linear leaf scan, scatter results
        let mut out = vec![None; n];
        for (k, orig) in routed {
            let q = ids[orig as usize];
            for t in 0..=B {
                let pos = k * B + t;
                if pos >= self.count { break; }
                match mmap_id(mmap, pos).cmp(&q) {
                    std::cmp::Ordering::Equal => { out[orig as usize] = Some(pos); break; }
                    std::cmp::Ordering::Greater => break,
                    _ => {}
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

/// Prefetch the given cacheline into L1 cache (pointer form).
#[inline]
fn prefetch_ptr<T>(ptr: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(ptr as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(target_arch = "x86")]
    unsafe {
        std::arch::x86::_mm_prefetch(ptr as *const i8, std::arch::x86::_MM_HINT_T0);
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "x86")))]
    {
        let _ = ptr;
    }
}

/// Prefetch the given cacheline by indexing into a slice.
#[inline]
fn prefetch_index<T>(s: &[T], index: usize) {
    let ptr = unsafe { s.as_ptr().add(index) } as *const T;
    prefetch_ptr(ptr);
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

// ── STree64 batch methods (Ragnar-style pipelined) ─────────────────────────────

impl STree64 {
    /// Pipelined batch search — process P queries lock-step through the tree,
    /// issuing prefetch for the next level's node while other queries overlap
    /// their cache-miss latency. This is the core of Ragnar's throughput trick.
    ///
    /// Returns `[Option<usize>; P]` — index in the original sorted slice or None.
    pub fn batch_prefetch<const P: usize>(&self, queries: &[i64; P]) -> [Option<usize>; P] {
        let height = self.offsets.len();
        let mut k = [0usize; P];

        // Internal levels with prefetch
        for h in 0..height - 1 {
            let o = self.offsets[h];
            let o2 = self.offsets[h + 1];
            for j in 0..P {
                let jump = count_lt(&self.tree[o + k[j]], queries[j]);
                k[j] = k[j] * (B + 1) + jump;
                // Prefetch next-level node for query j
                prefetch_index(&self.tree, o2 + k[j]);
            }
        }

        // Leaf level — no prefetch needed, just resolve
        let o = self.offsets[height - 1];
        let mut results = [None; P];
        for j in 0..P {
            let idx = count_lt(&self.tree[o + k[j]], queries[j]);
            let block = k[j] + idx / B;
            let slot = idx % B;
            let pos = block * B + slot;
            if pos < self.n && self.tree[o + block][slot] == queries[j] {
                results[j] = Some(pos);
            }
        }
        results
    }

    /// Batch search without prefetch (baseline for comparison).
    pub fn batch_no_prefetch<const P: usize>(&self, queries: &[i64; P]) -> [Option<usize>; P] {
        let height = self.offsets.len();
        let mut k = [0usize; P];

        for h in 0..height - 1 {
            let o = self.offsets[h];
            for j in 0..P {
                let jump = count_lt(&self.tree[o + k[j]], queries[j]);
                k[j] = k[j] * (B + 1) + jump;
            }
        }

        let o = self.offsets[height - 1];
        let mut results = [None; P];
        for j in 0..P {
            let idx = count_lt(&self.tree[o + k[j]], queries[j]);
            let block = k[j] + idx / B;
            let slot = idx % B;
            let pos = block * B + slot;
            if pos < self.n && self.tree[o + block][slot] == queries[j] {
                results[j] = Some(pos);
            }
        }
        results
    }

    /// Pointer-based pipelined batch (Ragnar's `batch_ptr` style).
    /// Uses raw pointers and byte offsets for the tightest inner loop.
    /// Safety: tree must be non-empty and offsets correct (guaranteed by constructor).
    pub fn batch_ptr<const P: usize>(&self, queries: &[i64; P]) -> [Option<usize>; P] {
        let height = self.offsets.len();
        let mut k = [0usize; P];

        let offsets: Vec<*const [i64; B]> = self.offsets.iter()
            .map(|&o| unsafe { self.tree.as_ptr().add(o) })
            .collect();

        // Internal levels with pointer-based prefetch
        for h in 0..height - 1 {
            let o = offsets[h];
            let o2 = offsets[h + 1];
            for j in 0..P {
                let node = unsafe { &*o.add(k[j]) };
                let jump = count_lt(node, queries[j]);
                k[j] = k[j] * (B + 1) + jump;
                prefetch_ptr(unsafe { o2.add(k[j]) });
            }
        }

        // Leaf level
        let o = offsets[height - 1];
        let mut results = [None; P];
        for j in 0..P {
            let node = unsafe { &*o.add(k[j]) };
            let idx = count_lt(node, queries[j]);
            let block = k[j] + idx / B;
            let slot = idx % B;
            let pos = block * B + slot;
            if pos < self.n {
                let leaf_node = unsafe { &*o.add(block) };
                if leaf_node[slot] == queries[j] {
                    results[j] = Some(pos);
                }
            }
        }
        results
    }

    /// Streaming batch: process a slice of arbitrary length, chunked into
    /// batches of P internally with prefetch. Returns Vec<Option<usize>>.
    pub fn batch_stream<const P: usize>(&self, queries: &[i64]) -> Vec<Option<usize>> {
        let n = queries.len();
        let mut out = Vec::with_capacity(n);
        let mut i = 0;

        while i + P <= n {
            let chunk: &[i64; P] = queries[i..i + P].try_into().unwrap();
            let results = self.batch_prefetch(chunk);
            out.extend_from_slice(&results);
            i += P;
        }

        // Remainder: single queries
        for j in i..n {
            out.push(self.find_exact(queries[j]));
        }
        out
    }
}

// ── STree64Mmap pointer-based pipelined batch ──────────────────────────────────

impl STree64Mmap {
    /// Pointer-based pipelined batch traversal (Ragnar's `batch_ptr` for mmap tree).
    /// Uses raw pointer arithmetic for maximum throughput on the in-RAM tree,
    /// then sorts positions and does sequential mmap leaf scan.
    pub fn lookup_batch_ptr<const P: usize>(&self, ids: &[i64], mmap: &[u8]) -> Vec<Option<usize>> {
        let n = ids.len();
        if n == 0 { return Vec::new(); }

        let offsets: Vec<*const [i64; B]> = self.offsets.iter()
            .map(|&o| unsafe { self.tree.as_ptr().add(o) })
            .collect();
        let n_levels = offsets.len();

        // 1. Route queries in batches of P using pointer-based prefetch
        let mut routed: Vec<(usize, u32)> = Vec::with_capacity(n);
        let mut i = 0usize;
        while i + P <= n {
            let mut kk = [0usize; P];
            for h in 0..n_levels - 1 {
                let o = offsets[h];
                let o2 = offsets[h + 1];
                for j in 0..P {
                    let node = unsafe { &*o.add(kk[j]) };
                    let jump = count_lt(node, ids[i + j]);
                    kk[j] = kk[j] * (B + 1) + jump;
                    prefetch_ptr(unsafe { o2.add(kk[j]) });
                }
            }
            // Last internal level (no next-level prefetch, but prefetch mmap leaf)
            let o_last = offsets[n_levels - 1];
            for j in 0..P {
                let node = unsafe { &*o_last.add(kk[j]) };
                let jump = count_lt(node, ids[i + j]);
                kk[j] = kk[j] * (B + 1) + jump;
                routed.push((kk[j], (i + j) as u32));
            }
            i += P;
        }
        // Remainder
        while i < n {
            routed.push((self.route_to_block(ids[i]), i as u32));
            i += 1;
        }

        // 2. Sort + madvise
        routed.sort_unstable_by_key(|&(k, _)| k);
        if let (Some(&(lo_k, _)), Some(&(hi_k, _))) = (routed.first(), routed.last()) {
            let lo_byte = lo_k * B * MMAP_RECORD;
            let hi_byte = ((hi_k + 1) * B + 1).min(self.count) * MMAP_RECORD;
            if hi_byte > lo_byte {
                advise_willneed(&mmap[lo_byte..hi_byte]);
            }
        }

        // 3. Leaf scan
        let mut out = vec![None; n];
        for (k, orig) in routed {
            let q = ids[orig as usize];
            for t in 0..=B {
                let pos = k * B + t;
                if pos >= self.count { break; }
                match mmap_id(mmap, pos).cmp(&q) {
                    std::cmp::Ordering::Equal => { out[orig as usize] = Some(pos); break; }
                    std::cmp::Ordering::Greater => break,
                    _ => {}
                }
            }
        }
        out
    }

    /// Interleaved batch: process two groups of P queries with half-tree
    /// interleaving so that one group's cache misses hide behind the other's
    /// compute. Inspired by Ragnar's `batch_interleave_half`.
    ///
    /// This processes `ids` in chunks of 2*P, interleaving the first half of
    /// group B's traversal with the second half of group A's traversal.
    pub fn lookup_batch_interleave<const P: usize>(&self, ids: &[i64], mmap: &[u8]) -> Vec<Option<usize>> {
        let n = ids.len();
        if n == 0 { return Vec::new(); }
        let n_levels = self.offsets.len();
        if n_levels < 2 {
            // Tree too shallow for interleaving; fall back to pipeline
            return self.lookup_batch_pipeline::<P>(ids, mmap);
        }

        let half = n_levels / 2;

        let offsets: Vec<*const [i64; B]> = self.offsets.iter()
            .map(|&o| unsafe { self.tree.as_ptr().add(o) })
            .collect();

        let mut routed: Vec<(usize, u32)> = Vec::with_capacity(n);
        let mut i = 0usize;

        while i + 2 * P <= n {
            let mut k1 = [0usize; P];
            let mut k2 = [0usize; P];

            // First half of group 1
            for h in 0..half {
                let o = offsets[h];
                let o2 = offsets[h + 1];
                for j in 0..P {
                    let node = unsafe { &*o.add(k1[j]) };
                    let jump = count_lt(node, ids[i + j]);
                    k1[j] = k1[j] * (B + 1) + jump;
                    prefetch_ptr(unsafe { o2.add(k1[j]) });
                }
            }

            // Interleaved: second half of group 1 + first half of group 2
            let g1_remaining = n_levels - half;
            let interleave_iters = g1_remaining.max(half);
            for step in 0..interleave_iters {
                let g1_level = half + step;
                let g2_level = step;
                if g1_level < n_levels {
                    let o1 = offsets[g1_level];
                    let o12 = if g1_level + 1 < n_levels { offsets[g1_level + 1] } else { o1 };
                    for j in 0..P {
                        let node = unsafe { &*o1.add(k1[j]) };
                        let jump = count_lt(node, ids[i + j]);
                        k1[j] = k1[j] * (B + 1) + jump;
                        if g1_level + 1 < n_levels {
                            prefetch_ptr(unsafe { o12.add(k1[j]) });
                        }
                    }
                }
                if g2_level < half {
                    let o2 = offsets[g2_level];
                    let o22 = offsets[g2_level + 1];
                    for j in 0..P {
                        let node = unsafe { &*o2.add(k2[j]) };
                        let jump = count_lt(node, ids[i + P + j]);
                        k2[j] = k2[j] * (B + 1) + jump;
                        prefetch_ptr(unsafe { o22.add(k2[j]) });
                    }
                }
            }

            // Finish group 2's second half
            for h in half..n_levels {
                let o = offsets[h];
                let o2 = if h + 1 < n_levels { offsets[h + 1] } else { o };
                for j in 0..P {
                    let node = unsafe { &*o.add(k2[j]) };
                    let jump = count_lt(node, ids[i + P + j]);
                    k2[j] = k2[j] * (B + 1) + jump;
                    if h + 1 < n_levels {
                        prefetch_ptr(unsafe { o2.add(k2[j]) });
                    }
                }
            }

            for j in 0..P { routed.push((k1[j], (i + j) as u32)); }
            for j in 0..P { routed.push((k2[j], (i + P + j) as u32)); }
            i += 2 * P;
        }

        // Remainder: fall back to route_to_block
        while i < n {
            routed.push((self.route_to_block(ids[i]), i as u32));
            i += 1;
        }

        // Sort + madvise + leaf scan (same as other batch methods)
        routed.sort_unstable_by_key(|&(k, _)| k);
        if let (Some(&(lo_k, _)), Some(&(hi_k, _))) = (routed.first(), routed.last()) {
            let lo_byte = lo_k * B * MMAP_RECORD;
            let hi_byte = ((hi_k + 1) * B + 1).min(self.count) * MMAP_RECORD;
            if hi_byte > lo_byte {
                advise_willneed(&mmap[lo_byte..hi_byte]);
            }
        }

        let mut out = vec![None; n];
        for (k, orig) in routed {
            let q = ids[orig as usize];
            for t in 0..=B {
                let pos = k * B + t;
                if pos >= self.count { break; }
                match mmap_id(mmap, pos).cmp(&q) {
                    std::cmp::Ordering::Equal => { out[orig as usize] = Some(pos); break; }
                    std::cmp::Ordering::Greater => break,
                    _ => {}
                }
            }
        }
        out
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// TESTS — extensive correctness + micro-bench comparisons
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};
    use rand::rngs::StdRng;
    use std::time::Instant;

    // ── Helpers ────────────────────────────────────────────────────────────────

    fn build_mmap_from_ids(ids: &[i64]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(ids.len() * MMAP_RECORD);
        for (i, &id) in ids.iter().enumerate() {
            buf.extend_from_slice(&id.to_le_bytes());
            // Fake lat/lon using index for verifiability
            let lat = (i as f32) * 0.001;
            let lon = (i as f32) * -0.002;
            buf.extend_from_slice(&lat.to_le_bytes());
            buf.extend_from_slice(&lon.to_le_bytes());
        }
        buf
    }

    fn gen_sorted_sparse_ids(n: usize, seed: u64) -> Vec<i64> {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut ids = Vec::with_capacity(n);
        let mut cur: i64 = 1;
        for _ in 0..n {
            cur += rng.gen_range(1..20) as i64;
            ids.push(cur);
        }
        ids
    }

    fn gen_queries(ids: &[i64], n_hit: usize, n_miss: usize, seed: u64) -> Vec<i64> {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut qs = Vec::with_capacity(n_hit + n_miss);
        for _ in 0..n_hit {
            qs.push(ids[rng.gen_range(0..ids.len())]);
        }
        let max_id = ids.last().copied().unwrap_or(1000);
        for _ in 0..n_miss {
            qs.push(rng.gen_range(0..max_id * 2) * 3 + 2); // likely misses (odd stride)
        }
        qs
    }

    // ── STree64 basic tests ───────────────────────────────────────────────────

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
        let vals: Vec<i64> = (0..1000).map(|i| i as i64 * 12_000_000).collect();
        let tree = STree64::new(&vals);
        for (i, &v) in vals.iter().enumerate() {
            assert_eq!(tree.find_exact(v), Some(i));
        }
    }

    #[test]
    fn single_element() {
        let tree = STree64::new(&[42]);
        assert_eq!(tree.find_exact(42), Some(0));
        assert_eq!(tree.find_exact(41), None);
        assert_eq!(tree.find_exact(43), None);
    }

    #[test]
    fn exactly_b_elements() {
        let vals: Vec<i64> = (1..=B as i64).collect();
        let tree = STree64::new(&vals);
        for (i, &v) in vals.iter().enumerate() {
            assert_eq!(tree.find_exact(v), Some(i));
        }
        assert_eq!(tree.find_exact(0), None);
        assert_eq!(tree.find_exact(B as i64 + 1), None);
    }

    #[test]
    fn b_plus_one_elements() {
        let vals: Vec<i64> = (1..=B as i64 + 1).collect();
        let tree = STree64::new(&vals);
        for (i, &v) in vals.iter().enumerate() {
            assert_eq!(tree.find_exact(v), Some(i));
        }
    }

    #[test]
    fn negative_ids() {
        let vals: Vec<i64> = (-500..500).collect();
        let tree = STree64::new(&vals);
        for (i, &v) in vals.iter().enumerate() {
            assert_eq!(tree.find_exact(v), Some(i));
        }
        assert_eq!(tree.find_exact(-501), None);
        assert_eq!(tree.find_exact(500), None);
    }

    #[test]
    fn duplicates() {
        let vals: Vec<i64> = vec![1, 1, 2, 2, 3, 3, 4, 4, 5, 5];
        let tree = STree64::new(&vals);
        // find_exact should find at least one occurrence
        assert!(tree.find_exact(1).is_some());
        assert!(tree.find_exact(5).is_some());
        assert_eq!(tree.find_exact(6), None);
    }

    // ── STree64 batch tests ───────────────────────────────────────────────────

    #[test]
    fn batch_prefetch_correctness() {
        let vals = gen_sorted_sparse_ids(50_000, 100);
        let tree = STree64::new(&vals);
        let qs = gen_queries(&vals, 500, 500, 200);

        // Process in chunks of 16
        for chunk in qs.chunks(16) {
            if chunk.len() == 16 {
                let arr: [i64; 16] = chunk.try_into().unwrap();
                let batch_res = tree.batch_prefetch(&arr);
                let serial_res: Vec<_> = chunk.iter().map(|&q| tree.find_exact(q)).collect();
                for (j, (&batch, &serial)) in batch_res.iter().zip(serial_res.iter()).enumerate() {
                    assert_eq!(batch, serial, "batch_prefetch mismatch at idx {j}, query={}", chunk[j]);
                }
            }
        }
    }

    #[test]
    fn batch_no_prefetch_correctness() {
        let vals = gen_sorted_sparse_ids(50_000, 101);
        let tree = STree64::new(&vals);
        let qs = gen_queries(&vals, 500, 500, 201);

        for chunk in qs.chunks(16) {
            if chunk.len() == 16 {
                let arr: [i64; 16] = chunk.try_into().unwrap();
                let batch_res = tree.batch_no_prefetch(&arr);
                let serial_res: Vec<_> = chunk.iter().map(|&q| tree.find_exact(q)).collect();
                for (j, (&batch, &serial)) in batch_res.iter().zip(serial_res.iter()).enumerate() {
                    assert_eq!(batch, serial, "batch_no_prefetch mismatch at idx {j}");
                }
            }
        }
    }

    #[test]
    fn batch_ptr_correctness() {
        let vals = gen_sorted_sparse_ids(50_000, 102);
        let tree = STree64::new(&vals);
        let qs = gen_queries(&vals, 500, 500, 202);

        for chunk in qs.chunks(16) {
            if chunk.len() == 16 {
                let arr: [i64; 16] = chunk.try_into().unwrap();
                let batch_res = tree.batch_ptr(&arr);
                let serial_res: Vec<_> = chunk.iter().map(|&q| tree.find_exact(q)).collect();
                for (j, (&batch, &serial)) in batch_res.iter().zip(serial_res.iter()).enumerate() {
                    assert_eq!(batch, serial, "batch_ptr mismatch at idx {j}");
                }
            }
        }
    }

    #[test]
    fn batch_stream_correctness() {
        let vals = gen_sorted_sparse_ids(50_000, 103);
        let tree = STree64::new(&vals);
        let qs = gen_queries(&vals, 1000, 1000, 203);

        let stream_res = tree.batch_stream::<16>(&qs);
        let serial_res: Vec<_> = qs.iter().map(|&q| tree.find_exact(q)).collect();
        assert_eq!(stream_res, serial_res);

        // P=8
        let stream8 = tree.batch_stream::<8>(&qs);
        assert_eq!(stream8, serial_res);

        // P=32
        let stream32 = tree.batch_stream::<32>(&qs);
        assert_eq!(stream32, serial_res);
    }

    #[test]
    fn batch_stream_various_sizes() {
        let vals = gen_sorted_sparse_ids(10_000, 104);
        let tree = STree64::new(&vals);

        // test sizes that are NOT multiples of P
        for size in [0, 1, 7, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 1000] {
            let qs = gen_queries(&vals, size / 2 + 1, size / 2, 300 + size as u64);
            let qs = &qs[..size.min(qs.len())];
            let stream_res = tree.batch_stream::<16>(qs);
            let serial_res: Vec<_> = qs.iter().map(|&q| tree.find_exact(q)).collect();
            assert_eq!(stream_res, serial_res, "batch_stream mismatch for size={size}");
        }
    }

    // ── STree64Mmap basic tests ───────────────────────────────────────────────

    #[test]
    fn mmap_find_exact_basic() {
        let ids = gen_sorted_sparse_ids(10_000, 50);
        let mmap = build_mmap_from_ids(&ids);
        let tree = STree64Mmap::new(&mmap, ids.len());

        for (i, &id) in ids.iter().enumerate() {
            assert_eq!(tree.find_exact(id, &mmap), Some(i), "find_exact failed for id={id} at pos={i}");
        }
        // Missing ids
        assert_eq!(tree.find_exact(0, &mmap), None);
        assert_eq!(tree.find_exact(ids.last().unwrap() + 1, &mmap), None);
    }

    #[test]
    fn mmap_find_exact_single() {
        let ids = vec![999i64];
        let mmap = build_mmap_from_ids(&ids);
        let tree = STree64Mmap::new(&mmap, 1);
        assert_eq!(tree.find_exact(999, &mmap), Some(0));
        assert_eq!(tree.find_exact(998, &mmap), None);
    }

    // ── STree64Mmap batch correctness ─────────────────────────────────────────

    #[test]
    fn mmap_lookup_batch_vs_serial() {
        let ids = gen_sorted_sparse_ids(20_000, 60);
        let mmap = build_mmap_from_ids(&ids);
        let tree = STree64Mmap::new(&mmap, ids.len());
        let qs = gen_queries(&ids, 2_500, 2_500, 61);

        let batch = tree.lookup_batch(&qs, &mmap);
        let serial: Vec<_> = qs.iter().map(|&q| tree.find_exact(q, &mmap)).collect();
        assert_eq!(batch, serial);
    }

    #[test]
    fn mmap_pipeline_vs_serial_all_p() {
        let ids = gen_sorted_sparse_ids(20_000, 70);
        let mmap = build_mmap_from_ids(&ids);
        let tree = STree64Mmap::new(&mmap, ids.len());
        let qs = gen_queries(&ids, 2_500, 2_500, 71);

        let serial: Vec<_> = qs.iter().map(|&q| tree.find_exact(q, &mmap)).collect();

        let p4 = tree.lookup_batch_pipeline::<4>(&qs, &mmap);
        let p8 = tree.lookup_batch_pipeline::<8>(&qs, &mmap);
        let p16 = tree.lookup_batch_pipeline::<16>(&qs, &mmap);
        let p32 = tree.lookup_batch_pipeline::<32>(&qs, &mmap);

        assert_eq!(p4, serial, "pipeline P=4 mismatch");
        assert_eq!(p8, serial, "pipeline P=8 mismatch");
        assert_eq!(p16, serial, "pipeline P=16 mismatch");
        assert_eq!(p32, serial, "pipeline P=32 mismatch");
    }

    #[test]
    fn mmap_batch_ptr_vs_serial() {
        let ids = gen_sorted_sparse_ids(20_000, 80);
        let mmap = build_mmap_from_ids(&ids);
        let tree = STree64Mmap::new(&mmap, ids.len());
        let qs = gen_queries(&ids, 2_500, 2_500, 81);

        let serial: Vec<_> = qs.iter().map(|&q| tree.find_exact(q, &mmap)).collect();
        let ptr8 = tree.lookup_batch_ptr::<8>(&qs, &mmap);
        let ptr16 = tree.lookup_batch_ptr::<16>(&qs, &mmap);
        let ptr32 = tree.lookup_batch_ptr::<32>(&qs, &mmap);

        assert_eq!(ptr8, serial, "batch_ptr P=8 mismatch");
        assert_eq!(ptr16, serial, "batch_ptr P=16 mismatch");
        assert_eq!(ptr32, serial, "batch_ptr P=32 mismatch");
    }

    #[test]
    fn mmap_interleave_vs_serial() {
        let ids = gen_sorted_sparse_ids(20_000, 90);
        let mmap = build_mmap_from_ids(&ids);
        let tree = STree64Mmap::new(&mmap, ids.len());
        let qs = gen_queries(&ids, 2_500, 2_500, 91);

        let serial: Vec<_> = qs.iter().map(|&q| tree.find_exact(q, &mmap)).collect();
        let interleave8 = tree.lookup_batch_interleave::<8>(&qs, &mmap);
        let interleave16 = tree.lookup_batch_interleave::<16>(&qs, &mmap);

        assert_eq!(interleave8, serial, "interleave P=8 mismatch");
        assert_eq!(interleave16, serial, "interleave P=16 mismatch");
    }

    // ── Edge cases ────────────────────────────────────────────────────────────

    #[test]
    fn mmap_batch_empty_queries() {
        let ids = gen_sorted_sparse_ids(1000, 110);
        let mmap = build_mmap_from_ids(&ids);
        let tree = STree64Mmap::new(&mmap, ids.len());

        assert_eq!(tree.lookup_batch(&[], &mmap), Vec::<Option<usize>>::new());
        assert_eq!(tree.lookup_batch_pipeline::<16>(&[], &mmap), Vec::<Option<usize>>::new());
        assert_eq!(tree.lookup_batch_ptr::<16>(&[], &mmap), Vec::<Option<usize>>::new());
        assert_eq!(tree.lookup_batch_interleave::<8>(&[], &mmap), Vec::<Option<usize>>::new());
    }

    #[test]
    fn mmap_batch_single_query() {
        let ids = gen_sorted_sparse_ids(1000, 111);
        let mmap = build_mmap_from_ids(&ids);
        let tree = STree64Mmap::new(&mmap, ids.len());

        let hit = vec![ids[500]];
        let miss = vec![ids[500] + 1];

        assert_eq!(tree.lookup_batch_pipeline::<4>(&hit, &mmap), vec![Some(500)]);
        assert_eq!(tree.lookup_batch_pipeline::<4>(&miss, &mmap), vec![None]);
        assert_eq!(tree.lookup_batch_ptr::<4>(&hit, &mmap), vec![Some(500)]);
    }

    #[test]
    fn mmap_batch_all_hits() {
        let ids = gen_sorted_sparse_ids(5_000, 120);
        let mmap = build_mmap_from_ids(&ids);
        let tree = STree64Mmap::new(&mmap, ids.len());

        let qs: Vec<i64> = ids.iter().copied().collect();
        let expected: Vec<_> = (0..ids.len()).map(Some).collect();

        assert_eq!(tree.lookup_batch(&qs, &mmap), expected);
        assert_eq!(tree.lookup_batch_pipeline::<16>(&qs, &mmap), expected);
        assert_eq!(tree.lookup_batch_ptr::<16>(&qs, &mmap), expected);
    }

    #[test]
    fn mmap_batch_all_misses() {
        let ids = gen_sorted_sparse_ids(5_000, 130);
        let mmap = build_mmap_from_ids(&ids);
        let tree = STree64Mmap::new(&mmap, ids.len());

        // Queries guaranteed to miss (even ids when stored are odd-spaced)
        let qs: Vec<i64> = (0..1000).map(|i| ids.last().unwrap() + i + 1).collect();
        let expected: Vec<_> = vec![None; 1000];

        assert_eq!(tree.lookup_batch(&qs, &mmap), expected);
        assert_eq!(tree.lookup_batch_pipeline::<16>(&qs, &mmap), expected);
        assert_eq!(tree.lookup_batch_ptr::<16>(&qs, &mmap), expected);
    }

    #[test]
    fn mmap_batch_remainder_handling() {
        // Test with sizes that leave various remainders for P
        let ids = gen_sorted_sparse_ids(5_000, 140);
        let mmap = build_mmap_from_ids(&ids);
        let tree = STree64Mmap::new(&mmap, ids.len());

        for qlen in [1, 3, 7, 15, 16, 17, 31, 32, 33, 63, 100] {
            let qs = gen_queries(&ids, qlen, 0, 140 + qlen as u64);
            let qs = &qs[..qlen.min(qs.len())];
            let serial: Vec<_> = qs.iter().map(|&q| tree.find_exact(q, &mmap)).collect();

            let p16 = tree.lookup_batch_pipeline::<16>(qs, &mmap);
            let ptr16 = tree.lookup_batch_ptr::<16>(qs, &mmap);

            assert_eq!(p16, serial, "pipeline remainder mismatch for qlen={qlen}");
            assert_eq!(ptr16, serial, "batch_ptr remainder mismatch for qlen={qlen}");
        }
    }

    // ── Larger stress tests ───────────────────────────────────────────────────

    #[test]
    fn stress_100k_ids_10k_queries() {
        let ids = gen_sorted_sparse_ids(100_000, 200);
        let mmap = build_mmap_from_ids(&ids);
        let tree = STree64Mmap::new(&mmap, ids.len());
        let qs = gen_queries(&ids, 5_000, 5_000, 201);

        let serial: Vec<_> = qs.iter().map(|&q| tree.find_exact(q, &mmap)).collect();
        let batch = tree.lookup_batch(&qs, &mmap);
        let pipe16 = tree.lookup_batch_pipeline::<16>(&qs, &mmap);
        let ptr16 = tree.lookup_batch_ptr::<16>(&qs, &mmap);
        let interleave8 = tree.lookup_batch_interleave::<8>(&qs, &mmap);

        assert_eq!(batch, serial);
        assert_eq!(pipe16, serial);
        assert_eq!(ptr16, serial);
        assert_eq!(interleave8, serial);
    }

    #[test]
    fn stress_osm_like_ids() {
        // Simulate OSM-like node IDs: start at ~1B, gaps of 1-100
        let mut rng = StdRng::seed_from_u64(300);
        let n = 50_000;
        let mut ids = Vec::with_capacity(n);
        let mut cur: i64 = 1_000_000_000;
        for _ in 0..n {
            cur += rng.gen_range(1..100) as i64;
            ids.push(cur);
        }

        let mmap = build_mmap_from_ids(&ids);
        let tree = STree64Mmap::new(&mmap, ids.len());
        let qs = gen_queries(&ids, 5_000, 5_000, 301);

        let serial: Vec<_> = qs.iter().map(|&q| tree.find_exact(q, &mmap)).collect();
        assert_eq!(tree.lookup_batch_pipeline::<16>(&qs, &mmap), serial);
        assert_eq!(tree.lookup_batch_ptr::<16>(&qs, &mmap), serial);
        assert_eq!(tree.lookup_batch_interleave::<8>(&qs, &mmap), serial);
    }

    // ── Micro-benchmark (prints timing, not gated) ────────────────────────────

    #[test]
    #[ignore]
    fn bench_compare_all_methods() {
        // 500K records, 100K queries — run with `cargo test --release -- --ignored`
        let ids = gen_sorted_sparse_ids(500_000, 400);
        let mmap = build_mmap_from_ids(&ids);
        let tree = STree64Mmap::new(&mmap, ids.len());
        let qs = gen_queries(&ids, 50_000, 50_000, 401);
        let n = qs.len();

        // Warm up
        let _ = tree.lookup_batch(&qs, &mmap);

        let t0 = Instant::now();
        let _r1 = tree.lookup_batch(&qs, &mmap);
        let d1 = t0.elapsed();

        let t0 = Instant::now();
        let _r2 = tree.lookup_batch_pipeline::<16>(&qs, &mmap);
        let d2 = t0.elapsed();

        let t0 = Instant::now();
        let _r3 = tree.lookup_batch_pipeline::<32>(&qs, &mmap);
        let d3 = t0.elapsed();

        let t0 = Instant::now();
        let _r4 = tree.lookup_batch_ptr::<16>(&qs, &mmap);
        let d4 = t0.elapsed();

        let t0 = Instant::now();
        let _r5 = tree.lookup_batch_ptr::<32>(&qs, &mmap);
        let d5 = t0.elapsed();

        let t0 = Instant::now();
        let _r6 = tree.lookup_batch_interleave::<8>(&qs, &mmap);
        let d6 = t0.elapsed();

        let t0 = Instant::now();
        let _r7 = tree.lookup_batch_interleave::<16>(&qs, &mmap);
        let d7 = t0.elapsed();

        // Serial baseline
        let t0 = Instant::now();
        let _r0: Vec<_> = qs.iter().map(|&q| tree.find_exact(q, &mmap)).collect();
        let d0 = t0.elapsed();

        eprintln!("\n=== STree64Mmap batch benchmark ({n} queries, {} records) ===", ids.len());
        eprintln!("  serial find_exact:       {:>8.3} ms  ({:.0} ns/query)", d0.as_secs_f64()*1000.0, d0.as_nanos() as f64 / n as f64);
        eprintln!("  lookup_batch (sort+adv): {:>8.3} ms  ({:.0} ns/query)", d1.as_secs_f64()*1000.0, d1.as_nanos() as f64 / n as f64);
        eprintln!("  pipeline P=16:           {:>8.3} ms  ({:.0} ns/query)", d2.as_secs_f64()*1000.0, d2.as_nanos() as f64 / n as f64);
        eprintln!("  pipeline P=32:           {:>8.3} ms  ({:.0} ns/query)", d3.as_secs_f64()*1000.0, d3.as_nanos() as f64 / n as f64);
        eprintln!("  batch_ptr P=16:          {:>8.3} ms  ({:.0} ns/query)", d4.as_secs_f64()*1000.0, d4.as_nanos() as f64 / n as f64);
        eprintln!("  batch_ptr P=32:          {:>8.3} ms  ({:.0} ns/query)", d5.as_secs_f64()*1000.0, d5.as_nanos() as f64 / n as f64);
        eprintln!("  interleave P=8:          {:>8.3} ms  ({:.0} ns/query)", d6.as_secs_f64()*1000.0, d6.as_nanos() as f64 / n as f64);
        eprintln!("  interleave P=16:         {:>8.3} ms  ({:.0} ns/query)", d7.as_secs_f64()*1000.0, d7.as_nanos() as f64 / n as f64);
    }

    #[test]
    #[ignore]
    fn bench_stree64_batch_variants() {
        // In-RAM tree (STree64) batch comparison — 1M records, 500K queries
        let ids = gen_sorted_sparse_ids(1_000_000, 500);
        let tree = STree64::new(&ids);
        let qs = gen_queries(&ids, 250_000, 250_000, 501);
        let n = qs.len();

        // Warm up
        let _ = tree.batch_stream::<16>(&qs);

        let t0 = Instant::now();
        let _r0: Vec<_> = qs.iter().map(|&q| tree.find_exact(q)).collect();
        let d0 = t0.elapsed();

        let t0 = Instant::now();
        let _r1 = tree.batch_stream::<8>(&qs);
        let d1 = t0.elapsed();

        let t0 = Instant::now();
        let _r2 = tree.batch_stream::<16>(&qs);
        let d2 = t0.elapsed();

        let t0 = Instant::now();
        let _r3 = tree.batch_stream::<32>(&qs);
        let d3 = t0.elapsed();

        eprintln!("\n=== STree64 batch benchmark ({n} queries, {} records) ===", ids.len());
        eprintln!("  serial find_exact:  {:>8.3} ms  ({:.0} ns/query)", d0.as_secs_f64()*1000.0, d0.as_nanos() as f64 / n as f64);
        eprintln!("  batch_stream P=8:   {:>8.3} ms  ({:.0} ns/query)", d1.as_secs_f64()*1000.0, d1.as_nanos() as f64 / n as f64);
        eprintln!("  batch_stream P=16:  {:>8.3} ms  ({:.0} ns/query)", d2.as_secs_f64()*1000.0, d2.as_nanos() as f64 / n as f64);
        eprintln!("  batch_stream P=32:  {:>8.3} ms  ({:.0} ns/query)", d3.as_secs_f64()*1000.0, d3.as_nanos() as f64 / n as f64);
    }

    #[test]
    #[ignore]
    fn bench_compare_1m_records_osm_ids() {
        // Simulate planet-like conditions: 1M records with OSM-style IDs
        let mut rng = StdRng::seed_from_u64(600);
        let n = 1_000_000;
        let mut ids = Vec::with_capacity(n);
        let mut cur: i64 = 100_000_000;
        for _ in 0..n {
            cur += rng.gen_range(1..1000) as i64;
            ids.push(cur);
        }

        let mmap = build_mmap_from_ids(&ids);
        let tree = STree64Mmap::new(&mmap, ids.len());
        let qs = gen_queries(&ids, 100_000, 100_000, 601);
        let n_q = qs.len();

        // Verify correctness first
        let serial: Vec<_> = qs.iter().map(|&q| tree.find_exact(q, &mmap)).collect();
        assert_eq!(tree.lookup_batch_pipeline::<16>(&qs, &mmap), serial);
        assert_eq!(tree.lookup_batch_ptr::<16>(&qs, &mmap), serial);

        // Benchmark
        let t0 = Instant::now();
        let _ = qs.iter().map(|&q| tree.find_exact(q, &mmap)).collect::<Vec<_>>();
        let d_serial = t0.elapsed();

        let t0 = Instant::now();
        let _ = tree.lookup_batch(&qs, &mmap);
        let d_batch = t0.elapsed();

        let t0 = Instant::now();
        let _ = tree.lookup_batch_pipeline::<16>(&qs, &mmap);
        let d_pipe = t0.elapsed();

        let t0 = Instant::now();
        let _ = tree.lookup_batch_ptr::<16>(&qs, &mmap);
        let d_ptr = t0.elapsed();

        eprintln!("\n=== 1M OSM-like records, {n_q} queries ===");
        eprintln!("  serial:       {:>8.3} ms ({:.0} ns/q)", d_serial.as_secs_f64()*1000.0, d_serial.as_nanos() as f64 / n_q as f64);
        eprintln!("  batch:        {:>8.3} ms ({:.0} ns/q)", d_batch.as_secs_f64()*1000.0, d_batch.as_nanos() as f64 / n_q as f64);
        eprintln!("  pipeline:     {:>8.3} ms ({:.0} ns/q)", d_pipe.as_secs_f64()*1000.0, d_pipe.as_nanos() as f64 / n_q as f64);
        eprintln!("  batch_ptr:    {:>8.3} ms ({:.0} ns/q)", d_ptr.as_secs_f64()*1000.0, d_ptr.as_nanos() as f64 / n_q as f64);
    }
}
