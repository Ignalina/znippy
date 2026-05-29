//! `psort` — parallel sort building blocks extracted from katana-osm.
//!
//! The workhorse here is [`samplesort_aos_by_i64_key`]: a parallel sample sort
//! over fixed 16-byte array-of-structs records keyed by their leading
//! little-endian `i64`. It is the fast-path sorter for katana-osm's node-coord
//! store (`[i64 id][f32 lat][f32 lon]`), but it is generally useful for any
//! 16-byte AoS record whose sort key is the first 8 bytes interpreted as `i64`.
//!
//! Like the rest of `znippy-zoomies`, it follows the design laws: no `rayon`
//! (pure `std::thread::scope`), no per-record allocation (two scratch buffers
//! allocated once), and a no-barrier embarrassingly-parallel tail.
//!
//! A [`radix_sort_aos_by_i64_key`] LSD radix sort is also provided; it is the
//! historical implementation, retained as a reference / regression oracle and a
//! fallback for callers that prefer it.

use std::sync::atomic::{AtomicUsize, Ordering};

/// Width of an AoS record, in bytes. The key is the first 8 bytes (`i64` LE).
pub const RECORD_SIZE: usize = 16;

/// Parallel **sample sort** over `[u8; 16]` records keyed by the leading
/// little-endian `i64`.
///
/// Why sample sort beats a fixed-radix LSD here:
/// - **Data-driven splitters.** OSM node IDs (the motivating workload) are
///   dense and positive, so the high bytes of the key barely vary — fixed-radix
///   MSD buckets would all collapse into one. Sampled splitters cut the key
///   space where the data actually lives, giving balanced buckets regardless of
///   distribution.
/// - **Fewer full record-moving passes.** A 4-pass 16-bit LSD radix moves every
///   16 B record 4 times (read+write each pass ≈ 8 × 16n bandwidth). Sample sort
///   does one classify pass, one scatter pass, parallel per-bucket comparison
///   sorts (each bucket is L2/L3-resident, so cheap), then one copy-back
///   (≈ 3 × 16n of big sequential traffic + small cache-hot sorts).
/// - **Embarrassingly parallel tail.** Buckets are disjoint contiguous ranges,
///   so the per-bucket sorts need zero cross-thread coordination; an atomic work
///   counter load-balances skewed buckets across all cores.
///
/// Correctness: equal keys map to the same bucket (`partition_point` upper bound
/// over sorted splitters is monotone in the key), so cross-bucket order is
/// preserved and within-bucket order is fixed by the comparison sort. Any
/// distribution — including all-equal keys — sorts correctly (it just lands in
/// fewer buckets). Output is left in `records` (in-place contract).
///
/// Sign handling: the leading `i64` is sign-flipped (`^ 1<<63`) before bucketing
/// so signed keys order correctly under the unsigned-compare splitter search.
pub fn samplesort_aos_by_i64_key(records: &mut [[u8; RECORD_SIZE]]) {
    const SEQ_FLOOR: usize = 1 << 16; // 65 536
    let n = records.len();
    if n <= 1 { return; }
    if n < SEQ_FLOOR {
        records.sort_unstable_by_key(|r| i64::from_le_bytes(r[..8].try_into().unwrap()));
        return;
    }

    #[inline(always)]
    fn key_of(rec: &[u8; RECORD_SIZE]) -> u64 {
        // Sign-flip so signed i64 keys order correctly under unsigned compare.
        u64::from_le_bytes(rec[..8].try_into().unwrap()) ^ 0x8000_0000_0000_0000
    }
    #[inline(always)]
    fn bucket_of(splitters: &[u64], key: u64) -> usize {
        // Count of splitters <= key → bucket in [0, splitters.len()].
        splitters.partition_point(|&s| s <= key)
    }

    let n_threads = std::thread::available_parallelism()
        .map(|x| x.get()).unwrap_or(1).max(1);

    // Aim for ~128 K records/bucket (≈ 2 MB, L2/L3-resident) and comfortably
    // more buckets than threads so the per-bucket work load-balances.
    const TARGET_BUCKET: usize = 128 * 1024;
    let k = (n / TARGET_BUCKET)
        .next_power_of_two()
        .max((n_threads * 4).next_power_of_two())
        .clamp(1 << 4, 1 << 16); // 16..=65536 buckets

    // ── Sample splitters ───────────────────────────────────────────────────
    // Oversample, sort, then pick evenly-spaced splitters. Deterministic
    // strided sampling avoids an RNG dependency while still spreading picks
    // across the whole input.
    const OVERSAMPLE: usize = 8;
    let n_samples = ((k - 1) * OVERSAMPLE).min(n);
    let stride = (n / n_samples).max(1);
    let mut samples: Vec<u64> = Vec::with_capacity(n_samples);
    let mut si = 0usize;
    while samples.len() < n_samples && si < n {
        samples.push(key_of(&records[si]));
        si += stride;
    }
    samples.sort_unstable();
    let mut splitters: Vec<u64> = Vec::with_capacity(k - 1);
    for j in 1..k {
        let idx = ((j * samples.len()) / k).min(samples.len() - 1);
        splitters.push(samples[idx]);
    }
    // `splitters` is sorted (samples were); duplicates just yield empty
    // buckets, which the prefix-sum and per-bucket sort both tolerate.
    debug_assert_eq!(splitters.len(), k - 1);

    let chunk = n.div_ceil(n_threads);

    // ── Phase 1: classify (per-thread bucket histograms) ─────────────────────
    // Record each element's bucket once into `bucket_id` so the scatter pass
    // doesn't re-run the binary search.
    let mut bucket_id: Vec<u16> = vec![0u16; n];
    // Offsets are `usize` so the prefix sum addresses >u32::MAX records
    // (e.g. planet's ~10.4 B nodes); per-thread × k buckets, so negligible RAM.
    let mut hists: Vec<Vec<usize>> = (0..n_threads).map(|_| vec![0usize; k]).collect();
    std::thread::scope(|s| {
        let recs = &*records;
        let spl = &splitters;
        for (t, (hist, bid_chunk)) in
            hists.iter_mut().zip(bucket_id.chunks_mut(chunk)).enumerate()
        {
            let start = t * chunk;
            s.spawn(move || {
                for (j, slot) in bid_chunk.iter_mut().enumerate() {
                    let b = bucket_of(spl, key_of(&recs[start + j]));
                    *slot = b as u16;
                    hist[b] += 1;
                }
            });
        }
    });

    // ── Phase 2: global prefix sum (bucket-major, per-thread offsets) ────────
    // hists[t][b] becomes the dst position where thread t starts writing its
    // bucket-b records. Globally non-overlapping → race-free scatter.
    {
        let mut running: usize = 0;
        for b in 0..k {
            for hist in hists.iter_mut() {
                let count = hist[b];
                hist[b] = running;
                running += count;
            }
        }
        debug_assert_eq!(running, n);
    }
    // Bucket b begins at thread 0's offset (it is first in the bucket-major
    // order). Snapshot before the scatter mutates `hists`.
    let mut bucket_start: Vec<usize> = (0..k).map(|b| hists[0][b]).collect();
    bucket_start.push(n);

    // ── Phase 3: parallel scatter (records → scratch, grouped by bucket) ─────
    let mut scratch: Vec<[u8; RECORD_SIZE]> = vec![[0u8; RECORD_SIZE]; n];
    let dst_addr = scratch.as_mut_ptr() as usize;
    std::thread::scope(|s| {
        let recs = &*records;
        let bid = &bucket_id;
        for (t, mut hist) in hists.into_iter().enumerate() {
            let start = t * chunk;
            let end = (start + chunk).min(n);
            s.spawn(move || {
                let dst = dst_addr as *mut [u8; RECORD_SIZE];
                for i in start..end {
                    let b = bid[i] as usize;
                    let pos = hist[b];
                    hist[b] += 1;
                    unsafe { std::ptr::copy_nonoverlapping(&recs[i], dst.add(pos), 1); }
                }
            });
        }
    });

    // ── Phase 4: parallel per-bucket sort (atomic work-stealing) ─────────────
    let next = AtomicUsize::new(0);
    let scratch_addr = scratch.as_mut_ptr() as usize;
    {
        let bucket_start = &bucket_start;
        let next = &next;
        std::thread::scope(|s| {
            for _ in 0..n_threads {
                s.spawn(move || {
                    loop {
                        let b = next.fetch_add(1, Ordering::Relaxed);
                        if b >= k { break; }
                        let lo = bucket_start[b];
                        let hi = bucket_start[b + 1];
                        if hi - lo > 1 {
                            let base = scratch_addr as *mut [u8; RECORD_SIZE];
                            let slice = unsafe {
                                std::slice::from_raw_parts_mut(base.add(lo), hi - lo)
                            };
                            slice.sort_unstable_by_key(
                                |r| i64::from_le_bytes(r[..8].try_into().unwrap()));
                        }
                    }
                });
            }
        });
    }

    // ── Phase 5: parallel copy-back (scratch → records) ──────────────────────
    let src_addr = scratch.as_ptr() as usize;
    let out_addr = records.as_mut_ptr() as usize;
    std::thread::scope(|s| {
        for t in 0..n_threads {
            let start = t * chunk;
            let end = (start + chunk).min(n);
            s.spawn(move || {
                let src = src_addr as *const [u8; RECORD_SIZE];
                let dst = out_addr as *mut [u8; RECORD_SIZE];
                unsafe {
                    std::ptr::copy_nonoverlapping(src.add(start), dst.add(start), end - start);
                }
            });
        }
    });

    drop(scratch);
}

/// LSD radix sort over `[u8; 16]` records keyed by the leading little-endian
/// `i64`. Historical implementation, retained as a reference / regression
/// oracle and as a fallback. [`samplesort_aos_by_i64_key`] is the recommended
/// fast path.
///
/// Implementation: 16-bit (65536-bucket) buckets, 4 passes, per-thread
/// histograms, sequential prefix-sum, race-free parallel scatter into a scratch
/// buffer via `std::thread::scope`. Sign bit is XORed before bucketing so signed
/// `i64` sorts correctly under unsigned radix; records are never mutated by the
/// key extraction.
pub fn radix_sort_aos_by_i64_key(records: &mut [[u8; RECORD_SIZE]]) {
    const RADIX:    usize = 1 << 16;
    const PASSES:   usize = 4;
    const SEQ_FLOOR: usize = 16_384;
    let n = records.len();
    if n <= 1 { return; }
    if n < SEQ_FLOOR {
        records.sort_unstable_by_key(|r| i64::from_le_bytes(r[..8].try_into().unwrap()));
        return;
    }

    let mut scratch: Vec<[u8; RECORD_SIZE]> = vec![[0u8; RECORD_SIZE]; n];
    let mut src_addr = records.as_mut_ptr() as usize;
    let mut dst_addr = scratch .as_mut_ptr() as usize;

    let n_threads = std::thread::available_parallelism()
        .map(|n| n.get()).unwrap_or(1).max(1);
    let chunk = n.div_ceil(n_threads);

    #[inline(always)]
    fn key_bits(rec: *const [u8; RECORD_SIZE], shift: u32) -> usize {
        unsafe {
            let bytes = &*(rec as *const [u8; 8]);
            let k = u64::from_le_bytes(*bytes) ^ 0x8000_0000_0000_0000;
            ((k >> shift) & 0xffff) as usize
        }
    }

    for pass in 0..PASSES {
        let shift = (pass * 16) as u32;

        let mut hists: Vec<Vec<usize>> = (0..n_threads).map(|_| vec![0usize; RADIX]).collect();
        std::thread::scope(|s| {
            for (t, hist) in hists.iter_mut().enumerate() {
                let start = t * chunk;
                let end = (start + chunk).min(n);
                let src_addr = src_addr;
                s.spawn(move || {
                    let src = src_addr as *const [u8; RECORD_SIZE];
                    for i in start..end {
                        let bucket = key_bits(unsafe { src.add(i) }, shift);
                        hist[bucket] += 1;
                    }
                });
            }
        });

        {
            let mut running: usize = 0;
            for b in 0..RADIX {
                for hist in hists.iter_mut() {
                    let count = hist[b];
                    hist[b] = running;
                    running += count;
                }
            }
            debug_assert_eq!(running, n);
        }

        std::thread::scope(|s| {
            for (t, mut hist) in hists.into_iter().enumerate() {
                let start = t * chunk;
                let end = (start + chunk).min(n);
                let src_addr = src_addr;
                let dst_addr = dst_addr;
                s.spawn(move || {
                    let src = src_addr as *const [u8; RECORD_SIZE];
                    let dst = dst_addr as *mut [u8; RECORD_SIZE];
                    for i in start..end {
                        let rec_ptr = unsafe { src.add(i) };
                        let bucket  = key_bits(rec_ptr, shift);
                        let pos = hist[bucket];
                        hist[bucket] += 1;
                        unsafe { std::ptr::copy_nonoverlapping(rec_ptr, dst.add(pos), 1); }
                    }
                });
            }
        });

        std::mem::swap(&mut src_addr, &mut dst_addr);
    }

    // After PASSES (4, even) the sorted output is back in `records`.
    debug_assert_eq!(src_addr, records.as_mut_ptr() as usize);
    drop(scratch);
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEQ_FLOOR_FOR_TEST: usize = 16_384;

    fn build_shuffled_records(n: usize) -> Vec<[u8; RECORD_SIZE]> {
        (0..n).map(|i| {
            let id  = ((i as u64).wrapping_mul(0x9E3779B97F4A7C15) % (1u64 << 50)) as i64 + 1;
            let lat = f32::from_bits(((id as u64) & 0xffffffff) as u32);
            let lon = f32::from_bits(((id as u64 >> 32) & 0xffffffff) as u32);
            let mut rec = [0u8; RECORD_SIZE];
            rec[..8].copy_from_slice(&id.to_le_bytes());
            rec[8..12].copy_from_slice(&lat.to_le_bytes());
            rec[12..16].copy_from_slice(&lon.to_le_bytes());
            rec
        }).collect()
    }

    #[test]
    fn samplesort_matches_std_sort() {
        for &n in &[0usize, 1, 7, SEQ_FLOOR_FOR_TEST - 1, SEQ_FLOOR_FOR_TEST, 100_000, 1_500_000] {
            let mut records = build_shuffled_records(n);
            let mut expected = records.clone();
            expected.sort_unstable_by_key(|r| i64::from_le_bytes(r[..8].try_into().unwrap()));
            samplesort_aos_by_i64_key(&mut records);
            assert_eq!(records, expected, "samplesort mismatch at n={n}");
        }
    }

    #[test]
    fn radix_sort_matches_std_sort() {
        for &n in &[0usize, 1, 7, SEQ_FLOOR_FOR_TEST - 1, SEQ_FLOOR_FOR_TEST, 100_000, 1_500_000] {
            let mut records = build_shuffled_records(n);
            let mut expected = records.clone();
            expected.sort_unstable_by_key(|r| i64::from_le_bytes(r[..8].try_into().unwrap()));
            radix_sort_aos_by_i64_key(&mut records);
            assert_eq!(records, expected, "radix sort mismatch at n={n}");
        }
    }

    #[test]
    fn samplesort_handles_signed_keys() {
        let n = 200_000;
        let mut records: Vec<[u8; RECORD_SIZE]> = (0..n).map(|i| {
            let id: i64 = ((i as i64).wrapping_mul(2654435761) % (1 << 40)) - (1 << 39);
            let mut rec = [0u8; RECORD_SIZE];
            rec[..8].copy_from_slice(&id.to_le_bytes());
            rec[8..12].copy_from_slice(&(i as f32).to_le_bytes());
            rec[12..16].copy_from_slice(&((-i as f32)).to_le_bytes());
            rec
        }).collect();
        let mut expected = records.clone();
        expected.sort_unstable_by_key(|r| i64::from_le_bytes(r[..8].try_into().unwrap()));
        samplesort_aos_by_i64_key(&mut records);
        assert_eq!(records, expected);
    }

    /// Degenerate distributions: with many duplicate keys the within-tie order
    /// is unspecified (unstable sort), so assert the real invariants — keys
    /// come out non-decreasing and the record multiset is preserved.
    #[test]
    fn samplesort_handles_degenerate_distributions() {
        fn assert_sorted_permutation(orig: &[[u8; RECORD_SIZE]], sorted: &[[u8; RECORD_SIZE]]) {
            let key = |r: &[u8; RECORD_SIZE]| i64::from_le_bytes(r[..8].try_into().unwrap());
            assert!(sorted.windows(2).all(|w| key(&w[0]) <= key(&w[1])), "not sorted by key");
            let mut a = orig.to_vec();
            let mut b = sorted.to_vec();
            a.sort_unstable();
            b.sort_unstable();
            assert_eq!(a, b, "record multiset not preserved");
        }

        let n = 120_000;

        let orig: Vec<[u8; RECORD_SIZE]> = (0..n).map(|i| {
            let mut rec = [0u8; RECORD_SIZE];
            rec[..8].copy_from_slice(&42i64.to_le_bytes());
            rec[8..12].copy_from_slice(&(i as f32).to_le_bytes());
            rec
        }).collect();
        let mut records = orig.clone();
        samplesort_aos_by_i64_key(&mut records);
        assert_sorted_permutation(&orig, &records);

        let orig: Vec<[u8; RECORD_SIZE]> = (0..n).map(|i| {
            let id: i64 = if i % 10 == 0 { (i as i64) * 7919 } else { 1 };
            let mut rec = [0u8; RECORD_SIZE];
            rec[..8].copy_from_slice(&id.to_le_bytes());
            rec[8..12].copy_from_slice(&(i as f32).to_le_bytes());
            rec
        }).collect();
        let mut records = orig.clone();
        samplesort_aos_by_i64_key(&mut records);
        assert_sorted_permutation(&orig, &records);
    }

    /// samplesort and radix must agree with each other (cross-check) on unique
    /// keys.
    #[test]
    fn samplesort_agrees_with_radix() {
        let mut a = build_shuffled_records(300_000);
        let mut b = a.clone();
        samplesort_aos_by_i64_key(&mut a);
        radix_sort_aos_by_i64_key(&mut b);
        assert_eq!(a, b);
    }
}
