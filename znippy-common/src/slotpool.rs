//! Magazine — shared large-slot buffer pool for the no-barrier slice pipeline.
//!
//! Design: TODO_NOW.md "THE FINAL SOLUTION 2026". Replaces the per-thread
//! ChunkRevolver rings. One reader thread fills shared slots; N workers pull
//! slices off a single global queue (built by the caller) and never wait for a
//! slot to complete — a worker that finishes a slice grabs the next one from
//! ANY slot.
//!
//! Lifecycle of a slot:
//!   FREE → (reader `claim`s) FILLING → (reader `publish`es) DRAINING
//!        → (workers `release_one` each slice; last one frees it) FREE
//!
//! Packing (done by the reader via `Clip`):
//!   - small file (≤ slice_size): read into the slot at the cursor, committed
//!     as one variable-width slice; many small files coalesce into one slot.
//!   - big file (> slice_size): cut into slice_size pieces, each committed as a
//!     slice; the file spills across as many slots as needed.
//!   Invariant: a slice ⊆ one file AND ⊆ one slot (contiguous in its slot).
//!
//! Safety: while a slot is FILLING only the reader touches it (exclusive, via
//! `writable`). After `publish` the reader drops the `Clip` and only workers
//! read the slot (shared, via `Round::as_slice`). The slot is not handed back to
//! the reader (`claim`) until its outstanding counter hits zero, so the
//! &mut/&  windows never overlap.

use crossbeam_channel::{Receiver, Sender, bounded};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Raw base pointer into a slot buffer. Send+Sync by the lifecycle discipline
/// documented above (same contract as `chunkrevolver::SendPtr`).
#[derive(Copy, Clone)]
struct SlotPtr(*mut u8);
unsafe impl Send for SlotPtr {}
unsafe impl Sync for SlotPtr {}

/// One unit of work. Borrows bytes inside slot `slot_id`; valid until the slot
/// is released. Carries everything needed to build a `ChunkMeta` later.
pub struct Round {
    pub slot_id: u32,
    ptr: *const u8,
    pub len: usize,
    pub skip: bool,
    pub file_index: u64,
    pub fdata_offset: u64,
    pub chunk_seq: u32,
}

// Safety: the pointer addresses an immutable region of a published slot. No
// thread writes the slot between publish and release, so sharing the read-only
// view across worker threads is sound.
unsafe impl Send for Round {}

impl Round {
    /// Borrow the slice bytes.
    ///
    /// Safety: the caller must hold this `Round` only while its slot is
    /// unreleased (i.e. call `Ejector::release_one` for this slot only after
    /// the returned borrow is dropped / the bytes are written).
    pub unsafe fn as_slice<'a>(&self) -> &'a [u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

/// Shared handle workers use to release slots back to the reader. Cloneable
/// (it is an `Arc` internally) — move a clone into each worker thread.
#[derive(Clone)]
pub struct Ejector {
    inner: Arc<EjectorInner>,
}

struct EjectorInner {
    free_tx: Sender<u32>,
    outstanding: Vec<AtomicUsize>,
}

impl Ejector {
    /// One slice of `slot_id` has been fully consumed (compressed, or pwritten
    /// for the skip path). When the slot's last slice is done, the slot returns
    /// to the free list so the reader may claim it again.
    pub fn release_one(&self, slot_id: u32) {
        let prev = self.inner.outstanding[slot_id as usize].fetch_sub(1, Ordering::AcqRel);
        debug_assert!(prev >= 1, "release_one underflow on slot {slot_id}");
        if prev == 1 {
            // Never blocks: we only ever return ids we previously took out, so
            // the bounded free channel can hold them all.
            self.inner.free_tx.send(slot_id).ok();
        }
    }
}

/// Pool of `num_slots` reusable slot buffers. Owned by the reader thread.
pub struct Magazine {
    slot_size: usize,
    slice_size: usize,
    base: Vec<SlotPtr>,
    _mem: Vec<Box<[u8]>>, // backing storage, kept alive for the pool's lifetime
    free_rx: Receiver<u32>,
    ret: Ejector,
}

impl Magazine {
    /// Allocate the pool. `slice_size = slot_size / num_workers` (≥1), the
    /// granularity at which the reader cuts big files. All slots start FREE.
    pub fn new(num_slots: usize, slot_size: usize, num_workers: usize) -> Self {
        assert!(num_slots > 0 && slot_size > 0 && num_workers > 0);
        let slice_size = (slot_size / num_workers).max(1);

        let mut mem: Vec<Box<[u8]>> = (0..num_slots)
            .map(|_| vec![0u8; slot_size].into_boxed_slice())
            .collect();
        // Take base pointers AFTER the buffers are in their final Vec slots; the
        // heap data addresses are stable from here on (we never reallocate mem).
        let base: Vec<SlotPtr> = mem.iter_mut().map(|b| SlotPtr(b.as_mut_ptr())).collect();

        let (free_tx, free_rx) = bounded(num_slots);
        for id in 0..num_slots as u32 {
            free_tx.send(id).expect("free channel send during init");
        }
        let outstanding = (0..num_slots).map(|_| AtomicUsize::new(0)).collect();

        Magazine {
            slot_size,
            slice_size,
            base,
            _mem: mem,
            free_rx,
            ret: Ejector { inner: Arc::new(EjectorInner { free_tx, outstanding }) },
        }
    }

    pub fn slot_size(&self) -> usize {
        self.slot_size
    }
    pub fn slice_size(&self) -> usize {
        self.slice_size
    }
    pub fn num_slots(&self) -> usize {
        self.base.len()
    }

    /// Handle workers use to release slots. Clone it into each worker thread.
    pub fn returner(&self) -> Ejector {
        self.ret.clone()
    }

    /// Reader: claim a free slot to fill. Blocks until one is free — this block
    /// is the ONLY backpressure in the pipeline. Returns `None` once the pool is
    /// shut down and drained (all return senders gone).
    pub fn claim(&self) -> Option<Clip<'_>> {
        let slot_id = self.free_rx.recv().ok()?;
        Some(Clip { pool: self, slot_id, cursor: 0, slices: Vec::new() })
    }
}

/// Reader-side handle for filling one claimed slot. Pack files into it via
/// `writable` + `commit_slice`, then `publish` to hand the slices to the queue.
pub struct Clip<'a> {
    pool: &'a Magazine,
    slot_id: u32,
    cursor: usize,
    slices: Vec<Round>,
}

impl<'a> Clip<'a> {
    pub fn slot_id(&self) -> u32 {
        self.slot_id
    }

    /// Free space left in the slot.
    pub fn remaining(&self) -> usize {
        self.pool.slot_size - self.cursor
    }

    /// Writable view at the cursor, up to `max` bytes (clamped to remaining).
    /// The reader reads file bytes directly into this — no intermediate buffer.
    /// The borrow must be dropped before calling `commit_slice`.
    pub fn writable(&mut self, max: usize) -> &mut [u8] {
        let n = max.min(self.remaining());
        let base = self.pool.base[self.slot_id as usize].0;
        // Safety: exclusive access — this slot is FILLING and only the reader
        // (holding &mut self) touches it; no slices are published yet.
        unsafe { std::slice::from_raw_parts_mut(base.add(self.cursor), n) }
    }

    /// Commit the `len` bytes just written at the cursor as one slice and advance.
    pub fn commit_slice(
        &mut self,
        len: usize,
        skip: bool,
        file_index: u64,
        fdata_offset: u64,
        chunk_seq: u32,
    ) {
        debug_assert!(len <= self.remaining());
        let base = self.pool.base[self.slot_id as usize].0 as *const u8;
        // Safety: cursor stays within the slot (asserted above).
        let ptr = unsafe { base.add(self.cursor) };
        self.slices.push(Round {
            slot_id: self.slot_id,
            ptr,
            len,
            skip,
            file_index,
            fdata_offset,
            chunk_seq,
        });
        self.cursor += len;
    }

    pub fn slice_count(&self) -> usize {
        self.slices.len()
    }

    /// Publish the slot: arm its outstanding counter and return the slices for
    /// the caller to push onto the global queue. An empty slot is immediately
    /// returned to the free list (and an empty Vec is returned).
    #[must_use]
    pub fn publish(self) -> Vec<Round> {
        let n = self.slices.len();
        if n == 0 {
            self.pool.ret.inner.free_tx.send(self.slot_id).ok();
            return Vec::new();
        }
        self.pool.ret.inner.outstanding[self.slot_id as usize].store(n, Ordering::Release);
        self.slices
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_returns_only_after_last_slice() {
        // 2 slots, 64 bytes each, 4 workers → slice_size 16.
        let pool = Magazine::new(2, 64, 4);
        assert_eq!(pool.slice_size(), 16);
        let ret = pool.returner();

        // Claim both slots; pool is now empty.
        let mut a = pool.claim().unwrap();
        let _b = pool.claim().unwrap();

        // Pack two small "files" into slot a.
        let n = { a.writable(10).len().min(10) };
        a.commit_slice(n, false, 0, 0, 0);
        let n2 = { a.writable(20).len().min(20) };
        a.commit_slice(n2, false, 1, 0, 0);
        let slices = a.publish();
        assert_eq!(slices.len(), 2);
        let slot_a = slices[0].slot_id;

        // Releasing the first slice must NOT free the slot yet.
        ret.release_one(slot_a);
        assert!(pool.claim_now().is_none(), "slot freed too early");

        // Releasing the last slice frees it.
        ret.release_one(slot_a);
        assert!(pool.claim_now().is_some(), "slot not freed after last slice");
    }

    #[test]
    fn writable_clamps_to_remaining() {
        let pool = Magazine::new(1, 32, 4);
        let mut f = pool.claim().unwrap();
        assert_eq!(f.writable(1000).len(), 32);
        f.commit_slice(30, false, 0, 0, 0);
        assert_eq!(f.remaining(), 2);
        assert_eq!(f.writable(1000).len(), 2);
    }

    impl Magazine {
        /// Non-blocking claim, for tests only.
        fn claim_now(&self) -> Option<u32> {
            self.free_rx.try_recv().ok()
        }
    }
}
