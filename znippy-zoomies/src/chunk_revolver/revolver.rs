use super::ring::{ChunkQueue, RingBuffer};
use std::ops::{Deref, DerefMut};

/// A raw pointer to a thread's memory block that is safe to send across threads.
/// Safety: the caller must ensure the pointer stays valid for the chunk's lifetime.
#[derive(Copy, Clone)]
pub struct SendPtr(*const u8);
unsafe impl Send for SendPtr {}
unsafe impl Sync for SendPtr {}

impl SendPtr {
    pub fn new(ptr: *const u8) -> Self { Self(ptr) }
    pub fn as_ptr(&self) -> *const u8 { self.0 }
}

/// A slot leased from the revolver.
/// Carries `ring_nr` + `index` so it can be returned to the correct ring.
pub struct Chunk<'a> {
    pub ring_nr: u8,
    pub index:   u64,
    pub data:    &'a mut [u8],
}

impl<'a> Deref    for Chunk<'a> { type Target = [u8]; fn deref(&self)     -> &[u8]     { self.data } }
impl<'a> DerefMut for Chunk<'a> {                      fn deref_mut(&mut self) -> &mut [u8] { self.data } }

/// Zero-allocation slot pool for a 1-reader / N-worker pipeline.
///
/// Memory layout: one contiguous `Box<[u8]>` per worker ring.
/// Each ring owns `chunks_per_ring` fixed-size slots and a free-list ring buffer.
///
/// # Usage
///
/// ```text
/// let mut rev = ChunkRevolver::new(chunk_size, total_chunks, num_workers);
/// let base_ptrs = rev.base_ptrs();   // send to worker threads
///
/// // Reader thread:
/// loop {
///     let mut chunk = loop {
///         if let Some(c) = rev.try_get_chunk() { break c; }
///         let (rn, idx) = rx_return.recv().unwrap();
///         rev.return_chunk(rn, idx);
///     };
///     read_into(&mut *chunk);
///     tx_per_ring[chunk.ring_nr as usize].send((chunk.ring_nr, chunk.index, bytes_read));
/// }
///
/// // Worker thread (one per ring):
/// while let Ok((ring_nr, index, used)) = rx_chunk.recv() {
///     let slice = unsafe { get_chunk_slice(base_ptr, chunk_size, index as u32, used) };
///     process(slice);
///     tx_return.send((ring_nr, index));
/// }
/// ```
pub struct ChunkRevolver {
    chunk_size:     usize,
    rings:          Vec<RingBuffer>,
    memory_blocks:  Vec<Box<[u8]>>,
    next_ring:      usize,
}

impl ChunkRevolver {
    /// Create a new revolver.
    ///
    /// - `chunk_size`:    bytes per slot (fixed, padded if needed)
    /// - `total_chunks`:  total number of pre-allocated slots across all rings
    /// - `num_workers`:   number of worker threads (= number of rings)
    pub fn new(chunk_size: usize, total_chunks: usize, num_workers: usize) -> Self {
        let safe_workers = num_workers.min(total_chunks);
        let chunks_per_ring = total_chunks / safe_workers;

        let mut rings         = Vec::with_capacity(safe_workers);
        let mut memory_blocks = Vec::with_capacity(safe_workers);

        for _ in 0..safe_workers {
            let mut ring = RingBuffer::new(chunks_per_ring);
            for i in 0..chunks_per_ring {
                ring.push(i as u64).expect("init ring overflow");
            }
            rings.push(ring);
            memory_blocks.push(vec![0u8; chunks_per_ring * chunk_size].into_boxed_slice());
        }

        Self { chunk_size, rings, memory_blocks, next_ring: 0 }
    }

    /// Base pointer for each ring's memory block — send these to worker threads.
    pub fn base_ptrs(&self) -> Vec<SendPtr> {
        self.memory_blocks.iter()
            .map(|b| SendPtr::new(b.as_ptr()))
            .collect()
    }

    pub fn base_ptr(&self, ring_nr: usize) -> *const u8 {
        self.memory_blocks[ring_nr].as_ptr()
    }

    pub fn chunk_size(&self) -> usize { self.chunk_size }

    pub fn num_rings(&self) -> usize { self.rings.len() }

    /// Try to lease a free slot, rotating through rings round-robin.
    /// Returns `None` if every slot is currently in-flight.
    pub fn try_get_chunk(&mut self) -> Option<Chunk<'_>> {
        let n = self.rings.len();
        for i in 0..n {
            let rn = (self.next_ring + i) % n;
            if let Some(index) = self.rings[rn].pop() {
                let offset = index as usize * self.chunk_size;
                let data = &mut self.memory_blocks[rn][offset..offset + self.chunk_size];
                self.next_ring = (rn + 1) % n;
                return Some(Chunk { ring_nr: rn as u8, index, data });
            }
        }
        None
    }

    /// Return a finished slot to its ring so the reader can reuse it.
    pub fn return_chunk(&mut self, ring_nr: u8, index: u64) {
        self.rings[ring_nr as usize]
            .push(index)
            .expect("ring overflow on return — double return?");
    }
}

/// Borrow `used` bytes from a worker's memory block without copying.
///
/// # Safety
/// `base_ptr` must be the pointer for `ring_nr`, obtained from `ChunkRevolver::base_ptrs()`.
/// The slot must currently be in-flight (not returned to the revolver).
pub unsafe fn get_chunk_slice<'a>(
    base_ptr:   *const u8,
    chunk_size: usize,
    index:      u32,
    used:       usize,
) -> &'a [u8] {
    let offset = index as usize * chunk_size;
    // SAFETY: caller guarantees base_ptr is valid for the ring's memory block
    // and the slot at `index` is currently in-flight (not returned to revolver).
    unsafe { std::slice::from_raw_parts(base_ptr.add(offset), used) }
}
