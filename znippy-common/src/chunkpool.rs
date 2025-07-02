use std::sync::{Arc, Mutex};
use crate::ChunkQueue;
use crate::int_ring::RingBuffer;

/// A preallocated pool of fixed-size chunk buffers, managed via chunk indices.
pub struct ChunkPool<const CHUNK_SIZE: usize> {
    pool: Vec<Arc<[u8; CHUNK_SIZE]>>,
    ring: Mutex<RingBuffer>,
}

impl<const CHUNK_SIZE: usize> ChunkPool<CHUNK_SIZE> {
    /// Create a new ChunkPool with `num_chunks` preallocated buffers.
    pub fn new(num_chunks: usize) -> Self {
        let mut pool = Vec::with_capacity(num_chunks);
        for _ in 0..num_chunks {
            let boxed = Box::new([0u8; CHUNK_SIZE]);
            pool.push(Arc::new(boxed));
        }

        let mut ring = RingBuffer::new(num_chunks);
        for i in 0..num_chunks {
            ring.push(i as u32);
        }

        Self {
            pool,
            ring: Mutex::new(ring),
        }
    }

    /// Try to get a free chunk index. Returns `Some(chunk_nr)` or `None` if pool is exhausted.
    pub fn try_get_chunk_nr(&self) -> Option<usize> {
        let mut ring = self.ring.lock().unwrap();
        ring.pop()
    }

    /// Return a previously used chunk index back to the pool.
    pub fn return_chunk_nr(&self, chunk_nr: usize) {
        let mut ring = self.ring.lock().unwrap();
        ring.push(chunk_nr);
    }

    /// Get a reference to the Arc buffer for a given chunk_nr.
    pub fn get(&self, chunk_nr: usize) -> &Arc<[u8; CHUNK_SIZE]> {
        &self.pool[chunk_nr]
    }
}
