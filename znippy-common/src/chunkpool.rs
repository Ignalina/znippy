use crate::int_ring::RingBuffer;
use std::sync::Arc;
use crate::ChunkQueue;

/// A preallocated pool of fixed-size chunk buffers, managed via chunk indices.
/// Only safe for single-threaded chunk allocation (reader thread only).
pub struct ChunkPool<const CHUNK_SIZE: usize> {
    pool: Vec<Arc<[u8; CHUNK_SIZE]>>,
    ring: RingBuffer, // No Mutex needed
}

impl<const CHUNK_SIZE: usize> ChunkPool<CHUNK_SIZE> {
    pub fn new(num_chunks: usize) -> Self {
        let mut pool = Vec::with_capacity(num_chunks);
        for _ in 0..num_chunks {
            let boxed = Box::new([0u8; CHUNK_SIZE]);
            pool.push(Arc::from(boxed));
        }

        let mut ring = RingBuffer::new(num_chunks);
        for i in 0..num_chunks {
            ring.push(i);
        }

        Self { pool, ring }
    }

    /// Only safe from reader thread
    pub fn try_get_chunk_nr(&mut self) -> Option<usize> {
        self.ring.pop()
    }

    /// Can be called from writer or compressor threads if needed
    pub fn return_chunk_nr(&mut self, chunk_nr: usize) {
        self.ring.push(chunk_nr);
    }

    pub fn get(&self, chunk_nr: usize) -> &Arc<[u8; CHUNK_SIZE]> {
        &self.pool[chunk_nr]
    }
}
