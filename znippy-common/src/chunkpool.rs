// znippy-common/src/chunkpool.rs

use crate::common_config::CONFIG;
use crate::int_ring::RingBuffer;
use std::sync::Arc;

/// Enkeltrådad pool av återanvändbara chunk-buffertar.
pub struct ChunkPool {
    buffers: Vec<Arc<Box<[u8; CHUNK_SIZE]>>>,
    ring: RingBuffer,
}

const CHUNK_SIZE:usize  = CONFIG.file_split_block_size as usize;

impl ChunkPool {
    pub fn new() -> Self {
        let num_chunks = CONFIG.max_chunks;
        let mut buffers = Vec::with_capacity(num_chunks);
        let mut ring = RingBuffer::new(num_chunks as u32);

        for i in 0..num_chunks {
            let buf = Box::new([0u8; CHUNK_SIZE]);
            buffers.push(Arc::new(buf));
            ring.push(i as u32);
        }

        Self { buffers, ring }
    }

    /// Hämtar nästa lediga chunk-index.
    pub fn get_index(&mut self) -> u32 {
        self.ring.pop()
    }

    /// Returnerar ett chunk-index till poolen.
    pub fn return_index(&mut self, index: u32) {
        self.ring.push(index);
    }

    /// Hämtar Arc till buffer givet ett index.
    pub fn get_buffer(&self, index: u32) -> Arc<Box<[u8; CHUNK_SIZE]>> {
        Arc::clone(&self.buffers[index as usize])
    }
}
