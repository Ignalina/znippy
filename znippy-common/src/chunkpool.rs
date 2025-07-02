use crate::common_config::CONFIG;
use crate::int_ring::{RingBuffer, ChunkQueue};
use std::sync::Arc;

/// Enkeltrådad pool av återanvändbara chunk-buffertar.
pub struct ChunkPool {
    buffers: Vec<Arc<[u8]>>,

    ring: RingBuffer,
}

impl ChunkPool {
    pub fn new() -> Self {




        let chunk_size = CONFIG.file_split_block_size_usize();
        let num_chunks = CONFIG.max_chunks;
        let mut buffers = Vec::with_capacity(num_chunks as usize);
        let mut ring = RingBuffer::new(num_chunks as usize);
        for i in 0..num_chunks {
            let buf: Box<[u8]> = vec![0u8; chunk_size].into_boxed_slice();
            buffers.push(buf.into()); // konverterar Box<[u8]> → Arc<[u8]>

            ring.push(i);
        }

        Self { buffers, ring }
    }

    /// Hämtar nästa lediga chunk-index från ringen.
    pub fn get_index(&mut self) -> u32 {
        self.ring.pop().expect("RingBuffer underrun: inga lediga chunk-index kvar")
    }

    /// Returnerar ett chunk-index till poolen.
    pub fn return_index(&mut self, index: u32) {
        self.ring.push(index);
    }

    /// Hämtar Arc till en buffer givet ett index.
    pub fn get_buffer(&self, index: u32) -> Arc<[u8]> {
        Arc::clone(&self.buffers[index as usize])
    }
}
