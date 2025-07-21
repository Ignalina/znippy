pub use crate::common_config::{StrategicConfig, CONFIG};
use crate::int_ring::RingBuffer;
use crate::ChunkQueue;
use std::ops::{Deref, DerefMut};

#[derive(Copy, Clone)]
pub struct SendPtr(*const u8);
unsafe impl Send for SendPtr {}
unsafe impl Sync for SendPtr {}
impl SendPtr {
    pub fn new(ptr: *const u8) -> Self {
        Self(ptr)
    }
    pub fn as_ptr(&self) -> *const u8 {
        self.0
    }
}

pub struct Chunk<'a> {
    pub ring_nr: u8,
    pub index: u64,
    pub data: &'a mut [u8],
}
impl<'a> Deref for Chunk<'a> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.data
    }
}
impl<'a> DerefMut for Chunk<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data
    }
}

pub struct ChunkRevolver {
    chunk_size: u64,
    rings: Vec<RingBuffer>,
    memory_blocks: Vec<Box<[u8]>>, // one memory block per thread/ring
    next_ring: usize,
}

impl ChunkRevolver {
    pub fn base_ptrs(&self) -> Vec<SendPtr> {
        (0..self.rings.len())
            .map(|thread_nr| SendPtr::new(self.base_ptr(thread_nr)))
            .collect()
    }
    pub fn new(config: &StrategicConfig) -> Self {
        let chunk_size = config.file_split_block_size_usize() as u64;
        let num_chunks = config.max_chunks as usize;
        let thread_count = config.max_core_in_flight as usize;

        let safe_thread_count = thread_count.min(num_chunks);
        let chunks_per_thread = num_chunks / safe_thread_count;

        let mut rings = Vec::with_capacity(safe_thread_count);
        let mut memory_blocks = Vec::with_capacity(safe_thread_count);

        for _ in 0..safe_thread_count {
            let mut ring = RingBuffer::new(chunks_per_thread);
            for i in 0..chunks_per_thread {
                ring.push(i as u64).expect("init ring overflow");
            }
            rings.push(ring);

            let block_size = chunks_per_thread * chunk_size as usize;
            let block = vec![0u8; block_size].into_boxed_slice();
            memory_blocks.push(block);
        }
        let next_ring=0;
        Self {
            chunk_size,
            rings,
            memory_blocks,
            next_ring
        }
    }

    pub fn try_get_chunk(&mut self) -> Option<Chunk> {
        let total_rings = self.rings.len();

        for i in 0..total_rings {
            let ring_nr = (self.next_ring + i) % total_rings;

            if let Some(index) = self.rings[ring_nr].pop() {
                let offset = index as usize * self.chunk_size as usize;
                let data = &mut self.memory_blocks[ring_nr][offset..offset + self.chunk_size as usize];
                self.next_ring = (ring_nr + 1) % total_rings;

                return Some(Chunk {
                    ring_nr: ring_nr as u8,
                    index,
                    data,
                });
            }
        }

        None
    }


    pub fn return_chunk(&mut self, thread_nr: u8, index: u64) {
        self.rings[thread_nr as usize]
            .push(index)
            .expect("Ring overflow on return");
    }

    /// Returns base pointer to this thread's memory block
    pub fn base_ptr(&self, thread_nr: usize) -> *const u8 {
        self.memory_blocks[thread_nr].as_ptr()
    }

    pub fn chunk_size(&self) -> usize {
        self.chunk_size as usize
    }
}

pub unsafe fn get_chunk_slice<'a>(
    base_ptr: *const u8,
    chunk_size: usize,
    chunk_index: u32,
    used: usize,
) -> &'a [u8] {
    let offset = chunk_index as usize * chunk_size;
    std::slice::from_raw_parts(base_ptr.add(offset), used)
}

pub fn split_into_microchunks<'a>(
    full_chunk: &'a [u8],
    micro_size: usize,
) -> Vec<&'a [u8]> {
    let mut microchunks = Vec::new();
    let mut offset = 0;
    while offset < full_chunk.len() {
        let end = (offset + micro_size).min(full_chunk.len());
        microchunks.push(&full_chunk[offset..end]);
        offset = end;
    }
    microchunks
}
