// znippy-common/src/chunkrevolver.rs

use crate::common_config::CONFIG;
use crate::int_ring::RingBuffer;
use std::ops::{Deref, DerefMut};
use crate::ChunkQueue;
/// En wrapper runt *const u8 som är markerad som Send
pub struct SendPtr(*const u8);

unsafe impl Send for SendPtr {}
unsafe impl Sync for SendPtr {} // om du även vill dela den till flera trådar

impl SendPtr {
    pub fn new(ptr: *const u8) -> Self {
        Self(ptr)
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.0
    }
}
/// En hanterad referens till en chunk i ChunkRevolver.
pub struct RevolverChunk<'a> {
    pub index: u32,
    pub data: &'a mut [u8],
}

impl<'a> Deref for RevolverChunk<'a> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.data
    }
}

impl<'a> DerefMut for RevolverChunk<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data
    }
}

/// En chunk-revolver: förallokerat minnesblock delat i fasta slices, återanvänds via ring.
pub struct ChunkRevolver {
    memory: Box<[u8]>,
    chunk_size: usize,
    ring: RingBuffer,
}

impl ChunkRevolver {
    pub fn new() -> Self {
        let chunk_size = CONFIG.file_split_block_size_usize();
        let num_chunks = CONFIG.max_chunks as usize;
        let total_size = chunk_size * num_chunks;

        let memory = vec![0u8; total_size].into_boxed_slice();
        let mut ring = RingBuffer::new(num_chunks);
        for i in 0..num_chunks as u32 {
            ring.push(i);
        }

        Self {
            memory,
            chunk_size,
            ring,
        }
    }

    /// Få nästa tillgängliga chunk som en mutbar slice.
    pub fn get_chunk(&mut self) -> RevolverChunk {
        let index = self.ring.pop().expect("ChunkRevolver underrun");
        let offset = index as usize * self.chunk_size;
        let data = &mut self.memory[offset..offset + self.chunk_size];

        RevolverChunk { index, data }
    }

    /// Returnera en chunk för återanvändning.
    pub fn return_chunk(&mut self, index: u32) {
        self.ring.push(index);
    }

    /// Returnerar en råpekare till start av memoryblocket (för användning i andra trådar).
    pub fn base_ptr(&self) -> *const u8 {
        self.memory.as_ptr()
    }

    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }
}

/// Hjälpfunktion för att hämta en slice med `chunk_index`, `used`, och råpekare
pub unsafe fn get_chunk_slice<'a>(
    base_ptr: *const u8,
    chunk_size: usize,
    chunk_index: u32,
    used: usize,
) -> &'a [u8] {
    let offset = chunk_index as usize * chunk_size;
    std::slice::from_raw_parts(base_ptr.add(offset), used)
}
