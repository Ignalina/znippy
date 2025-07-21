//! Reusable RingBuffer enum with fixed array backing and trait implementation
//! Reusable RingBuffer enum with fixed array backing and trait implementation
//!
//! Memory usage is estimated assuming 10MB per chunk.
//! Each thread gets its own RingBuffer. Total memory = SIZE × 10MB × NUM_THREADS (e.g. 32).
//! The total RAM consumption per size tier (for 32 threads) is approximately:
//
//! MINI_SIZE:         4    × 10MB × 32 ≈   1.25 GB
//! MEDIUM_SIZE:      32    × 10MB × 32 ≈  10.00 GB
//! LARGE_SIZE:      128    × 10MB × 32 ≈  40.00 GB
//! STORLEK_ENORM:  8192    × 10MB × 32 ≈ 2560.00 GB (~2.5 TB)

pub const MINI_SIZE: usize = 4;         // ~1.25 GB total at 10MB per chunk × 32 threads
pub const MEDIUM_SIZE: usize = 32;      // ~10.00 GB total
pub const LARGE_SIZE: usize = 128;      // ~40.00 GB total
pub const STORLEK_ENORM: usize = 8192;  // ~2.5 TB total
struct RingState {
    head: usize,
    tail: usize,
    len: usize,
}

struct RingInner<const N: usize> {
    buf: Box<[u64; N]>,
    state: RingState,
}

impl<const N: usize> RingInner<N> {
    fn new() -> Self {
        let mut array = Box::new([0u64; N]);
        for i in 0..(N - 1) {
            array[i] = i as u64;
        }        Self {
            buf: array,
            state: RingState {
                head: 0,
                tail: 0,
                len: N - 1, // one slot free for safety
            },
        }
    }

    fn pop(&mut self) -> Option<u64> {
        if self.state.len == 0 {
            None
        } else {
            let val = self.buf[self.state.tail];
            self.state.tail = (self.state.tail + 1) % N;
            self.state.len -= 1;
            Some(val)
        }
    }

    fn push(&mut self, val: u64) -> Result<(), &'static str> {
        if self.is_full() {
            return Err("RingBuffer overflow: push attempted while full");
        }
        self.buf[self.state.head] = val;
        self.state.head = (self.state.head + 1) % N;
        self.state.len += 1;
        Ok(())
    }
    fn is_empty(&self) -> bool {
        self.state.len == 0
    }

    fn is_full(&self) -> bool {
        self.state.len == N
    }

    fn capacity(&self) -> usize {
        N
    }
}

pub enum RingBuffer {
    Mini(RingInner<MINI_SIZE>),
    Medium(RingInner<MEDIUM_SIZE>),
    Large(RingInner<LARGE_SIZE>),
    StorlekEnorm(RingInner<STORLEK_ENORM>),
}

pub trait ChunkQueue {
    fn pop(&mut self) -> Option<u64>;
    fn push(&mut self, val: u64) -> Result<(), &'static str>;
    fn is_empty(&self) -> bool;
    fn is_full(&self) -> bool;
    fn capacity(&self) -> usize;
}

impl RingBuffer {
    pub fn new(max_chunks: usize) -> Self {
        if max_chunks <= MINI_SIZE {
            RingBuffer::Mini(RingInner::new())
        } else if max_chunks <= MEDIUM_SIZE {
            RingBuffer::Medium(RingInner::new())
        } else if max_chunks <= LARGE_SIZE {
            RingBuffer::Large(RingInner::new())
        } else {
            RingBuffer::StorlekEnorm(RingInner::new())
        }
    }
}

impl ChunkQueue for RingBuffer {
    fn pop(&mut self) -> Option<u64> {
        match self {
            RingBuffer::Mini(inner) => inner.pop(),
            RingBuffer::Medium(inner) => inner.pop(),
            RingBuffer::Large(inner) => inner.pop(),
            RingBuffer::StorlekEnorm(inner) => inner.pop(),
        }
    }

    fn push(&mut self, val: u64) -> Result<(), &'static str> {
        match self {
            RingBuffer::Mini(inner) => inner.push(val),
            RingBuffer::Medium(inner) => inner.push(val),
            RingBuffer::Large(inner) => inner.push(val),
            RingBuffer::StorlekEnorm(inner) => inner.push(val),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            RingBuffer::Mini(inner) => inner.is_empty(),
            RingBuffer::Medium(inner) => inner.is_empty(),
            RingBuffer::Large(inner) => inner.is_empty(),
            RingBuffer::StorlekEnorm(inner) => inner.is_empty(),
        }
    }

    fn is_full(&self) -> bool {
        match self {
            RingBuffer::Mini(inner) => inner.is_full(),
            RingBuffer::Medium(inner) => inner.is_full(),
            RingBuffer::Large(inner) => inner.is_full(),
            RingBuffer::StorlekEnorm(inner) => inner.is_full(),
        }
    }

    fn capacity(&self) -> usize {
        match self {
            RingBuffer::Mini(inner) => inner.capacity(),
            RingBuffer::Medium(inner) => inner.capacity(),
            RingBuffer::Large(inner) => inner.capacity(),
            RingBuffer::StorlekEnorm(inner) => inner.capacity(),
        }
    }
}