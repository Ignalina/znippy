//! Reusable RingBuffer enum with fixed array backing and trait implementation

pub const MINI_SIZE: usize = 64; //  ~1G * 0.75 / 10MB
pub const MEDIUM_SIZE: usize = 512; // ~8GB * 0.75 / 10MB
pub const LARGE_SIZE: usize = 2048; // ~32GB * 0.75 / 10MB 
pub const STORLEK_ENORM: usize = 256_000; // ~2.44 TB @ 10MB per chunk

struct RingState {
    head: usize,
    tail: usize,
    len: usize,
}

struct RingInner<const N: usize> {
    buf: Box<[u32; N]>,
    state: RingState,
}

impl<const N: usize> RingInner<N> {
    fn new() -> Self {
        let mut array = Box::new([0u32; N]);
        for i in 0..N {
             array[i] = i as u32;
        }
        Self {
            buf: array,
            state: RingState {
                head: 0,
                tail: 0,
                len: N,
            },
        }
    }

    fn pop(&mut self) -> Option<u32> {
        if self.state.len == 0 {
            None
        } else {
            let val = self.buf[self.state.tail];
            self.state.tail = (self.state.tail + 1) % N;
            self.state.len -= 1;
            Some(val)
        }
    }

    fn push(&mut self, val: u32) {
        self.buf[self.state.head] = val;
        self.state.head = (self.state.head + 1) % N;
        self.state.len += 1;
    }

    fn is_empty(&self) -> bool {
        self.state.len == 0
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
    fn pop(&mut self) -> Option<u32>;
    fn push(&mut self, val: u32);
    fn is_empty(&self) -> bool;
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
    fn pop(&mut self) -> Option<u32> {
        match self {
            RingBuffer::Mini(inner) => inner.pop(),
            RingBuffer::Large(inner) => inner.pop(),
            RingBuffer::Medium(inner) => inner.pop(),
            RingBuffer::StorlekEnorm(inner) => inner.pop(),
        }
    }

    fn push(&mut self, val: u32) {
        match self {
            RingBuffer::Mini(inner) => inner.push(val),
            RingBuffer::Large(inner) => inner.push(val),
            RingBuffer::Medium(inner) => inner.push(val),
            RingBuffer::StorlekEnorm(inner) => inner.push(val),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            RingBuffer::Mini(inner) => inner.is_empty(),
            RingBuffer::Large(inner) => inner.is_empty(),
            RingBuffer::Medium(inner) => inner.is_empty(),
            RingBuffer::StorlekEnorm(inner) => inner.is_empty(),
        }
    }

    fn capacity(&self) -> usize {
        match self {
            RingBuffer::Mini(inner) => inner.capacity(),
            RingBuffer::Large(inner) => inner.capacity(),
            RingBuffer::Medium(inner) => inner.capacity(),
            RingBuffer::StorlekEnorm(inner) => inner.capacity(),
        }
    }
}

