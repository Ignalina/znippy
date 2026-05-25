//! Zero-allocation slot pool for 1-reader / N-worker streaming pipelines.
//!
//! Moved verbatim into znippy-zoomies from the former standalone `chunk-revolver`
//! crate. One reader thread fills pre-allocated fixed-size slots; N workers borrow
//! each slot as `&[u8]` (zero-copy) via [`get_chunk_slice`] and process it.
#![allow(unsafe_code, reason = "raw pointer slot pool — safety documented per site")]

mod revolver;
mod ring;

pub use revolver::{Chunk, ChunkRevolver, SendPtr, get_chunk_slice};
pub use ring::{HUGE, LARGE, MEDIUM, MINI};
