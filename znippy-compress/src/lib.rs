// compress/src/lib.rs

pub mod packer;
pub mod stream_packer;

pub use packer::compress_dir;
pub use stream_packer::{compress_stream, ArchiveEntry, StreamCompressor};

/// Blob passed from compressor threads to the writer thread.
///
/// `Owned` carries the compressed bytes (already a fresh allocation from cctx.compress —
/// wrapping in Arc<[u8]> would copy them a second time, so we don't).
///
/// `Revolver` is a zero-copy reference into a chunk-revolver slot.  The slot stays live
/// until the *writer* calls tx_ret.send((ring_nr, chunk_nr)); compressor threads must NOT
/// call tx_ret for Revolver blobs.
pub(crate) enum Blob {
    Owned(Vec<u8>),
    Revolver { ptr: usize, len: usize, ring_nr: u8, chunk_nr: u64 },
}

// Safety: Revolver holds a raw pointer into the ChunkRevolver's mmapped/heap region.
// The region is valid until tx_ret.send is called, which happens only after write_all
// in the writer thread.  No other thread touches the slot in the interim.
unsafe impl Send for Blob {}

impl Blob {
    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            Blob::Owned(v) => v,
            Blob::Revolver { ptr, len, .. } => unsafe {
                std::slice::from_raw_parts(*ptr as *const u8, *len)
            },
        }
    }
    pub(crate) fn len(&self) -> usize {
        match self {
            Blob::Owned(v) => v.len(),
            Blob::Revolver { len, .. } => *len,
        }
    }
}
