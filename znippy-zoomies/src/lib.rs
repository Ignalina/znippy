//! znippy-zoomies — fast-as-hell building blocks extracted from katana-osm.
//!
//! - [`vtd`]: VTD-style parallel OSM-XML scanner producing a compact `ElemIndex`
//!   (byte offsets into the file), plus zone-map summaries and byte-level attr parsers.
//! - [`stree`]: Ragnar Groot Koerkamp's static search tree over sorted `i64` keys
//!   (AVX2), including an mmap-backed variant whose leaf layer IS the sorted file.
//! - [`chunk_revolver`]: zero-allocation slot pool for 1-reader / N-worker streaming.

pub mod chunk_revolver;
pub mod stree;
pub mod vtd;
