//! znippy-zoomies — fast-as-hell building blocks extracted from katana-osm.
//!
//! - [`vtd`]: VTD-style parallel OSM-XML scanner producing a compact `ElemIndex`
//!   (byte offsets into the file), plus zone-map summaries and byte-level attr parsers.
//! - [`stree`]: Ragnar Groot Koerkamp's static search tree over sorted `i64` keys
//!   (AVX2), including an mmap-backed variant whose leaf layer IS the sorted file.
//! - [`chunk_revolver`]: zero-allocation slot pool for 1-reader / N-worker streaming.
//! - [`gatling`]: generic no-barrier worker-pool engine (split → N decode → in-order
//!   collect → sink), parameterised by a [`gatling::Codec`] + [`gatling::Sink`].
//! - [`psort`]: parallel sample sort (and reference LSD radix) over 16-byte AoS
//!   records keyed by their leading `i64`.

pub mod chunk_revolver;
pub mod gatling;
pub mod psort;
pub mod stree;
pub mod vtd;
