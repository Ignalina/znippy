# znippy-zoomies

Fast-as-hell building blocks extracted from [katana-osm](https://github.com/Ignalina):

- **`vtd`** — VTD-style parallel OSM-XML scanner producing a compact `ElemIndex`
  (byte offsets into the file) plus zone-map summaries and byte-level attribute
  parsers. Streams or mmaps; bounded RAM from regional extracts to planet.
- **`stree`** — static search tree over sorted `i64` keys (AVX2), with an
  mmap-backed variant (`STree64Mmap`) whose leaf layer *is* the sorted file.
  Derived from [Ragnar Groot Koerkamp's `static-search-tree`](https://github.com/RagnarGrootKoerkamp/static-search-tree) (MIT).
- **`chunk_revolver`** — zero-allocation slot pool for 1-reader / N-worker
  streaming pipelines (workers borrow each slot as `&[u8]`, no copy).

## License

MIT. The `stree` module retains Ragnar Groot Koerkamp's copyright notice
(see `LICENSE`).
