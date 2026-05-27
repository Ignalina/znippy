//! Gatling — generic no-barrier worker-pool streaming engine.
//!
//! Extracted from katana-osm's `xml_to_pbf_bz2` (the pipeline behind its planet /
//! Sweden benchmarks). The engine carries **no** codec / format knowledge — the
//! caller plugs in a [`Codec`] (how to split a buffer into independent decode units
//! and decode one) and a [`Sink`] (how to find a safe boundary in the decoded
//! stream and process a contiguous run). bz2 / XML / PBF / Arrow specifics live in
//! the consumer crate.
//!
//! ```text
//! Reader ─fill slot─► Main (carry + split) ─► N decode Workers (NO barrier)
//!   ▲                                                    │
//!   │                                                    ▼
//!   └──────── slot recycle ◄──── Collector (in-order assemble + Sink)
//! ```
//!
//! Properties preserved from the reference implementation:
//! - **Zero-copy decode:** compressed bytes stay in the slot; workers get a raw
//!   pointer into the in-flight slot (no per-segment copy).
//! - **No barrier:** slot N+1 is split and dispatched while slot N is still
//!   decoding/processing.
//! - **Carry in headroom:** the compressed tail not consumed by `split` is copied
//!   into reserved slot headroom, never a growing per-chunk `Vec`.

use std::io::Read;
use std::sync::{mpsc, Arc, Mutex};

use anyhow::Result;

/// Result of splitting a compressed buffer into independent decode units.
pub struct Split<S> {
    /// One descriptor per decode unit, in stream order.
    pub segments: Vec<S>,
    /// Number of bytes consumed from the input slice; the remainder becomes carry.
    pub consumed: usize,
}

/// Decompressor plug-in. `Seg` is an opaque, `Copy` per-unit descriptor (e.g. a
/// bz2 bit range) computed by `split` and handed back to `decode`.
pub trait Codec: Sync {
    /// Per-decode-unit descriptor. Must be cheap to copy and `Send`.
    type Seg: Send + Copy + 'static;

    /// Find independent decode units in `data`. Return `None` when no complete
    /// unit is available yet (the caller grows the carry and retries next chunk).
    /// `is_last` is true for the final chunk of the stream.
    fn split(&self, data: &[u8], n_workers: usize, is_last: bool) -> Option<Split<Self::Seg>>;

    /// Decode one unit of `data` into its uncompressed bytes.
    fn decode(&self, data: &[u8], seg: &Self::Seg) -> Vec<u8>;
}

/// Consumer plug-in. Methods run on the engine's single collector thread, so a
/// `Sink` may safely own mutable state and fan work out to its own writer thread.
pub trait Sink: Send {
    /// Largest prefix length of `assembled` (decoded bytes) that ends on a safe
    /// boundary (e.g. the end of an XML element). Return `0` if none yet; return
    /// `assembled.len()` when `is_last` to flush everything.
    fn safe_end(&self, assembled: &[u8], is_last: bool) -> usize;

    /// Process one contiguous decoded run, already cut at a safe boundary.
    fn process(&mut self, bytes: &[u8]) -> Result<()>;
}

/// Engine tuning. `slot size = carry_headroom + chunk_size`.
pub struct Config {
    /// Compressed bytes read per slot.
    pub chunk_size: usize,
    /// Bytes reserved at the head of each slot for prepended carry.
    pub carry_headroom: usize,
    /// Slot-pool depth (slots in flight).
    pub ring_slots: usize,
    /// Bytes to prepend to the first chunk (e.g. a stream header already read).
    pub initial_carry: Vec<u8>,
}

// ── Internal message types ──────────────────────────────────────────────────

struct WorkItem<S> {
    chunk_id: u64,
    seg_id: usize,
    data_ptr: *const u8,
    data_len: usize,
    seg: S,
}
// SAFETY: `data_ptr` points into a slot held alive in `InFlight` by the collector
// until every segment of that chunk has been decoded and the slot recycled. The
// reader never overwrites a slot while it is in flight (it is removed from the
// free pool). `S: Send` covers the descriptor payload.
unsafe impl<S: Send> Send for WorkItem<S> {}

struct SegResult {
    chunk_id: u64,
    seg_id: usize,
    output: Vec<u8>,
}

struct InFlight {
    chunk_id: u64,
    slot: Vec<u8>,
    n_segments: usize,
    results: Vec<Option<Vec<u8>>>,
    done: usize,
    is_last: bool,
}

enum Msg {
    New(InFlight),
    Result(SegResult),
}

// ── Engine ──────────────────────────────────────────────────────────────────

/// Run the engine to completion over `reader`, driving `sink`.
///
/// `sink` is borrowed mutably for the duration; counts / outputs the consumer
/// accumulates are read from it after `run` returns (e.g. via a `finish` method
/// of your own). `codec` is shared across the decode workers.
pub fn run<C: Codec, S: Sink>(
    reader: impl Read + Send,
    codec: C,
    sink: &mut S,
    n_workers: usize,
    cfg: Config,
) -> Result<()> {
    let n_workers = n_workers.max(1);
    let slot_size = cfg.carry_headroom + cfg.chunk_size;
    assert!(
        cfg.initial_carry.len() <= cfg.carry_headroom,
        "initial_carry {} exceeds carry_headroom {}",
        cfg.initial_carry.len(),
        cfg.carry_headroom
    );

    let codec_ref = &codec;
    let sink_ref: &mut S = sink;

    std::thread::scope(|s| -> Result<()> {
        // ── Slot pool (lazy) ──────────────────────────────────────────────────
        // Slots are allocated on demand, capped at `ring_slots`. A small input only
        // ever touches one slot, so it never pays the full `ring_slots × slot_size`
        // up front (a 5 MB file used to zero 6 × 232 MB = 1.4 GB before reading a byte).
        let (slot_return_tx, slot_return_rx) = mpsc::sync_channel::<Vec<u8>>(cfg.ring_slots);

        // ── Reader thread ─────────────────────────────────────────────────────
        let (filled_tx, filled_rx) =
            mpsc::sync_channel::<(Vec<u8>, usize, bool)>(cfg.ring_slots);
        let chunk_size = cfg.chunk_size;
        let carry_headroom = cfg.carry_headroom;
        let ring_slots = cfg.ring_slots;
        s.spawn(move || {
            use std::sync::mpsc::TryRecvError;
            let mut src = reader;
            let mut allocated = 0usize;
            loop {
                // Reuse a recycled slot if one is waiting; else allocate a fresh
                // slot while under budget; else block for a slot to come back.
                let mut slot = match slot_return_rx.try_recv() {
                    Ok(sl) => sl,
                    Err(TryRecvError::Empty) if allocated < ring_slots => {
                        allocated += 1;
                        let mut sl = Vec::with_capacity(slot_size);
                        sl.resize(slot_size, 0);
                        sl
                    }
                    Err(TryRecvError::Empty) => match slot_return_rx.recv() {
                        Ok(sl) => sl,
                        Err(_) => break,
                    },
                    Err(TryRecvError::Disconnected) => break,
                };
                let mut got = 0usize;
                while got < chunk_size {
                    match src.read(&mut slot[carry_headroom + got..slot_size]) {
                        Ok(0) => break,
                        Ok(k) => got += k,
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(_) => break,
                    }
                }
                let is_last = got < chunk_size;
                if filled_tx.send((slot, got, is_last)).is_err() {
                    break;
                }
                if is_last {
                    break;
                }
            }
        });

        // ── Decode workers (persistent, no barrier) ───────────────────────────
        let (work_tx, work_rx) = mpsc::sync_channel::<WorkItem<C::Seg>>(n_workers * 2);
        let (collector_tx, collector_rx) = mpsc::sync_channel::<Msg>(n_workers * 4);
        let work_rx = Arc::new(Mutex::new(work_rx));

        for _ in 0..n_workers {
            let work_rx = Arc::clone(&work_rx);
            let collector_tx = collector_tx.clone();
            s.spawn(move || {
                loop {
                    let item = {
                        let rx = work_rx.lock().expect("work_rx lock");
                        match rx.recv() {
                            Ok(item) => item,
                            Err(_) => break,
                        }
                    };
                    // SAFETY: the slot backing `data_ptr` is held in `InFlight`
                    // by the collector and not recycled until all its segments
                    // are decoded, so the pointer is valid for this read.
                    let data = unsafe {
                        std::slice::from_raw_parts(item.data_ptr, item.data_len)
                    };
                    let output = codec_ref.decode(data, &item.seg);
                    let _ = collector_tx.send(Msg::Result(SegResult {
                        chunk_id: item.chunk_id,
                        seg_id: item.seg_id,
                        output,
                    }));
                }
            });
        }
        drop(work_rx);

        // Main keeps an inflight sender; workers keep their clones.
        let inflight_tx = collector_tx.clone();
        drop(collector_tx);

        // ── Collector thread ──────────────────────────────────────────────────
        let slot_return_for_collector = slot_return_tx.clone();
        let collector = s.spawn(move || -> Result<()> {
            let mut in_flight: Vec<InFlight> = Vec::new();
            let mut next_id: u64 = 0;
            let mut carry: Vec<u8> = Vec::new();

            while let Ok(msg) = collector_rx.recv() {
                match msg {
                    Msg::New(slot) => in_flight.push(slot),
                    Msg::Result(r) => {
                        for slot in in_flight.iter_mut() {
                            if slot.chunk_id == r.chunk_id {
                                slot.results[r.seg_id] = Some(r.output);
                                slot.done += 1;
                                break;
                            }
                        }
                    }
                }

                // Flush completed slots strictly in stream order.
                loop {
                    let Some(idx) = in_flight.iter().position(|s| s.chunk_id == next_id) else {
                        break;
                    };
                    if in_flight[idx].done < in_flight[idx].n_segments {
                        break;
                    }

                    let mut done = in_flight.remove(idx);
                    let is_last = done.is_last;

                    let mut buf = std::mem::take(&mut carry);
                    for seg in done.results.drain(..) {
                        if let Some(bytes) = seg {
                            buf.extend_from_slice(&bytes);
                        }
                    }

                    let safe = sink_ref.safe_end(&buf, is_last);
                    if safe == 0 {
                        carry = buf;
                    } else {
                        carry = buf[safe..].to_vec();
                        buf.truncate(safe);
                        sink_ref.process(&buf)?;
                    }

                    slot_return_for_collector.send(done.slot).ok();
                    next_id += 1;
                }
            }

            if !carry.is_empty() {
                sink_ref.process(&carry)?;
            }
            Ok(())
        });

        // ── Main: carry + split + dispatch ────────────────────────────────────
        // A zero-read chunk whose carry has not grown past the seed length means
        // the stream is finished (the carry holds only the prepended header).
        let carry_floor = cfg.initial_carry.len();
        let mut carry: Vec<u8> = cfg.initial_carry;
        let mut chunk_id: u64 = 0;

        for (mut slot, read_len, is_last) in filled_rx.iter() {
            if read_len == 0 && carry.len() <= carry_floor {
                slot_return_tx.send(slot).ok();
                break;
            }

            let carry_len = carry.len();
            assert!(
                carry_len <= carry_headroom,
                "carry {carry_len} exceeds headroom {carry_headroom}"
            );
            let data_start = carry_headroom - carry_len;
            slot[data_start..carry_headroom].copy_from_slice(&carry);
            let data_end = carry_headroom + read_len;

            let data = &slot[data_start..data_end];
            let split = match codec_ref.split(data, n_workers, is_last) {
                Some(sp) => sp,
                None => {
                    carry.clear();
                    carry.extend_from_slice(data);
                    slot_return_tx.send(slot).ok();
                    if is_last {
                        break;
                    }
                    continue;
                }
            };

            carry.clear();
            carry.extend_from_slice(&data[split.consumed..]);

            let n_segments = split.segments.len();
            let inflight = InFlight {
                chunk_id,
                slot,
                n_segments,
                results: (0..n_segments).map(|_| None).collect(),
                done: 0,
                is_last,
            };

            // Pointer into the slot before it is moved into the collector.
            let data_ptr = inflight.slot[data_start..].as_ptr();
            let data_len = data_end - data_start;

            inflight_tx
                .send(Msg::New(inflight))
                .map_err(|_| anyhow::anyhow!("collector closed"))?;

            for (seg_id, seg) in split.segments.into_iter().enumerate() {
                work_tx
                    .send(WorkItem { chunk_id, seg_id, data_ptr, data_len, seg })
                    .map_err(|_| anyhow::anyhow!("work channel closed"))?;
            }

            chunk_id += 1;
            if is_last {
                break;
            }
        }

        drop(work_tx);
        drop(inflight_tx);
        drop(slot_return_tx);

        collector.join().expect("collector panicked")?;
        Ok(())
    })
}
