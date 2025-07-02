//! Multithreaded file compressor using preallocated chunk buffers and zero-copy RingBuffer coordination.

use snippy_common::{RingBuffer, ChunkQueue};
use crossbeam_channel::{bounded, unbounded};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::sync::Arc;
use crate::config::CONFIG;

pub fn run_packer(all_files: Vec<PathBuf>) -> anyhow::Result<()> {
    let chunk_size = CONFIG.file_split_block_size as usize;
    let max_chunks = CONFIG.max_chunks;

    // Preallocate chunk buffers
    let buffers: Vec<Arc<Box<[u8]>>> = (0..max_chunks)
        .map(|_| Arc::new(vec![0u8; chunk_size].into_boxed_slice()))
        .collect();

    let (tx_chunk, rx_chunk) = unbounded();
    let (tx_free, rx_free) = bounded::<u32>(max_chunks);
    let mut free_ring = RingBuffer::new(max_chunks);

    // Fill initial free chunk IDs
    for i in 0..max_chunks as u32 {
        free_ring.push(i);
    }

    let reader_buffers = buffers.clone();
    let reader_tx_chunk = tx_chunk.clone();
    let reader_tx_free = tx_free.clone();
    let reader_rx_free = rx_free.clone();

    std::thread::spawn(move || {
        for (index, path) in all_files.iter().enumerate() {
            let file = match File::open(path) {
                Ok(f) => f,
                Err(_) => continue,
            };
            let mut reader = BufReader::new(file);
            let mut offset = 0u64;

            loop {
                let chunk_nr = loop {
                    if let Some(nr) = free_ring.pop() {
                        break nr;
                    } else if let Ok(nr) = reader_rx_free.recv() {
                        free_ring.push(nr);
                    }
                };

                let buf_arc = &reader_buffers[chunk_nr as usize];
                let buf = Arc::get_mut(buf_arc).unwrap();

                let n = match reader.read(buf) {
                    Ok(n) if n > 0 => n,
                    _ => break,
                };

                let data_arc = Arc::clone(buf_arc);
                reader_tx_chunk.send((index, offset, chunk_nr, data_arc)).unwrap();
                offset += n as u64;
            }
        }
    });

    // Compressor thread example (could use thread pool)
    let compressor_tx_free = tx_free.clone();
    std::thread::spawn(move || {
        while let Ok((index, offset, chunk_nr, data_arc)) = rx_chunk.recv() {
            // TODO: compress `data_arc[..]` into some output
            // Example only:
            // let compressed = zstd_compress(&data_arc);
            // write_compressed(index, offset, &compressed);

            compressor_tx_free.send(chunk_nr).unwrap();
        }
    });

    Ok(())
}
