use znippy_common::chunkrevolver::ChunkRevolver;
use znippy_common::common_config::CONFIG;
use std::collections::HashSet;
use znippy_common::RingBuffer;
use znippy_common::ChunkQueue;
#[test]
fn test_try_get_chunk_exhaustion() {
    let mut revolver = ChunkRevolver::new(&CONFIG);
    let max = CONFIG.max_chunks;

    let mut chunks = Vec::new();

    // Försök att ta ut max antal chunkar
    for _ in 0..max {
        let chunk = revolver.try_get_chunk();
        assert!(chunk.is_some(), "Expected Some(chunk), got None before exhaustion");
        chunks.push(chunk.unwrap().index);
    }

    // Alla chunkar ska nu vara tagna – nästa anrop ska ge None
    let extra = revolver.try_get_chunk();
    assert!(extra.is_none(), "Expected None when all chunks are in use");
}
#[test]
fn test_chunk_revolver_overflow() {
    let mut revolver = ChunkRevolver::new(&CONFIG);
    let mut used = HashSet::new();

    // Ta alla chunks
    for _ in 0..CONFIG.max_chunks {
        if let Some(chunk) = revolver.try_get_chunk() {
            assert!(used.insert(chunk.index), "Duplicate chunk index {}", chunk.index);
        } else {
            panic!("Expected chunk but got None");
        }
    }

    // Revolvern ska nu vara tom
    assert!(revolver.try_get_chunk().is_none(), "Expected None but got Some");

    // Returnera en och ta ut den igen
    revolver.return_chunk(0);
    let c = revolver.try_get_chunk().expect("Expected chunk after return");
    assert_eq!(c.index, 0, "Expected returned chunk index to be reused");

    // 🧨 Dubbel return — om detta inte skyddas kommer felaktig återanvändning ske
    revolver.return_chunk(0); // borde orsaka problem/logikfel i framtida anrop
}
#[test]
fn test_try_get_chunk_no_overflow() {
    let mut revolver = ChunkRevolver::new(&CONFIG);
    let max_chunks = CONFIG.max_chunks;

    // Hämta exakt så många chunkar som finns
    let mut chunks = Vec::with_capacity(max_chunks as usize);
    for _ in 0..max_chunks {
        let chunk = revolver.try_get_chunk();
        assert!(chunk.is_some(), "Expected Some(chunk), got None before max_chunks");
        chunks.push(chunk.unwrap().index);
    }

    // Nu ska det inte finnas fler
    let extra = revolver.try_get_chunk();
    assert!(extra.is_none(), "Expected None after all chunks taken");
}
#[test]
fn test_return_and_reuse_chunk() {
    let mut revolver = ChunkRevolver::new(&CONFIG);
    let max_chunks = CONFIG.max_chunks;

    // Ta ut en chunk
    let c1 = revolver.try_get_chunk().unwrap();
    let index1 = c1.index;

    // Returnera den
    revolver.return_chunk(index1);

    // Hämta en ny (kan vara samma eller annan beroende på ringordning)
    let c2 = revolver.try_get_chunk().unwrap();
    let index2 = c2.index;

    // Den återanvända måste vara inom 0..max_chunks
    assert!(index2 < max_chunks as u64);

    // Valfritt: kontrollera att man totalt inte får fler än max_chunks
    let mut all_indices = vec![index2];
    while let Some(c) = revolver.try_get_chunk() {
        all_indices.push(c.index);
    }
    all_indices.sort();
    all_indices.dedup();
    assert_eq!(all_indices.len(), max_chunks as usize, "More unique chunks than allowed");
}

#[test]
fn test_ring_buffer_one_slot_safety() {
    let mut revolver = ChunkRevolver::new(&CONFIG);
    let max = CONFIG.max_chunks;

    let mut used = 0;

    for _ in 0..max {
        if revolver.try_get_chunk().is_some() {
            used += 1;
        } else {
            break;
        }
    }

    assert_eq!(
        used,
        max - 1,
        "Expected to get max - 1 chunks due to 1-slot safety, got {}",
        used
    );
}

#[test]
fn test_return_chunks_out_of_order() {

    let mut ring = RingBuffer::new(64);    let capacity = ring.capacity();

    let mut taken = vec![];

    // Ta ut alla chunkar
    for _ in 0..capacity {
        let val = ring.pop();
        assert!(val.is_some());
        taken.push(val.unwrap());
    }

    // Nu bör det vara tomt
    assert!(ring.pop().is_none());

    // Returnera chunkar i omvänd ordning
    for &val in taken.iter().rev() {
        ring.push(val).unwrap();
    }

    // Plocka ut igen – bör få exakt samma antal
    let mut seen = std::collections::HashSet::new();
    for _ in 0..capacity {
        let val = ring.pop();
        assert!(val.is_some());
        let v = val.unwrap();
        assert!(
            seen.insert(v),
            "Duplicate chunk detected: {v} – chunk reused too early!"
        );
    }

    // Ska vara tomt igen
    assert!(ring.pop().is_none());
}
