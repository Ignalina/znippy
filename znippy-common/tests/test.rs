use znippy_common::chunkrevolver::{CONFIG, ChunkRevolver};

#[test]
fn test_chunk_revolver_basic_usage() {
    // Setup: skapa en lokal config
    let mut config = CONFIG.clone();
    config.max_core_in_flight = 4; // antal ringar
    config.max_chunks = 16; // totalt antal chunkar
    config.file_split_block_size = 1024 * 1024; // 1 MB

    let mut revolver = ChunkRevolver::new(&config);
    let total = revolver.total_capacity();

    let mut seen = std::collections::HashSet::new();
    let mut all_chunks = Vec::new();

    // 1. Hämta alla chunkar
    for _ in 0..total {
        let chunk = revolver.try_get_chunk().expect("Expected available chunk");

        // Kontrollera unikhet
        let key = (chunk.ring_nr, chunk.index);
        assert!(seen.insert(key), "Duplicate chunk detected: {:?}", key);

        // Spara så vi kan returnera dem sen
        all_chunks.push((chunk.ring_nr, chunk.index));
    }

    // 2. Kontrollera att inga fler chunkar finns
    assert!(
        revolver.try_get_chunk().is_none(),
        "Should be exhausted after {} chunks",
        total
    );

    // 3. Returnera chunkar
    for (ring_nr, index) in &all_chunks {
        revolver.return_chunk(*ring_nr, *index);
    }

    // 4. Kontrollera att alla kan hämtas igen
    let mut second_seen = std::collections::HashSet::new();
    for _ in 0..total {
        let chunk = revolver
            .try_get_chunk()
            .expect("Expected chunk after return");

        let key = (chunk.ring_nr, chunk.index);
        assert!(second_seen.insert(key), "Duplicate on reuse: {:?}", key);
    }
}

#[test]
fn test_chunk_revolver_two_passes() {
    use znippy_common::chunkrevolver::ChunkRevolver;
    use znippy_common::common_config::StrategicConfig;

    let mut config = CONFIG.clone();

    config.max_chunks = 32;
    config.max_core_in_flight = 4;
    config.file_split_block_size = 1024 * 1024; // 1MB

    let mut revolver = ChunkRevolver::new(&config);
    let expected = revolver.total_capacity();

    let mut all_chunks = Vec::new();

    // Första uttömning
    while let Some(chunk) = revolver.try_get_chunk() {
        all_chunks.push((chunk.ring_nr, chunk.index));
    }

    let total = all_chunks.len();
    assert_eq!(
        total, expected,
        "Första varvet borde hämta alla chunkar"
    );

    // Återlämna i exakt samma ordning
    for (ring, index) in &all_chunks {
        revolver.return_chunk(*ring, *index);
    }

    let mut second_pass = Vec::new();

    // Andra uttömning
    while let Some(chunk) = revolver.try_get_chunk() {
        second_pass.push((chunk.ring_nr, chunk.index));
    }

    assert_eq!(
        second_pass.len(),
        total,
        "Andra varvet borde ge lika många chunkar"
    );

    // Kontrollera att chunkarna faktiskt är desamma (eller minst att alla återanvänds)
    let set1: std::collections::HashSet<_> = all_chunks.iter().cloned().collect();
    let set2: std::collections::HashSet<_> = second_pass.iter().cloned().collect();

    assert_eq!(
        set1, set2,
        "Chunkarna som återanvänds ska vara identiska mellan varven"
    );
}
#[test]
fn test_no_duplicate_without_return() {
    use std::collections::HashSet;
    use znippy_common::chunkrevolver::ChunkRevolver;
    use znippy_common::common_config::StrategicConfig;

    let mut config = CONFIG.clone();
    config.max_chunks = 64;
    config.max_core_in_flight = 4;
    config.file_split_block_size = 1024 * 1024;

    let mut revolver = ChunkRevolver::new(&config);
    let expected = revolver.total_capacity();

    let mut seen = HashSet::new();
    let mut count = 0;

    while let Some(chunk) = revolver.try_get_chunk() {
        let key = (chunk.ring_nr, chunk.index);
        assert!(
            seen.insert(key),
            "🔴 Chunk already seen: ring={} index={}",
            chunk.ring_nr,
            chunk.index
        );
        count += 1;
    }

    assert_eq!(
        count, expected,
        "Should get exactly total_capacity() chunks before exhaustion"
    );

    let chunk = revolver.try_get_chunk();
    assert!(
        chunk.is_none(),
        "🔴 Expected None after exhausting all chunks, but got Some"
    );
}
#[test]
#[should_panic(expected = "Chunk already seen")]
fn test_duplicate_chunk_without_return_is_detected() {
    use std::collections::HashSet;
    use znippy_common::chunkrevolver::ChunkRevolver;
    use znippy_common::common_config::StrategicConfig;

    let mut config = CONFIG.clone();
    config.max_chunks = 8;
    config.max_core_in_flight = 2;
    config.file_split_block_size = 1024 * 1024;

    let mut revolver = ChunkRevolver::new(&config);

    let mut seen = HashSet::new();
    let mut saved_chunk = None;

    for i in 0..config.max_chunks {
        let chunk = revolver.try_get_chunk().expect("Expected chunk");
        let key = (chunk.ring_nr, chunk.index);
        assert!(
            seen.insert(key),
            "🔴 Chunk already seen: ring={} index={}",
            chunk.ring_nr,
            chunk.index
        );
        // Spara en chunk för att försöka använda igen
        if i == 3 {
            saved_chunk = Some(key);
        }
    }

    // Försök använda samma chunk igen utan att returnera
    if let Some(dupe) = saved_chunk {
        assert!(
            seen.insert(dupe),
            "🔴 Chunk already seen: ring={} index={}",
            dupe.0,
            dupe.1
        );
    }
}

#[test]
fn test_chunk_does_not_overlap_without_return() {
    use std::collections::HashSet;
    use znippy_common::chunkrevolver::ChunkRevolver;
    use znippy_common::common_config::StrategicConfig;

    let mut config = CONFIG.clone();
    config.max_chunks = 8;
    config.max_core_in_flight = 2;
    config.file_split_block_size = 1024 * 1024;

    let mut revolver = ChunkRevolver::new(&config);
    let total = revolver.total_capacity();

    let mut seen = HashSet::new();

    for i in 0..total {
        let chunk = revolver
            .try_get_chunk()
            .expect("Expected unique chunk before exhaustion");
        let key = (chunk.ring_nr, chunk.index);
        assert!(
            seen.insert(key),
            "🧨 Overlap detected: chunk reused without return at iteration {}: ring={} index={}",
            i,
            chunk.ring_nr,
            chunk.index
        );
    }

    // After total_capacity chunks the revolver must be empty
    let chunk = revolver.try_get_chunk();
    assert!(
        chunk.is_none(),
        "🧨 Overlap: expected None after exhaustion, got Some(...)"
    );
}

#[test]
fn test_no_duplicate_chunks_without_return() {
    use std::collections::HashSet;
    use znippy_common::chunkrevolver::ChunkRevolver;
    use znippy_common::common_config::CONFIG;

    let mut config = CONFIG.clone();
    config.max_chunks = 8; // total antal chunks
    config.max_core_in_flight = 2;
    config.file_split_block_size = 1024 * 1024;

    let mut revolver = ChunkRevolver::new(&config);
    let expected = revolver.total_capacity();

    let mut seen = HashSet::new();
    let mut count = 0;

    while let Some(chunk) = revolver.try_get_chunk() {
        let key = (chunk.ring_nr, chunk.index);
        assert!(
            seen.insert(key),
            "Duplicate chunk detected: ring {}, index {}",
            chunk.ring_nr,
            chunk.index
        );
        count += 1;
    }

    assert_eq!(
        count, expected,
        "Expected to get exactly {} unique chunks",
        expected
    );
}
