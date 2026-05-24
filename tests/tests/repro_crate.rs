//! Throwaway reproduction: many small high-entropy blobs compressed with
//! no_skip=true, then full round-trip verify + random-access extract.
//! Mirrors holger's crate bench workload (mixed sizes, coalesced small files).

use tempfile::TempDir;
use znippy_compress::{ArchiveEntry, compress_stream};

fn incompressible(seed: u64, len: usize) -> Vec<u8> {
    let mut v = seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(1);
    (0..len)
        .map(|_| {
            v = v.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (v >> 33) as u8
        })
        .collect()
}

#[test]
fn repro_crate_roundtrip() {
    let n = 5000usize;
    let sizes = [1_000usize, 5_000, 10_000, 50_000, 100_000];

    let dir = TempDir::new().unwrap();
    let base = dir.path().join("repro_crates");
    let archive = base.with_extension("znippy");

    // no_skip = true → force openzl compression of high-entropy blobs.
    let compressor = compress_stream(&base, true).unwrap();
    let sender = compressor.sender().clone();
    for i in 0..n {
        let len = sizes[i % sizes.len()];
        sender
            .send(ArchiveEntry {
                relative_path: format!("bench-crate-{i:06}-1.0.0.crate"),
                data: incompressible(i as u64, len),
                pkg_type: None,
                repo: None,
            })
            .unwrap();
    }
    drop(sender);
    let report = compressor.finish().unwrap();
    eprintln!(
        "compressed {} files, {} chunks, {} bytes out",
        report.total_files, report.chunks, report.total_bytes_out
    );

    let out = TempDir::new().unwrap();
    let verify = znippy_common::decompress_archive(&archive, true, out.path()).unwrap();
    eprintln!(
        "verify: total_files={} verified={} corrupt={} corrupt_bytes={}",
        verify.total_files, verify.verified_files, verify.corrupt_files, verify.corrupt_bytes
    );
    assert_eq!(verify.corrupt_files, 0, "round-trip corruption detected");

    // Random-access path used by holger get_file().
    use znippy_common::archive::{ZnippyArchive, ZnippyReader};
    let arc = ZnippyArchive::open(&archive).unwrap();
    for i in (0..n).step_by(137) {
        let fname = format!("bench-crate-{i:06}-1.0.0.crate");
        let got = arc
            .extract_file(&fname)
            .unwrap_or_else(|e| panic!("extract {fname}: {e}"));
        assert_eq!(got.len(), sizes[i % sizes.len()], "wrong length for {fname}");
    }
    eprintln!("random-access extract_file: OK");
}
