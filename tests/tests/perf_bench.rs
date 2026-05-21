//! Performance benchmarks for znippy compression/decompression.

use anyhow::Result;
use std::fs;
use std::time::Instant;
use tempfile::TempDir;
use znippy_compress::{ArchiveEntry, compress_stream};

struct BenchResult {
    label: String,
    input_size: u64,
    output_size: u64,
    compress_ms: u128,
    decompress_ms: u128,
}

impl BenchResult {
    fn ratio(&self) -> f64 {
        self.input_size as f64 / self.output_size as f64
    }

    fn compress_speed_mbs(&self) -> f64 {
        (self.input_size as f64 / (1024.0 * 1024.0)) / (self.compress_ms as f64 / 1000.0)
    }

    fn decompress_speed_mbs(&self) -> f64 {
        (self.input_size as f64 / (1024.0 * 1024.0)) / (self.decompress_ms as f64 / 1000.0)
    }
}

fn bench_roundtrip(label: &str, entries: Vec<ArchiveEntry>) -> Result<BenchResult> {
    let input_size: u64 = entries.iter().map(|e| e.data.len() as u64).sum();

    let out_dir = TempDir::new()?;
    let archive_path = out_dir.path().join("bench.znippy");

    // Compress
    let t0 = Instant::now();
    let compressor = compress_stream(&archive_path.to_path_buf(), false)?;
    for entry in entries {
        compressor.sender().send(entry)?;
    }
    let _report = compressor.finish()?;
    let compress_ms = t0.elapsed().as_millis();

    // Measure output size
    let znippy_size = fs::metadata(&archive_path)?.len();
    let zdata_size = fs::metadata(archive_path.with_extension("zdata"))?.len();
    let output_size = znippy_size + zdata_size;

    // Decompress
    let decomp_dir = TempDir::new()?;
    let t1 = Instant::now();
    let _verify = znippy_common::decompress_archive(&archive_path, true, decomp_dir.path())?;
    let decompress_ms = t1.elapsed().as_millis();

    Ok(BenchResult {
        label: label.to_string(),
        input_size,
        output_size,
        compress_ms,
        decompress_ms,
    })
}

fn generate_text_data(size: usize) -> Vec<u8> {
    let phrase = b"The quick brown fox jumps over the lazy dog. ";
    phrase.iter().cycle().take(size).copied().collect()
}

fn generate_binary_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 251) as u8).collect()
}

fn generate_random_data(size: usize) -> Vec<u8> {
    // Pseudo-random (deterministic) using simple LCG
    let mut val: u64 = 12345;
    (0..size)
        .map(|_| {
            val = val.wrapping_mul(6364136223846793005).wrapping_add(1);
            (val >> 33) as u8
        })
        .collect()
}

#[test]
fn perf_benchmark_suite() -> Result<()> {
    let mut results = Vec::new();

    // 1. Text data - 500MB
    let size = 500 * 1024 * 1024;
    results.push(bench_roundtrip(
        "text_500mb",
        vec![ArchiveEntry {
            relative_path: "text.txt".into(),
            data: generate_text_data(size),
        }],
    )?);

    // 2. Binary pattern data - 500MB
    results.push(bench_roundtrip(
        "binary_pattern_500mb",
        vec![ArchiveEntry {
            relative_path: "pattern.bin".into(),
            data: generate_binary_data(size),
        }],
    )?);

    // 3. Random (incompressible) data - 500MB
    results.push(bench_roundtrip(
        "random_500mb",
        vec![ArchiveEntry {
            relative_path: "random.bin".into(),
            data: generate_random_data(size),
        }],
    )?);

    // 4. Many small files - 100000 x 10KB
    let small_entries: Vec<ArchiveEntry> = (0..100000)
        .map(|i| ArchiveEntry {
            relative_path: format!("files/file_{:06}.txt", i),
            data: generate_text_data(10 * 1024),
        })
        .collect();
    results.push(bench_roundtrip("100k_small_files_10kb", small_entries)?);

    // 5. Mixed workload - simulating large repo (1GB+)
    let mixed_entries = vec![
        ArchiveEntry {
            relative_path: "pom.xml".into(),
            data: generate_text_data(32 * 1024),
        },
        ArchiveEntry {
            relative_path: "app.jar".into(),
            data: generate_random_data(200 * 1024 * 1024),
        },
        ArchiveEntry {
            relative_path: "sources.jar".into(),
            data: generate_text_data(100 * 1024 * 1024),
        },
        ArchiveEntry {
            relative_path: "javadoc.jar".into(),
            data: generate_text_data(80 * 1024 * 1024),
        },
        ArchiveEntry {
            relative_path: "metadata.xml".into(),
            data: generate_text_data(16 * 1024),
        },
        ArchiveEntry {
            relative_path: "deps.tar.gz".into(),
            data: generate_random_data(150 * 1024 * 1024),
        },
    ];
    results.push(bench_roundtrip("mixed_repo_530mb", mixed_entries)?);

    // 6. Single huge file - 2GB (stress multi-chunk)
    let huge_size = 2 * 1024 * 1024 * 1024;
    results.push(bench_roundtrip(
        "single_file_2gb",
        vec![ArchiveEntry {
            relative_path: "huge.bin".into(),
            data: generate_text_data(huge_size),
        }],
    )?);

    // Print results
    println!("\n============================================================");
    println!("ZNIPPY PERFORMANCE BENCHMARK (zstd backend)");
    println!("============================================================");
    println!(
        "{:<25} {:>8} {:>8} {:>6} {:>10} {:>10}",
        "Test", "In(MB)", "Out(MB)", "Ratio", "Comp MB/s", "Dec MB/s"
    );
    println!("{:-<75}", "");

    for r in &results {
        println!(
            "{:<25} {:>8.2} {:>8.2} {:>6.2}x {:>9.1} {:>9.1}",
            r.label,
            r.input_size as f64 / (1024.0 * 1024.0),
            r.output_size as f64 / (1024.0 * 1024.0),
            r.ratio(),
            r.compress_speed_mbs(),
            r.decompress_speed_mbs(),
        );
    }
    println!();

    Ok(())
}
