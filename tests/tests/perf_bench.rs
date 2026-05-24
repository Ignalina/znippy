//! Performance benchmarks for znippy compression/decompression.

use anyhow::Result;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tempfile::TempDir;
use znippy_compress::{ArchiveEntry, compress_stream};

struct BenchResult {
    label: String,
    input_size: u64,
    output_size: u64,
    compress_ms: u128,
    decompress_ms: u128,
    file_count: usize,
    compressed_files: u64,
    skipped_files: u64,
    chunks: u64,
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
    let file_count = entries.len();

    let out_dir = TempDir::new()?;
    let archive_path = out_dir.path().join("bench.znippy");

    // Compress
    let t0 = Instant::now();
    let compressor = compress_stream(&archive_path.to_path_buf(), false)?;
    for entry in entries {
        compressor.sender().send(entry)?;
    }
    let report = compressor.finish()?;
    let compress_ms = t0.elapsed().as_millis();

    // Measure output size (v2: single file, no .zdata)
    let output_size = fs::metadata(&archive_path)?.len();

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
        file_count,
        compressed_files: report.compressed_files,
        skipped_files: report.uncompressed_files,
        chunks: report.chunks,
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
        pkg_type: None,
        repo: None,
        }],
    )?);

    // 2. Binary pattern data - 500MB
    results.push(bench_roundtrip(
        "binary_pattern_500mb",
        vec![ArchiveEntry {
            relative_path: "pattern.bin".into(),
            data: generate_binary_data(size),
        pkg_type: None,
        repo: None,
        }],
    )?);

    // 3. Random (incompressible) data - 500MB
    results.push(bench_roundtrip(
        "random_500mb",
        vec![ArchiveEntry {
            relative_path: "random.bin".into(),
            data: generate_random_data(size),
        pkg_type: None,
        repo: None,
        }],
    )?);

    // 4. Many small files - 100000 x 10KB
    let small_entries: Vec<ArchiveEntry> = (0..100000)
        .map(|i| ArchiveEntry {
            relative_path: format!("files/file_{:06}.txt", i),
            data: generate_text_data(10 * 1024),
            pkg_type: None,
            repo: None,
        })
        .collect();
    results.push(bench_roundtrip("100k_small_files_10kb", small_entries)?);

    // 5. Mixed workload - simulating large repo (1GB+)
    let mixed_entries = vec![
        ArchiveEntry {
            relative_path: "pom.xml".into(),
            data: generate_text_data(32 * 1024),
        pkg_type: None,
        repo: None,
        },
        ArchiveEntry {
            relative_path: "app.jar".into(),
            data: generate_random_data(200 * 1024 * 1024),
        pkg_type: None,
        repo: None,
        },
        ArchiveEntry {
            relative_path: "sources.jar".into(),
            data: generate_text_data(100 * 1024 * 1024),
        pkg_type: None,
        repo: None,
        },
        ArchiveEntry {
            relative_path: "javadoc.jar".into(),
            data: generate_text_data(80 * 1024 * 1024),
        pkg_type: None,
        repo: None,
        },
        ArchiveEntry {
            relative_path: "metadata.xml".into(),
            data: generate_text_data(16 * 1024),
        pkg_type: None,
        repo: None,
        },
        ArchiveEntry {
            relative_path: "deps.tar.gz".into(),
            data: generate_random_data(150 * 1024 * 1024),
        pkg_type: None,
        repo: None,
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
        pkg_type: None,
        repo: None,
        }],
    )?);

    // Print results
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0);
    println!("\n=============================================================================================================");
    println!("ZNIPPY PERFORMANCE BENCHMARK (OpenZL backend) — {} cores", cores);
    println!("=============================================================================================================");
    println!(
        "{:<25} {:>8} {:>8} {:>6} {:>10} {:>10} {:>8} {:>8} {:>8} {:>8} {:>8}",
        "Test", "In(MB)", "Out(MB)", "Ratio", "Comp MB/s", "Dec MB/s",
        "Comp ms", "Dec ms", "Chunks", "Files", "Skipped"
    );
    println!("{:-<118}", "");

    for r in &results {
        println!(
            "{:<25} {:>8.2} {:>8.2} {:>6.2}x {:>9.1} {:>9.1} {:>8} {:>8} {:>8} {:>8} {:>8}",
            r.label,
            r.input_size as f64 / (1024.0 * 1024.0),
            r.output_size as f64 / (1024.0 * 1024.0),
            r.ratio(),
            r.compress_speed_mbs(),
            r.decompress_speed_mbs(),
            r.compress_ms,
            r.decompress_ms,
            r.chunks,
            r.file_count,
            r.skipped_files,
        );
    }
    println!();

    // Dump for xtask regression tracking (per-entry file count; cores added by xtask).
    let entries: Vec<String> = results.iter().map(|r| {
        format!(r#"{{"name":"{}","compress_mbs":{:.1},"decompress_mbs":{:.1},"files":{}}}"#,
            r.label, r.compress_speed_mbs(), r.decompress_speed_mbs(), r.file_count)
    }).collect();
    let _ = std::fs::write("/tmp/znippy_bench_last.json", format!("[{}]", entries.join(",")));

    Ok(())
}

// --- Real-world benchmarks (read from ~/work/holger_tests/) ---

fn holger_tests_dir() -> PathBuf {
    dirs::home_dir().expect("no home dir").join("work/holger_tests")
}

fn collect_files_recursive(dir: &Path) -> Vec<ArchiveEntry> {
    let mut entries = Vec::new();
    if !dir.exists() {
        return entries;
    }
    for entry in walkdir::WalkDir::new(dir).into_iter().filter_map(|e| e.ok()) {
        if entry.file_type().is_file() {
            if let Ok(data) = fs::read(entry.path()) {
                let rel = entry.path().strip_prefix(dir).unwrap_or(entry.path());
                entries.push(ArchiveEntry {
                    relative_path: rel.to_string_lossy().to_string(),
                    data,
                pkg_type: None,
                repo: None,
                });
            }
        }
    }
    entries
}

fn print_single_result(r: &BenchResult) {
    println!("\n=============================================================================================================");
    println!("ZNIPPY REAL-WORLD BENCHMARK (OpenZL backend)");
    println!("=============================================================================================================");
    println!(
        "{:<25} {:>8} {:>8} {:>6} {:>10} {:>10} {:>8} {:>8} {:>8} {:>8}",
        "Test", "In(MB)", "Out(MB)", "Ratio", "Comp MB/s", "Dec MB/s",
        "Comp ms", "Dec ms", "Chunks", "Skipped"
    );
    println!("{:-<109}", "");
    println!(
        "{:<25} {:>8.2} {:>8.2} {:>6.2}x {:>9.1} {:>9.1} {:>8} {:>8} {:>8} {:>8}",
        r.label,
        r.input_size as f64 / (1024.0 * 1024.0),
        r.output_size as f64 / (1024.0 * 1024.0),
        r.ratio(),
        r.compress_speed_mbs(),
        r.decompress_speed_mbs(),
        r.compress_ms,
        r.decompress_ms,
        r.chunks,
        r.skipped_files,
    );
    println!(
        "  Files: {} (compressed: {}, skipped: {})",
        r.file_count, r.compressed_files, r.skipped_files
    );
    println!();
}

impl BenchResult {
    fn with_file_count(mut self, count: usize) -> Self {
        self.file_count = count;
        self
    }
}

/// Benchmark 4730 real Java JARs from ~/work/holger_tests/jars/.
#[test]
fn perf_real_java_jars() -> Result<()> {
    let jars_dir = holger_tests_dir().join("jars");
    if !jars_dir.exists() {
        eprintln!("  [skip] ~/work/holger_tests/jars not found");
        return Ok(());
    }
    let entries = collect_files_recursive(&jars_dir);
    let entries: Vec<_> = entries.into_iter()
        .filter(|e| e.relative_path.ends_with(".jar"))
        .collect();
    if entries.is_empty() {
        eprintln!("  [skip] no .jar files in ~/work/holger_tests/jars");
        return Ok(());
    }
    let count = entries.len();
    let total: u64 = entries.iter().map(|e| e.data.len() as u64).sum();
    println!("  Java JARs: {} files, {:.1} MB", count, total as f64 / (1024.0 * 1024.0));
    let result = bench_roundtrip("java_jars", entries)?.with_file_count(count);
    print_single_result(&result);
    Ok(())
}

/// Benchmark 9162 real Rust .crate files from ~/work/holger_tests/crates/.
#[test]
fn perf_real_rust_crates() -> Result<()> {
    let crates_dir = holger_tests_dir().join("crates");
    if !crates_dir.exists() {
        eprintln!("  [skip] ~/work/holger_tests/crates not found");
        return Ok(());
    }
    let entries = collect_files_recursive(&crates_dir);
    let entries: Vec<_> = entries.into_iter()
        .filter(|e| e.relative_path.ends_with(".crate"))
        .collect();
    if entries.is_empty() {
        eprintln!("  [skip] no .crate files in ~/work/holger_tests/crates");
        return Ok(());
    }
    let count = entries.len();
    let total: u64 = entries.iter().map(|e| e.data.len() as u64).sum();
    println!("  Rust crates: {} files, {:.1} MB", count, total as f64 / (1024.0 * 1024.0));
    let result = bench_roundtrip("rust_crates", entries)?.with_file_count(count);
    print_single_result(&result);
    Ok(())
}

/// Benchmark 541 real Python wheels/sdists from ~/work/holger_tests/wheels/.
#[test]
fn perf_real_python_wheels() -> Result<()> {
    let wheels_dir = holger_tests_dir().join("wheels");
    if !wheels_dir.exists() {
        eprintln!("  [skip] ~/work/holger_tests/wheels not found");
        return Ok(());
    }
    let entries = collect_files_recursive(&wheels_dir);
    if entries.is_empty() {
        eprintln!("  [skip] no files in ~/work/holger_tests/wheels");
        return Ok(());
    }
    let count = entries.len();
    let total: u64 = entries.iter().map(|e| e.data.len() as u64).sum();
    println!("  Python wheels: {} files, {:.1} MB", count, total as f64 / (1024.0 * 1024.0));
    let result = bench_roundtrip("python_wheels", entries)?.with_file_count(count);
    print_single_result(&result);
    Ok(())
}
