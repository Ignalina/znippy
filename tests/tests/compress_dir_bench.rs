//! Benchmark for compress_dir (slot_packer + io_uring path).
//!
//! Unlike perf_bench.rs which uses stream_packer (in-memory data via channel),
//! this exercises the actual filesystem read path where io_uring batch reads
//! provide real benefit on NVMe.
//!
//! Tests ALWAYS delete any existing .znippy output before running — no caching.

use anyhow::Result;
use std::fs;
use std::path::Path;
use std::time::Instant;
use tempfile::TempDir;
use znippy_compress::compress_dir;
use znippy_plugin_maven::NativeMavenPlugin;
use znippy_common::plugin::PluginRegistry;

fn generate_text_data(size: usize) -> Vec<u8> {
    let phrase = b"The quick brown fox jumps over the lazy dog. ";
    phrase.iter().cycle().take(size).copied().collect()
}

fn generate_random_data(size: usize) -> Vec<u8> {
    let mut val: u64 = 12345;
    (0..size)
        .map(|_| {
            val = val.wrapping_mul(6364136223846793005).wrapping_add(1);
            (val >> 33) as u8
        })
        .collect()
}

/// Create a directory with many small files (simulates extracted JARs).
fn create_small_files_dir(dir: &Path, count: usize, size: usize) {
    fs::create_dir_all(dir).unwrap();
    for i in 0..count {
        let subdir = dir.join(format!("d{:03}", i / 1000));
        fs::create_dir_all(&subdir).unwrap();
        let path = subdir.join(format!("file_{:06}.txt", i));
        fs::write(&path, &generate_text_data(size)).unwrap();
    }
}

/// Create a directory with a mix of file sizes (simulates a maven repo cache).
fn create_mixed_files_dir(dir: &Path) {
    fs::create_dir_all(dir).unwrap();
    // 500 small xml/pom files (1-8 KB)
    let pom_dir = dir.join("poms");
    fs::create_dir_all(&pom_dir).unwrap();
    for i in 0..500 {
        let size = 1024 + (i % 8) * 1024;
        fs::write(pom_dir.join(format!("pom_{:04}.xml", i)), generate_text_data(size)).unwrap();
    }
    // 200 medium JARs (100KB - 2MB)
    let jar_dir = dir.join("jars");
    fs::create_dir_all(&jar_dir).unwrap();
    for i in 0..200 {
        let size = 100 * 1024 + (i % 20) * 100 * 1024;
        fs::write(jar_dir.join(format!("lib_{:04}.jar", i)), generate_random_data(size)).unwrap();
    }
    // 20 large JARs (5-20MB)
    let big_dir = dir.join("big");
    fs::create_dir_all(&big_dir).unwrap();
    for i in 0..20 {
        let size = 5 * 1024 * 1024 + i * 1024 * 1024;
        fs::write(big_dir.join(format!("big_{:02}.jar", i)), generate_random_data(size)).unwrap();
    }
}

struct DirBenchResult {
    label: String,
    file_count: usize,
    input_bytes: u64,
    output_bytes: u64,
    compress_ms: u128,
    decompress_ms: u128,
}

impl DirBenchResult {
    fn compress_mbs(&self) -> f64 {
        (self.input_bytes as f64 / (1024.0 * 1024.0)) / (self.compress_ms as f64 / 1000.0)
    }
    fn decompress_mbs(&self) -> f64 {
        (self.input_bytes as f64 / (1024.0 * 1024.0)) / (self.decompress_ms as f64 / 1000.0)
    }
    fn ratio(&self) -> f64 {
        self.input_bytes as f64 / self.output_bytes as f64
    }
}

fn dir_size(dir: &Path) -> u64 {
    walkdir::WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .map(|e| e.metadata().map(|m| m.len()).unwrap_or(0))
        .sum()
}

fn file_count(dir: &Path) -> usize {
    walkdir::WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .count()
}

fn bench_compress_dir(label: &str, input_dir: &Path) -> Result<DirBenchResult> {
    let out_dir = TempDir::new()?;
    let archive_path = out_dir.path().join("output.znippy");

    // ALWAYS delete before benchmark
    let _ = fs::remove_file(&archive_path);

    let input_bytes = dir_size(input_dir);
    let count = file_count(input_dir);

    let registry = PluginRegistry::with_plugin(Box::new(NativeMavenPlugin));

    // Compress via compress_dir (slot_packer + io_uring)
    let t0 = Instant::now();
    let _report = compress_dir(
        &input_dir.to_path_buf(),
        &archive_path.to_path_buf(),
        false,
        Some(&registry),
        None,
    )?;
    let compress_ms = t0.elapsed().as_millis();

    let output_bytes = fs::metadata(&archive_path)?.len();

    // Decompress to verify correctness
    let decomp_dir = TempDir::new()?;
    let t1 = Instant::now();
    let _verify = znippy_common::decompress_archive(&archive_path, true, decomp_dir.path())?;
    let decompress_ms = t1.elapsed().as_millis();

    Ok(DirBenchResult {
        label: label.to_string(),
        file_count: count,
        input_bytes,
        output_bytes,
        compress_ms,
        decompress_ms,
    })
}

/// Synthetic compress_dir benchmark: 10k small files from disk.
/// Exercises io_uring batch read path in slot_packer.
#[test]
fn compress_dir_10k_small_files() -> Result<()> {
    let tmp = TempDir::new()?;
    let input_dir = tmp.path().join("input");
    create_small_files_dir(&input_dir, 10_000, 10 * 1024);

    let result = bench_compress_dir("10k_small_10kb", &input_dir)?;
    print_result(&result);
    Ok(())
}

/// Synthetic compress_dir benchmark: mixed file sizes.
/// Tests io_uring for small files + sequential fallback for large files.
#[test]
fn compress_dir_mixed_sizes() -> Result<()> {
    let tmp = TempDir::new()?;
    let input_dir = tmp.path().join("input");
    create_mixed_files_dir(&input_dir);

    let result = bench_compress_dir("mixed_720_files", &input_dir)?;
    print_result(&result);
    Ok(())
}

/// Real-world compress_dir benchmark: actual JARs from ~/work/holger_tests/jars/.
/// This is the most representative test for io_uring benefit on NVMe.
#[test]
#[ignore] // requires cached test data
fn compress_dir_real_jars() -> Result<()> {
    let jars_dir = dirs::home_dir().unwrap().join("work/holger_tests/jars");
    if !jars_dir.exists() {
        eprintln!("  [skip] ~/work/holger_tests/jars not found");
        return Ok(());
    }
    let result = bench_compress_dir("real_jars", &jars_dir)?;
    print_result(&result);
    dump_for_xtask(&[&result]);
    Ok(())
}

/// Real-world compress_dir benchmark: actual crates from ~/work/holger_tests/crates/.
#[test]
#[ignore] // requires cached test data
fn compress_dir_real_crates() -> Result<()> {
    let crates_dir = dirs::home_dir().unwrap().join("work/holger_tests/crates");
    if !crates_dir.exists() {
        eprintln!("  [skip] ~/work/holger_tests/crates not found");
        return Ok(());
    }
    let result = bench_compress_dir("real_crates", &crates_dir)?;
    print_result(&result);
    dump_for_xtask(&[&result]);
    Ok(())
}

/// Full compress_dir benchmark suite (synthetic + real if available).
/// Always deletes output archives — no caching.
#[test]
#[ignore] // full suite takes time
fn compress_dir_benchmark_suite() -> Result<()> {
    let mut results = Vec::new();

    // Synthetic: 10k small files
    let tmp1 = TempDir::new()?;
    let small_dir = tmp1.path().join("small");
    create_small_files_dir(&small_dir, 10_000, 10 * 1024);
    results.push(bench_compress_dir("10k_small_10kb", &small_dir)?);

    // Synthetic: 50k small files (stresses io_uring batching)
    let tmp2 = TempDir::new()?;
    let many_dir = tmp2.path().join("many");
    create_small_files_dir(&many_dir, 50_000, 4 * 1024);
    results.push(bench_compress_dir("50k_small_4kb", &many_dir)?);

    // Synthetic: mixed sizes
    let tmp3 = TempDir::new()?;
    let mixed_dir = tmp3.path().join("mixed");
    create_mixed_files_dir(&mixed_dir);
    results.push(bench_compress_dir("mixed_720_files", &mixed_dir)?);

    // Real JARs (if available)
    let jars_dir = dirs::home_dir().unwrap().join("work/holger_tests/jars");
    if jars_dir.exists() {
        results.push(bench_compress_dir("real_jars", &jars_dir)?);
    }

    // Real crates (if available)
    let crates_dir = dirs::home_dir().unwrap().join("work/holger_tests/crates");
    if crates_dir.exists() {
        results.push(bench_compress_dir("real_crates", &crates_dir)?);
    }

    println!("\n=============================================================================================================");
    println!("ZNIPPY compress_dir BENCHMARK (slot_packer + io_uring)");
    println!("=============================================================================================================");
    println!(
        "{:<20} {:>8} {:>8} {:>6} {:>10} {:>10} {:>8} {:>8} {:>8}",
        "Test", "In(MB)", "Out(MB)", "Ratio", "Comp MB/s", "Dec MB/s", "Comp ms", "Dec ms", "Files"
    );
    println!("{:-<100}", "");
    for r in &results {
        println!(
            "{:<20} {:>8.1} {:>8.1} {:>6.2}x {:>9.1} {:>9.1} {:>8} {:>8} {:>8}",
            r.label,
            r.input_bytes as f64 / (1024.0 * 1024.0),
            r.output_bytes as f64 / (1024.0 * 1024.0),
            r.ratio(),
            r.compress_mbs(),
            r.decompress_mbs(),
            r.compress_ms,
            r.decompress_ms,
            r.file_count,
        );
    }
    println!();

    let refs: Vec<&DirBenchResult> = results.iter().collect();
    dump_for_xtask(&refs);
    Ok(())
}

fn print_result(r: &DirBenchResult) {
    println!(
        "\n  {} — {} files, {:.1} MB → {:.1} MB ({:.2}x)",
        r.label,
        r.file_count,
        r.input_bytes as f64 / (1024.0 * 1024.0),
        r.output_bytes as f64 / (1024.0 * 1024.0),
        r.ratio(),
    );
    println!(
        "  compress: {:.1} MB/s ({} ms)  decompress: {:.1} MB/s ({} ms)",
        r.compress_mbs(), r.compress_ms, r.decompress_mbs(), r.decompress_ms,
    );
}

fn dump_for_xtask(results: &[&DirBenchResult]) {
    let entries: Vec<String> = results.iter().map(|r| {
        format!(
            r#"{{"name":"dir_{}","compress_mbs":{:.1},"decompress_mbs":{:.1},"files":{}}}"#,
            r.label, r.compress_mbs(), r.decompress_mbs(), r.file_count
        )
    }).collect();
    let _ = fs::write("target/znippy_dir_bench_last.json", format!("[{}]", entries.join(",")));
}

/// Minimal test: just 10 .jar files (skipped compression). Isolates the skip path.
#[test]
fn compress_dir_skipped_jars_only() -> Result<()> {
    let tmp = TempDir::new()?;
    let input_dir = tmp.path().join("input");
    let jar_dir = input_dir.join("jars");
    std::fs::create_dir_all(&jar_dir)?;
    for i in 0..10 {
        let data: Vec<u8> = (0..2_000_000).map(|j| ((i*1000+j) % 251) as u8).collect();
        std::fs::write(jar_dir.join(format!("lib_{:02}.jar", i)), &data)?;
    }
    eprintln!("  Created 10 jar files (2MB each)...");
    let result = bench_compress_dir("10_jars_skip", &input_dir)?;
    print_result(&result);
    Ok(())
}

/// Large .jar files that exceed slice_size (sequential path).
#[test]
fn compress_dir_large_jars() -> Result<()> {
    let tmp = TempDir::new()?;
    let input_dir = tmp.path().join("input");
    std::fs::create_dir_all(&input_dir)?;
    // 20 jars, 5-20MB each (some exceed slice_size=6.25MB)
    for i in 0..20 {
        let size = 5 * 1024 * 1024 + i * 1024 * 1024;
        let data: Vec<u8> = (0..size).map(|j| ((j) % 251) as u8).collect();
        std::fs::write(input_dir.join(format!("big_{:02}.jar", i)), &data)?;
    }
    eprintln!("  Created 20 large jar files (5-24MB)...");
    let result = bench_compress_dir("20_large_jars", &input_dir)?;
    print_result(&result);
    Ok(())
}

/// Reproducer: mix of small text + medium jars + large jars.
#[test]
fn compress_dir_mixed_repro() -> Result<()> {
    let tmp = TempDir::new()?;
    let input_dir = tmp.path().join("input");
    // 500 small poms
    let pom_dir = input_dir.join("poms");
    std::fs::create_dir_all(&pom_dir)?;
    for i in 0..500 {
        let size = 1024 + (i % 8) * 1024;
        std::fs::write(pom_dir.join(format!("pom_{:04}.xml", i)), generate_text_data(size))?;
    }
    eprintln!("  500 poms created");
    // 200 medium jars
    let jar_dir = input_dir.join("jars");
    std::fs::create_dir_all(&jar_dir)?;
    for i in 0..200 {
        let size = 100 * 1024 + (i % 20) * 100 * 1024;
        std::fs::write(jar_dir.join(format!("lib_{:04}.jar", i)), generate_random_data(size))?;
    }
    eprintln!("  200 medium jars created");
    // 20 large jars
    let big_dir = input_dir.join("big");
    std::fs::create_dir_all(&big_dir)?;
    for i in 0..20 {
        let size = 5 * 1024 * 1024 + i * 1024 * 1024;
        std::fs::write(big_dir.join(format!("big_{:02}.jar", i)), generate_random_data(size))?;
    }
    eprintln!("  20 large jars created, compressing...");
    let result = bench_compress_dir("mixed_repro", &input_dir)?;
    print_result(&result);
    Ok(())
}

/// Quick test with real jar files from ~/work/holger_tests/jars_39
#[test]
fn compress_dir_39_real_jars() -> Result<()> {
    let jar_dir = std::path::PathBuf::from(
        format!("{}/work/holger_tests/jars_39", std::env::var("HOME").unwrap_or_default()));
    if !jar_dir.exists() { return Ok(()); }
    let result = bench_compress_dir("39_real_jars", &jar_dir)?;
    print_result(&result);
    Ok(())
}

#[test]
fn compress_dir_500_real_jars() -> Result<()> {
    let jar_dir = std::path::PathBuf::from(
        format!("{}/work/holger_tests/jars_500", std::env::var("HOME").unwrap_or_default()));
    if !jar_dir.exists() { return Ok(()); }
    let result = bench_compress_dir("500_real_jars", &jar_dir)?;
    print_result(&result);
    Ok(())
}

/// Pure read benchmark — no compression, just io_uring open+read+close to measure raw I/O.
#[test]
fn compress_dir_read_only_10k() -> Result<()> {
    use io_uring::{IoUring, opcode, types};
    use std::ffi::CString;
    use walkdir::WalkDir;

    let tmp = TempDir::new()?;
    let dir = tmp.path().join("small");
    create_small_files_dir(&dir, 10000, 10240);

    // Collect all file paths
    let files: Vec<_> = WalkDir::new(&dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .map(|e| e.into_path())
        .collect();

    let total_bytes: u64 = files.iter().map(|f| f.metadata().unwrap().len()).sum();

    let start = Instant::now();

    const BATCH: usize = 64;
    let mut ring = IoUring::new((BATCH * 2) as u32).unwrap();
    let mut buf = vec![0u8; 6_500_000]; // one slot

    let mut i = 0;
    while i < files.len() {
        let end = (i + BATCH).min(files.len());
        let batch_count = end - i;

        // Open batch
        let cpaths: Vec<CString> = files[i..end].iter()
            .map(|p| CString::new(p.as_os_str().as_encoded_bytes()).unwrap())
            .collect();
        for (j, cp) in cpaths.iter().enumerate() {
            let op = opcode::OpenAt::new(types::Fd(-1), cp.as_ptr())
                .flags(libc::O_RDONLY)
                .build()
                .user_data(j as u64);
            unsafe { ring.submission().push(&op).unwrap(); }
        }
        ring.submit_and_wait(batch_count).unwrap();

        let mut fds = vec![0i32; batch_count];
        for _ in 0..batch_count {
            let cqe = ring.completion().next().unwrap();
            fds[cqe.user_data() as usize] = cqe.result();
        }

        // Read batch
        for (j, &fd) in fds.iter().enumerate() {
            let size = files[i + j].metadata().unwrap().len() as u32;
            let op = opcode::Read::new(types::Fd(fd), buf.as_mut_ptr(), size)
                .build()
                .user_data(j as u64);
            unsafe { ring.submission().push(&op).unwrap(); }
        }
        ring.submit_and_wait(batch_count).unwrap();
        for _ in 0..batch_count { ring.completion().next().unwrap(); }

        // Close batch
        for &fd in &fds {
            let op = opcode::Close::new(types::Fd(fd)).build().user_data(0);
            unsafe { ring.submission().push(&op).unwrap(); }
        }
        ring.submit_and_wait(batch_count).unwrap();
        for _ in 0..batch_count { ring.completion().next().unwrap(); }

        i = end;
    }

    let elapsed = start.elapsed();
    let mbs = total_bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
    println!("\n  READ-ONLY 10k×10KB: {:.1} MB/s ({:.0} ms), {} files, {:.1} MB total\n",
        mbs, elapsed.as_secs_f64() * 1000.0, files.len(), total_bytes as f64 / (1024.0*1024.0));
    Ok(())
}
