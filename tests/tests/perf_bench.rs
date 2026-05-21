//! Performance benchmarks for znippy compression/decompression.

use anyhow::Result;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
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
        file_count: 0,
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
    println!("ZNIPPY PERFORMANCE BENCHMARK (OpenZL backend)");
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

// --- Real-world benchmarks (require network + tools, run with --ignored) ---

fn cache_dir() -> PathBuf {
    let dir = PathBuf::from("/tmp/znippy-bench-cache");
    fs::create_dir_all(&dir).ok();
    dir
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
                });
            }
        }
    }
    entries
}

fn print_single_result(r: &BenchResult) {
    println!("\n============================================================");
    println!("ZNIPPY REAL-WORLD BENCHMARK (OpenZL backend)");
    println!("============================================================");
    println!(
        "{:<30} {:>8} {:>8} {:>6} {:>10} {:>10}",
        "Test", "In(MB)", "Out(MB)", "Ratio", "Comp MB/s", "Dec MB/s"
    );
    println!("{:-<80}", "");
    println!(
        "{:<30} {:>8.2} {:>8.2} {:>6.2}x {:>9.1} {:>9.1}",
        r.label,
        r.input_size as f64 / (1024.0 * 1024.0),
        r.output_size as f64 / (1024.0 * 1024.0),
        r.ratio(),
        r.compress_speed_mbs(),
        r.decompress_speed_mbs(),
    );
    println!("  Files: {}", r.file_count);
    println!();
}

impl BenchResult {
    fn with_file_count(mut self, count: usize) -> Self {
        self.file_count = count;
        self
    }
}

/// Download and cache a large Java project's dependencies (~2GB).
/// Uses Spring Boot + Kafka + Elasticsearch as dep-heavy POM.
fn prepare_java_deps() -> PathBuf {
    let java_dir = cache_dir().join("java-deps");
    let marker = java_dir.join(".done");
    if marker.exists() {
        println!("  [cached] Java deps at {}", java_dir.display());
        return java_dir;
    }

    println!("  Downloading Java dependencies (~2GB)...");
    fs::create_dir_all(&java_dir).unwrap();

    let pom = r#"<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.bench</groupId>
    <artifactId>znippy-bench</artifactId>
    <version>1.0</version>
    <dependencies>
        <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-web</artifactId><version>3.2.5</version></dependency>
        <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-data-jpa</artifactId><version>3.2.5</version></dependency>
        <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-security</artifactId><version>3.2.5</version></dependency>
        <dependency><groupId>org.springframework.kafka</groupId><artifactId>spring-kafka</artifactId><version>3.1.4</version></dependency>
        <dependency><groupId>org.elasticsearch.client</groupId><artifactId>elasticsearch-rest-high-level-client</artifactId><version>7.17.21</version></dependency>
        <dependency><groupId>io.netty</groupId><artifactId>netty-all</artifactId><version>4.1.109.Final</version></dependency>
        <dependency><groupId>com.google.guava</groupId><artifactId>guava</artifactId><version>33.2.0-jre</version></dependency>
        <dependency><groupId>org.apache.hadoop</groupId><artifactId>hadoop-client</artifactId><version>3.3.6</version></dependency>
        <dependency><groupId>org.apache.spark</groupId><artifactId>spark-core_2.13</artifactId><version>3.5.1</version></dependency>
        <dependency><groupId>org.apache.flink</groupId><artifactId>flink-java</artifactId><version>1.19.0</version></dependency>
        <dependency><groupId>io.grpc</groupId><artifactId>grpc-all</artifactId><version>1.63.0</version></dependency>
        <dependency><groupId>software.amazon.awssdk</groupId><artifactId>s3</artifactId><version>2.25.40</version></dependency>
        <dependency><groupId>software.amazon.awssdk</groupId><artifactId>dynamodb</artifactId><version>2.25.40</version></dependency>
    </dependencies>
</project>"#;

    let pom_path = java_dir.join("pom.xml");
    fs::write(&pom_path, pom).unwrap();

    let deps_dir = java_dir.join("deps");
    fs::create_dir_all(&deps_dir).unwrap();

    let status = Command::new("mvn")
        .args([
            "dependency:copy-dependencies",
            &format!("-DoutputDirectory={}", deps_dir.display()),
            "-DincludeScope=runtime",
            "-f", &pom_path.to_string_lossy(),
        ])
        .status();

    match status {
        Ok(s) if s.success() => {
            fs::write(&marker, "ok").unwrap();
        }
        _ => {
            eprintln!("  WARNING: mvn failed — is Maven installed?");
        }
    }
    java_dir
}

/// Download and cache a large Rust project's vendored deps (~2GB).
/// Uses a Cargo.toml with many heavy crates, then `cargo vendor`.
fn prepare_rust_deps() -> PathBuf {
    let rust_dir = cache_dir().join("rust-deps");
    let marker = rust_dir.join(".done");
    if marker.exists() {
        println!("  [cached] Rust deps at {}", rust_dir.display());
        return rust_dir;
    }

    println!("  Downloading Rust dependencies (~2GB)...");
    fs::create_dir_all(&rust_dir).unwrap();

    let cargo_toml = r#"[package]
name = "znippy-bench-deps"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
reqwest = { version = "0.12", features = ["json", "rustls-tls"] }
sqlx = { version = "0.7", features = ["runtime-tokio", "postgres", "sqlite", "mysql"] }
axum = { version = "0.7", features = ["ws"] }
tonic = "0.11"
prost = "0.12"
diesel = { version = "2", features = ["postgres", "sqlite", "mysql"] }
sea-orm = { version = "0.12", features = ["sqlx-postgres", "runtime-tokio-rustls"] }
polars = { version = "0.38", features = ["lazy", "csv", "parquet", "json"] }
arrow = "51"
parquet = "51"
datafusion = "37"
tantivy = "0.22"
rustls = "0.23"
hyper = { version = "1", features = ["full"] }
tower = { version = "0.4", features = ["full"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
clap = { version = "4", features = ["derive"] }
rayon = "1"
crossbeam = "0.8"
dashmap = "5"
bytes = "1"
uuid = { version = "1", features = ["v4"] }
chrono = { version = "0.4", features = ["serde"] }
regex = "1"
rand = "0.8"
num = "0.4"
image = "0.25"
wgpu = "0.19"
bevy_ecs = "0.13"
"#;

    let cargo_path = rust_dir.join("Cargo.toml");
    fs::write(&cargo_path, cargo_toml).unwrap();

    // Need a src/lib.rs for cargo to accept it
    let src_dir = rust_dir.join("src");
    fs::create_dir_all(&src_dir).unwrap();
    fs::write(src_dir.join("lib.rs"), "").unwrap();

    let vendor_dir = rust_dir.join("vendor");
    let status = Command::new("cargo")
        .args(["vendor", &vendor_dir.to_string_lossy()])
        .current_dir(&rust_dir)
        .status();

    match status {
        Ok(s) if s.success() => {
            fs::write(&marker, "ok").unwrap();
        }
        _ => {
            eprintln!("  WARNING: cargo vendor failed");
        }
    }
    rust_dir
}

#[test]
#[ignore]
fn perf_real_java_deps() -> Result<()> {
    let java_dir = prepare_java_deps();
    let deps_dir = java_dir.join("deps");
    if !deps_dir.exists() {
        eprintln!("Java deps not available, skipping");
        return Ok(());
    }

    let entries = collect_files_recursive(&deps_dir);
    let count = entries.len();
    if entries.is_empty() {
        eprintln!("No Java deps found, skipping");
        return Ok(());
    }

    let total: u64 = entries.iter().map(|e| e.data.len() as u64).sum();
    println!("  Java deps: {} files, {:.1} MB", count, total as f64 / (1024.0 * 1024.0));

    let result = bench_roundtrip("java_deps_real", entries)?.with_file_count(count);
    print_single_result(&result);
    Ok(())
}

#[test]
#[ignore]
fn perf_real_rust_deps() -> Result<()> {
    let rust_dir = prepare_rust_deps();
    let vendor_dir = rust_dir.join("vendor");
    if !vendor_dir.exists() {
        eprintln!("Rust vendor not available, skipping");
        return Ok(());
    }

    let entries = collect_files_recursive(&vendor_dir);
    let count = entries.len();
    if entries.is_empty() {
        eprintln!("No Rust deps found, skipping");
        return Ok(());
    }

    let total: u64 = entries.iter().map(|e| e.data.len() as u64).sum();
    println!("  Rust deps: {} files, {:.1} MB", count, total as f64 / (1024.0 * 1024.0));

    let result = bench_roundtrip("rust_deps_real", entries)?.with_file_count(count);
    print_single_result(&result);
    Ok(())
}
