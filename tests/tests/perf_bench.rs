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
        file_count: 0,
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
    println!("\n=============================================================================================================");
    println!("ZNIPPY PERFORMANCE BENCHMARK (OpenZL backend)");
    println!("=============================================================================================================");
    println!(
        "{:<25} {:>8} {:>8} {:>6} {:>10} {:>10} {:>8} {:>8} {:>8} {:>8}",
        "Test", "In(MB)", "Out(MB)", "Ratio", "Comp MB/s", "Dec MB/s",
        "Comp ms", "Dec ms", "Chunks", "Skipped"
    );
    println!("{:-<109}", "");

    for r in &results {
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
    }
    println!();

    // Dump for xtask regression tracking
    let entries: Vec<String> = results.iter().map(|r| {
        format!(r#"{{"name":"{}","compress_mbs":{:.1},"decompress_mbs":{:.1}}}"#,
            r.label, r.compress_speed_mbs(), r.decompress_speed_mbs())
    }).collect();
    let _ = std::fs::write("/tmp/znippy_bench_last.json", format!("[{}]", entries.join(",")));

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

/// Download and cache a large Java project's dependencies (~2GB).
/// Uses POM parser + transitive resolver to fetch JARs from Maven Central.
#[cfg(feature = "resolve")]
fn prepare_java_deps() -> PathBuf {
    let java_dir = cache_dir().join("java-deps");
    let marker = java_dir.join(".done");
    if marker.exists() {
        println!("  [cached] Java deps at {}", java_dir.display());
        return java_dir;
    }

    println!("  Resolving Java dependencies from Maven Central...");
    fs::create_dir_all(&java_dir).unwrap();

    let pom = br#"<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.bench</groupId>
    <artifactId>znippy-bench</artifactId>
    <version>1.0</version>
    <dependencies>
        <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-web</artifactId><version>3.2.5</version></dependency>
        <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-data-jpa</artifactId><version>3.2.5</version></dependency>
        <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-security</artifactId><version>3.2.5</version></dependency>
        <dependency><groupId>org.springframework.kafka</groupId><artifactId>spring-kafka</artifactId><version>3.1.4</version></dependency>
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

    use znippy_plugin_maven::resolver::{resolve_transitive, download_artifact_to_file};

    println!("  Resolving transitive dependencies (depth=3)...");
    let all_deps = resolve_transitive(pom, 3);
    println!("  Resolved {} total artifacts", all_deps.len());

    let mut downloaded = 0;
    for coord in &all_deps {
        let dest = java_dir.join(coord.filename());
        if dest.exists() {
            downloaded += 1;
            continue;
        }
        if download_artifact_to_file(coord, &dest) {
            downloaded += 1;
            if downloaded % 50 == 0 {
                println!("  Downloaded {}/{} artifacts...", downloaded, all_deps.len());
            }
        }
    }
    println!("  Done: {} artifacts downloaded", downloaded);
    fs::write(&marker, format!("{} artifacts", all_deps.len())).unwrap();
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

/// Extract JAR contents into a cached `raw/` directory. Reuses cache if present.
fn prepare_java_raw(java_dir: &Path) -> PathBuf {
    let raw_dir = java_dir.join("raw");
    let marker = raw_dir.join(".done");
    if marker.exists() {
        println!("  [cached] Exploded JARs at {}", raw_dir.display());
        return raw_dir;
    }

    println!("  Exploding JARs into raw files...");
    fs::create_dir_all(&raw_dir).unwrap();

    let mut total_files = 0usize;
    for entry in walkdir::WalkDir::new(java_dir).into_iter().filter_map(|e| e.ok()) {
        if !entry.file_type().is_file() { continue; }
        let path = entry.path();
        if !path.to_string_lossy().ends_with(".jar") { continue; }

        let data = match fs::read(path) {
            Ok(d) => d,
            Err(_) => continue,
        };
        let jar_name = path.file_stem().unwrap_or_default().to_string_lossy().to_string();
        let jar_out = raw_dir.join(&jar_name);
        fs::create_dir_all(&jar_out).ok();

        if let Some(files) = extract_jar_contents(&jar_name, &data) {
            for f in files {
                // Strip the jar_name/ prefix we added
                let rel = f.relative_path.strip_prefix(&format!("{}/", jar_name))
                    .unwrap_or(&f.relative_path);
                let dest = jar_out.join(rel);
                if let Some(parent) = dest.parent() {
                    fs::create_dir_all(parent).ok();
                }
                fs::write(&dest, &f.data).ok();
                total_files += 1;
            }
        }
    }

    println!("  Exploded {} raw files", total_files);
    fs::write(&marker, format!("{} files", total_files)).unwrap();
    raw_dir
}

/// Extract all files from a JAR/ZIP into ArchiveEntries with raw (decompressed) data.
fn extract_jar_contents(jar_name: &str, data: &[u8]) -> Option<Vec<ArchiveEntry>> {
    let eocd_pos = find_eocd_bench(data)?;
    let cd_offset = u32::from_le_bytes(data[eocd_pos + 16..eocd_pos + 20].try_into().ok()?) as usize;
    let cd_entries = u16::from_le_bytes(data[eocd_pos + 10..eocd_pos + 12].try_into().ok()?) as usize;

    let mut results = Vec::new();
    let mut pos = cd_offset;

    for _ in 0..cd_entries {
        if pos + 46 > data.len() { break; }
        let sig = u32::from_le_bytes(data[pos..pos+4].try_into().ok()?);
        if sig != 0x02014b50 { break; }

        let compression = u16::from_le_bytes(data[pos+10..pos+12].try_into().ok()?);
        let compressed_size = u32::from_le_bytes(data[pos+20..pos+24].try_into().ok()?) as usize;
        let name_len = u16::from_le_bytes(data[pos+28..pos+30].try_into().ok()?) as usize;
        let extra_len = u16::from_le_bytes(data[pos+30..pos+32].try_into().ok()?) as usize;
        let comment_len = u16::from_le_bytes(data[pos+32..pos+34].try_into().ok()?) as usize;
        let local_offset = u32::from_le_bytes(data[pos+42..pos+46].try_into().ok()?) as usize;

        let name = std::str::from_utf8(&data[pos+46..pos+46+name_len]).unwrap_or("");

        if !name.ends_with('/') {
            if let Some(raw_data) = decompress_zip_entry(data, local_offset, compression, compressed_size) {
                if !raw_data.is_empty() {
                    results.push(ArchiveEntry {
                        relative_path: format!("{}/{}", jar_name, name),
                        data: raw_data,
                        pkg_type: None,
                        repo: None,
                    });
                }
            }
        }

        pos += 46 + name_len + extra_len + comment_len;
    }

    if results.is_empty() { None } else { Some(results) }
}

fn find_eocd_bench(data: &[u8]) -> Option<usize> {
    let start = data.len().saturating_sub(65557);
    for i in (start..data.len().saturating_sub(21)).rev() {
        if u32::from_le_bytes(data[i..i+4].try_into().ok()?) == 0x06054b50 {
            return Some(i);
        }
    }
    None
}

fn decompress_zip_entry(data: &[u8], offset: usize, compression: u16, comp_size: usize) -> Option<Vec<u8>> {
    if offset + 30 > data.len() { return None; }
    let sig = u32::from_le_bytes(data[offset..offset+4].try_into().ok()?);
    if sig != 0x04034b50 { return None; }

    let name_len = u16::from_le_bytes(data[offset+26..offset+28].try_into().ok()?) as usize;
    let extra_len = u16::from_le_bytes(data[offset+28..offset+30].try_into().ok()?) as usize;
    let data_start = offset + 30 + name_len + extra_len;

    if data_start + comp_size > data.len() { return None; }
    let compressed = &data[data_start..data_start + comp_size];

    match compression {
        0 => Some(compressed.to_vec()),
        8 => miniz_oxide::inflate::decompress_to_vec(compressed).ok(),
        _ => None,
    }
}

#[test]
#[ignore]
#[cfg(feature = "resolve")]
fn perf_real_java_deps() -> Result<()> {
    let java_dir = prepare_java_deps();
    if !java_dir.join(".done").exists() {
        eprintln!("Java deps not available, skipping");
        return Ok(());
    }

    let entries = collect_files_recursive(&java_dir);
    let entries: Vec<_> = entries.into_iter().filter(|e| e.relative_path.ends_with(".jar")).collect();
    let jar_count = entries.len();
    if entries.is_empty() {
        eprintln!("No Java deps found, skipping");
        return Ok(());
    }

    let total: u64 = entries.iter().map(|e| e.data.len() as u64).sum();
    println!("  Java deps: {} JARs, {:.1} MB", jar_count, total as f64 / (1024.0 * 1024.0));

    // 1) JARs as-is — skipped (already compressed)
    let result = bench_roundtrip("java_jars_skipped", entries)?.with_file_count(jar_count);
    print_single_result(&result);

    // 2) Raw extracted contents — real compression
    let raw_dir = prepare_java_raw(&java_dir);
    let raw_entries = collect_files_recursive(&raw_dir);
    let raw_count = raw_entries.len();
    let raw_total: u64 = raw_entries.iter().map(|e| e.data.len() as u64).sum();
    println!("  Raw Java: {} files, {:.1} MB", raw_count, raw_total as f64 / (1024.0 * 1024.0));

    let result = bench_roundtrip("java_raw_compressed", raw_entries)?.with_file_count(raw_count);
    print_single_result(&result);

    Ok(())
}

/// Benchmark .crate files from the local cargo registry cache.
/// These are .tar.gz internally and should be skipped by compression.
#[test]
#[ignore]
fn perf_real_rust_crates() -> Result<()> {
    let cache_dir = dirs::home_dir()
        .expect("no home dir")
        .join(".cargo/registry/cache");

    if !cache_dir.exists() {
        eprintln!("No cargo registry cache found at {}, skipping", cache_dir.display());
        return Ok(());
    }

    let mut entries = Vec::new();
    for index_dir in fs::read_dir(&cache_dir)? {
        let index_dir = index_dir?;
        if !index_dir.file_type()?.is_dir() { continue; }
        for entry in walkdir::WalkDir::new(index_dir.path()).into_iter().filter_map(|e| e.ok()) {
            if !entry.file_type().is_file() { continue; }
            let path = entry.path();
            if !path.to_string_lossy().ends_with(".crate") { continue; }
            if let Ok(data) = fs::read(path) {
                let rel = path.file_name().unwrap_or_default().to_string_lossy().to_string();
                entries.push(ArchiveEntry {
                    relative_path: rel,
                    data,
                pkg_type: None,
                repo: None,
                });
            }
        }
    }

    let count = entries.len();
    if entries.is_empty() {
        eprintln!("No .crate files found, skipping");
        return Ok(());
    }

    let total: u64 = entries.iter().map(|e| e.data.len() as u64).sum();
    println!("  Rust crates: {} files, {:.1} MB", count, total as f64 / (1024.0 * 1024.0));

    let result = bench_roundtrip("rust_crates", entries)?.with_file_count(count);
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

    // Rust source is mostly .rs/.toml — already compressible, not skipped
    let result = bench_roundtrip("rust_deps_real", entries)?.with_file_count(count);
    print_single_result(&result);

    Ok(())
}
