//! Tests and benchmarks for maven metadata extraction.
//!
//! Compares two native paths:
//!   - NativeMavenPlugin: calls ljar::decompress_jar_filter (rayon, multi-core)
//!   - extract_maven_metadata: minimal single-threaded JAR parser (fallback path)
//!
//! WASM plugin benchmarks require a compiled maven.wasm and are documented below:
//!   cargo build --target wasm32-unknown-unknown --release -p znippy-plugin-maven
//!   # then use WasmPlugin::load("target/.../maven.wasm", "maven", 1)

use znippy_common::plugin::{ArchiveTypePlugin, ExtensionValue};
use znippy_plugin_maven::{NativeMavenPlugin, extract_maven_metadata};
use std::time::Instant;

// ─── Synthetic JAR builder ───────────────────────────────────────────────────

fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 { (crc >> 1) ^ 0xEDB8_8320 } else { crc >> 1 };
        }
    }
    !crc
}

/// Build a minimal valid ZIP (JAR) with a single stored entry.
fn build_zip_stored(entry_name: &str, entry_data: &[u8]) -> Vec<u8> {
    let name_bytes = entry_name.as_bytes();
    let crc = crc32(entry_data);
    let size = entry_data.len() as u32;

    let mut out = Vec::new();

    // Local file header
    let local_offset = out.len() as u32;
    out.extend_from_slice(b"PK\x03\x04"); // signature
    out.extend_from_slice(&20u16.to_le_bytes()); // version needed
    out.extend_from_slice(&0u16.to_le_bytes()); // flags
    out.extend_from_slice(&0u16.to_le_bytes()); // compression: stored
    out.extend_from_slice(&0u16.to_le_bytes()); // mod time
    out.extend_from_slice(&0u16.to_le_bytes()); // mod date
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&size.to_le_bytes()); // compressed size
    out.extend_from_slice(&size.to_le_bytes()); // uncompressed size
    out.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes()); // extra field length
    out.extend_from_slice(name_bytes);
    out.extend_from_slice(entry_data);

    // Central directory header
    let cd_offset = out.len() as u32;
    out.extend_from_slice(b"PK\x01\x02"); // signature
    out.extend_from_slice(&20u16.to_le_bytes()); // version made by
    out.extend_from_slice(&20u16.to_le_bytes()); // version needed
    out.extend_from_slice(&0u16.to_le_bytes()); // flags
    out.extend_from_slice(&0u16.to_le_bytes()); // compression: stored
    out.extend_from_slice(&0u16.to_le_bytes()); // mod time
    out.extend_from_slice(&0u16.to_le_bytes()); // mod date
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&size.to_le_bytes()); // compressed size
    out.extend_from_slice(&size.to_le_bytes()); // uncompressed size
    out.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes()); // extra field length
    out.extend_from_slice(&0u16.to_le_bytes()); // comment length
    out.extend_from_slice(&0u16.to_le_bytes()); // disk start
    out.extend_from_slice(&0u16.to_le_bytes()); // internal attr
    out.extend_from_slice(&0u32.to_le_bytes()); // external attr
    out.extend_from_slice(&local_offset.to_le_bytes());
    out.extend_from_slice(name_bytes);

    // End of central directory
    let cd_size = out.len() as u32 - cd_offset;
    out.extend_from_slice(b"PK\x05\x06"); // signature
    out.extend_from_slice(&0u16.to_le_bytes()); // disk number
    out.extend_from_slice(&0u16.to_le_bytes()); // disk with cd
    out.extend_from_slice(&1u16.to_le_bytes()); // entries on disk
    out.extend_from_slice(&1u16.to_le_bytes()); // total entries
    out.extend_from_slice(&cd_size.to_le_bytes());
    out.extend_from_slice(&cd_offset.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes()); // comment length

    out
}

fn make_test_jar(group_id: &str, artifact_id: &str, version: &str) -> Vec<u8> {
    let pom_xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <project>\n  \
           <groupId>{}</groupId>\n  \
           <artifactId>{}</artifactId>\n  \
           <version>{}</version>\n  \
           <packaging>jar</packaging>\n\
         </project>",
        group_id, artifact_id, version
    );
    let pom_path = format!("META-INF/maven/{}/{}/pom.xml", group_id, artifact_id);
    build_zip_stored(&pom_path, pom_xml.as_bytes())
}

// ─── Correctness tests ───────────────────────────────────────────────────────

#[test]
fn maven_native_extracts_gav_from_jar() {
    let jar = make_test_jar("com.example", "mylib", "1.2.3");
    let plugin = NativeMavenPlugin;

    let row = plugin.extract_metadata("mylib-1.2.3.jar", &jar)
        .expect("NativeMavenPlugin should extract GAV from synthetic JAR");

    assert_eq!(row.fields.get("group_id"),    Some(&ExtensionValue::Str("com.example".into())));
    assert_eq!(row.fields.get("artifact_id"), Some(&ExtensionValue::Str("mylib".into())));
    assert_eq!(row.fields.get("version"),     Some(&ExtensionValue::Str("1.2.3".into())));
    assert_eq!(row.fields.get("packaging"),   Some(&ExtensionValue::Str("jar".into())));
}

#[test]
fn maven_fallback_extracts_gav_from_jar() {
    // extract_maven_metadata uses the minimal single-threaded JAR parser
    let jar = make_test_jar("org.apache", "commons-lang3", "3.14.0");

    let meta = extract_maven_metadata("commons-lang3-3.14.0.jar", &jar)
        .expect("fallback parser should extract GAV from synthetic JAR");

    assert_eq!(meta.group_id,    "org.apache");
    assert_eq!(meta.artifact_id, "commons-lang3");
    assert_eq!(meta.version,     "3.14.0");
}

#[test]
fn maven_native_extracts_from_pom_file() {
    let pom = b"<project>\
        <groupId>io.grpc</groupId>\
        <artifactId>grpc-core</artifactId>\
        <version>1.63.0</version>\
        </project>";
    let plugin = NativeMavenPlugin;

    let row = plugin.extract_metadata("grpc-core-1.63.0.pom", pom)
        .expect("NativeMavenPlugin should parse .pom directly");

    assert_eq!(row.fields.get("group_id"),    Some(&ExtensionValue::Str("io.grpc".into())));
    assert_eq!(row.fields.get("artifact_id"), Some(&ExtensionValue::Str("grpc-core".into())));
    assert_eq!(row.fields.get("version"),     Some(&ExtensionValue::Str("1.63.0".into())));
}

#[test]
fn maven_native_returns_none_for_jar_without_pom() {
    // A JAR with only a MANIFEST.MF — no pom.xml anywhere
    let jar = build_zip_stored("META-INF/MANIFEST.MF", b"Manifest-Version: 1.0\n");
    let plugin = NativeMavenPlugin;

    assert!(
        plugin.extract_metadata("nopom.jar", &jar).is_none(),
        "JAR without pom.xml should return None"
    );
}

#[test]
fn maven_native_matches_path() {
    let plugin = NativeMavenPlugin;
    assert!(plugin.matches_path("org/example/lib-1.0.jar"));
    assert!(plugin.matches_path("repo/app.war"));
    assert!(plugin.matches_path("lib.ear"));
    assert!(plugin.matches_path("artifact-1.0.pom"));
    assert!(!plugin.matches_path("some-crate-1.0.crate"));
    assert!(!plugin.matches_path("Cargo.toml"));
}

#[test]
fn maven_both_paths_agree() {
    // Both native (ljar) and fallback (minimal) should produce the same result
    let cases = [
        ("com.google.guava",    "guava",             "33.2.0-jre"),
        ("org.springframework", "spring-core",       "6.1.8"),
        ("software.amazon.awssdk", "s3",             "2.25.40"),
        ("io.netty",            "netty-all",         "4.1.109.Final"),
    ];

    for (group, artifact, version) in cases {
        let jar = make_test_jar(group, artifact, version);

        let native_row = NativeMavenPlugin.extract_metadata(
            &format!("{}-{}.jar", artifact, version), &jar
        );
        let fallback = extract_maven_metadata(
            &format!("{}-{}.jar", artifact, version), &jar
        );

        let native_row = native_row.expect(&format!("native failed for {}", artifact));
        let fallback   = fallback.expect(&format!("fallback failed for {}", artifact));

        assert_eq!(
            native_row.fields.get("group_id"),
            Some(&ExtensionValue::Str(fallback.group_id.clone())),
            "group_id mismatch for {}",
            artifact
        );
        assert_eq!(
            native_row.fields.get("artifact_id"),
            Some(&ExtensionValue::Str(fallback.artifact_id.clone())),
            "artifact_id mismatch for {}",
            artifact
        );
        assert_eq!(
            native_row.fields.get("version"),
            Some(&ExtensionValue::Str(fallback.version.clone())),
            "version mismatch for {}",
            artifact
        );
    }
}

// ─── Throughput benchmarks (run with: cargo test -p znippy-tests -- maven --include-ignored --nocapture) ─

#[test]
#[ignore]
fn maven_bench_native_throughput() {
    let jar = make_test_jar("com.example", "biglib", "2.0.0");
    let plugin = NativeMavenPlugin;
    let iterations = 2000;

    let t0 = Instant::now();
    for _ in 0..iterations {
        let _ = plugin.extract_metadata("biglib-2.0.0.jar", &jar);
    }
    let elapsed = t0.elapsed();

    println!("\nNativeMavenPlugin (ljar path):");
    println!("  {} iterations in {}ms = {:.1}μs/call",
        iterations,
        elapsed.as_millis(),
        elapsed.as_micros() as f64 / iterations as f64,
    );
}

#[test]
#[ignore]
fn maven_bench_fallback_throughput() {
    let jar = make_test_jar("com.example", "biglib", "2.0.0");
    let iterations = 2000;

    let t0 = Instant::now();
    for _ in 0..iterations {
        let _ = extract_maven_metadata("biglib-2.0.0.jar", &jar);
    }
    let elapsed = t0.elapsed();

    println!("\nextract_maven_metadata (minimal single-threaded path):");
    println!("  {} iterations in {}ms = {:.1}μs/call",
        iterations,
        elapsed.as_millis(),
        elapsed.as_micros() as f64 / iterations as f64,
    );
}

#[test]
#[ignore]
fn maven_bench_compare() {
    // Side-by-side comparison of native vs fallback on same JAR
    let jar = make_test_jar("com.example", "biglib", "2.0.0");
    let plugin = NativeMavenPlugin;
    let iterations = 5000;

    // Warmup
    for _ in 0..100 {
        let _ = plugin.extract_metadata("biglib.jar", &jar);
        let _ = extract_maven_metadata("biglib.jar", &jar);
    }

    let t_native = {
        let t = Instant::now();
        for _ in 0..iterations {
            let _ = plugin.extract_metadata("biglib-2.0.0.jar", &jar);
        }
        t.elapsed()
    };

    let t_fallback = {
        let t = Instant::now();
        for _ in 0..iterations {
            let _ = extract_maven_metadata("biglib-2.0.0.jar", &jar);
        }
        t.elapsed()
    };

    println!("\n=== Maven Plugin Throughput Comparison ({} iterations) ===", iterations);
    println!("NativeMavenPlugin (ljar):          {:>8.1}μs/call  ({:.0}ms total)",
        t_native.as_micros() as f64 / iterations as f64,
        t_native.as_millis());
    println!("extract_maven_metadata (fallback): {:>8.1}μs/call  ({:.0}ms total)",
        t_fallback.as_micros() as f64 / iterations as f64,
        t_fallback.as_millis());
    println!("Speedup: {:.2}x",
        t_fallback.as_micros() as f64 / t_native.as_micros() as f64);
    println!();
    println!("Note: WASM plugin benchmarks require building maven.wasm:");
    println!("  cargo build --target wasm32-unknown-unknown --release -p znippy-plugin-maven");
    println!("  Then: WasmPlugin::load(\"target/wasm32-unknown-unknown/release/znippy_plugin_maven.wasm\", \"maven\", 1)");
}
