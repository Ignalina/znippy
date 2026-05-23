use serde::{Deserialize, Serialize};
use std::process::Command;

#[derive(Serialize, Deserialize, Clone, Debug)]
struct BenchEntry {
    name: String,
    compress_mbs: f64,
    decompress_mbs: f64,
}

#[derive(Serialize, Deserialize)]
struct BenchRun {
    date: String,
    results: Vec<BenchEntry>,
}

fn run(args: &[&str]) {
    let status = Command::new("cargo")
        .args(args)
        .status()
        .unwrap_or_else(|e| panic!("failed to run cargo: {}", e));
    if !status.success() {
        eprintln!("\n✗ cargo {} failed", args[0]);
        std::process::exit(1);
    }
}

fn today() -> String {
    Command::new("date")
        .arg("+%Y-%m-%d")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn check_and_record(tmp_file: &str, history_file: &str) {
    let current_json = match std::fs::read_to_string(tmp_file) {
        Ok(s) => s,
        Err(_) => {
            eprintln!("warn: no bench results found at {} — skipping history", tmp_file);
            return;
        }
    };

    let current: Vec<BenchEntry> = match serde_json::from_str(&current_json) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("warn: could not parse bench results: {} — skipping history", e);
            return;
        }
    };

    // Load history and compare against last run
    let history_raw = std::fs::read_to_string(history_file).unwrap_or_default();
    let baseline: Option<Vec<BenchEntry>> = history_raw
        .lines()
        .filter(|l| !l.trim().is_empty())
        .last()
        .and_then(|line| serde_json::from_str::<BenchRun>(line).ok())
        .map(|run| run.results);

    if let Some(baseline) = &baseline {
        let mut regressions = Vec::new();
        for curr in &current {
            if let Some(base) = baseline.iter().find(|b| b.name == curr.name) {
                let threshold = 0.80;
                if curr.compress_mbs < base.compress_mbs * threshold {
                    let drop = (1.0 - curr.compress_mbs / base.compress_mbs) * 100.0;
                    regressions.push(format!(
                        "  {} compress: {:.1} → {:.1} MB/s  ({:.0}% drop)",
                        curr.name, base.compress_mbs, curr.compress_mbs, drop
                    ));
                }
                if curr.decompress_mbs < base.decompress_mbs * threshold {
                    let drop = (1.0 - curr.decompress_mbs / base.decompress_mbs) * 100.0;
                    regressions.push(format!(
                        "  {} decompress: {:.1} → {:.1} MB/s  ({:.0}% drop)",
                        curr.name, base.decompress_mbs, curr.decompress_mbs, drop
                    ));
                }
            }
        }
        if !regressions.is_empty() {
            eprintln!("\n✗ Performance regression detected (>20% drop):");
            for r in &regressions {
                eprintln!("{}", r);
            }
            std::process::exit(1);
        }
        println!("  bench ok — no regressions vs last run");
    } else {
        println!("  bench ok — no baseline yet, recording first run");
    }

    // Append new run to history
    let new_run = BenchRun { date: today(), results: current };
    let line = serde_json::to_string(&new_run).expect("serialize bench run");
    let mut content = history_raw;
    if !content.is_empty() && !content.ends_with('\n') {
        content.push('\n');
    }
    content.push_str(&line);
    content.push('\n');
    std::fs::write(history_file, &content).expect("write bench_history.json");
    println!("  appended to {}", history_file);
}

fn main() {
    let real = std::env::args().any(|a| a == "--real");

    println!("=== workspace tests ===");
    run(&["test", "--workspace"]);

    println!("\n=== synthetic performance suite ===");
    run(&[
        "test", "--release", "-p", "znippy-tests",
        "--test", "perf_bench", "perf_benchmark_suite",
        "--", "--nocapture",
    ]);
    check_and_record("/tmp/znippy_bench_last.json", "bench_history.json");

    println!("\n=== Maven plugin tests ===");
    run(&[
        "test", "--release", "-p", "znippy-tests",
        "--test", "maven_bench",
        "--", "--nocapture",
    ]);

    if real {
        println!("\n=== real-world benchmarks (network, caches to /tmp/znippy-bench-cache/) ===");
        run(&[
            "test", "--release", "-p", "znippy-tests",
            "--test", "perf_bench",
            "--", "--ignored", "--nocapture",
        ]);
    } else {
        println!("\n(skip real-world benchmarks — pass --real to include)");
    }

    println!("\n✅ All clear — safe to crate.");
}
