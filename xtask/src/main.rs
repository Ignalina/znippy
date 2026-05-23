use std::process::{Command, ExitStatus};

fn run(args: &[&str]) -> ExitStatus {
    let status = Command::new("cargo")
        .args(args)
        .status()
        .unwrap_or_else(|e| panic!("failed to run cargo {}: {}", args[0], e));
    if !status.success() {
        eprintln!("\n✗ cargo {} failed", args[0]);
        std::process::exit(1);
    }
    status
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
