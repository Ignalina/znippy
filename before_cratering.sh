#!/usr/bin/env bash
# Run before publishing crates. All tests must pass; synthetic benchmarks print numbers.
# Usage: ./before_cratering.sh           — quick + synthetic
#        REAL=1 ./before_cratering.sh    — also run real-world benchmarks (network, ~5 min)
set -e

echo "=== workspace tests ==="
cargo test --workspace

echo ""
echo "=== synthetic performance suite ==="
cargo test --release -p znippy-tests --test perf_bench perf_benchmark_suite -- --nocapture

echo ""
echo "=== Maven plugin unit tests ==="
cargo test --release -p znippy-tests --test maven_bench -- --nocapture

if [[ "${REAL:-0}" == "1" ]]; then
    echo ""
    echo "=== real-world benchmarks (network, caches to /tmp/znippy-bench-cache/) ==="
    cargo test --release -p znippy-tests --test perf_bench -- --ignored --nocapture
fi

echo ""
echo "✅ All clear — safe to crate."
