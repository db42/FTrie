#!/usr/bin/env bash
set -euo pipefail

# Generates the 10x Rust trie perf artifact:
# - bench/results/scale_x10_rust_trie.json
#
# This is a thin wrapper around bench_scale_perf.sh so reruns are one command.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUST_BACKEND=trie RUN_ES=0 FACTORS=10 ./scripts/bench_scale_perf.sh

echo "artifact=bench/results/scale_x10_rust_trie.json"
