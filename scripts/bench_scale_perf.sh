#!/usr/bin/env bash
set -euo pipefail

# Runs perf sweeps over scaled datasets for Rust and/or Elasticsearch.
#
# Defaults:
# - FACTORS="25 50 100"
# - CONCURRENCY=32, WARMUP_S=10, DURATION_S=30
# - WORKLOAD=bench/workloads/hot_len3_k10.jsonl
# - RESULT_DIR=bench/results
# - PORT=50051, ENDPOINT=127.0.0.1:50051
# - RUST_BACKEND=fst (or trie)
# - RUN_RUST=1, RUN_ES=1
# - ES_ENDPOINT=http://127.0.0.1:9200, ES_INDEX=prefix_words
# - ES_HEAP=4g
#
# Example:
#   ./scripts/bench_scale_perf.sh

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

FACTORS="${FACTORS:-25 50 100}"
CONCURRENCY="${CONCURRENCY:-32}"
WARMUP_S="${WARMUP_S:-10}"
DURATION_S="${DURATION_S:-30}"
WORKLOAD="${WORKLOAD:-bench/workloads/hot_len3_k10.jsonl}"
RESULT_DIR="${RESULT_DIR:-bench/results}"
PORT="${PORT:-50051}"
ENDPOINT="${ENDPOINT:-127.0.0.1:${PORT}}"
RUST_BACKEND="${RUST_BACKEND:-fst}"
RUN_RUST="${RUN_RUST:-1}"
RUN_ES="${RUN_ES:-1}"
ES_ENDPOINT="${ES_ENDPOINT:-http://127.0.0.1:9200}"
ES_INDEX="${ES_INDEX:-prefix_words}"
ES_HEAP="${ES_HEAP:-4g}"

mkdir -p "$RESULT_DIR"

SERVER_LAUNCH_PID=""
SERVER_DATA_DIR=""
cleanup() {
  if [[ -n "${SERVER_LAUNCH_PID}" ]]; then
    kill "${SERVER_LAUNCH_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_LAUNCH_PID}" >/dev/null 2>&1 || true
    SERVER_LAUNCH_PID=""
  fi
  if [[ -n "${SERVER_DATA_DIR}" && -d "${SERVER_DATA_DIR}" ]]; then
    rm -rf "${SERVER_DATA_DIR}" >/dev/null 2>&1 || true
    SERVER_DATA_DIR=""
  fi
}
trap cleanup EXIT

ensure_scaled_wordlist() {
  local factor="$1"
  local out="bench/data/words_alpha_x${factor}.txt"
  if [[ -f "$out" ]]; then
    return 0
  fi
  mkdir -p bench/data
  ./scripts/generate_scaled_wordlist.py \
    --input ./words_alpha.txt \
    --output "$out" \
    --factor "$factor"
}

wait_rust_ready() {
  local endpoint="$1"
  for _ in $(seq 1 180); do
    if grpcurl -plaintext -import-path ./proto -proto ftrie.proto \
      -d '{"name":"app","tenant":"power","top_k":10}' \
      "$endpoint" ftrie.PrefixMatcher/GetPrefixMatch >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

kill_listener_on_port() {
  local port="$1"
  local pids
  pids="$(lsof -nP -tiTCP:${port} -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -z "${pids}" ]]; then
    return 0
  fi
  kill ${pids} >/dev/null 2>&1 || true
  sleep 0.2
  pids="$(lsof -nP -tiTCP:${port} -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -n "${pids}" ]]; then
    kill -9 ${pids} >/dev/null 2>&1 || true
  fi
}

start_rust_server() {
  local wordlist="$1"
  local log="$2"
  kill_listener_on_port "$PORT"
  SERVER_DATA_DIR="$(mktemp -d "/tmp/ftrie-perf-${PORT}-XXXXXX")"
  (
    export BIND_ADDR="127.0.0.1:${PORT}"
    export PREFIX_RANGE="a-z"
    export DATA_DIR="${SERVER_DATA_DIR}"
    export FSYNC="0"
    export INCLUDE_NODE_ID_IN_REPLY="0"
    export NODE_ID="node"
    export DISABLE_STATIC_INDEX="0"
    export RAFT_ENABLED="1"
    export RAFT_NODE_ID="1"
    export RAFT_BOOTSTRAP="1"
    export RAFT_MEMBERS="1=http://127.0.0.1:${PORT}"
    export INCLUDE_MESSAGE="0"
    export DEFAULT_TOP_K="0"
    export INDEX_BACKEND="${RUST_BACKEND}"
    export WORDS_THOUGHTSPOT_FILE="./words.txt"
    export WORDS_POWER_FILE="${wordlist}"
    ./target/debug/ftrie-server
  ) >"${log}" 2>&1 &
  SERVER_LAUNCH_PID="$!"
}

if [[ "${RUN_RUST}" == "1" ]]; then
  cargo build --bin ftrie-server >/dev/null
fi

for factor in $FACTORS; do
  echo "=== [x${factor}] prepare wordlist ==="
  ensure_scaled_wordlist "$factor"
  wordlist="./bench/data/words_alpha_x${factor}.txt"
  wc -l "$wordlist" | awk '{print "rows="$1" file='"$wordlist"'"}'

  if [[ "${RUN_RUST}" == "1" ]]; then
    echo "=== [x${factor}] rust benchmark (${RUST_BACKEND}) ==="
    rust_out="${RESULT_DIR}/scale_x${factor}_rust_${RUST_BACKEND}.json"
    rust_log="/tmp/ftrie_scale_x${factor}_rust_${RUST_BACKEND}.log"
    start_rust_server "$wordlist" "$rust_log"
    if ! wait_rust_ready "$ENDPOINT"; then
      echo "ERROR: Rust server did not become ready for x${factor}"
      tail -n 120 "$rust_log" || true
      exit 1
    fi
    python3 bench/bench.py run \
      --endpoint "$ENDPOINT" \
      --concurrency "$CONCURRENCY" \
      --warmup-s "$WARMUP_S" \
      --duration-s "$DURATION_S" \
      --deployment single_node_raft \
      --nodes 1 \
      --deployment-notes "INDEX_BACKEND=${RUST_BACKEND}; words_factor=${factor}; workload=$(basename "$WORKLOAD" .jsonl)" \
      --output "$rust_out" \
      "$WORKLOAD"
    cleanup
  fi

  if [[ "${RUN_ES}" == "1" ]]; then
    echo "=== [x${factor}] es ingest ==="
    WORDLIST="$wordlist" ES_HEAP="$ES_HEAP" ./scripts/run_local_es_single_node.sh

    echo "=== [x${factor}] es benchmark ==="
    es_out="${RESULT_DIR}/scale_x${factor}_es.json"
    python3 bench/bench.py run-es \
      --endpoint "$ES_ENDPOINT" \
      --index "$ES_INDEX" \
      --concurrency "$CONCURRENCY" \
      --warmup-s "$WARMUP_S" \
      --duration-s "$DURATION_S" \
      --deployment single_node_es \
      --nodes 1 \
      --deployment-notes "docker; heap=${ES_HEAP}; words_factor=${factor}; query=prefix_keyword" \
      --output "$es_out" \
      "$WORKLOAD"
  fi
done

echo "=== sweep complete ==="
