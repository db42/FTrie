#!/usr/bin/env bash
set -euo pipefail

# Runs memory sweep for Rust and/or Elasticsearch over scaled datasets.
#
# Defaults:
# - FACTORS="25 50 100"
# - PORT=50051, ENDPOINT=127.0.0.1:50051
# - RESULT_DIR=bench/results
# - RUST_BACKEND=fst (or trie)
# - RUN_RUST=1, RUN_ES=1
# - ES_CONTAINER_NAME=ftrie-es-bench, ES_HEAP=4g
# - APPEND=0 (set 1 to append to existing OUT file)
#
# Output:
# - TSV with columns:
#   factor, rust_backend, rust_rss_kb, es_vmrss_kb, es_memusage_sample
#
# Example:
#   ./scripts/bench_scale_memory.sh

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

FACTORS="${FACTORS:-25 50 100}"
PORT="${PORT:-50051}"
ENDPOINT="${ENDPOINT:-127.0.0.1:${PORT}}"
RESULT_DIR="${RESULT_DIR:-bench/results}"
RUST_BACKEND="${RUST_BACKEND:-fst}"
RUN_RUST="${RUN_RUST:-1}"
RUN_ES="${RUN_ES:-1}"
ES_CONTAINER_NAME="${ES_CONTAINER_NAME:-ftrie-es-bench}"
ES_HEAP="${ES_HEAP:-4g}"

mkdir -p "$RESULT_DIR"
APPEND="${APPEND:-0}"
OUT="${OUT:-${RESULT_DIR}/memory_scale_$(date +%Y%m%d_%H%M%S).tsv}"
if [[ "$APPEND" == "1" && -f "$OUT" ]]; then
  :
else
  echo -e "factor\trust_backend\trust_rss_kb\tes_vmrss_kb\tes_memusage_sample" > "$OUT"
fi

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
  SERVER_DATA_DIR="$(mktemp -d "/tmp/ftrie-mem-${PORT}-XXXXXX")"
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

  rust_rss_kb="-"
  if [[ "${RUN_RUST}" == "1" ]]; then
    echo "=== [x${factor}] rust rss (${RUST_BACKEND}) ==="
    rust_log="/tmp/ftrie_mem_x${factor}_rust_${RUST_BACKEND}.log"
    start_rust_server "$wordlist" "$rust_log"
    if ! wait_rust_ready "$ENDPOINT"; then
      echo "ERROR: Rust server did not become ready for x${factor}"
      tail -n 120 "$rust_log" || true
      exit 1
    fi

    server_pid="$(lsof -nP -tiTCP:${PORT} -sTCP:LISTEN | head -n1)"
    if [[ -z "${server_pid:-}" ]]; then
      echo "ERROR: could not resolve Rust server PID on port ${PORT}"
      tail -n 120 "$rust_log" || true
      exit 1
    fi
    r1="$(ps -o rss= -p "$server_pid" | tr -d ' ')"
    sleep 2
    r2="$(ps -o rss= -p "$server_pid" | tr -d ' ')"
    sleep 2
    r3="$(ps -o rss= -p "$server_pid" | tr -d ' ')"
    rust_rss_kb="$(printf "%s\n%s\n%s\n" "$r1" "$r2" "$r3" | sort -n | tail -n1)"
    echo "x${factor} rust rss samples(kB): ${r1} ${r2} ${r3}; max=${rust_rss_kb}"
    cleanup
  fi

  es_vmrss_kb="-"
  es_memusage_sample="-"
  if [[ "${RUN_ES}" == "1" ]]; then
    echo "=== [x${factor}] es rss/container ==="
    WORDLIST="$wordlist" ES_CONTAINER_NAME="$ES_CONTAINER_NAME" ES_HEAP="$ES_HEAP" \
      ./scripts/run_local_es_single_node.sh

    es_pid="$(docker exec "$ES_CONTAINER_NAME" sh -lc "pgrep -f org.elasticsearch.bootstrap.Elasticsearch | head -n1")"
    if [[ -z "${es_pid:-}" ]]; then
      echo "ERROR: could not resolve Elasticsearch JVM PID"
      docker exec "$ES_CONTAINER_NAME" sh -lc "ps -eo pid,args | sed -n '1,120p'"
      exit 1
    fi
    es_vmrss_kb="$(docker exec "$ES_CONTAINER_NAME" sh -lc "awk '/VmRSS:/ {print \$2}' /proc/${es_pid}/status")"

    m1="$(docker stats --no-stream --format '{{.MemUsage}}' "$ES_CONTAINER_NAME")"
    sleep 2
    m2="$(docker stats --no-stream --format '{{.MemUsage}}' "$ES_CONTAINER_NAME")"
    sleep 2
    m3="$(docker stats --no-stream --format '{{.MemUsage}}' "$ES_CONTAINER_NAME")"
    es_memusage_sample="${m1}; ${m2}; ${m3}"

    echo "x${factor} es vmrss(kB): ${es_vmrss_kb}"
    echo "x${factor} es memusage samples: ${es_memusage_sample}"
  fi
  echo -e "${factor}\t${RUST_BACKEND}\t${rust_rss_kb}\t${es_vmrss_kb}\t${es_memusage_sample}" >> "$OUT"
done

echo "RESULT_TSV=${OUT}"
