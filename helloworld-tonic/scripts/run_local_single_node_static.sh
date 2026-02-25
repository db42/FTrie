#!/usr/bin/env bash
set -euo pipefail

# Single-node prefix service with built-in static word lists.
#
# Starts one `helloworld-server` that loads:
# - tenant=thoughtspot from ./words.txt
# - tenant=power      from ./words_alpha.txt
#
# Env knobs:
# - PORT (default 50051)
# - PREFIX_RANGE (default a-z)
# - DATA_DIR (default ./data-single)
# - FSYNC (default 0)
# - INCLUDE_NODE_ID_IN_REPLY (default 0)
# - NODE_ID (default node)
# - DISABLE_STATIC_INDEX (default 0; set 1 to disable preloading word lists)
# - RAFT_ENABLED (default 1; this server binary expects raft mode currently)
# - INCLUDE_MESSAGE (default 1; set 0 to skip formatting HelloReply.message)
# - DEFAULT_TOP_K (default 0; used when client omits top_k)
# - INDEX_BACKEND (default trie; set fst to use the Tier-4 FST index)
# - SKIP_BUILD (default 0)
# - TOP_K (optional; shown in grpcurl examples)

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

trap 'jobs -p | xargs -r kill; wait || true' EXIT

PORT="${PORT:-50051}"
PREFIX_RANGE="${PREFIX_RANGE:-a-z}"
DATA_DIR="${DATA_DIR:-./data-single}"
FSYNC="${FSYNC:-0}"
INCLUDE_NODE_ID_IN_REPLY="${INCLUDE_NODE_ID_IN_REPLY:-0}"
NODE_ID="${NODE_ID:-node}"
DISABLE_STATIC_INDEX="${DISABLE_STATIC_INDEX:-0}"
RAFT_ENABLED="${RAFT_ENABLED:-1}"
INCLUDE_MESSAGE="${INCLUDE_MESSAGE:-1}"
DEFAULT_TOP_K="${DEFAULT_TOP_K:-0}"
INDEX_BACKEND="${INDEX_BACKEND:-trie}"
SKIP_BUILD="${SKIP_BUILD:-0}"
TOP_K="${TOP_K:-}"

kill_listeners_on_port() {
  local port="$1"
  if ! command -v lsof >/dev/null 2>&1; then
    echo "WARN: lsof not found; cannot auto-kill listeners on port ${port}"
    return 0
  fi

  local pids
  pids="$(lsof -nP -tiTCP:"${port}" -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -z "${pids}" ]]; then
    return 0
  fi

  echo "Killing existing listeners on port ${port}: ${pids}"
  kill ${pids} >/dev/null 2>&1 || true
  sleep 0.2

  pids="$(lsof -nP -tiTCP:"${port}" -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -n "${pids}" ]]; then
    kill -9 ${pids} >/dev/null 2>&1 || true
  fi
}

echo "Cleaning up existing listeners (if any)..."
kill_listeners_on_port "${PORT}"

if [[ "${SKIP_BUILD}" != "1" ]]; then
  echo "Building..."
  cargo build --bin helloworld-server >/dev/null
fi

mkdir -p "${DATA_DIR}"

echo "Starting single node on :${PORT} (PREFIX_RANGE=${PREFIX_RANGE})..."
(
  export BIND_ADDR="127.0.0.1:${PORT}"
  export PREFIX_RANGE="${PREFIX_RANGE}"
  export DATA_DIR="${DATA_DIR}"
  export FSYNC="${FSYNC}"
  export INCLUDE_NODE_ID_IN_REPLY="${INCLUDE_NODE_ID_IN_REPLY}"
  export NODE_ID="${NODE_ID}"
  export DISABLE_STATIC_INDEX="${DISABLE_STATIC_INDEX}"
  export RAFT_ENABLED="${RAFT_ENABLED}"
  export RAFT_NODE_ID="${RAFT_NODE_ID:-1}"
  export RAFT_BOOTSTRAP="${RAFT_BOOTSTRAP:-1}"
  export RAFT_MEMBERS="${RAFT_MEMBERS:-1=http://127.0.0.1:${PORT}}"
  export INCLUDE_MESSAGE="${INCLUDE_MESSAGE}"
  export DEFAULT_TOP_K="${DEFAULT_TOP_K}"
  export INDEX_BACKEND="${INDEX_BACKEND}"

  ./target/debug/helloworld-server
) &
PID=$!

sleep 0.5

HELLO_REQ_POWER='{"name":"app","tenant":"power"}'
HELLO_REQ_TS='{"name":"app","tenant":"thoughtspot"}'
if [[ -n "${TOP_K}" ]]; then
  HELLO_REQ_POWER="{\"name\":\"app\",\"tenant\":\"power\",\"top_k\":${TOP_K}}"
  HELLO_REQ_TS="{\"name\":\"app\",\"tenant\":\"thoughtspot\",\"top_k\":${TOP_K}}"
fi

echo "Ready:"
echo "  server: 127.0.0.1:${PORT} (pid=${PID})"
echo "  DISABLE_STATIC_INDEX=${DISABLE_STATIC_INDEX} (0 means preloaded wordlists)"
echo ""
echo "Query examples:"
echo "  grpcurl -plaintext -import-path ./proto -proto helloworld.proto \\"
echo "    -d '${HELLO_REQ_POWER}' \\"
echo "    127.0.0.1:${PORT} helloworld.Greeter/SayHello"
echo ""
echo "  grpcurl -plaintext -import-path ./proto -proto helloworld.proto \\"
echo "    -d '${HELLO_REQ_TS}' \\"
echo "    127.0.0.1:${PORT} helloworld.Greeter/SayHello"
echo ""
echo "Stop:"
echo "  kill ${PID}"

wait
