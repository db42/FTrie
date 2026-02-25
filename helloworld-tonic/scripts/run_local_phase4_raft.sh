#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

trap 'jobs -p | xargs -r kill; wait || true' EXIT

echo "Building..."
cargo build --bins >/dev/null

R="${R:-1}"
# Set CLEAN=1 to wipe persisted raft state before starting.
CLEAN="${CLEAN:-0}"
# Optional: set TOP_K (e.g. TOP_K=10) to include it in SayHello grpcurl examples.
TOP_K="${TOP_K:-}"
# Optional: set DEFAULT_TOP_K (e.g. DEFAULT_TOP_K=10) to change server-side default when
# a client omits top_k (proto3 default 0).
DEFAULT_TOP_K="${DEFAULT_TOP_K:-}"

LB_PORT="${LB_PORT:-50052}"
N1_PORT="${N1_PORT:-50053}"
N2_PORT="${N2_PORT:-50063}"
N3_PORT="${N3_PORT:-50073}"

N1_ADDR="http://127.0.0.1:${N1_PORT}"
N2_ADDR="http://127.0.0.1:${N2_PORT}"
N3_ADDR="http://127.0.0.1:${N3_PORT}"

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

  # Force kill any that didn't exit.
  pids="$(lsof -nP -tiTCP:"${port}" -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -n "${pids}" ]]; then
    kill -9 ${pids} >/dev/null 2>&1 || true
  fi
}

echo "Cleaning up existing listeners (if any)..."
kill_listeners_on_port "${LB_PORT}"
kill_listeners_on_port "${N1_PORT}"
kill_listeners_on_port "${N2_PORT}"
kill_listeners_on_port "${N3_PORT}"

# NOTE:
# - Raft state is persisted under DATA_DIR/raft (log.aof, vote.aof, committed.aof).
# - Use CLEAN=1 to wipe persisted raft state.
echo "Starting shard j-r (RF=3) with Raft..."
if [[ "${CLEAN}" == "1" ]]; then
  rm -rf ./data-raft >/dev/null 2>&1 || true
fi
mkdir -p ./data-raft/n1 ./data-raft/n2 ./data-raft/n3

RAFT_MEMBERS="1=${N1_ADDR},2=${N2_ADDR},3=${N3_ADDR}"

(
  export NODE_ID="n1"
  export BIND_ADDR="127.0.0.1:${N1_PORT}"
  export PREFIX_RANGE="j-r"
  export DATA_DIR="./data-raft/n1"
  export RAFT_FSYNC="0"
  export INCLUDE_NODE_ID_IN_REPLY="1"
  export DISABLE_STATIC_INDEX="1"
  if [[ -n "${DEFAULT_TOP_K}" ]]; then export DEFAULT_TOP_K="${DEFAULT_TOP_K}"; fi

  export RAFT_ENABLED="1"
  export RAFT_NODE_ID="1"
  if [[ "${CLEAN}" == "1" ]]; then
    export RAFT_BOOTSTRAP="1"
  else
    export RAFT_BOOTSTRAP="0"
  fi
  export RAFT_MEMBERS="$RAFT_MEMBERS"

  ./target/debug/helloworld-server
) &
PID_N1=$!

(
  export NODE_ID="n2"
  export BIND_ADDR="127.0.0.1:${N2_PORT}"
  export PREFIX_RANGE="j-r"
  export DATA_DIR="./data-raft/n2"
  export RAFT_FSYNC="0"
  export INCLUDE_NODE_ID_IN_REPLY="1"
  export DISABLE_STATIC_INDEX="1"
  if [[ -n "${DEFAULT_TOP_K}" ]]; then export DEFAULT_TOP_K="${DEFAULT_TOP_K}"; fi

  export RAFT_ENABLED="1"
  export RAFT_NODE_ID="2"
  export RAFT_MEMBERS="$RAFT_MEMBERS"

  ./target/debug/helloworld-server
) &
PID_N2=$!

(
  export NODE_ID="n3"
  export BIND_ADDR="127.0.0.1:${N3_PORT}"
  export PREFIX_RANGE="j-r"
  export DATA_DIR="./data-raft/n3"
  export RAFT_FSYNC="0"
  export INCLUDE_NODE_ID_IN_REPLY="1"
  export DISABLE_STATIC_INDEX="1"
  if [[ -n "${DEFAULT_TOP_K}" ]]; then export DEFAULT_TOP_K="${DEFAULT_TOP_K}"; fi

  export RAFT_ENABLED="1"
  export RAFT_NODE_ID="3"
  export RAFT_MEMBERS="$RAFT_MEMBERS"

  ./target/debug/helloworld-server
) &
PID_N3=$!

sleep 0.8

echo "Starting LB (Raft-only, R=$R)..."
(
  export BIND_ADDR="127.0.0.1:${LB_PORT}"
  export LB_POLICY="random"
  export LB_MAX_ATTEMPTS="6"
  export LB_HEALTH_INTERVAL_MS="500"
  export LB_FAIL_AFTER="1"
  export LB_RECOVER_AFTER="1"
  export READ_TIMEOUT_MS="300"
  export WRITE_TIMEOUT_MS="1200"
  export R="$R"
  export PARTITION_MAP="j-r=${N1_ADDR}|${N2_ADDR}|${N3_ADDR}"
  ./target/debug/helloworld-lb
) &
PID_LB=$!

echo "Ready:"
echo "  LB: http://127.0.0.1:${LB_PORT}"
if [[ "${CLEAN}" == "1" ]]; then
  echo "  (CLEAN=1: raft state wiped)"
fi
echo "  j-r replicas:"
echo "    n1: ${N1_PORT}(pid=$PID_N1)"
echo "    n2: ${N2_PORT}(pid=$PID_N2)"
echo "    n3: ${N3_PORT}(pid=$PID_N3)"
echo ""
echo "Write (PutWord) via LB, then read from all replicas:"
echo "  grpcurl -plaintext -import-path ./proto -proto helloworld.proto \\"
echo "    -d '{\"word\":\"jokerraft\",\"tenant\":\"power\"}' \\"
echo "    127.0.0.1:${LB_PORT} helloworld.Greeter/PutWord"
echo ""

HELLO_REQ='{"name":"joker","tenant":"power"}'
if [[ -n "${TOP_K}" ]]; then
  HELLO_REQ="{\"name\":\"joker\",\"tenant\":\"power\",\"top_k\":${TOP_K}}"
fi

echo "  grpcurl -plaintext -import-path ./proto -proto helloworld.proto \\"
echo "    -d '${HELLO_REQ}' \\"
echo "    127.0.0.1:${N1_PORT} helloworld.Greeter/SayHello"
echo ""
echo "  grpcurl -plaintext -import-path ./proto -proto helloworld.proto \\"
echo "    -d '${HELLO_REQ}' \\"
echo "    127.0.0.1:${N2_PORT} helloworld.Greeter/SayHello"
echo ""
echo "  grpcurl -plaintext -import-path ./proto -proto helloworld.proto \\"
echo "    -d '${HELLO_REQ}' \\"
echo "    127.0.0.1:${N3_PORT} helloworld.Greeter/SayHello"
echo ""
echo "Try leader redirection (send PutWord to a follower directly):"
echo "  grpcurl -plaintext -import-path ./proto -proto helloworld.proto \\"
echo "    -d '{\"word\":\"jokerraft2\",\"tenant\":\"power\"}' \\"
echo "    127.0.0.1:${N2_PORT} helloworld.Greeter/PutWord"
echo ""
echo "Failure test:"
echo "  kill $PID_N2"
echo "  (writes should still succeed; Raft needs a majority of 3 = 2)"

wait
