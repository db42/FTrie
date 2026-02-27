#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

trap 'jobs -p | xargs -r kill; wait || true' EXIT

echo "Building..."
cargo build --bins >/dev/null

echo "Starting servers (RF=2 per shard)..."

# a-i
(
  export NODE_ID="a-i-r1"
  export BIND_ADDR="127.0.0.1:50051"
  export PREFIX_RANGE="a-i"
  export INCLUDE_NODE_ID_IN_REPLY="1"
  ./target/debug/ftrie-server
) &
PID_AI_1=$!

(
  export NODE_ID="a-i-r2"
  export BIND_ADDR="127.0.0.1:50061"
  export PREFIX_RANGE="a-i"
  export INCLUDE_NODE_ID_IN_REPLY="1"
  ./target/debug/ftrie-server
) &
PID_AI_2=$!

# j-r
(
  export NODE_ID="j-r-r1"
  export BIND_ADDR="127.0.0.1:50053"
  export PREFIX_RANGE="j-r"
  export INCLUDE_NODE_ID_IN_REPLY="1"
  ./target/debug/ftrie-server
) &
PID_JR_1=$!

(
  export NODE_ID="j-r-r2"
  export BIND_ADDR="127.0.0.1:50063"
  export PREFIX_RANGE="j-r"
  export INCLUDE_NODE_ID_IN_REPLY="1"
  ./target/debug/ftrie-server
) &
PID_JR_2=$!

# s-z
(
  export NODE_ID="s-z-r1"
  export BIND_ADDR="127.0.0.1:50054"
  export PREFIX_RANGE="s-z"
  export INCLUDE_NODE_ID_IN_REPLY="1"
  ./target/debug/ftrie-server
) &
PID_SZ_1=$!

(
  export NODE_ID="s-z-r2"
  export BIND_ADDR="127.0.0.1:50064"
  export PREFIX_RANGE="s-z"
  export INCLUDE_NODE_ID_IN_REPLY="1"
  ./target/debug/ftrie-server
) &
PID_SZ_2=$!

sleep 0.6

echo "Starting LB..."
(
  export BIND_ADDR="127.0.0.1:50052"
  export LB_POLICY="random"
  export LB_MAX_ATTEMPTS="2"
  export LB_HEALTH_INTERVAL_MS="500"
  export LB_FAIL_AFTER="2"
  export LB_RECOVER_AFTER="1"
  export PARTITION_MAP="a-i=http://127.0.0.1:50051|http://127.0.0.1:50061,j-r=http://127.0.0.1:50053|http://127.0.0.1:50063,s-z=http://127.0.0.1:50054|http://127.0.0.1:50064"
  ./target/debug/ftrie-lb
) &
PID_LB=$!

echo "Ready:"
echo "  LB: http://127.0.0.1:50052"
echo "  a-i replicas: 50051(pid=$PID_AI_1), 50061(pid=$PID_AI_2)"
echo "  j-r replicas: 50053(pid=$PID_JR_1), 50063(pid=$PID_JR_2)"
echo "  s-z replicas: 50054(pid=$PID_SZ_1), 50064(pid=$PID_SZ_2)"
echo ""
echo "Try:"
echo "  cargo run --bin ftrie-client -- apr power http://127.0.0.1:50052"
echo "  cargo run --bin ftrie-client -- ter power http://127.0.0.1:50052"
echo "  cargo run --bin ftrie-client -- zebra power http://127.0.0.1:50052"
echo ""
echo "Failover test (kill one replica and retry):"
echo "  kill $PID_JR_1   # kill j-r-r1"
echo "  cargo run --bin ftrie-client -- ter power http://127.0.0.1:50052"
echo "  kill $PID_JR_2   # kill j-r-r2 (now shard is down)"
echo "  cargo run --bin ftrie-client -- ter power http://127.0.0.1:50052"

wait
