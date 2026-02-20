#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

trap 'jobs -p | xargs -r kill; wait || true' EXIT

echo "Building..."
cargo build --bins >/dev/null

R="${R:-1}"
W="${W:-1}"

echo "Starting shard j-r (RF=2) with WAL..."
rm -rf ./data-phase3 >/dev/null 2>&1 || true
mkdir -p ./data-phase3/jr1 ./data-phase3/jr2

(
  export NODE_ID="jr1"
  export BIND_ADDR="127.0.0.1:50053"
  export PREFIX_RANGE="j-r"
  export DATA_DIR="./data-phase3/jr1"
  export FSYNC="0"
  export INCLUDE_NODE_ID_IN_REPLY="1"
  ./target/debug/helloworld-server
) &
PID_JR_1=$!

(
  export NODE_ID="jr2"
  export BIND_ADDR="127.0.0.1:50063"
  export PREFIX_RANGE="j-r"
  export DATA_DIR="./data-phase3/jr2"
  export FSYNC="0"
  export INCLUDE_NODE_ID_IN_REPLY="1"
  ./target/debug/helloworld-server
) &
PID_JR_2=$!

sleep 0.6

echo "Starting LB (R=$R W=$W)..."
(
  export BIND_ADDR="127.0.0.1:50052"
  export LB_POLICY="random"
  export LB_MAX_ATTEMPTS="3"
  export LB_HEALTH_INTERVAL_MS="500"
  export LB_FAIL_AFTER="1"
  export LB_RECOVER_AFTER="1"
  export READ_TIMEOUT_MS="300"
  export WRITE_TIMEOUT_MS="800"
  export R="$R"
  export W="$W"
  export PARTITION_MAP="j-r=http://127.0.0.1:50053|http://127.0.0.1:50063"
  ./target/debug/helloworld-lb
) &
PID_LB=$!

echo "Ready:"
echo "  LB: http://127.0.0.1:50052"
echo "  j-r replicas: 50053(pid=$PID_JR_1), 50063(pid=$PID_JR_2)"
echo ""
echo "Write (PutWord) then read (SayHello):"
echo "  grpcurl -plaintext -import-path ./proto -proto helloworld.proto \\"
echo "    -d '{\"word\":\"jokerz\",\"tenant\":\"power\"}' \\"
echo "    127.0.0.1:50052 helloworld.Greeter/PutWord"
echo ""
echo "  grpcurl -plaintext -import-path ./proto -proto helloworld.proto \\"
echo "    -d '{\"name\":\"joker\",\"tenant\":\"power\"}' \\"
echo "    127.0.0.1:50052 helloworld.Greeter/SayHello"
echo ""
echo "Failover:"
echo "  kill $PID_JR_1"
echo "  (writes still succeed if W=1)"

wait

