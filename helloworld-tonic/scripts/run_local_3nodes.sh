#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

trap 'jobs -p | xargs -r kill; wait || true' EXIT

echo "Building..."
cargo build --bins >/dev/null

echo "Starting 3 shard servers..."
(
  export BIND_ADDR="127.0.0.1:50051"
  export PREFIX_RANGE="a-i"
  ./target/debug/helloworld-server
) &

(
  export BIND_ADDR="127.0.0.1:50053"
  export PREFIX_RANGE="j-r"
  ./target/debug/helloworld-server
) &

(
  export BIND_ADDR="127.0.0.1:50054"
  export PREFIX_RANGE="s-z"
  ./target/debug/helloworld-server
) &

sleep 0.5

echo "Starting LB..."
(
  export BIND_ADDR="127.0.0.1:50052"
  export PARTITION_MAP="a-i=http://127.0.0.1:50051,j-r=http://127.0.0.1:50053,s-z=http://127.0.0.1:50054"
  ./target/debug/helloworld-lb
) &

echo "Ready:"
echo "  LB:       http://127.0.0.1:50052"
echo "  Node a-i: http://127.0.0.1:50051"
echo "  Node j-r: http://127.0.0.1:50053"
echo "  Node s-z: http://127.0.0.1:50054"
echo ""
echo "Try:"
echo "  cargo run --bin helloworld-client -- apr power http://127.0.0.1:50052"
echo "  cargo run --bin helloworld-client -- mango power http://127.0.0.1:50052"
echo "  cargo run --bin helloworld-client -- zebra power http://127.0.0.1:50052"

wait
