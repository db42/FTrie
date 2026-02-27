#!/usr/bin/env bash
set -euo pipefail

# Starts a local single-node Elasticsearch for benchmarking and prepares the index.
#
# Env knobs:
# - ES_CONTAINER_NAME (default ftrie-es-bench)
# - ES_IMAGE (default docker.elastic.co/elasticsearch/elasticsearch:8.15.5)
# - ES_PORT (default 9200)
# - ES_HEAP (default 4g)
# - ES_INDEX (default prefix_words)
# - WORDLIST (default ./words_alpha.txt)
# - TENANT (default power)
# - BATCH_SIZE (default 5000)
# - RECREATE (default 1; delete/recreate index before ingest)
# - FORCE_MERGE (default 0; set 1 for max_num_segments=1)
# - STOP_EXISTING (default 1; remove existing same-name container)

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ES_CONTAINER_NAME="${ES_CONTAINER_NAME:-ftrie-es-bench}"
ES_IMAGE="${ES_IMAGE:-docker.elastic.co/elasticsearch/elasticsearch:8.15.5}"
ES_PORT="${ES_PORT:-9200}"
ES_HEAP="${ES_HEAP:-4g}"
ES_INDEX="${ES_INDEX:-prefix_words}"
WORDLIST="${WORDLIST:-./words_alpha.txt}"
TENANT="${TENANT:-power}"
BATCH_SIZE="${BATCH_SIZE:-5000}"
RECREATE="${RECREATE:-1}"
FORCE_MERGE="${FORCE_MERGE:-0}"
STOP_EXISTING="${STOP_EXISTING:-1}"
ES_ENDPOINT="http://127.0.0.1:${ES_PORT}"

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker not found in PATH"
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "ERROR: Docker daemon is not running. Start Docker Desktop first."
  exit 1
fi

if docker ps -a --format '{{.Names}}' | grep -Fxq "${ES_CONTAINER_NAME}"; then
  if [[ "${STOP_EXISTING}" == "1" ]]; then
    echo "Removing existing container: ${ES_CONTAINER_NAME}"
    docker rm -f "${ES_CONTAINER_NAME}" >/dev/null
  else
    echo "ERROR: container ${ES_CONTAINER_NAME} already exists (set STOP_EXISTING=1 to replace)"
    exit 1
  fi
fi

echo "Starting Elasticsearch container ${ES_CONTAINER_NAME} on :${ES_PORT}"
docker run -d \
  --name "${ES_CONTAINER_NAME}" \
  -p "${ES_PORT}:9200" \
  -e "discovery.type=single-node" \
  -e "xpack.security.enabled=false" \
  -e "xpack.security.enrollment.enabled=false" \
  -e "ES_JAVA_OPTS=-Xms${ES_HEAP} -Xmx${ES_HEAP}" \
  --ulimit nofile=65535:65535 \
  "${ES_IMAGE}" >/dev/null

echo "Waiting for Elasticsearch readiness at ${ES_ENDPOINT} ..."
for _ in $(seq 1 120); do
  if curl -fsS "${ES_ENDPOINT}" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

if ! curl -fsS "${ES_ENDPOINT}" >/dev/null 2>&1; then
  echo "ERROR: Elasticsearch did not become ready"
  echo "Container logs:"
  docker logs "${ES_CONTAINER_NAME}" | tail -n 80
  exit 1
fi

PREP_ARGS=(
  es-prepare
  --endpoint "${ES_ENDPOINT}"
  --index "${ES_INDEX}"
  --wordlist "${WORDLIST}"
  --tenant "${TENANT}"
  --batch-size "${BATCH_SIZE}"
)
if [[ "${RECREATE}" == "1" ]]; then
  PREP_ARGS+=(--recreate)
fi
if [[ "${FORCE_MERGE}" == "1" ]]; then
  PREP_ARGS+=(--force-merge)
fi

python3 bench/bench.py "${PREP_ARGS[@]}"

echo ""
echo "Ready for ES benchmark runs."
echo "Example:"
echo "  python3 bench/bench.py run-es --endpoint ${ES_ENDPOINT} --index ${ES_INDEX} \\"
echo "    --concurrency 32 --warmup-s 10 --duration-s 30 \\"
echo "    bench/workloads/hot_len3_k10.jsonl"
echo ""
echo "Stop:"
echo "  docker rm -f ${ES_CONTAINER_NAME}"
