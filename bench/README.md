# Prefix Bench (Python)

This directory contains a small, reproducible end-to-end benchmark harness for the gRPC prefix
service using fixed `top_k` and prefix-length workloads.

## Setup

Install Python deps:

```bash
python3 -m pip install -r bench/requirements.txt
```

Start the service (single node or LB + shards) separately, then run the benchmark against the
listening endpoint.

## Generate Workloads

Generates JSONL files under `bench/workloads/` (prefix lengths 1/3/5, `top_k`
10/50, hot/uniform/miss):

```bash
python3 bench/bench.py generate
```

## Run

Example against the load balancer (default `127.0.0.1:50052`):

```bash
python3 bench/bench.py run \
  --endpoint 127.0.0.1:50052 \
  --concurrency 32 \
  --warmup-s 20 \
  --duration-s 60 \
  bench/workloads/hot_len1_k10.jsonl \
  bench/workloads/hot_len3_k10.jsonl \
  bench/workloads/hot_len5_k10.jsonl
```

Results are written to `bench/results/` as JSON.

## Matrix

Runs the same workload set across multiple concurrencies against an already-running endpoint:

```bash
python3 bench/bench.py matrix \
  --endpoint 127.0.0.1:50051 \
  --concurrency 1,8,32,128 \
  --warmup-s 20 \
  --duration-s 60 \
  bench/workloads/hot_len1_k10.jsonl
```

## Suite (Recommended)

Runs a local `ftrie-server` and then executes a workload matrix (all `*.jsonl` under
`bench/workloads/` by default) across a set of concurrencies, producing one JSON artifact.

```bash
python3 bench/bench.py suite \
  --concurrency 1,8,32,128 \
  --warmup-s 20 \
  --duration-s 60
```

Tips:
- Use `--disable-static-index` only if you plan to pre-seed data (e.g. via `PutWord`) in another step; otherwise reads will be mostly empty.
- Use `--skip-build` once `target/release/ftrie-server` is built.
- Use `--server-bin /path/to/ftrie-server` to benchmark a prebuilt binary without building the current working tree.

## Compare

```bash
python3 bench/bench.py compare \
  bench/results/runA.json \
  bench/results/runB.json
```

## Elasticsearch Setup + Run

Start local single-node Elasticsearch and ingest `words_alpha.txt`:

```bash
./scripts/run_local_es_single_node.sh
```

Or prepare an already-running ES endpoint manually:

```bash
python3 bench/bench.py es-prepare \
  --endpoint http://127.0.0.1:9200 \
  --index prefix_words \
  --recreate
```

Run workload(s) against Elasticsearch:

```bash
python3 bench/bench.py run-es \
  --endpoint http://127.0.0.1:9200 \
  --index prefix_words \
  --concurrency 32 \
  --warmup-s 10 \
  --duration-s 30 \
  bench/workloads/hot_len3_k10.jsonl
```

Run concurrency matrix against Elasticsearch:

```bash
python3 bench/bench.py matrix-es \
  --endpoint http://127.0.0.1:9200 \
  --index prefix_words \
  --concurrency 1,8,32,128 \
  --warmup-s 10 \
  --duration-s 30 \
  bench/workloads/hot_len3_k10.jsonl
```
