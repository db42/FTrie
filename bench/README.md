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

## Larger Dataset (10x)

Create a 10x unique power wordlist (~3.7M words):

```bash
./scripts/generate_scaled_wordlist.py \
  --input ./words_alpha.txt \
  --output ./bench/data/words_alpha_x10.txt \
  --factor 10
```

Generate workloads from that list:

```bash
python3 bench/bench.py generate \
  --wordlist ./bench/data/words_alpha_x10.txt \
  --out-dir ./bench/workloads_x10
```

Use it with Rust static index:

```bash
WORDS_POWER_FILE=./bench/data/words_alpha_x10.txt ./scripts/run_local_single_node_static.sh
```

Use it with Elasticsearch ingest:

```bash
WORDLIST=./bench/data/words_alpha_x10.txt ./scripts/run_local_es_single_node.sh
```

## Scale Sweep Scripts

Run Rust(FST) vs ES performance sweep across 25x/50x/100x:

```bash
./scripts/bench_scale_perf.sh
```

Run Rust trie only (no ES):

```bash
RUST_BACKEND=trie RUN_ES=0 ./scripts/bench_scale_perf.sh
```

Generate only the missing 10x Rust trie perf artifact:

```bash
./scripts/bench_generate_trie_x10_perf.sh
```

Run Rust(FST) vs ES memory sweep across 25x/50x/100x:

```bash
./scripts/bench_scale_memory.sh
```

Run Rust trie memory only (no ES):

```bash
RUST_BACKEND=trie RUN_ES=0 ./scripts/bench_scale_memory.sh
```

Three-way summary reference:

- `bench/THREE_WAY_COMPARISON.md`

Useful env overrides:

- `FACTORS="25 50 100"`
- `CONCURRENCY=32 WARMUP_S=10 DURATION_S=30` (perf script)
- `RUST_BACKEND=fst|trie`
- `RUN_RUST=1 RUN_ES=0|1`
- `ES_HEAP=4g`
- `OUT=bench/results/memory_scale_custom.tsv` (memory script)
