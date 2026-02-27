# Elasticsearch Benchmark Plan (Single Node)

Goal: produce a reproducible, performance-focused comparison between this Rust prefix service and Elasticsearch on the same machine, same dataset, and same workload artifacts.

## Current Rust Single-Node Baseline

From existing benchmark artifacts (`bench/results/`), workload `hot_len3_k10`, `concurrency=32`:

| Run | Duration | QPS (`qps_ok`) | P99 (ms) | Errors |
|---|---:|---:|---:|---:|
| `20260225_121759_7214fa3.json` | 10s | 11073.8 | 4.503 | 0 |
| `20260225_142047_e3c7a9c.json` | 30s | 10731.47 | 4.971 | 0 |
| `20260225_133536_625040f.json` | 30s | 10212.9 | 5.267 | 0 |

Working baseline range for positioning:
- QPS: ~10.2k to ~11.1k
- P99: ~4.5 to ~5.3 ms

## Fairness Rules

- Same hardware and power mode for both systems.
- Same dataset source: `words_alpha.txt`.
- Same workload files from `bench/workloads/*.jsonl`.
- Same benchmark schedule: warmup, duration, concurrency.
- Single-node target for both systems (no replicas/cluster fanout).
- Report both throughput and tail latency (`p95`/`p99`), not just best-case QPS.

## Elasticsearch Profile (Phase 1)

Purpose: strict apples-to-apples prefix lookup.

- Deployment: Elasticsearch single node.
- Index settings:
  - `number_of_shards=1`
  - `number_of_replicas=0`
  - `refresh_interval=-1` during ingest, then set to `1s`.
- Mapping:
  - `tenant` as `keyword`
  - `word` as `keyword`
- Query mode:
  - bool filter with `term(tenant)` + `prefix(word, value=<prefix>)`
  - `size = top_k`
  - `sort` by `word:asc` for deterministic ordering.

## Implementation Added

- `bench/bench.py es-prepare`: create/recreate ES index and bulk ingest wordlist.
- `bench/bench.py run-es`: run workload(s) against Elasticsearch and write JSON result in current schema.
- `bench/bench.py matrix-es`: run concurrency matrix against Elasticsearch.
- `scripts/run_local_es_single_node.sh`: start Docker ES single node and prepare the benchmark index.

## Runbook

1. Start/prepare Elasticsearch locally:
```bash
./scripts/run_local_es_single_node.sh
```

2. Run ES benchmark (single workload):
```bash
python3 bench/bench.py run-es \
  --endpoint http://127.0.0.1:9200 \
  --index prefix_words \
  --concurrency 32 \
  --warmup-s 10 \
  --duration-s 30 \
  bench/workloads/hot_len3_k10.jsonl
```

3. Compare with Rust run:
```bash
python3 bench/bench.py compare \
  bench/results/<rust_run>.json \
  bench/results/<es_run>.json
```

4. Run matrix for stronger evidence:
```bash
python3 bench/bench.py matrix-es \
  --endpoint http://127.0.0.1:9200 \
  --index prefix_words \
  --concurrency 1,8,32,128 \
  --warmup-s 10 \
  --duration-s 30 \
  bench/workloads/hot_len3_k10.jsonl
```

## Positioning Guidance

Preferred claim style:
- "In reproducible single-node prefix benchmarks on identical workloads, this Rust engine outperformed Elasticsearch prefix lookup configuration on QPS and/or tail latency."

Avoid:
- Absolute "fastest prefix match" claims without broader public benchmark coverage across multiple engines and hardware setups.
