# Three-Way Comparison (FST vs Trie vs Elasticsearch)

Date: 2026-03-08

Workload:

- `bench/workloads/hot_len3_k10.jsonl`
- Read-path only (`GetPrefixMatch` / ES `_search` prefix query)
- `concurrency=32`, `warmup=10s`, `duration=30s`

## Commands

Rust trie perf (no ES):

```bash
./scripts/bench_generate_trie_x10_perf.sh
RUST_BACKEND=trie RUN_ES=0 FACTORS='25 50' ./scripts/bench_scale_perf.sh
```

Rust trie memory (no ES):

```bash
RUST_BACKEND=trie RUN_ES=0 FACTORS='10 25 50' ./scripts/bench_scale_memory.sh
```

## Performance (QPS / p95 ms)

Scale legend (base list has ~370,104 words):

- `10x` -> ~3.70M words
- `25x` -> ~9.25M words
- `50x` -> ~18.51M words

| Scale (Words) | Rust FST QPS | Rust Trie QPS | ES QPS | FST p95 | Trie p95 | ES p95 |
| ----- | ------------ | ------------- | ------ | ------- | -------- | ------ |
| 10x (~3.70M)  | 11006.2      | 10720.0       | 6142.1 | 3.767   | 4.055    | 6.775  |
| 25x (~9.25M)  | 10921.6      | 11175.0       | 4614.2 | 3.871   | 3.703    | 12.703 |
| 50x (~18.51M) | 10779.6      | 11049.2       | 4250.9 | 3.969   | 3.771    | 13.279 |

## Memory (RSS GB)

| Scale | Rust FST RSS | Rust Trie RSS | ES JVM RSS |
| ----- | ------------ | ------------- | ---------- |
| 10x   | 0.17         | 2.39          | 4.74       |
| 25x   | 0.40         | 9.77          | 4.64       |
| 50x   | 0.80         | 15.80         | 4.62       |

Note:

- 100x rows are intentionally excluded in this summary.
- ES heap was configured as `-Xms4g -Xmx4g`; observed ES JVM RSS stays around `~4.6-4.7 GB` in these runs.
- This is expected: process RSS is usually near/above heap due non-heap JVM memory and off-heap/native usage.

## Artifacts

Performance:

- `bench/results/scale_x10_rust.json`
- `bench/results/scale_x10_rust_trie.json`
- `bench/results/scale_x10_es.json`
- `bench/results/scale_x25_rust.json`
- `bench/results/scale_x50_rust.json`
- `bench/results/scale_x25_rust_trie.json`
- `bench/results/scale_x50_rust_trie.json`
- `bench/results/scale_x25_es.json`
- `bench/results/scale_x50_es.json`

Memory:

- `bench/results/memory_scale_x10.json`
- `bench/results/memory_scale_fst_es_x25_x50_x100.tsv`
- `bench/results/memory_scale_trie_x25_x50_x100_verified.tsv`
