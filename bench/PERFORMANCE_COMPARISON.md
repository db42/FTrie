# Performance Comparison How-To

This guide captures exactly how to generate and compare performance numbers between:

- Rust service (`ftrie-server`)
- Elasticsearch single node (`helloworld-es-bench` container)

## Files Used

- Workload file: [hot_len3_k10.jsonl](/Users/dushyant.bansal/work/ftrie/bench/workloads/hot_len3_k10.jsonl)
- Benchmark runner: [bench.py](/Users/dushyant.bansal/work/ftrie/bench/bench.py)
- Rust server launcher: [run_local_single_node_static.sh](/Users/dushyant.bansal/work/ftrie/scripts/run_local_single_node_static.sh)
- ES launcher/index prep: [run_local_es_single_node.sh](/Users/dushyant.bansal/work/ftrie/scripts/run_local_es_single_node.sh)

## Workload Details

### JSONL request shape

Each line in workload files is a JSON object with:

- `tenant` (string)
- `prefix` (string)
- `top_k` (int)

Example row:

```json
{"tenant":"power","prefix":"app","top_k":10}
```

### Workload families

Workloads are generated in [bench.py](/Users/dushyant.bansal/work/ftrie/bench/bench.py):

- `hot`: prefix sampled from top frequent prefixes (weighted by frequency)
- `uniform`: prefix sampled uniformly from valid prefixes
- `miss`: random prefix not present in dataset

Generation logic references:

- [bench.py](/Users/dushyant.bansal/work/ftrie/bench/bench.py:243)
- [bench.py](/Users/dushyant.bansal/work/ftrie/bench/bench.py:250)
- [bench.py](/Users/dushyant.bansal/work/ftrie/bench/bench.py:253)
- [bench.py](/Users/dushyant.bansal/work/ftrie/bench/bench.py:255)

### What this benchmark measures

- This comparison is **read-path performance only**.
- Each request is a prefix lookup with `top_k` (`GetPrefixMatch` on Rust, `_search` prefix query on ES).
- No write traffic is included during the timed run.
- Index build/ingest time is measured separately and is not part of these latency/QPS numbers.

### Default parameter grid

Defaults from generator command:

- prefix lengths: `1,3,5`
- top_k values: `10,50`
- counts: `hot=20000`, `uniform=20000`, `miss=5000`

References:

- [bench.py](/Users/dushyant.bansal/work/ftrie/bench/bench.py:771)
- [bench.py](/Users/dushyant.bansal/work/ftrie/bench/bench.py:772)
- [bench.py](/Users/dushyant.bansal/work/ftrie/bench/bench.py:1238)

For this comparison, `hot_len3_k10.jsonl` is used.

### Scaling to 10x words (~3.7M)

Generate a 10x unique `power` wordlist from `words_alpha.txt`:

```bash
cd /Users/dushyant.bansal/work/ftrie
./scripts/generate_scaled_wordlist.py \
  --input ./words_alpha.txt \
  --output ./bench/data/words_alpha_x10.txt \
  --factor 10
```

Generate workload JSONL from the 10x list:

```bash
python3 bench/bench.py generate \
  --wordlist ./bench/data/words_alpha_x10.txt \
  --out-dir ./bench/workloads_x10
```

Run Rust with the 10x power list:

```bash
WORDS_POWER_FILE=./bench/data/words_alpha_x10.txt \
PORT=50051 INCLUDE_MESSAGE=0 INDEX_BACKEND=fst \
./scripts/run_local_single_node_static.sh
```

Run ES ingest with the 10x power list:

```bash
WORDLIST=./bench/data/words_alpha_x10.txt \
./scripts/run_local_es_single_node.sh
```

## 1) Run Rust Benchmark

Start Rust single-node server:

```bash
cd /Users/dushyant.bansal/work/ftrie
PORT=50051 INCLUDE_MESSAGE=0 INDEX_BACKEND=fst ./scripts/run_local_single_node_static.sh
```

In another terminal, run benchmark:

```bash
cd /Users/dushyant.bansal/work/ftrie
python3 bench/bench.py run \
  --endpoint 127.0.0.1:50051 \
  --concurrency 32 \
  --warmup-s 10 \
  --duration-s 30 \
  --deployment single_node_raft \
  --nodes 1 \
  --deployment-notes "INDEX_BACKEND=fst; prefix_len=3 top_k=10" \
  bench/workloads/hot_len3_k10.jsonl
```

Example Rust artifact:

- [20260225_142047_e3c7a9c.json](/Users/dushyant.bansal/work/ftrie/bench/results/20260225_142047_e3c7a9c.json)

## 2) Run Elasticsearch Benchmark

Start ES and ingest words:

```bash
cd /Users/dushyant.bansal/work/ftrie
./scripts/run_local_es_single_node.sh
```

Run ES benchmark:

```bash
cd /Users/dushyant.bansal/work/ftrie
python3 bench/bench.py run-es \
  --endpoint http://127.0.0.1:9200 \
  --index prefix_words \
  --concurrency 32 \
  --warmup-s 10 \
  --duration-s 30 \
  --deployment single_node_es \
  --nodes 1 \
  --deployment-notes "docker; es8.15.5; heap=4g; shards=1; replicas=0; query=prefix_keyword" \
  bench/workloads/hot_len3_k10.jsonl
```

Example ES artifact:

- [20260227_143315_2f1b8b9.json](/Users/dushyant.bansal/work/ftrie/bench/results/20260227_143315_2f1b8b9.json)

## 3) Compare Two Runs

```bash
cd /Users/dushyant.bansal/work/ftrie
python3 bench/bench.py compare \
  bench/results/20260225_142047_e3c7a9c.json \
  bench/results/20260227_143315_2f1b8b9.json
```

This compares:

- `QPS` (`qps_ok` when present)
- `latency_avg_ms`
- `latency_p95_ms`
- `latency_p99_ms`

## 4) Optional: Concurrency Matrix

Rust matrix:

```bash
python3 bench/bench.py matrix \
  --endpoint 127.0.0.1:50051 \
  --concurrency 1,8,32,128 \
  --warmup-s 10 \
  --duration-s 30 \
  bench/workloads/hot_len3_k10.jsonl
```

ES matrix:

```bash
python3 bench/bench.py matrix-es \
  --endpoint http://127.0.0.1:9200 \
  --index prefix_words \
  --concurrency 1,8,32,128 \
  --warmup-s 10 \
  --duration-s 30 \
  bench/workloads/hot_len3_k10.jsonl
```

## Reproducibility Notes

- Keep workload file identical for both engines.
- Keep `concurrency`, `warmup`, and `duration` identical.
- Avoid running heavy background tasks.
- Record deployment notes in each artifact (`deployment-notes` flag).
