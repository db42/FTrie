# Performance Plan

Goal: Make single-node prefix match extremely fast (competitive with dedicated autocomplete/search systems) under the end-to-end benchmark harness in `bench/`.

## Current Benchmark Baseline

Track results via `helloworld-tonic/bench/results/*.json` and compare runs with:

```bash
python3 helloworld-tonic/bench/bench.py compare runA.json runB.json
```

## Tier 0: Benchmark Hygiene (Do First)

These changes reduce benchmark noise and remove work that is not part of the core prefix algorithm.

- Disable hot-path logging: remove/gate `println!`/`dbg!` in request path and trie traversal.
- Make reply formatting optional: avoid building large `message` strings for benchmarks.
- Keep response bounded: always use `top_k` (and ensure the implementation stops early).
- Make deployment metadata explicit in artifacts: `deployment.type`, `deployment.nodes`, `deployment.notes`.

## Tier 1: Hot Path Algorithm Fixes (Same Data Structure)

- Avoid per-step allocations in DFS:
  - Replace `format!("{}{}", pre, ch)` with a push/pop buffer.
  - Only allocate when emitting a completed word.
- Prefer ASCII byte traversal to `chars()` for `a-z` words/prefixes.
- Avoid recursion overhead in traversal (iterative stack).
- Avoid extra copies in results; consider returning IDs/offsets internally.

## Tier 2: Data Layout (Cache-Friendly)

- Replace per-node `Vec<Option<Node>>` with a flat arena:
  - `Vec<Node>` and children as indices (`u32`) instead of nested heap objects.
  - Start with `[u32; 26]` children; then consider bitmap + packed child list to reduce memory.
- Consider path compression (radix-like) to reduce depth.

## Tier 3: Concurrency & Runtime Overheads

- Reduce lock overhead in read path (`RwLock`):
  - Consider `Arc<Indexer>` snapshots swapped on writes (or `RwLock<Arc<_>>` and clone `Arc` on read).
- Keep CPU-only operations synchronous where possible to reduce async scheduling overhead.

## Tier 4: Static Index “Max Speed” Option

If the dictionary is mostly static (batch rebuilds are acceptable):

- Use a minimal automaton / FST (DAWG-like) built offline.
- `mmap` the built index for very fast startup and cache-friendly reads.

