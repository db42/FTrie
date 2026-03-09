# FTrie

The fastest prefix-matching service on a single machine.

FTrie is a high-throughput prefix search service for autocomplete-style workloads.
It is optimized for low-latency reads, bounded `top_k` responses, and optional
Raft replication for high availability.

## Goals

* **Maximum single-node throughput**: keep prefix lookup and writes fast with in-memory indexing.
* **Practical distributed mode**: run leader-replica replication with Raft when availability matters.
* **Low-ops defaults**: start quickly with local scripts and minimal configuration.
* **Predictable responses**: always support bounded result sets with `top_k`.

## Why FTrie?

Many autocomplete use cases do not need a full-text search cluster. FTrie focuses
on one workload, prefix lookup, and keeps the stack small: trie/FST indexing, gRPC,
partition-aware routing, and optional Raft.

## Features

- **gRPC API**: `GetPrefixMatch` for prefix lookup and `PutWord` for writes.
- **Index backend (`trie`)**: mutable in-memory writes.
- **Index backend (`fst`)**: compact, read-heavy lookups.
- **Partition-aware load balancer**: routes by prefix range and supports read quorum `R`.
- **Raft replication**: leader-based writes with follower catch-up.
- **Health-aware routing**: backend probing and failure tracking in the LB.
- **Black-box integration tests**: routing, `top_k`, replication, and persistence scenarios.

## Quick Start

### Build

```bash
cargo build --bins
```

Deploy via Docker (single-node): `docker build -t ftrie-server:dev . && docker run --rm -p 50051:50051 ftrie-server:dev`.

### Single Node

```bash
./scripts/run_local_single_node_static.sh
```

Use a larger custom power wordlist (example 10x):

```bash
WORDS_POWER_FILE=./bench/data/words_alpha_x10.txt ./scripts/run_local_single_node_static.sh
```

Example query:

```bash
grpcurl -plaintext -import-path ./proto -proto ftrie.proto \
  -d '{"name":"app","tenant":"power"}' \
  127.0.0.1:50051 ftrie.PrefixMatcher/GetPrefixMatch
```

### 3-Node Raft Cluster

```bash
CLEAN=1 ./scripts/run_local_phase4_raft.sh
```

Example write through LB:

```bash
grpcurl -plaintext -import-path ./proto -proto ftrie.proto \
  -d '{"word":"jokerraft","tenant":"power"}' \
  127.0.0.1:50052 ftrie.PrefixMatcher/PutWord
```

## Performance Snapshot

QPS is 2.5x better compared to ES and p95 latency is 3.5x better.

Read workload: `hot_len3_k10` (`concurrency=32`, `warmup=10s`, `duration=30s`)

| Scale (Words) | Rust FST QPS | Rust Trie QPS | ES QPS | FST p95 (ms) | Trie p95 (ms) | ES p95 (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 10x (~3.70M) | 11006 | 10720 | 6142 | 3.8 | 4.1 | 6.8 |
| 25x (~9.25M) | 10922 | 11175 | 4614 | 3.9 | 3.7 | 12.7 |
| 50x (~18.51M) | 10780 | 11049 | 4251 | 4.0 | 3.8 | 13.3 |


Detailed results including memory:
- `bench/THREE_WAY_COMPARISON.md`

## Architecture

```text
.
├── proto/           # gRPC service definitions (query + raft RPC)
├── scripts/         # Local run scripts (single node, replicated, raft phase)
├── src/
│   ├── server.rs    # Main shard server (gRPC + Raft write path)
│   ├── lb.rs        # Partition-aware load balancer with health/read quorum
│   ├── store.rs     # Backend-agnostic shard store
│   ├── trie.rs      # Trie index implementation
│   ├── fst_index.rs # FST index implementation
│   ├── raft_*.rs    # Raft network, storage, and state machine
│   └── partition.rs # Prefix-range routing primitives
└── tests/           # Integration tests for routing/top_k/raft
```

## Test Suite

```bash
cargo test
```

Notable integration tests:
- `tests/it_routing.rs`
- `tests/it_top_k.rs`
- `tests/it_raft_basic.rs`
- `tests/it_raft_persistence.rs`

## Known Limitations

- Input validation is currently ASCII `a-z` focused in the live request path.
- `INDEX_BACKEND=fst` is currently read-only for writes (`PutWord` requires trie backend).

## Dependencies

Key dependencies:
- `tonic` for gRPC server/client
- `openraft` for consensus replication
- `fst` for compact finite-state-transducer indexing

## Naming Note

This README uses **FTrie** as the project name. Binary names are
`ftrie-server`, `ftrie-lb`, and `ftrie-client`.
