# Distributed Trie at Scale - Implementation Plan

Transform a single-node prefix search service into a horizontally-scalable, fault-tolerant distributed system suitable for production workloads at deep tech companies.

---

## Strategic Goal: DoE Brand Building

**Target**: Director of Engineering roles at deep tech infrastructure companies (GCP, AWS, Snowflake, Databricks, etc.)

### Why This Project Matters

| Signal | What It Demonstrates | Relevance to Target Roles |
|--------|---------------------|---------------------------|
| **Rust implementation** | Low-level systems expertise | Core competency for infrastructure teams |
| **Distributed consensus (Raft)** | Understanding of coordination protocols | Table stakes for distributed databases |
| **Horizontal sharding** | Scalability thinking | Central to Snowflake/Databricks architecture |
| **Prefix-aware partitioning** | Domain-specific optimization vs generic solutions | Shows depth, not just breadth |
| **Kubernetes deployment** | Cloud-native operational maturity | Expected for modern infrastructure leadership |
| **Technical blog series** | Ability to communicate complex systems | Critical for DoE-level influence |

### Differentiators vs. Typical GitHub Projects

1. **Not a tutorial clone** — Custom architecture decisions with documented trade-offs
2. **Production-oriented** — Observability, failure handling, and operational concerns baked in
3. **Quantified outcomes** — Benchmarks comparing to Elasticsearch, not just "it works"
4. **End-to-end ownership** — From algorithm design to Kubernetes deployment

### Deliverables for Brand Building

- [ ] Working distributed system deployed on EKS/GKE
- [ ] 5-part technical blog series (see outline below)
- [ ] Conference talk proposal: "Building Distributed Systems in Rust: Lessons from a Side Project"
- [ ] Benchmark report comparing latency/throughput with Elasticsearch prefix queries

## Architecture Decisions

### Open Questions (To Be Decided)
1. **Consensus Protocol**: Using embedded Raft (via `openraft` crate) vs external coordination (etcd/ZooKeeper)
2. **Sharding Strategy**: Prefix-based consistent hashing vs hash-based (trade-offs discussed below)
3. **Caching Layer**: Redis vs in-memory LRU cache
4. **Deployment Target**: EKS (AWS) vs GKE (GCP) vs local K8s (kind/minikube for development)

---

## System Architecture

```
                           ┌─────────────────────────────────────────────────────────────┐
                           │                     Client Layer                            │
                           │              gRPC Client    gRPC-Web Client                 │
                           └─────────────────────────┬───────────────────────────────────┘
                                                     │
                                                     ▼
                           ┌─────────────────────────────────────────────────────────────┐
                           │                   Gateway Layer                             │
                           │         Smart Load Balancer (Prefix-Aware Routing)          │
                           └─────────────────────────┬───────────────────────────────────┘
                                                     │
                      ┌──────────────────────────────┼──────────────────────────────┐
                      │                              │                              │
                      ▼                              ▼                              ▼
        ┌─────────────────────────┐   ┌─────────────────────────┐   ┌─────────────────────────┐
        │     Shard 1: a-i        │   │     Shard 2: j-r        │   │     Shard 3: s-z        │
        │  ┌───────┐ ┌─────────┐  │   │  ┌───────┐ ┌─────────┐  │   │  ┌───────┐ ┌─────────┐  │
        │  │Leader │ │Follower │  │   │  │Leader │ │Follower │  │   │  │Leader │ │Follower │  │
        │  └───────┘ └─────────┘  │   │  └───────┘ └─────────┘  │   │  └───────┘ └─────────┘  │
        │           ┌─────────┐   │   │           ┌─────────┐   │   │           ┌─────────┐   │
        │           │Follower │   │   │           │Follower │   │   │           │Follower │   │
        │           └─────────┘   │   │           └─────────┘   │   │           └─────────┘   │
        └────────────┬────────────┘   └────────────┬────────────┘   └────────────┬────────────┘
                     │                             │                             │
                     └─────────────────────────────┼─────────────────────────────┘
                                                   │
                                                   ▼
                           ┌─────────────────────────────────────────────────────────────┐
                           │                    Cache Layer                              │
                           │              Redis Cluster (Hot Path Cache)                 │
                           └─────────────────────────────────────────────────────────────┘


        Coordination Layer (Raft Consensus + Cluster Metadata) runs within each shard
```

---

## Key Design Decisions

### 1. Prefix-Based Consistent Hashing

Unlike traditional consistent hashing (hash the key), we use **prefix-aware partitioning**:

| Approach | Pros | Cons |
|----------|------|------|
| **Hash-based** | Even distribution | Breaks prefix locality; queries spanning prefixes hit all nodes |
| **Prefix-based** | Prefix queries hit single shard | Potential hot spots (e.g., words starting with 's') |

**Recommendation**: Prefix-based with dynamic split/merge to handle hot spots.

```
Partition Map Example:
┌──────────────┬─────────────────────────┐
│ Prefix Range │ Shard Assignment        │
├──────────────┼─────────────────────────┤
│ a - f        │ shard-1 (node-1, node-2)│
│ g - m        │ shard-2 (node-3, node-4)│
│ n - s        │ shard-3 (node-5, node-6)│
│ t - z        │ shard-4 (node-7, node-8)│
└──────────────┴─────────────────────────┘
```

### 2. Replication Strategy

Each shard has 3 replicas (1 leader + 2 followers):
- **Writes**: Go to leader, replicated via Raft log
- **Reads**: Can be served from any replica (eventual consistency) or leader-only (strong consistency)

### 3. Failure Handling

```
Leader Failure Sequence:
┌────────────────┐     ┌────────────────┐     ┌────────────────┐
│  Shard Leader  │     │   Follower 1   │     │   Follower 2   │
│    (fails)     │     │                │     │                │
└───────┬────────┘     └───────┬────────┘     └───────┬────────┘
        │                      │                      │
        X (leader fails)       │                      │
                               │                      │
                    election timeout                  │
                               │                      │
                    RequestVote ──────────────────────►
                               │                      │
                    ◄─────────── VoteGranted          │
                               │                      │
                    Elected as new leader             │
                               │                      │
        Queries now route to ──►                      │
```

---

## Proposed Changes

### Core Infrastructure

#### [NEW] proto/cluster.proto
New protobuf definitions for cluster operations:
- Node registration/deregistration
- Health checks
- Partition map updates
- Inter-node replication

#### [NEW] src/partition.rs
Consistent hashing ring with prefix-aware routing:
- `PartitionMap` structure holding prefix ranges → node assignments
- `get_shard_for_prefix(prefix: &str) -> ShardId` 
- `rebalance_partitions()` for dynamic scaling

#### [NEW] src/consensus.rs
Raft consensus implementation using `openraft` crate:
- Leader election within each shard
- Log replication for write operations
- Cluster membership changes

#### [NEW] src/node.rs
Individual node implementation:
- Joins cluster via seed nodes
- Reports health to coordinator
- Handles shard assignment

---

### Modified Components

#### [MODIFY] src/lb.rs
Transform from simple proxy to smart router:
- Query partition map to route by prefix
- Handle shard leader changes
- Implement retry logic with circuit breaker
- Fan-out for cross-shard queries

#### [MODIFY] src/server.rs
Evolve to be shard-aware:
- Only load words for assigned prefix ranges
- Participate in Raft consensus
- Report metrics to Prometheus

#### [MODIFY] src/indexer.rs
Add shard-aware indexing:
- Filter words by prefix range during indexing
- Support incremental indexing
- Add index statistics (word count, memory usage)

#### [MODIFY] Cargo.toml
Add new dependencies:
```toml
[dependencies]
openraft = "0.9"           # Raft consensus
redis = "0.24"             # Caching layer
prometheus = "0.13"        # Metrics
tracing = "0.1"            # Structured logging
tracing-subscriber = "0.3"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
```

---

### Caching Layer

#### [NEW] src/cache.rs
Redis integration for hot-path caching:
- Cache prefix → results mapping
- TTL-based expiration
- Cache invalidation on writes
- Fallback to trie on cache miss

---

### Observability

#### [NEW] src/metrics.rs
Prometheus metrics:
- `prefix_search_latency_seconds` (histogram)
- `prefix_search_requests_total` (counter by shard, status)
- `trie_memory_bytes` (gauge per shard)
- `raft_leader_elections_total` (counter)
- `cache_hit_ratio` (gauge)

---

### Kubernetes Deployment

#### [NEW] k8s/statefulset.yaml
StatefulSet for stable network identities:
- Headless service for peer discovery
- Persistent volume claims for Raft log + snapshots
- Pod anti-affinity for HA

#### [NEW] k8s/configmap.yaml
Cluster configuration:
- Seed nodes
- Partition configuration
- Redis connection

#### [NEW] k8s/service.yaml
- Headless service for internal peer discovery
- LoadBalancer service for external access

---

## Phased Delivery

### Phase 1: Multi-Node Foundation (Implemented)
- Implement prefix partition map (`PARTITION_MAP`) and prefix routing
- Modify LB to route based on prefix
- Deploy multiple nodes with static partition assignment (`PREFIX_RANGE`)

### Phase 2: Read Replicas + HA (Implemented)
- Support multiple replicas per shard (replica lists in `PARTITION_MAP` via `|`)
- Add standard gRPC health checks on servers (`grpc.health.v1.Health`)
- LB does random replica selection + active health probing + bounded retries/failover

### Phase 3: Writes + Durability (Deprecated)
- This project no longer supports LB-fanout replication + per-node WAL.
- Writes are persisted and replicated via Raft (Phase 4+).

### Phase 4: Consensus (Raft) + Leader-Based Replication (Implemented MVP)
- Integrate `openraft` for per-shard leader election and membership changes
- Replace LB fanout writes with leader-coordinated replication (Raft log replication + commit index)

### Phase 5: Read Semantics (Raft) - HA / Eventual Reads First
- Document Raft-mode read semantics as HA/eventual by default.
- **Contract (Raft mode, current behavior)**:
- `SayHello` is served from a node's local in-memory trie without a Raft read barrier.
- LB may route reads to any healthy replica (random + retries). If `R>1`, LB requires `R` successful responses
  but does not reconcile/verify payload equality across replicas.
- **Implication**: reads can be stale if a follower is behind (or immediately after leader change).
- **Why**: prioritizes availability and simplicity while we harden leader discovery and membership changes.
- **Non-goals in this phase**:
- No linearizable reads (ReadIndex / `ensure_linearizable()`).
- No digest comparison, version reconciliation, or read-repair (Cassandra-style).
- Linearizable reads + an explicit `READ_CONSISTENCY` knob are deferred to Phase 11.

### Phase 6: Leader Discovery Hardening (Raft)
- Replace string-parsed leader redirects with a structured leader discovery mechanism:
- Return leader hint via gRPC response metadata/trailers (e.g. `x-raft-leader`) or a dedicated `GetLeader` RPC.
- LB caches leader per shard with TTL and falls back to probing on errors.
- Keep error codes stable (`FAILED_PRECONDITION` for "not leader") without embedding parse-only semantics in message text.

### Phase 7: Membership Changes / Control Plane (Raft)
- Turn cluster membership into an explicit control-plane workflow (per shard):
- Add/remove nodes safely (learners -> voters), with clear operational steps and guardrails.
- Expose minimal admin API/CLI for membership changes (or scriptable control-plane entrypoints).

### Phase 8: Storage Boundary Cleanup (Raft)
- Remove `data_dir` from the Raft state machine/build path:
- For persistent Raft, attach `data_dir` to Raft storage implementations (raft log store + snapshot store), not the trie state machine.
- Prepare for snapshots + log compaction after the boundary is cleaned up.

### Phase 9: Caching + Observability
- Redis integration for hot prefixes
- Prometheus metrics + Grafana dashboards
- Structured logging + tracing

### Phase 10: Kubernetes + Production Hardening
- StatefulSet deployment with PVCs for Raft log/snapshots
- CI/CD pipeline
- Load testing + benchmarking and failure injection

### Phase 11: Strong Reads (Raft) - Linearizable Reads via ReadIndex
- Implement `READ_CONSISTENCY=linearizable`:
- Route reads to leader and use OpenRaft linearizable read barrier (ReadIndex / `ensure_linearizable()`).
- Decide whether to support follower reads with read-index/leases after the leader-only variant is stable.
- Add tests that demonstrate linearizability vs eventual reads under leader change and follower lag.
- Add `READ_CONSISTENCY=eventual|linearizable` config knob (default `eventual`) and document HA vs strong tradeoffs.

### Phase 12: Documentation + Blog
- Architecture documentation and operational runbooks
- "Building a Distributed Prefix Search" blog series

---

## Verification Plan

### Automated Tests

```bash
# Unit tests for partition logic
cargo test partition::

# Integration test for cluster formation
cargo test --test cluster_formation

# Chaos testing - kill leader, verify failover
./scripts/chaos_test.sh

# Load test with ghz
ghz --insecure --proto ./proto/helloworld.proto \
    --call helloworld.Greeter/SayHello \
    -d '{"name":"app","tenant":"default"}' \
    -n 10000 -c 50 \
    localhost:50052
```

### Manual Verification
- Deploy 3-node cluster locally with docker-compose
- Verify prefix routing by checking server logs
- Kill leader node, observe failover time
- Measure latency with and without Redis cache

---

## Success Metrics

| Metric | Target | Why It Matters |
|--------|--------|----------------|
| Query latency (P99) | < 10ms | Competitive with Elasticsearch |
| Failover time | < 5s | Production-ready HA |
| Throughput | > 10K QPS | Demonstrates horizontal scaling |
| Memory efficiency | < 300MB per 370K words | Improved from current 700MB |

---

## Blog Series Outline

1. **"Why I Built My Own Distributed Prefix Search in Rust"** - Problem statement, architecture overview
2. **"Prefix-Aware Consistent Hashing"** - Deep dive into sharding strategy
3. **"Implementing Raft Consensus in a Real System"** - Practical lessons from openraft integration
4. **"From EC2 to EKS: Kubernetes-Native Rust Services"** - Deployment patterns
5. **"Benchmarking Against Elasticsearch"** - Performance comparison and analysis
