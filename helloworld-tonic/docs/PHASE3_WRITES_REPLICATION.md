# Phase 3: Writes + Replication + Durability (WAL)

This phase adds a write API and durability per replica.

- Writes are replicated by the LB (fanout) and acknowledged based on `W` successes.
- Reads require `R` successful replica responses.
- Each replica persists writes in a per-tenant WAL file and replays it on restart.

## Run

From `helloworld-tonic/`:

```bash
./scripts/run_local_phase3_writes.sh
```

Override quorums:

```bash
R=1 W=2 ./scripts/run_local_phase3_writes.sh
```

## API

### PutWord

```bash
grpcurl -plaintext -import-path ./proto -proto helloworld.proto \
  -d '{"word":"jokerz","tenant":"power"}' \
  127.0.0.1:50052 helloworld.Greeter/PutWord
```

### SayHello (prefix query)

```bash
grpcurl -plaintext -import-path ./proto -proto helloworld.proto \
  -d '{"name":"joker","tenant":"power"}' \
  127.0.0.1:50052 helloworld.Greeter/SayHello
```

## Configuration

### LB

- `R` (default `1`): require `R` successful read responses
- `W` (default `1`): require `W` successful write responses

### Server

- `DATA_DIR` (default `./data`): WAL directory
- `FSYNC` (default `0`): set `1` for stronger durability (slower)

