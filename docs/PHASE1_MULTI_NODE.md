# Phase 1: Static 3-Node Prefix Sharding

This phase runs 3 `ftrie-server` instances, each indexing only a static prefix range, and a `ftrie-lb` that routes requests to the correct server based on the first character of the requested prefix.

## Run

From repo root:

```bash
./scripts/run_local_3nodes.sh
```

## How Routing Works

- The LB looks at `HelloRequest.name` (the prefix being queried).
- It takes the first character, normalizes it to lowercase ASCII, and routes based on `PARTITION_MAP`.
- Each server only indexes words whose first character is within its `PREFIX_RANGE`.

## Configuration

### Servers

- `BIND_ADDR` (default `[::]:50051`)
- `PREFIX_RANGE` (default `a-z`, format `a-z`)

### Load Balancer

- `BIND_ADDR` (default `[::]:50052`)
- `PARTITION_MAP` (required for multi-node)
  - Example: `a-i=http://127.0.0.1:50051,j-r=http://127.0.0.1:50053,s-z=http://127.0.0.1:50054`
  - Backends can be `|` separated (reserved for replication later): `a-i=addr1|addr2|addr3`
