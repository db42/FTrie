# Phase 2: Read Replicas + HA (Static Config)

This phase adds multiple replicas per prefix shard and makes the load balancer (LB) do:

- random replica selection
- active health checks (gRPC health service)
- bounded retries / failover

No write path is added in this phase. All replicas are "read replicas" because they build the same in-memory index from the same static word files.

## Run

From `helloworld-tonic/`:

```bash
./scripts/run_local_phase2_replicas.sh
```

## LB Configuration

- `LB_POLICY` (only `random` supported for now)
- `LB_MAX_ATTEMPTS` (default `3`)
- `LB_HEALTH_INTERVAL_MS` (default `1000`)
- `LB_FAIL_AFTER` consecutive failures to mark unhealthy (default `2`)
- `LB_RECOVER_AFTER` consecutive successes to mark healthy (default `1`)
- `PARTITION_MAP`
  - Example: `a-i=http://127.0.0.1:50051|http://127.0.0.1:50061,j-r=http://127.0.0.1:50053|http://127.0.0.1:50063`

## Server Configuration

- `BIND_ADDR`
- `PREFIX_RANGE`
- `NODE_ID` (optional, for logs)
- `INCLUDE_NODE_ID_IN_REPLY=1` (optional, includes `node=<NODE_ID>` in the response message for testing)

## Notes

- Health probing uses the standard gRPC health API (`grpc.health.v1.Health`) with an empty service name.
- When all replicas for a shard are unhealthy/unreachable, the LB returns `UNAVAILABLE`.
