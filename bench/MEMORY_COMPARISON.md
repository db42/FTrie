# Memory Comparison Methodology

This note captures exactly how memory was compared between:
- Rust prefix service (`ftrie-server`)
- Elasticsearch (Docker, single node)

## Dataset Files Used

- `words_alpha.txt`: [words_alpha.txt](/Users/dushyant.bansal/work/ftrie/words_alpha.txt)
- `words.txt`: [words.txt](/Users/dushyant.bansal/work/ftrie/words.txt)

These are the same files loaded by the Rust server static index path and by ES ingest.

## Elasticsearch Memory Measurement

### Setup

Elasticsearch was started using:
- [run_local_es_single_node.sh](/Users/dushyant.bansal/work/ftrie/scripts/run_local_es_single_node.sh)

Important settings in that script:
- container name: `helloworld-es-bench`
- heap: `ES_JAVA_OPTS=-Xms4g -Xmx4g`
- single node mode

### Runtime Memory Commands

Container-level memory:

```bash
docker stats --no-stream --format 'name={{.Name}} mem={{.MemUsage}} cpu={{.CPUPerc}}' helloworld-es-bench
```

Process-level memory from inside container:

```bash
docker exec helloworld-es-bench sh -lc "ps -eo pid,args | sed -n '1,40p'"
docker exec helloworld-es-bench sh -lc "cat /proc/67/status | egrep 'Name|VmRSS|VmSize|VmHWM|Threads'"
```

Observed ES JVM process (PID `67`):
- `VmRSS`: `4,871,292 kB` (~4.65 GiB)
- `VmSize`: `11,354,336 kB` (~10.83 GiB)
- `VmHWM`: `4,875,944 kB`
- `Threads`: `127`

Container-level sample:
- `4.736GiB / 7.654GiB`

## Rust Server Memory Measurement

### Binary and Script References

- server binary: `./target/debug/ftrie-server`
- baseline launcher reference: [run_local_single_node_static.sh](/Users/dushyant.bansal/work/ftrie/scripts/run_local_single_node_static.sh)

The measurement was done by running `ftrie-server` directly (same env knobs as script), then reading RSS via `ps`.

### Backends Measured

- `INDEX_BACKEND=trie`
- `INDEX_BACKEND=fst`

### Runtime Memory Command Pattern

```bash
ps -o rss= -p <pid>
```

Observed Rust process RSS:
- `trie`: `830,208 kB` (~811 MiB)
- `fst`: `45,472 kB` (~44.4 MiB)

Temporary logs used during measurement:
- `/tmp/ftrie_server_trie.log`
- `/tmp/ftrie_server_fst.log`

## Index Footprint Estimation (Separate from RSS)

Tool used:
- [mem_report.rs](/Users/dushyant.bansal/work/ftrie/src/mem_report.rs)
- binary registration in [Cargo.toml](/Users/dushyant.bansal/work/ftrie/Cargo.toml:18)

Run command:

```bash
cargo run --bin mem-report
```

Observed outputs:
- `total_fst_bytes = 1,650,560` (~1.57 MiB)
- `approx_trie_bytes = 888,032,160` (~847 MiB)

## What This Comparison Represents

- **Runtime memory**: ES JVM RSS vs Rust server RSS.
- **Index-size estimate**: FST actual bytes and trie approximate bytes from `mem-report`.

So this is both:
1. operational service memory comparison, and
2. data-structure footprint comparison.
