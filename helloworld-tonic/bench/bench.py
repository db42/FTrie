#!/usr/bin/env python3
import argparse
import asyncio
import hashlib
import json
import os
import random
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
PROTO = ROOT / "proto" / "helloworld.proto"
GEN_DIR = Path(__file__).resolve().parent / "_gen"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _git(cmd: List[str]) -> str:
    try:
        out = subprocess.check_output(cmd, cwd=str(ROOT), stderr=subprocess.DEVNULL)
        return out.decode("utf-8", errors="replace").strip()
    except Exception:
        return "unknown"

def _pick_unused_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])
    finally:
        s.close()


def _wait_for_port(host: str, port: int, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    last_err: Optional[Exception] = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.25):
                return
        except Exception as e:
            last_err = e
            time.sleep(0.05)
    raise RuntimeError(f"timed out waiting for {host}:{port} ({last_err})")


def ensure_proto_generated() -> None:
    GEN_DIR.mkdir(parents=True, exist_ok=True)
    stamp = GEN_DIR / ".stamp"
    proto_hash = _sha256_file(PROTO)
    if stamp.exists() and stamp.read_text().strip() == proto_hash:
        return

    try:
        from grpc_tools import protoc  # type: ignore
    except Exception as e:
        raise SystemExit(
            "grpcio-tools is required. Install with: pip install -r bench/requirements.txt"
        ) from e

    args = [
        "protoc",
        f"-I{ROOT / 'proto'}",
        f"--python_out={GEN_DIR}",
        f"--grpc_python_out={GEN_DIR}",
        str(PROTO),
    ]
    rc = protoc.main(args)
    if rc != 0:
        raise SystemExit(f"protoc failed with exit code {rc}")
    stamp.write_text(proto_hash)


def _load_words(path: Path, prefix_lengths: List[int]) -> Tuple[List[str], Dict[int, List[str]], Dict[int, Dict[str, int]]]:
    words: List[str] = []
    prefixes_by_len: Dict[int, List[str]] = {l: [] for l in prefix_lengths}
    freq_by_len: Dict[int, Dict[str, int]] = {l: {} for l in prefix_lengths}

    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            w = line.strip().lower()
            if not w:
                continue
            if not all("a" <= c <= "z" for c in w):
                continue
            words.append(w)
            for l in prefix_lengths:
                if len(w) < l:
                    continue
                p = w[:l]
                prefixes_by_len[l].append(p)
                freq_by_len[l][p] = freq_by_len[l].get(p, 0) + 1

    # Dedup prefix lists for uniform sampling.
    for l in prefix_lengths:
        prefixes_by_len[l] = sorted(set(prefixes_by_len[l]))
    return words, prefixes_by_len, freq_by_len


def generate_workloads(
    out_dir: Path,
    wordlist: Path,
    tenant: str,
    prefix_lengths: List[int],
    top_ks: List[int],
    n_hot: int,
    n_uniform: int,
    n_miss: int,
    seed: int,
) -> Dict[str, Any]:
    rng = random.Random(seed)
    _, prefixes_by_len, freq_by_len = _load_words(wordlist, prefix_lengths)

    all_prefixes = {l: set(prefixes_by_len[l]) for l in prefix_lengths}

    out_dir.mkdir(parents=True, exist_ok=True)

    def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
        with path.open("w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, separators=(",", ":")) + "\n")

    meta: Dict[str, Any] = {
        "seed": seed,
        "wordlist": str(wordlist),
        "wordlist_sha256": _sha256_file(wordlist),
        "tenant": tenant,
        "prefix_lengths": prefix_lengths,
        "top_ks": top_ks,
        "counts": {"hot": n_hot, "uniform": n_uniform, "miss": n_miss},
        "files": [],
    }

    for top_k in top_ks:
        for l in prefix_lengths:
            # Hot: weighted by prefix frequency, restricted to top-N frequent prefixes.
            freq_items = sorted(freq_by_len[l].items(), key=lambda kv: kv[1], reverse=True)
            hot_pool = freq_items[: min(2000, len(freq_items))]
            hot_prefixes = [p for p, _ in hot_pool]
            hot_weights = [w for _, w in hot_pool]
            hot_rows = [{"tenant": tenant, "prefix": rng.choices(hot_prefixes, weights=hot_weights, k=1)[0], "top_k": top_k} for _ in range(n_hot)]

            uniform_prefixes = prefixes_by_len[l]
            uniform_rows = [{"tenant": tenant, "prefix": rng.choice(uniform_prefixes), "top_k": top_k} for _ in range(n_uniform)]

            miss_rows: List[Dict[str, Any]] = []
            attempts = 0
            while len(miss_rows) < n_miss and attempts < n_miss * 50:
                attempts += 1
                p = "".join(chr(ord("a") + rng.randrange(26)) for _ in range(l))
                if p in all_prefixes[l]:
                    continue
                miss_rows.append({"tenant": tenant, "prefix": p, "top_k": top_k})

            hot_path = out_dir / f"hot_len{l}_k{top_k}.jsonl"
            uniform_path = out_dir / f"uniform_len{l}_k{top_k}.jsonl"
            miss_path = out_dir / f"miss_len{l}_k{top_k}.jsonl"

            write_jsonl(hot_path, hot_rows)
            write_jsonl(uniform_path, uniform_rows)
            write_jsonl(miss_path, miss_rows)

            meta["files"].extend(
                [
                    {"name": hot_path.name, "sha256": _sha256_file(hot_path), "rows": len(hot_rows)},
                    {"name": uniform_path.name, "sha256": _sha256_file(uniform_path), "rows": len(uniform_rows)},
                    {"name": miss_path.name, "sha256": _sha256_file(miss_path), "rows": len(miss_rows)},
                ]
            )

    return meta


def _load_queries(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    if not rows:
        raise SystemExit(f"empty workload file: {path}")
    return rows


@dataclass
class RunStats:
    requests: int
    errors: int
    elapsed_s: float
    avg_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float


async def run_workload(
    endpoint: str,
    workload_path: Path,
    concurrency: int,
    warmup_s: float,
    duration_s: float,
    validate: bool,
) -> RunStats:
    ensure_proto_generated()
    sys.path.insert(0, str(GEN_DIR))
    import grpc  # type: ignore
    from hdrh.histogram import HdrHistogram  # type: ignore

    import helloworld_pb2  # type: ignore
    import helloworld_pb2_grpc  # type: ignore

    queries = _load_queries(workload_path)
    idx = 0

    async def next_query() -> Dict[str, Any]:
        nonlocal idx
        q = queries[idx]
        idx = (idx + 1) % len(queries)
        return q

    warmup_deadline = time.monotonic() + warmup_s
    end_deadline = warmup_deadline + duration_s

    async def worker() -> Tuple[int, int, float, float, HdrHistogram]:
        channel = grpc.aio.insecure_channel(endpoint)
        stub = helloworld_pb2_grpc.GreeterStub(channel)
        hist = HdrHistogram(1, 60_000_000, 3)  # 1us..60s in us
        requests = 0
        errors = 0
        sum_ms = 0.0
        max_ms = 0.0
        while True:
            now = time.monotonic()
            if now >= end_deadline:
                await channel.close()
                return (requests, errors, sum_ms, max_ms, hist)
            q = await next_query()
            req = helloworld_pb2.HelloRequest(
                name=q["prefix"],
                tenant=q["tenant"],
                top_k=int(q.get("top_k", 0)),
            )
            t0 = time.perf_counter_ns()
            try:
                resp = await stub.SayHello(req)
                # Basic correctness guard.
                if validate:
                    tk = int(q.get("top_k", 0))
                    if tk > 0 and len(resp.matches) > tk:
                        raise RuntimeError(f"server returned {len(resp.matches)} > top_k={tk}")
                ok = True
            except Exception:
                ok = False

            t1 = time.perf_counter_ns()
            dur_ms = (t1 - t0) / 1e6

            # Warmup: ignore stats but keep pressure on the server.
            if time.monotonic() < warmup_deadline:
                continue
            requests += 1
            if not ok:
                errors += 1
                continue
            sum_ms += dur_ms
            if dur_ms > max_ms:
                max_ms = dur_ms
            hist.record_value(int(dur_ms * 1000.0))  # us

    parts = await asyncio.gather(*[asyncio.create_task(worker()) for _ in range(concurrency)])
    requests = sum(p[0] for p in parts)
    errors = sum(p[1] for p in parts)
    sum_ms = sum(p[2] for p in parts)
    max_ms = max((p[3] for p in parts), default=0.0)
    hist = HdrHistogram(1, 60_000_000, 3)
    for p in parts:
        if hasattr(hist, "add"):
            hist.add(p[4])
        else:
            hist = p[4]
            break

    elapsed_s = duration_s
    avg_ms = (sum_ms / requests) if requests else 0.0
    p50_ms = hist.get_value_at_percentile(50.0) / 1000.0 if requests else 0.0
    p95_ms = hist.get_value_at_percentile(95.0) / 1000.0 if requests else 0.0
    p99_ms = hist.get_value_at_percentile(99.0) / 1000.0 if requests else 0.0

    return RunStats(
        requests=requests,
        errors=errors,
        elapsed_s=elapsed_s,
        avg_ms=avg_ms,
        p50_ms=p50_ms,
        p95_ms=p95_ms,
        p99_ms=p99_ms,
        max_ms=max_ms,
    )


def _workload_key(path: Path) -> str:
    # hot_len3_k10.jsonl -> hot_len3_k10
    return path.stem


def cmd_generate(args: argparse.Namespace) -> None:
    out_dir = Path(args.out_dir)
    meta = generate_workloads(
        out_dir=out_dir,
        wordlist=Path(args.wordlist),
        tenant=args.tenant,
        prefix_lengths=[1, 3, 5],
        top_ks=[10, 50],
        n_hot=args.n_hot,
        n_uniform=args.n_uniform,
        n_miss=args.n_miss,
        seed=args.seed,
    )
    meta_path = out_dir / "META.json"
    meta_path.write_text(json.dumps(meta, indent=2) + "\n")
    print(str(meta_path))


def cmd_compare(args: argparse.Namespace) -> None:
    a = json.loads(Path(args.a).read_text())
    b = json.loads(Path(args.b).read_text())

    def hdr(d: Dict[str, Any]) -> str:
        return f"{d.get('git_commit','?')} {d.get('timestamp','?')}"

    print("========================================")
    print("  Benchmark Comparison")
    print("========================================")
    print("")
    print(f"Run A: {args.a}")
    print(f"  {hdr(a)}")
    print(f"Run B: {args.b}")
    print(f"  {hdr(b)}")
    print("")

    wa = a.get("workloads", {})
    wb = b.get("workloads", {})
    keys = sorted(set(wa.keys()) & set(wb.keys()))
    for k in keys:
        ra = wa[k]
        rb = wb[k]
        print(f"Workload: {k}")
        print(f"{'':20s} {'A':>12s} {'B':>12s} {'Delta':>12s}")
        print(f"{'---':20s} {'---':>12s} {'---':>12s} {'---':>12s}")

        def row(label: str, field: str, invert: bool = False) -> None:
            va = float(ra.get(field, 0))
            vb = float(rb.get(field, 0))
            if va == 0:
                delta = "n/a"
            else:
                pct = (vb - va) * 100.0 / va
                if invert:
                    # lower is better
                    pct = -pct
                delta = f"{pct:+.1f}%"
            print(f"{label:20s} {va:12.3f} {vb:12.3f} {delta:>12s}")

        row("QPS", "qps", invert=False)
        row("Avg Lat (ms)", "latency_avg_ms", invert=True)
        row("P95 Lat (ms)", "latency_p95_ms", invert=True)
        row("P99 Lat (ms)", "latency_p99_ms", invert=True)
        print("")


def cmd_run(args: argparse.Namespace) -> None:
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    workload_paths = [Path(p) for p in args.workloads]
    for p in workload_paths:
        if not p.exists():
            raise SystemExit(f"workload not found: {p}")

    git_commit = _git(["git", "rev-parse", "--short", "HEAD"])
    git_branch = _git(["git", "rev-parse", "--abbrev-ref", "HEAD"])
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    results: Dict[str, Any] = {
        "timestamp": timestamp,
        "git_commit": git_commit,
        "git_branch": git_branch,
        "config": {
            "endpoint": args.endpoint,
            "concurrency": args.concurrency,
            "warmup_s": args.warmup_s,
            "duration_s": args.duration_s,
        },
        "workloads": {},
    }

    async def run_all() -> None:
        for wp in workload_paths:
            key = _workload_key(wp)
            st = await run_workload(
                endpoint=args.endpoint,
                workload_path=wp,
                concurrency=args.concurrency,
                warmup_s=args.warmup_s,
                duration_s=args.duration_s,
                validate=args.validate,
            )
            qps = (st.requests / st.elapsed_s) if st.elapsed_s > 0 else 0.0
            results["workloads"][key] = {
                "qps": qps,
                "requests": st.requests,
                "errors": st.errors,
                "latency_avg_ms": st.avg_ms,
                "latency_p50_ms": st.p50_ms,
                "latency_p95_ms": st.p95_ms,
                "latency_p99_ms": st.p99_ms,
                "latency_max_ms": st.max_ms,
                "workload_sha256": _sha256_file(wp),
            }
            print(
                f"{key}: qps={qps:.1f} p50={st.p50_ms:.3f}ms p95={st.p95_ms:.3f}ms p99={st.p99_ms:.3f}ms errors={st.errors}"
            )

    asyncio.run(run_all())

    if args.output:
        out_path = Path(args.output)
    else:
        out_path = out_dir / f"{time.strftime('%Y%m%d_%H%M%S')}_{git_commit}.json"

    out_path.write_text(json.dumps(results, indent=2) + "\n")
    print(str(out_path))

def cmd_matrix(args: argparse.Namespace) -> None:
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    workload_paths = [Path(p) for p in args.workloads]
    for p in workload_paths:
        if not p.exists():
            raise SystemExit(f"workload not found: {p}")

    concurrencies = [int(x) for x in args.concurrency.split(",") if x.strip()]

    results: Dict[str, Any] = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "git_commit": _git(["git", "rev-parse", "--short", "HEAD"]),
        "git_branch": _git(["git", "rev-parse", "--abbrev-ref", "HEAD"]),
        "config": {
            "endpoint": args.endpoint,
            "warmup_s": args.warmup_s,
            "duration_s": args.duration_s,
            "concurrencies": concurrencies,
            "validate": args.validate,
        },
        "workloads": {},
    }

    async def run_one(c: int) -> None:
        for wp in workload_paths:
            key = f"{_workload_key(wp)}_c{c}"
            st = await run_workload(
                endpoint=args.endpoint,
                workload_path=wp,
                concurrency=c,
                warmup_s=args.warmup_s,
                duration_s=args.duration_s,
                validate=args.validate,
            )
            qps = (st.requests / st.elapsed_s) if st.elapsed_s > 0 else 0.0
            results["workloads"][key] = {
                "qps": qps,
                "requests": st.requests,
                "errors": st.errors,
                "latency_avg_ms": st.avg_ms,
                "latency_p50_ms": st.p50_ms,
                "latency_p95_ms": st.p95_ms,
                "latency_p99_ms": st.p99_ms,
                "latency_max_ms": st.max_ms,
                "workload_sha256": _sha256_file(wp),
                "workload_file": wp.name,
                "concurrency": c,
            }
            print(
                f"{key}: qps={qps:.1f} p50={st.p50_ms:.3f}ms p95={st.p95_ms:.3f}ms p99={st.p99_ms:.3f}ms errors={st.errors}"
            )

    for c in concurrencies:
        asyncio.run(run_one(c))

    out_path = Path(args.output) if args.output else out_dir / f"{time.strftime('%Y%m%d_%H%M%S')}_{results['git_commit']}.json"
    out_path.write_text(json.dumps(results, indent=2) + "\n")
    print(str(out_path))

def cmd_suite(args: argparse.Namespace) -> None:
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    workload_dir = Path(args.workload_dir)
    if not workload_dir.exists():
        raise SystemExit(f"workload_dir not found: {workload_dir}")

    workload_paths = sorted(workload_dir.glob("*.jsonl"))
    if args.only:
        want = set(args.only)
        workload_paths = [p for p in workload_paths if p.name in want or p.stem in want]
    if not workload_paths:
        raise SystemExit("no workloads selected")

    concurrencies = [int(x) for x in args.concurrency.split(",") if x.strip()]

    server_bin = Path(args.server_bin) if args.server_bin else (ROOT / "target" / "release" / "helloworld-server")
    if not args.skip_build and not args.server_bin:
        subprocess.check_call(["cargo", "build", "--release"], cwd=str(ROOT))

    if not server_bin.exists():
        raise SystemExit(f"server binary not found: {server_bin}")

    port = _pick_unused_port()
    bind_addr = f"127.0.0.1:{port}"
    endpoint = bind_addr

    data_dir = tempfile.mkdtemp(prefix="helloworld-bench-")
    log_path = out_dir / "server.log"
    log = open(log_path, "wb")

    env = os.environ.copy()
    env["BIND_ADDR"] = bind_addr
    env["NODE_ID"] = "bench"
    env["PREFIX_RANGE"] = "a-z"
    env["DATA_DIR"] = data_dir
    env["FSYNC"] = "0"
    env["INCLUDE_NODE_ID_IN_REPLY"] = "0"
    env["DISABLE_STATIC_INDEX"] = "1" if args.disable_static_index else "0"

    proc = subprocess.Popen([str(server_bin)], cwd=str(ROOT), env=env, stdout=log, stderr=log)
    try:
        _wait_for_port("127.0.0.1", port, timeout_s=15.0)

        results: Dict[str, Any] = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "git_commit": _git(["git", "rev-parse", "--short", "HEAD"]),
            "git_branch": _git(["git", "rev-parse", "--abbrev-ref", "HEAD"]),
            "config": {
                "endpoint": endpoint,
                "warmup_s": args.warmup_s,
                "duration_s": args.duration_s,
                "concurrencies": concurrencies,
                "validate": args.validate,
                "disable_static_index": bool(args.disable_static_index),
            },
            "workloads": {},
        }

        async def run_one(c: int) -> None:
            for wp in workload_paths:
                key = f"{_workload_key(wp)}_c{c}"
                st = await run_workload(
                    endpoint=endpoint,
                    workload_path=wp,
                    concurrency=c,
                    warmup_s=args.warmup_s,
                    duration_s=args.duration_s,
                    validate=args.validate,
                )
                qps = (st.requests / st.elapsed_s) if st.elapsed_s > 0 else 0.0
                results["workloads"][key] = {
                    "qps": qps,
                    "requests": st.requests,
                    "errors": st.errors,
                    "latency_avg_ms": st.avg_ms,
                    "latency_p50_ms": st.p50_ms,
                    "latency_p95_ms": st.p95_ms,
                    "latency_p99_ms": st.p99_ms,
                    "latency_max_ms": st.max_ms,
                    "workload_sha256": _sha256_file(wp),
                    "workload_file": wp.name,
                    "concurrency": c,
                }
                print(
                    f"{key}: qps={qps:.1f} p50={st.p50_ms:.3f}ms p95={st.p95_ms:.3f}ms p99={st.p99_ms:.3f}ms errors={st.errors}"
                )

        for c in concurrencies:
            asyncio.run(run_one(c))

        out_path = out_dir / f"{time.strftime('%Y%m%d_%H%M%S')}_{results['git_commit']}.json"
        out_path.write_text(json.dumps(results, indent=2) + "\n")
        print(str(out_path))
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
        log.close()


def main() -> None:
    ap = argparse.ArgumentParser(prog="bench.py")
    sub = ap.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate", help="generate workload jsonl files")
    g.add_argument("--wordlist", default=str(ROOT / "words_alpha.txt"))
    g.add_argument("--tenant", default="power")
    g.add_argument("--out-dir", default=str(Path(__file__).resolve().parent / "workloads"))
    g.add_argument("--n-hot", type=int, default=20000)
    g.add_argument("--n-uniform", type=int, default=20000)
    g.add_argument("--n-miss", type=int, default=5000)
    g.add_argument("--seed", type=int, default=1)
    g.set_defaults(func=cmd_generate)

    r = sub.add_parser("run", help="run end-to-end gRPC benchmarks")
    r.add_argument("--endpoint", default="127.0.0.1:50052", help="host:port (no scheme)")
    r.add_argument("--concurrency", type=int, default=32)
    r.add_argument("--warmup-s", type=float, default=20.0)
    r.add_argument("--duration-s", type=float, default=60.0)
    r.add_argument("--out-dir", default=str(Path(__file__).resolve().parent / "results"))
    r.add_argument("--output", default="")
    r.add_argument("--validate", action="store_true", help="validate replies (slower)")
    r.add_argument("workloads", nargs="+")
    r.set_defaults(func=cmd_run)

    m = sub.add_parser("matrix", help="run a workload matrix against an existing endpoint")
    m.add_argument("--endpoint", default="127.0.0.1:50052", help="host:port (no scheme)")
    m.add_argument("--concurrency", default="1,8,32,128")
    m.add_argument("--warmup-s", type=float, default=20.0)
    m.add_argument("--duration-s", type=float, default=60.0)
    m.add_argument("--out-dir", default=str(Path(__file__).resolve().parent / "results"))
    m.add_argument("--output", default="")
    m.add_argument("--validate", action="store_true")
    m.add_argument("workloads", nargs="+")
    m.set_defaults(func=cmd_matrix)

    s = sub.add_parser("suite", help="run a local server and a workload matrix")
    s.add_argument("--skip-build", action="store_true")
    s.add_argument("--server-bin", default="", help="path to helloworld-server (skips build)")
    s.add_argument("--disable-static-index", action="store_true")
    s.add_argument("--workload-dir", default=str(Path(__file__).resolve().parent / "workloads"))
    s.add_argument("--only", action="append", default=[], help="workload file name or stem")
    s.add_argument("--concurrency", default="1,8,32,128")
    s.add_argument("--warmup-s", type=float, default=20.0)
    s.add_argument("--duration-s", type=float, default=60.0)
    s.add_argument("--out-dir", default=str(Path(__file__).resolve().parent / "results"))
    s.add_argument("--validate", action="store_true")
    s.set_defaults(func=cmd_suite)

    c = sub.add_parser("compare", help="compare two result json files")
    c.add_argument("a")
    c.add_argument("b")
    c.set_defaults(func=cmd_compare)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
