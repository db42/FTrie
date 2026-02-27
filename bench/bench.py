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
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
PROTO = ROOT / "proto" / "ftrie.proto"
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

def _normalize_error(e: Exception) -> str:
    # grpc.aio.AioRpcError has .code() and .details(). Prefer stable strings (avoid timestamps).
    code = getattr(e, "code", None)
    details = getattr(e, "details", None)
    try:
        if callable(code) and callable(details):
            c = code()
            d = details()
            c_name = getattr(c, "name", None)
            c_str = c_name if isinstance(c_name, str) else str(c)
            return f"{c_str}: {d}"
    except Exception:
        pass
    msg = str(e).strip()
    if msg:
        return msg[:300]
    return e.__class__.__name__

def _top_n_counts(d: Dict[str, int], n: int) -> Dict[str, int]:
    if len(d) <= n:
        return d
    items = sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:n]
    return dict(items)

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


def _normalize_http_endpoint(endpoint: str) -> str:
    e = endpoint.strip()
    if not e:
        raise SystemExit("empty endpoint")
    if e.startswith("http://") or e.startswith("https://"):
        return e.rstrip("/")
    return f"http://{e}".rstrip("/")


def _http_json(
    method: str,
    url: str,
    payload: Optional[Dict[str, Any]] = None,
    timeout_s: float = 30.0,
) -> Tuple[int, Any]:
    data = None
    headers = {"accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["content-type"] = "application/json"
    req = urllib.request.Request(url=url, method=method.upper(), data=data, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            status = int(resp.getcode())
            raw = resp.read()
    except urllib.error.HTTPError as e:
        status = int(e.code)
        raw = e.read()
    body: Any = None
    if raw:
        txt = raw.decode("utf-8", errors="replace")
        try:
            body = json.loads(txt)
        except Exception:
            body = txt
    return status, body


def _http_ndjson(
    method: str,
    url: str,
    payload: bytes,
    timeout_s: float = 60.0,
) -> Tuple[int, Any]:
    req = urllib.request.Request(
        url=url,
        method=method.upper(),
        data=payload,
        headers={
            "accept": "application/json",
            "content-type": "application/x-ndjson",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            status = int(resp.getcode())
            raw = resp.read()
    except urllib.error.HTTPError as e:
        status = int(e.code)
        raw = e.read()
    body: Any = None
    if raw:
        txt = raw.decode("utf-8", errors="replace")
        try:
            body = json.loads(txt)
        except Exception:
            body = txt
    return status, body


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
    requests_total: int
    requests_ok: int
    errors: int
    elapsed_s: float
    avg_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float
    error_counts: Dict[str, int]


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

    import ftrie_pb2  # type: ignore
    import ftrie_pb2_grpc  # type: ignore

    queries = _load_queries(workload_path)
    idx = 0

    async def next_query() -> Dict[str, Any]:
        nonlocal idx
        q = queries[idx]
        idx = (idx + 1) % len(queries)
        return q

    warmup_deadline = time.monotonic() + warmup_s
    end_deadline = warmup_deadline + duration_s

    async def worker() -> Tuple[int, int, int, float, float, HdrHistogram, Dict[str, int]]:
        channel = grpc.aio.insecure_channel(endpoint)
        stub = ftrie_pb2_grpc.PrefixMatcherStub(channel)
        hist = HdrHistogram(1, 60_000_000, 3)  # 1us..60s in us
        requests_total = 0
        requests_ok = 0
        errors = 0
        sum_ms = 0.0
        max_ms = 0.0
        err_counts: Dict[str, int] = {}
        while True:
            now = time.monotonic()
            if now >= end_deadline:
                await channel.close()
                return (requests_total, requests_ok, errors, sum_ms, max_ms, hist, err_counts)
            q = await next_query()
            req = ftrie_pb2.HelloRequest(
                name=q["prefix"],
                tenant=q["tenant"],
                top_k=int(q.get("top_k", 0)),
            )
            t0 = time.perf_counter_ns()
            try:
                resp = await stub.GetPrefixMatch(req)
                # Basic correctness guard.
                if validate:
                    tk = int(q.get("top_k", 0))
                    if tk > 0 and len(resp.matches) > tk:
                        raise RuntimeError(f"server returned {len(resp.matches)} > top_k={tk}")
                ok = True
            except Exception as e:
                msg = _normalize_error(e)
                err_counts[msg] = err_counts.get(msg, 0) + 1
                ok = False

            t1 = time.perf_counter_ns()
            dur_ms = (t1 - t0) / 1e6

            # Warmup: ignore stats but keep pressure on the server.
            if time.monotonic() < warmup_deadline:
                continue

            requests_total += 1
            if not ok:
                errors += 1
                continue
            requests_ok += 1
            sum_ms += dur_ms
            if dur_ms > max_ms:
                max_ms = dur_ms
            hist.record_value(int(dur_ms * 1000.0))  # us

    parts = await asyncio.gather(*[asyncio.create_task(worker()) for _ in range(concurrency)])
    requests_total = sum(p[0] for p in parts)
    requests_ok = sum(p[1] for p in parts)
    errors = sum(p[2] for p in parts)
    sum_ms = sum(p[3] for p in parts)
    max_ms = max((p[4] for p in parts), default=0.0)
    hist = HdrHistogram(1, 60_000_000, 3)
    error_counts: Dict[str, int] = {}
    for p in parts:
        if hasattr(hist, "add"):
            hist.add(p[5])
        else:
            hist = p[5]
            break
        for k, v in p[6].items():
            error_counts[k] = error_counts.get(k, 0) + v
    error_counts = _top_n_counts(error_counts, 20)

    elapsed_s = duration_s
    avg_ms = (sum_ms / requests_ok) if requests_ok else 0.0
    p50_ms = hist.get_value_at_percentile(50.0) / 1000.0 if requests_ok else 0.0
    p95_ms = hist.get_value_at_percentile(95.0) / 1000.0 if requests_ok else 0.0
    p99_ms = hist.get_value_at_percentile(99.0) / 1000.0 if requests_ok else 0.0

    return RunStats(
        requests_total=requests_total,
        requests_ok=requests_ok,
        errors=errors,
        elapsed_s=elapsed_s,
        avg_ms=avg_ms,
        p50_ms=p50_ms,
        p95_ms=p95_ms,
        p99_ms=p99_ms,
        max_ms=max_ms,
        error_counts=error_counts,
    )


def _load_index_words(path: Path) -> List[str]:
    words: List[str] = []
    seen: set[str] = set()
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            w = line.strip().lower()
            if not w:
                continue
            if not all("a" <= c <= "z" for c in w):
                continue
            if w in seen:
                continue
            seen.add(w)
            words.append(w)
    return words


def cmd_es_prepare(args: argparse.Namespace) -> None:
    endpoint = _normalize_http_endpoint(args.endpoint)

    st, _ = _http_json("GET", f"{endpoint}/")
    if st >= 400:
        raise SystemExit(f"failed to connect to Elasticsearch at {endpoint} (status={st})")

    index_exists = _http_json("GET", f"{endpoint}/{args.index}")[0] == 200

    if args.recreate and index_exists:
        st, body = _http_json("DELETE", f"{endpoint}/{args.index}", timeout_s=120.0)
        if st not in (200, 202, 404):
            raise SystemExit(f"failed to delete index {args.index}: status={st} body={body}")
        index_exists = False

    if not index_exists:
        create_body = {
            "settings": {
                "index": {
                    "number_of_shards": args.shards,
                    "number_of_replicas": args.replicas,
                    "refresh_interval": "-1",
                }
            },
            "mappings": {
                "dynamic": "strict",
                "properties": {
                    "tenant": {"type": "keyword"},
                    "word": {"type": "keyword"},
                },
            },
        }
        st, body = _http_json("PUT", f"{endpoint}/{args.index}", payload=create_body, timeout_s=120.0)
        if st not in (200, 201):
            raise SystemExit(f"failed to create index {args.index}: status={st} body={body}")

    words = _load_index_words(Path(args.wordlist))
    if not words:
        raise SystemExit(f"no indexable words found in {args.wordlist}")

    started = time.monotonic()
    sent = 0
    batch_size = max(1, int(args.batch_size))

    for i in range(0, len(words), batch_size):
        batch = words[i : i + batch_size]
        lines: List[str] = []
        for w in batch:
            lines.append('{"index":{}}')
            lines.append(json.dumps({"tenant": args.tenant, "word": w}, separators=(",", ":")))
        payload = ("\n".join(lines) + "\n").encode("utf-8")
        st, body = _http_ndjson(
            "POST",
            f"{endpoint}/{args.index}/_bulk?refresh=false",
            payload,
            timeout_s=float(args.bulk_timeout_s),
        )
        if st >= 300:
            raise SystemExit(f"bulk ingest failed at batch {i // batch_size}: status={st} body={body}")
        if isinstance(body, dict) and body.get("errors"):
            errs: List[str] = []
            for item in body.get("items", []):
                meta = item.get("index", {})
                if "error" in meta:
                    errs.append(str(meta["error"]))
                    if len(errs) >= 3:
                        break
            raise SystemExit(f"bulk ingest contained errors: {errs}")
        sent += len(batch)
        if sent % (batch_size * 20) == 0 or sent == len(words):
            print(f"indexed {sent}/{len(words)} docs")

    st, body = _http_json(
        "PUT",
        f"{endpoint}/{args.index}/_settings",
        payload={"index": {"refresh_interval": args.refresh_interval}},
        timeout_s=120.0,
    )
    if st >= 300:
        raise SystemExit(f"failed to update index settings: status={st} body={body}")

    st, body = _http_json("POST", f"{endpoint}/{args.index}/_refresh", timeout_s=120.0)
    if st >= 300:
        raise SystemExit(f"refresh failed: status={st} body={body}")

    if args.force_merge:
        st, body = _http_json(
            "POST",
            f"{endpoint}/{args.index}/_forcemerge?max_num_segments=1",
            timeout_s=600.0,
        )
        if st >= 300:
            raise SystemExit(f"force merge failed: status={st} body={body}")

    st, body = _http_json("GET", f"{endpoint}/{args.index}/_count")
    if st >= 300:
        raise SystemExit(f"failed to read count: status={st} body={body}")
    count = body.get("count") if isinstance(body, dict) else None

    elapsed = time.monotonic() - started
    print(
        f"es_prepare complete: endpoint={endpoint} index={args.index} docs={sent} count={count} elapsed_s={elapsed:.2f}"
    )


async def run_es_workload(
    endpoint: str,
    index: str,
    workload_path: Path,
    concurrency: int,
    warmup_s: float,
    duration_s: float,
    validate: bool,
    tenant_field: str,
    word_field: str,
    request_timeout_s: float,
) -> RunStats:
    try:
        import aiohttp  # type: ignore
    except Exception as e:
        raise SystemExit(
            "aiohttp is required for ES benchmarks. Install with: pip install -r bench/requirements.txt"
        ) from e
    from hdrh.histogram import HdrHistogram  # type: ignore

    endpoint = _normalize_http_endpoint(endpoint)
    search_url = f"{endpoint}/{index}/_search"
    queries = _load_queries(workload_path)
    idx = 0

    def next_query() -> Dict[str, Any]:
        nonlocal idx
        q = queries[idx]
        idx = (idx + 1) % len(queries)
        return q

    warmup_deadline = time.monotonic() + warmup_s
    end_deadline = warmup_deadline + duration_s
    timeout = aiohttp.ClientTimeout(total=request_timeout_s)
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)

    async def worker(session: Any) -> Tuple[int, int, int, float, float, HdrHistogram, Dict[str, int]]:
        hist = HdrHistogram(1, 60_000_000, 3)  # 1us..60s in us
        requests_total = 0
        requests_ok = 0
        errors = 0
        sum_ms = 0.0
        max_ms = 0.0
        err_counts: Dict[str, int] = {}

        while True:
            if time.monotonic() >= end_deadline:
                return (requests_total, requests_ok, errors, sum_ms, max_ms, hist, err_counts)

            q = next_query()
            tenant = str(q.get("tenant", ""))
            prefix = str(q.get("prefix", ""))
            top_k = int(q.get("top_k", 10))
            if top_k <= 0:
                top_k = 10

            body = {
                "size": top_k,
                "track_total_hits": False,
                "_source": False,
                "query": {
                    "bool": {
                        "filter": [
                            {"term": {tenant_field: tenant}},
                            {"prefix": {word_field: {"value": prefix}}},
                        ]
                    }
                },
                "sort": [{word_field: "asc"}],
            }

            t0 = time.perf_counter_ns()
            try:
                async with session.post(search_url, json=body) as resp:
                    txt = await resp.text()
                    if resp.status >= 300:
                        raise RuntimeError(f"HTTP {resp.status}: {txt[:300]}")
                    data = json.loads(txt) if txt else {}
                    hits = data.get("hits", {}).get("hits", [])
                    if validate and len(hits) > top_k:
                        raise RuntimeError(f"server returned {len(hits)} > top_k={top_k}")
                ok = True
            except Exception as e:
                msg = _normalize_error(e)
                err_counts[msg] = err_counts.get(msg, 0) + 1
                ok = False

            t1 = time.perf_counter_ns()
            dur_ms = (t1 - t0) / 1e6

            if time.monotonic() < warmup_deadline:
                continue

            requests_total += 1
            if not ok:
                errors += 1
                continue

            requests_ok += 1
            sum_ms += dur_ms
            if dur_ms > max_ms:
                max_ms = dur_ms
            hist.record_value(int(dur_ms * 1000.0))  # us

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        parts = await asyncio.gather(*[asyncio.create_task(worker(session)) for _ in range(concurrency)])

    requests_total = sum(p[0] for p in parts)
    requests_ok = sum(p[1] for p in parts)
    errors = sum(p[2] for p in parts)
    sum_ms = sum(p[3] for p in parts)
    max_ms = max((p[4] for p in parts), default=0.0)
    hist = HdrHistogram(1, 60_000_000, 3)
    error_counts: Dict[str, int] = {}
    for p in parts:
        if hasattr(hist, "add"):
            hist.add(p[5])
        else:
            hist = p[5]
            break
        for k, v in p[6].items():
            error_counts[k] = error_counts.get(k, 0) + v
    error_counts = _top_n_counts(error_counts, 20)

    elapsed_s = duration_s
    avg_ms = (sum_ms / requests_ok) if requests_ok else 0.0
    p50_ms = hist.get_value_at_percentile(50.0) / 1000.0 if requests_ok else 0.0
    p95_ms = hist.get_value_at_percentile(95.0) / 1000.0 if requests_ok else 0.0
    p99_ms = hist.get_value_at_percentile(99.0) / 1000.0 if requests_ok else 0.0

    return RunStats(
        requests_total=requests_total,
        requests_ok=requests_ok,
        errors=errors,
        elapsed_s=elapsed_s,
        avg_ms=avg_ms,
        p50_ms=p50_ms,
        p95_ms=p95_ms,
        p99_ms=p99_ms,
        max_ms=max_ms,
        error_counts=error_counts,
    )


def _workload_key(path: Path) -> str:
    # hot_len3_k10.jsonl -> hot_len3_k10
    return path.stem

def _describe_workload(path: Path) -> Dict[str, Any]:
    stem = path.stem
    # Expected: <kind>_len<LEN>_k<K>
    kind = ""
    prefix_len = None
    top_k = None
    parts = stem.split("_")
    if len(parts) >= 3:
        kind = parts[0]
        if parts[1].startswith("len"):
            try:
                prefix_len = int(parts[1][3:])
            except Exception:
                prefix_len = None
        if parts[2].startswith("k"):
            try:
                top_k = int(parts[2][1:])
            except Exception:
                top_k = None

    desc_bits: List[str] = []
    if kind:
        desc_bits.append(kind)
    if prefix_len is not None:
        desc_bits.append(f"prefix_len={prefix_len}")
    if top_k is not None:
        desc_bits.append(f"top_k={top_k}")
    desc = " ".join(desc_bits) if desc_bits else stem

    return {
        "workload_key": stem,
        "workload_file": path.name,
        "workload_desc": desc,
        "kind": kind or None,
        "prefix_len": prefix_len,
        "top_k": top_k,
    }


def _stats_to_workload_record(st: RunStats, wp: Path, concurrency: Optional[int] = None) -> Dict[str, Any]:
    qps_total = (st.requests_total / st.elapsed_s) if st.elapsed_s > 0 else 0.0
    qps_ok = (st.requests_ok / st.elapsed_s) if st.elapsed_s > 0 else 0.0
    wdesc = _describe_workload(wp)
    rec: Dict[str, Any] = {
        "qps_total": qps_total,
        "qps_ok": qps_ok,
        "requests_total": st.requests_total,
        "requests_ok": st.requests_ok,
        "errors": st.errors,
        "latency_avg_ms": st.avg_ms,
        "latency_p50_ms": st.p50_ms,
        "latency_p95_ms": st.p95_ms,
        "latency_p99_ms": st.p99_ms,
        "latency_max_ms": st.max_ms,
        "error_counts": st.error_counts,
        "workload_sha256": _sha256_file(wp),
        "workload_file": wdesc["workload_file"],
        "workload_desc": wdesc["workload_desc"],
        "kind": wdesc["kind"],
        "prefix_len": wdesc["prefix_len"],
        "top_k": wdesc["top_k"],
    }
    if concurrency is not None:
        rec["concurrency"] = concurrency
    return rec


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

        qps_field = "qps_ok" if ("qps_ok" in ra and "qps_ok" in rb) else "qps"
        row("QPS", qps_field, invert=False)
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
            "deployment": {
                "type": args.deployment,
                "nodes": args.nodes,
                "notes": args.deployment_notes,
            },
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
            qps_total = (st.requests_total / st.elapsed_s) if st.elapsed_s > 0 else 0.0
            qps_ok = (st.requests_ok / st.elapsed_s) if st.elapsed_s > 0 else 0.0
            wdesc = _describe_workload(wp)
            results["workloads"][key] = {
                "qps_total": qps_total,
                "qps_ok": qps_ok,
                "requests_total": st.requests_total,
                "requests_ok": st.requests_ok,
                "errors": st.errors,
                "latency_avg_ms": st.avg_ms,
                "latency_p50_ms": st.p50_ms,
                "latency_p95_ms": st.p95_ms,
                "latency_p99_ms": st.p99_ms,
                "latency_max_ms": st.max_ms,
                "error_counts": st.error_counts,
                "workload_sha256": _sha256_file(wp),
                "workload_file": wdesc["workload_file"],
                "workload_desc": wdesc["workload_desc"],
                "kind": wdesc["kind"],
                "prefix_len": wdesc["prefix_len"],
                "top_k": wdesc["top_k"],
            }
            print(
                f"{key}: qps_ok={qps_ok:.1f} p50={st.p50_ms:.3f}ms p95={st.p95_ms:.3f}ms p99={st.p99_ms:.3f}ms errors={st.errors}"
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
            "deployment": {
                "type": args.deployment,
                "nodes": args.nodes,
                "notes": args.deployment_notes,
            },
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
            qps_total = (st.requests_total / st.elapsed_s) if st.elapsed_s > 0 else 0.0
            qps_ok = (st.requests_ok / st.elapsed_s) if st.elapsed_s > 0 else 0.0
            wdesc = _describe_workload(wp)
            results["workloads"][key] = {
                "qps_total": qps_total,
                "qps_ok": qps_ok,
                "requests_total": st.requests_total,
                "requests_ok": st.requests_ok,
                "errors": st.errors,
                "latency_avg_ms": st.avg_ms,
                "latency_p50_ms": st.p50_ms,
                "latency_p95_ms": st.p95_ms,
                "latency_p99_ms": st.p99_ms,
                "latency_max_ms": st.max_ms,
                "error_counts": st.error_counts,
                "workload_sha256": _sha256_file(wp),
                "workload_file": wdesc["workload_file"],
                "workload_desc": wdesc["workload_desc"],
                "kind": wdesc["kind"],
                "prefix_len": wdesc["prefix_len"],
                "top_k": wdesc["top_k"],
                "concurrency": c,
            }
            print(
                f"{key}: qps_ok={qps_ok:.1f} p50={st.p50_ms:.3f}ms p95={st.p95_ms:.3f}ms p99={st.p99_ms:.3f}ms errors={st.errors}"
            )

    for c in concurrencies:
        asyncio.run(run_one(c))

    out_path = Path(args.output) if args.output else out_dir / f"{time.strftime('%Y%m%d_%H%M%S')}_{results['git_commit']}.json"
    out_path.write_text(json.dumps(results, indent=2) + "\n")
    print(str(out_path))


def cmd_run_es(args: argparse.Namespace) -> None:
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    workload_paths = [Path(p) for p in args.workloads]
    for p in workload_paths:
        if not p.exists():
            raise SystemExit(f"workload not found: {p}")

    endpoint = _normalize_http_endpoint(args.endpoint)
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    git_commit = _git(["git", "rev-parse", "--short", "HEAD"])
    git_branch = _git(["git", "rev-parse", "--abbrev-ref", "HEAD"])

    results: Dict[str, Any] = {
        "timestamp": timestamp,
        "git_commit": git_commit,
        "git_branch": git_branch,
        "config": {
            "endpoint": endpoint,
            "engine": "elasticsearch",
            "index": args.index,
            "query_mode": "prefix_keyword",
            "tenant_field": args.tenant_field,
            "word_field": args.word_field,
            "concurrency": args.concurrency,
            "warmup_s": args.warmup_s,
            "duration_s": args.duration_s,
            "deployment": {
                "type": args.deployment,
                "nodes": args.nodes,
                "notes": args.deployment_notes,
            },
        },
        "workloads": {},
    }

    async def run_all() -> None:
        for wp in workload_paths:
            key = _workload_key(wp)
            st = await run_es_workload(
                endpoint=endpoint,
                index=args.index,
                workload_path=wp,
                concurrency=args.concurrency,
                warmup_s=args.warmup_s,
                duration_s=args.duration_s,
                validate=args.validate,
                tenant_field=args.tenant_field,
                word_field=args.word_field,
                request_timeout_s=args.request_timeout_s,
            )
            results["workloads"][key] = _stats_to_workload_record(st, wp)
            print(
                f"{key}: qps_ok={results['workloads'][key]['qps_ok']:.1f} "
                f"p50={st.p50_ms:.3f}ms p95={st.p95_ms:.3f}ms p99={st.p99_ms:.3f}ms errors={st.errors}"
            )

    asyncio.run(run_all())

    out_path = Path(args.output) if args.output else out_dir / f"{time.strftime('%Y%m%d_%H%M%S')}_{git_commit}.json"
    out_path.write_text(json.dumps(results, indent=2) + "\n")
    print(str(out_path))


def cmd_matrix_es(args: argparse.Namespace) -> None:
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    workload_paths = [Path(p) for p in args.workloads]
    for p in workload_paths:
        if not p.exists():
            raise SystemExit(f"workload not found: {p}")

    endpoint = _normalize_http_endpoint(args.endpoint)
    concurrencies = [int(x) for x in args.concurrency.split(",") if x.strip()]

    results: Dict[str, Any] = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "git_commit": _git(["git", "rev-parse", "--short", "HEAD"]),
        "git_branch": _git(["git", "rev-parse", "--abbrev-ref", "HEAD"]),
        "config": {
            "endpoint": endpoint,
            "engine": "elasticsearch",
            "index": args.index,
            "query_mode": "prefix_keyword",
            "tenant_field": args.tenant_field,
            "word_field": args.word_field,
            "warmup_s": args.warmup_s,
            "duration_s": args.duration_s,
            "concurrencies": concurrencies,
            "validate": args.validate,
            "deployment": {
                "type": args.deployment,
                "nodes": args.nodes,
                "notes": args.deployment_notes,
            },
        },
        "workloads": {},
    }

    async def run_one(c: int) -> None:
        for wp in workload_paths:
            key = f"{_workload_key(wp)}_c{c}"
            st = await run_es_workload(
                endpoint=endpoint,
                index=args.index,
                workload_path=wp,
                concurrency=c,
                warmup_s=args.warmup_s,
                duration_s=args.duration_s,
                validate=args.validate,
                tenant_field=args.tenant_field,
                word_field=args.word_field,
                request_timeout_s=args.request_timeout_s,
            )
            results["workloads"][key] = _stats_to_workload_record(st, wp, concurrency=c)
            print(
                f"{key}: qps_ok={results['workloads'][key]['qps_ok']:.1f} "
                f"p50={st.p50_ms:.3f}ms p95={st.p95_ms:.3f}ms p99={st.p99_ms:.3f}ms errors={st.errors}"
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

    server_bin = Path(args.server_bin) if args.server_bin else (ROOT / "target" / "release" / "ftrie-server")
    if not args.skip_build and not args.server_bin:
        subprocess.check_call(["cargo", "build", "--release"], cwd=str(ROOT))

    if not server_bin.exists():
        raise SystemExit(f"server binary not found: {server_bin}")

    port = _pick_unused_port()
    bind_addr = f"127.0.0.1:{port}"
    endpoint = bind_addr

    data_dir = tempfile.mkdtemp(prefix="ftrie-bench-")
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
                "deployment": {
                    "type": args.deployment or "local_single_node",
                    "nodes": 1,
                    "notes": args.deployment_notes,
                    "server_bin": str(server_bin),
                    "data_dir": data_dir,
                },
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
                qps_total = (st.requests_total / st.elapsed_s) if st.elapsed_s > 0 else 0.0
                qps_ok = (st.requests_ok / st.elapsed_s) if st.elapsed_s > 0 else 0.0
                results["workloads"][key] = {
                    "qps_total": qps_total,
                    "qps_ok": qps_ok,
                    "requests_total": st.requests_total,
                    "requests_ok": st.requests_ok,
                    "errors": st.errors,
                    "latency_avg_ms": st.avg_ms,
                    "latency_p50_ms": st.p50_ms,
                    "latency_p95_ms": st.p95_ms,
                    "latency_p99_ms": st.p99_ms,
                    "latency_max_ms": st.max_ms,
                    "error_counts": st.error_counts,
                    "workload_sha256": _sha256_file(wp),
                    "workload_file": wp.name,
                    "workload_desc": _describe_workload(wp)["workload_desc"],
                    "concurrency": c,
                }
                print(
                    f"{key}: qps_ok={qps_ok:.1f} p50={st.p50_ms:.3f}ms p95={st.p95_ms:.3f}ms p99={st.p99_ms:.3f}ms errors={st.errors}"
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

    ep = sub.add_parser("es-prepare", help="create/populate an Elasticsearch index from a wordlist")
    ep.add_argument("--endpoint", default="http://127.0.0.1:9200")
    ep.add_argument("--index", default="prefix_words")
    ep.add_argument("--wordlist", default=str(ROOT / "words_alpha.txt"))
    ep.add_argument("--tenant", default="power")
    ep.add_argument("--batch-size", type=int, default=5000)
    ep.add_argument("--bulk-timeout-s", type=float, default=120.0)
    ep.add_argument("--shards", type=int, default=1)
    ep.add_argument("--replicas", type=int, default=0)
    ep.add_argument("--refresh-interval", default="1s")
    ep.add_argument("--recreate", action="store_true", help="delete/recreate index before ingest")
    ep.add_argument("--force-merge", action="store_true", help="force merge to 1 segment after ingest")
    ep.set_defaults(func=cmd_es_prepare)

    r = sub.add_parser("run", help="run end-to-end gRPC benchmarks")
    r.add_argument("--endpoint", default="127.0.0.1:50052", help="host:port (no scheme)")
    r.add_argument("--concurrency", type=int, default=32)
    r.add_argument("--warmup-s", type=float, default=20.0)
    r.add_argument("--duration-s", type=float, default=60.0)
    r.add_argument("--out-dir", default=str(Path(__file__).resolve().parent / "results"))
    r.add_argument("--output", default="")
    r.add_argument("--deployment", default="", help="e.g. single_node, three_nodes, k8s")
    r.add_argument("--nodes", type=int, default=0, help="number of nodes in deployment (if known)")
    r.add_argument("--deployment-notes", default="", help="free-form notes (instance type, flags, etc.)")
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
    m.add_argument("--deployment", default="", help="e.g. single_node, three_nodes, k8s")
    m.add_argument("--nodes", type=int, default=0, help="number of nodes in deployment (if known)")
    m.add_argument("--deployment-notes", default="", help="free-form notes (instance type, flags, etc.)")
    m.add_argument("--validate", action="store_true")
    m.add_argument("workloads", nargs="+")
    m.set_defaults(func=cmd_matrix)

    re = sub.add_parser("run-es", help="run workload(s) against Elasticsearch")
    re.add_argument("--endpoint", default="http://127.0.0.1:9200")
    re.add_argument("--index", default="prefix_words")
    re.add_argument("--tenant-field", default="tenant")
    re.add_argument("--word-field", default="word")
    re.add_argument("--request-timeout-s", type=float, default=10.0)
    re.add_argument("--concurrency", type=int, default=32)
    re.add_argument("--warmup-s", type=float, default=20.0)
    re.add_argument("--duration-s", type=float, default=60.0)
    re.add_argument("--out-dir", default=str(Path(__file__).resolve().parent / "results"))
    re.add_argument("--output", default="")
    re.add_argument("--deployment", default="single_node_es", help="e.g. single_node_es")
    re.add_argument("--nodes", type=int, default=1, help="number of nodes in deployment")
    re.add_argument("--deployment-notes", default="", help="free-form notes (heap, shard count, etc.)")
    re.add_argument("--validate", action="store_true")
    re.add_argument("workloads", nargs="+")
    re.set_defaults(func=cmd_run_es)

    me = sub.add_parser("matrix-es", help="run a workload matrix against Elasticsearch")
    me.add_argument("--endpoint", default="http://127.0.0.1:9200")
    me.add_argument("--index", default="prefix_words")
    me.add_argument("--tenant-field", default="tenant")
    me.add_argument("--word-field", default="word")
    me.add_argument("--request-timeout-s", type=float, default=10.0)
    me.add_argument("--concurrency", default="1,8,32,128")
    me.add_argument("--warmup-s", type=float, default=20.0)
    me.add_argument("--duration-s", type=float, default=60.0)
    me.add_argument("--out-dir", default=str(Path(__file__).resolve().parent / "results"))
    me.add_argument("--output", default="")
    me.add_argument("--deployment", default="single_node_es", help="e.g. single_node_es")
    me.add_argument("--nodes", type=int, default=1, help="number of nodes in deployment")
    me.add_argument("--deployment-notes", default="", help="free-form notes (heap, shard count, etc.)")
    me.add_argument("--validate", action="store_true")
    me.add_argument("workloads", nargs="+")
    me.set_defaults(func=cmd_matrix_es)

    s = sub.add_parser("suite", help="run a local server and a workload matrix")
    s.add_argument("--skip-build", action="store_true")
    s.add_argument("--server-bin", default="", help="path to ftrie-server (skips build)")
    s.add_argument("--disable-static-index", action="store_true")
    s.add_argument("--deployment", default="", help="override deployment type string")
    s.add_argument("--deployment-notes", default="", help="free-form notes (instance type, flags, etc.)")
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
