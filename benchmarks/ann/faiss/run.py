#!/usr/bin/env python3
"""FAISS HNSW runner for the #53 ANN gate.

Reads a fixture directory written by `ann_bench fixture`, sweeps
IndexHNSWFlat (inner product) over the same M, ef_construction and efSearch
grid as the Rust runner, and writes a results file in the same
`protomolt-ann-bench-results-v1` schema so `ann_bench gate` can merge it.

Timing:
- Latency percentiles always come from one query per call on one OpenMP
  thread.
- Throughput for unfiltered cells uses FAISS's batch search: one OpenMP
  thread for the one-core figure, all threads for the all-cores figure.
- Filtered cells, shared (uncorrelated) masks included, are timed one query
  per call with the mask built inside the timed region, as the Rust runner
  does: sequential for one core, and for all cores one forked worker process
  per core (the mask is numpy work under the GIL, so threads would measure
  the GIL, not FAISS). A shared mask is still rebuilt per query, so every
  filtered QPS is a measurement, never a modelled mask charge.
- The filter is FAISS's native IDSelectorBitmap, built from the column
  postings inside the timed region, as the Rust engines do.

Usage:
  python3 run.py --fixture=DIR --out=faiss.json [--m=16,32,48,64]
      [--ef-construction=100,200,400] [--ef=...] [--k=10,100]
      [--segment-rows=N] [--min-time-ms=300] [--filters=none,far-10]
"""

import argparse
import multiprocessing
import json
import os
import platform
import socket
import sys
import time

import numpy as np
import faiss

FORMAT = "protomolt-ann-bench-results-v1"


def ints(s):
    return sorted({int(x) for x in s.split(",") if x.strip()})


def load(dirpath):
    m = json.load(open(os.path.join(dirpath, "manifest.json")))
    if m["format"] != "protomolt-ann-fixture-v2":
        sys.exit(f"faiss: {dirpath} is a {m['format']} fixture, not protomolt-ann-fixture-v2; rebuild it")
    n, d, qn, kt, kt_none = m["rows"], m["dim"], m["queries"], m["k_truth"], m["k_truth_none"]
    base = np.fromfile(os.path.join(dirpath, "base.f32"), dtype="<f4").reshape(n, d)
    queries = np.fromfile(os.path.join(dirpath, "queries.f32"), dtype="<f4").reshape(qn, d)
    bucket = np.fromfile(os.path.join(dirpath, "bucket.u8"), dtype=np.uint8)
    cell = np.fromfile(os.path.join(dirpath, "cell.u16"), dtype="<u2")
    postings = {
        "bucket": [np.flatnonzero(bucket == v) for v in range(100)],
        "cell": [np.flatnonzero(cell == v) for v in range(m["cells"])],
    }
    filters = [("none", None, None, None)]
    truth = {"none": np.fromfile(os.path.join(dirpath, "truth/none.u32"), dtype="<u4").reshape(qn, kt_none)}
    for f in m["filters"]:
        body = json.load(open(os.path.join(dirpath, "filters", f["name"] + ".json")))
        shared = body.get("shared")
        filters.append((f["name"], body["column"], shared, body.get("per_query")))
        truth[f["name"]] = np.fromfile(
            os.path.join(dirpath, "truth", f["name"] + ".u32"), dtype="<u4"
        ).reshape(qn, kt)
    return m, base, queries, postings, filters, truth


def recall(got, truth_row, k):
    t = truth_row[:k]
    t = t[t != 0xFFFFFFFF]
    if len(t) == 0:
        return 1.0
    return len(np.intersect1d(got[:k], t)) / len(t)


def available_threads():
    if hasattr(os, "sched_getaffinity"):
        try:
            n = len(os.sched_getaffinity(0))
            if n > 0:
                return n
        except OSError:
            pass
    return os.cpu_count() or 1


def host():
    cpu = ""
    try:
        for line in open("/proc/cpuinfo"):
            if line.startswith("model name") or line.startswith("Model"):
                cpu = line.split(":", 1)[1].strip()
                break
    except OSError:
        pass
    return {
        "name": os.environ.get("ANN_BENCH_HOST", socket.gethostname()),
        "cpu": cpu,
        "arch": platform.machine(),
        "kernel": platform.release(),
        "threads": available_threads(),
        "cpu_features": [],
    }


class Shape:
    """Whole fixture, or sealed segments plus a tail, each its own index."""

    def __init__(self, base, ranges, m, efc, threads):
        self.ranges = ranges
        self.n = base.shape[0]
        self.indexes = []
        faiss.omp_set_num_threads(threads)
        for off, ln in ranges:
            idx = faiss.IndexHNSWFlat(base.shape[1], m, faiss.METRIC_INNER_PRODUCT)
            idx.hnsw.efConstruction = efc
            if len(ranges) == 1:
                idx.add(base[off : off + ln])
                self.indexes.append((off, idx, idx))
            else:
                wrapped = faiss.IndexIDMap(idx)
                wrapped.add_with_ids(base[off : off + ln], np.arange(off, off + ln, dtype=np.int64))
                self.indexes.append((off, wrapped, idx))

    def disk_bytes(self):
        return int(sum(faiss.serialize_index(w).nbytes for _, w, _ in self.indexes))

    def search(self, q, k, ef, bitmap):
        """q: (b, d). bitmap: packed global allow bits or None."""
        params = faiss.SearchParametersHNSW()
        params.efSearch = ef
        sel = None
        if bitmap is not None:
            sel = faiss.IDSelectorBitmap(self.n, faiss.swig_ptr(bitmap))
            params.sel = sel
        if len(self.indexes) == 1:
            _, w, _ = self.indexes[0]
            D, I = w.search(q, k, params=params)
            return D, I
        Ds, Is = [], []
        for off, w, _ in self.indexes:
            D, I = w.search(q, k, params=params)
            Ds.append(D)
            Is.append(I)
        D = np.concatenate(Ds, axis=1)
        I = np.concatenate(Is, axis=1)
        order = np.argsort(-D, axis=1, kind="stable")[:, :k]
        return np.take_along_axis(D, order, 1), np.take_along_axis(I, order, 1)


_STATE = None


def _worker(tid):
    shape, queries, bits_for, k, ef, min_time, threads, qn = _STATE
    local = 0
    q = tid
    start = time.perf_counter()
    while time.perf_counter() - start < min_time:
        shape.search(queries[q % qn : q % qn + 1], k, ef, bits_for(q % qn))
        local += 1
        q += threads
    return local, time.perf_counter() - start


def mask_bits(postings, column, values, n):
    allow = np.zeros(n, dtype=bool)
    for v in values:
        allow[postings[column][v]] = True
    return np.packbits(allow, bitorder="little")


def measure(shape, queries, postings, filt, truth, k, ef, min_time, threads):
    name, column, shared, per_query = filt
    qn = queries.shape[0]
    n = shape.n

    def bits_for(q):
        if column is None:
            return None
        vals = shared if shared is not None else per_query[q]
        return mask_bits(postings, column, vals, n)

    # Recall pass and per-query latency on one thread.
    faiss.omp_set_num_threads(1)
    recalls = []
    faiss.cvar.hnsw_stats.reset()
    for q in range(qn):
        _, I = shape.search(queries[q : q + 1], k, ef, bits_for(q))
        recalls.append(recall(I[0], truth[q], k))
    hstats = faiss.cvar.hnsw_stats
    hops_mean = hstats.nhops / qn
    scored_mean = hstats.ndis / qn
    lat = []
    pred = 0.0
    done = 0
    t0 = time.perf_counter()
    min_queries = min(qn, 200)
    while done < min_queries or time.perf_counter() - t0 < min_time:
        q = done % qn
        a = time.perf_counter()
        b = bits_for(q)
        p = time.perf_counter()
        shape.search(queries[q : q + 1], k, ef, b)
        e = time.perf_counter()
        lat.append((e - a) * 1e3)
        pred += p - a
        done += 1
    per_query_one_core = done / (time.perf_counter() - t0)

    if column is None:
        def run_batch(nthreads):
            faiss.omp_set_num_threads(nthreads)
            count = 0
            start = time.perf_counter()
            while count == 0 or time.perf_counter() - start < min_time:
                shape.search(queries, k, ef, None)
                count += qn
            return count / (time.perf_counter() - start)

        qps_one = run_batch(1)
        qps_all = run_batch(threads)
        timing = "batch"
    else:
        qps_one = per_query_one_core
        # Per-query masks are built in numpy under the GIL, so threads would
        # measure the GIL; forked worker processes share the built index
        # copy-on-write and each runs one-thread searches.
        faiss.omp_set_num_threads(1)
        global _STATE
        _STATE = (shape, queries, bits_for, k, ef, min_time, threads, qn)
        ctx = multiprocessing.get_context("fork")
        # Each worker times its own loop, so pool start-up is not charged.
        with ctx.Pool(threads) as pool:
            qps_all = sum(n / s for n, s in pool.map(_worker, range(threads)))
        timing = "per-query"
    lat.sort()
    pct = lambda f: lat[int(round((len(lat) - 1) * f))]
    return {
        "k": k,
        "ef": ef,
        "recall_mean": float(np.mean(recalls)),
        "recall_worst": float(np.min(recalls)),
        "qps_one_core": qps_one,
        "qps_all_cores": qps_all,
        "threads": threads,
        "p50_ms": pct(0.5),
        "p99_ms": pct(0.99),
        "predicate_ms_mean": pred * 1e3 / done,
        "hops_mean": hops_mean,
        "exact_scores_mean": scored_mean,
        "timing": timing,
    }


def rss():
    try:
        for line in open("/proc/self/status"):
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    except OSError:
        pass
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fixture", required=True)
    ap.add_argument("--out", default="faiss.json")
    ap.add_argument("--m", default="16,32,48,64")
    ap.add_argument("--ef-construction", default="100,200,400")
    ap.add_argument("--ef", default="16,32,48,64,96,128,192,256,384,512")
    ap.add_argument("--k", default="10,100")
    ap.add_argument("--segment-rows", type=int, default=0)
    ap.add_argument("--min-time-ms", type=int, default=300)
    ap.add_argument("--threads", type=int, default=available_threads())
    ap.add_argument("--recall-stop", type=float, default=0.99)
    ap.add_argument("--filters", default="")
    a = ap.parse_args()

    m, base, queries, postings, filters, truth = load(a.fixture)
    n = base.shape[0]
    if a.segment_rows and a.segment_rows < n:
        ranges = [(o, min(a.segment_rows, n - o)) for o in range(0, n, a.segment_rows)]
        sealed = sum(1 for r in ranges if r[1] == a.segment_rows)
        tail = sum(r[1] for r in ranges if r[1] != a.segment_rows)
        shape_name = f"segmented:{sealed}x{a.segment_rows}+tail{tail}"
    else:
        ranges = [(0, n)]
        shape_name = "single"
    only = [f for f in a.filters.split(",") if f]
    if only:
        filters = [f for f in filters if f[0] in only]
    # Recall at a k deeper than the truth cannot be judged; refuse it.
    for filt in filters:
        depth = truth[filt[0]].shape[1]
        for k in ints(a.k):
            if k > depth:
                sys.exit(f"faiss: k={k} exceeds the fixture's truth depth {depth} for {filt[0]}")
    report = {
        "format": FORMAT,
        "fixture": {
            "name": m["name"], "dir": a.fixture, "rows": n, "dim": m["dim"], "queries": m["queries"],
            "base_sha256": m["base_sha256"], "filters": m["filters"],
        },
        "shape": shape_name,
        "segments": [list(r) for r in ranges],
        "host": host(),
        "harness": {
            "runner": "faiss/run.py", "python": sys.version.split()[0], "numpy": np.__version__,
            "commit": os.environ.get("ANN_BENCH_COMMIT", ""), "started_at": int(time.time()),
        },
        "settings": {
            "k": ints(a.k), "ef": ints(a.ef), "m": ints(a.m), "ef_construction": ints(a.ef_construction),
            "min_time_ms": a.min_time_ms, "threads": a.threads, "recall_stop": a.recall_stop,
        },
        "engines": [],
    }
    engine = {
        "engine": "faiss-hnsw",
        "library": "FAISS IndexHNSWFlat, METRIC_INNER_PRODUCT (faiss-cpu wheel)",
        "version": faiss.__version__,
        "license": "MIT",
        "filter": "IDSelectorBitmap from the column postings",
        "builds": [],
    }
    report["engines"].append(engine)
    for mm in ints(a.m):
        for efc in ints(a.ef_construction):
            r0 = rss()
            t0 = time.perf_counter()
            shape = Shape(base, ranges, mm, efc, a.threads)
            build_s = time.perf_counter() - t0
            retained = max(rss() - r0, 0)
            disk = shape.disk_bytes()
            print(f"faiss: M={mm} efc={efc} built in {build_s:.1f}s", file=sys.stderr)
            cells = []
            for filt in filters:
                for k in ints(a.k):
                    points = []
                    for ef in [e for e in ints(a.ef) if e >= k]:
                        p = measure(shape, queries, postings, filt, truth[filt[0]], k, ef,
                                    a.min_time_ms / 1000.0, a.threads)
                        points.append(p)
                        if p["recall_mean"] >= a.recall_stop:
                            break
                        # Plateau: three more breadths added < 0.005 recall.
                        if len(points) >= 4 and p["recall_mean"] - points[-4]["recall_mean"] < 0.005:
                            break
                    cells.append({"filter": filt[0], "k": k, "points": points})
                    print(f"faiss: M={mm} efc={efc} {filt[0]} k={k}: {len(points)} points", file=sys.stderr)
            engine["builds"].append({
                "params": {"m": mm, "ef_construction": efc, "bits": 0},
                "build_s": build_s,
                "retained_rss_bytes": retained,
                "retained_bytes_per_row": retained / n,
                "disk_bytes": disk,
                "disk_bytes_per_row": disk / n,
                "cells": cells,
            })
            with open(a.out, "w") as f:
                json.dump(report, f, indent=2)
            del shape
    print(f"faiss: wrote {a.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
