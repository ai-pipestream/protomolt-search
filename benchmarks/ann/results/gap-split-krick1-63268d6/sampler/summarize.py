import json, sys

rust, faiss = sys.argv[1], sys.argv[2]
engines = json.load(open(rust))["engines"] + json.load(open(faiss))["engines"]
filters = ["none", "uncorrelated-50", "near-10"]
order = ["faiss-hnsw", "hnsw", "usearch"]

def points(eng, flt, k):
    for b in eng["builds"]:
        m = b["params"]["m"]
        for c in b["cells"]:
            if c["filter"] == flt and c["k"] == k:
                for p in c["points"]:
                    yield m, p

print("Best one-core QPS point per engine (qps_one_core = 1 / mean single-thread latency;")
print("qpsAll at --threads=4). ns/scored = 1e9 / (qps1 * exact_scores_mean).")
print("vs faiss = hnsw (or usearch) qps1 / faiss qps1.")
for flt in filters:
    for k in (10, 100):
        for thr in (0.95, 0.99):
            print(f"\n== filter={flt} k={k} recall>={thr}")
            best = {}
            for eng in engines:
                cands = [(m, p) for m, p in points(eng, flt, k) if p["recall_mean"] >= thr]
                if cands:
                    best[eng["engine"]] = max(cands, key=lambda mp: mp[1]["qps_one_core"])
            fq = best.get("faiss-hnsw", (0, {"qps_one_core": 0}))[1]["qps_one_core"]
            for name in order:
                if name not in best:
                    print(f"  {name:11s} (no point reaches {thr})")
                    continue
                m, p = best[name]
                q1 = p["qps_one_core"]
                sc = p.get("exact_scores_mean") or 0
                ns = 1e9 / (q1 * sc) if sc else 0
                br = p.get("bridges_mean")
                brs = f" bridges={br:7.1f}" if name == "hnsw" and br is not None else ""
                rel = f" vs faiss={q1 / fq:5.2f}" if fq and name != "faiss-hnsw" else ""
                print(f"  {name:11s} M={m:2d} ef={p['ef']:4d} rec={p['recall_mean']:.3f} "
                      f"qps1={q1:8.0f} qpsAll={p['qps_all_cores']:8.0f} "
                      f"hops={p.get('hops_mean') or 0:7.1f} scored={sc:7.1f}{brs} "
                      f"ns/scored={ns:6.1f}{rel}")
