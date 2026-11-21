#!/usr/bin/env python3
"""analyse.py <dir> <mode> <base> <variant...>: per-point medians over rounds.

metric one: qps_one_core; metric all: qps_all_cores. ratio = variant / base
(above 1 is faster). Also checks that hops, scored rows and recall match the
base exactly in mode one (one-thread builds give every variant one graph).
"""
import glob, json, os, re, statistics, sys
d, mode, base, *variants = sys.argv[1:]
metric = "qps_one_core" if mode == "one" else "qps_all_cores"
data = {}
for f in sorted(glob.glob(os.path.join(d, f"{mode}-*.json"))):
    m = re.fullmatch(rf"{mode}-(.+)-(none|filt)-(\d+)\.json", os.path.basename(f))
    v = m.group(1)
    for e in json.load(open(f))["engines"]:
        for b in e["builds"]:
            for c in b["cells"]:
                for p in c["points"]:
                    key = (c["filter"], c["k"], p["ef"])
                    data.setdefault(v, {}).setdefault(key, []).append(p)
med = statistics.median
order = {"none": 0, "uncorrelated-50": 1, "uncorrelated-10": 2}
keys = sorted(data[base], key=lambda k: (order.get(k[0], 9), k[1], k[2]))
names = [base] + variants
print(f"metric={metric} rounds={len(next(iter(data[base].values())))}")
print(f"{'filter':16}{'k':>5}{'ef':>6}{'recall':>8}{'hops':>8}{'scored':>9} | " + " | ".join(f"{n:>26}" for n in names))
ratios = {v: [] for v in variants}
identical = {v: True for v in variants}
for key in keys:
    bp = data[base][key]
    bm = med(p[metric] for p in bp)
    cells = []
    for n in names:
        ps = data.get(n, {}).get(key)
        if not ps:
            cells.append(f"{'-':>26}"); continue
        vals = [p[metric] for p in ps]
        m = med(vals)
        spread = (max(vals) - min(vals)) / m * 100
        cells.append(f"{m:9.0f} ±{spread:4.1f}% x{m / bm:5.3f}")
        if n != base:
            ratios[n].append(m / bm)
            for a, b in zip(ps, bp):
                if (a["hops_mean"], a["exact_scores_mean"], a["recall_mean"], a["bridges_mean"]) != \
                   (b["hops_mean"], b["exact_scores_mean"], b["recall_mean"], b["bridges_mean"]):
                    identical[n] = False
    p0 = bp[0]
    print(f"{key[0]:16}{key[1]:>5}{key[2]:>6}{p0['recall_mean']:8.3f}{p0['hops_mean']:8.1f}{p0['exact_scores_mean']:9.1f} | " + " | ".join(cells))
for v in variants:
    r = ratios[v]
    g = statistics.geometric_mean(r) if r else float('nan')
    print(f"{v}: geomean x{g:.4f}, min x{min(r):.3f}, max x{max(r):.3f}; hops/scored/recall identical to {base}: {identical[v]}")
