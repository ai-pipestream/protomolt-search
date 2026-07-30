#!/usr/bin/env python3
"""Benchmark SVGs for turbovec-search, in the same visual idiom as
upstream turbovec's docs (900x460, white surface, slate ink, indigo
accent). Data is inlined from the recorded sweep JSONL / recall logs."""

STYLE = """  <style>
  .title { font: 700 20px -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; fill: #0f172a; }
  .subtitle { font: 400 12px -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; fill: #475569; }
  .label { font: 600 12px -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; fill: #0f172a; }
  .tick { font: 400 11px -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; fill: #64748b; }
  .value { font: 700 12px -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; fill: #0f172a; }
  .value-accent { font: 700 12px -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; fill: #4338ca; }
  .axis { font: 600 12px -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; fill: #334155; }
  .legend { font: 600 12px -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; fill: #0f172a; }
  .panel { font: 700 13px -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; fill: #0f172a; }
</style>
"""

INDIGO = "#4338ca"
AMBER = "#b45309"
GRID = "#e2e8f0"
AXISLINE = "#cbd5e1"


def svg_open(title, subtitle, w=900, h=460):
    parts = [
        f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" '
        f'viewBox="0 0 {w} {h}" role="img" aria-label="{title}">\n',
        STYLE,
        '  <rect width="100%" height="100%" fill="#ffffff" />\n',
        f'  <text x="84" y="32" class="title">{title}</text>\n',
    ]
    for i, line in enumerate(subtitle):
        parts.append(f'  <text x="84" y="{52 + i * 16}" class="subtitle">{line}</text>\n')
    return parts


def rounded_top_bar(x, y, w, h, fill, r=4):
    r = min(r, w / 2, h)
    return (
        f'  <path d="M{x},{y + h} L{x},{y + r} Q{x},{y} {x + r},{y} '
        f'L{x + w - r},{y} Q{x + w},{y} {x + w},{y + r} L{x + w},{y + h} Z" fill="{fill}" />\n'
    )


def fmt_ms(v):
    return f"{v / 1000:.1f} s" if v >= 1000 else f"{v:.0f} ms"


def chart_scaling():
    data = [(1, 6592.2), (2, 1942.9), (4, 625.9), (8, 313.4)]
    speedups = ["baseline", "3.4x", "10.5x", "21x"]
    p = svg_open(
        "Exact search latency vs shard count",
        [
            "86.6M chunks (dim 256, 4-bit TQ+), one machine, k=10, p50 of 40 single-client queries, floor sharing on.",
            "Same corpus at every width; the correctness gate proves results bitwise-identical across layouts.",
        ],
    )
    left, right, top, bottom = 84, 860, 96, 396
    vmax = 7000.0
    span = bottom - top
    # y grid at 0/2/4/6 s
    for g in [0, 2000, 4000, 6000]:
        y = bottom - span * g / vmax
        p.append(f'  <line x1="{left}" y1="{y:.1f}" x2="{right}" y2="{y:.1f}" stroke="{GRID}" stroke-width="1" />\n')
        p.append(f'  <text x="{left - 8}" y="{y + 4:.1f}" class="tick" text-anchor="end">{g // 1000} s</text>\n')
    p.append(f'  <line x1="{left}" y1="{bottom}" x2="{right}" y2="{bottom}" stroke="{AXISLINE}" stroke-width="1" />\n')
    n = len(data)
    slot = (right - left) / n
    bw = 96
    for i, (shards, ms) in enumerate(data):
        cx = left + slot * (i + 0.5)
        h = span * ms / vmax
        p.append(rounded_top_bar(cx - bw / 2, bottom - h, bw, h, INDIGO))
        p.append(f'  <text x="{cx:.1f}" y="{bottom - h - 26:.1f}" class="value" text-anchor="middle">{fmt_ms(ms)}</text>\n')
        cls = "value-accent" if i else "tick"
        p.append(f'  <text x="{cx:.1f}" y="{bottom - h - 10:.1f}" class="{cls}" text-anchor="middle">{speedups[i]}</text>\n')
        p.append(f'  <text x="{cx:.1f}" y="{bottom + 20}" class="label" text-anchor="middle">{shards} shard{"s" if shards > 1 else ""}</text>\n')
    p.append(f'  <text x="{(left + right) / 2}" y="{bottom + 44}" class="axis" text-anchor="middle">shards (processes), one machine</text>\n')
    p.append("</svg>\n")
    return "".join(p)


def chart_recall():
    depths = ["recall@10", "recall@100", "recall@1000"]
    raw = [0.830, 0.827, 0.838]
    rerank = [1.000, 1.000, None]  # pool = top-1000; @1000 not measurable within it
    p = svg_open(
        "Recall vs exact fp32 search - 86.6M vectors",
        [
            "Quantized (4-bit TQ+) top-k vs exact fp32 brute force over the same corpus, 20 probes.",
            "Rerank = exact fp32 rescoring of the quantized top-1,000 pool (~1 ms/query, seek reads into the vector file).",
        ],
    )
    left, right, top, bottom = 84, 860, 110, 396
    span = bottom - top
    for g in [0.0, 0.25, 0.5, 0.75, 1.0]:
        y = bottom - span * g
        p.append(f'  <line x1="{left}" y1="{y:.1f}" x2="{right}" y2="{y:.1f}" stroke="{GRID}" stroke-width="1" />\n')
        p.append(f'  <text x="{left - 8}" y="{y + 4:.1f}" class="tick" text-anchor="end">{g:.2f}</text>\n')
    p.append(f'  <line x1="{left}" y1="{bottom}" x2="{right}" y2="{bottom}" stroke="{AXISLINE}" stroke-width="1" />\n')
    n = len(depths)
    slot = (right - left) / n
    bw, gap = 88, 2
    for i, name in enumerate(depths):
        cx = left + slot * (i + 0.5)
        pairs = [(raw[i], AMBER, "quantized")]
        if rerank[i] is not None:
            pairs.append((rerank[i], INDIGO, "reranked"))
        total = len(pairs) * bw + (len(pairs) - 1) * gap
        x0 = cx - total / 2
        for j, (v, color, _) in enumerate(pairs):
            x = x0 + j * (bw + gap)
            h = span * v
            p.append(rounded_top_bar(x, bottom - h, bw, h, color))
            p.append(f'  <text x="{x + bw / 2:.1f}" y="{bottom - h - 8:.1f}" class="value" text-anchor="middle">{v:.3f}</text>\n')
        if rerank[i] is None:
            p.append(f'  <text x="{x0 + bw + gap + bw / 2:.1f}" y="{bottom - 12:.1f}" class="tick" text-anchor="middle">(pool depth)</text>\n')
        p.append(f'  <text x="{cx:.1f}" y="{bottom + 20}" class="label" text-anchor="middle">{name}</text>\n')
    lx = right - 240
    p.append(f'  <rect x="{lx}" y="88" width="12" height="12" rx="3" fill="{AMBER}" />\n')
    p.append(f'  <text x="{lx + 18}" y="99" class="legend">quantized scan</text>\n')
    p.append(f'  <rect x="{lx + 140}" y="88" width="12" height="12" rx="3" fill="{INDIGO}" />\n')
    p.append(f'  <text x="{lx + 158}" y="99" class="legend">+ fp32 rerank</text>\n')
    p.append("</svg>\n")
    return "".join(p)


def chart_pruning():
    ks = ["k=10", "k=100", "k=1,000", "k=10,000"]
    on = [209, 2542, 28631, 247442]
    off = [392, 4055, 41460, 646560]
    p = svg_open(
        "Collaborative floor sharing - candidates per query",
        [
            "Vector candidates entering top-k heaps per query, 8 shards over 86.6M chunks, 40 queries per point.",
            "Sharing on vs off return bitwise-identical results (correctness gate); each panel has its own scale.",
        ],
    )
    left, right, top, bottom = 84, 860, 128, 380
    n = len(ks)
    panel_w = (right - left) / n
    span = bottom - top
    bw, gap = 64, 2
    for i, name in enumerate(ks):
        px = left + panel_w * i
        cx = px + panel_w / 2
        vmax = off[i] * 1.15
        p.append(f'  <line x1="{px + 14}" y1="{bottom}" x2="{px + panel_w - 14}" y2="{bottom}" stroke="{AXISLINE}" stroke-width="1" />\n')
        for j, (v, color) in enumerate([(on[i], INDIGO), (off[i], AMBER)]):
            x = cx - bw - gap / 2 + j * (bw + gap)
            h = span * v / vmax
            p.append(rounded_top_bar(x, bottom - h, bw, h, color))
            label = f"{v / 1000:.0f}k" if v >= 10000 else f"{v:,}"
            p.append(f'  <text x="{x + bw / 2:.1f}" y="{bottom - h - 8:.1f}" class="value" text-anchor="middle">{label}</text>\n')
        cut = 1 - on[i] / off[i]
        p.append(f'  <text x="{cx:.1f}" y="{top - 6:.1f}" class="value-accent" text-anchor="middle">-{cut * 100:.0f}%</text>\n')
        p.append(f'  <text x="{cx:.1f}" y="{bottom + 20}" class="panel" text-anchor="middle">{name}</text>\n')
    lx = right - 250
    p.append(f'  <rect x="{lx}" y="84" width="12" height="12" rx="3" fill="{INDIGO}" />\n')
    p.append(f'  <text x="{lx + 18}" y="95" class="legend">sharing on</text>\n')
    p.append(f'  <rect x="{lx + 120}" y="84" width="12" height="12" rx="3" fill="{AMBER}" />\n')
    p.append(f'  <text x="{lx + 138}" y="95" class="legend">sharing off</text>\n')
    p.append("</svg>\n")
    return "".join(p)


def chart_concurrency():
    import math
    clients = [1, 2, 4, 8, 16, 32]
    series = [
        (8, "#2a78d6", [3.16, 3.29, 3.20, 3.16, 3.17, 3.24]),
        (4, "#eb6834", [1.59, 2.42, 2.58, 2.49, 2.46, 2.52]),
        (2, "#1baf7a", [0.52, 0.97, 1.66, 2.10, 2.22, 2.16]),
        (1, "#eda100", [0.15, 0.30, 0.57, 0.99, 1.30, 1.41]),
    ]
    p = svg_open(
        "Throughput vs concurrent clients",
        [
            "QPS at k=100 over 86.6M chunks, 64 queries per cell, one machine. Concurrency never reaches",
            "the 8-shard ceiling from a narrower layout: process parallelism beats query parallelism at equal load.",
        ],
    )
    left, right, top, bottom = 84, 700, 110, 396
    span = bottom - top
    vmax = 3.6
    for g in [0, 1, 2, 3]:
        y = bottom - span * g / vmax
        p.append(f'  <line x1="{left}" y1="{y:.1f}" x2="{right}" y2="{y:.1f}" stroke="{GRID}" stroke-width="1" />\n')
        p.append(f'  <text x="{left - 8}" y="{y + 4:.1f}" class="tick" text-anchor="end">{g}</text>\n')
    p.append(f'  <line x1="{left}" y1="{bottom}" x2="{right}" y2="{bottom}" stroke="{AXISLINE}" stroke-width="1" />\n')

    def xpos(c):
        return left + 40 + (right - left - 80) * math.log2(c) / 5

    for c in clients:
        p.append(f'  <text x="{xpos(c):.1f}" y="{bottom + 20}" class="tick" text-anchor="middle">{c}</text>\n')
    p.append(f'  <text x="{(left + right) / 2}" y="{bottom + 44}" class="axis" text-anchor="middle">concurrent clients (log scale)</text>\n')
    p.append(f'  <text x="30" y="{(top + bottom) / 2}" class="axis" transform="rotate(-90 30 {(top + bottom) / 2})" text-anchor="middle">queries per second</text>\n')

    for shards, color, qps in series:
        pts = " ".join(f"{xpos(c):.1f},{bottom - span * q / vmax:.1f}" for c, q in zip(clients, qps))
        p.append(f'  <polyline points="{pts}" fill="none" stroke="{color}" stroke-width="2" />\n')
        for c, q in zip(clients, qps):
            p.append(f'  <circle cx="{xpos(c):.1f}" cy="{bottom - span * q / vmax:.1f}" r="4" fill="{color}" stroke="#ffffff" stroke-width="2" />\n')
        ex, ey = xpos(32), bottom - span * qps[-1] / vmax
        p.append(f'  <text x="{ex + 14}" y="{ey + 4:.1f}" class="label">{shards} shard{"s" if shards > 1 else ""} - {qps[-1]:.1f} QPS</text>\n')
    p.append("</svg>\n")
    return "".join(p)


import os

out = "/work/worktrees/turbovec-workspace/turbovec-search/docs/benchmarks"
os.makedirs(out, exist_ok=True)
for name, svg in [
    ("scaling_ladder.svg", chart_scaling()),
    ("recall_rerank.svg", chart_recall()),
    ("floor_sharing_pruning.svg", chart_pruning()),
    ("concurrency_throughput.svg", chart_concurrency()),
]:
    with open(f"{out}/{name}", "w") as f:
        f.write(svg)
    print(f"wrote {out}/{name}")
