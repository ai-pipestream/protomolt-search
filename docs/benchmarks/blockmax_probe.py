#!/usr/bin/env python3
"""Feasibility probe for block-max bounds on the CourtListener corpus.

Answers one question before any kernel work: with 32-vector blocks, how
often would a per-block score upper bound fall at or below the top-k
floor, so the block's codes never have to be read?

Two bound families:
  box  -- per-dimension [min, max] over the block; bound = sum_d
          (q_d > 0 ? q_d * hi_d : q_d * lo_d).  Exact, LUT-evaluable.
  ball -- centroid + radius; bound = q . c + r (q is unit norm).

Two layouts:
  corpus order (as ingested: chunks of the same opinion are adjacent)
  rp-tree order (recursive median split on a random direction, down to
  blocks of 32) -- a cheap stand-in for a clustering reorder pass.
"""
import numpy as np
import sys, time

PATH = "/work/court-corpus/embeddings-full.bin"
STRIDE = 12 + 256 * 4
DIM = 256
BLOCK = 32
K = 10

N = int(sys.argv[1]) if len(sys.argv) > 1 else 1 << 20
NQ = 64


def load(offset_rec, count):
    raw = np.memmap(PATH, dtype=np.uint8, mode="r",
                    offset=offset_rec * STRIDE, shape=(count, STRIDE))
    v = raw[:, 12:].copy().view(np.float32).reshape(count, DIM)
    return v


def floor_for(q, v, k=K):
    """kth-best score of each query over v."""
    out = np.empty(len(q), dtype=np.float32)
    CH = 1 << 18
    tops = [np.full(k, -np.inf, dtype=np.float32) for _ in range(len(q))]
    for s in range(0, len(v), CH):
        sc = q @ v[s:s + CH].T
        for i in range(len(q)):
            merged = np.concatenate([tops[i], sc[i]])
            tops[i] = np.partition(merged, -k)[-k:]
    for i in range(len(q)):
        out[i] = np.sort(tops[i])[0]
    return out


def block_summaries(v):
    b = v.reshape(-1, BLOCK, DIM)
    lo = b.min(axis=1)
    hi = b.max(axis=1)
    c = b.mean(axis=1)
    r = np.linalg.norm(b - c[:, None, :], axis=2).max(axis=1)
    true_max = None
    return lo, hi, c, r, b


def report(name, v, q, floor):
    lo, hi, c, r, b = block_summaries(v)
    qp = np.maximum(q, 0.0)
    qn = np.minimum(q, 0.0)
    box = qp @ hi.T + qn @ lo.T          # (NQ, nblocks)
    ball = q @ c.T + r[None, :]
    # true per-block max, for tightness reporting
    tmax = (q @ v.T).reshape(len(q), -1, BLOCK).max(axis=2)

    f = floor[:, None]
    box_prune = (box <= f).mean()
    ball_prune = (ball <= f).mean()
    print(f"\n--- {name} ({v.shape[0]} vectors, {len(lo)} blocks) ---")
    # Coherence diagnostics: the ball bound's slack is ~ the block radius,
    # so "radius needed" = mean(floor) - mean(true block max).
    need = floor.mean() - tmax.mean()
    print(f"  block radius: mean {r.mean():.4f} p10 {np.percentile(r, 10):.4f}"
          f"   radius needed to prune the average block: {need:.4f}")
    print(f"  floor (k={K}) mean {floor.mean():.4f}   true block max: "
          f"mean {tmax.mean():.4f} p99 {np.percentile(tmax, 99):.4f}")
    print(f"  box  bound: mean {box.mean():.4f}  slack over true max "
          f"{np.mean(box - tmax):.4f}  PRUNED {box_prune*100:.2f}%")
    print(f"  ball bound: mean {ball.mean():.4f}  slack over true max "
          f"{np.mean(ball - tmax):.4f}  PRUNED {ball_prune*100:.2f}%")
    for p, lbl in ((box_prune, "box"), (ball_prune, "ball")):
        # bytes model: 128B codes/vector -> 4096B per block; box sidecar
        # 256B/block (two nibble corner rows), ball sidecar 132B/block.
        side = 256 if lbl == "box" else 132
        eff = 4096.0 / (side + (1 - p) * 4096.0)
        print(f"    {lbl}: bytes read x{eff:.2f} less (sidecar {side} B/block)")
    return box_prune, ball_prune


def rp_reorder(v, rng, leaf=BLOCK):
    """Recursive median split on a random direction, returning a permutation."""
    order = np.arange(len(v))
    stack = [(0, len(v))]
    while stack:
        s, e = stack.pop()
        if e - s <= leaf:
            continue
        idx = order[s:e]
        d = rng.standard_normal(DIM).astype(np.float32)
        d /= np.linalg.norm(d)
        proj = v[idx] @ d
        mid = (e - s) // 2
        part = np.argpartition(proj, mid)
        order[s:e] = idx[part]
        stack.append((s, s + mid))
        stack.append((s + mid, e))
    return order


t0 = time.time()
print(f"loading {N} vectors in corpus order ...", flush=True)
v = load(0, N)
q = load(40_000_000, NQ).copy()
q /= np.linalg.norm(q, axis=1, keepdims=True)
print(f"  loaded in {time.time()-t0:.1f}s", flush=True)

# Floor from a wide stride sample, closer to the real 86.6M-chunk floor
# than the local subset would give.
print("estimating the real top-k floor from a 4M-vector stride sample ...",
      flush=True)
samp = np.concatenate([load(i * 8_000_000, 500_000) for i in range(8)])
floor_wide = floor_for(q, samp)
del samp
floor_local = floor_for(q, v)
print(f"  floor: local-subset mean {floor_local.mean():.4f}, "
      f"wide-sample mean {floor_wide.mean():.4f}", flush=True)

report("corpus order, local floor", v, q, floor_local)
report("corpus order, wide floor", v, q, floor_wide)

print("\nreordering with an rp-tree ...", flush=True)
t0 = time.time()
rng = np.random.default_rng(0xB10C)
perm = rp_reorder(v, rng)
vr = v[perm]
print(f"  reordered in {time.time()-t0:.1f}s", flush=True)
report("rp-tree order, wide floor", vr, q, floor_wide)
