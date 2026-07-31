# Block-max BM25

**One common term used to cost more than the entire vector cluster.
Now the lexical leg skips what it can prove it doesn't need.**

On the 86.6M-chunk CourtListener cluster, a single-term BM25 query for
`court` walked 4.9 million postings and took 2.81 s — nine times the
313 ms the vector leg needs across all eight shards. With block-max,
the same work drops to single-digit milliseconds on a court-shaped
corpus, with **bit-identical results**.

Design doc with full measurements, format spec, and attribution:
[docs/block-max.md](docs/block-max.md).

## What it is

A block-max index stores, next to every fixed-size block of postings,
an upper bound on the score any entry in that block can produce. A
top-k search that already holds a floor (the current k-th best score)
tests the bound first: if the block cannot beat the floor, the block
is skipped without being decoded. Everything skipped was provably
non-competitive, so the result set is unchanged — the same contract
the vector leg's floor sharing already carries.

The technique is Lucene's (`Impacts`, WAND/MaxScore, Ding & Suel 2011).
It works for BM25 because a term's contribution is monotone in tf and
doc length, so a block's true maximum is attained by a real document —
no slack term. It does **not** work for dense vectors (measured: high-dimensional
bounds prune 0.2–0.5% of blocks, less than the sidecar they cost), so
the vector leg is deliberately untouched.

## Measured

1M-doc court-shaped corpus, medians, k=10 / k=1000. "v5" is the new
format with the exhaustive scorer; "pruned" adds block-max; "seeded"
adds an externally supplied floor (`min_score`, the realistic
coordinator re-query case).

| query (postings) | old v4 | v5 | pruned | seeded |
|---|---:|---:|---:|---:|
| high-df (450k) | 160 ms | 40 ms | 0.02 ms | 0.01 ms |
| mixed (1.84M) | 700 ms | 185 ms | 3.8 / 16 ms | 1.3 / 13 ms |
| mid (1.5M) | 617 ms | 169 ms | 2.9 / 8 ms | 2.5 / 6 ms |
| rare (5.2k) | 0.7 ms | 0.4 ms | 0.03 / 0.3 ms | 0.01 / 0.2 ms |

(old v4 and v5 columns are k=10 — they walk every posting regardless;
pruned/seeded cells are k=10 / k=1000.)

Allocations per query fell from ~2 per posting walked to ~40 total
(counting-allocator instrumented). The shape matters more than the
numbers: high-df terms fall hardest, rare terms barely move, and small
k prunes far more than large k — exactly what the theory predicts.
Reproduce with `cargo run --release --example bm25_bench`.

## What's in the box

- **`TVBM2505` format** — per-term fixed-stride doc run, separate
  occurrence run (offsets decoded only for the k survivors), and a
  two-level skip run: Pareto `(tf, dl)` frontiers per 128 postings,
  plus per 4096 for long lists. Parameter-agnostic: bounds are pairs,
  not scores, so any k1/b works. v3/v4 files keep loading forever.
- **MaxScore pruning** — range skips, 4096-posting level-1 leaps, and
  the essential / non-essential term partition in competitive windows
  (up to 440× fewer full evaluations).
- **Seeded lexical floor** — `min_score` on `Bm25SearchRequest` is
  forwarded to every shard; `kth_best` comes back so a client re-query
  seeds the whole fleet. The lexical twin of the vector side's
  `initial_threshold`.
- **A/B switch** — `--block-max=false` / `TURBOVEC_BLOCK_MAX` forces
  the exhaustive path on v5 files, so one cluster can race itself.
- **Fleet factorial** — `cluster_sweep --bm25-terms="court,state,appeal"`
  runs `{floor seeding} × {block-max}` with a hit-signature gate that
  fails the run if any cell disagrees.

## The exactness contract

Results are **bit-identical** to the exhaustive scorer — scores, order,
survivor offsets — proven, not assumed: a 30-round property gate over
random corpora/k/term counts/k1/b/floor seeds, tie torture at the
floor (`bound <= cutoff` with doc-ordered scan and strictly-greater
heap replacement), and IEEE-careful bound sums (addition is not
associative; every sum is accumulated in term-index order so the bound
provably dominates the true score). The exhaustive path stays in the
tree permanently as the oracle every optimization is gated against.

## Upgrading existing shards

Nothing breaks: v3/v4 files load and serve — but they are **legacy
formats, kept for migration only**. They serve on the unoptimized
exhaustive path, and the occurrence-split scorer does O(k·df)
survivor-offset lookups on them, so large-k `Bm25Search` against an
unmigrated v4 file is slower than it was before v5 (crossover around
k≈300). There is no backward-compatibility commitment to that path:
production shards should be migrated to v5. No pipeline rerun needed —
the full document history is in the shard's WAL, so a `reshard` replay
(merge or split) plus `InstallSnapshot` rebuilds it, and any shard that
receives new documents writes v5 on its next flush.
