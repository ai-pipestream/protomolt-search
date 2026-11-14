# #107: the FAISS gap, split (same host)

One 4-vCPU cloud VM (Intel Xeon @ 2.10GHz, AVX-512), one run, PR #77 head
`683ee59` (see `revision`). Challenge fixture (100,000 x 64, 500 queries,
truth to k=1000), unfiltered, M in {16, 32}, ef_construction 200, ef swept
16..2048. Rust engines from `ann_bench run`, then FAISS 1.16.0 (`faiss-cpu`)
from `faiss/run.py`, sequentially on the same host. `faiss/run.py` now
records FAISS's own `hnsw_stats` (`nhops` = nodes expanded, `ndis` =
distances computed) into `hops_mean` / `exact_scores_mean`, the same
meaning as our `hops` / `scored`.

`summary.txt` is the best point per engine per cell (one core). Headline:

| cell | hnsw QPS | FAISS QPS | ratio | hops hnsw/FAISS | scored hnsw/FAISS | ns per scored row hnsw/FAISS |
|---|---:|---:|---:|---:|---:|---:|
| k=10, r>=0.95 | 9,296 | 12,919 | 0.72 | 75 / 73 | 1,627 / 1,446 | 66 / 54 |
| k=10, r>=0.99 | 6,435 | 8,166 | 0.79 | 107 / 136 | 2,147 / 2,307 | 72 / 53 |
| k=100, r>=0.95 | 3,821 | 5,859 | 0.65 | 139 / 136 | 2,572 / 2,307 | 102 / 74 |
| k=100, r>=0.99 | 3,642 | 4,030 | 0.90 | 203 / 200 | 3,246 / 2,938 | 85 / 84 |
| k=1000, r>=0.99 | 1,052 | 777 | 1.35 | 1,031 / 1,032 | 6,320 / 5,625 | 150 / 229 |

Reading: the graphs are equally good (hops at matched recall within a few
percent). The gap at k=10/100 is cost per hop: about 10% more rows scored
per hop and 20-35% more time per scored row. At k=1000 hnsw is already
ahead of FAISS. `profile-hnsw-k10-ef64.txt` shows where the time goes.

Caveats: a shared cloud VM (noisy), not krick; FAISS one-core QPS is its
batched search, ours per query; Lucene was not run here.
