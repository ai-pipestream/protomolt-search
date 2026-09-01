# TurboQuant exact-rerank scaling at one million vectors

> **AI-GENERATED EXPERIMENT REPORT. VERIFY BEFORE RELYING ON IT.** These are
> deterministic, same-host measurements from one synthetic fixture and one
> slice of the CourtListener embedding artifact. They are not universal recall
> thresholds or production latency claims.

## Result

The 100,000-vector candidate-expansion factors do not remain constant when the
same synthetic fixture grows to one million vectors and k=10,000 falls from
10% to 1% of the corpus.

| Recall target | 100k × 64, k=10k | Expansion | 1M × 64, k=10k | Expansion |
|---:|---:|---:|---:|---:|
| 95% | 13,967 | 1.3967x | 20,677 | 2.0677x |
| 99% | 19,486 | 1.9486x | 28,286 | 2.8286x |
| 99.9% | 26,194 | 2.6194x | 35,484 | 3.5484x |
| 100% | 35,777 | 3.5777x | 45,040 | 4.5040x |

At the old 35,777-candidate depth, the one-million-vector run retained mean
recall 0.9993375 and worst-query recall 0.9990 after FP32 reranking. It no
longer recovered every exact top-10,000 neighbor.

The complete one-million-vector synthetic k sweep was:

| k | Corpus fraction | Native mean recall | 95% every-query depth | 99% | 99.9% | 100% |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 0.001% | 0.306250 | 834 (83.40x) | 834 (83.40x) | 834 (83.40x) | 834 (83.40x) |
| 100 | 0.01% | 0.415625 | 1,500 (15.00x) | 3,699 (36.99x) | 5,503 (55.03x) | 5,503 (55.03x) |
| 1,000 | 0.1% | 0.540688 | 5,527 (5.527x) | 10,272 (10.272x) | 15,063 (15.063x) | 21,071 (21.071x) |
| 10,000 | 1% | 0.720931 | 20,677 (2.0677x) | 28,286 (2.8286x) | 35,484 (3.5484x) | 45,040 (4.5040x) |

For k=10, a 95% target rounds up to all ten neighbors, so every target in the
table is full recall. For k=100, 99.9% similarly rounds up to all 100.

## Production-dimension correction

The active CourtListener generation is **256 dimensions**, not 768:

- `/work/court-corpus/embeddings-full.bin` starts with `TVEMB001` followed by
  little-endian dimension 256;
- `/work/court-corpus/shards-v9/shard-0.tv.wal/gen-000000/manifest.toml`
  records `dim = 256`.

The production-scale run therefore used the first one million real rows from
that embedding file. Its FP32 sidecar is 1,024,000,080 bytes including the
80-byte sidecar header. Sixteen evenly spaced corpus rows served as queries.
That query choice is reproducible and useful for a storage/data-path test, but
it is not a substitute for held-out user query embeddings.

| k | Native mean recall | 95% every-query depth | 99% | 99.9% | 100% |
|---:|---:|---:|---:|---:|---:|
| 10 | 0.856250 | 24 (2.40x) | 24 (2.40x) | 24 (2.40x) | 24 (2.40x) |
| 100 | 0.893750 | 137 (1.37x) | 178 (1.78x) | 211 (2.11x) | 211 (2.11x) |
| 1,000 | 0.927750 | 1,137 (1.137x) | 1,385 (1.385x) | 1,609 (1.609x) | 1,802 (1.802x) |
| 10,000 | 0.942687 | 10,373 (1.0373x) | 12,078 (1.2078x) | 14,324 (1.4324x) | 17,265 (1.7265x) |

The hypothetical 768-dimensional arithmetic is still useful: one million
rows would occupy 3,072,000,080 bytes in this sidecar format, and 35,777 rows
contain 109,906,944 bytes (104.81 MiB) of FP32 payload. Those are calculated
sizes, not the production format measured here.

## Public rerank latency

The harness starts one loopback node and coordinator, enables the candidate
stream, and issues the public `Query` shape with
`DENSE_SCORE_MODE_FP32_RERANK`. For every timed depth, the returned ids must
equal an independently computed exact rerank of the same fixed TurboQuant
candidate pool. The dynamically measured 100%-recall depth must also equal the
global FP32 top-k. Latency uses query 0 only. Three warmups preceded seven timed
samples. These are warm-cache measurements on an AMD Ryzen 9 9950X3D, with the
process pinned to CPUs 0 through 6. The run was rejected and repeated whenever
unrelated host load rose during the timed cells.

| Corpus | Candidates | FP32 payload | Median rerank | Effective payload rate | Median public wall time |
|---|---:|---:|---:|---:|---:|
| 1M × 64 synthetic | 10,000 | 2.44 MiB | 2.11 ms | 1.21 GB/s | 11.59 ms |
| 1M × 64 synthetic | 35,777 | 8.73 MiB | 9.35 ms | 0.98 GB/s | 30.35 ms |
| 1M × 64 synthetic | 45,040 | 11.00 MiB | 12.02 ms | 0.96 GB/s | 36.84 ms |
| 1M × 256 CourtListener | 10,000 | 9.77 MiB | 4.23 ms | 2.42 GB/s | 18.16 ms |
| 1M × 256 CourtListener | 17,265 | 16.86 MiB | 7.59 ms | 2.33 GB/s | 24.65 ms |
| 1M × 256 CourtListener | 35,777 | 34.94 MiB | 15.05 ms | 2.43 GB/s | 39.64 ms |

`rerank_ms` includes candidate-id routing, mmap FP32 dot products, result
collection, and reorder inside the public adapter. The payload rate divides
logical vector bytes by that phase time; it is not a hardware DRAM bandwidth
measurement and does not account for page or cache-line amplification.

## Method

[`exact_rerank_scale.rs`](../../examples/exact_rerank_scale.rs) avoids the
JSONL expansion and whole-stream buffering of the cross-engine challenge:

1. Build or reuse one TurboQuant provider image and one checksummed mmap FP32
   sidecar.
2. Compute the exact FP32 top-10,000 for each query.
3. Consume a completion-certified provider candidate stream over the entire
   corpus and sort it under the product's score-descending, id-ascending order.
4. Record the provider rank of every exact top-k row. This determines the
   minimum prefix for each recall target without sampling candidate depths.
5. Exercise selected depths through the public FP32-rerank query and compare
   its complete returned order to an independent fixed-pool oracle.

The 100k × 64 control reproduced the earlier k=10,000 thresholds exactly,
including the 35,777 full-recall depth. Both million-row sidecars passed their
stored full-payload SHA-256 verification, both provider images loaded with the
declared shape, and every candidate stream ended complete.

## Product implication

There is no evidence for a single universal expansion factor. On the
controlled synthetic fixture, the full-recall factor at k=10,000 increased
from 3.5777x to 4.5040x as the corpus grew tenfold. The real 256-dimensional
slice was much easier for the same provider, but its queries were sampled
corpus rows.

Candidate depth should remain an explicit quality policy keyed by provider,
bit width, embedding model, corpus generation, k, and a measured recall target.
A 5x pool covers the measured one-million-vector synthetic k=10,000 fixture,
but this report does not justify making 5x a universal default. The next
quality gate should use held-out production query embeddings and judgments,
then test cold-cache latency and multiple shards separately.

## Reproduction and artifacts

```bash
# Apples-to-apples 100k control; public timings are unnecessary here.
taskset -c 0-6 cargo run --release --locked --example exact_rerank_scale -- \
  --out=/work/court-corpus/bench/exact-rerank-scale/control-100k-64 \
  --source=synthetic --vectors=100000 --dimensions=64 \
  --topics=16 --queries=16 --k=10,100,1000,10000 --public=false

# One million synthetic rows.
taskset -c 0-6 cargo run --release --locked --example exact_rerank_scale -- \
  --out=/work/court-corpus/bench/exact-rerank-scale/1m-64 \
  --source=synthetic --vectors=1000000 --dimensions=64 \
  --topics=16 --queries=16 --k=10,100,1000,10000 \
  --public-depths=10000,35777 --public-warmup=3 --public-iterations=7

# One million rows from the current production embedding artifact.
taskset -c 0-6 cargo run --release --locked --example exact_rerank_scale -- \
  --out=/work/court-corpus/bench/exact-rerank-scale/1m-production \
  --source=court --input=/work/court-corpus/embeddings-full.bin \
  --vectors=1000000 --dimensions=256 --queries=16 \
  --k=10,100,1000,10000 --public-depths=10000,35777 \
  --public-warmup=3 --public-iterations=7
```

Measured result directories:

- Benchmark code revision: `15f9b8ea9d3f5ab0a70837bb05f7b627157e64c2`
- `/work/court-corpus/bench/exact-rerank-scale/2026-09-01-15f9b8e-100k-64-control`
- `/work/court-corpus/bench/exact-rerank-scale/2026-09-01-15f9b8e-1m-64`
- `/work/court-corpus/bench/exact-rerank-scale/2026-09-01-15f9b8e-1m-production-256`

Final report digests:

| Report | SHA-256 |
|---|---|
| 100k × 64 control | `6d8f2525a5a92683f7dddc03101666ce1b245908c16c6e021a3c5a4b1a013f7f` |
| 1M × 64 | `634e10cc86475d58aad2f1a3758efdf00844da90c80308dcdf61b3dcde06549c` |
| 1M × production 256 | `7b6c04bbfcc56bfbde5d1ad341a37838d2ac3d446e86fed6892be333c779f676` |
