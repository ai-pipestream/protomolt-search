# Protomolt Search versus OpenSearch local baseline

> **AI-GENERATED EXPERIMENT REPORT. VERIFY BEFORE RELYING ON IT.** This is a
> small synthetic same-host baseline, not a production benchmark and not a
> claim that Protomolt Search is generally better than OpenSearch.

## Reproducibility

The run used committed Protomolt Search revision
`8500ab3feadc5d5813c9cdede012318734e298c6`, TurboVec revision
`65699eff623cefa0aeddbf7c67847372c106a3e2`, and the official OpenSearch 3.8.0
image pinned at digest
`sha256:39a8f8c63028e8b5d6b70539af1d0339b15a6729002dd5b3f4a65f520376fd30`.
Both engines ran sequentially on CPUs 0 through 7 of an AMD Ryzen 9 9950X3D.

```bash
./deploy/opensearch-challenge/run.sh \
  --documents=4096 --dimensions=64 --topics=16 \
  --iterations=10 --warmup=2 --concurrency=1,8 --cpuset=0-7 \
  --out=/work/court-corpus/bench/opensearch-challenge/2026-08-31-8500ab3-local
```

The fixture contains 96 queries, 16 each across lexical, vector, hybrid,
filtered lexical, filtered vector, and parent-collapse vector workloads. Each
engine completed 960 measured requests in each concurrency cell, for 3,840
successful terminal responses overall. Hybrid uses rank constant 60 and the
same explicit per-leg fusion depth of 100 on both engines.

Artifact digests:

| Artifact | SHA-256 |
|---|---|
| `report.json` | `cea865b65b67b55ab6133e44ef2b2b5ccd0eec22b1a3afe356c8fee00b341148` |
| `resources.json` | `bbde868cc1bbd7384e456b70a832ece948567d6811a18a416b41800487f375a1` |
| `manifest.json` | `1d22502b57c66315c7639b04279e2f18c8c2a993b24ee6b328582ce8d32af610` |

An earlier diagnostic run at revision `70a515a` is not reported because it
gave the engines different hybrid fusion depths. The mismatch was corrected
and committed before this run.

## Query results

These are concurrency-1 values. Latencies are end-to-end client observations.
Vector and collapse judgments come from exact FP32 inner-product rankings, not
from either engine.

| Workload | Engine | p50 ms | p99 ms | p50 first hit ms | Recall@10 | NDCG@10 |
|---|---|---:|---:|---:|---:|---:|
| lexical | Protomolt | 0.198 | 0.259 | 0.175 | 1.000 | 1.000 |
| lexical | OpenSearch | 0.685 | 1.010 | 0.672 | 1.000 | 1.000 |
| filtered lexical | Protomolt | 0.193 | 0.290 | 0.169 | 1.000 | 1.000 |
| filtered lexical | OpenSearch | 0.878 | 1.277 | 0.859 | 1.000 | 1.000 |
| vector | Protomolt | 0.350 | 0.409 | 0.341 | 0.544 | 0.618 |
| vector | OpenSearch | 0.749 | 1.115 | 0.732 | 1.000 | 1.000 |
| filtered vector | Protomolt | 0.261 | 0.313 | 0.252 | 0.669 | 0.695 |
| filtered vector | OpenSearch | 0.875 | 1.227 | 0.857 | 1.000 | 1.000 |
| collapse vector | Protomolt | 0.473 | 0.542 | 0.473 | 0.544 | 0.618 |
| collapse vector | OpenSearch | 0.811 | 1.118 | 0.792 | 1.000 | 1.000 |
| hybrid | Protomolt | 0.296 | 0.375 | 0.295 | 1.000 | 0.673 |
| hybrid | OpenSearch | 1.775 | 2.307 | 1.755 | 1.000 | 0.638 |

Aggregate throughput was 3,282 versus 1,000 requests per second at concurrency
1, and 14,138 versus 5,181 at concurrency 8, in Protomolt's favor. These ratios
are 3.28 and 2.73 respectively. The corpus is too small for those ratios to be
capacity-planning numbers.

## Lifecycle and footprint

| Metric | Protomolt | OpenSearch |
|---|---:|---:|
| ingest | 185.5 ms | 607.8 ms |
| cold ready | 8.3 ms | 7,307.8 ms |
| SIGKILL to complete recovery query | 114.2 ms | 7,518.8 ms |
| steady RSS | 32.1 MB, service plus analyzer | 2.689 GB |
| persisted bytes | 2.745 MB | 5.625 MB |

The lifecycle protocol waits for application health and, after a crash, a
complete query. It does not equate an open port with readiness. Protomolt's
reported memory includes both the search process and mock analysis service;
OpenSearch reports the container's Java process.

## Decision

This run validates the challenge harness and makes one product problem
unambiguous: 4-bit TurboQuant quality is the current blocker to a broad
OpenSearch superiority claim. OpenSearch's Lucene HNSW happened to reproduce
the exact top 10 throughout this tiny corpus, while Protomolt recalled 54.4%
of unfiltered exact-vector neighbors and 66.9% under the year filter. The
hybrid route recovered full relevance-set recall and slightly higher NDCG in
this fixture, but that does not erase the pure-vector loss.

The next evidence-driven work should be:

1. Add a vector quality ladder using exact FP32 judgments, including the
   current 4-bit path, any upstream TQ+ quality mode, and a candidate-expansion
   plus exact-rerank option if raw or residual vectors can be stored honestly.
2. Repeat the gate at realistic dimensionality and corpus scale with captured
   production queries. The 4,096-document fixture makes HNSW unusually easy
   and produces sub-millisecond timings where scheduler noise matters.
3. Tune provisional batch cadence against time to first hit at scale. Lexical
   streaming arrived before terminal completion here, but the gap was only
   about 23 microseconds at p50; vector and collapse first-hit times were nearly
   terminal at this size.

The retained OpenSearch documentation used to construct the opponent lives in
`docs/ai-slop/source-reference/opensearch`, with checksums in the source bundle.
