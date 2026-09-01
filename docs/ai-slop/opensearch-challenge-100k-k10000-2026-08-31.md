# 100k-vector recall at k=10,000

> **AI-GENERATED EXPERIMENT REPORT. VERIFY BEFORE RELYING ON IT.** This is a
> deterministic synthetic same-host measurement. It is not a production-corpus
> quality claim.

## Result

Across 16 vector queries over 100,000 unit-normalized 64-dimensional vectors:

| Engine | Recall@10,000 | Linear-gain NDCG@10,000 | p50 first hit | p50 completion |
|---|---:|---:|---:|---:|
| Protomolt Search, exhaustive 4-bit TurboQuant | 0.89163125 | 0.97284590 | 1.809 ms | 10.893 ms |
| OpenSearch 3.8.0, Lucene HNSW | 1.00000000 | 1.00000000 | 46.832 ms | 49.899 ms |

The ground truth is exact FP32 inner-product top-10,000 computed before either
index is built. Protomolt recovered 142,661 of the 160,000 judged neighbors
across all queries, an average of 8,916.3 per query. Because TurboVec exhaustively
scans the quantized codes, this loss measures 4-bit score distortion rather
than ANN graph traversal recall. The high NDCG relative to recall means the
retained results still preserve most of the exact ranking gain.

Related vector workloads:

| Workload | Protomolt recall | Protomolt NDCG | OpenSearch recall |
|---|---:|---:|---:|
| filtered vector | 0.878250 | 0.962685 | 1.000000 |
| parent-collapse vector | 0.895825 | 0.974117 | 1.000000 |
| hybrid | 0.891631 | 0.958828 | 1.000000 |

All 96 measured requests per engine completed successfully. The mixed-workload
cell ran at 106.54 requests per second for Protomolt and 20.43 for OpenSearch,
but one iteration per query is enough for deterministic recall, not a robust
latency-capacity conclusion.

## Reproduction

```bash
./deploy/opensearch-challenge/run.sh \
  --documents=100000 --dimensions=64 --topics=16 --k=10000 \
  --iterations=1 --warmup=0 --concurrency=1 --cpuset=0-7 \
  --out=/work/court-corpus/bench/opensearch-challenge/2026-08-31-03cb444-100k-k10k
```

- Protomolt revision: `03cb444c0f9702afebdd56b3b0141c28262e0309`
- TurboVec revision: `65699eff623cefa0aeddbf7c67847372c106a3e2`
- OpenSearch image digest:
  `sha256:39a8f8c63028e8b5d6b70539af1d0339b15a6729002dd5b3f4a65f520376fd30`
- CPU allocation: CPUs 0 through 7 on an AMD Ryzen 9 9950X3D
- Report format: `protomolt-opensearch-challenge-report-v2`
- NDCG gain: linear

Artifact digests:

| Artifact | SHA-256 |
|---|---|
| `report.json` | `0a6a5f679fdfef3630c540c17a875797a4f8ac30b83a662896d3b47b7cd280fe` |
| `resources.json` | `7b7947da4bf6fe3dd3669657b2d34dc25b4be78bb97b58196a7eebf3250be6d0` |
| `manifest.json` | `a8df785947bdba77131ebaa7f8739a7e5c91c8f29c83680dac4e6ea25e1abfa4` |

An initial run at revision `09d046c` produced the same recall but exposed
overflow in exponential NDCG when ordinal gains reached 10,000. That report is
invalid for NDCG and is superseded by this run. Revision `03cb444` uses and
records finite linear graded gain, with a regression test at depth 10,000.

## Interpretation

This materially improves the quality picture from the earlier k=10 fixture,
where 4-bit recall was 0.544. A wider result set absorbs much of the quantized
rank displacement, but 0.892 still leaves a 10.8-point recall gap against the
exact FP32 target. The next useful experiment is candidate expansion: retrieve
more than 10,000 quantized candidates, exact-rerank them from retained FP32 or
residual data, and measure the smallest expansion factor that closes the gap
without surrendering the observed streaming latency.

That experiment is now recorded in
[`turboquant-exact-rerank-expansion-100k-k10000-2026-08-31.md`](turboquant-exact-rerank-expansion-100k-k10000-2026-08-31.md).
