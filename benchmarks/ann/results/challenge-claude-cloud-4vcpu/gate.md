# ANN gate

Candidate: **leann-graph**. Filtered cells gate (candidate QPS at least the best baseline's). Unfiltered cells are a bound: within 20% of the best baseline (`--unfiltered-margin=0.2`, a placeholder until Kristian sets it). Every engine is taken at its best point that reaches the recall target (mean recall over the queries, against exact FP32 truth).

Hosts: claude-cloud-4vcpu (Intel(R) Xeon(R) Processor @ 2.10GHz, amd64, 4 threads); claude-cloud-4vcpu (Intel(R) Xeon(R) Processor @ 2.10GHz, x86_64, 4 threads)

Calibration (hnsw_rs, fashion-mnist, M=24 efc=400 ef=24 k=10): measured recall 0.9909 against the published 0.977 (tolerance 0.01): **FAIL**. Comparisons count only once this passes.

## Engines

| Engine | Library | Version | License | Filter |
| --- | --- | --- | --- | --- |
| corenn-hnswlib | hnswlib-rs (CoreNN), InnerProduct, borrowed-slice VectorStore | 0.10.0 | Apache-2.0 | Fn(&key) closure over the allow bitset |
| enumerate | ours: exact FP32 scan over the allow mask's set bits | 0.1.0 | ours | allow bitset, set bits iterated |
| exhaustive-tq | ours: embedded TurboVec (TurboQuant) scan + FP32 rerank of the top ef | 0.1.0 | ours | Vec<bool> mask (VectorSearchOptions::with_mask) |
| faiss-hnsw | FAISS IndexHNSWFlat, METRIC_INNER_PRODUCT (faiss-cpu wheel) | 1.15.1 | MIT | IDSelectorBitmap from the column postings |
| hnsw_rs | hnsw_rs (jean-pierreBoth), DistL2, simdeez_f | 0.3.5 | MIT OR Apache-2.0 | FilterT closure over the allow bitset |
| leann-graph | ours: src/leann_graph.rs (to be replaced by #54) | 0.1.0 | ours | Vec<bool> mask (VectorSearchOptions::with_mask) |
| lucene-hnsw | Lucene99HnswVectorsFormat (Lucene104Codec), DOT_PRODUCT, KnnFloatVectorQuery | 10.6.0 | Apache-2.0 | IntField.newSetQuery filter on the KNN query (Lucene's own filter path) |
| rust-cv-hnsw | hnsw (rust-cv), L2 as ordered u32 bits | 0.11.0 | MIT | none: the library has no predicate filter, filtered cells are skipped |
| usearch | usearch Rust bindings, MetricKind::IP, f32 | 2.26.6 | Apache-2.0 | filtered_search closure over the allow bitset |

## challenge-100000x64 | single

| Filter | allows | k | recall | cores | leann-graph QPS | best baseline | baseline QPS | ratio | enumerate QPS | verdict |
| --- | ---: | ---: | ---: | --- | ---: | --- | ---: | ---: | ---: | --- |
| none | 100% | 10 | 0.95 | 1 | 1270 | faiss-hnsw | 14929 | 0.09 | 565 | FAIL |
| none | 100% | 10 | 0.95 | all | 4288 | faiss-hnsw | 52294 | 0.08 | 2497 | FAIL |
| none | 100% | 10 | 0.99 | 1 | 905 | faiss-hnsw | 10072 | 0.09 | 565 | FAIL |
| none | 100% | 10 | 0.99 | all | 3869 | faiss-hnsw | 40973 | 0.09 | 2497 | FAIL |
| none | 100% | 100 | 0.95 | 1 | 924 | faiss-hnsw | 6418 | 0.14 | 655 | FAIL |
| none | 100% | 100 | 0.95 | all | 3426 | faiss-hnsw | 25441 | 0.13 | 2281 | FAIL |
| none | 100% | 100 | 0.99 | 1 | 825 | faiss-hnsw | 4735 | 0.17 | 655 | FAIL |
| none | 100% | 100 | 0.99 | all | 3352 | faiss-hnsw | 19039 | 0.18 | 2281 | FAIL |
| far-1 | 1.3% | 10 | 0.95 | 1 | 54 | lucene-hnsw | 3864 | 0.01 | 17650 | FAIL |
| far-1 | 1.3% | 10 | 0.95 | all | 210 | lucene-hnsw | 15474 | 0.01 | 52180 | FAIL |
| far-1 | 1.3% | 10 | 0.99 | 1 | 37 | lucene-hnsw | 3864 | 0.01 | 17650 | FAIL |
| far-1 | 1.3% | 10 | 0.99 | all | 159 | lucene-hnsw | 15474 | 0.01 | 52180 | FAIL |
| far-1 | 1.3% | 100 | 0.95 | 1 | 35 | lucene-hnsw | 3540 | 0.01 | 8562 | FAIL |
| far-1 | 1.3% | 100 | 0.95 | all | 150 | lucene-hnsw | 14431 | 0.01 | 35621 | FAIL |
| far-1 | 1.3% | 100 | 0.99 | 1 | 34 | lucene-hnsw | 3540 | 0.01 | 8562 | FAIL |
| far-1 | 1.3% | 100 | 0.99 | all | 132 | lucene-hnsw | 14431 | 0.01 | 35621 | FAIL |
| far-10 | 10.3% | 10 | 0.95 | 1 | 79 | exhaustive-tq | 562 | 0.14 | 2476 | FAIL |
| far-10 | 10.3% | 10 | 0.95 | all | 342 | exhaustive-tq | 2284 | 0.15 | 9303 | FAIL |
| far-10 | 10.3% | 10 | 0.99 | 1 | 54 | exhaustive-tq | 562 | 0.10 | 2476 | FAIL |
| far-10 | 10.3% | 10 | 0.99 | all | 214 | exhaustive-tq | 2284 | 0.09 | 9303 | FAIL |
| far-10 | 10.3% | 100 | 0.95 | 1 | 65 | exhaustive-tq | 527 | 0.12 | 2046 | FAIL |
| far-10 | 10.3% | 100 | 0.95 | all | 267 | exhaustive-tq | 1993 | 0.13 | 7819 | FAIL |
| far-10 | 10.3% | 100 | 0.99 | 1 | 48 | exhaustive-tq | 527 | 0.09 | 2046 | FAIL |
| far-10 | 10.3% | 100 | 0.99 | all | 203 | exhaustive-tq | 1993 | 0.10 | 7819 | FAIL |
| far-50 | 50.3% | 10 | 0.95 | 1 | 74 | usearch | 183 | 0.40 | 904 | FAIL |
| far-50 | 50.3% | 10 | 0.95 | all | 337 | usearch | 939 | 0.36 | 3002 | FAIL |
| far-50 | 50.3% | 10 | 0.99 | 1 | - | exhaustive-tq | 140 | - | 904 | FAIL (target not reached) |
| far-50 | 50.3% | 10 | 0.99 | all | - | exhaustive-tq | 521 | - | 3002 | FAIL (target not reached) |
| far-50 | 50.3% | 100 | 0.95 | 1 | - | exhaustive-tq | 131 | - | 845 | FAIL (target not reached) |
| far-50 | 50.3% | 100 | 0.95 | all | - | exhaustive-tq | 482 | - | 2934 | FAIL (target not reached) |
| far-50 | 50.3% | 100 | 0.99 | 1 | - | exhaustive-tq | 131 | - | 845 | FAIL (target not reached) |
| far-50 | 50.3% | 100 | 0.99 | all | - | exhaustive-tq | 482 | - | 2934 | FAIL (target not reached) |
| near-1 | 1.3% | 10 | 0.95 | 1 | 1265 | faiss-hnsw | 7873 | 0.16 | 17129 | FAIL |
| near-1 | 1.3% | 10 | 0.95 | all | 5311 | faiss-hnsw | 31424 | 0.17 | 69892 | FAIL |
| near-1 | 1.3% | 10 | 0.99 | 1 | 1227 | faiss-hnsw | 5729 | 0.21 | 17129 | FAIL |
| near-1 | 1.3% | 10 | 0.99 | all | 4378 | faiss-hnsw | 20179 | 0.22 | 69892 | FAIL |
| near-1 | 1.3% | 100 | 0.95 | 1 | 722 | faiss-hnsw | 3842 | 0.19 | 12113 | FAIL |
| near-1 | 1.3% | 100 | 0.95 | all | 2888 | lucene-hnsw | 15686 | 0.18 | 39977 | FAIL |
| near-1 | 1.3% | 100 | 0.99 | 1 | 722 | lucene-hnsw | 3820 | 0.19 | 12113 | FAIL |
| near-1 | 1.3% | 100 | 0.99 | all | 2888 | lucene-hnsw | 15686 | 0.18 | 39977 | FAIL |
| near-10 | 10.3% | 10 | 0.95 | 1 | 1299 | faiss-hnsw | 7184 | 0.18 | 2422 | FAIL |
| near-10 | 10.3% | 10 | 0.95 | all | 5548 | faiss-hnsw | 28616 | 0.19 | 9647 | FAIL |
| near-10 | 10.3% | 10 | 0.99 | 1 | 1134 | faiss-hnsw | 5867 | 0.19 | 2422 | FAIL |
| near-10 | 10.3% | 10 | 0.99 | all | 4283 | faiss-hnsw | 21617 | 0.20 | 9647 | FAIL |
| near-10 | 10.3% | 100 | 0.95 | 1 | 1181 | faiss-hnsw | 4333 | 0.27 | 2172 | FAIL |
| near-10 | 10.3% | 100 | 0.95 | all | 4236 | faiss-hnsw | 17450 | 0.24 | 7227 | FAIL |
| near-10 | 10.3% | 100 | 0.99 | 1 | 757 | faiss-hnsw | 3398 | 0.22 | 2172 | FAIL |
| near-10 | 10.3% | 100 | 0.99 | all | 3070 | faiss-hnsw | 13345 | 0.23 | 7227 | FAIL |
| near-50 | 50.3% | 10 | 0.95 | 1 | 1055 | faiss-hnsw | 4177 | 0.25 | 852 | FAIL |
| near-50 | 50.3% | 10 | 0.95 | all | 4304 | usearch | 15223 | 0.28 | 3029 | FAIL |
| near-50 | 50.3% | 10 | 0.99 | 1 | 788 | faiss-hnsw | 3536 | 0.22 | 852 | FAIL |
| near-50 | 50.3% | 10 | 0.99 | all | 3636 | faiss-hnsw | 12860 | 0.28 | 3029 | FAIL |
| near-50 | 50.3% | 100 | 0.95 | 1 | 978 | faiss-hnsw | 3073 | 0.32 | 747 | FAIL |
| near-50 | 50.3% | 100 | 0.95 | all | 3420 | faiss-hnsw | 11373 | 0.30 | 2757 | FAIL |
| near-50 | 50.3% | 100 | 0.99 | 1 | 729 | faiss-hnsw | 2613 | 0.28 | 747 | FAIL |
| near-50 | 50.3% | 100 | 0.99 | all | 2846 | faiss-hnsw | 9409 | 0.30 | 2757 | FAIL |
| uncorrelated-1 | 1.1% | 10 | 0.95 | 1 | 425 | lucene-hnsw | 5643 | 0.08 | 42750 | FAIL |
| uncorrelated-1 | 1.1% | 10 | 0.95 | all | 1784 | lucene-hnsw | 22346 | 0.08 | 145168 | FAIL |
| uncorrelated-1 | 1.1% | 10 | 0.99 | 1 | 425 | lucene-hnsw | 5643 | 0.08 | 42750 | FAIL |
| uncorrelated-1 | 1.1% | 10 | 0.99 | all | 1784 | lucene-hnsw | 22346 | 0.08 | 145168 | FAIL |
| uncorrelated-1 | 1.1% | 100 | 0.95 | 1 | 61 | lucene-hnsw | 5150 | 0.01 | 15232 | FAIL |
| uncorrelated-1 | 1.1% | 100 | 0.95 | all | 310 | lucene-hnsw | 18988 | 0.02 | 70111 | FAIL |
| uncorrelated-1 | 1.1% | 100 | 0.99 | 1 | 52 | lucene-hnsw | 5150 | 0.01 | 15232 | FAIL |
| uncorrelated-1 | 1.1% | 100 | 0.99 | all | 220 | lucene-hnsw | 18988 | 0.01 | 70111 | FAIL |
| uncorrelated-10 | 10.1% | 10 | 0.95 | 1 | 995 | faiss-hnsw | 9959 | 0.10 | 2228 | FAIL |
| uncorrelated-10 | 10.1% | 10 | 0.95 | all | 3533 | faiss-hnsw | 39959 | 0.09 | 8000 | FAIL |
| uncorrelated-10 | 10.1% | 10 | 0.99 | 1 | 994 | faiss-hnsw | 5411 | 0.18 | 2228 | FAIL |
| uncorrelated-10 | 10.1% | 10 | 0.99 | all | 3481 | faiss-hnsw | 21598 | 0.16 | 8000 | FAIL |
| uncorrelated-10 | 10.1% | 100 | 0.95 | 1 | 456 | faiss-hnsw | 3984 | 0.11 | 2458 | FAIL |
| uncorrelated-10 | 10.1% | 100 | 0.95 | all | 1799 | faiss-hnsw | 16063 | 0.11 | 7751 | FAIL |
| uncorrelated-10 | 10.1% | 100 | 0.99 | 1 | 456 | faiss-hnsw | 2951 | 0.15 | 2458 | FAIL |
| uncorrelated-10 | 10.1% | 100 | 0.99 | all | 1799 | faiss-hnsw | 11649 | 0.15 | 7751 | FAIL |
| uncorrelated-50 | 50.1% | 10 | 0.95 | 1 | 1089 | faiss-hnsw | 12793 | 0.09 | 627 | FAIL |
| uncorrelated-50 | 50.1% | 10 | 0.95 | all | 4461 | faiss-hnsw | 52456 | 0.09 | 2750 | FAIL |
| uncorrelated-50 | 50.1% | 10 | 0.99 | 1 | 726 | faiss-hnsw | 7055 | 0.10 | 627 | FAIL |
| uncorrelated-50 | 50.1% | 10 | 0.99 | all | 3153 | faiss-hnsw | 28466 | 0.11 | 2750 | FAIL |
| uncorrelated-50 | 50.1% | 100 | 0.95 | 1 | 823 | faiss-hnsw | 5939 | 0.14 | 666 | FAIL |
| uncorrelated-50 | 50.1% | 100 | 0.95 | all | 3263 | faiss-hnsw | 24389 | 0.13 | 2722 | FAIL |
| uncorrelated-50 | 50.1% | 100 | 0.99 | 1 | 693 | faiss-hnsw | 4397 | 0.16 | 666 | FAIL |
| uncorrelated-50 | 50.1% | 100 | 0.99 | all | 2640 | faiss-hnsw | 17060 | 0.15 | 2722 | FAIL |

### Crossover with the exact enumerated scan

Ratio is candidate QPS / enumerate QPS at each measured selectivity (one core); above 1 the graph wins. T1 is interpolated in log selectivity where the ratio crosses 1.

| Set | k | recall | ratio by allowed % | T1 |
| --- | ---: | ---: | --- | --- |
| uncorrelated | 10 | 0.95 | 1.1%: 0.01, 10.1%: 0.45, 50.1%: 1.74 | 20.1% |
| uncorrelated | 10 | 0.99 | 1.1%: 0.01, 10.1%: 0.45, 50.1%: 1.16 | 35.0% |
| uncorrelated | 100 | 0.95 | 1.1%: 0.00, 10.1%: 0.19, 50.1%: 1.23 | 35.0% |
| uncorrelated | 100 | 0.99 | 1.1%: 0.00, 10.1%: 0.19, 50.1%: 1.04 | 46.5% |
| near | 10 | 0.95 | 1.3%: 0.07, 10.3%: 0.54, 50.3%: 1.24 | 29.3% |
| near | 10 | 0.99 | 1.3%: 0.07, 10.3%: 0.47, 50.3%: 0.92 | enumerate wins at every measured selectivity (<= 50.3%) |
| near | 100 | 0.95 | 1.3%: 0.06, 10.3%: 0.54, 50.3%: 1.31 | 26.4% |
| near | 100 | 0.99 | 1.3%: 0.06, 10.3%: 0.35, 50.3%: 0.98 | enumerate wins at every measured selectivity (<= 50.3%) |
| far | 10 | 0.95 | 1.3%: 0.00, 10.3%: 0.03, 50.3%: 0.08 | enumerate wins at every measured selectivity (<= 50.3%) |
| far | 10 | 0.99 | 1.3%: 0.00, 10.3%: 0.02 | enumerate wins at every measured selectivity (<= 10.3%) |
| far | 100 | 0.95 | 1.3%: 0.00, 10.3%: 0.03 | enumerate wins at every measured selectivity (<= 10.3%) |
| far | 100 | 0.99 | 1.3%: 0.00, 10.3%: 0.02 | enumerate wins at every measured selectivity (<= 10.3%) |

### Build cost (best unfiltered k=10 recall 0.95 one-core point)

| Engine | params | ef | build s | retained bytes/row | disk bytes/row |
| --- | --- | ---: | ---: | ---: | ---: |
| corenn-hnswlib | {"bits":0,"ef_construction":400,"m":16} | 64 | 17.9 | 0.0 | 362.2 |
| enumerate | {"bits":0,"ef_construction":0,"m":0} | 10 | 0.0 | 0.7 | 256.0 |
| exhaustive-tq | {"bits":4,"ef_construction":0,"m":0} | 2000 | 0.2 | 326.3 | 37.0 |
| faiss-hnsw | {"bits":0,"ef_construction":200,"m":16} | 64 | 5.6 | 413.9 | 400.3 |
| hnsw_rs | {"bits":0,"ef_construction":200,"m":32} | 32 | 27.8 | 333.4 | - |
| leann-graph | {"bits":8,"ef_construction":400,"m":32} | 96 | 239.0 | 524.4 | 472.1 |
| lucene-hnsw | {"bits":0,"ef_construction":200,"m":16} | 48 | 16.1 | - | 333.2 |
| usearch | {"bits":0,"ef_construction":400,"m":16} | 64 | 14.5 | 452.4 | 404.6 |

## Not yet measurable

These #53 cells need provider features that land with #54/#55; the harness reserves them and reports them here until they run:

- Collaborative floor (K = 100 and 1,000, floor on/off, segmented and two shards) and the Lucene collaborative-floor row (apache/lucene#16958 sandbox build).
- Collapse cells (`collapse_parents`, D = 10 and 100, parent table on/off) on the court fixture.
- Seeded-entry cells (own top / local skeleton descent / coordinator-seeded entry).
- Tombstone cells (10% and 30% deleted rows).


**Overall: FAIL** (80 judged cells; calibration has not passed, so this verdict does not count yet).
