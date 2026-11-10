# ANN gate: replacement

Candidate: **hnsw**. Filtered cells gate (candidate QPS at least the best required baseline's). Unfiltered cells are a bound: within 20% of the best required baseline (**PROVISIONAL**: the margin is not decided, #59). Every engine is taken at its best point that reaches the recall target (mean recall over the queries, against exact FP32 truth). Only the matrix's expected cells are judged, and each must be measured for the candidate and for every baseline the matrix requires.

## Provenance

| File | Runner | Host | CPU | Arch | Threads | Kernel | Commit | Started (unix s) | Fixture | base_sha256 |
| --- | --- | --- | --- | --- | ---: | --- | --- | ---: | --- | --- |
| results/challenge-claude-cloud-4vcpu/rust-single-ours.json | ann_bench run | claude-cloud-4vcpu | Intel(R) Xeon(R) Processor @ 2.10GHz | x86_64 | 4 | **missing** | 6968885a3ce9 | **missing** | challenge-100000x64 | ffe7206193b1 |
| results/challenge-claude-cloud-4vcpu/rust-single-baselines.json | ann_bench run | claude-cloud-4vcpu | Intel(R) Xeon(R) Processor @ 2.10GHz | x86_64 | 4 | **missing** | **missing** | **missing** | challenge-100000x64 | ffe7206193b1 |
| results/challenge-claude-cloud-4vcpu/faiss-single.json | faiss/run.py | claude-cloud-4vcpu | Intel(R) Xeon(R) Processor @ 2.10GHz | x86_64 | 4 | **missing** | **missing** | **missing** | challenge-100000x64 | ffe7206193b1 |
| results/challenge-claude-cloud-4vcpu/lucene-single.json | lucene/LuceneRunner | claude-cloud-4vcpu | Intel(R) Xeon(R) Processor @ 2.10GHz | x86_64 | 4 | **missing** | **missing** | **missing** | challenge-100000x64 | ffe7206193b1 |

Calibration (hnsw_rs, fashion-mnist, M=24 efc=400 ef=24 k=10): measured recall 0.9909 against the published 0.977 (tolerance 0.01): **FAIL**.

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

| Filter | allows | k | recall | cores | hnsw QPS | best baseline | baseline QPS | ratio | enumerate QPS | verdict |
| --- | ---: | ---: | ---: | --- | ---: | --- | ---: | ---: | ---: | --- |
| none | 100% | 10 | 0.95 | 1 | - | - | - | - | 565 | INCOMPLETE (candidate not measured) |
| none | 100% | 10 | 0.95 | all | - | - | - | - | 2497 | INCOMPLETE (candidate not measured) |
| none | 100% | 10 | 0.99 | 1 | - | - | - | - | 565 | INCOMPLETE (candidate not measured) |
| none | 100% | 10 | 0.99 | all | - | - | - | - | 2497 | INCOMPLETE (candidate not measured) |
| none | 100% | 100 | 0.95 | 1 | - | - | - | - | 655 | INCOMPLETE (candidate not measured) |
| none | 100% | 100 | 0.95 | all | - | - | - | - | 2281 | INCOMPLETE (candidate not measured) |
| none | 100% | 100 | 0.99 | 1 | - | - | - | - | 655 | INCOMPLETE (candidate not measured) |
| none | 100% | 100 | 0.99 | all | - | - | - | - | 2281 | INCOMPLETE (candidate not measured) |
| uncorrelated-50 | 50.1% | 10 | 0.95 | 1 | - | - | - | - | 627 | INCOMPLETE (candidate not measured) |
| uncorrelated-50 | 50.1% | 10 | 0.95 | all | - | - | - | - | 2750 | INCOMPLETE (candidate not measured) |
| uncorrelated-50 | 50.1% | 10 | 0.99 | 1 | - | - | - | - | 627 | INCOMPLETE (candidate not measured) |
| uncorrelated-50 | 50.1% | 10 | 0.99 | all | - | - | - | - | 2750 | INCOMPLETE (candidate not measured) |
| uncorrelated-50 | 50.1% | 100 | 0.95 | 1 | - | - | - | - | 666 | INCOMPLETE (candidate not measured) |
| uncorrelated-50 | 50.1% | 100 | 0.95 | all | - | - | - | - | 2722 | INCOMPLETE (candidate not measured) |
| uncorrelated-50 | 50.1% | 100 | 0.99 | 1 | - | - | - | - | 666 | INCOMPLETE (candidate not measured) |
| uncorrelated-50 | 50.1% | 100 | 0.99 | all | - | - | - | - | 2722 | INCOMPLETE (candidate not measured) |
| uncorrelated-10 | 10.1% | 10 | 0.95 | 1 | - | - | - | - | 2228 | INCOMPLETE (candidate not measured) |
| uncorrelated-10 | 10.1% | 10 | 0.95 | all | - | - | - | - | 8000 | INCOMPLETE (candidate not measured) |
| uncorrelated-10 | 10.1% | 10 | 0.99 | 1 | - | - | - | - | 2228 | INCOMPLETE (candidate not measured) |
| uncorrelated-10 | 10.1% | 10 | 0.99 | all | - | - | - | - | 8000 | INCOMPLETE (candidate not measured) |
| uncorrelated-10 | 10.1% | 100 | 0.95 | 1 | - | - | - | - | 2458 | INCOMPLETE (candidate not measured) |
| uncorrelated-10 | 10.1% | 100 | 0.95 | all | - | - | - | - | 7751 | INCOMPLETE (candidate not measured) |
| uncorrelated-10 | 10.1% | 100 | 0.99 | 1 | - | - | - | - | 2458 | INCOMPLETE (candidate not measured) |
| uncorrelated-10 | 10.1% | 100 | 0.99 | all | - | - | - | - | 7751 | INCOMPLETE (candidate not measured) |
| uncorrelated-1 | 1.1% | 10 | 0.95 | 1 | - | - | - | - | 42750 | INCOMPLETE (candidate not measured) |
| uncorrelated-1 | 1.1% | 10 | 0.95 | all | - | - | - | - | 145168 | INCOMPLETE (candidate not measured) |
| uncorrelated-1 | 1.1% | 10 | 0.99 | 1 | - | - | - | - | 42750 | INCOMPLETE (candidate not measured) |
| uncorrelated-1 | 1.1% | 10 | 0.99 | all | - | - | - | - | 145168 | INCOMPLETE (candidate not measured) |
| uncorrelated-1 | 1.1% | 100 | 0.95 | 1 | - | - | - | - | 15232 | INCOMPLETE (candidate not measured) |
| uncorrelated-1 | 1.1% | 100 | 0.95 | all | - | - | - | - | 70111 | INCOMPLETE (candidate not measured) |
| uncorrelated-1 | 1.1% | 100 | 0.99 | 1 | - | - | - | - | 15232 | INCOMPLETE (candidate not measured) |
| uncorrelated-1 | 1.1% | 100 | 0.99 | all | - | - | - | - | 70111 | INCOMPLETE (candidate not measured) |
| near-50 | 50.3% | 10 | 0.95 | 1 | - | - | - | - | 852 | INCOMPLETE (candidate not measured) |
| near-50 | 50.3% | 10 | 0.95 | all | - | - | - | - | 3029 | INCOMPLETE (candidate not measured) |
| near-50 | 50.3% | 10 | 0.99 | 1 | - | - | - | - | 852 | INCOMPLETE (candidate not measured) |
| near-50 | 50.3% | 10 | 0.99 | all | - | - | - | - | 3029 | INCOMPLETE (candidate not measured) |
| near-50 | 50.3% | 100 | 0.95 | 1 | - | - | - | - | 747 | INCOMPLETE (candidate not measured) |
| near-50 | 50.3% | 100 | 0.95 | all | - | - | - | - | 2757 | INCOMPLETE (candidate not measured) |
| near-50 | 50.3% | 100 | 0.99 | 1 | - | - | - | - | 747 | INCOMPLETE (candidate not measured) |
| near-50 | 50.3% | 100 | 0.99 | all | - | - | - | - | 2757 | INCOMPLETE (candidate not measured) |
| near-10 | 10.3% | 10 | 0.95 | 1 | - | - | - | - | 2422 | INCOMPLETE (candidate not measured) |
| near-10 | 10.3% | 10 | 0.95 | all | - | - | - | - | 9647 | INCOMPLETE (candidate not measured) |
| near-10 | 10.3% | 10 | 0.99 | 1 | - | - | - | - | 2422 | INCOMPLETE (candidate not measured) |
| near-10 | 10.3% | 10 | 0.99 | all | - | - | - | - | 9647 | INCOMPLETE (candidate not measured) |
| near-10 | 10.3% | 100 | 0.95 | 1 | - | - | - | - | 2172 | INCOMPLETE (candidate not measured) |
| near-10 | 10.3% | 100 | 0.95 | all | - | - | - | - | 7227 | INCOMPLETE (candidate not measured) |
| near-10 | 10.3% | 100 | 0.99 | 1 | - | - | - | - | 2172 | INCOMPLETE (candidate not measured) |
| near-10 | 10.3% | 100 | 0.99 | all | - | - | - | - | 7227 | INCOMPLETE (candidate not measured) |
| near-1 | 1.3% | 10 | 0.95 | 1 | - | - | - | - | 17129 | INCOMPLETE (candidate not measured) |
| near-1 | 1.3% | 10 | 0.95 | all | - | - | - | - | 69892 | INCOMPLETE (candidate not measured) |
| near-1 | 1.3% | 10 | 0.99 | 1 | - | - | - | - | 17129 | INCOMPLETE (candidate not measured) |
| near-1 | 1.3% | 10 | 0.99 | all | - | - | - | - | 69892 | INCOMPLETE (candidate not measured) |
| near-1 | 1.3% | 100 | 0.95 | 1 | - | - | - | - | 12113 | INCOMPLETE (candidate not measured) |
| near-1 | 1.3% | 100 | 0.95 | all | - | - | - | - | 39977 | INCOMPLETE (candidate not measured) |
| near-1 | 1.3% | 100 | 0.99 | 1 | - | - | - | - | 12113 | INCOMPLETE (candidate not measured) |
| near-1 | 1.3% | 100 | 0.99 | all | - | - | - | - | 39977 | INCOMPLETE (candidate not measured) |
| far-50 | 50.3% | 10 | 0.95 | 1 | - | - | - | - | 904 | INCOMPLETE (candidate not measured) |
| far-50 | 50.3% | 10 | 0.95 | all | - | - | - | - | 3002 | INCOMPLETE (candidate not measured) |
| far-50 | 50.3% | 10 | 0.99 | 1 | - | - | - | - | 904 | INCOMPLETE (candidate not measured) |
| far-50 | 50.3% | 10 | 0.99 | all | - | - | - | - | 3002 | INCOMPLETE (candidate not measured) |
| far-50 | 50.3% | 100 | 0.95 | 1 | - | - | - | - | 845 | INCOMPLETE (candidate not measured) |
| far-50 | 50.3% | 100 | 0.95 | all | - | - | - | - | 2934 | INCOMPLETE (candidate not measured) |
| far-50 | 50.3% | 100 | 0.99 | 1 | - | - | - | - | 845 | INCOMPLETE (candidate not measured) |
| far-50 | 50.3% | 100 | 0.99 | all | - | - | - | - | 2934 | INCOMPLETE (candidate not measured) |
| far-10 | 10.3% | 10 | 0.95 | 1 | - | - | - | - | 2476 | INCOMPLETE (candidate not measured) |
| far-10 | 10.3% | 10 | 0.95 | all | - | - | - | - | 9303 | INCOMPLETE (candidate not measured) |
| far-10 | 10.3% | 10 | 0.99 | 1 | - | - | - | - | 2476 | INCOMPLETE (candidate not measured) |
| far-10 | 10.3% | 10 | 0.99 | all | - | - | - | - | 9303 | INCOMPLETE (candidate not measured) |
| far-10 | 10.3% | 100 | 0.95 | 1 | - | - | - | - | 2046 | INCOMPLETE (candidate not measured) |
| far-10 | 10.3% | 100 | 0.95 | all | - | - | - | - | 7819 | INCOMPLETE (candidate not measured) |
| far-10 | 10.3% | 100 | 0.99 | 1 | - | - | - | - | 2046 | INCOMPLETE (candidate not measured) |
| far-10 | 10.3% | 100 | 0.99 | all | - | - | - | - | 7819 | INCOMPLETE (candidate not measured) |
| far-1 | 1.3% | 10 | 0.95 | 1 | - | - | - | - | 17650 | INCOMPLETE (candidate not measured) |
| far-1 | 1.3% | 10 | 0.95 | all | - | - | - | - | 52180 | INCOMPLETE (candidate not measured) |
| far-1 | 1.3% | 10 | 0.99 | 1 | - | - | - | - | 17650 | INCOMPLETE (candidate not measured) |
| far-1 | 1.3% | 10 | 0.99 | all | - | - | - | - | 52180 | INCOMPLETE (candidate not measured) |
| far-1 | 1.3% | 100 | 0.95 | 1 | - | - | - | - | 8562 | INCOMPLETE (candidate not measured) |
| far-1 | 1.3% | 100 | 0.95 | all | - | - | - | - | 35621 | INCOMPLETE (candidate not measured) |
| far-1 | 1.3% | 100 | 0.99 | 1 | - | - | - | - | 8562 | INCOMPLETE (candidate not measured) |
| far-1 | 1.3% | 100 | 0.99 | all | - | - | - | - | 35621 | INCOMPLETE (candidate not measured) |

### Crossover with the exact enumerated scan

Ratio is candidate QPS / enumerate QPS at each measured selectivity (one core); above 1 the graph wins. T1 is interpolated in log selectivity where the ratio crosses 1.

| Set | k | recall | ratio by allowed % | T1 |
| --- | ---: | ---: | --- | --- |

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

## court-1000000x1024 | single

No result file covers this fixture and shape.

| Filter | allows | k | recall | cores | hnsw QPS | best baseline | baseline QPS | ratio | enumerate QPS | verdict |
| --- | ---: | ---: | ---: | --- | ---: | --- | ---: | ---: | ---: | --- |
| none | 100% | 10 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| none | 100% | 10 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| none | 100% | 10 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| none | 100% | 10 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| none | 100% | 100 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| none | 100% | 100 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| none | 100% | 100 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| none | 100% | 100 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-50 | ? | 10 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-50 | ? | 10 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-50 | ? | 10 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-50 | ? | 10 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-50 | ? | 100 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-50 | ? | 100 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-50 | ? | 100 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-50 | ? | 100 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-10 | ? | 10 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-10 | ? | 10 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-10 | ? | 10 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-10 | ? | 10 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-10 | ? | 100 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-10 | ? | 100 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-10 | ? | 100 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-10 | ? | 100 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-1 | ? | 10 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-1 | ? | 10 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-1 | ? | 10 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-1 | ? | 10 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-1 | ? | 100 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-1 | ? | 100 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-1 | ? | 100 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-1 | ? | 100 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-50 | ? | 10 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-50 | ? | 10 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-50 | ? | 10 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-50 | ? | 10 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-50 | ? | 100 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-50 | ? | 100 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-50 | ? | 100 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-50 | ? | 100 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-10 | ? | 10 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-10 | ? | 10 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-10 | ? | 10 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-10 | ? | 10 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-10 | ? | 100 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-10 | ? | 100 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-10 | ? | 100 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-10 | ? | 100 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-1 | ? | 10 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-1 | ? | 10 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-1 | ? | 10 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-1 | ? | 10 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-1 | ? | 100 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-1 | ? | 100 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-1 | ? | 100 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-1 | ? | 100 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-50 | ? | 10 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-50 | ? | 10 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-50 | ? | 10 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-50 | ? | 10 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-50 | ? | 100 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-50 | ? | 100 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-50 | ? | 100 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-50 | ? | 100 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-10 | ? | 10 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-10 | ? | 10 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-10 | ? | 10 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-10 | ? | 10 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-10 | ? | 100 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-10 | ? | 100 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-10 | ? | 100 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-10 | ? | 100 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-1 | ? | 10 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-1 | ? | 10 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-1 | ? | 10 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-1 | ? | 10 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-1 | ? | 100 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-1 | ? | 100 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-1 | ? | 100 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-1 | ? | 100 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |

## court-1000000x1024 | segmented:8x120000+tail40000

No result file covers this fixture and shape.

| Filter | allows | k | recall | cores | hnsw QPS | best baseline | baseline QPS | ratio | enumerate QPS | verdict |
| --- | ---: | ---: | ---: | --- | ---: | --- | ---: | ---: | ---: | --- |
| none | 100% | 10 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| none | 100% | 10 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| none | 100% | 10 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| none | 100% | 10 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| none | 100% | 100 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| none | 100% | 100 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| none | 100% | 100 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| none | 100% | 100 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-50 | ? | 10 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-50 | ? | 10 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-50 | ? | 10 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-50 | ? | 10 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-50 | ? | 100 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-50 | ? | 100 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-50 | ? | 100 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-50 | ? | 100 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-10 | ? | 10 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-10 | ? | 10 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-10 | ? | 10 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-10 | ? | 10 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-10 | ? | 100 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-10 | ? | 100 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-10 | ? | 100 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-10 | ? | 100 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-1 | ? | 10 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-1 | ? | 10 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-1 | ? | 10 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-1 | ? | 10 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-1 | ? | 100 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-1 | ? | 100 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-1 | ? | 100 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| uncorrelated-1 | ? | 100 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-50 | ? | 10 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-50 | ? | 10 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-50 | ? | 10 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-50 | ? | 10 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-50 | ? | 100 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-50 | ? | 100 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-50 | ? | 100 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-50 | ? | 100 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-10 | ? | 10 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-10 | ? | 10 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-10 | ? | 10 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-10 | ? | 10 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-10 | ? | 100 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-10 | ? | 100 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-10 | ? | 100 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-10 | ? | 100 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-1 | ? | 10 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-1 | ? | 10 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-1 | ? | 10 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-1 | ? | 10 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-1 | ? | 100 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-1 | ? | 100 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-1 | ? | 100 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| near-1 | ? | 100 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-50 | ? | 10 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-50 | ? | 10 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-50 | ? | 10 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-50 | ? | 10 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-50 | ? | 100 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-50 | ? | 100 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-50 | ? | 100 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-50 | ? | 100 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-10 | ? | 10 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-10 | ? | 10 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-10 | ? | 10 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-10 | ? | 10 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-10 | ? | 100 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-10 | ? | 100 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-10 | ? | 100 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-10 | ? | 100 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-1 | ? | 10 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-1 | ? | 10 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-1 | ? | 10 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-1 | ? | 10 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-1 | ? | 100 | 0.95 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-1 | ? | 100 | 0.95 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-1 | ? | 100 | 0.99 | 1 | - | - | - | - | - | INCOMPLETE (candidate not measured) |
| far-1 | ? | 100 | 0.99 | all | - | - | - | - | - | INCOMPLETE (candidate not measured) |

## Not yet measurable

These #53 cells need provider features and harness cells that land with #60, #72, #73 and #76; no matrix lists them until they can run:

- Collaborative floor (K = 100, 1,000 and 10,000, floor on/off, segmented and sharded) and the Lucene collaborative-floor row (apache/lucene#16958 sandbox build).
- Collapse cells (`collapse_parents`, D = 10 and 100, parent table on/off) on the court fixture.
- Seeded-entry cells (own top / local skeleton descent / coordinator-seeded entry).
- Tombstone cells (10% and 30% deleted rows).


**Overall: INCOMPLETE** (240 expected cells: 0 pass, 0 fail, 240 incomplete, 0 without a baseline).

This is not a verdict on the candidate. The evidence is incomplete:

- results/challenge-claude-cloud-4vcpu/rust-single-ours.json does not record: kernel, run time
- results/challenge-claude-cloud-4vcpu/rust-single-baselines.json does not record: kernel, commit, run time
- results/challenge-claude-cloud-4vcpu/faiss-single.json does not record: kernel, commit, run time
- results/challenge-claude-cloud-4vcpu/lucene-single.json does not record: kernel, commit, run time
- calibration has not passed (FAIL)
- 240 expected cells are not fully measured
