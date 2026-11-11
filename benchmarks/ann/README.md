# Graph ANN benchmark harness and gate (#53)

The bar every graph provider must clear before it replaces the exhaustive
path or `leann-graph`. One fixture directory, three runners (Rust in-process,
FAISS in Python, Lucene in Java) that all read it and write the same results
schema, and a gate that merges the results and judges a candidate against
the best baseline in every cell.

This crate is dev tooling only. It has its own workspace and lockfile, so no
baseline engine enters the product's dependency graph or `Cargo.lock`.

## Quick start

```bash
# The whole matrix on one host (fixture, every engine, FAISS, Lucene, gate):
benchmarks/ann/run-matrix.sh challenge
INPUT=/work/court-corpus/embeddings-full.bin benchmarks/ann/run-matrix.sh court

# Or step by step:
cd benchmarks/ann
RUSTC_BOOTSTRAP=corenn_kernels cargo build --release --locked
B=target/release/protomolt-ann-bench
$B fixture --source=challenge --queries=500 --out=/tmp/fx
$B run --fixture=/tmp/fx --out=rust.json                    # single graph
$B run --fixture=/tmp/fx --segment-rows=120000 --out=seg.json   # sealed segments + tail
python3 faiss/run.py --fixture=/tmp/fx --out=faiss.json
mvn -q -f lucene/pom.xml package && java --add-modules jdk.incubator.vector \
  -cp lucene/target/protomolt-ann-bench-lucene-0.1.0.jar:$(cat lucene/target/classpath.txt) \
  ai.pipestream.annbench.LuceneRunner --fixture=/tmp/fx --out=lucene.json
./fetch-fashion-mnist.sh /tmp/fmnist && $B calibrate --dir=/tmp/fmnist --out=calibration.json
$B gate --results=rust.json,faiss.json,lucene.json --calibration=calibration.json \
  --candidate=leann-graph --unfiltered-margin=0.20 --out=gate.md
```

`RUSTC_BOOTSTRAP=corenn_kernels` is needed because CoreNN's kernel crate uses
`#![feature(f16)]`; the value enables unstable features for that one crate
only. FAISS needs `pip install faiss-cpu numpy`. Lucene needs JDK 21 and
Maven.

## Fixtures (`ann_bench fixture`)

| Source | Rows | Queries | Parent |
| --- | --- | --- | --- |
| `challenge` | The OpenSearch challenge fixture regenerated bit for bit: 100,000 unit vectors, 64 dimensions, 16 topics, seed `0xC0DE51A7` (`examples/opensearch_challenge.rs`). | The challenge's 16 vector workloads, then more drawn by the same generator and recipe, so they are held out. | `id / 3 + 1`, as the challenge sets it |
| `court` | The first `--vectors` rows of a CourtListener embeddings file (dimension from its header: 256 or 512 per `docs/plans/embedder-model-choice.md`). | The rows after the indexed prefix. | The chunk's opinion id |
| `synthetic:ROWS:DIM` | Topic-shaped unit vectors, for smoke runs. | Same generator. | `row / 4` |

Every fixture gets two filter columns and nine filter cells, written once so
every engine filters on exactly the same rows:

- `bucket` is `splitmix64(row) mod 100`, uncorrelated with the vectors.
- `cell` is the row's spherical k-means cell (200 cells, 400 from 500k rows).
- **uncorrelated-p**: `bucket in 0..p`, one mask for every query.
- **near-p**: per query, the cell holding most of the query's unfiltered
  top-10, then cells by centroid similarity to the query until p% of rows.
- **far-p**: per query, cells holding none of the query's unfiltered top-100,
  in a seeded random order, until p% of rows. The hard case.

for p in 50, 10 and 1. The manifest records the mean selectivity actually
reached. Truth is the exact FP32 top 100 under each filter, per query.

Files: `manifest.json`, `base.f32`, `queries.f32`, `parents.u64`, `bucket.u8`,
`cell.u16`, `filters/<name>.json`, `truth/<name>.u32` (all little endian).

## Engines

| Engine | Runner | Filter path (inside the timed region) |
| --- | --- | --- |
| `enumerate` | ours | Exact FP32 scan over the allow bitset's set bits (#55's Enumerate regime). Unfiltered it is the flat scan. |
| `exhaustive-tq` | ours | Embedded TurboVec over every row, FP32 rerank of the top `ef` (the product's default path); `Vec<bool>` mask. |
| `leann-graph` | ours | `src/leann_graph.rs`, uniform degree (`m = M`), beam = `ef`; `Vec<bool>` mask. The starting candidate until #54 replaces it. |
| `hnsw_rs` 0.3.5 | Rust | `FilterT` closure over the allow bitset. `DistL2` with `simdeez_f`. |
| `rust-cv-hnsw` 0.11 | Rust | No predicate filter in the library: filtered cells are skipped and reported as such. |
| `corenn-hnswlib` 0.10 | Rust | `Fn(&key)` closure. Vectors served from a borrowed-slice `VectorStore`, so it does not pay the default store's per-read `Arc`. |
| `usearch` 2.26 | Rust | `filtered_search` closure. |
| `faiss-hnsw` | `faiss/run.py` | `IndexHNSWFlat`, `IDSelectorBitmap`. |
| `lucene-hnsw` 10.6 | `lucene/` | `Lucene99HnswVectorsFormat`, `IntField.newSetQuery` as the KNN filter (Lucene's own filter path, including its exact fallback). Query cache off. |

Masks for the Rust engines and FAISS are built from column postings (value to
rows), which is what #55 makes the product do; the time is reported per point
as `predicate_ms_mean`. Engines on the product's `VectorIndex` seam also pay
the per-query `Vec<bool>` the product builds today.

Not runnable yet, reserved in the gate output: the Lucene collaborative-floor
row (apache/lucene#16958 sandbox build; `-Dlucene.version=` points the Lucene
runner at a locally installed build), the floor, collapse and seeded-entry
cells (need #54), and the tombstone cells.

## Measurement

Every engine gets the same sweep: M in 16, 32, 48, 64; ef_construction in
100, 200, 400; query breadth 16 to 512 (rerank depth 10 to 5,000 for the
exhaustive path). Per point:

- **Recall** against the exact truth: mean and worst over the queries, at
  k = 10 and k = 100. A filter with fewer allowed rows than k is judged on
  the rows it has.
- **One core**: one query at a time on one thread, at least 200 queries and
  `--min-time-ms`; p50 and p99 latency come from these.
- **All cores**: one thread per core pulling queries for `--min-time-ms`
  (FAISS: batch search for unfiltered and shared masks, forked one-thread
  workers for per-query masks; Lucene: one searcher shared by all threads).
- Hops and exact scores per query where the engine reports them
  (`leann-graph`, `enumerate`, `exhaustive-tq`).
- Build time, retained memory (RSS delta) and serialized bytes per row.
  Read the byte columns with care: `corenn-hnswlib` and `enumerate` read the
  fixture's shared rows, so their RSS delta excludes the vectors;
  `exhaustive-tq`'s image holds the codes only, not the FP32 copy its rerank
  reads (the product's `.exact` sidecar, 4 x dim bytes per row).

A breadth sweep stops once mean recall reaches `--recall-stop` (0.99, the
highest target): a larger breadth is only slower and the gate never picks
it. It also stops on a plateau, when three more breadths added less than
0.005 recall. When the recall pass over every query already takes longer
than `--min-time-ms` (slow filtered points), it is also the one-core timing
pass. With `--min-qps=N`, a point slower than N queries per second on one
core is abandoned after 50 queries (recorded as `below_qps_floor`, ignored by
the gate) and the sweep moves on; the committed cloud run used 20 for the Rust baselines, below every leann-graph point it measured, so no verdict can depend on a skipped point.

The Rust harness compiles for the default target, as the product does; the
C++ and Java engines dispatch SIMD at run time. Record any `RUSTFLAGS` you
add.

## The gate (`ann_bench gate`)

For every fixture and shape, every cell (filter x k), each recall target
(0.95, 0.99) and each mode (one core, all cores), every engine is taken at
its best point: the highest QPS of any build and breadth that reaches the
target.

- **Filtered cells gate**: the candidate's QPS must be at least the best
  baseline's. The exhaustive TurboVec row counts as a baseline (it is the bar
  a graph must clear to enter the planner at all).
- **Unfiltered cells are a bound**: within `--unfiltered-margin` of the best
  baseline (0.20 is a placeholder until Kristian sets it). The flat scan
  (`enumerate`) counts as a baseline here.
- **Enumerate** is a reported column in filtered cells, and the gate prints
  the crossover selectivity where the candidate overtakes it: T1 for #55.
- **Calibration**: hnsw_rs must reproduce its published recall at a
  published point within one recall point (`ann_bench calibrate`) before any
  comparison counts. The published point is fashion-mnist-784-euclidean,
  M = 24, ef_construction = 400, ef = 24, k = 10, recall 0.977. The dataset is
  rebuilt from the Fashion-MNIST IDX files, as ann-benchmarks builds it.

The gate writes `gate.md` (tables) and `gate.json` (every verdict with both
points).

## Results

`results/` holds committed runs: one directory per fixture and host, with the
raw result files, the run logs and the gate table. Fixture directories and
Lucene index directories are not committed; regenerate them with the same
command (the fixture is deterministic and its `base_sha256` is in every
result file).
