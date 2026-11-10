# Graph ANN benchmark harness and gate (#53)

The bar every graph provider must clear before it replaces the exhaustive
path. One fixture directory, three runners (Rust in-process,
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
  --matrix=gates/core-unfiltered.json --out=gate.md
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
| `hnsw` | ours | `src/hnsw/` (`docs/hnsw.md`), parallel build, beam = `ef`; bitmap mask, the provider's filter regimes (`--hnsw-regime=enumerate|filter_aware|plain` pins one; `--hnsw-enumerate-max-rows=N` sets the enumerate threshold a policy point would carry). The candidate (#70), and the gate's default `--candidate`. |
| `hnsw_rs` 0.3.5 | Rust | `FilterT` closure over the allow bitset. `DistL2` with `simdeez_f`. |
| `rust-cv-hnsw` 0.11 | Rust | No predicate filter in the library: filtered cells are skipped and reported as such. |
| `corenn-hnswlib` 0.10 | Rust | `Fn(&key)` closure. Vectors served from a borrowed-slice `VectorStore`, so it does not pay the default store's per-read `Arc`. |
| `usearch` 2.26 | Rust | `filtered_search` closure. |
| `faiss-hnsw` | `faiss/run.py` | `IndexHNSWFlat`, `IDSelectorBitmap`. |
| `lucene-hnsw` 10.6 | `lucene/` | `Lucene99HnswVectorsFormat`, `IntField.newSetQuery` as the KNN filter (Lucene's own filter path, including its exact fallback). Query cache off. |

Masks for the Rust engines and FAISS are built from column postings (value to
rows), which is what #55 makes the product do; the time is reported per point
as `predicate_ms_mean`. `exhaustive-tq` also pays the per-query `Vec<bool>`
its scan kernel masks with. `hnsw` takes the bitmap as it is
(`VectorSearchOptions::with_bits`); the shard still builds a byte mask per
query, so a filtered `hnsw` row describes the provider, and the product
reaches it once its masks are bitmaps (the product half of #55).

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
  (`hnsw`, `enumerate`, `exhaustive-tq`).
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
the gate) and the sweep moves on; the committed cloud run used 20 for the Rust baselines, below every candidate point it measured, so no verdict can depend on a skipped point.

The Rust harness compiles for the default target, as the product does; the
C++ and Java engines dispatch SIMD at run time. Record any `RUSTFLAGS` you
add.

## The gate (`ann_bench gate`)

The gate judges an explicit **matrix of expected cells**, a checked-in file
under `gates/` passed as `--matrix`. A matrix names the candidate, the
unfiltered margin and whether it is still provisional, and blocks of cells:
fixture, shape, filters, k, recall targets (0.95, 0.99), cores (`1`, `all`)
and the baselines every cell of the block requires.

| Matrix | For | Cells |
| --- | --- | --- |
| `gates/core-unfiltered.json` | #70, the core milestone | unfiltered, challenge fixture |
| `gates/filtered.json` | #55 | three filter sets at 50%, 10%, 1%, both fixtures |
| `gates/replacement.json` | #54 | everything the harness can measure today: both fixtures, single and segmented court shape. The floor, collapse, seeded-entry and tombstone cells join it with #60. |

For each expected cell every engine is taken at its best point: the highest
QPS of any build and breadth that reaches the recall target.

- **Filtered cells gate**: the candidate's QPS must be at least the best
  required baseline's. The exhaustive TurboVec row is a required baseline (it
  is the bar a graph must clear to enter the planner at all).
- **Unfiltered cells are a bound**: within the matrix's `unfiltered_margin`
  of the best required baseline. The flat scan (`enumerate`) is a required
  baseline here.
- **Enumerate** is a reported column in filtered cells, and the gate prints
  the crossover selectivity where the candidate overtakes it: T1 for #55.
- **Calibration**: hnsw_rs must reproduce its published recall at a
  published point within one recall point (`ann_bench calibrate`). The
  published point is fashion-mnist-784-euclidean, M = 24,
  ef_construction = 400, ef = 24, k = 10, recall 0.977. The dataset is
  rebuilt from the Fashion-MNIST IDX files, as ann-benchmarks builds it.

### Verdicts

A cell's verdict is one of:

| Cell verdict | Meaning |
| --- | --- |
| `PASS` | The candidate reaches the target and meets the bar against the best required baseline, and every required baseline was measured. |
| `FAIL` | The candidate was measured and misses the recall target, or its QPS misses the bar. A missing baseline cannot rescue a ratio that already fails. |
| `INCOMPLETE` | The candidate, or a required baseline, has no measurement for the cell. Never skipped. |
| `NO-BASELINE` | Every required baseline was measured and none reaches the target. Counts as incomplete unless the matrix block sets `no_baseline_expected`. |

The overall result is the only thing to quote as acceptance:

| Overall | When |
| --- | --- |
| `FAIL` | Any expected cell fails. |
| `INCOMPLETE` | No cell fails, but a cell is incomplete or without a baseline, a result file lacks provenance, or calibration is missing or has not passed. Not a verdict on the candidate. |
| `PROVISIONAL PASS` | Everything passes, but the matrix still marks the unfiltered margin provisional (#59). |
| `PASS` | Every expected cell passes, calibration passes, provenance is complete and the margin is decided. |

A row quoted from `gate.md` on its own is not acceptance; the overall line is.

### Provenance

Every result file records the host (name, CPU model, architecture, threads,
kernel), the commit (`ANN_BENCH_COMMIT`; the bench crate and the product
share one repository, so one commit names both), the start time, and the
fixture's `base_sha256`. The gate **refuses** to combine files that disagree
on the host or on the digest of a fixture name, and reports a file that
lacks any of these as incomplete evidence. Files from different commits are
combined and the difference is stated. The calibration is a recall
measurement and may come from another host.

The gate writes `gate.md` (tables) and `gate.json` (the overall result, the
reasons the evidence is incomplete, and every verdict with both points).

## Results

`results/` holds committed runs: one directory per fixture and host, with the
raw result files, the run logs and the gate table. Fixture directories and
Lucene index directories are not committed; regenerate them with the same
command (the fixture is deterministic and its `base_sha256` is in every
result file).
