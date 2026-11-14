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
# The whole matrix on one host (fixture, every engine, FAISS, Lucene, every
# shape, gate.md from gates/replacement.json and gate-floor.md from
# gates/floor.json; LUCENE_16357_VERSION adds the #16357 Lucene build):
benchmarks/ann/run-matrix.sh challenge
INPUT=/work/court-corpus/embeddings-full.bin benchmarks/ann/run-matrix.sh court

# Or step by step:
cd benchmarks/ann
RUSTC_BOOTSTRAP=corenn_kernels cargo build --release --locked
B=target/release/protomolt-ann-bench
$B fixture --source=challenge --queries=500 --out=/tmp/fx
$B run --fixture=/tmp/fx --out=rust.json                    # single graph
$B run --fixture=/tmp/fx --segment-rows=120000 --out=seg.json   # sealed segments + tail
$B run --fixture=/tmp/fx --placement=skewed --shards=4 --engines=hnsw,enumerate \
  --out=sharded.json                                           # 4 shards, k = 100, 1,000, 10,000
python3 faiss/run.py --fixture=/tmp/fx --out=faiss.json
mvn -q -f lucene/pom.xml package && java --add-modules jdk.incubator.vector \
  -cp lucene/target/protomolt-ann-bench-lucene-0.1.0.jar:$(cat lucene/target/classpath.txt) \
  ai.pipestream.annbench.LuceneRunner --fixture=/tmp/fx --out=lucene.json   # same shape flags
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
reached. Truth is the exact FP32 top rows per query: 10,000 deep unfiltered
(`--k-truth-none`), 100 deep under each filter (`--k-truth`). A run refuses a
k deeper than the truth of a cell it measures.

Every fixture also writes the shard placements the sharded shape reads, for
each shard count in `--shard-counts` (default 4 and 16):

- **hash**: `splitmix64(row) mod shards`, the hash the bucket column takes
  mod 100 (the bucket column itself does not divide evenly over 16 shards).
  Every shard holds a uniform sample, so a query's true neighbors spread
  evenly over the shards.
- **skewed**: whole k-means cells per shard. Cells are taken in the order of
  a walk from cell 0 to each centroid's most similar unvisited centroid, so a
  shard holds a neighborhood of cells, and a shard is closed once it holds
  its share of the rows not yet placed. Shard sizes stay close, but most of
  a query's true neighbors live on few shards.

Files: `manifest.json`, `base.f32`, `queries.f32`, `parents.u64`, `bucket.u8`,
`cell.u16`, `filters/<name>.json`, `truth/<name>.u32`,
`placements/<hash|skewed>-<shards>.u16` (all little endian). The format is
`protomolt-ann-fixture-v2`; every runner refuses a v1 fixture, so rebuild it.

The challenge fixture has 100,000 rows. At k = 10,000 its truth is a tenth
of the corpus, and with 16 shards of about 6,250 rows each shard holds fewer
rows than k, so every shard returns all of its rows: the k = 10,000 cells
run there, but they say little about a graph. They are meaningful on the
court fixture (1M rows).

## Engines

| Engine | Runner | Filter path (inside the timed region) |
| --- | --- | --- |
| `enumerate` | ours | Exact FP32 scan over the allow bitset's set bits (#55's Enumerate regime). Unfiltered it is the flat scan. |
| `exhaustive-tq` | ours | Embedded TurboVec over every row, FP32 rerank of the top `ef` (the product's default path); `Vec<bool>` mask. |
| `hnsw` | ours | `src/hnsw/` (`docs/hnsw.md`), parallel build, beam = `ef`; bitmap mask, the provider's filter regimes (`--hnsw-regime=enumerate|filter_aware|plain` pins one; `--hnsw-enumerate-max-rows=N` sets the enumerate threshold a policy point would carry; `--hnsw-routing=speed|exact` chooses what the provider optimizes for, speed by default). The candidate (#70), and the gate's default `--candidate`. |
| `hnsw_rs` 0.3.5 | Rust | `FilterT` closure over the allow bitset. `DistL2` with `simdeez_f`. |
| `rust-cv-hnsw` 0.11 | Rust | No predicate filter in the library: filtered cells are skipped and reported as such. |
| `corenn-hnswlib` 0.10 | Rust | `Fn(&key)` closure. Vectors served from a borrowed-slice `VectorStore`, so it does not pay the default store's per-read `Arc`. |
| `usearch` 2.26 | Rust | `filtered_search` closure. |
| `faiss-hnsw` | `faiss/run.py` | `IndexHNSWFlat`, `IDSelectorBitmap`. |
| `lucene-hnsw` 10.6 | `lucene/` | `Lucene99HnswVectorsFormat`, `IntField.newSetQuery` as the KNN filter (Lucene's own filter path, including its exact fallback). Query cache off. |
| `lucene-hnsw-16357` | `lucene/` | The same runner packaged against a local build of apache/lucene PR #16357 (see below). Its shared floor is not driven yet, so it writes floor-off results only. |

Masks for the Rust engines and FAISS are built from column postings (value to
rows), which is what #55 makes the product do; the time is reported per point
as `predicate_ms_mean`. `exhaustive-tq` also pays the per-query `Vec<bool>`
its scan kernel masks with. `hnsw` takes the bitmap as it is
(`VectorSearchOptions::with_bits`); the shard still builds a byte mask per
query, so a filtered `hnsw` row describes the provider, and the product
reaches it once its masks are bitmaps (the product half of #55).

Not runnable yet, reserved in the gate output: the floor-on cells (our shared
floor is #72; the Lucene runner does not drive the shared-floor code of
apache/lucene PR #16357, which is the sandbox code for apache/lucene issue
#16958), the collapse and seeded-entry cells, and the tombstone cells.

### The Lucene #16357 build

PR #16357 is not released, so it is built from Kristian's fork branch
`sandbox/shared-floor-knn` and installed into the local Maven repository
under its own version, then selected when the runner is packaged:

```bash
git clone --branch sandbox/shared-floor-knn <fork of apache/lucene> lucene-16357
cd lucene-16357
./gradlew mavenToLocal          # Lucene's Gradle task that installs into ~/.m2
# Note the version it installed, e.g. ls ~/.m2/repository/org/apache/lucene/lucene-core/
cd .../benchmarks/ann
mvn -q -f lucene/pom.xml package -Dlucene.version=<that version> -Dlucene.engine=lucene-hnsw-16357
```

`run-matrix.sh` does the packaging when `LUCENE_16357_VERSION` names the
installed version. The jar records the version and the engine name, and the
runner refuses the stock 10.6.0 under any name but `lucene-hnsw`, and any
other version under `lucene-hnsw`. The runner wraps the default codec rather
than naming a codec class, so it should build against a main-branch Lucene;
that and the Gradle task name are not verified against the fork here.

## Shapes

| Shape | Flags (Rust and Lucene runners) | Result `shape` |
| --- | --- | --- |
| Single | none | `single` |
| Segmented | `--segment-rows=N` | `segmented:<sealed>x<N>+tail<rows>` |
| Sharded | `--placement=hash\|skewed --shards=N` (default 4) | `sharded:<N>:<placement>:floor-off` |

**The sharded shape** splits the rows over N shards by the fixture's
placement file and builds one index per shard (Lucene: one index directory
per shard, merged to one segment). A query asks every shard for its local
top k at the point's breadth, maps each hit to its global row and merges
the hits to the global top k by score descending, then global row ascending
(a full selection, so ties at the k-th score resolve by row). `hnsw`,
`enumerate` and `exhaustive-tq` report exact FP32 inner products; the other
engines and Lucene report a monotone map of their own distance, which ranks
the same way.

What it does not exercise:

- **No product serving path.** It drives the in-process engines, not the
  coordinator and nodes. The bench crate builds the product without its
  `net` feature, and the real path (`src/harness.rs` starts nodes and a
  coordinator over loopback gRPC) would add transport, protobuf, the
  coordinator's planning and the node's dense policy to every query, which
  the Lucene and FAISS baselines do not pay. The merge rule is the
  coordinator's (score, then row), but the coordinator's code is not run.
- **No parallel fan-out.** Shards are searched one after another within a
  query, so one-core QPS is the total work of a query and the latencies are
  sums over shards, not the latency of a parallel fan-out. The all-cores
  figure runs many such queries at once.
- **No shared floor.** Every shard searches to its own top k; nothing
  passes between shards. That is the floor-off baseline. There is no floor
  to turn on until #72, so no runner writes a `floor-on` result.
- **Unfiltered only.** Engines read the global allow mask at their own row
  offset, which a shard's rows do not follow; per-shard masks are not built,
  so the runners refuse filtered cells on this shape.
- **Engines.** Every in-process engine runs it except `rust-cv-hnsw`
  (hnsw 0.11 panics when a walk finds fewer rows than the k it was asked
  for, which a shard's local top 1,000 or 10,000 reaches). FAISS has no
  sharded shape.

Its defaults differ from the other shapes: k = 100, 1,000 and 10,000, and
breadths from 100 to 32,768 (rerank depths to 50,000), since a breadth below
k is skipped and the beam may exceed k. The results file adds `shards`:
the count, placement, floor (`off`), rows per shard, and `truth_top_share`
per k, the mean over queries of the largest share of the query's true top k
that one shard holds (near 1/N under hash placement, higher under skewed).

## Measurement

Every engine gets the same sweep: M in 16, 32, 48, 64; ef_construction in
100, 200, 400; query breadth 16 to 512 (rerank depth 10 to 5,000 for the
exhaustive path; the sharded shape's defaults above). Per point:

- **Recall** against the exact truth: mean and worst over the queries, at
  k = 10 and k = 100 (the sharded shape: 100, 1,000 and 10,000). A filter
  with fewer allowed rows than k is judged on the rows it has.
- **One core**: one query at a time on one thread, at least 200 queries and
  `--min-time-ms`; p50 and p99 latency come from these.
- **All cores**: one thread per core pulling queries for `--min-time-ms`
  (FAISS: batch search for unfiltered and shared masks, forked one-thread
  workers for per-query masks; Lucene: one searcher shared by all threads).
- Hops and exact scores per query where the engine reports them
  (`hnsw`, `enumerate`, `exhaustive-tq`); Lucene reports `visited_mean`,
  the vectors it compared per query.
- **Per part**, for a shape with more than one part (segments or shards):
  `per_part` holds, per query, the least, median and most working part,
  averaged over the queries: `hops` and `exact_scores` from the Rust
  runner (null where the engine reports none), `visited` from Lucene.
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
| `gates/replacement.json` | #54 | both fixtures, single and segmented court shape, k = 10 and 100. The collapse, seeded-entry and tombstone cells join it when the harness can measure them. |
| `gates/floor.json` | #54, with `replacement.json` | sharded, both fixtures: 4 and 16 shards, hash and skewed placement, k = 100, 1,000 (the lead cell, decided 2026-11-13) and 10,000, floor off and floor on. Floor-off cells require `lucene-hnsw` and `lucene-hnsw-16357`; floor-on cells require `lucene-hnsw-16357` with its shared floor (stock Lucene has none). The floor-on cells are INCOMPLETE until #72 lands and the Lucene runner drives the #16357 floor. |

A sharded block names its cells with `"shape": "sharded"` and three more
lists, `shards`, `placements` (`hash`, `skewed`) and `floor` (`off`, `on`);
the block stands for every combination, each the group
`sharded:<shards>:<placement>:floor-<off|on>` that a sharded result file
records as its shape. Other blocks may not carry those keys, and a shape
spelled `sharded:...` directly is refused. What "required" means does not
change: every listed cell must be measured for the candidate and every
baseline it names, or it is INCOMPLETE.

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
