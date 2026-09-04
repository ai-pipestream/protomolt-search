# Dense quality profile

Landed 2026-09-04. A dense quality profile is the measured answer to one
question: how many quantized candidates must `DENSE_SCORE_MODE_FP32_RERANK`
select so that the exact top-`k` it returns reaches a recall target against
the exhaustive FP32 order. The file binds `(k, target) -> candidates` points
to the provider score space and corpus generation they were measured on,
carries the ladder they were drawn from, and names an optional default
target. The coordinator resolves it exactly: a point is served as measured,
an unmeasured `(k, target)` is refused, nothing is interpolated, and no
expansion factor is defaulted. `src/quality.rs` is the format,
`src/quality/measure.rs` the measurement, `examples/dense_profile.rs` the
tool, `tests/dense_quality.rs` the evidence.

## Why a measured depth

The 4-bit TurboQuant selection is exhaustive but quantized. On the OpenSearch
challenge fixture (100,000 unit vectors, 64 dimensions) its raw recall@10 was
0.544 and recall@10,000 was 0.892 against HNSW's 1.0
(`ai-slop/opensearch-challenge-100k-k10000-2026-08-31.md`). Selecting more
candidates and reranking them from the product-owned FP32 sidecar closes that
gap: at k = 10,000, 2x candidates recovered at least 99% on every query, 2.62x
at least 99.9%, and 3.5777x the exact top-10,000
(`ai-slop/turboquant-exact-rerank-expansion-100k-k10000-2026-08-31.md`).
Those are facts about that fixture. A different embedding model, corpus, or
`k` needs its own ladder, which is what the profile records.

## The file

`--dense-quality-profile=<path>` (or `dense_quality_profile` in the config
file, per collection or cluster-wide) installs one profile on a coordinator.
Strict TOML: unknown keys are refused; the fingerprint is the SHA-256 of the
file bytes and is what `DenseQualityPolicy.required_profile_fingerprint`
pins. `DenseQualityProfile::save` writes the document `load` reads, so a
saved copy has the fingerprint the tool printed.

```toml
format_version = 2
profile_id = "challenge-100k-k10000"
embedding_model = "synthetic-64"
corpus_generation = 3            # the coordinator's topology generation
corpus_rows = 100000
dimensions = 64
provider_backend = "embedded-turbovec"
scoring_fingerprint = "<GetVectorBackend descriptor fingerprint>"
measured_queries = 16
default_target_recall_ppm = 990000

[[measurements]]                 # one rung of the ladder
k = 10000
candidates = 20000
queries = 16
mean_recall_ppm = 995543
min_recall_ppm = 991700          # the worst query
p50_total_ms = 52.0
p50_selection_ms = 33.0
p50_rerank_ms = 17.5

[[measurements]]
k = 10000
candidates = 30000
queries = 16
mean_recall_ppm = 999962
min_recall_ppm = 999800
p50_total_ms = 63.0
p50_selection_ms = 36.0
p50_rerank_ms = 25.0

[[points]]                       # what a request can resolve
k = 10000
target_recall_ppm = 990000
candidates = 20000

[[points]]
k = 10000
target_recall_ppm = 999000
candidates = 30000
```

Recall is in parts per million so the common targets are exact integers on
the wire: 950000, 990000, 999000, 1000000. Latencies are the p50 of the
public `QueryProfile` phases in milliseconds (the lower median by rank, no
interpolation between samples).

Validation refuses by name: an unsupported `format_version`; an empty
identity string or one that needs escaping; zero rows, dimensions, or
measured queries; no points; a point with `k = 0`, a target outside
`1..=1000000`, a candidate depth below `k` or above the corpus, or a
duplicate `(k, target)`; a measurement with `k = 0`, a depth below `k` or
above the corpus, zero queries, a recall outside `0..=1000000` or a minimum
above the mean, a negative or non-finite latency, or a duplicate
`(k, candidates)`; a `default_target_recall_ppm` outside the range or naming
a target no point carries.

**Justification.** When `[[measurements]]` are present every point must be
one of the rungs — the same `k` and `candidates` — and that rung's
`min_recall_ppm` must be at or above the point's target. A point at a depth
the ladder never measured, or one promising 100% where the worst query
recovered 99.98%, is refused at load ("claims more than was measured"). That
is the reason the format carries the ladder: a number typed by hand can be
checked against the evidence it cites.

**Version 1.** A points-only file (`format_version = 1`) still loads
unchanged and serves explicit policies exactly as before. It carries no
`default_target_recall_ppm` and no measurements; a version 1 file naming
either is refused ("write format_version 2"). Nothing is upgraded on load:
`to_toml` always writes version 2, but only the tool or an operator writes.

## Choosing points

`quality::choose_points(measurements, targets)` is the rule, pure and
tested on a fixture ladder: for every measured `k` and every target, the
smallest measured depth whose **worst-query** recall meets the target. The
mean is recorded but never decides — a point promises every held-out query,
not the average one. On the challenge ladder the mean at 20,000 candidates
(0.99554) clears 99.5% but the worst query (0.9917) does not, so a 995000
target resolves to 30,000. A `(k, target)` no rung satisfies is reported as
unmet with the best worst-query recall reached and at what depth; it never
becomes a point.

## Measuring

```text
cargo run --release --example dense_profile -- \
    --coord=http://127.0.0.1:59291 [--collection=<name>] \
    --queries=<held-out vectors> [--dim=N] [--sample=N --seed=S] \
    --k=10,100 --depths=10,20,50,100,200,500,1000,2000 \
    --targets=950000,990000,999000,1000000 \
    --ground-truth=full-depth | brute:<rows file> \
    --embedding-model=<model> --profile-id=<id> [--default-target=990000] \
    --out=<profile.toml>
```

The example is the shell; `quality::measure::measure` does the work over a
`ProfileRoute`, which is the gRPC client in the tool and the coordinator
handler itself in the tests. Both are the public route.

- **Queries** are held out from the corpus: a raw little-endian f32 rows
  file with `--dim`, or the court embeddings `.bin` record format
  `examples/court_embed.rs` writes (detected by its header, dimension from
  it). `--sample=N --seed=S` takes N rows by a seeded partial Fisher-Yates
  (splitmix64), so the choice is reproducible.
- **Identity** comes from `ClusterHealth`: every primary must be reachable
  and populated, agree on `vector_backend`, `scoring_fingerprint`, and
  `dim`, own an aligned FP32 sidecar for every vector, and carry no
  tombstones; `rows` is the sum, `corpus_generation` the coordinator's
  `topology_generation` (a new field on `ClusterHealthResponse`, with
  `scoring_fingerprint` and `dimensions` on `ClusteredVectorHealth` for the
  clustered provider). Anything else refuses before a query runs.
- **Ground truth** is the exhaustive FP32 top-`k_max` per query.
  `full-depth` takes it from the route at `selection_k = rows`, which the
  coordinator refuses above its `--max-k` (the tool forwards that refusal
  and suggests `brute`). `brute:<rows file>` computes it over a rows file
  whose record `i` is global doc id `i`, with the rerank's own dot product
  (`exact_vectors::dot`, scalar accumulation in row order); its row count
  must equal the live corpus exactly. Every smaller `k` is a prefix, because
  the rerank order is total (score descending, id ascending).
- **The ladder.** For each `k` and each depth at or above it, every query
  runs `Query { k, selection_k: depth, FP32_RERANK, EXACT, profile: true }`.
  A response short of `k`, one whose `dense_execution` did not resolve
  `EXACT`, or one without a `QueryProfile` refuses the run. Per query the
  recall is `|top-k ∩ truth| / k` in integer ppm; the rung records the mean,
  the minimum, and the p50 of `total_ms`, `selection_ms`, `rerank_ms`, plus
  the client wall p50 on the table. A rung at `depth = rows` must recover
  every query; if it does not, the ground truth does not describe this
  generation and the run refuses.
- **Output.** `choose_points` over the ladder, then the profile is built,
  validated by the same rules `load` applies, saved, and its fingerprint
  printed with the table and any unmet targets. A run in which no rung met
  any target writes nothing and says which targets were unmet.

The ladder on the test harness (4,096 unit vectors in 32 dimensions over two
shards, 8 held-out queries, k = 10, 4-bit calibration), for scale:

```text
k   candidates  expansion  mean_recall  min_recall  p50_total_ms  p50_selection_ms  p50_rerank_ms
10          10    1.0000x     0.837500    0.700000         3.051             1.788          0.635
10          20    2.0000x     1.000000    1.000000         2.843             1.654          0.604
10          40    4.0000x     1.000000    1.000000         3.137             1.839          0.664
10        4096  409.6000x     1.000000    1.000000        12.245             4.064          6.670
```

Raw quantized top-10 loses three of ten on the worst query; twice the depth
recovers all of them on every query, so every target resolves to 20
candidates. Debug-build loopback timings, not a latency claim.

## How the coordinator serves it

An explicit `DenseQualityPolicy { target_recall_ppm }` on a single dense
leaf with `FP32_RERANK` and `selection_k = 0` resolves as before: the
profile point for `(k, target)`, checked against the live provider kind,
scoring fingerprint, dimensions, rows, generation, exact-row alignment, and
tombstones (`resolve_dense_quality`, `docs/query-api.md`).

**AUTO.** `DENSE_EXECUTION_MODE_AUTO` with `FP32_RERANK`, no
`DenseQualityPolicy`, and `selection_k = 0` used to run at `selection_k = k`
— the raw quantized top-`k`, the 0.544 number, under a mode whose contract
is "choose through evidence". It now resolves the depth through the
installed profile's `default_target_recall_ppm`, exactly as an explicit
policy naming that target would: the same `resolve_dense_quality` call, the
same hits bitwise, `dense_quality` set with the same fields, and
`dense_execution.planner_reason` extended with
`FP32 rerank depth selection_k=<n> resolved through quality profile "<id>"
default_target_recall_ppm=<t>`. The rule applies only when AUTO resolved to
`EXACT` on an exhaustive provider; AUTO through a dense execution policy on
an ANN provider already fixed its depth (`docs/dense-execution-policy.md`)
and is untouched.

Without a profile, or with a profile carrying no default, AUTO with FP32
refuses (`FAILED_PRECONDITION`): "AUTO with FP32 rerank needs a measured
quality profile with default_target_recall_ppm, or an explicit
DenseQualityPolicy or selection_k", followed by which of the two is missing.

`EXACT` and `UNSPECIFIED` with `FP32_RERANK` and `selection_k = 0` keep
today's behavior: the pool is `k`, `dense_quality` is absent, and the
rerank reorders exactly those `k` candidates. That is the caller's explicit
choice of traversal, not a planner's, and the response says so
(`rerank_rows = k`). AUTO with an explicit `selection_k` likewise runs at
the caller's depth with no `dense_quality`.

| Request | Depth | `dense_quality` |
|---|---|---|
| any mode, `quality` set, `selection_k = 0` | profile point | set |
| any mode, `quality` set, `selection_k != 0` | refused: competing depth authorities | — |
| AUTO, FP32, no `quality`, `selection_k = 0`, profile with default | profile point at the default target | set, planner reason names the default |
| AUTO, FP32, no `quality`, `selection_k = 0`, no profile / no default | refused by name | — |
| AUTO, FP32, `selection_k = n` | `n` | absent |
| EXACT / UNSPECIFIED, FP32, `selection_k = 0` | `k` | absent |
| AUTO through an ANN execution policy | the policy point's depth | absent |

## What is not guaranteed

- Recall is measured against the exhaustive FP32 order on the measured
  generation, for the held-out queries the ladder ran. A different query
  distribution can do worse; `measured_queries` and the per-rung `queries`
  say how many were run.
- A tombstone invalidates the profile: the sidecar rows and the quantized
  order no longer describe the same live set. The coordinator's existing
  `deleted_docs != 0` refusal ("compact and remeasure") applies to the AUTO
  default exactly as to an explicit policy, and the tool refuses to measure
  a generation with tombstones.
- A `(k, target)` the profile did not measure is refused, not
  interpolated; a request that needs `k = 100` against a profile measured
  at `k = 10` must be measured.
- The ladder is measured unfiltered. A quality policy on a filtered dense
  leaf resolves the same depth (the existing behavior); the measurement does
  not speak to recall within the filtered set.
- The profile is bound to one generation: a rebuild, a reshard that changes
  `corpus_rows` or the topology generation, or a recalibration that changes
  the scoring fingerprint refuses the profile until it is remeasured.

## Cost

Nothing here grows postings or resident memory: a profile is a few KiB of
validated TOML held once per coordinator. The cost is per query and
explicit — the FP32 rerank reads `selection_k * dimensions * 4` bytes of the
mapped sidecar (page cache, faulted by row) instead of `k * dimensions * 4`;
the ladder's `p50_rerank_ms` is that cost measured, and
`QueryProfile.rerank_rows` reports it on every response, which is the test's
gate against the depth silently falling back to `k`. `--max-rerank-mib`
still bounds the pool before fan-out.

## Tests

`src/quality.rs`: a version 1 file loads unchanged and refuses the new
fields by name; a version 2 file carries its evidence and default;
`choose_points` on the challenge ladder, including the mean-vs-worst case
and an unmet target; an unjustified point (unmeasured depth, over-claimed
recall, default naming no point, malformed rungs) refused by name; the
document round trip, fingerprint, and `from_measurements` refusing what
`load` would.

`tests/dense_quality.rs`, 4,096 rows over two shards: the measurement
produces a profile that loads, whose ladder is monotone in depth and exact
at full depth, whose points hold when every query is re-checked through the
public route at the point's depth and fail one rung below, and whose brute
ground truth equals the full-depth one; the two-shard ladder equals the
single-shard ladder and both drive AUTO to the same hits; AUTO with FP32
resolves through the default bitwise as the explicit policy, with the
outcome and planner reason, at the measured depth (`rerank_rows`), and again
after the shards are reopened from their images; no profile, no default,
row/fingerprint/generation drift, competing `selection_k`, `k = 0`, and an
unmeasured target refuse by name; EXACT and UNSPECIFIED keep the pool at `k`
and AUTO with native scoring never consults the profile; the tool refuses a
depth above the corpus, a `k` without a depth, a depth below `k`, a
dimension mismatch, a short brute file, a default naming no point, full
depth above `max_k`, and a tombstoned generation, which the coordinator
also refuses to serve.
