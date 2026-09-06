# Dense execution policy

Implemented on branch 2026-09-02. `DENSE_EXECUTION_MODE_AUTO` chooses a
dense traversal only through evidence: an exhaustive provider resolves to
`EXACT`, and a provider with a configured approximate traversal resolves to
`ANN` only through a generation-bound policy that says the approximation was
measured good enough at that exact request key. There is no interpolation
between measured points, no default candidate depth, and no provider control
(an IVF `nprobe`, a graph `ef`) hidden behind a fixed setting. Everything the
planner decided is reported in `QueryResponse.dense_execution`.
Document-scoped execution omits physical geometry and free-form labels with an
explicit redaction flag; [Query execution disclosure](query-disclosure.md)
defines what remains and how `evidence_scope` describes the benchmark.

Today the product's own provider (embedded TurboVec, clustered TurboVec) is
exhaustive, so AUTO resolves to `EXACT` everywhere in production, bitwise the
same as `EXACT`. The policy machinery is complete and exercised through a
test provider that advertises a configured ANN contract over an exhaustive
image (`harness::fake_ann`), so the rules are pinned before a real ANN
provider exists.

## The policy file

`--dense-execution-policy=<path>` (or `dense_execution_policy` in the config
file, per collection or cluster-wide) installs one policy on a coordinator.
The file is the persistence: a strict TOML document (unknown keys refused),
fingerprinted by the SHA-256 of its bytes. `DenseExecutionPolicy::save`
writes the same document form `load` reads.

```toml
format_version = 1
policy_id = "court-ann-2026-09"
embedding_model = "bge-m3"
corpus_generation = 42          # the topology generation the measurement ran on
corpus_rows = 1000000
dimensions = 1024
provider_backend = "fake-ann"   # GetVectorBackend backend_kind
scoring_fingerprint = "<GetVectorBackend descriptor fingerprint>"
measured_queries = 128

[[points]]
k = 10
filter_selectivity_ppm_min = 1000000   # unfiltered
filter_selectivity_ppm_max = 1000000
candidates = 200                       # the depth the providers are asked for
measured_recall_ppm = 992000           # against the exhaustive traversal

[[points]]
k = 10
filter_selectivity_ppm_min = 100000    # filters admitting 10%..50% of rows
filter_selectivity_ppm_max = 500000
candidates = 800
measured_recall_ppm = 985000
```

Validation refuses by name: an unsupported `format_version`; an empty
identity string, or one that needs escaping; zero rows, dimensions, or
measured queries; no points; a point with `k = 0`, a selectivity band outside
`1..=1000000` or inverted, a recall outside that range, a candidate depth
below `k` or above the corpus; and two points for the same `k` and depth
whose bands overlap.

## How AUTO resolves

1. Preflight, as for every mode: every shard's provider kind, scoring
   fingerprint, dimensions, quality contract, and completion capability must
   agree, and the query dimension must match.
2. An exhaustive provider (`EXHAUSTIVE_NATIVE_SCORE` with
   `exhaustive_completion`) resolves to `EXACT`. The policy is not consulted.
3. Otherwise a policy must be installed and the provider must advertise a
   configured ANN traversal (`CONFIGURED_ANN` or `PROBABILISTIC_BOUND`); no
   policy refuses naming the provider and the flag.
4. The policy's identity is checked against the live cluster field by field:
   `provider_backend`, `scoring_fingerprint`, `corpus_generation` (the
   coordinator's topology generation), `corpus_rows` (the sum of the shards'
   vector rows), `dimensions`. The first mismatch refuses, naming the field
   and both values.
5. The request key: `k` as sent (0 is refused — a policy point needs a
   number, not the coordinator's default), the candidate depth the request
   named in `selection_k` (0 when none), and the filter's live selectivity:
   the coordinator resolves the mandatory document view and caller filters
   against live vector membership. It takes admitted vector rows over the
   policy's physical vector corpus rows in parts per million. Documents without
   vectors do not contribute. A document view applies even when the request
   carries no filter; 1,000,000 is the shortcut only when both are absent.
   The vector and document membership passes validate the same admitted
   physical epochs and incarnations, including standalone planner calls.
6. The point must match exactly: the same `k`, a band containing the live
   selectivity, and the named candidate depth. With no depth named, the point
   qualifies only when the policy measured exactly one depth for that `k` and
   band; several depths refuse and list them. A miss refuses and lists what
   the policy measured for that `k`.
7. The dense leg runs at the point's candidate depth; the response trims to
   `k`. `dense_execution` reports `resolved_mode = ANN`,
   `exhaustive_completion = false`, `policy_id`, `policy_fingerprint`, the
   `policy_point`, `filter_selectivity_ppm`, and `candidate_depth`.

`ANN` requested explicitly on a configured ANN provider never needs a
policy: the caller accepted the provider's traversal. It is still marked
approximate, and `DENSE_SCORE_MODE_FP32_RERANK` on top of it rescores the
candidate pool without widening it — the outcome stays `ANN` and says so.

The measured recall belongs to the policy's benchmark cohort and selectivity
band. Matching that band does not establish recall for every predicate with
the same cardinality. [Document-restricted Query](document-query-authorization.md)
retains that evidence scope and withholds physical metadata. The authority
predicate must not be bypassed to select an unfiltered policy point.

## AUTO and FP32 rerank

Landed 2026-09-04. Step 2 resolves the traversal, not the rerank depth. When
AUTO resolved to `EXACT` and the leaf asks for `DENSE_SCORE_MODE_FP32_RERANK`
with no `DenseQualityPolicy` and `selection_k = 0`, the candidate depth is
resolved through the installed dense quality profile's
`default_target_recall_ppm` (`docs/dense-quality-profile.md`), exactly as an
explicit policy naming that target would — the same hits, `dense_quality`
set, and `planner_reason` extended with the profile and default that chose
the depth. No profile, or one without a default, refuses by name rather than
running the raw quantized top-`k`. The two files are separate: the execution
policy qualifies an approximate traversal at a request key; the quality
profile prices an exhaustive one's FP32 rerank depth. AUTO through a
qualified ANN point keeps that point's depth and never consults the quality
profile.

## Tests

`tests/dense_execution.rs`: AUTO on an exhaustive provider equals `EXACT`
bitwise and consults no policy even when one is installed; AUTO on the fake
ANN provider refuses by name without a policy; a policy bound to another
generation or row count refuses naming the field; a qualified point runs
`ANN`, reports its provenance, and returns the same hits and provenance over
two shards as over one; a filter keys the point on its measured selectivity
band and an unmeasured band refuses naming the live ppm; a named candidate
depth must be a measured one; explicit `ANN` is marked approximate and FP32
rerank does not upgrade it. `src/dense_policy.rs` unit tests pin the file
round trip, the fingerprint, the exact-key rule, identity mismatches, and
the malformed-document refusals.
