# Dense execution policy

`DENSE_EXECUTION_MODE_AUTO` selects an approximate technique only through
measured evidence: a version-2 policy, bound to one provider, score space,
corpus generation and execution shape, that names the request keys at which
an approximate technique met the policy's recall target against the
exhaustive selection. Without a qualifying point AUTO runs the exhaustive
technique at the requested precision and reports why in
`dense_execution.fallback_reason`. Explicit `BALANCED` and `FAST` never
consult a policy: they run versioned built-in presets and report
`policy_qualified = false` with no measured-recall claim
([Query API](query-api.md#dense-execution-contract)).

## The policy document

A coordinator loads one policy with `--dense-execution-policy=<path>` (or
`dense_execution_policy` in the config file). The file is a binary
`DenseExecutionPolicyDocument` protobuf (`search.proto`), fingerprinted by
SHA-256 of its bytes and written atomically (temporary file, fsync, rename).
Loading is strict and fails the coordinator by name:

- `format_version` must be `2`; the version-1 TOML schema is not read.
- `policy_id`, `embedding_model`, `provider_backend`,
  `scoring_fingerprint` and `corpus_fingerprint` are non-empty;
  `corpus_rows`, `dimensions` and `measured_queries` are positive;
  `deleted_docs <= corpus_rows`; `target_recall_ppm` is in `1..=1000000`.
- Every point names `mode` (`BALANCED` or `FAST`), `technique`
  (`HNSW_WALK` or `BITPLANE_STAGED`), `selection_precision` and
  `output_precision` (`FP32` or `QUANTIZED`), the measured
  `filter_strategy` (`SPEED` or `ENUMERATE_SMALL`) and `shape` (`UNARY` or
  `STREAMING`, with `BITPLANE_STAGED` restricted to `UNARY`),
  `1 <= k <= candidates <= corpus_rows`, a selectivity band inside
  `1..=1000000` ppm, and `measured_recall_ppm` in `1..=1000000`.
- Filtered `HNSW_WALK` points under `ENUMERATE_SMALL` pin
  `enumerate_max_rows <= corpus_rows`; unfiltered and `BITPLANE_STAGED`
  points leave `enumerate_max_rows` unset. Control pairs are set together
  (`patience_saturation_ppm` with `patience_hops`; `floor_greediness_ppm`
  with `floor_clamp_min`), within range, no two points share an exact
  measurement cell, and distinct selectivity bands for the same
  `(k, output_precision, shape)` do not overlap.

## How AUTO resolves

1. The policy's identity (provider kind, scoring fingerprint, corpus
   generation, `corpus_rows`, `deleted_docs`, `corpus_fingerprint` over
   per-shard WAL/stats/incarnation state, and `dimensions`) is checked
   against the live fleet. When any field drifts (such as an append,
   tombstone, replacement, or same-row-count corpus swap), `AUTO` fails
   closed to `EXHAUSTIVE`, discloses the drifted field in
   `dense_execution.fallback_reason`, increments
   `turbovec_dense_policy_drift_total`, and logs a warning (refusing only
   when the provider has no exhaustive technique).
2. The coordinator measures the live filter selectivity of the request's
   filters under the caller's authority view (1,000,000 ppm unfiltered).
3. A point qualifies when its `k`, selectivity band, output precision and
   execution shape (`STREAMING` when the coordinator streams dense search)
   match the request exactly, a named `selection_k` equals its `candidates`,
   and its `measured_recall_ppm` meets `target_recall_ppm`. Among
   qualifying points the fewest candidates wins, then `FAST` before
   `BALANCED`. Nothing is interpolated; evidence never transfers across a
   changed precision, routing or shape.
4. The live provider must serve every regime the point was measured under
   (a graph walk, staged bit-plane selection, an enumerate threshold,
   patience, candidate streaming, shared-floor pruning, and the selection
   precision); otherwise the request is refused.
5. The plan runs at the point's depth and controls, with
   `policy_qualified = true`, `policy_id`, `policy_fingerprint`,
   `policy_point`, `filter_selectivity_ppm` and
   `evidence_scope = SELECTIVITY_BAND_BENCHMARK`. Coverage stays
   `CANDIDATE_SET`; the recall evidence applies to the dense candidates at
   `k`, not to a document aggregate.

A request without a qualifying point runs the exhaustive technique (a
quantized scan, an FP32 sidecar scan, or pinned enumeration) and discloses
the reason; on a provider without an exhaustive technique it refuses.

## Staged bit-plane selection

An owned embedded TurboVec image of at least 32,768 rows at a supported
bit-plane geometry (`2`-bit or `4`-bit storage, `dim % 32 == 0`) on a host
whose kernels read bit planes (`aarch64` with `dotprod` for 4-bit, or
`x86_64` with AVX-512 F/BW/VBMI/VNNI) advertises `staged_top_k`. `BALANCED`
there selects by staged top-k (`BITPLANE_STAGED`, preset
`bitplane-balanced-v1`) instead of disclosing an exhaustive fallback, and
an FP32 `BALANCED` leaf rescores the staged pool. Staged selection is a
unary technique: a streaming coordinator does not offer it, and a node
refuses it on the streaming route.
