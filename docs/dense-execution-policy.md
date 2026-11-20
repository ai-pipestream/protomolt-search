# Dense execution policy

`DENSE_EXECUTION_MODE_AUTO` selects an approximate technique only through
measured evidence: a version-2 policy, bound to one provider, score space,
corpus generation, hardware profile and execution shape, that names the request
keys at which an approximate technique met the policy's recall target and
measured faster than the exhaustive selection. Without a qualifying point `AUTO`
runs the exhaustive technique at the requested precision and reports why in
`dense_execution.fallback_reason`. Explicit `BALANCED` and `FAST` never
consult a policy: they run versioned built-in presets and report
`policy_qualified = false` with no measured-recall claim
([Query API](query-api.md#dense-execution-contract)).

## The policy document

A coordinator loads one policy with `--dense-execution-policy=<path>` (or
`dense_execution_policy` in the config file, globally or per collection). The
file is a binary `DenseExecutionPolicyDocument` protobuf (`search.proto`),
fingerprinted by SHA-256 of its bytes and persisted atomically via
`OpenOptions::create_new` on a unique `.tmp.<pid>.<seq>` file, `sync_all`,
`rename`, and a mandatory parent-directory `sync_all`. Loading is strict and
fails startup by name:

- `format_version` must be `2`; the version-1 TOML schema is refused at load.
- `policy_id`, `embedding_model`, `provider_backend`,
  `scoring_fingerprint`, `corpus_fingerprint` and `hardware_fingerprint`
  are non-empty (after trimming); `corpus_rows`, `dimensions`,
  `measured_queries` and `node_count` are positive; `hosts` lists `node_count`
  profiled `DenseHostProfile` entries matching `hardware_fingerprint`;
  `deleted_docs <= corpus_rows`; `target_recall_ppm` is in `1..=1000000`.
- Every point names `mode` (`BALANCED` or `FAST`), `technique`
  (`HNSW_WALK` or `BITPLANE_STAGED`), `selection_precision` and
  `output_precision` (`FP32` or `QUANTIZED`), `filter_strategy` (`SPEED` or
  `ENUMERATE_SMALL` on `HNSW_WALK`; `DEFAULT` on `BITPLANE_STAGED`), and
  `shape` (`UNARY` or `STREAMING`, with `BITPLANE_STAGED` restricted to
  `UNARY`), plus `1 <= k <= candidates <= corpus_rows`, a selectivity band
  inside `1..=1000000` ppm, `measured_recall_ppm` in `1..=1000000`, and both
  techniques' measured latency (`approximate_p50_us`/`p99_us`,
  `exhaustive_p50_us`/`p99_us`) and positive finite QPS.
- Filtered `HNSW_WALK` points under `ENUMERATE_SMALL` pin
  `enumerate_max_rows <= corpus_rows`; unfiltered points leave
  `enumerate_max_rows` unset, `UNARY` points forbid `finish_settle_us`, and
  `BITPLANE_STAGED` points forbid `enumerate_max_rows`, patience,
  `finish_settle_us`, and shared-floor controls. Control pairs are set
  together (`patience_saturation_ppm` with `patience_hops`;
  `floor_greediness_ppm` with `floor_clamp_min`) within range, no two points
  share an exact measurement cell, and distinct selectivity bands for the same
  `(k, output_precision, shape)` do not overlap.

## How AUTO resolves

1. **Live identity, hardware & restart drift.** The policy's corpus and
   hardware identity (`provider_backend`, `scoring_fingerprint`,
   `corpus_generation`, `corpus_rows`, `deleted_docs`, `corpus_fingerprint`,
   `dimensions`, and `hardware_fingerprint` over the per-shard CPU/SIMD/core
   profile) is checked against the live fleet. `corpus_fingerprint` hashes each
   shard's `num_vectors`, `deleted_docs`, `wal_generation`,
   `wal_high_watermark`, `stats_epoch`, and non-empty per-process
   `stats_incarnation` (peers reporting an empty `stats_incarnation` fail
   preflight). Because `stats_incarnation` is generated per process start, any
   node or relay restart—like an append, tombstone, replacement, snapshot swap,
   or hardware mismatch—invalidates installed policies and fails `AUTO` closed
   to `EXHAUSTIVE`, disclosing the drifted field in
   `dense_execution.fallback_reason`, incrementing
   `turbovec_dense_policy_drift_total`, and writing a warning to stderr via
   `eprintln!` until a fresh policy is generated (refusing only when the
   provider has no exhaustive technique).
2. **Compact bitmap selectivity.** On filtered or `document_visibility`-scoped
   requests, the coordinator counts `|filter ∩ vectors|` directly over
   validated per-shard compact wire bitmaps (word-by-word popcount and
   disjoint-interval bitwise AND) without materializing document ID sets
   (`1,000,000` ppm when unfiltered and unrestricted).
3. **Exact-key qualification.** A point qualifies when its `k`, selectivity
   band, output precision, and execution shape (`STREAMING` on single-leaf
   `--stream-search`/finish-window scans and `DECOMPOSED` composite scans;
   `UNARY` on single-leaf unary scans and `RRF`/`SCORE_BLEND`/`CASCADE`
   composite leg rounds) match the request, any explicit `selection_k` equals
   `candidates`, `measured_recall_ppm >= target_recall_ppm`, **and** its
   approximate technique measured faster than the exhaustive one
   (`approximate_p50_us < exhaustive_p50_us`). Among qualifying points the
   fastest p50 wins, then fewest `candidates`, then `FAST` before `BALANCED`.
   Nothing is interpolated across keys, precisions, routings, shapes, or
   hardware.
4. **Live capability drift.** The live fleet must advertise every capability
   the qualifying point requires (`walks`, `staged_top_k`, `mask_enumeration`,
   `patience_early_stop`, `candidate_stream`, `floor_pruning`, matching native
   selection precision, and shard-local `FP32` sidecars for quantized `FP32`
   pools). If a required capability is no longer available (for example after
   moving to a host without bit-plane SIMD, reopening as a mapped image,
   shrinking below `32,768` rows, or disabling bit planes via environment
   gates), `AUTO` fails closed to `EXHAUSTIVE`, increments
   `turbovec_dense_policy_drift_total`, logs a warning to stderr (`eprintln!`),
   and names the lost capability in `fallback_reason`.
5. **Qualified execution & provenance.** A live-capable qualifying point runs
   at its measured `candidates` depth and controls with
   `policy_qualified = true`, `policy_id`, `policy_fingerprint`,
   `policy_point`, `filter_selectivity_ppm`, and
   `evidence_scope = SELECTIVITY_BAND_BENCHMARK`. Coverage stays
   `CANDIDATE_SET`; recall evidence applies to dense candidates at `k`, never
   to a document aggregate.

`clustered-turbovec` exposes no approximate policy techniques (`walks = false`,
`staged = false`); `AUTO` there serves single-leaf `FP32` exhaustively from
product-owned `FP32` sidecars and refuses uncertified quantized `AUTO`.

## Staged bit-plane selection

An owned embedded TurboVec image of at least `32,768` rows at a supported
bit-plane geometry (`2`-bit or `4`-bit storage, `dim % 32 == 0`) on a host
whose kernels read bit planes (`aarch64` with `dotprod` for 4-bit, or
`x86_64` with AVX-512 F/BW/VBMI/VNNI) advertises `staged_top_k`, latching
`TURBOVEC_NO_VECTOR_MAJOR`, `TURBOVEC_NO_VNNI`, `TURBOVEC_2BIT_PLANES`,
`TURBOVEC_4BIT_PLANES`, and `TURBOVEC_2BIT_VM8` once per process in
`OnceLock`s to match the engine. Relay coordinators intersect child backend
capabilities so mixed row counts or mapped children serve cleanly without
advertising `staged_top_k` fleet-wide.

`SegmentedProvider` starts capability folding from its live writable tail, so a
segmented shard advertises `staged_top_k` only while the writable tail itself
holds at least `32,768` eligible rows; after sealing a segment, staging stays
off until the tail regrows past `32,768` rows. Staged selection is `UNARY`
only: streaming coordinators do not advertise it, and nodes refuse it on the
streaming route.

## optimize()

`SearchService.OptimizeDensePolicy` (Admin; `examples/dense_optimize.rs` is
the CLI) measures a homogeneous shard fleet under a single-flight guard and
produces the policy:

- **Sample.** Supply representative held-out sample query vectors at the
  fleet's dimension (at least 64 queries is recommended, as each cell
  qualifies on worst-query recall).
- **Sweep.** For each deduplicated `k` band (default 10, 100, 1,000 and
  10,000 within `max_k.min(corpus_rows)`), the coordinator executes its own
  `Query` route (`ExhaustiveCertified` verified on truth baselines) and
  measures each approximate technique at depths `k`, `2k`, `4k` and `8k`.
  On quantized providers with exact-vector sidecars, both `QUANTIZED` and
  reranked `FP32` output points are measured and emitted.
- **Bound & Recheck.** `max_runtime_ms` bounds fleet discovery, every
  in-flight `Query` RPC, cell scoring, and post-sweep verification; incomplete
  cells are discarded and `truncated = true` is reported. Before installing or
  returning, `LiveIdentity` and normalized shard `DenseHostProfile`s are
  re-verified and abort if concurrent mutations or hardware changes occurred
  mid-sweep.
- **Install.** With `install`, the coordinator persists the policy in a
  blocking worker and swaps the serving `Arc` atomically under its
  `--dense-execution-policy` slot.
