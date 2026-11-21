# Dense execution policy

`DENSE_EXECUTION_MODE_AUTO` selects an approximate technique only through
measured evidence: a version-2 policy, bound to one provider, score space,
corpus generation, shard mutation state, hardware profile and execution shape,
that names the request keys at which an approximate technique met the policy's
recall target and measured faster than the exhaustive selection. Without a
qualifying point `AUTO` runs the exhaustive technique at the requested
precision and reports why in `dense_execution.fallback_reason`. Explicit
`BALANCED` and `FAST` never consult a policy: they run versioned built-in
presets and report `policy_qualified = false` with no measured-recall claim
([Query API](query-api.md#dense-execution-contract)).

## The policy document

A coordinator loads one policy with `--dense-execution-policy=<path>` (or
`dense_execution_policy` in the config file, globally or per collection). The
file is a binary `DenseExecutionPolicyDocument` protobuf (`search.proto`),
fingerprinted by SHA-256 of its bytes and persisted atomically via
`OpenOptions::create_new` on a unique `.tmp.<pid>.<seq>` file, `sync_all`,
`rename`, and a mandatory parent-directory `sync_all`. Across the dense v2
upgrade, `format_version` is `2` and requires the full v2 field set
(coordinators, relays, nodes, and policy artifacts upgrade together). Loading
is strict and fails startup by name:

- `format_version` must be `2`; the version-1 TOML schema is refused at load.
- `policy_id`, `embedding_model`, `provider_backend`,
  `scoring_fingerprint`, `corpus_fingerprint` and `hardware_fingerprint`
  are non-empty (after trimming); `corpus_rows`, `dimensions`,
  `measured_queries` and `node_count` are positive; `hosts` is non-empty and
  lists `node_count` profiled `DenseHostProfile` entries whose canonical digest
  equals `hardware_fingerprint`; `deleted_docs <= corpus_rows`;
  `target_recall_ppm` is in `1..=1000000`.
- Every point names `mode` (`BALANCED` or `FAST`), `technique`
  (`HNSW_WALK` or `BITPLANE_STAGED`), `selection_precision` and
  `output_precision` (`FP32` or `QUANTIZED`), `filter_strategy` (`SPEED` or
  `ENUMERATE_SMALL` on `HNSW_WALK`; `DEFAULT` on `BITPLANE_STAGED`), and
  `shape` (`UNARY` or `STREAMING`, with `BITPLANE_STAGED` restricted to
  `UNARY`), plus `1 <= k <= candidates <= corpus_rows`, a selectivity band
  inside `1..=1000000` ppm, `measured_recall_ppm` in `1..=1000000`, and both
  techniques' measured latency (`approximate_p50_us`/`p99_us`,
  `exhaustive_p50_us`/`p99_us`) and positive finite sequential QPS.
  `approximate_qps` and `exhaustive_qps` record sequential single-query rates
  (`measured_queries / sum(latency_s)`, i.e. `1 / mean_latency`), not
  concurrent multi-client saturation throughput.
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

1. **Live identity, hardware & topology drift.** The policy's corpus and
   hardware identity (`provider_backend`, `scoring_fingerprint`,
   `corpus_generation`, `corpus_rows`, `deleted_docs`, `corpus_fingerprint`,
   `dimensions`, and `hardware_fingerprint`) is checked against the live fleet.
   `hardware_fingerprint` (`fleet_fingerprint`) hashes the sorted, deduplicated
   set of normalized per-shard `DenseHostProfile`s—independent of shard count
   across homogeneous hosts, while changing on any CPU/SIMD/core difference or
   unprofiled shard/relay. `corpus_fingerprint`, by contrast, hashes each shard
   by index together with its `num_vectors`, `deleted_docs`, `wal_generation`,
   `wal_high_watermark`, `stats_epoch`, and non-empty per-process
   `stats_incarnation` (peers reporting an empty `stats_incarnation` fail
   preflight). Adding, removing, replacing, or restarting any shard or relay—as
   well as any append, tombstone, replacement, snapshot swap, or hardware
   change—invalidates installed policies and fails `AUTO` closed to
   `EXHAUSTIVE`, disclosing the drifted field in
   `dense_execution.fallback_reason`, incrementing
   `turbovec_dense_policy_drift_total`, and writing a warning to stderr via
   `eprintln!` until `OptimizeDensePolicy` re-measures the live fleet (refusing
   only when the provider has no exhaustive technique).
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
- **Warm-up & Sweep.** For each precision pass (`QUANTIZED` native and, when
  exact-vector sidecars are available, reranked `FP32`), the coordinator runs
  an untimed exhaustive warm-up `Query` before timing `k` bands (default 10,
  100, 1,000 and 10,000 within `max_k.min(corpus_rows)`). Each truth cell
  verifies `ExhaustiveCertified` coverage without fallback; each candidate
  technique is measured at depths `k`, `2k`, `4k` and `8k`, verifying zero
  fallback and recording the bound `filter_strategy` from the execution
  receipt. Native approximate cells invoke in-process `SearchService::query`
  at `k = depth`, matching the per-shard candidate fetch (`fetch_k = depth`)
  that served `AUTO` dispatches for `policy_depth = depth` while scoring top-`k`
  recall in `O(k)` via pre-indexed ground-truth ID sets.
- **Unfiltered Selectivity Scope.** `OptimizeDensePolicy` measures unfiltered
  queries and records `filter_selectivity_ppm_min = filter_selectivity_ppm_max
  = 1_000_000`. On coordinators configured with restrictive
  `document_visibility` (or caller filters) where live visible selectivity is
  below `1,000,000` ppm, `AUTO` finds no matching selectivity band and fails
  safe to `EXHAUSTIVE`.
- **Bound & Recheck.** `max_runtime_ms` bounds fleet discovery, warm-up, every
  in-flight `Query` RPC, cell scoring, and post-sweep verification; incomplete
  cells are discarded and `truncated = true` is reported. After a successful
  sweep, `LiveIdentity` and normalized shard `DenseHostProfile`s are
  re-verified within the deadline and abort with `ABORTED` if concurrent
  mutations or hardware changes occurred mid-sweep.
- **Install.** With `install`, `PolicySlot::install` serializes persistence
  under an async mutex, writes and fsyncs the policy on the blocking pool
  without holding the serving `RwLock` write guard, and swaps the serving `Arc`
  atomically.
