# Bandwidth as the budget

An unfiltered vector query reads every encoded vector of every shard it
touches. There is no per-chunk upper bound to stop early on, so the cost
of a query is bytes read divided by the node's scan rate, and the slowest
node sets the latency. The fix is placing bytes where the rate is, on
what each node was observed to do. This document defines the measurement
and the balance dry run. Both exist since 2026-09-05, shaped by the
[review](scale-out-coordination-review-2026-09-05.md): the measurement
in the scan path and the lease renewal, the dry run on the control
plane. Execution of a move does not exist and is a separate gate.

## The measurement

The number is the node's **observed scan rate**: encoded index bytes the
provider scan processed per second of active scan time.

- **Bytes** come from the index geometry (`chunked::encoded_row_bytes`):
  rows streamed times the image's row bytes, which are the packed codes
  (`dim / (8 / bits)` bytes, the format's own integer geometry) plus the
  row's 4-byte scale. Ids, headers, calibration tables, and the FP32
  sidecar are not read by the scan and are not counted. An unfiltered
  chunk streams every row; a filtered chunk streams the rows of the
  32-row blocks the allowlist left non-empty, because the kernel skips
  a block whose slots are all masked; a chunk the mask empties and a
  segment the pruner skips count nothing. A shared batched pass counts
  once, however many queries rode it: the observer sees each kernel
  call once, and a query's own profile figure is the pass it rode.
- **Time** is the wall time inside the kernel call, not the route's
  latency: queueing, analysis, rerank, and network are excluded.
- **Window.** The node keeps its last 64 scans within ten minutes
  (`node::SCAN_WINDOW_SAMPLES`, `node::SCAN_WINDOW_MS`), one sample per
  scan over a shard (a batched scan is one sample), and reports the
  ratio of their byte and time sums, the observation time of the newest
  sample, the sample count, and the window length. Fewer than four
  samples (`node::SCAN_WINDOW_MIN_SAMPLES`) report zero, which means
  unknown; a scan that streamed nothing is not a sample. The window is
  one per process: the rate is the node's, which is what the lease
  carries.
- **Scope.** The rate describes this node's provider and index settings
  under its recent workload. It is not disk, memory, ingest, or replica
  capacity, and the plan never treats it as one.

Where it lives: `NodeCapacity.scan_bytes_per_second`, with
`scan_rate_observed_unix_ms`, `scan_rate_samples`, and
`scan_rate_window_ms` beside it, carried on `RenewNodeLease`. Zero is
unknown; the authority never derives a time from an unknown rate.

## Residency

`NodeCapacity.residency` says where the node's bytes live. A `DEVICE`
node (an iOS or Android phone, [device-shards.md](device-shards.md)) is
excluded from every export, replication, relocation, and segment copy in
every plan and every executor, before any capacity logic runs, with the
exclusion reported. An `UNSPECIFIED` residency is reported and never
assumed movable.

## The balance dry run

`ClusterControl.PlanBalance` computes, and never executes. The provider's
row bytes come from the live shards' health through the coordinator
(one geometry cluster-wide, refused by name on disagreement or with no
coordinator attached), and a shard's pool from the coordinator's
placement tree when the leaf names nodes (matched to registered nodes by
advertised address or node id).

1. Per node, the bytes it serves (the sum over its shards of encoded
   index bytes), its rate, and the estimate `seconds = bytes / rate`.
2. Exclusions, one reason per node: `unmeasured` (zero rate or too few
   samples), `stale` (observed longer ago than `max_rate_age_ms`),
   `device`, `residency-unspecified`, `draining`, `no-lease`. The
   failure-domain rule is per move: a destination in the domain of one
   of the shard's ready copies is skipped.
3. Moves: whole-shard primary moves within a placement leaf's node set
   (the whole cluster without a tree), greedy on the largest estimated
   seconds, taking the shard whose move lowers the maximum the most, to
   the eligible node that ends up lowest, ties broken by node id then
   shard id. A move is kept only when it lowers the slowest node's
   estimate by at least `min_gain` of its value; at most `max_moves`
   moves.
4. The response carries the topology generation and control revision it
   was computed from, the loads as the plan saw them (before any move),
   the moves with the estimate after each, the exclusions with reasons,
   and the thresholds used. The move's `shard` is the shard's index in
   the durable topology, matched by its listener address.

Estimates are for the observed workload, not capacity: a faster measured
scan does not establish spare disk, memory, ingest headroom, or a safe
replica placement, and the plan says so by name in its fields.

## What this does not do

- It moves nothing. Execution, with hysteresis, copy cost, generation and
  tombstone races, and the copy-failure paths, is a separate gate.
- It never proposes a segment subset. A later segment plan carries its
  own identity and does not reuse these messages.
- It never touches a device node's bytes.
