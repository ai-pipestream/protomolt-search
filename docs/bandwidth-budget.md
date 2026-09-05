# Bandwidth as the budget

An unfiltered vector query reads every encoded vector of every shard it
touches. There is no per-chunk upper bound to stop early on, so the cost
of a query is bytes read divided by the node's scan rate, and the slowest
node sets the latency. The fix is placing bytes where the rate is, on
what each node was observed to do. This document defines the measurement
and the balance dry run. Both are proposals accepted for implementation
after the [review](scale-out-coordination-review-2026-09-05.md); neither
exists yet beyond the reserved contract.

## The measurement

The number is the node's **observed scan rate**: encoded index bytes the
provider scan processed per second of active scan time.

- **Bytes** come from the index geometry: rows scanned times encoded row
  bytes (dimensions times bits per component over eight, plus the
  per-row overhead the image format carries). A chunk the mask empties
  is not scanned and counts nothing; a segment the pruner skips counts
  nothing. A shared batched pass counts once, however many queries rode
  it.
- **Time** is the scan's own wall time inside the kernel call, not the
  route's latency: queueing, analysis, rerank, and network are excluded.
- **Window.** The node keeps the last N scans (N bounded, default 64)
  within a bounded age (default ten minutes) and reports the ratio of
  their byte and time sums, the observation time of the newest sample,
  the sample count, and the window length. Fewer than a minimum number of
  samples (default 4) reports zero, which means unknown.
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

`ClusterControl.PlanBalance` computes, and never executes:

1. Per node, the bytes it serves (the sum over its shards of encoded
   index bytes), its rate, and the estimate `seconds = bytes / rate`.
2. Exclusions: unmeasured (zero rate or too few samples), stale (observed
   longer ago than `max_rate_age_ms`), device residency, draining, no
   live lease, or a failure-domain rule.
3. Moves: whole-shard primary moves within a placement leaf's node set
   (the whole cluster without a tree), greedy on the largest estimated
   seconds, taking the shard whose move lowers the maximum the most, to
   the eligible node that ends up lowest, ties broken by node id then
   shard id. A move is kept only when it lowers the slowest node's
   estimate by at least `min_gain` of its value; at most `max_moves`
   moves.
4. The response carries the topology generation and control revision it
   was computed from, the loads, the moves with the estimate after each,
   the exclusions with reasons, and the thresholds used.

Estimates are for the observed workload, not capacity: a faster measured
scan does not establish spare disk, memory, ingest headroom, or a safe
replica placement, and the plan says so by name in its fields.

## What this does not do

- It moves nothing. Execution, with hysteresis, copy cost, generation and
  tombstone races, and the copy-failure paths, is a separate gate.
- It never proposes a segment subset. A later segment plan carries its
  own identity and does not reuse these messages.
- It never touches a device node's bytes.
