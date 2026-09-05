# Operating a cluster

## Nodes and the coordinator

One binary, three roles: `node`, `coordinator`, `both`. A node serves one gRPC
listener per shard. A coordinator keeps one lazily established HTTP/2 channel per
node address, multiplexes every concurrent query over it, and reconnects on its
own after a node restart.

`SearchService.ClusterHealth` fans health out to every primary and every
configured replica and reports per-target reachability. An unreachable node is a
reported outcome, not a request failure. It also reports the live topology
generation and, when the reachable shards do not score in one space, names each
provider kind and scoring fingerprint seen and which shards serve them. A fleet
in that state is rejected on search and routed ingest until it is repaired.

Useful settings:

- `--max-k` (default 10000) is the coordinator's heap guardrail. `k = 0` on any
  route selects it; a `k` above it is rejected, not clamped. `--max-k=0` is
  itself rejected, since it would reject every query.
- `--shard-deadline-ms` (0 is off) bounds one query's entire per-shard attempt. A
  shard that exceeds it fails the query with DEADLINE_EXCEEDED instead of
  stalling it.
- `--hedge-delay-ms` (0 is off) sends a second identical search to a shard's
  replica when the shard is still running after the delay, and the first success
  wins. Search is exact, so either copy returns identical results. This is stall
  insurance, not a latency optimization: set the delay above the healthy p99.
  Against a stalled node it improved fleet p99 by 26 to 37 percent;
  on a healthy bandwidth-bound fleet it cost 25 to 40 percent
  throughput, because the timer trips on ordinary bottleneck shards and the
  duplicate scan compounds the saturation it was meant to escape.
- `--floor-sharing` (default true) turns cross-shard cutoff sharing on and off.
  `--floor-delta`, `--floor-warmup-chunks`, and `--floor-min-interval-ms` reduce
  how many cutoff messages cross the network. None of them can change results: a
  delayed or skipped raise only defers pruning.
- `--block-max` (default true) enables lexical pruning; `false` forces the
  exhaustive scorer for an A/B. Results are identical either way.
- `--bm25-stream` (default true) uses the coordinator-owned lexical candidate
  stream; `false` is the exact unary A/B route.
- `--max-message-mib` (default 64) caps gRPC messages in both directions.

Every flag also reads an environment variable in two forms, the neutral
`PIPESTREAM_SEARCH_<NAME>` (checked first) and the legacy `TURBOVEC_<NAME>`, and
a config-file key with underscores. Settings resolve CLI flag, environment
variable, config file, built-in default.

## Replicas

A shard-map entry may name a `replica` serving the same data. On a primary error
the coordinator fails over to it. With a shard map containing replicas,
`--replica-sync-ms` (default 1000) tails each primary's fully clocked
write-ahead log into its replica, with cursors persisted next to the map
(`--replica-state`). A log generation rotation requires installing the new base
snapshot instead of a guess at missing history.

## The shard map and topology generations

`--shard-map=<file>` replaces `--nodes` on the coordinator; passing both is an
error. The file has a `generation` number and, per shard, the primary address,
an optional replica, the slot base, and the stable-key hash range.
`--shard-map-reload-ms` polls for complete newer maps and swaps one immutable
snapshot; an in-flight query keeps the map it started on.

Every `Query` response reports the generation it was served from. Set
`required_topology_generation` to fail before analysis and fan-out when the
generation is not the one you expect, so a retry cannot silently cross a
cutover. Routed ingest requires the generation explicitly and rejects zero.

`FreezeTopologyWrites`, `PublishTopology`, and `AbortTopologyCutover` are the
cutover handshake. Freeze waits for every routed write in the required
generation, then blocks new writes while queries continue. The opaque token
proves this process owns the freeze. If publication fails after the map is
durable, writes stay frozen: restart from the durable map or retry publication
and do not reopen the old generation and losing a tail.

## Relay coordinators

`--relay` on a coordinator makes it serve the node-facing surface over
its shard set and present itself to a parent coordinator as one shard,
which is how a root stands over thousands of shards with a level in
between. A relay serves `StreamSearch`, `TermStats`, and `Health` and
refuses every other node route by name; it takes one unnamed collection
on a dedicated endpoint, needs children with contiguous slot ranges, and
under a placement tree serves one leaf. Its statistics epoch is a token
bound to its children's epochs and to the map revision it was issued
under; a parent that holds an older token is refused and refetches.
Reference: `docs/relay-coordinators.md`.

## Splits, merges, and resharding

Records are routed to write-ahead-log bucket files by the same partition
function a split uses, so a split into at most `--wal-buckets` children hands
each child a contiguous set of bucket files without re-hashing a record. Finer
splits still work and repartition every record.

Enforced rules:

- One provider configuration per split or merge. Merging requires the same
  provider configuration byte for byte, and identical bucket counts across all
  inputs. Shards
  with no locked provider state cannot be resharded, because their scores cannot
  be certified comparable.
- Ids are generation-scoped. Children reassign dense local slots in original id
  order and take their slot base from the new shard map. Parent ids do not leak
  into a child.
- The shard map is the id-to-shard authority. A flip is metadata, not a data
  move.
- A split can be keyed by the placement code a row carries instead of the
  stable-key hash (chapter 10, `docs/placement.md`): one child per code or
  code range, a `default` child for rows with no code, and no CEL at replay.
  `SearchService.PlanPlacement` reports, per shard and leaf, what a proposed
  tree would move before any split runs.
- A shard without a log can serve but can only be rebuilt, not split or merged.
- Resharding requires full history. A generation that began from a snapshot
  install records that in its manifest, and the reshard tool rejects it. Such
  shards serve normally.

The offline flow is snapshot, replay the log written after it, then swap the
reshaped images in and point the coordinator at the new topology. The hitless
1-to-N flow partitions by stable product keys, tails while the parent serves,
and performs a freeze, catch-up, and map-publish cutover.

## Cluster control

Optional, and enabled by giving the coordinator a shard map with a generation number
plus a durable state path (`--control-state`). Every shard-map entry then needs
a complete gap-free hash range.

Nodes register with `--node-id`, `--control-addr`, `--failure-domain`, and
`--data-dir`, renew a lease, report every shard under its own listener, and run
a worker that executes placement actions. `--advertise-addr` is required when a
shard listener binds `0.0.0.0`. `--replica-listen` with port 0 lets the OS
choose, and the bound port is remembered across restarts.

Every lease renewal carries the node's observed scan rate (encoded
index bytes per second of kernel time, over its last scans) with its
observation time and sample count, and the node's residency: a phone
declares `DEVICE` and is never a source or a destination of any plan.
`ClusterControl.PlanBalance` is a read-only dry run that reports the
whole-shard moves that would lower the slowest node's estimated scan
time, with every excluded node and its reason (`docs/bandwidth-budget.md`).

Policy settings and defaults: `--control-reconcile-ms` 1000,
`--control-lease-ms` 15000, `--control-replication-factor` 2 (the primary
included), `--control-split-rows` 25000000, `--control-merge-rows` 2000000,
`--control-compact-segments` 8, `--control-compact-tombstone-ppm` 100000 (10
percent). A lease under 1000 ms, a replication factor of 0, a tombstone
threshold above 1000000, or a non-positive rows or segments setting is rejected
at startup.

Placement actions are `COPY_REPLICA`, `DROP_REPLICA`, `PROMOTE_REPLICA`,
`SPLIT_SHARD`, `MERGE_SHARDS`, and `COMPACT_SHARD`. Completion is the commit
point: it validates the assigned node, target generation, scoring and analysis
identity, row conservation, a tombstone-free dense rewrite, and exact hash-range
tiling. Invalid or partial output keeps the action and the live topology
unchanged. Only a complete gap-free topology is published, and rollback restores
a historical route set as a new, monotonically increasing generation.

A replica copy is idempotent across a crash at any point: place, install from
the primary's snapshot stream, catch up until the source's watermark is within
`--replica-lag-bound`, then complete with counts that match the source. A source
under continuous ingest is not fully up to date, so give that bound a value.
A stale copy is not completed. Run either the node worker's follow loop or the
coordinator's `--replica-sync-ms` loop for a pair, not both.

An online shard split builds two children from the source's own log by stable-key
range, places them on fresh listeners, tails them by key, fences the source for
the final drain, completes with the children as primaries, and takes the source
out of service. Ingest routed by the old topology between the fence and publication is
rejected, not lost. Queries keep answering throughout.

Placement prefers a different failure domain, then lower disk use. The reconciler
fills replication deficits and can move one large primary when active nodes
differ by at least 15 percentage points of disk use.

Lease credentials are returned only to the node that registered; they are not
part of a published plan.

## The console

The `console` binary is the operator's front end: a loopback HTTP
server that transcodes proto3 JSON to the cluster's gRPC and serves a
search page and a dashboard. It holds the TLS material and the bearer
token, so a browser carries neither. The dashboard shows the live
metrics stream, the runtime knobs, the shard map with each shard's
placement group and any relay in it, the recent queries, and the two
dry runs: a proposed placement tree evaluated over the live documents
(`PlanPlacement`), and the balance plan from each node's observed scan
rate (`PlanBalance`, the one cluster-control method the console
exposes). Neither dry run moves anything. Reference:
`docs/console-facade.md`.

## Metrics

`--metrics-listen=host:port` exposes Prometheus text. It is off by default; the
counters still count when it is unset.

It is plain HTTP with no authentication, on its own listener. TLS and bearer
principals cover the gRPC listeners, not this one. Bind it to loopback or a
private scrape network, and leave it unset on a host with no such interface.

Series: `turbovec_requests_total{rpc}`, `turbovec_requests_in_flight{rpc}`,
`turbovec_request_duration_seconds` as bucket, sum, and count with `{rpc}` and,
on streaming routes, `{phase}`, `turbovec_request_errors_total{rpc,code}`, the
scan counters, the ingest counters, and per-shard gauges labeled by slot offset.

Requests count at arrival, so a shard erroring under load shows as traffic
and not as silence. One arrival is one count however many layers it passes
through. Histogram buckets are fixed and identical for every route, from 1 ms to
10 s plus an infinity bucket, compared in integer nanoseconds.

Streaming routes include a `phase` label: `first_response` is arrival to the first
message given to the transport, `complete` is arrival to the stream's terminal
event. Both phases count every request, so a `sum by (rpc)` over a streaming
route sums two phases. Select the phase you mean.

Error codes are labeled individually, with an `other` bucket that groups
cancelled, unknown, already-exists, aborted, out-of-range, unimplemented, and
data-loss, including a dropped handler or stream. Every route-and-code row is
declared up front, so a `rate()` does not start from an absent series. Arrivals
minus errors is the success count.

Gauges are sampled at scrape time from live shard state, so they cannot go
stale. In-flight is the exception: arrivals minus departures.

There are, by design, no per-principal or per-collection labels, and no
sidecar or fleet client latency split; that belongs to the per-request profile
blocks.

## The work queue

Cluster control does not push work at nodes. The authority computes placement
actions and publishes them in the plan; each node's worker polls the plan on its
reconcile timer, picks up the actions assigned to it, and reports back through
`CompletePlacementAction`. Actions are idempotent, so a worker that crashed
mid-action resumes from durable state on the node, and does not start over or
apply part of the work.

That shape is why an action can be long-running (a replica copy, a split, a
compaction) without holding an RPC open, and why the authority can be restarted
without losing queued work: the queue is the durable plan, not a process's
memory. `ReconcileCluster` with `dry_run` computes and returns the actions the
authority would queue without persisting them.

Reference: `docs/cluster-control.md`, `docs/resharding.md`, `docs/metrics.md`.
