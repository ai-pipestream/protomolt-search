# Durable cluster control

`ClusterControl` is an optional control service hosted beside
`SearchService`. It owns leases, capacity reports, placement decisions,
topology history, and safe publication. Query ranking and data movement remain
in the existing coordinator and node paths.

Enable it on a coordinator or `both` process with a generation-stamped shard
map and a durable state path. Every shard-map entry must have a complete,
gap-free `hash_lo`/`hash_hi` range when cluster control is enabled:

```sh
protomolt-search \
  --role=coordinator \
  --shard-map=/etc/protomolt-search/shards.toml \
  --control-state=/var/lib/protomolt-search/control.json
```

The default reconciliation interval is one second when control state is
configured. Policy flags are:

- `--control-lease-ms` (default 15000);
- `--control-replication-factor` (default 2, including the primary);
- `--control-split-rows` (default 25000000);
- `--control-merge-rows` (default 2000000 combined rows);
- `--control-compact-segments` (default 8);
- `--control-compact-tombstone-ppm` (default 100000, or 10 percent);
- `--control-reconcile-ms` (zero disables the timer but leaves manual RPCs).

Equivalent `PIPESTREAM_SEARCH_CONTROL_*` environment variables and config-file
keys are supported.

On the first start, the pristine durable store adopts the generation and route
set already loaded from the shard map. On later starts, durable state is the
authority: a newer durable generation is published before serving, while a
state file behind the live generation or conflicting at the same generation is
refused.

## Node lifecycle

1. `RegisterNode` returns a lease token only to that node.
2. `RenewNodeLease` refreshes the deadline and capacity. Draining nodes keep
   renewing until their shards have moved.
3. `ReportShard` publishes ready replica facts and segment/tombstone counts.
4. `DrainNode` stops new placements on the node and schedules copy-before-drop.
5. An expired primary is replaced by the newest compatible ready replica.

That last rule describes the current implementation, not safe writable failover
under a network partition. Lease expiry does not stop the old primary from
accepting writes. The [Raft design](raft-control-design.md) requires durable
ownership fencing before replacement-writer activation; its first release
leaves activation unavailable when the old owner cannot be fenced. Replicating
control decisions alone does not supply this data-plane guarantee.

Lease tokens are not included in `ClusterPlan`. A reused node id at a different
address is rejected while its lease is live.

Since 2026-09-04 the node side of this is real (`src/node_agent.rs`).
A node process registers when started with:

```sh
protomolt-search --role=node --config /etc/protomolt-search/host-b.toml \
  --node-id=b --control-addr=coordinator:50050 --failure-domain=rack-2 \
  --data-dir=/var/lib/protomolt-search/placed --advertise-addr=10.0.0.2:50051
```

- `--node-id` names the node; `--control-addr` is the coordinator's
  `ClusterControl` endpoint, reached through the same TLS/mTLS material
  as every cluster-internal channel (`docs/security.md`).
- `--failure-domain` goes into the capacity report; placement prefers
  another domain.
- `--data-dir` is where placed replicas live, `<data-dir>/<shard_id>/`:
  the shard's files (`shard`, `shard.wal/`, `shard.snap/` or
  `shard.segments/`) and `placed.toml`, the record of what the plan said
  and how far the bootstrap got.
- `--advertise-addr=host:port` is the address the node registers and
  the host every one of its listeners is advertised under; it defaults
  to the first shard listener when that binds a concrete interface, and
  is required when the listener binds `0.0.0.0`.
- `--replica-listen=ip:port` is the interface and first port placed
  replicas listen on (port 0, the default, lets the OS choose; the port a
  replica bound is remembered in `placed.toml` and reused after a
  restart). Every placed replica has a listener of its own, TLS when the
  node's listeners have it.
- `--node-report-ms` (default 10000), `--node-reconcile-ms` (default
  2000), `--node-lease-ms` (0: the plane's policy), and
  `--replica-lag-bound` (default 0) are the loops' knobs; the
  `PIPESTREAM_SEARCH_NODE_*` environment variables and config-file keys
  match.

Each configured shard is reported under its `shard_id` (config key per
`[[shards]]` entry; `slot-<offset>` when unset), at its own listener
address, with the hash range the configuration names (`hash_lo`/`hash_hi`,
both or neither) — else the range the plane's records hold for that
shard, else the published topology's route whose primary or replica
address is this listener (`ClusterPlan.topology` carries the routes for
that). A shard with no range from any of those is not reported, and the
log says so once. The plane owns roles: a shard the plane has a record
of is reported with that record's role (a promotion or demotion
happened there); a shard it has never seen reports itself primary if
configured, replica if placed.

The loops: registration retries every second until the plane answers;
the lease renews every lease/3 (a lease the plane no longer holds
re-registers); every served shard is reported on the timer, after
every flush, and right after registration — rows (the larger of the
vector and document tips), tombstones, bytes on disk, immutable
segments, generation, scoring and analysis fingerprints, ready; the
worker reads the plan every reconcile interval and executes the actions
whose `target_node_id` is this node, in plan order. A shard report names
the listener serving that shard; the lease owner vouches for its
listeners, and another node's registered address is refused.

## Replica bootstrap

`COPY_REPLICA` on a target node runs this sequence, idempotent across a
crash at any point (the durable `placed.toml` says where to resume:
a half-installed shard installs again, a finished copy reports again
and re-sends its completion, which the plane acknowledges):

1. Place: open a new shard under `<data-dir>/<shard_id>/` with the
   source's slot offset, hash range, and collection, on a fresh
   listener; the node's field tables and layout apply.
2. Install: `InstallSnapshotFrom{peer_addr}` against the source's
   listener (`docs/snapshots.md`): the source exports under its read
   lock, the copy stages, verifies, and installs the image, and learns
   the WAL cutoff it contains.
3. Catch up: `replication::sync_once` from that cutoff, repeatedly,
   until the source's watermark is within `--replica-lag-bound` clocks
   of the cursor (0 means exactly caught up, which a source under
   continuous ingest never is — give a bound there). Ready is reported
   then.
4. Complete: sync once more, compare the copy's rows and tombstones to
   the source's live health — a source that moved is synced again before
   anything is sent — and `CompletePlacementAction` with the copy's
   state at that clock, generation being the action's
   `target_generation`. The plane matches the output against the
   source's last report; a source whose report is behind or ahead of the
   live copy refuses ("copied replica differs from its source"), and the
   worker retries on a later tick, after the source has reported. A
   stale copy is never completed.

After completion the worker keeps a placed replica following its
primary every reconcile interval (the coordinator's `--replica-sync-ms`
loop can do the same for mapped replicas; run one of the two for a
given pair — a replica's single-writer ingest gate makes a race between
them a loud refusal, not a corruption).

`DROP_REPLICA` closes the copy's listener, removes its directory, and
completes with no output; the plane lists drops for retired copies
after a promotion. A drop is refused, and the action stays pending,
when the plane still lists this node's copy as the shard's primary, or
when the shard is configured statically on the node (remove it from the
configuration instead). `COMPACT_SHARD` and `MERGE_SHARDS` are logged as
unhandled by name and stay pending; `SPLIT_SHARD` runs the sequence
below.

## Shard split

`SPLIT_SHARD` on the node serving a primary splits it online into two
children that tile its hash range, with queries served throughout and
ingest paused only for the final drain. The sequence, durable in
`<data-dir>/<shard_id>.split/split.toml` so a crash at any point resumes
the same split:

1. Choose: two children named `<shard_id>-0` and `<shard_id>-1`, the
   range halved at its midpoint, and fresh slot ranges above every
   range the plan and this node know (spaced by the source's row count
   rounded up to a mebi), so a child's ids never reuse another shard's.
   A source with tombstones refuses ("compact it first"): the live tail
   moves appends only.
2. Build: `reshard::split_stable_logs_ranged` replays the source's own
   full-history WAL and writes each child's image (vectors, the FP32
   sidecar, postings rebuilt through the node's analysis backend),
   partitioned by the stable routing key's hash into the two ranges; a
   row without a stable key refuses by name (legacy rows are rebuilt
   before a live split). The images move into the children's placed
   directories and each child opens on a fresh listener with
   `installed = true`, exactly as a placed replica does.
3. Tail: `replication::catch_up_children_once` streams the source's log
   after the baseline cutoff and applies each record to the child its
   key routes to, until the children are within `--replica-lag-bound`.
4. Fence and drain: the source's ingest is fenced
   (`NodeServiceImpl::fence_ingest`; every later ingest stream refuses
   `FAILED_PRECONDITION` naming the children and the action, queries
   keep answering), the tail runs once more, and the source's watermark
   must not move past it. The fenced source is reported so the plane's
   record carries its final counts, and the children's rows are checked
   against them here as well.
5. Complete: `CompletePlacementAction` with the children as ready
   primaries at the action's target generation. The plane's checks (range
   tiling, row conservation, the scoring and analysis identity) replace
   the source's record with the children's and publish the topology; a
   refusal leaves the source fenced and reported, and the next tick
   retries.
6. Retire: the source is no longer served or reported. A placed source
   is removed like a dropped copy; a configured source keeps its files
   and leaves a marker under `<data-dir>/retired/<shard_id>`, which the
   agent honors across restarts until the shard is removed from the
   configuration and the marker deleted.

The children are single-image shards built from images, so they carry
`preexisting` rows in their WAL manifests: a later split of a child
waits on image-aware resharding (`docs/resharding.md`). Ingest routed
by the old topology between the fence and the publication is refused,
not lost; the client retries against the new generation.
`tests/split_shard.rs` pins the sequence: the plane plans the split of an
over-full primary, rows ingested after the plan reach the children
through the tail, the hook before completion sees the fence refusal by
name, the plan shows two ready primaries tiling the range and
conserving the rows, the coordinator answers with the same scores from
the published children, and a restart keeps the source retired and
re-serves the children.

Costs: the copy under the source's read lock is the export's copy time
(`ExportSnapshotResponse.copy_millis`; 7-11 ms for the 195 KB test
fixture, one sequential read plus write of the generation at scale);
writes wait for it, queries do not. The transient export staging on the
source and the placed copy on the target are the disk cost. Nothing
grows postings or resident memory.

`tests/replica_bootstrap.rs` pins the sequence: A serves s0 and keeps
ingesting; B registers empty; the plane plans the copy; B's worker
installs from A, catches up, and is refused once because A moved after
its last report (the plan still has the action, the copy already
matches A's live counts); A reports, the retry completes; the plan shows
B ready as s0's replica and the coordinator's live map lists B's
listener; with A stopped, every query answers with the same hits and
scores from B; a restart of B resumes the placed shard at the same
address; A's lease expiry promotes B and the query still answers; B
refuses to drop the copy it serves as primary and A refuses to drop a
configured shard; draining B copies s0 to C, promotes C, and B's worker
removes its copy when the drop is planned. The export under A's read
lock is measured with ingest running, and ingest resumes after it.

Placement prefers a different failure domain, then lower disk utilization.
The reconciler fills replication deficits and can move one large primary when
active-node disk utilization differs by at least 15 percentage points.

## Balance dry run

`PlanBalance` (`docs/bandwidth-budget.md`) answers with the whole-shard
primary moves that would bring the slowest node's estimated unfiltered
scan time down, and moves nothing. Its inputs are the durable state
(nodes, leases, capacities, primary replicas and their rows), the
provider's encoded row bytes read from the live shards' health through
the coordinator (one geometry cluster-wide; a disagreement is refused by
name, and a plane with no coordinator attached refuses), and each
shard's placement leaf from the coordinator's live topology, whose node
set bounds the move when the leaf names nodes.

Per node the plan reports the bytes it serves (rows of its primaries
times the row bytes), its observed rate, and `bytes / rate` as an
estimate; a node with no estimate is listed in `excluded` with one
reason: `unmeasured` (no rate, or too few samples), `stale` (observed
longer ago than `max_rate_age_ms`, default ten minutes),
`device` (a phone: never a source or a destination, before any capacity
logic runs), `residency-unspecified`, `draining`, or `no-lease`. A
destination in the failure domain of one of the shard's ready copies is
skipped. The greedy step takes the slowest eligible node, tries each of
its shards against each eligible node in the shard's pool, keeps the
move that leaves the lowest maximum (ties by node id, then shard id), and
stops when a move would lower the maximum by less than `min_gain` (in
[0, 1], default 0.10) or when `max_moves` (default 8) is reached. The
response carries the topology generation and control revision it was
computed from and the thresholds it used. Cluster trust, like the other
control routes; it is not a public route.

## Action protocol

`GetClusterPlan` returns durable, idempotent actions (and, since
2026-09-04, the published topology routes). Node workers execute only
actions assigned to their node:

- `COPY_REPLICA`: bootstrap a copy from the source's snapshot and catch
  its WAL tail up ("Replica bootstrap" above);
- `DROP_REPLICA`: remove a retired copy after promotion;
- `COMPACT_SHARD`: run bounded segment compaction;
- `SPLIT_SHARD`: build every child covering the source range ("Shard
  split" above);
- `MERGE_SHARDS`: combine the named adjacent pair.

`CompletePlacementAction` is the commit point. COPY/COMPACT/MERGE return one
ready output, SPLIT returns every child, and DROP returns no output. Completion
validates the assigned node, target generation, scoring and analysis identity,
row conservation, dense tombstone-free rewrite, and exact hash-range tiling.
Invalid or partial output leaves the action and live topology unchanged.

Completion acknowledgements are retained so a crash-window retry does not
repeat the mutation. The control state is written with fsync and atomic rename.
Only a complete gap-free topology is added to history and published to the
live coordinator. Rollback restores a historical route set as a new, monotonic
topology generation.

## Embedded and mobile

The same authority can run in the application process. A phone remains a
private one-node or multi-shard local cluster: registration, reconciliation,
compaction, and ranking all stay in process, and no phone index or query is
sent to the server. This is local coordination, not federation.
