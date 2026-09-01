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

Lease tokens are not included in `ClusterPlan`. A reused node id at a different
address is rejected while its lease is live.

Placement prefers a different failure domain, then lower disk utilization.
The reconciler fills replication deficits and can move one large primary when
active-node disk utilization differs by at least 15 percentage points.

## Action protocol

`GetClusterPlan` returns durable, idempotent actions. Node workers execute only
actions assigned to their node:

- `COPY_REPLICA`: use the existing snapshot/WAL catch-up path;
- `DROP_REPLICA`: remove a retired copy after promotion;
- `COMPACT_SHARD`: run bounded segment compaction;
- `SPLIT_SHARD`: build every child covering the source range;
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
