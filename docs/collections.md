# Collections

Implemented on branch 2026-09-02 (roadmap item 11). One cluster serves many datasets,
and no request, statistic, floor, fusion, facet, aggregation, browse, or
placement ever crosses from one to another. This is the real version
the roadmap distinguished from "a name on every request, validated
against the shard's bound name": the isolation is structural, not a
comparison.

## What a collection is

A `CoordinatorServiceImpl` already owns everything one dataset needs:
its shard set and replica list, hash-range topology and generation, the
per-shard statistics cache (global df and avgdl), the vector backend and
calibration, BM25 parameters, the analysis backend, the phrase index,
the dense quality profile, fan-out limits. A **collection is one of
those, under a name**. `CollectionSet` (`src/collections.rs`) contains
several and routes each public request to the one its `collection`
field names. There is no table two collections share, so there is
nothing to leak through: the same term has a different df in each
collection because each keeps its own statistics cache over its own
shards.

A **shard belongs to only one collection.** The node is configured
with it (`--collection`, or `collection` per `[[shards]]` entry), reports
it in `Health.collection`, writes it on every document it logs, refuses
a document or a bind that names another, writes it into the WAL
manifest, and refuses to open a log that names another. Reshard children
inherit it from the parent generation, and generations of different
collections never merge (`same_backend_config` compares it first).

The identity of a collection is the operator's name plus, for a mapped
(descriptor-bound) dataset, the plan fingerprint the shards already
store: the name says which dataset, the fingerprint says which schema of
it, and the bind refuses a mismatch of either.

## Naming rules

| set | unnamed request | named request |
|---|---|---|
| one unnamed dataset (every pre-collection deployment) | served | `INVALID_ARGUMENT`: "unknown collection X: this coordinator serves one unnamed dataset" |
| named collections, no default | `INVALID_ARGUMENT` naming the collections it contains | that collection, or `INVALID_ARGUMENT` "unknown collection X; this coordinator serves [...]" |
| named collections with `default_collection` | the default | same |

An unnamed request is never routed to "whichever" dataset, and an empty
collection returns no hits — it is empty, not "the other dataset".
The set writes the resolved name into the request before delegating,
and each coordinator checks it again (`admit`): a coordinator built for
collection `a` refuses a request for `b` even when reached directly.

Collection names are printable ASCII without whitespace, quotes,
colons, or slashes, at most 128 bytes: a name that survives a flag, a
directory, and a metric label unchanged.

## Configuration

```toml
default_collection = "opinions"      # optional

[[collections]]
name = "opinions"
nodes = ["10.0.0.1:59300", "10.0.0.2:59300"]
analysis_addr = "http://10.0.0.9:59202"

[[collections]]
name = "dockets"
shard_map = "/etc/pipestream/dockets.shard-map.toml"
control_state = "/var/lib/pipestream/dockets.control.json"
replica_state = "/var/lib/pipestream/dockets.replicas.json"
bm25_k1 = 1.2

[[shards]]
collection = "opinions"
listen = "0.0.0.0:59300"
index = "/data/opinions/shard0.tv"
```

Per collection: `nodes` or `shard_map` (only one), `analysis_addr`,
`bm25_k1`, `bm25_b`, `dense_quality_profile`, `replica_state`,
`control_state`; anything omitted inherits the top-level value. With
`[[collections]]` present the top-level `nodes`, `shard_map`,
`control_state`, and `replica_state` must be absent (there is no unnamed
dataset for them to describe), a node address may appear under one
collection only, `default_collection` must be a declared one, and a
process that also runs the node role must give every shard a declared
collection. The clustered TurboVec backend is one dataset and is not yet
configurable per collection; a configuration with both refuses.

At startup the coordinator asks every node which collection it serves.
A node that reports another collection refuses the start; a node
that does not answer is reported and re-checked by `ClusterHealth`,
which flags such a node in its `error` text rather than counting it.

## The wire

`string collection` on every `SearchService` request (`Search`,
`Bm25Search`, `PhraseSearch`, `HybridSearch`, `VariantSearch`, `Query`,
`QueryStream`, `Aggregate`, `PlanIndex`, the topology and broadcast
calls, `ClusterHealth`) and on the routed mapped ingest's first message
(`RoutedMappedBind`). On nodes: `AddDocumentsRequest.collection`
(refused when it names another node's; written when empty),
`MappedBind.collection`, `ApplyWalBindingRequest.collection`,
`HealthResponse.collection`. On cluster control: every request, and the
plan's `ClusterPlan`, `ClusterNode`, `ShardReplicaState`, and
`PlacementAction` records carry the collection they govern.

`ClusterHealth` with no name on a named set returns one
`CollectionHealth` per collection, each with its own targets, and an
empty top-level target list: row counts are never summed across
collections.

## Cluster control

One `DurableControlPlane` per collection, bound with `with_collection`:
its JSON state records the collection, an older state file without one
is adopted and written, and one that names another collection refuses
to open. `ClusterControlSet` dispatches by name with the same rules as
the search set; each `ClusterControlService` admits only its own
collection, and a shard report whose replica names another collection
refuses. Placement actions are therefore inside one collection by
construction — a split, merge, or move never pairs shards of two.

## The WAL and resharding

`WalManifest.collection` is written at generation creation. A log
written before collections (empty field) is adopted by the node that
opens it under a named collection and written; a log that names another
collection refuses to open, naming both, because a replay of it here
would put that dataset's documents into this one. `reshard` children copy
the parent manifest, so they carry the collection, and merging
generations of different collections is refused as a backend mismatch.

## What this does not do

- No per-request "default to the only collection" when several exist:
  the default is configuration, or the request refuses.
- No cross-collection query, join, or federated score: a request names
  one collection and gets that collection's answer.
- No per-collection clustered TurboVec yet (refused by configuration).
- No collection on the internal node query messages: a node is placed
  in one collection by configuration, verified by health, and never
  written to across the line; the coordinator that fans a query out to
  it is that collection's own.

## Tests

`tests/collections.rs`: two collections on one coordinator with the
same term at different df and different scores, set returns equal to a
single coordinator's (distributed == monolithic per collection), the
naming table (unnamed refusal listing names, default, unknown, unnamed
set refusing names, a single coordinator refusing another name), an empty
collection answering empty, ingest refusal and writing through health,
`ApplyWalBinding` refusal, the WAL manifest (written at creation,
adopted when legacy, refused when foreign), the control plane
(registration and shard reports of another collection refused, plan
records written, foreign state refused at open, the control set's
naming), cluster health listing collections without mixing counts and
flagging a foreign node, and configuration parsing with its refusals.
`src/collections.rs` unit-tests the naming rules.
