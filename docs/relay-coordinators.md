# Relay coordinators

A relay is a coordinator process that presents itself to its parent
coordinator as one shard. It serves the node-facing surface over its
children, which are ordinary shard nodes or further relays, so a root
that fans out to a few hundred targets can stand over thousands of shards
with a level in between. Nothing new is spoken between levels: the parent
calls `NodeService` routes on the relay exactly as it calls them on a
node, and the relay calls the same routes on its children through the
coordinator's fan-out.

This is the restricted, read-only relay the
[2026-09-05 review](scale-out-coordination-review-2026-09-05.md) cleared.
The [proposal](scale-out-coordination.md) records the choices behind it.

## Running one

```sh
protomolt-search --role=coordinator --relay \
  --nodes=http://leaf-a:50051,http://leaf-b:50051 \
  --coord-listen=0.0.0.0:50060
```

`--relay` (`PIPESTREAM_SEARCH_RELAY`, `relay = true` in the config file)
takes the coordinator role only and one unnamed collection: a relay is a
dedicated endpoint for one collection, never a multiplexer of several.
The coordinator listener serves `NodeService` instead of `SearchService`,
with the parent-facing UDP signal lane on the same port (signed with
`--udp-hmac-key`, unsigned on loopback only, as on a node). A shard map
(`--shard-map`) works as it does on any coordinator, placement codes
included.

The parent lists the relay as a shard. Under a placement tree the relay
serves one leaf, and the parent's map entry carries that leaf's code.

At startup the relay checks its children and refuses to serve when their
slot ranges are not contiguous (see `Health`).

## The routes it serves

`StreamSearch`. The relay opens one stream per child, forwards every
child batch upward untouched (the packed records keep their global ids,
original scores, and order, so boundary ties survive), and forwards the
parent's floor raises and cancellation to every child on the relay's own
signed UDP sessions plus the gRPC twin. Queues are bounded; a
`grpc-timeout` on the parent's request ends the attempt when it passes.
The relay's terminal summary is issued only after every child's: sums of
the children's counts, the union of their column-known flags (the root's
typo rule reads them), and the one scoring fingerprint every child
reported. A child error, a child stopping before completing, a missing
fingerprint, or fingerprints that differ fail the attempt with an error;
the parent's `Stop` yields `completed = false`. The relay keeps no heap
and raises no floor of its own, because `StartStreamSearch` carries no k.

When `identity_limits` is present, the relay also forwards the
[snapshot-bound identity exchange](dense-identity.md). Child readiness includes
captured ID ranges; these route only the parent's selected winners back to the
same child streams, including through nested relays. Each unused child receives
an empty selection. The relay preserves caller order, validates each terminal
certificate and bounds the combined identity response. Legacy requests retain
the candidate/summary protocol. A disconnect or deadline releases child readers
even while delivery to the parent is backpressured.

`TermStats`. The children's shares are summed with checked arithmetic. A
`u32` document frequency that would overflow refuses by name; that is the
statistics contract's ceiling, and a relay does not hide it. Field
capabilities must agree across children: `known`, `positions`, and
`sentences` are one boolean each on the wire, a phrase needs positions
everywhere and a typo check needs the field somewhere, and a mixture has
no faithful spelling in one, so the relay refuses it by name. The epoch
the relay reports is a token (below).

When `TermStats.visibility` is present, every child must echo the requested
[document visibility](document-visibility.md) fingerprint. A missing or
different echo refuses, including a response from an older node that ignored
the restriction. Visibility-column known flags are ORed across children; these
are separate from the homogeneous field capability flags above. Counts and term
frequencies describe the requested view, while the epoch still names the
children's physical data state.

`Health`. The children's reports merged into one: counts summed, the base
slot the lowest child's, dimension and provider identity required to
agree. The parent derives one contiguous id interval from a health
report, so the relay serves `Health` only when its children's slot ranges
are contiguous, and refuses by name, naming the gap or overlap, when they
are not. A relay has no WAL and no live revision; it reports zeros and
`wal_clocked = false` and never invents a watermark for a subtree.

The keyword leg. `Bm25Query`, `Bm25PhraseQuery`, `Bm25QueryStream`,
`Bm25Rescore`, and `ShardLegs` (the raw per-leg lists behind global-rank
and score-blend fusion) forward to every child with the root's global
statistics, field order, and score stages unchanged and the parent's
epoch claim translated into each child's (below). Candidate batches and
the children's cutoff raises go up untouched, monotone; the parent's
raises and its stop go down; the relay's completion certificate follows
the last child's, with the children's counts summed and one fingerprint
required of all. Each child's terminal response merges by value into the
one the parent reads as a shard's: the local top-k lists concatenated
(the parent's global merge picks from the union), facet counts summed by
value and range buckets by position after checking each returned field, key
and exact interval against the request. Range count overflow refuses. Typed
bounds remain intact through nested relays. Column-known flags are ORed so the
root's typo rule sees the subtree, segment counts added with a check. A
facet no child knows stays unknown rather than refused, because the root
decides over shards this relay does not see. Hits pass through whole,
explain and identity included; a rescore routes each candidate id to the
child whose slot range holds it and refuses an id in none.

Two request shapes are refused by name on these routes: `stats_fields`
(a column statistic folds floating-point partials in the root's shard
order, and a relay would change it) and `cardinality_fields` (an exact
cardinality is a union of values, not a sum). A phrase through a relay
whose children disagree on positions is refused when the root fetches
statistics, as any mixed capability is.

One ordering note. The flat unary fan-out orders equal scores by shard
index and then id; a relay's children share one shard index at the
parent, so equal scores across those children order by id, as a
monolith orders them. The streaming route and the fused routes order by
id and by competition rank in both shapes, and the exactness gate covers
them bit for bit.

Every other `NodeService` route refuses UNIMPLEMENTED naming the route
and the relay: no ingest, no administration, no snapshots, no
aggregation, no dictionary routes, no follow-up fetches by id, and no
per-shard fusion (`HybridShard`) or unary vector search (`SearchShard`)
through this level. The cascade's vector gate takes `SearchShard`, so a
cascade does not run through a relay yet; its rescoring half does.

## The epoch token

A node's `TermStats` epoch is its own counter, and a scoring request that
echoes it (`expected_stats_epoch`) is refused with the `stale stats
epoch` prefix when the node's postings have moved since. A relay has no
single counter to report, so it reports a token: a nonzero number bound
to the tuple (collection, map revision, children in shard order, each
child's epoch). The same tuple gets the same token, so a parent's stats
cache keeps hitting while nothing moves; a moved child is a new token.
The relay translates a token back into one claim per child
(`RelayService::translate_epoch`), and the child enforces its own claim:
every keyword-leg request the parent sends with the token as its
`expected_stats_epoch` reaches each child with that child's recorded
epoch instead, and a child whose postings moved since refuses with the
`stale stats epoch` prefix, which the relay keeps at the front of the
error so the parent's retry rule (invalidate, refetch, repeat once
unclaimed) fires as it does for a node.

Tokens are `incarnation << 32 | counter`: the incarnation is taken at
process start, so a token from before a restart is unknown afterwards
rather than reused. The relay retains the newest 256 tokens. An unknown
token, and a token issued under a map revision that is no longer current,
refuse with the `stale stats epoch` prefix, and the parent refetches.
Token 0 is no claim and translates to no claim on any child.

## Map interface

The relay never reads its shard map from a file or from the
coordinator's authority directly. It consumes a `MapSource`
(`src/relay.rs`):

```rust
pub trait MapSource: Send + Sync {
    fn current(&self) -> MapSnapshot;          // one reading, stamped
    fn changes(&self) -> watch::Receiver<u64>; // wakes on a new revision
}
pub struct MapSnapshot {
    pub control_revision: u64,
    pub topology_generation: u64,
    pub map: Arc<RelayMap>,                    // routes with codes, placement tree
}
```

Every decision pins the snapshot it was made under and refuses by name
when that revision is no longer the current one: a stream that is still
waiting on children when the map moves is cancelled and fails with the
two revisions named; a `TermStats` or `Health` answer computed under an
older map is not served; a token carries the revision it was issued under
and refuses afterwards. A parent retries under the current map.

Today the only source is `CoordinatorMapSource`: the coordinator's
file-polled shard map, with the topology generation as the control
revision and the coordinator's publication watch as the change
notification. Each reading is one frozen snapshot of one publication. A
replicated control state implements the same two methods behind the
relay without touching relay code.

## What is not composed yet

The review names what a general relay needs beyond this scope and why:
follow-up fetches routed by original id, the unary vector search the
cascade gates on, per-shard fusion, bitmap routes over sparse ranges,
aggregation with the root's fold order preserved (column statistics and
cardinalities included), bounded dictionaries, recursive ingest, and a
wider statistics contract past `u32`. Each is a separate gate with its
own equivalence test.

Reference: `tests/relay.rs` (flat, one-level, and two-level execution
bit for bit, ties across relays, an initial floor, the token and the
child's enforcement, a map move, a child error, a parent's stop, the
refusals, the contiguity rule; and for the keyword leg: lexical queries
on the unary and streaming routes with explain and facets, global-rank
and score-blend hybrids, the stale-epoch refusal end to end and the
refetch that restores it, a phrase under mixed positions, a stop
mid-stream, and a rescore routed by id).
