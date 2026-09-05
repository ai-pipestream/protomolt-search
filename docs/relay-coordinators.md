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

`Health`. The children's reports merged into one: counts summed, the base
slot the lowest child's, dimension and provider identity required to
agree. The parent derives one contiguous id interval from a health
report, so the relay serves `Health` only when its children's slot ranges
are contiguous, and refuses by name, naming the gap or overlap, when they
are not. A relay has no WAL and no live revision; it reports zeros and
`wal_clocked = false` and never invents a watermark for a subtree.

Every other `NodeService` route refuses UNIMPLEMENTED naming the route
and the relay: no ingest, no administration, no snapshots, no
aggregation, no follow-up fetches, no BM25 scoring through this level
yet.

## The epoch token

A node's `TermStats` epoch is its own counter, and a scoring request that
echoes it (`expected_stats_epoch`) is refused with the `stale stats
epoch` prefix when the node's postings have moved since. A relay has no
single counter to report, so it reports a token: a nonzero number bound
to the tuple (collection, map revision, children in shard order, each
child's epoch). The same tuple gets the same token, so a parent's stats
cache keeps hitting while nothing moves; a moved child is a new token.
The relay translates a token back into one claim per child
(`RelayService::translate_epoch`), and the child enforces its own claim.

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
BM25 scoring routes with the root's global statistics and the per-child
epoch claim, follow-up fetches routed by original id, bitmap routes over
sparse ranges, aggregation with the root's fold order preserved, bounded
dictionaries, recursive ingest, and a wider statistics contract past
`u32`. Each is a separate gate with its own equivalence test.

Reference: `tests/relay.rs` (flat, one-level, and two-level execution
bit for bit, ties across relays, an initial floor, the token and the
child's enforcement, a map move, a child error, a parent's stop, the
refusals, and the contiguity rule).
