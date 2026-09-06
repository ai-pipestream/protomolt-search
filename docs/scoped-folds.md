# Scoped browse and aggregation

Private in-process `SearchService.Aggregate` enforces the authority's mandatory
document view and field permissions. The collection facade rechecks the complete
access decision before returning results, including changes to a predicate or
field policy that did not advance the authority revision. Network-backed
restricted collections remain unavailable until delegated node authorization is
implemented.

## Selection and disclosure

The coordinator checks the caller's compiled filter and geo columns for `USE`
before contacting nodes. Every input to an aggregate, histogram or percentile,
and the group-by column, requires both `USE` and `DISCLOSE`: these operations
return information derived from the values. The expression walk includes every
conditional branch and map read. A constant count has no field dependency, but
still counts only authorized documents.

Authority predicates remain separate from user filters on the wire. The node
intersects them under its shard read lock; the caller cannot replace the
predicate or widen it with an OR filter. Candidate ID lists further restrict
that intersection. Authority predicates may use columns the caller cannot name.
Their unknown-column errors remain generic. Scoped fan-outs contact every shard
to validate these columns, including shards a user predicate would prune.

The shared browse helper applies the same document view. It checks `USE` on
user filters and the lexical body, and both actions on sort columns because it
returns sort values. This covers internal browse execution; restricted public
`Query` and `QueryStream` still require the remaining retrieval and disclosure
work and remain gated. No new public browse route is introduced.

## One physical read set

Standalone aggregate and browse execution use the same admission and final
validation helpers as public Query. They capture each shard's epoch and lifetime
incarnation, pin the admitted endpoint, and require those versions throughout
execution. An enclosing Query supplies its existing read set and owns the final
validation instead of nesting additional probes.

`BrowseShard` and `AggregateShard` requests can omit both version fields for an
independent node read. A half-specified or stale version fails. `QuantileCounts`
requires the complete version admitted by the initial aggregate pass, even on an
empty shard. Its count-below rounds cannot combine initial extrema with a later
index generation. Final validation also covers constant-valued percentiles that
need no count-below round. A mutation or same-address node replacement requires
restarting the entire operation; there is no retained historical snapshot.

Every response carries its physical version, canonical visibility fingerprint
and authority-column flags, even when no rows match or the shard has no document
store. The coordinator validates these before merging rows, aggregate partials
or percentile counts. Missing or incorrect metadata is a failed precondition.
These checks preserve the existing deterministic shard merge order and exact
nearest-rank percentile algorithm.

The protocol adds 21 fields to six existing messages. Nodes, coordinators and
clients must be upgraded together: a legacy coordinator cannot execute the new
required quantile version protocol, and a new coordinator refuses legacy read
responses. Stored index, WAL and source formats are unchanged. No corpus rebuild
is required by this change alone.

## Evidence and remaining work

`tests/scoped_folds.rs` exercises document-view intersection, candidate-pool
restriction, physical versions, malformed and missing claims, and empty shards
on the heap and segment layouts. `tests/field_grants.rs` compares public Aggregate
with a physically restricted corpus, tests field-input denials with a warm cache,
and rejects a changed field policy before disclosure. `tests/document_grants.rs`
also rejects a changed document view before Aggregate disclosure.

`tests/stats_incarnation.rs` replaces a node with the same rows at the aggregate
pass, first and subsequent quantile rounds, and final validation. Coordinator
unit tests check browse fields before I/O and reject malformed metadata for all
three response types before merging it.

Remaining authorization work includes the other retrieval routes, provisional
query frames, source and lineage disclosure, remote delegation and eventual RAG.
Physical read versions are not stable document identity, conditional write
versions, persistent idempotency or durability receipts. Those retain their own
contracts and unfinished requirements.

Local validation: 462 library tests, 630 integration tests across 110 targets,
and 12 embedded tests passed (1,104 total); one existing live-sidecar conformance
test remains ignored. All five Android/iOS Rust target checks, tests/examples
compilation, formatting and vendored-proto checks passed. Descriptor comparison
against `32f32d3` confirms exactly 21 additive fields with existing declarations
unchanged. No fleet deployment, device-runtime test or fleet latency measurement
was performed.
