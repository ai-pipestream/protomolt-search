# Candidate value reads and selection versions

Public query execution can select rows in one phase and fetch their stored
values in another. A row ID alone is not proof that the second phase is reading
the same document: compaction renumbers rows, a mutation can replace a row, and
a node can restart at the same address. A document view applied during selection
also does not authorize an unchecked later read.

`NodeService.FetchValues` now accepts a planner-owned `DocumentVisibility` and
an optional complete statistics claim (`expected_stats_epoch` plus
`expected_stats_incarnation`). The node validates the view, then checks the
claim, resolves the predicate and evaluates candidate values under one shard
read lock. Only live candidates admitted by the view produce rows. Duplicate
candidate IDs collapse; IDs outside the shard are ignored. The predicate is
evaluated on the requested candidates, without allocating a corpus-sized mask.

The response carries the actual shard epoch and incarnation, the canonical view
fingerprint, and the view's column-known flags, even when no rows are returned.
A shard with no column store returns an empty share and false column flags.
An unfinished bulk build refuses value reads. Missing predicate values retain
the filter language's three-valued semantics.

The coordinator verifies the view echo, complete version, projection metadata,
row types and requested ID membership before publishing its merged values.
Duplicate row ownership and nonnumeric stage contributions refuse. A mandatory
column unknown across all shards refuses with a generic policy error. Authority
field checks run before fan-out: projections require both use and disclosure on
every input leaf; stored-value score dimensions require use. Explanation
disclosure remains the query planner's responsibility.

`CoordinatorServiceImpl::fetch_values_at` requires a complete claim for every
node in the pinned coordinator's order. A mutation, compaction or new shard
lifetime refuses instead of fetching current values under an old selection.
The coordinator also checks each response against the expected claim, so an
older or faulty peer cannot silently ignore the request fields. Returned
`FetchedValues.epochs` records the versions read. The existing `fetch_values`
entry point uses the query's admitted claims when called by the public query
executor; standalone callers without a bound read set retain an unpinned read.
Both enforce any authority view bound to their coordinator. An unscoped call
with no projections or stages skips fan-out and returns no version claims.

## Integration boundary

The public query executor now binds these reads to its admitted versions and
validates the whole query again before completion; see
[query read versions](query-read-versions.md). Private in-process restricted
`Query` and `QueryStream` now enforce the mandatory view and field grants;
see [document-authorized queries](document-query-authorization.md). Read-version validation rejects changed data;
it does not retain a historical snapshot.

The internal visibility and version fields are not credentials. Direct node
authorization and remote delegation remain separate work. Relays compose
`FetchValues` with the child read receipts. These ephemeral versions are not
durable document identity or idempotency receipts. The optional
[identity evaluation](query-result-identity.md) returns imported stable keys
separately under those receipts.

Use matching coordinator and node builds: older fetch responses omit version
metadata and are refused, including for unrestricted projected queries. The
change adds seven protobuf fields and changes no stored index, WAL or original
source format.

## Evidence

`tests/candidate_fetch.rs` covers live visibility, hidden and deleted rows,
duplicate/out-of-range IDs, empty responses, malformed views and claims, node
lifetime replacement, complete multi-node claims, and a wire peer that omits or
misstates response metadata or returns unrequested/duplicate rows. The
coordinator unit test composes mandatory document and field policies, including
an authority predicate on a column not granted to the caller and denial before
an empty-candidate fetch.

`tests/granted_dictionary.rs` also exercises value reads across heap, sealed
segments and live tails, flush, delete, compaction and reopen. Claims from before
compaction and restart are refused; fresh scoped reads retain the visible values.

Validation: 455 library tests, 614 integration tests across 108 targets, and
12 embedded tests passed (1,081 total); one existing live-sidecar conformance
test remains ignored. All five Android/iOS Rust target checks, tests/examples
compilation, formatting and vendored-proto checks passed. Descriptor comparison
against `0841f6a` confirms exactly seven additive candidate-fetch fields with
existing declarations unchanged. No fleet deployment or device-runtime test ran.
Stored index and WAL formats are unchanged.

The first full library run exposed a metrics test that assumed its process-wide
ingest counters started at zero. It now checks increments from the observed
baseline, allowing other tests to ingest concurrently. The complete suite above
passed after that test-isolation fix.


[Candidate lineage reads](lineage-reads.md) now apply the mandatory document view
and the query's admitted physical versions. Parent and group keys have separate
field projections and use/disclosure checks. These checks also protect the [public document-query paths](document-query-authorization.md).
