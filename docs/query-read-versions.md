# Query read versions

Public `SearchService.Query` captures physical shard versions before selection
and checks them again after all query phases. `QueryStream` uses the same
executor before publishing its final result. Each version contains a mutation
epoch and a random shard-lifetime incarnation, so a restarted node at the same
address cannot impersonate the earlier read even with identical rows and scores.

## Selection and value reads

The coordinator probes every product `NodeService` endpoint, then pins the
successful endpoint for every phase of that query. Configured replica fallback
or hedging can select a reachable copy during this admission probe. Subsequent
phases do not switch copies: losing the admitted copy requires a new whole
query. Candidate `FetchValues` requests carry the admitted versions, which the
node checks under the same read lock as visibility and values.

[Membership reads](membership-visibility.md) also validate their complete version
against the admitted read set before returning any row IDs to the planner.
Filter and vector membership now return versions as well as lexical membership.
[Browse and aggregation](scoped-folds.md) use this same read set, including every
percentile round. Standalone execution admits and validates its own read set.

After execution, fresh probes must match all admitted versions. A scoring retry
that obtains newer statistics cannot turn an earlier selection into a successful
mixed-generation result. Mutation, compaction or node replacement refuses the
whole query with `FailedPrecondition`. On covered shards, the unchanged read
intervals overlap between the two fan-outs. There is no historical image
retention or MVCC: the caller starts again when data changes.

The adapter in `query.rs` is crate-private; public handlers own these checks.
Other public routes and internal node operations retain their existing protocol
guarantees. Versions describe the product shards' physical rows. Independently
hosted vector-provider images still depend on that provider's existing protocol;
these probes do not establish a new cross-provider snapshot guarantee.

## Metadata-only probes

`TermStatsRequest.version_only = true` requires empty terms and fields. Nodes
return only their version, canonical visibility fingerprint and column-known
flags, with `TermStatsResponse.version_only = true`. Corpus counts and lengths
are zero and statistics shares are empty. The node does not walk rows, postings
or tombstones for this probe, and can report its version during a bulk build;
that does not make the unfinished build searchable.

Relays forward the mode, require matching child responses, and publish their
composite child-version token. A changed child lifetime therefore invalidates a
root query or cursor through nested relays. Probe responses cannot populate or
evict corpus statistics in the coordinator cache. Missing mode echoes, mixed
modes and statistics in a probe response refuse rather than masquerading as an
empty corpus. Matching node, relay and coordinator builds are required.

This adds two metadata fan-outs to each query. The admission probe follows the
configured shard deadline and hedge delay. Pending admission probe tasks are
aborted when their owning query future is dropped. No fleet performance claim
has been measured for this change.

## Cursors and streams

Cursor envelope format 2 adds an authenticated digest of the ordered read
versions, separate from the existing query, authority and topology digest.
Static mismatches reject before shard I/O; data mismatches reject after the
admission probes and before selection. A mutation, compaction, restart or
replica change invalidates continuation even when the boundary row and score
look unchanged. Clients echo opaque tokens. The `pqc1:` transport prefix remains;
format 1 envelopes require restarting from the first page.

Stream admission and final version probes run inside `QueryStream.timeout_ms`.
A failed check produces no final revision and no successful completion. Pending
collector progress is not drained after failure. Previously emitted revisions
remain provisional and must be discarded on failed completion. They are not a
snapshot or authorization certificate. The full query profile's total time
includes the probes.

## Authorization and identity boundary

Restricted `Query` and `QueryStream` remain unavailable. Enabling them still
requires mandatory document selection and field use/disclosure checks across
every query shape, intermediate read, response and provisional frame. Physical
version checks support that work but do not authorize any read on their own.
Direct node authentication and remote delegation are separate requirements.

These ephemeral versions and opaque cursor digests are not stable document keys,
conditional-write versions, idempotency records or durability receipts. Stored
index, WAL and source formats are unchanged.

## Evidence

`tests/stats_incarnation.rs` replaces nodes between selection and value fetch,
and during scoring retries on unary and streaming routes. It also exercises
replica admission, an initial probe stalled beyond the stream deadline, cursor
refusal after same-content node replacement, and child replacement through one
and two relay levels. `tests/granted_dictionary.rs` checks cursor refusal after
compaction. Cursor unit tests cover ordered complete claims, rebinding refusal,
opaque digests, malformed envelopes and old-format refusal.

`tests/visibility_stats.rs` compares metadata probes with real statistics across
heap, persisted and segmented stores, deletes, flush, compaction and reopen, and
checks that bulk builds expose only versions. Relay unit tests reject legacy,
mixed and nonempty probe shares. A cache regression proves that even a probe
with a newer incarnation cannot replace or evict existing corpus statistics.

Validation: 458 library tests, 621 integration tests across 108 targets and
12 embedded tests passed (1,091 total). One existing live-sidecar conformance
test remains ignored. All five Android/iOS Rust target checks, tests/examples
compilation, formatting and vendored-proto checks passed. Descriptor comparison
against `5b09cdb` confirms exactly three additive fields and two messages, with
existing declarations unchanged. No fleet benchmark, deployment or device-runtime
test ran; stored index and WAL formats are unchanged.

The full integration pass initially found two compaction assertions expecting
the older boundary error. They now require the earlier data-version refusal,
and the affected group plus every remaining integration target passed.
