# Authority views in vector scans

`StartShardSearch.read_context` and `StartStreamSearch.read_context` opt into a
`VectorReadContext`. It names the indexed vector field, the authority's document
view, and an optional admitted physical epoch/incarnation. This extends the
[field-aware membership and candidate-score contract](vector-field-reads.md) to
the scan kernels that supply top-k, live floors and streamed candidates.

The node resolves the field from its durable mapping, validates the physical
claim and intersects the authority predicate with the caller's filters under
the scan's read lock. A missing or malformed binding, wrong field, malformed
view or stale version refuses before selection and before any response payload.
The view's known-column handshake remains separate from the caller's filter
handshake. A vector without document metadata cannot satisfy a document view,
including a view that tests for a missing value. Tombstones remain excluded.

The response begins with exactly one `ReadReady(VectorReadReceipt)` frame.
It carries the actual binding, physical version, view fingerprint and known
columns. It precedes floors, packed candidates, identity frames and terminal
completion. It is also emitted when the eligible result set is empty. The
same read guard protects all scan work, so the receipt does not merely describe
a preceding health probe.

Classic solo scans, coalesced scans, parent-collapsed scans and streaming scans
use this contract. Each coalesced job has its own context, allowlist and receipt;
a refused job cannot supply a floor or alter another job's authority view.
The kernels and floor mathematics are unchanged. Collapse uses the maximum over
a parent's surviving chunks, and stream identity resolution retains the scan's
eligibility mask, so a later request for an excluded identity refuses.

## Parent-map consistency

The old parent cache used row count as its validation rule. Its build acquired
a read lock and published after acquiring a separate write lock, with no check
for a replacement in between. A same-sized replacement could therefore reuse
or publish old lineage. A scan could also obtain a parent map, release its lock,
and then scan a different generation of the same size.

Parent maps now carry a `StatsClaim`. A cached map must match both that claim
and the requested row count. Publishing a newly built map verifies the claim
under the write lock; taking the scan's final read lock verifies it again.
A concurrent change refuses the scan, including an unscoped legacy scan, rather
than returning parent groups from a different generation. This fixes cache
consistency; it does not make the existing fallback parent IDs stable public
document keys. The shared mutation version conservatively invalidates this cache
even when a mutation does not change lineage, so the next collapse may rebuild
it. No fleet performance claim is made for that cost.

## Consumers and compatibility

The context and receipt are trusted-planner metadata, not credentials. A
consumer opting in must require ReadReady first, validate the field, authority
view, column handshake and admitted version, and only then consume scan output.
It must require every participating shard to finish. Receiving a receipt alone
is not successful scan completion or durable storage acknowledgment.

The coordinator now supplies read contexts for named or authority-scoped vector
scans. `SearchRequest.field` and `DenseQuery.field` carry the indexed name through
selection and candidate scoring. A barrier validates all initial receipts before
sharing floors, merging candidates or publishing provisional results. Readers
stop after their initial receipt until the whole read set is admitted; failures
cancel peer work. Duplicate or unsolicited receipts also fail legacy streams.

Private Query/QueryStream enforce document and field grants together; see the
[document-query contract](document-query-authorization.md) for admission evidence. Parent collapse requires both Use and
Disclose on `parent_id`. Relays still refuse supplied scan contexts before contacting children;
composing candidate-score receipts does not yet implement streamed receipt
composition. See the [main reconciliation](main-reconciliation-2026-09.md) for
current relay support and the QuantileCounts wire incompatibility. Direct-node
authorization and network delegation remain separate work.

Requests without `read_context` retain their existing response sequence, without
ReadReady. The change adds two messages and four fields, including two oneof
alternatives, with existing declarations unchanged. Old nodes cannot satisfy an
opt-in consumer by silently ignoring the context. No index, WAL or original
source format changes; this increment alone requires no reindexing.

## Evidence

`tests/vector_scan_views.rs` compares each scan mode's scoped scores against
unrestricted reference scores, with caller predicates that conflict with or
attempt to widen the authority view. It exercises single-image storage and a
sealed segment plus active tail, deleted/private/vector-only rows, empty results,
malformed contexts, stale versions and the ordering of the initial receipt.
It also proves that streamed identity resolution refuses an excluded row and
that a relay refuses the context before dialing a child.

The node's `vector_scan_view_tests` places public, private, missing-value and
invalid-field jobs in one actual coalesced pass. A separate regression test
replaces lineage without changing row count, checks fresh parent resolution,
and refuses stale cache publication and use. Existing node-loopback, streamed
identity and streaming-search tests exercise the unchanged legacy protocol.

Validation on 2026-09-06 passed 472 library tests, 653 integration tests across
115 targets and 12 embedded tests (1,137 total), with one existing live-sidecar
conformance test ignored. All five Android/iOS compile checks, tests/examples
compilation, formatting and vendored-proto identity passed. The descriptor
comparison against `b1cd31d` confirms exactly the two new messages and four
additive fields, with existing declarations unchanged. Source/build/test hashes
were unchanged through validation. No fleet benchmark, deployment or physical
device-runtime test ran.
