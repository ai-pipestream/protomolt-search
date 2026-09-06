# Candidate lineage reads

`ResolveParents` reads lineage for candidate row IDs. It now applies the
planner's mandatory document view and optional admitted physical version under
one shard read lock. Hidden, deleted and unknown candidates are omitted;
duplicates collapse in request order. A document-restricted read cannot admit a
vector-only row, including through a negative predicate on a missing column.
An unfinished bulk document build refuses lineage reads.

## Explicit key selection

`ResolveParentsRequest.fields` selects `LINEAGE_FIELD_PARENT_ID`,
`LINEAGE_FIELD_GROUP_ID`, or both. An empty list preserves the old request's
meaning of both keys. Unspecified, unknown and duplicate enum values are invalid.
Responses echo the canonical ascending selection even when there are no rows.
Unselected key fields are zero and must not be interpreted as present values;
selected group zero retains its existing meaning for a row without lineage.

The coordinator's `lineage_key` helper requests exactly one key. Query collapse
uses it, so a parent collapse does not fetch or disclose a group key. The
existing `lineage_keys` helper still requests both. Each requested key requires
its own `USE` and `DISCLOSE` grant before node I/O, including for an empty
candidate list. These are exact `parent_id` and `group_id` column permissions;
neither grants the other. The independent raw `DocumentIdentity` disclosure flag
continues to govern that message, not explicitly authorized lineage columns.

## Read and response validation

Every reply includes the physical epoch/incarnation, canonical document-view
fingerprint and authority-column-known flags. A bound public Query's lineage
reads must match the versions admitted before selection. Mutation, compaction
or node replacement fails the read; Query also validates the complete read set
before returning its result. Standalone lineage helpers validate complete
response versions but do not establish a multi-shard snapshot on their own.

The collector validates metadata, the exact field selection, zero values for
unrequested keys, requested candidate membership, and unique row ownership.
Legacy replies or a peer that ignores key selection cannot silently broaden the
result. Missing authority columns across all shards produce a generic policy
error. Candidate predicates are evaluated without a corpus-sized mask.

The clustered parent-collapse path uses the same collector and requests only
parent IDs. It keeps its owner-based candidate batching. Scoped reads contact
all product shards for the mandatory-column handshake even when a shard owns no
requested candidates. Relays still refuse `ResolveParents`.

## Identity and authorization limits

Stored lineage keys survive compaction and reopen. A row without stored lineage
still uses the existing high-bit-tagged row ID as its self-parent and group zero.
That fallback is generation-local and can change when compaction renumbers rows;
it is not a stable public document key. This change protects lineage reads from
using old row locators against new physical data; it does not replace the stable
identity and publication work.

Restricted public Query/QueryStream remain gated while the remaining selection,
scoring and disclosure paths are completed. The new request fields are trusted
planner context, not credentials for direct-node access. Source retrieval,
remote delegated authorization, provisional frames and RAG remain separate
requirements. The original three foundation objectives remain unfinished.

The protocol adds nine fields and one enum under `ai.protomolt.search.v1`.
Existing declarations are unchanged. New coordinators require matching node
responses; old requests with no projection remain supported by new nodes.
Stored index, WAL and source formats are unchanged.

## Evidence

`tests/lineage_reads.rs` covers projection, document views, duplicate and missing
candidates, malformed claims, empty and vector-only shards, compaction/reopen on
heap and segmented layouts, and a wire peer with malformed metadata, extra keys,
unrequested rows or duplicate ownership. Coordinator tests check independent
field actions before I/O, mandatory predicates on ungranted columns and stale
admitted versions. `tests/stats_incarnation.rs` replaces the node at lineage
resolution during a real public collapsed query.

Local validation: 463 library tests, 635 integration tests across 111 targets,
and 12 embedded tests passed (1,110 total); one existing live-sidecar conformance
test remains ignored. All five Android/iOS Rust target checks, tests/examples
compilation, formatting and vendored-proto checks passed. Descriptor comparison
against `ea0484b` confirms exactly nine additive fields and one enum with existing
declarations unchanged. No fleet deployment, device-runtime validation or fleet
latency measurement was performed.
