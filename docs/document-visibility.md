# Document visibility and term statistics

The 2026-09-06 feature-branch increment adds a document view to the internal
`TermStats` protocol. The subsequent [document-grant increment](document-grants.md)
uses it for public BM25 and dictionary routes over private in-process shards. Other restricted public
routes and network collections refuse until their enforcement is implemented.
A caller-supplied `DocumentVisibility` is not a credential.

## Contract

`TermStatsRequest.visibility` holds a `DocumentVisibility` with one required
`FilterExpr`. An absent visibility retains whole-corpus statistics. A present
message with a missing or malformed filter refuses, including on an empty node.
The existing typed filter contract applies: scalar, map, presence, numeric and
geo predicates, with Kleene logic and only TRUE admitted. Tombstones always
exclude a row.

The visibility changes the statistical population, separately from the user's
query filter. The node computes document count, body and named-field lengths,
and every requested term's document frequency over that view while holding the
same shard read guard as the returned data epoch. As elsewhere in this engine,
a row contributes to BM25 document count when at least one indexed text field
has positive length. Rows with only a title still count; tokenless rows do not.

The response carries `visibility_fingerprint` and `visibility_columns_known`.
For a restricted view, the fingerprint is SHA-256 of the bytes
`protomolt.search.document-visibility.v1\0` followed by normalized protobuf
encoding of `DocumentVisibility`: emit present fields in ascending numeric
field-number order recursively, using the active oneof alternative's number;
retain repeated-value order, omit proto3 implicit defaults and use protobuf's
minimal scalar encoding. The visibility message graph contains no protobuf
maps or extensions; introducing either requires its ordering to be defined.
Generated encoders are not sufficient by themselves: Prost groups oneofs by
their lowest possible tag, so an unsigned `FilterBound` may be written out of
field-number order. The implementation normalizes through its descriptor.
The fingerprint is empty for an unrestricted request.
`visibility::VisibilityScope` validates the filter, derives this identity and
checks the response echo. Missing or different echoes refuse before a relay
merges shares or the cache stores them. This prevents a new planner from
accepting whole-corpus shares from an old node that ignores the new request
field. The digest is neither authentication nor encryption.

Known-column flags follow `filter::walk_leaves` order, even for zero matches.
Relays OR these flags and require every child's fingerprint to match the request.
The root planner must refuse a visibility column unknown across the whole
queried collection. Data epochs retain their physical-state meaning; the
visibility fingerprint is a separate identity. Ordinary field capability flags
retain the relay's existing homogeneous-child requirement.

## Cache and execution

`StatsCache` has separate entries for validated visibility scopes within each
node. Body and fused lookups carry the view's known-column flags. Restricted
responses cannot enter the unrestricted cache, and the reverse is also refused.
Insertion checks response dimensions, a nonzero epoch, and a 32-byte
statistics incarnation. Rebinding an address to another node cannot reuse its
shares, even at the same counter. See [statistics lifetimes](statistics-lifetimes.md). Malformed shares do
not replace valid cached entries. A change to a node's incarnation or epoch evicts all its views;
explicit invalidation also clears every view of that node. At most 32 scopes
remain per node; adding another clears that node's cached views. The existing
per-channel term limits still apply. This is not an authorization-decision cache:
the caller must authorize every operation before using any data-view cache.

A cold restricted request builds a row membership mask and walks admitted row
lengths and requested-term postings. This work is proportional to the affected
rows and postings, not just the number of requested terms. Unrestricted requests
retain their existing fast path and sparse tombstone-length subtraction. No
fleet latency claim is made for restricted views yet.

## Evidence and remaining work

`tests/visibility_stats.rs` compares scoped statistics and BM25 score bits with a
physically restricted corpus. It covers title-only and tokenless rows, empty
views, missing-column handshakes, malformed filters, cache separation, mutation
invalidation, one- and two-level relays, missing echoes, and tombstones, flush,
compaction and reopen under both storage layouts. Unit tests pin an independently
encoded protobuf/hash vectors, including an unsigned oneof bound with its
exclusive flag, and bounded scope churn. The fixtures specify the normalized
wire bytes and hashes independently of the generated encoder.

Private-shard BM25, prefix expansion and suggestion paths now consume
authority-issued document grants.
[Field grants](field-grants.md) now cover the supported private-shard routes.
The remaining query paths still need mandatory visibility in
selection, rescoring, facets, source/projection fetch
and eventual RAG context.
Field-use and field-disclosure checks must precede cache lookup. Public requests
must not override a mandatory predicate or bypass it through a direct node
connection. Those requirements remain open; uncertified restricted routes and
network-backed collections refuse. No persisted index format changes or reindex
are required for this protocol addition.
