# Identity on final query results

The public `Query` response and terminal `QueryStream` response resolve imported
`DocumentIdentity` for every returned hit and collapse inner hit. This covers
lexical, dense, Boolean, browse and supported hybrid strategies, including
sorting, scoring and projection adapters that previously dropped identity.
The opaque document key, source version and optional chunk ordinal come from
the source archive. They are never derived from a physical row ID.

The coordinator resolves the selected page under the same admitted shard epoch
and incarnation as selection, then validates the complete read set again before
publishing. An identity already supplied by selection must agree. Missing,
duplicate, unrequested, malformed or changed identity records refuse the query.
Compaction can renumber `doc_id`; it cannot change the imported identity attached
to that document. This check is a current-version read, not retained snapshot
isolation or a retry against a newer generation.

## Protobuf contract

`FetchValuesRequest.include_identities` requests candidate identity evaluation.
`FetchValuesResponse.identities_included` explicitly acknowledges it, including
an empty shard. `identities` contains one `CandidateIdentity` for each owned,
live, visible requested row. An absent `identity` inside a returned record means
that row has no imported identity. An omitted record means the row was not
served. These records are independent of projected-value rows, so a vector-only
row can explicitly have no identity even when it has no column store.

Nodes evaluate identities under the value-fetch read lock and mandatory document
view. Relays forward the request, verify each child's acknowledgment and read
receipt, check identity ownership against the child's range, and preserve the
records under their composite receipt. The root requires complete coverage for
the hits it is about to return. Old peers that omit the acknowledgment are
refused, rather than silently treating every row as unkeyed.

Identity fetch accepts at most 1,000,000 input IDs. Nodes, relays and the root
limit accumulated encoded identity records, including repeated-field framing,
to 32 MiB. Exceeding a bound refuses the operation without truncating its answer;
transport limits also apply. The limit does not cover unrelated projection
payloads. Identity resolution adds a candidate-sized fetch fan-out to nonempty
public queries that permit identity disclosure.

## Authorization and remaining work

The authority's document view is independent of user filters and applies to the
identity read. Field permissions that deny identity disclosure skip this extra
fetch and remove identities already supplied by selection, including inside
collapse groups. Public stream authority checks still run before delivery.
These internal request fields do not authenticate callers. Remote delegation
and direct-node authorization remain separate requirements.

[Provisional `QueryStreamHit` revisions](query-stream-identity.md) now resolve
imported identity with explicit disclosure state under the same admitted read
context. Their row IDs remain generation-local locators. Legacy rows without imported keys
remain explicitly unkeyed. Server-side catalog publication, stable identity for
all legacy public route shapes, conditional projection transactions and
searchable receipts remain unfinished. No identity claim is inferred from
unchanged text or from a passing ranking test.

The wire additions stay in `ai.protomolt.search.v1`. They change no index, WAL or
source archive format and need no reindex. Deploy matching query coordinators,
relays and nodes together; the new identity request deliberately detects older
peers. Fleet rollout and measurements are owned separately.

## Evidence

`tests/query_api.rs` exercises keyed and legacy rows through public unary and
terminal streamed browse, lexical/dense Boolean, lexical, dense and hybrid
queries, and supported collapse shapes. `tests/candidate_fetch.rs` checks live
views, explicit absence, opt-in behavior, policy-column knowledge, stale epochs,
node lifetimes and malformed or older wire peers. The relay fetch test compares
actual keyed and unkeyed rows through one and two levels with flat children.

The existing online-compaction test now checks public lexical, browse and
Boolean result identities against independent logical source expectations in
its observation pass, including compacted images, segmented catalogs, reopened
nodes and WAL replay. Existing field-grant regressions contain private imported
keys and verify disclosure denial, including document-restricted groups.

`tests/stats_incarnation.rs` replaces the node at the same address immediately
before the identity fetch. Unary and streamed queries refuse without a terminal
result. The same test retains the lexical-retry case: even when that delegate
retries successfully, final read-set validation rejects the changed generation.
Final validation also runs when identity resolution itself fails.

## Validation, 2026-09-06

The final source passed 505 library tests, 701 integration tests across 120
targets, 12 embedded tests and two isolated IVF-provider tests: 1,220 passed,
zero failed. The existing live OpenNLP conformance test remains ignored. All
five Android/iOS compile targets, the tests/examples build, formatting,
vendored-proto identity and whitespace checks passed. Mobile compilation keeps
the three existing relay dead-code warnings. These are local tests and compile
checks; hosted CI, device runtime and fleet measurements were not inspected.

Descriptor comparison against main `20ac5f1` found exactly three additive fields
and the two-field `CandidateIdentity` message; every existing declaration is
unchanged. Dependency manifests, locks and codegen are unchanged. The tracked
source and validation manifest stayed unchanged throughout the final run.

The full rerun followed a correction to the query error path: an identity-fetch
failure must still pass through whole-read-set validation. Test fixtures were
also corrected to use supported collapse/pool combinations and to select both
keyed and legacy visible rows through the first relay. No production refusal
was relaxed to make an unsupported query shape pass.
