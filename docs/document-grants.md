# Mandatory document grants

The 2026-09-06 feature-branch increments add revisioned document grants to the
public collection authority. Certified execution covers `Bm25Search`, `Suggest`,
`TermSuggest` and [Aggregate](scoped-folds.md) over private in-process shards.
BM25 includes prefix expansion, flat and fused fields, the internal streaming scorer, facets, supported
projections, snippets and score explains. Other retrieval routes, network-node
delegation and RAG disclosure remain required work. [Field grants](field-grants.md)
now apply to these same private-shard routes.

## Protobuf authority contract

`AccessPolicy.format_version = 2` or `3` permits
`CollectionGrant.document_visibility`, a `DocumentVisibility` containing one
required, validated `FilterExpr`. Format 1 refuses this field. Absent visibility
retains unrestricted document access; a present but empty message is invalid.
A restricted grant must include the `SEARCH` action. Any other actions explicitly
listed on the same grant remain independent; this predicate does not restrict
writes or administration. No action is implied by another.

For example, this protobuf text-format policy grants one reader a document view:

```text
format_version: 2
revision: 1
resources { workspace: "phone" collection: "" }
grants {
  principal: "guest"
  workspace: "phone"
  collection: ""
  actions: ACCESS_ACTION_SEARCH
  document_visibility {
    filter { facet { column: "audience" values: "shared" } }
  }
}
```

`PolicyAuthority` validates snapshots before replacement and includes the
predicate in its `AccessDecision` for search. `AccessPermit` validates custom
authorizer decisions and compares the complete decision again before disclosure.
A changed view invalidates the operation, even if a faulty provider neglected to
advance its revision. Principals and workspace ownership still come from the
configured authority. Public request metadata cannot supply or replace a view.
Policy format 3 adds [field restrictions](field-grants.md); format 2 refuses
them instead of silently ignoring their restrictions.

## Enforcement

`CollectionSet` binds the decision to a private coordinator clone before executing
any public route. The clone shares the existing cache but retains its own immutable
visibility. The grant predicate is ANDed with the compiled caller filter; a caller's
OR, negation, or absent filter cannot broaden it. The combined predicate obeys the
existing filter depth/leaf limits, and exceeding them refuses the query.

Both body and fused statistics request the mandatory view and use its validated
fingerprint as the cache key. Known-column flags are merged across the collection,
including when analysis produces an empty selection. An unbound grant column
refuses with a generic error rather than disclosing the policy's column name.
Statistics remain over the entire authorized view, independently of a user's
ordinary query filter. The incarnation/epoch fence remains active on scoring and
on its one retry. Revoked principals are checked before cache access and again
before a response leaves the service.

Facet counts, projections and snippets are produced only for admitted documents.
Explains use scoped corpus statistics. Physical segment counts describe storage
that may include unauthorized rows, so restricted responses set them to zero and
set `Bm25SearchResponse.execution_details_redacted = true`. Those zeros are omitted
measurements, not a claim that no segments were consulted.

## Current boundaries

A restricted search decision refuses `Search`, `PhraseSearch`, `HybridSearch`,
`VariantSearch`, `Query` and `QueryStream` before execution.
The supported dictionary routes and BM25 prefix expansion apply the document view
before counting terms or documents. Configured synonym rules continue to expand
query terms independently of the corpus dictionary.

Restricted execution currently requires local node links, no network fallback,
no clustered vector backend and no live topology source. Node mTLS presently
establishes cluster trust rather than a delegated per-principal grant. Enabling
restricted reads over those listeners would leave a direct-node bypass. Network
collections therefore refuse until authenticated delegation and node enforcement
are implemented. The embedding host must keep node and owner handles private;
re-exporting the raw node service is not an authorized facade.

`EmbeddedSearch::authorized_service(Arc<Principals>)` returns the authenticated
`CollectionSet` over its private nodes. The mobile package re-exports `Principals`,
`PrincipalConfig`, `PolicyAuthority`, `Authorizer` and `CollectionSet`. The existing
owner methods and `search_service()` remain trusted-owner APIs. The facade uses
the same protobuf service and does not add network dependencies. Hosts supply
protobuf policy snapshots through `Principals::with_policy` or their `Authorizer`.
The command-line TOML adapter does not yet configure document predicates.

Before enabling additional routes, carry visibility into every candidate and
fetch boundary, source expansion, aggregations and RAG
context. Field-use and field-disclosure grants apply on the supported
private-shard routes; the additional routes still need their enforcement. Query
cursors
must bind the visibility identity without serializing the private predicate to
callers; the current restricted Query refusal precedes cursor construction.

## Evidence

`tests/document_grants.rs` compares private multi-shard execution with a physically
restricted corpus under flat/fused and unary/internal-streaming modes. It checks
scores, complete hits, explains, snippets, projections, facets, separate cache
views, hidden-corpus mutations, spoofed context, caller filters, revocation and
unsupported route/deployment refusals. The embedded package's `document_grants`
test exercises the exported facade with authentication while its dependency gate
continues to forbid a network stack. No on-device or fleet claim is made.


Validation: 454 library tests, 601 integration tests across 105 targets, and
12 embedded tests passed (1,067 total); the existing live-sidecar conformance
test remains ignored. All five Android/iOS Rust target checks, tests/examples
compilation, formatting and vendored-proto checks passed. Descriptor comparison
against `5e18438` confirms three additive grant/redaction fields and the
corresponding authorization import, with existing declarations unchanged.
These are local checks; no fleet deployment or device runtime test ran. Stored
index and WAL formats are unchanged.


## Permission-scoped dictionaries (2026-09-06)

`Suggest`, `TermSuggest` and BM25 `TermPrefix` expansion now scan the dictionary
of the authorized live view. A term must occur in at least one admitted,
non-deleted document. Its df counts only those documents, and its shard tally
counts only shards with a positive scoped df. Hidden-only and deleted-only terms
are absent from both successful responses and expansion-limit counts. A term
present only in hidden rows has df zero in did-you-mean's `MISSING` mode.
The public tombstone flag is false for these exact live frequencies; physical
tombstone counts are not disclosed.

Both internal dictionary requests carry `DocumentVisibility`. Every response,
including unknown-field and over-cap responses, echoes the normalized view
fingerprint and its column-known flags. Coordinators check this handshake before
merging entries. An old node that ignores the view cannot silently supply an
unrestricted dictionary. The network deployment restriction still applies;
these internal fields describe a trusted planner's request, not a credential.

The node resolves the predicate and scans postings under one shard read guard.
Heap and mapped dictionaries seek to the prefix and iterate in byte order.
Segmented stores merge one cursor per part, eliminating duplicate terms before
counting admitted postings. Retained term storage is bounded by the requested
visible-term cap plus the number of physical parts, rather than the entire
hidden dictionary. A full allowlist still costs one boolean per shard row.

For scoped requests, `max_scan` and `max_expansions` bound the authorized term
set, not physical dictionary work. Determining whether a term is visible requires
walking its postings; exact over-cap counts require visiting all terms under the
prefix even after the cap is exceeded. This is more work than unrestricted
posting-df lookup. Hidden terms must not make a visible query fail its term cap.
No timing noninterference or new cancellation guarantee is claimed. Unrestricted
requests keep their existing posting-df and tombstone behavior.

`tests/document_grants.rs` compares suggestions, did-you-mean and flat/fused BM25
prefix expansions against a physically restricted corpus, including hidden
vocabulary larger than the reader's cap and policy changes before disclosure.
`tests/granted_dictionary.rs` checks heap/tail, sealed, mapped, tombstoned,
compacted and reopened stores, exact visible over-cap counts, duplicate terms
across segments, and view handshakes on empty and unknown-field responses.


Validation: 454 library tests, 603 integration tests across 106 targets, and
12 embedded tests passed (1,069 total); one existing live-sidecar conformance
test remains ignored. All five Android/iOS Rust target checks, tests/examples
compilation, formatting and vendored-proto checks passed. Descriptor comparison
against `2e30bdf` confirms exactly six additive dictionary visibility fields;
existing declarations are unchanged. These are local checks, with no fleet or
device-runtime validation. No stored index or WAL format changes were introduced.
