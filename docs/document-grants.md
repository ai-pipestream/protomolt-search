# Mandatory document grants

The 2026-09-06 feature-branch increment adds revisioned document grants to the
public collection authority. Its certified execution surface is `Bm25Search`
over private in-process shards. This includes flat and fused fields, the internal
streaming BM25 scorer, facets, supported projections, snippets and score explains.
It is an intermediate foundation increment: other retrieval routes, network-node
delegation, field grants and RAG disclosure remain required work.

## Protobuf authority contract

`AccessPolicy.format_version = 2` permits
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
Future field restrictions require another policy format version; they must never
be encoded as silently ignored additions to format 2.

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
`VariantSearch`, `Query`, `QueryStream`, `Aggregate`, `Suggest` and `TermSuggest`
before execution. `Bm25Search` also refuses explicit term-prefix expansion,
including prefixes on fused fields, because its dictionary protocol does not yet
carry the mandatory view. Existing supported synonym expansion uses configured
rules, not a corpus dictionary. These refusals are coverage boundaries to remove
as enforcement lands, not the target product scope.

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
fetch boundary, term dictionary access, source expansion, aggregations and RAG
context. Field-use and field-disclosure grants also remain open. Query cursors
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
