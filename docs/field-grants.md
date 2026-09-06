# Field grants

Policy format 3 adds an exact field allowlist to the public search authority.
`CollectionGrant.field_permissions` and `AccessDecision.field_permissions` use
the same protobuf `FieldPermissions`. Absent means unrestricted fields. Present
with no grants means no field access. Formats 1 and 2 reject field permissions;
format 3 can combine them with the format-2 mandatory document view. Future
restrictions require a new policy format so older engines refuse them.

Private in-process `Bm25Search`, `Suggest`, `TermSuggest` and
[Aggregate](scoped-folds.md) enforce both document and field grants.
`Query` and `QueryStream` now enforce field grants on private in-process shards
when the decision has no mandatory document view. Document-restricted public
queries and network-backed restricted collections still refuse until the
remaining execution-metadata, selection and delegation work is certified.
This does not complete source-fetch or eventual RAG authorization.

## Names and actions

A `FieldGrant` names one exact engine field or column. There are no wildcards or
parent-path inheritance rules: `body` does not grant `body.bigrams`, and a grant
for a projection alias does not authorize its input column. A map-column grant
covers every key in that column. Indexed names come from the index definition;
this contract does not interpret a protobuf source path as a wildcard over its
materialized projections. Per-map-key restrictions are not implemented.

Actions are independent:

- `FIELD_ACTION_USE` allows reading a field in selection, ranking, filters,
  expressions, aggregates or dictionaries. Matches, ranking and scores can
  reveal information about fields the caller is allowed to use.
- `FIELD_ACTION_DISCLOSE` allows content, values, occurrence offsets, detailed
  explanations and dictionary entries to leave the service. Explicit operations
  that both read and disclose require both actions.

Only the `SEARCH` capability is narrowed. Ingestion and administration explicitly
granted on the same resource remain independent. Empty field names/actions,
duplicate fields/actions and unknown actions are invalid policies. Custom
`Authorizer` decisions receive the same validation before execution.

For example, this search grant permits body details, permits a ranking input
without disclosing its value, and permits a projected color:

```text
principal: "reader"
workspace: "work"
collection: "docs"
actions: ACCESS_ACTION_SEARCH
field_permissions {
  grants { field: "body" actions: FIELD_ACTION_USE actions: FIELD_ACTION_DISCLOSE }
  grants { field: "boost" actions: FIELD_ACTION_USE }
  grants { field: "color" actions: FIELD_ACTION_USE actions: FIELD_ACTION_DISCLOSE }
  disclose_document_identity: false
}
```

`disclose_document_identity` independently controls raw `DocumentIdentity`
metadata, because its exact document-key bytes can contain source data. It is
false by default inside a present field policy. It does not alter keys, source
versions or stored bytes. Generation-local row locators and scores remain in
search hits; stable public identity/publication remains a separate foundation
workstream, and withholding a raw key does not implement a new opaque identity.

## Execution and disclosure

The coordinator binds the authority's field policy to a private execution clone.
Before statistics/cache access or dictionary fan-out, it checks every selected
BM25 field, score-stage column, geo column and compiled user-filter leaf for
`USE`. Compiled projection leaves are checked under every branch, including map
reads; aliases cannot hide a forbidden dependency. Projection inputs, facets,
ranges, column statistics, cardinality, requested snippet fields, suggestions and
prefix expansions require both actions. Explicit explanations also require
`DISCLOSE` on every scoring input. The exhaustive request bindings make a new
public request field a compile-time audit point.

The mandatory document predicate is authority-owned context. It can use a column
that the reader cannot name in a user filter or projection; applying that policy
does not grant access to its dependencies. User-field checks run before that
predicate is combined with the caller's compiled filter.

A phrase's auxiliary bigram field is a separate indexed field. The planner does
not probe or use it without `USE`; when an explanation is requested it also
requires `DISCLOSE`. It uses the authorized body's positions when available,
or reports that it cannot execute the phrase with the available payload. An
auxiliary field is never implicitly granted because its name starts with `body`.
The existing phrase-route score semantics remain observable through the route
metadata when that metadata may be disclosed.

Ordinary BM25 hits always carry occurrence details internally. For a field with
`USE` alone, the public response omits those details while retaining its ranking.
It also omits disallowed automatic synonym/routing details and raw identity.
`Bm25SearchResponse.field_details_redacted` reports such omissions. Explicitly
unauthorized projections, snippets, explanations, facets and dictionaries fail
with `PERMISSION_DENIED`; they do not silently return empty values. No policy
field names or allowlist are included in the denial message.

Statistics remain keyed by document view and actual queried field. Field checks
precede their reuse, and no result cache can authorize a request. The complete
`AccessDecision` is checked again before disclosure. A field-policy change
invalidates a computed result even if a faulty provider forgot to advance its
revision. Query cursors bind the complete authority decision; a policy change
requires a fresh first page. Authorized streams revalidate before disclosure,
including an event already produced before revocation.

## Query and QueryStream

Query admission walks the entire selection tree, including negative Boolean
clauses, nested composites and boost queries. It checks every input before
pinning shard read versions. A disabled scorer dimension still needs `USE`,
because disabled values participate in missing-value validation and provenance.
Sorting and collapse reveal their keys, so both actions are required. Every
projection and aggregate expression uses its compiled input columns, not its
output alias. Named dense selection and boosts require the actual indexed field;
an empty name or source-path alias cannot borrow another field's grant.

Stored-value scorer dimensions normally expose raw per-document values and their
normalized contributions. With `USE` alone they still contribute to ranking,
but the public response omits their `DimensionScore` entries. Explicit explain
requests require `DISCLOSE` for every scoring input, including stored dimensions.
`QueryResponse.field_details_redacted` signals withheld automatic details; when
it is set, the disclosed dimensions may be insufficient to reconstruct a score.
`USE` permits scores and ranking derived from a field; this is not a promise
that the caller cannot infer values through those allowed operations.

The final disclosure pass covers representatives and all collapse inner hits.
It removes raw document identities unless separately granted, filters automatic
dictionary expansions and propagates redaction from the lexical adapter. It
preserves row locators, scores, ranks, cursor boundaries and permitted values.
QueryStream provisional revisions carry only locators and scores; its successful
completion uses the same disclosure pass as unary Query. A denied field request
cannot emit provisional hits. Policy replacement invalidates an outstanding
stream or computed response even when the new policy is more permissive.

## Evidence and limits

`tests/field_grants.rs` checks policy versions, empty/exact grants, independent
actions, query-only ranking, identity omission, fused field details, body versus
auxiliary phrase fields, all current BM25 input categories, CEL aliases and map
reads, dictionary disclosure, document/field composition, warm-cache denials,
network refusal and policy changes before disclosure. Query cases compare scored,
Boolean, browse, boosted and collapsed answers against unrestricted execution;
they check raw-dimension and inner-hit redaction, explicit-explain denial,
negative-clause/disabled-dimension admission, cursor invalidation and streaming
revocation. Coordinator tests cover named native/FP32 selection in classic and
streaming scan modes. The embedded facade test
uses format 3 and verifies the same field denial without network dependencies.

These grants do not configure the CLI's TOML adapter, authorize direct node or
cluster-control listeners, implement per-key map grants, or finish the other
retrieval routes. Hosts provide protobuf policies or a trusted `Authorizer`.

The [membership boundary](membership-visibility.md) checks user filter inputs
and lexical body use before planning, independently of authority-owned predicate
columns. A field-restricted vector membership call refuses until the dense
contract names an explicit indexed vector field. This does not enable restricted
public Query or QueryStream.

The [candidate value-fetch boundary](candidate-fetch.md) also checks an
authority-bound coordinator's field policy before fetching projections and
stored-value score dimensions. This prepares the later query phases; it does
not enable restricted public `Query` or `QueryStream`.

Validation: 454 library tests, 610 integration tests across 107 targets, and
12 embedded tests passed (1,076 total); one existing live-sidecar conformance
test remains ignored. All five Android/iOS Rust target checks, tests/examples
compilation, formatting and vendored-proto checks passed. Descriptor comparison
against `7e9496b` confirms exactly three additive fields, two field-policy
messages and one action enum; existing declarations are unchanged. These are
local checks, with no fleet deployment or device-runtime validation. Stored
index and WAL formats are unchanged.


[Candidate lineage reads](lineage-reads.md) now apply the mandatory document view
and the query's admitted physical versions. Parent and group keys have separate
field projections and use/disclosure checks. This prepares collapsed query
execution; restricted public Query and QueryStream remain gated.


Validation of the public-query increment passed 487 library tests, 682
integration tests across 118 targets, and 12 embedded tests (1,181 passed,
zero failed). The existing exhaustive live-OpenNLP conformance test remains
ignored because it requires its sidecar. All five Android/iOS compile targets,
tests/examples compilation, formatting and vendored-proto identity checks pass.
Descriptor comparison against `1565d07` preserves every existing declaration;
the only wire addition is `QueryResponse.field_details_redacted = 12`.
Source hashes were unchanged throughout the test and compile gates. These are
local tests and mobile compile checks, not a fleet rollout or phone-runtime run.

The subsequent [hybrid read integration](hybrid-read-views.md) closes the raw-leg
selection seam: a permitted vector field must match the actual durable binding
on every shard, including disabled dense legs and empty children. Public
composite-query regressions cover RRF, blend, decomposed scoring and cascade.
