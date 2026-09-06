# Document-authorized Query and QueryStream

The public collection facade admits `Query` and `QueryStream` with a mandatory
document view on private in-process shards. Document views and optional field
grants compose: the caller does not need use permission for the authority's
predicate columns, and a user filter cannot replace that predicate.
Network-backed collections, clustered vector backends and live-topology
coordinators still refuse restricted requests before node execution. Direct
node and cluster-control authorization remain separate work.

This enables the query shapes the executor already serves, including lexical,
dense native/FP32, Boolean and filter selection, supported hybrid strategies,
boosts, stored scorers, sorting, collapse, aggregates, projections, highlighting
and unary explanation. It does not add support for shapes the ordinary query
contract refuses. In particular, explanations remain unary-only and Boolean
membership does not acquire an ANN interpretation.

## Enforcement and disclosure

Query-wide field admission runs before physical reads. Selection and candidate
scoring carry the authority view independently of caller filters and bind the
actual indexed field and physical epoch/incarnation. Read validation, field
redaction and document execution disclosure run before the final response.
The underlying route contracts are recorded in [hybrid read views](hybrid-read-views.md),
[vector scan views](vector-scan-views.md), [field grants](field-grants.md),
[execution disclosure](query-disclosure.md) and [error disclosure](error-disclosure.md).

Provisional revisions contain authorized candidate row IDs and scores, with the
phase and replacement-revision semantics of the existing stream. They are not
the terminal boosted, projected or collapsed answer. A missing authority column
cannot produce any hits, including provisional hits. An empty accepted revision
may precede an execution refusal. Failures cannot carry a successful response
or allow later stream items through the collection wrapper.

The authority permit checks the decision before each item leaves the public
stream. Policy replacement invalidates a pending stream, including already
buffered final results. It also invalidates old cursors. A fresh operation uses
the new document view and its own statistics and membership. Already delivered
provisional results cannot be recalled; clients discard an uncertified result
when its operation fails. This contract does not claim timing noninterference.

Field-use-only grants still omit raw identity and stored scorer dimensions from
representatives and inner hits. Document restrictions additionally withhold
physical execution details. `doc_id` remains a generation-local locator.
[Final query responses](query-result-identity.md) now resolve imported identity
under the same authority and version. Provisional identity and transactional
source publication remain unfinished foundations.

## Evidence

The public regression compares answers against separately indexed visible-only
shards, with and without field grants. It checks lexical and stored scoring,
highlighting, explanation, Boolean and negative-only membership, browse sorting,
projections, aggregate results, collapse groups and boosts. Every stream
revision is checked for membership, increasing revision and final-result
agreement. Simple scored leaves must produce real provisional hits.

Coordinator tests exercise document-scoped native and FP32 dense queries, RRF,
blend, decomposed and cascade hybrids, Boolean dense selection and lexical
selection through unary and streaming execution. Other regressions check
pagination, field redaction inside document-restricted groups, unbound authority
columns, network refusal, cursor invalidation, policy changes without a revision
bump, and policy replacement after provisional hits have reached the client.

The admission change adds no protobuf field, route or stored format and requires
no reindex. Broader protobuf shape coverage, remote authorization, RAG context
and stable write/publication identity are not declared complete by this change.

## Validation, 2026-09-06

Against incorporated main `c3783a2`, the final source passed 504 library tests,
699 integration tests across 120 targets, 12 embedded tests and two isolated
IVF-provider tests: 1,217 passed, zero failed. The existing exhaustive live
OpenNLP conformance test remains ignored. All five Android/iOS compile checks,
the tests/examples build, formatting, vendored-proto identity and whitespace
checks passed. Protobuf definitions, codegen and dependency manifests/locks are
unchanged from that main checkpoint. The 344-file source/build/test/script/lock
manifest remained unchanged through the final validation run.

A previous library assertion expected blanket document-query denial. It now
checks that both public query routes retain the exact document view and field
restrictions; the complete library suite passed after that update. These are
local tests and compile checks, not hosted CI, device-runtime or fleet results.
