# Main reconciliation, 2026-09-06

This feature branch incorporates main through `5fdedf324f190d83434f950af0f8d3c994163d73`.
It includes the placement trust checks, the expanded relay read routes and
shard-side Boolean evaluation. The public package remains
`ai.protomolt.search.v1`. This note describes the reconciliation; it is not a
fleet deployment record or a claim that the foundations project is complete.

## Contracts to retain on subsequent merges

- Every read uses a complete physical epoch/incarnation claim. Retrying with
  fresh statistics retains the new claim; it never retries without a fence.
  The public query's admitted read set still bounds all phases and cursor use.
- Authority views are independent of caller filters. Apply them before Boolean
  algebra, scans, scoring, aggregation and dictionary collection. Validate their
  fingerprints and known-column flags before consuming a response.
- Dense field names refer to the durable mapping's indexed name. Source-path
  aliases cannot substitute for it. Native and FP32 reads, including empty
  requests, acknowledge the actual binding; all participating nodes must agree.
- Relays compose child epoch/incarnation claims into their existing revision-bound
  tokens. New bitmap and candidate-score routes return the same metadata as direct
  nodes. Empty candidate children still participate in the binding/version check.
  A placement-pruned filter-bitmap child supplies a version-only probe. Dictionary
  merges validate every child's view before returning entries.
- `EvaluateBoolean` carries a document view, a complete expected physical claim,
  per-lexical-leaf analyzer fingerprints and per-dense-leaf field names. Its
  response carries a `VectorReadReceipt`. Relays validate and compose these
  receipts before publishing the merged candidates. Boolean aggregation through
  a relay remains unavailable, as on main.

## The protobuf tag collision

Main deployed `QuantileCountsRequest.boolean = 7`. This feature branch had
previously published `visibility = 7`. Main's field 7 is retained; visibility
moves to field 10, while the feature branch's epoch/incarnation fields remain
8 and 9. There is no dual interpretation of tag 7.

Feature-branch binaries and generated clients built before this reconciliation
must be rebuilt together. The feature's old QuantileCounts wire contract is
incompatible. Main's deployed declarations and tags are preserved. The namespace
and source/index storage formats do not change in this reconciliation.

## Boolean set semantics

The shard evaluates concrete per-leaf bitmaps. Lexical and filter leaves contain
live document rows; dense leaves contain live vector rows, which may include
rows without document metadata. A mandatory document view excludes such
vector-only rows. Negative-only groups start from the live document universe.

A dense leaf is not the universal set: a document without a vector can qualify
through another SHOULD clause and survive MUST_NOT dense. A dense clause scores
only the final members that also belong to that leaf. Missing products for those
members are errors. Optional dense clauses cannot remove otherwise qualifying
members or add their signal/provenance to non-members. The single-dense fast
path is used only when all selected members belong to that dense leaf.

This retains main's shard-local wordwise evaluation and candidate-scoring
improvements. Membership bitmaps do not return to the coordinator. The parallel
Boolean audit should supply independent cases for nested groups, minimum-should-
match, vector-only rows, vector-less documents and native/FP32 parity.

## Vector scan admission

Named `SearchRequest.field` and `DenseQuery.field` select the durable vector
binding. Classic and streaming fan-outs admit a `ReadReady` frame from every
participating node before using any floors, candidates or provisional results.
Waiting readers apply backpressure after the first frame. Missing, duplicate,
malformed, stale or incompatible receipts fail the attempt and cancel peer work.

Scoped scan contexts through relays still refuse before dialing children; the
new relay candidate-score receipts do not imply streamed receipt composition.
Restricted public Query/QueryStream remain gated pending the complete disclosure
and field-grant audit. Direct-node authentication/delegation, stable identity on
all public hit forms and the remaining source durability work are separate.

## Integration order

Keep this feature branch isolated while the combined tests run. The dense
membership audit and bounded re-placement split can continue on their own
branches. Recheck live main immediately before publishing this branch and again
before its eventual merge to main. Do not infer fleet readiness from a source
merge or a launched restart; the other task owns that rollout and readiness
verification.
