# Main reconciliation, 2026-09-06

Checkpoint `1565d07` incorporated main through `5fdedf324f190d83434f950af0f8d3c994163d73`.
It includes the placement trust checks, the expanded relay read routes and
shard-side Boolean evaluation. The public package remains
`ai.protomolt.search.v1`. This note describes the reconciliation; it is not a
fleet deployment record or a claim that the foundations project is complete.

The later field-authorization branch now incorporates main `7b0faa9`, including
spill staging, relay fetches/folds and the bulk-analysis end-of-stream fix.
The route limitations below describe the
original checkpoint; current relay support is recorded in
[relay coordinators](relay-coordinators.md), and the later hybrid read contracts
are in [hybrid read views](hybrid-read-views.md).

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
improvements. Membership bitmaps do not return to the coordinator. The independent
fix `7c44e282352aa4ccaf9ff3d1f608ffd9833c1b3c` is incorporated with the authority
and receipt checks retained. Both Boolean membership and `ResolveVectorBitmap`
use actual provider image ranges, not a prefix of the logical row extent.
`tests/boolean_segment_gaps.rs` covers a document-only sealed segment between
two vector-bearing segments plus a vector-bearing tail. Selecting FP32 scoring
does not redefine membership; missing sidecar data must fail instead of silently
dropping native vector rows.

The segmented-gap regression constructs the provider and lexical shard directly.
It does not certify reopening a mixed catalog with its whole-shard FP32 sidecar:
the existing recovery code concatenates vector-bearing parts, while the sidecar
shape check requires positional alignment. Supporting FP32 recovery across such
gaps needs an explicit slot mapping or aligned storage, with recovery tests.

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

**Accepted on 2026-09-06.** Checkpoint `1565d07` is accepted for main after
independent review. The reconciliation hold is lifted. The integration task
may advance main to that checkpoint, merge spill staging `39e9c0f` and relay
folds `1f6d576`, validate the combined tree and push both remotes, then build
and roll the fleet and execute the planned year-band split and partitioned
compaction using the operational readiness and cutover checks. Those steps do
not wait on the later field-authorization branch. The earlier checkpoint test
results below do not substitute for testing the combined merge or verifying
fleet readiness.

The combined tests passed; publishing this feature branch is separate from
merging it to main. The bounded
re-placement split remains on its own branch. Recheck live main immediately
before publishing this branch and again
before its eventual merge to main. Do not infer fleet readiness from a source
merge or a launched restart; the other task owns that rollout and readiness
verification.

Fork E is parked at `eb45b61` on `feat/relay-folds-2026-09`; it is not
incorporated here. Its coordinator fold helpers have no route integration or
behavior tests. The resumed work must translate full epoch/incarnation claims,
forward authority views and named field bindings, and validate each child's
receipt before folding or disclosing values. Rebase those helpers onto this
reconciled branch before route integration: retain unsigned scalar types,
full-width integer partials and response type-agreement checks. Empty-id
children still participate
where field/view knowledge is required. `FetchValues`, `ResolveParents` and
`BrowseShard` are public Query dependencies; their current refusals matter.
`HybridShard` remains used by the partition-dependent two-level fusion mode,
so its relay refusal stays.

Treat Fork E's floating-point findings as proposed acceptance criteria until
exercised by tests. Preserve leaf fold order for a bitwise claim, or document and
test a numerical bound for regrouped double sum, mean and variance. A topology
change must not silently weaken the public aggregation contract. Exact integer
and set/count shapes also need overflow, schema, missing-child and multi-level
composition tests before their routes are enabled.


## Validation

The reconciled source passed 486 library tests and 677 integration tests across
118 targets, followed by 12 embedded tests: 1,175 passed, zero failed. The
existing `native_matches_opennlp_contract` integration test remains ignored;
it requires `OPENNLP_ANALYSIS_ADDR` and scans every Unicode scalar. Integration
targets ran in groups of six with `CARGO_BUILD_JOBS=2` and four test threads.

Descriptor comparison against main `5fdedf3` preserves every deployed protobuf
declaration. Comparison against the previous feature checkpoint `67c0290`
allows only the documented QuantileCounts visibility tag move. Source hashes
were recorded before the full run and checked afterwards.

All five embedded mobile compile targets pass: `aarch64-linux-android`,
`x86_64-linux-android`, `aarch64-apple-ios`, `aarch64-apple-ios-sim` and
`x86_64-apple-ios`. These are compile checks, not executions on phones.
The tests/examples build check, formatting check, vendored-proto identity
check and whitespace check also pass. No fleet operation or measurement was
performed by this task.
