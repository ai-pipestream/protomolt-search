# Hybrid field and document reads

A composite query's dense field names the durable indexed vector binding. A
field grant and a matching vector dimension do not prove that a shard owns that
binding. The raw `ShardLegs` route now carries an optional `VectorReadContext`
and returns a `VectorReadReceipt`. Local two-level fusion uses the same contract
on `HybridShard`.

The node resolves the field and checks the physical epoch and incarnation under
the read guard that protects both legs. The authority predicate is intersected
with caller filters before either lexical selection or vector scanning. The
caller-filter column handshake stays separate from the authority handshake.
Deleted rows remain excluded, and a vector without document metadata cannot
satisfy a document view. Empty requests and disabled legs still acknowledge the
binding, view and version.

For named or scoped raw-leg and local-fusion reads, the coordinator contacts
every admitted shard, including shards a caller predicate could otherwise prune.
It validates every receipt against the root query's physical read set and requires
the complete durable binding to agree across shards before returning fused
results. Standalone internal hybrid execution captures and validates a read set
when one was not supplied by its caller. Native clustered-vector providers still
lack product-field receipts and refuse scoped execution.

Relays translate both the legacy statistics claim and the context's physical
claim independently. They require each child's receipt, verify field agreement,
and compose the child claims and authority metadata into the relay's
revision-bound receipt. An empty child cannot hide a different binding. Clearing
the legacy statistics claim does not bypass the admitted context's version.
`HybridShard` continues to refuse through relays because its two-level fusion is
partition dependent; the new contract does not change that fusion's semantics.

The four wire fields are additive: `ShardLegsRequest.read_context = 15`,
`ShardLegsResponse.read_receipt = 5`, `HybridShardRequest.read_context = 18`, and
`HybridShardResponse.read_receipt = 4`. Requests without a context retain their
legacy response shape. New scoped consumers refuse a missing receipt from an
older node. The package remains `ai.protomolt.search.v1`; source and index formats
are unchanged, so this protocol change alone needs no reindexing.

## Authorization scope and remaining work

Private public Query execution with field grants covers RRF, score blend,
decomposed scoring and cascade. The first two use the raw-leg receipt; decomposed
and cascade also use the existing named candidate-score and scan receipts.
The coordinator regressions exercise all four strategies, a disabled dense leg,
an authorized source-path alias, and incompatible durable bindings of the same
dimension.

Document-restricted public Query remains gated. The selection and candidate authority checks here do not complete its
public execution-metadata and quality-policy audit. Direct-node authorization, network delegation, and
relay composition of streamed scan receipts remain separate unfinished work.
These contexts are trusted-planner metadata, not credentials.

`tests/hybrid_read_views.rs` compares a two-level relay's raw leg scores against
flat leaf reads under identical global statistics. It covers private, deleted
and vector-only rows, conflicting caller filters, empty requests, empty children
with incompatible bindings, and stale nested claims. Coordinator tests additionally
cover empty shards and authority selection in both raw-list and local-fusion
execution.

## Validation, 2026-09-06

The source passed 489 library tests, 684 integration tests across 119 targets,
and 12 embedded tests: 1,185 passed, zero failed. The existing live OpenNLP
Unicode conformance test remains ignored. All five Android/iOS compile checks,
the tests/examples build, formatting, vendored-proto identity and whitespace
checks passed. Descriptor comparison against `8101d10` preserves every existing
declaration; the four read-context/receipt fields are additive. Source, build
and test hashes were unchanged throughout the full run. These are local tests
and mobile compile checks, not phone-runtime or fleet measurements.

## Decomposed lexical admission and candidate rescores

Decomposed search now obtains its initial lexical list from `ShardLegs` with
an empty vector payload and the same mandatory view as its vector pass. A
private lexical winner can no longer occupy the retained list and erase a
visible document's lexical-rank provenance. The regression compares complete
hits and scores against a physically restricted corpus with private rows that
would otherwise dominate lexical selection. Scoped shard admission also uses
the same unpruned read set as the vector stream.

`Bm25RescoreRequest.visibility = 12` applies the trusted planner's document view
before candidate scoring. Responses carry the physical epoch, incarnation,
view fingerprint and known-column flags in additive fields 3 through 6, even
with no candidates or no postings. Candidate bookkeeping is proportional to the
request. Invalid BM25 parameters refuse before scoring. The request's analyzer
and physical claims remain checked under the scoring guard.

Cascade, decomposed scoring and legacy boosts share a coordinator rescore
validator. It requires field Use on body and score-stage inputs, checks the
response against both the global-statistics claim and any root read set, and
rejects foreign, duplicate and nonfinite scores. Scoped or staged rescores
contact empty candidate owners to validate authority and stage columns. Relays
include every child in the receipt and reject child scores outside the IDs sent
to that child. All-visible responses retain their existing score semantics.

New coordinators require these receipts from every scoring node. Earlier nodes
cannot silently satisfy that contract. Upgrade nodes and coordinators together;
these five additive protobuf fields do not change any stored source or index
format. No new route or public document-query permission is enabled here.
