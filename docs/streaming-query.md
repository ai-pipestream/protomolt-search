# Public streaming query contract

`SearchService.QueryStream` is the streaming form of `SearchService.Query`.
It does not define a second search language or a weaker result. It executes the
same `QueryRequest`, publishes complete replacement snapshots while certified
collectors are running, then returns the byte-identical unary `QueryResponse`
inside one terminal completion message.

## State machine

Every accepted call emits these shapes in order:

1. Revision 1, phase `ACCEPTED`, with no hits.
2. Zero or more provisional replacement revisions. Exact lexical collectors
   use phase `LEXICAL`; exact vector collectors use `DENSE`; composite routes
   may additionally publish a `HYBRID` snapshot.
3. One `FINAL` replacement revision containing the authoritative ordered hits.
4. Exactly one `QueryStreamCompletion`.

Revision numbers strictly increase. A revision is a full snapshot, not a
patch, so a slow client can discard every snapshot older than the greatest
revision it has seen. Ranks are one-based and match list order. The content
fingerprint hashes the phase and ordered `(document id, score bits)` rows;
identical final results therefore have a retry-stable fingerprint even when
timing changes which provisional revisions are observed.

The terminal success contains the ordinary `QueryResponse` and identifies the
last revision. That response is the source of truth for projections, score
dimensions, boosts, profile data, paging, and any other phase that is not safe
to expose provisionally.

For a dense leaf using `DENSE_SCORE_MODE_FP32_RERANK`, `DENSE` revisions are
the provider-native candidate order and remain provisional. The `FINAL`
revision is the FP32-reranked order from the fixed `selection_k` pool. Clients
must not compare those two score magnitudes as if they were one score space.

## Completion and failure

Hits are usable as a complete answer only when the terminal message says
`completed=true`. Success requires every configured shard to finish its exact
collector and provide a compatible scoring fingerprint. EOF is not a
completion certificate.

Deadline, missing shard, transport failure, analyzer drift, malformed request,
or an incomplete provider certificate produces one well-formed terminal
message with:

- `completed=false`;
- no `QueryResponse`;
- the canonical gRPC code and message.

The optional `timeout_ms` covers the complete operation from acceptance
through final response construction. A client that drops the response stream
cancels the execution future and its in-flight shard work; the server does not
manufacture a terminal event for a client that is no longer listening.

## Collector guarantees

Public streaming forces the exact candidate protocols even if the ordinary
service configuration selects a unary route:

- lexical shards emit packed BM25 candidates and adopt the coordinator's
  monotone inclusive floor;
- vector shards emit provider candidates above the same kind of proven floor;
- the coordinator owns the authoritative global heap;
- every successful shard terminates with `completed=true` and a non-empty
  score-space fingerprint.

The final revision and terminal response use the documented deterministic
order. Tests compare their document ids, f32 score bits, and ranks with unary
`Query`, repeat requests to pin final fingerprints, and exercise invalid
requests, missing shards, deadline cancellation, and client cancellation.

## Current provisional coverage

Single lexical and dense selections expose live provisional heaps. Composite
routes always produce the exact terminal response and may expose only accepted
and final revisions when the existing fusion path cannot publish a meaningful
intermediate order. Parent collapse currently remains on its exact unary route,
so the OpenSearch challenge records its first-hit and terminal times as equal.
Phrase-aware BM25 also remains unary until its strongest-nonoverlapping-phrase
score has an equivalent candidate certificate.

This is deliberate. A route gains provisional results only when their meaning
and final completeness can be stated precisely; the service never labels a
partial shard set as an exact result.

## Client rule

A client can render the newest provisional revision for responsiveness, but it
must keep that presentation marked provisional. Commit the result, cache a
page token, or make an automated decision only from a successful terminal
response. On `completed=false`, discard every provisional revision from that
request.
