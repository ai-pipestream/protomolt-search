# Identity on streamed query revisions

`QueryStreamHit.identity` carries the imported document key, source version and
optional chunk ordinal on provisional and final revisions. Identity is read
under the same admitted shard epochs and incarnations as selection, with the
mandatory document view and field permissions. `doc_id` remains a physical,
generation-local locator. Compaction may renumber it; it does not manufacture
or rewrite the imported identity.

## Explicit identity state

Every revision carries `identity_state`:

- `UNSPECIFIED`: no identity claim, including an older peer or the initial empty
  accepted revision before selection is admitted.
- `RESOLVED`: identity was evaluated for every hit; an absent identity means a
  legacy or vector-only row has no imported key.
- `WITHHELD`: policy denies identity disclosure. Absence says nothing about
  whether the underlying row has a key.

The final revision copies identities from the already authorized terminal query
response. Provisional revisions resolve their candidate IDs through the
[versioned identity fetch](query-result-identity.md). A missing candidate,
missing identity acknowledgment, changed epoch/incarnation or invalid identity
refuses the attempt. The complete admitted read set is checked after each
provisional identity read, including when the read fails. No fresh generation
is substituted for the selection's generation.

Field denial skips the identity fetch and strips any identities before hashing
or sending a revision. The authority permit still checks every public stream
item. Revocation invalidates buffered items and the operation's eventual
completion. Already delivered provisional results cannot be recalled: clients
discard them if the request does not end with successful completion.

## Fingerprint version 2

`content_fingerprint_version` is 2 on new revisions; an omitted value identifies
the legacy v1 hash. The v2 SHA-256 input is the bytes
`protomolt-query-revision-v2` followed by a NUL, then:

1. Phase and identity state as little-endian 32-bit values.
2. Each ordered hit's little-endian row ID (64 bits), score bits (32 bits), and
   rank (32 bits).
3. A one-byte identity presence flag. If present, the raw key length (64-bit
   little-endian), raw key bytes, version (64-bit little-endian), a one-byte
   chunk-ordinal presence flag and, when present, its 32-bit little-endian value.

Either signed zero score hashes as positive zero. This implicit protobuf float
field omits both zeros, so a decoder receives positive zero; hashing the original
negative-zero bits would produce a fingerprint the client cannot reproduce.
Other finite scores retain their f32 bits. This normalization changes the hash,
not the scoring or ranking calculation. A round-trip regression checks the hash
against the decoded protobuf message.

The revision number and scoring fingerprint are excluded so duplicate content
remains detectable independently of delivery timing. Withheld keys never enter
the hash. A key, version or optional ordinal change alters a resolved revision's
fingerprint even if the physical row ID and score are unchanged. This hash
identifies revision content; it is not an authorization token, a cache scope,
a durable document key or a completion certificate. Workspace/collection and
policy context still govern reuse, and success still requires completion.

## Execution and cancellation

The collector runs in an owned task while revision identity reads await their
responses. A waiting identity RPC must not suspend the collector that is still
consuming shard streams. The progress watch channel conflates intermediate
snapshots; emitted revisions remain complete replacements with increasing
revision numbers. Final success still requires ordinary query completion.

The stream deadline covers selection, identity lookups, final read validation
and sending the final revision. Dropping the stream or expiring its deadline
aborts the owned query task. Candidate fetches and flat/fused lexical collectors
also own their shard tasks through cancellation scopes; errors and cancellation
abort outstanding calls. Replies are folded in configured node order, preserving
existing deterministic aggregation behavior. These checks do not claim that
arbitrary blocking work already executing on a node is instantly preemptible.

Identity records retain the fetch's one-million-input-ID and 32-MiB encoded-record
bounds, including at relays and the root. A bound failure refuses the attempt
without truncating its results. Each revision can require an additional
candidate-sized identity fetch and version-validation fan-out. No source payload
or index image is transferred by this read.

## Evidence and remaining work

Public query tests check identities and explicit state across streamed lexical,
dense, Boolean, browse, hybrid and supported collapse results. Field-grant tests
check withheld provisional identities and revocation after keyed provisional
hits. Same-address replacement tests refuse changed generations. Gated RPC tests
prove that provisional and terminal identity reads can both start while waiting,
and that deadline expiry and client drop cancel those requests. Separate gates
check cancellation of flat and fused lexical collectors. Fingerprint tests cover
key/version changes, optional ordinal presence, hidden keys, signed zero and
protobuf round trips.

This adds three fields and one enum in `ai.protomolt.search.v1`; existing wire
declarations are unchanged. No stored format or dependency pin changes. Nodes
and relays need the identity-fetch contract from `40d0df7`; new clients should
inspect identity state and fingerprint version. Remote authorization, identity
on remaining legacy routes, transactional catalog publication and searchable
receipts remain unfinished foundations. This does not certify a catalog version
as published or provide retained snapshot isolation.

## Validation

Validated with main `8106be6` incorporated: 507 library tests, 704 integration
tests across 120 targets, 12 embedded tests and two IVF evaluation tests passed
(1,225 total). The existing live OpenNLP conformance test remains ignored.
Build concurrency was two and test concurrency four.

All five iOS/Android Rust target checks passed, along with locked tests/examples
compilation, formatting, vendored-proto consistency and whitespace checks. The
mobile checks retain three existing relay dead-code warnings; these are cross
compilation checks, not device execution. Descriptor comparison against main
confirmed exactly three additive fields and one enum, with every prior wire
declaration unchanged. These results are local validation, not hosted CI or a
fleet rollout.
