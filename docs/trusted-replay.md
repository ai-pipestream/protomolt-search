# Trusted replay and durable receiver decisions

The integer-map/derived-column checkpoint exposed a missing contract:
`replication::sync_once` and live-child catch-up send logged documents to
`AddDocuments`, which accepts fresh ingest and refuses carried derived values.
A fingerprint must not become permission to bypass that rule. Source clock,
row count and server address also cannot prove that a retry is the same write.

## Intended path

1. The control authority assigns a source history to a target history within
   one workspace/collection. An assignment binds durable shard incarnations,
   writer epochs, the source WAL generation, a verified baseline snapshot and
   the full index contract. A URL or a statistics incarnation is not a shard
   history. Device-local shards remain ineligible for export or replication.
2. The receiver authenticates the cluster peer and authorizes this assignment
   on every admission. A certificate chaining to the cluster CA proves peer
   authentication only; it does not grant all-shard replay. Policy revocation
   and epoch changes must fence acceptance and application. Unrelated map
   revisions do not rewrite a live assignment or cancel an in-flight query.
3. The receiver durably accepts a frame in an ordered journal. An
   exact retry returns the same accepted frame proof; conflicting content at
   the same sequence refuses. The proof binds the assignment, sequence,
   source clock, previous digest and exact WAL bytes. Selected child streams
   have their own contiguous delivery sequence; the assignment selector must
   justify any gaps in the source clock.
4. A journal consumer validates the source/target index contract and applies
   the already-materialized values, source bytes and identities. It does not
   recompute derived expressions. Analysis, when needed, must use the pinned
   analyzer contract. Application must record the journal identity and frame
   digest at the target mutation boundary, not infer completion from row tips.
5. Accepted, searchable and durable index state are separate receipts. A
   receiver journal acknowledgment proves synced redo history only. A target
   becomes ready for queries or cutover only after application and checkpoint
   proofs reach the required fence. Losing the sender's local cursor cannot
   create duplicates or certify unrelated existing rows.

The application proof must be part of the target's durable mutation history
and checkpoint manifest. Updating a separate cursor after an index flush is
insufficient: a crash between them leaves ambiguous rows. Recovery must use
that proof to complete an accepted pending operation or return its prior
result. It must never add a client-controlled fresh-ingest bypass, remove the
derived stamp, or replace the proof with document counts.

## Implemented receiver journal

`src/replay_journal.rs` and the protobufs `v1/replay.proto` and
`storage/v1/replay_journal.proto` implement the local acceptance boundary.
They expose no RPC, require no network runtime, and do not change fresh
`AddDocuments` or claim that derived network replication is available.

Each journal has one immutable `ReplayStreamBinding`. Creation requires a
new file in an existing durable directory; open requires the existing file
and the exact binding. A lifetime file lock prevents another writer. The
redb cache is bounded to 8 MiB. A frame and its accepted frontier commit in
one transaction with immediate durability, and creation syncs the containing
directory before returning.

Sequence numbers start at 1. Source clocks strictly increase after the
baseline; per-file WAL sequence numbers remain inside the original record.
The first predecessor is the binding digest. Both digests use SHA-256 and
separate versioned domains. The protobuf comments specify the digest encoding:
ascending known tags, shortest varints and omitted defaults for the flat
binding/frame metadata. An independent hash fixture pins that contract.
Adding metadata fields requires a new digest version.

Input records must decode as a `WalRecord` with a sequence, matching clock and
an operation; the original bytes are stored and hashed without re-encoding.
Field order, nonminimal varints and unknown fields survive. Acceptance proves
storage, not that unknown or otherwise unsupported semantics can execute; the
future admission/applier contract must validate those separately. The original
document protobuf also remains opaque inside its source field.

A frame is at most 64 MiB including its encoded journal envelope. Reads have
an explicit count limit (1..1000), byte limit (1..64 MiB) and optional pinned
accepted-sequence fence. An oversized first frame refuses without advancing.
Reusing the returned fence excludes subsequent appends. Open verifies the
header and tip; reads verify each requested frame and predecessor chain.
Missing, noncanonical or inconsistent persisted records are data loss, not
an empty stream to adopt.

An acceptance receipt has `accepted=true`, `durable=true` and
`searchable=false`; `durable` refers to the local receiver journal. An exact
retry returns the original sequence, source clock and content digest even
when the journal has advanced. Only `replayed` changes. The kernel checks the
binding's shape and consistency, not a caller's network authority.

## Persisted admission gate

`ReplayAdmissionPolicy` is a typed decision installed through the trusted local
`publish_authorization` API. It binds one authority incarnation and monotonic
revision to the immutable stream digest, an exact certificate allowlist,
current source/target histories and writer epochs, explicit residency and
an enable decision. This API is not a credential endpoint for senders.

First publication is allowed only before any locally accepted frames. It
atomically upgrades the journal to format 2 and persists the policy. An old
format-1 reader refuses that journal; missing policy or a mismatched format
also refuses. Exact policy retries are idempotent, conflicting reuse of a
revision refuses, and an authority change or older revision cannot overwrite
the decision. Unrelated higher revisions leave an unchanged assignment usable.

`accept_authenticated` takes its peer identity only from tonic's verified TLS
leaf certificate, hashing the DER bytes with SHA-256. The receiver checks the
persisted decision inside the same write transaction that appends the frame.
Revocation and acceptance therefore have one durable order; after revocation
commits, even an exact frame retry on an existing TLS connection refuses. The
plain local `accept` API cannot bypass an installed policy. The local `head`
and `read` methods remain trusted application APIs, not network routes.

A source/target history change, writer-epoch mismatch or device residency
permanently fences this journal. Reopening or publishing a higher policy
revision cannot reactivate that old assignment. Ordinary enable/allowlist
revocation can be reversed only while the original histories, epochs and
server residency still hold. Both ends must explicitly be servers: unspecified
residency refuses and phone-resident shards are ineligible for this replay path.

The gate validates the installed decision; it does not manufacture control
authority or prove that a publisher observed current state. Assignment issuance,
baseline/selector verification, authority delivery and the index application
commit boundary must be connected before exposing a replay RPC. No production
listener, placement worker, fresh-ingest route or fleet process changes here.

## Work remaining before network replay

- Assign and persist durable source/target history identities and baseline
  proofs; do not reuse ephemeral statistics tokens or addresses.
- Feed the receiver admission policy from the trusted published control state.
  The map today has neither durable shard histories nor these assignments;
  lease membership, addresses and statistics epochs cannot substitute for them.
  Publish revocation before activating a replacement writer and fence the
  applier against the same authority. The persisted receiver gate below is
  implemented; that publisher and application integration remain outstanding.
- Persist application identities and frame digests inside target mutation
  history and checkpoints, including the crash window after application but
  before a receiver progress update.
- Expose typed admission/progress RPCs with explicit capabilities. Apply
  bounded backpressure and deadlines and preserve source/field authorization.
- Replace `ReadWal`'s collection of the entire history under the shard lock
  with a pinned bounded reader before using it for sustained catch-up.
- Move both replica and selected-child consumers to these proofs, then test
  source rotation, target replacement, wrong baselines, partial mutation,
  revocation during apply, crash recovery and stable public identities.

The local journal is a component of this path. Its tests are not evidence
that the network, application or cutover steps are finished. The existing
explicit refusal of derived network WAL forwarding remains in force.

## Receiver journal checkpoint 402a64b validation

The broad run passed 524 unit tests, 790 integration tests across 139 targets,
12 embedded tests and two IVF adapter tests (1,328 total). One existing
live-service integration test remains ignored. A subsequent interoperability
correction affected only the new journal module, its tests and protobuf
comments: the journal now retains valid original WAL encodings instead of
requiring a Prost re-encoding match. The focused rerun passed all nine journal
tests, including the two added byte-preservation and independent digest cases.
That brings the tested cases to 1,330; this is a broad run plus the focused
rerun, not a second full-suite run of the final tree.

That checkpoint also passed the five Android/iOS target checks, embedded and
IVF tests, test/example compilation, formatting, vendored-proto checks and
the existing-field descriptor comparisons. All builds and tests used an
8 GiB memory limit with swap disabled and two Cargo build jobs. Source hashes
confirmed that existing production paths did not change during the final
journal correction. These are local results; no fleet rollout or hosted CI
result is implied.

## Admission gate validation

This increment passed 563 local tests: the full unit suite (533, including
nine admission tests), nine journal integration tests, seven security tests,
12 embedded tests and two IVF adapter tests. It also passed all five Android/
iOS target checks, test/example compilation, formatting, vendored-proto checks
and the existing-field descriptor comparisons. The complete integration suite
from the receiver checkpoint was not rerun for this isolated journal change.
Source hashes stayed unchanged during final validation. Every build and test
ran with an 8 GiB memory cap, no swap and two Cargo build jobs.

The admission tests cover exact policy retries, stale/conflicting revisions,
authority replacement, persistent revocation, permanent history/epoch/device
fencing, empty or rotated peer allowlists, concurrent delivery after revocation,
refusal to adopt previously accepted local history, and missing-policy/format
corruption. A real loopback mTLS fixture verifies certificate-based admission:
CA membership is insufficient, metadata cannot supply identity, and revoking
the peer denies further retries over the same established TLS channel. No
production replay RPC or live-fleet claim is implied by that fixture.
