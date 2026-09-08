# Network source-write authorization

This document records the locally validated `DocumentWriteService` receiver and
the constraints that remain for later integration. Validation of the receiver
change above `8b5e215a` is recorded below; it does not establish managed-owner
or fleet readiness. The synchronous local boundary is described in
[source access](source-access.md), and the larger ownership design remains in
[source authority activation](source-authority-activation.md).

## Implemented local receiver boundary

`document_write.proto` now declares a unary `DocumentWriteService` with two
operations. `GetWriteTarget` returns the explicitly configured local workspace,
collection, and catalog history identity. `AcceptDocument` accepts one logical
source transaction and returns the existing `DocumentWriteReceipt`. Target
discovery grants no ownership or source access, and a successful acceptance
still reports `searchable=false`; publication is a separate operation.

`DocumentWriteServiceImpl` is a programmatic adapter over a caller-supplied set
of `AccessControlledCatalog` instances. Construction rejects an empty catalog
set and duplicate collection bindings. The embedding application must have
opened each catalog under `Admin` permission before supplying it. The service
does not discover, provision, activate, transfer, or replicate catalogs.
`into_server` is the required registration path because it applies the
configured maximum request size to tonic decoding as well as execution
admission.

Both RPCs authenticate at the receiver and require current `Ingest` permission
for the exact configured collection. Acceptance passes the receiver's
authenticated principal into `AccessControlledCatalog::accept`, so retry
identity is scoped to that actor in the catalog. Network writes require contract
version 2 and an exact 16-byte `history_id`; the catalog compares that identity
with its persistent history before applying or replaying the operation. The
existing catalog transaction preserves the version precondition, operation
record, actor attribution, source bytes, accepted sequence, and exact retry
receipt durably. A reused actor operation ID with changed input refuses; an
exact retry returns its recorded decision.

Persist the complete original version-2 envelope for retry, including its history
identity. Discovering a replacement history and reusing the operation ID there
would be a new operation, not recovery of the old decision. An `Ingest` grant
does not imply `Admin`, search disclosure, or permission to export the source.

The blocking worker owns the request admission and an `AccessPermit` clone.
`AccessControlledCatalog::accept` pins that permission inside the worker through
the synchronous local commit. Dropping or cancelling the RPC future does not
cancel `spawn_blocking`, release the worker's pin, or release its admitted
capacity before the worker finishes. Consequently, a lost reply leaves the
outcome unknown and the caller must retry the same actor operation under current
permission. After the worker returns, the RPC checks current permission again
before disclosing either the write target or receipt. Revocation after a commit
can therefore suppress the reply without undoing the durable accepted decision.

## Receiver admission limits

The constructor requires explicit limits:

- `max_request_bytes` is 1 byte through 64 MiB and caps the complete encoded
  unary request. `into_server` also applies it as tonic's decoding limit.
  Tonic's decoder returns `OUT_OF_RANGE` for an oversized network message before
  the handler runs. The handler independently checks the decoded envelope and
  returns `RESOURCE_EXHAUSTED` naming `max_request_bytes`, including for direct
  in-process trait calls.
- `max_pending_bytes` is at least one maximum request and at most 256 MiB. Each
  admitted request holds permits equal to its protobuf `encoded_len` while its
  blocking worker is queued or running.
- `max_in_flight` is 1 through 1024. Each admitted target lookup or write holds
  one shared service slot; full capacity refuses instead of waiting.
- A write also consumes the authenticated principal's existing request and
  one-document ingest admissions. Service clones share the byte and operation
  semaphores.

These limits cover admitted unary request envelopes and queued/running receiver
work. They do not claim a bound for total process memory, HTTP/2 transport
buffers across all connections, source export, projection work, or the legacy
client-streaming ingest paths.

## Remaining integration work

The service is not wired into CLI configuration or `src/main.rs` server
construction. An embedding application can construct it directly, but there is
no managed selection of a source owner, catalog history or incarnation, no
current-authority routing or activation, and no remote authority protocol or
revocation completion barrier. The local resource binding is not proof of
exclusive distributed ownership.

The service accepts source history only. It exposes no source export or raw
history read, creates no projection, writes no search row, and activates no
serving index. Publication and restoration keep their existing permission and
authority requirements.

Legacy `RoutedIngestMapped` remains a separate raw index-ingest path. Its public
facade authorizes at the coordinator, buffers the input into per-shard batches,
and sends ordinary mapped-ingest calls without passing the source-catalog actor
transaction to the receiver. Shard calls may complete independently. This work
does not repair that authorization boundary, make the stream atomic, or turn its
row-count response into a durable logical-source receipt.

Future integration must preserve the implemented receiver properties while it
adds explicit owner/history/incarnation routing, activation, and remote
authority behavior. It must keep accepted, durable, searchable, and disclosed
states distinct; retain actor-scoped retries after lost replies; reject stale or
fabricated targets; and make partial routed progress explicit. Until those links
exist and are validated, this is a local receiver increment rather than
completion of A4.

A coordinator-side permission guard held across an RPC cannot substitute for
receiving-side enforcement: a timeout or cancellation can release that guard
while the remote worker still commits. A signed policy snapshot or numeric
revision also cannot prove that revocation has not completed elsewhere. A remote
authority provider needs a committed decision or an explicit revocation protocol
whose completion waits for admitted operations. Providers unable to pin a
decision must continue to refuse that operation.

The eventual source-owned projection journal may publish an already-authorized,
durably accepted version after the accepting principal loses permission. That
does not authorize a new source write or grant query, field, or RAG disclosure.
Publication must consume the trusted accepted decision, never caller-fabricated
source metadata. Raw stream integration additionally needs bounded total
buffering, backpressure and durable partial-progress receipts; a per-message
limit or a long-lived policy guard cannot supply those guarantees.

Required integration evidence includes revocation before admission with no new
version or retry record; revocation after commit with retained history but
current disclosure checks; lost and cancelled replies with exact actor-scoped
recovery; stale owner/history/incarnation refusals; and explicit partial progress
across multiple owners. Local receiver tests prove only the local portions of
these requirements.

The JSON control-state failure path has now been fault-injected: an error after
rename left disk on the new decision while memory returned to the old decision
and remained usable. The branch's [control recovery change](cluster-control.md#persistence-failures-and-recovery)
closes the shared instance after an ambiguous publication and provides strict
existing-state reopening; its validation is separate from the receiver gate
below. This is not transactional managed-owner storage. Managed ownership still
requires the accepted control storage/recovery design, and missing authority
state must never bootstrap a replacement history.

## Local receiver validation

The full gate on the frozen receiver change above `8b5e215a` passed:

- 688 library tests and 150 integration targets: 903 top-level tests passed,
  plus the source-access worker subprocess's one test. The existing native
  OpenNLP conformance test remained ignored because it requires a sidecar.
- 13 embedded tests, two IVF adapter tests, nine protobuf comparator tests,
  and all five mobile compilation checks.
- Tests/examples compilation, formatting, vendored-proto identity and diff checks.
- A descriptor comparison against `8b5e215a`: removing exactly the added service
  and four messages leaves the original descriptor set unchanged. The other
  20 proto files and the original protobuf fixtures remain byte-identical.

All 650 tracked and untracked input hashes remained identical during the gate.
The scope enforced `MemoryMax=8G`, `MemorySwapMax=0` and two Cargo build jobs.
Peak memory reached 8 GiB; the recorded limit events were 42,738 and socket
throttle events 865, with zero swap, OOM or OOM kills. This was correctness
validation under a memory cap, not a latency measurement. Raw records use the
`/tmp/psearch-source-service-full-*` prefix and scope
`psearch-source-service-full-1788850197250727962.scope`.

The seven new receiver tests cover real gRPC discovery, exact protobuf payload
preservation through reopen, actor-scoped retries, permission/history/size
refusals, and shared byte/operation admission. Deterministic handler tests
separately prove refusal before admission, receipt suppression after an admitted
commit, and durable retry recovery after dropping the actual handler future.
They do not claim to have observed HTTP/2 cancellation delivery or a remote
authority's revocation protocol.
