# Network ingest authorization: next integration constraints

Proposed integration constraints, read against the foundations branch at
`202446c` and the actor-catalog extension under validation. Network enforcement
is unfinished. The synchronous local boundary is documented in
[source access](source-access.md); ownership and activation requirements are in
[source authority activation](source-authority-activation.md).

Evidence:
- `src/collections.rs` routed_ingest_mapped resolves an Ingest permit, wraps the
  incoming stream with AuthorizedStream, forwards to routed_ingest_mapped_bound,
  then calls access.check() after the result. The underlying node commits can
  precede that final check; refusing the response cannot undo the writes.
- `src/coordinator.rs` routed_ingest_mapped_bound buffers the whole input into
  per-shard vectors before spawning network ingest calls. Its topology write
  guard and required_topology_generation are placement/cutover controls, not
  user-policy commit authorization. The user principal/permit is not passed to
  the node calls in this function.
- `src/authorization.rs` AccessPermit::pin verifies a matching guard decision and
  policy revision; the current PolicyAuthority guard holds a local policy read
  lock. That proves the synchronous catalog commit boundary, not a remote one.

Do not implement A4 as an owned coordinator-side guard held across an RPC and
claim remote fencing. A timeout, cancellation or lost reply can release that
local guard while the remote node can still commit. Similarly, a signed policy
snapshot or its revision cannot establish that revocation has not completed at
another authority. A remote participant must enforce a committed decision or a
revocation protocol with an explicit completion barrier.

The proposed integration uses durable source acceptance as its authorization boundary:
accept an actor-scoped conditional version while current permission is pinned,
then publish only that already-accepted source version through the source-owned
projection journal. Publication may finish after the accepting principal loses
permission because it materializes an earlier authorized durable decision; it
must not authorize any new source write, grant result disclosure, or let an
untrusted caller fabricate a source decision. This needs real network routing,
source-owner/history/incarnation checks and retry recovery, not just a new RPC
forwarding to the old raw ingest path.

Legacy raw mapped-ingest routes still need a stated, enforced authorization
boundary. Do not silently reinterpret their row-count response as a durable
logical-source receipt or claim whole-stream atomicity: shard calls can complete
independently. Preserve explicit accepted/searchable/durable semantics.

Also account for the whole-stream per-shard buffering when designing this path.
A per-message or per-principal rate limit is not a bounded total stream buffer.
Use explicit byte/document admission and backpressure together with stable
idempotency and partial-progress receipts; do not fix the authorization race by
retaining an arbitrarily large stream inside a long-lived policy lock.

Required negative scenarios for the eventual network implementation:
1. Revocation before source commit refuses, with no new version or retry record.
2. Revocation after source commit does not erase the receipt; future receipt
   disclosure still needs current Ingest permission.
3. Lost/cancelled coordinator RPC does not allow a remote late commit to cross
   a completed revocation, or is explicitly an already-authorized source commit.
4. Stale source owner/history/incarnation and fabricated publication input refuse.
5. Partial multi-shard progress and same-actor retry are durable and explicit;
   another actor using the same caller operation ID cannot inherit that progress.
