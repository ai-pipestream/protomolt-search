# Source authority activation and recovery

Status: proposed implementation contract, 2026-09-07. This extends the accepted
[control-authority design](raft-control-design.md) using the implemented
[backup, staging and terminal seal](source-index-backup.md). It introduces no
RPC, field allocation, runtime activation, transfer or fleet action.

## The boundary established by the code

`DocumentCatalog::seal_history` retires one local store at an exact accepted
sequence. The database writer serializes it with acceptance and every source
and maintenance journal mutation. `begin_retirement` first persists admission
closure and its atomically captured accepted watermark in format 5. New writes
and preparations then refuse, while existing intents can drain through recovery.
Pending intents prevent sealing. Direct sealing produces format 4; sealing
after closure produces format 6 and retains the begin decision. Both remain
sealed after restart and after copying a backup; old readers refuse the new
formats. The seal neither fences an earlier independent copy nor
authorizes another writer.

`VerifiedSourceRestore` holds a verified private bundle and a shared source-file
lock. It exposes no writable source or serving activation. Its manifest binds
original source history, retry records and all journaled index artifacts. An
accepted sequence ahead of an index sequence is retained backlog, not damage.

The current public `DocumentIdentity` is collection-scoped and contains a key,
version and optional chunk ordinal. It has no history ID. Write receipts and
pinned version-2 acceptance requests do carry a history ID. Consequently,
changing a source history ID alone cannot make reused query identities safe.
Likewise, two devices can use the same local document key. A federated result
must retain the originating resource/owner scope before deduplication or RAG
assembly; the current `DocumentIdentity` alone cannot identify a document across
independent owners. Do not silently rewrite exact keys to hide that distinction.

## Authority record and trusted owner

The authority must persist an immutable workspace/collection binding, the
logical source-owner identity, source history, owner incarnation, ownership
generation and current operation state. A physical path, server address, shard
slot, lease deadline or statistics epoch is not that identity. Collection-wide
server authority and device-local authority use the same ownership rules;
federation must retain the originating owner of each device's local history.
Registering a device does not grant the mesh permission to copy its source.

Keep this record in the control state machine, separate from source databases.
Use the same deterministic transition function in the existing single-authority
adapter and the future OpenRaft adapter. Commands contain versioned protobuf
values, an operation ID, expected authority revision/generation and a canonical
request digest. Store successful and rejected control decisions with their
responses; a retry cannot become a different operation when policy or topology
changes. Apply performs no filesystem or network work. Durable intents drive
idempotent owner work, and verified completion facts return as commands.

Source bytes and document retry tables stay in their owner storage. The control
log records bounded ownership and completion metadata, never a source backup.
There is no transaction shared by the two databases: each crash boundary needs
an explicit pending or terminal state and a recovery action.

Operation identities are scoped by the authenticated actor and immutable
workspace/collection/owner binding, not a bare caller-chosen byte string. Check
current access before returning a stored response: persistence preserves the
decision, not permission to disclose it after revocation. An authenticated retry
with the same bound inputs returns that decision without re-executing owner
work; changed inputs refuse. A rejected operation needs a new operation identity
to attempt changed policy or preconditions. Unauthenticated requests cannot
reserve operation identities in the control log. Bound each command, completion
and response and account for the persistent decision table; exhausted capacity
refuses new operations instead of discarding retry history.

## Normal replacement retains the entire history

1. **Prepare.** Authorize administration against the current workspace authority
   and commit the operation, source binding, intended replacement and residency
   constraints. A target must belong to an explicit allowed owner; address
   discovery cannot choose one. The old owner remains the published owner until
   activation, with admission closed before retirement is acknowledged.
2. **Retire.** The trusted owner closes admission with `begin_retirement`, then
   resolves pending source/maintenance decisions against their actual durable
   catalogs and seals the captured accepted watermark under the same operation
   ID. The control operation records both requests and their completion facts.
   A lost reply is recovered by retrying the same request or reading its
   persisted decision. Neither a timeout nor lease expiry counts as a seal.
3. **Capture and stage.** Produce the final bundle from the sealed source and
   its journaled indexes. Verify the replacement through the existing private
   staging operation. Require the same source history, exact seal and accepted
   sequence, and every committed index hash, source sequence and maintenance
   cursor. An earlier live backup is not the final retirement bundle. There is
   no catch-up shortcut based only on a maximum row ID or statistics epoch.
4. **Commit activation.** Recheck the current authorization binding, residency,
   operation, old owner, expected generation, durable seal and verified target
   completion. Commit a new ownership generation and target incarnation. The
   control result authorizes installation; it does not claim that the target is
   already serving or that source backlog has become searchable.
5. **Install and publish.** The trusted target consumes verified staging under
   that committed decision, persists its new owner binding and reports readiness.
   Only then publish the generation-bearing map. The old store stays sealed.
   Keep the previous complete files until retirement and reader cleanup permit
   removal. An ambiguous installation is reconciled from durable state, never
   repaired by creating an empty catalog or replaying only its newest rows.

Record distinct durable phases for preparation, verified retirement,
activation commitment and target readiness. Before retirement, cancellation
may reopen the same owner's admission only under a current authority decision.
The implemented local begin operation has no cancellation; after it commits,
the owner must drain and seal. After a seal commits, cancellation cannot reopen
that store. Preserve the
sealed source and resume recovery or authorize another explicitly bound target;
never interpret an aborted control request as an unseal. Before activation
commitment, a replacement-target change invalidates the previous target's
staging completion. Once installation has been authorized, a second replacement
requires another complete ownership transition, even if the first target has
not reported readiness. A disconnected target may already have consumed the
installation decision. Changing the control record alone cannot fence it;
require its durable retirement or the explicit external fence allowed by the
control-authority contract before authorizing any other writable target.

The persistent local retirement gate covers source acceptance and new index
decisions, avoiding repeated watermark races against continuing writes. The
managed owner must still connect that gate to committed control operations,
current authorization and every routed write entry point. The existing node
ingest fence alone does not cover `DocumentCatalog::accept`. Local closure is
not that distributed authority integration.

The replacement must not clear `history_seal` and call ordinary `open`. Managed
storage needs a versioned authority binding and a guarded write transaction.
Reopening managed storage starts without live admission. The authority adapter
must establish that this is still the committed owner before enabling writes;
a persisted old grant or numeric generation alone is insufficient. The local
standalone mode remains explicit, and a managed store cannot fall back to it.

The owner holds the admission/policy guard through the source write commit.
Guard acquisition precedes the database writer consistently, including seal and
recovery paths. Do not take that guard after a transaction has already acquired
the writer. Revocation and replacement serialize against admitted operations;
the control API must define when a revocation has been enforced by the owner,
rather than reporting enforcement merely because a policy file changed.

Phones retain their files, WAL, vectors, indexes and snapshots on the same
device. A device may recover local storage under its local authority. An absent
device cannot be replaced by a server copy, become a control learner, or have
its shard exported by this protocol.

## Rollback is not a counter reset

With complete current history available, logical rollback appends new
conditional versions representing the selected earlier content. It preserves
all accepted sequence numbers, prior versions and operation decisions. Index
publication follows those new source decisions. Replacing the current database
with an earlier snapshot would instead forget accepted writes and let old
operation IDs execute again, so it is not this operation.

If acknowledged history is missing, normal replacement refuses. Recovery must
first reconstruct the complete history or perform an explicitly separate
data-loss/fork operation with an identity contract that cannot alias the old
collection's document versions. A new history ID alone is inadequate for the
current query identity schema. Do not enable such an operation by silently
accepting legacy unpinned writes, truncating retry records, relabeling an old
checkpoint as current, or inventing success receipts. Its public identity and
recovery contract require their own implementation and conformance evidence.

## Required recovery evidence

Cover process exit before and after each durable boundary above, including a
committed activation whose target has not opened yet and a ready target whose
map publication reply is lost. The result must be the old owner still active,
no writable owner with an explicit recoverable operation, or exactly one new
owner. It must never be two admitted writers.

Tests must include a prepared source and maintenance intent, a private candidate
built before retirement, accepted-but-unpublished backlog, a stale earlier
backup, changed target completion bytes, a duplicated or reordered control
command, an exhausted generation counter, a disconnected owner, revoked policy
between preparation and activation, and the same replica address with a new
incarnation. Repeat source write retries across a successful replacement and
prove the original durable decision and logical identity are unchanged.

Policy grants are always current authority input. Backup content must not grant
search, ingest, administration, document visibility, field access or RAG context
access. Exercise the recovered indexes under different current document and
field policies, including identity-disclosure denial, rather than merely
checking that an index opens or returns the same ranking.
