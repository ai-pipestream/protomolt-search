# Owner admission and readiness through the source authority

Status: implemented 2026-09-08 in `src/source_authority.rs`,
`src/document_catalog/managed.rs` and `src/source_owner.rs`; tests in
`src/source_authority/admission_tests.rs` and
`src/document_catalog/managed/tests.rs`. This closes two gates of the
[single-authority convergence boundary](raft-control-design.md#single-authority-convergence-boundary-2026-09-08):
initial owner readiness with crash recovery, and current-policy admission
held through the source commit. It publishes no map and activates no writer.

## The committed policy is the workspace authority

`SourceAuthorityStore` implements `Authorizer`. `authorize` evaluates the
committed `AccessPolicy` with the same snapshot evaluation the general
authorizer uses; `subscribe` observes the committed policy revision, published
after every command commit under the exclusive admission guard, so permits
see one ordered policy history; `pin` is one `SourceAdmission`.

The store keeps an admission `RwLock` beside its `closed` latch. Admitted
work (a pin, a `SourceAdmission`) holds it shared; every control command —
`execute`, the import steps, readiness confirmation — holds it exclusively
around its transaction. A revocation therefore waits for admitted operations
to drain and no admitted operation observes a policy or ownership change
under its guard. The lock is acquired before `closed` and never while
`closed` is held. Store failures still latch the instance; a source-side
failure under an admission does not.

Reentrancy is the hazard the design named: an exclusive waiter blocks later
shared acquisitions, so an operation that pinned a permit and then acquired a
second admission would wait on itself. The rule is one acquisition per
operation. `SourceAdmission` resolves everything an owner-side commit needs —
`authorize(collection, action)` for the current decision and
`prepared_owner(expected)` for current Admin plus the exact pending owner —
and is dropped after the source commit. `bind_prepared_owner` takes that
admission instead of a separate policy pin.

## Readiness

`PreparedSourceOwnerPhase::READY` is the phase after the owner persisted its
managed binding (`SourceManagedBinding`, catalog format 9) and the hosting
adapter verified that completion. The owner row carries
`SourceOwnerReadiness { completion }` exactly in that phase; a READY row
without it, or a completion naming another history, host or storage
incarnation than the prepared target, refuses at open (`DataLoss`).

`ConfirmSourceOwnerReady { workflow_id, completion }` moves PREPARED to
READY. Its admission is the adapter's held binding, not caller bytes:

- `execute` refuses every `ConfirmReady` (`PermissionDenied`).
- `PreparedManagedCatalog::confirm_ready(principal, command)` derives a
  `VerifiedOwnerCompletion` from the binding it validated against the file
  it holds under its exclusive lock (`completion()`), and calls
  `confirm_owner_ready`. Only the crate's managed adapter can construct that
  holder.
- Before the transition, the store requires the held binding's preparation to
  equal the committed owner row exactly and the command's completion to equal
  the holder's. Both are direct refusals; nothing is recorded, so a replica
  applying the committed command later performs no such check. The
  transition itself is pure over the committed command: current prepared
  workflow, completion consistent with the target, then READY.
- The decision is actor-scoped and retryable like every command: an exact
  retry, with or without the holder, answers from the record. A new
  confirmation id after READY is refused at admission because the binding's
  preparation is no longer the committed owner.

READY is terminal for the ownership generation: cancellation refuses, and a
new preparation refuses until a complete ownership transition exists.

## Joined through the source commit

The two databases share no transaction; each boundary has a durable state
and a recovery action:

| Interruption | Control | Source | Recovery |
|---|---|---|---|
| Before the binding commits | PREPARED | prior format | bind again under a fresh admission |
| After the binding commits, before readiness | PREPARED | format 9 | `PreparedManagedCatalog::recover`, then `confirm_ready` |
| Before the readiness transaction | PREPARED | format 9 | confirm again; its decision is absent |
| After the readiness transaction, before the reply | READY | format 9 | the exact retry returns the stored decision |

READY implies a durable binding: it is reachable only through the held
binding. A durable binding without READY is a prepared owner with an explicit
recoverable step, never a writer. Neither outcome yields two admitted
writers. Revocation between preparation and readiness is enforced at the next
step (`PermissionDenied`); a revoked actor's admission is a fence that admits
nothing.

## Tests

- The store as authorizer: permits acquire against it; a pinned permit
  delays a revocation committed by another administrator until dropped; the
  revision is published afterwards; stale permits, pins and new acquisitions
  refuse; reopen publishes the committed revision.
- Readiness: general path refused; differing bytes refused; a binding of
  another preparation or authority refused; a malformed binding is
  `InvalidArgument`; stale revision recorded; READY with the completion;
  exact retry; cancel and re-prepare refused; reopen validates; a READY row
  without its completion refuses to open.
- Revocation between preparation and readiness, then restoration.
- Process exit before and after the readiness commit; recovery completes.
- Managed adapter: readiness from the held binding with the binding digest
  checked; exact managed recovery after readiness; a binding committed before
  a crash confirmed after recovery; current Admin required.

## What this does not decide

- Writable managed activation and the committed map feed
  (`same-revision/different-map` refusal, publication).
- Replacement of a READY owner (retire, capture, activate, install).
- A replacement administrator's abort or override paths.
- Raft storage, transport and membership. The shape is already the
  committed-log one: the admission checks above run at proposal, and
  `replay_command` applies a committed `ConfirmReady` without the managed
  binding (tested: the same two commands on a fresh replica yield the same
  READY row, while the general path still refuses them).
