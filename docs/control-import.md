# Bounded staged import of the retired legacy authority

Status: contract for review, 2026-09-08. Nothing below is implemented. It
follows [control-checkpoint.md](control-checkpoint.md),
[control-retirement.md](control-retirement.md) and the
[single-authority convergence boundary](raft-control-design.md#single-authority-convergence-boundary-2026-09-08),
and it is the first gate in that boundary's list: faithful bounded import.

## The problem this contract solves

The retired record carries a validated checkpoint of up to 16 MiB. The
destination `SourceAuthorityStore` admits commands of at most 1 MiB
(`max_command_bytes`) and records of at most 2 MiB (`MAX_RECORD_BYTES`), and
every accepted command is one durable, actor-scoped, retryable decision in one
redb transaction. The checkpoint cannot pass through that path in one piece,
raising the limits would weaken every other command, and reading the payload
from a local file at apply time would make replay depend on a file that is
not part of committed state. The import therefore moves the payload through
the same command path in bounded pieces that are themselves committed state.

## Inputs

An import consumes exactly three explicit inputs. None is inferred.

1. **The retirement record.** A `RetiredLegacyControl` holder obtained from
   `retire_legacy_control` or `recover_legacy_retirement` for this destination
   authority (same `SourceAuthorityIdentity`) and this resource. The import
   binds the SHA-256 of the canonical `LegacyControlRetirement` payload — the
   same bytes the `PCTRLRET` frame checksums — not merely the checkpoint digest,
   so the actor, operation and destination identity of the retirement are part
   of what the import commits to.
2. **The supplement.** A `LegacyControlImportSupplement` (storage proto, format
   1) carrying what the legacy JSON never held and the import must not invent:
   the placement tree and per-route placement codes of the current published
   generation, the derived-column declaration, the provider geometry
   (dimension, bits per component, encoded-row-byte formula version) and the
   effective control policy declaration with its planner version. Each part
   carries its own SHA-256; the supplement carries the SHA-256 over the whole.
   The tree and codes must cover every route of the checkpoint's current
   topology exactly, or the import refuses by naming the route. A collection
   whose quiesced map carries no tree declares that explicitly (`no_placement =
   true`); absence of the field is a refusal, not "no tree".
3. **The command.** `SourceAuthorityCommand` actions under one workflow id:
   `BeginControlImport`, `ControlImportChunk`, `CommitControlImport`,
   `AbortControlImport`. The actor is the server-resolved principal; the
   resource key is the collection with an empty owner id; admission is current
   resource Admin exactly as for retirement.

## The staged protocol

Every step is an ordinary command: actor-scoped, retry-safe by `command_id`,
bounded by `max_command_bytes`, and one durable transaction. The pieces live
in the destination's own tables, so a follower replaying the same commands
reconstructs the same staging without any local file.

| Step | Payload | Effect on commit |
|---|---|---|
| `BeginControlImport` | workflow id; retirement payload SHA-256; supplement SHA-256; total staged bytes; chunk count `n` (1..=17); expected control and policy revisions | Reserves the workflow; records an `ImportStaging` header. Refuses if another import workflow is pending for the resource, if the destination already holds an applied import for this resource, or if the revisions differ. |
| `ControlImportChunk` | workflow id; ordinal `i` in `0..n`; bytes (≤ 1 MiB minus envelope); SHA-256 of the bytes | Stores chunk `i` if absent; an existing chunk with the same digest is the retry result, a different digest refuses. Chunk ordinals may arrive in any order. |
| `CommitControlImport` | workflow id; the supplement (it is small: tree, codes, declaration, geometry, policy — bounded to 512 KiB); the retirement payload SHA-256 again | Requires every chunk present. Concatenates chunks in ordinal order, checks the total length and the whole-payload SHA-256 against `Begin`, decodes the retirement payload, re-runs `LegacyControlCheckpoint::decode` on its checkpoint, validates the supplement against the checkpoint, applies the state, records the import receipt, and deletes the staged chunks — all in one transaction. |
| `AbortControlImport` | workflow id | Deletes staged chunks and closes the workflow. The workflow id is never reusable. |

The staged bytes are the exact `LegacyControlRetirement` canonical payload,
so the destination re-validates the retirement itself: the destination
identity inside the record must equal this store's identity, and the
operation inside the record must name the same actor and resource as the
import commands. A retirement made for another authority, actor or resource
cannot be imported.

Bounds: `n ≤ 17` chunks of at most 1 MiB gives 17 MiB, the retirement frame's
own limit. Staged bytes count against `max_payload_bytes` while pending, so a
half-staged import cannot exceed capacity silently, and an abort or commit
returns them. At most one pending import workflow per resource.

## What applies, and what does not

The commit applies the checkpoint into the unified control state, whose
schema and transition function are the next contract (`control-state.md`,
not yet written). In this document only the invariants of the application
matter:

- **Allocators are preserved, never rebased.** `next_token`, `next_action`,
  topology generation and the ordered history import as they are. Lease
  tokens below `next_token` remain distinct; a token at or above it refuses.
- **The destination's control revision advances by one for the commit.** The
  legacy `revision` is recorded as `imported_legacy_revision` provenance and
  is never mirrored into the destination's revision. Two independently
  committed authorities do not share a counter.
- **Node registrations import as observations, not authority.** Every legacy
  lease imports with its expiry, but no imported registration is a live owner
  of anything: readiness is not created by import, an expired lease still
  does not authorize replacement, and the first managed release keeps
  automatic replacement unavailable.
- **Pending actions import in order with their exact payload.** Their
  execution state is `imported-unreconciled`; no worker consumes them until
  the reconciliation step in the migration rules runs under the new authority.
- **Completed-action ids import with provenance `legacy-id-only`.** The legacy
  set has no request fingerprints, so a retry claiming one of these ids is
  refused as unsupported rather than answered from the id alone.
- **The placement tree, codes, derived declaration, geometry and policy** come
  from the supplement and become committed collection configuration with the
  import's revision. A later dry run reads them from state, not from a live
  coordinator.
- **Nothing of a device enters.** The checkpoint holds control metadata only;
  the import carries no source, index, WAL or snapshot bytes, and the
  supplement's residency fields are copied, never widened.

## Receipt and recovery

The `CommitControlImport` decision is the import receipt: actor-scoped,
retryable, and the only durable statement that this destination imported this
retirement. It records the retirement payload SHA-256, the supplement SHA-256,
the imported legacy revision and generation, the destination revision after
commit, and counts of routes, nodes, replicas, actions and completed ids.
Recovery of a lost reply is the same command under the same actor and id.

Crash windows:

| Interruption | Durable state | Recovery |
|---|---|---|
| During any step before its commit | previous step's state | retry the step; its decision is absent |
| After a chunk's commit, before its reply | chunk stored | retry returns the stored decision |
| After `Commit`'s transaction, before its reply | state applied, chunks gone, receipt stored | retry returns the receipt; a second commit refuses (`AlreadyExists`) |
| Process exit mid-workflow, indefinitely | staging retained and charged | resume with the remaining chunks, or abort |

A destination with an applied import refuses a second `Begin` for the same
resource. There is no reverse operation: the legacy file is already retired,
and the migration rules forbid restoring it once new decisions are
acknowledged.

## Replication semantics

Under Raft each step is one log entry and the staged chunks are state-machine
state, included in control snapshots. A follower installs a snapshot taken
mid-import and continues from the same staging. Apply of `Commit` is
deterministic over committed inputs: it reads chunks from state, never from
disk or the network, and the leader supplies no clock. Entry size stays under
the transport's existing limits because a chunk is at most 1 MiB.

## Refusals named in the contract

- Retirement for another authority, actor, resource or operation.
- Supplement absent, unsigned, or covering a different route set than the
  checkpoint's current topology.
- Chunk count outside 1..=17; chunk digest differing under the same ordinal;
  total length or whole-payload digest differing from `Begin`.
- A second pending import for the resource; a second applied import.
- Control or policy revision differing at `Begin` (the CAS the commands already
  carry); the import being asked to invent readiness, a writer, or a tree.

## What this contract does not decide

- The unified state schema and the deterministic transition over it
  (`control-state.md`).
- Managed readiness/admission joined through source commit, and the
  policy-pin/authority-lock ordering.
- Raft storage, transport and membership.

Those follow in that order. Reviewers: this contract changes the durability
model of the destination store (staged payload bytes as committed state under
capacity accounting) and binds the retirement record, not just its checkpoint,
into the import. Both are deliberate and are the points to review.
