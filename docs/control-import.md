# Bounded staged import of the retired legacy authority

Status: implemented 2026-09-08 in `src/source_authority/import.rs`, store
format 2, with the review corrections of `REVIEW-FABLE-CONTROL-IMPORT`
carried into the code and the tests (`src/source_authority/import_tests.rs`).
It follows [control-checkpoint.md](control-checkpoint.md),
[control-retirement.md](control-retirement.md) and the
[single-authority convergence boundary](raft-control-design.md#single-authority-convergence-boundary-2026-09-08),
and it is the first gate in that boundary's list: faithful bounded import.

## The problem this contract solves

The retired record carries a validated checkpoint of up to 16 MiB. The
destination `SourceAuthorityStore` admits commands of at most
`max_command_bytes` (1 MiB by default) and records of at most 2 MiB
(`MAX_RECORD_BYTES`), and every accepted command is one durable, actor-scoped,
retryable decision in one redb transaction. The checkpoint cannot pass through
that path in one piece, raising the limits would weaken every other command,
and reading the payload from a local file at apply time would make replay
depend on a file that is not part of committed state. The import therefore
moves the payload through the same command path in bounded pieces that are
themselves committed state.

## Inputs

An import consumes exactly three explicit inputs. None is inferred.

1. **The retirement holder.** A `RetiredLegacyControl` obtained from
   `retire_legacy_control` or `recover_legacy_retirement`. It is the proof of
   retirement: it exists only after the legacy file carries the durable
   `PCTRLRET` fence, and the destination binds the plain SHA-256 of the
   canonical `LegacyControlRetirement` record (actor, operation, destination
   identity, request and checkpoint together), not merely the checkpoint
   digest.
2. **The supplement.** A `LegacyControlImportSupplement` (storage proto,
   format 1) carrying what the legacy JSON never held and the import must not
   invent. Every part is presence-bearing: a placement tree or `no_placement =
   true`; one `PlacementRouteCode` per current route when a tree is present
   and none otherwise; a derived declaration with its exact fingerprint or
   `no_derived = true`; provider geometry (backend kind and version, config
   format and payload, dimension, bits per component, row-byte formula
   version, scoring fingerprint) or `no_provider = true`; a planner policy
   whose `control` must equal the checkpoint's captured policy; and
   `history_placement_unavailable = true`, the only value format 1 accepts,
   since legacy history carries no trees or codes. Trust comes from the
   digests the destination computes over the staged bytes and records in the
   receipt; the supplement carries no digest fields of its own.
3. **The commands.** `ControlImportCommand` actions under one workflow id:
   `Begin`, `Chunk`, `Commit`, `Abort`. The actor is the server-resolved
   principal; the resource key is the collection with an empty owner id;
   every step, including an exact retry, requires current resource Admin.

## The staged protocol

Every step is actor-scoped, retry-safe by `command_id` (the same namespace as
`SourceAuthorityCommand`; an id used by one path cannot be reused by the
other), bounded by `max_command_bytes`, and one durable transaction. The
pieces live in the destination's own tables, so a follower replaying the same
commands reconstructs the same staging without any local file.

| Step | Declares | Effect on commit |
|---|---|---|
| `Begin` | retirement SHA-256 and operation key; payload length and domain-separated SHA-256; `chunk_bytes`; `chunk_count = ceil(payload_bytes / chunk_bytes)` | Admitted only through `begin_control_import(principal, command, holder)`: the declared digests must equal the holder's, the holder's authority must be this store, its operation must name this actor and resource. `execute_control_import` refuses every `Begin` (`PermissionDenied`). Refuses if the workflow id was used, the resource already holds an applied import, another workflow is pending, `chunk_bytes` exceeds the store's chunk capacity, or the reservation (below) does not fit. |
| `Chunk` | ordinal, bytes, domain-separated SHA-256 | Stores a reference to the command's own retained record; the bytes are kept once, in that record. An identical restage under the same ordinal is a recorded `AlreadyExists`; different content under a staged ordinal is a recorded `FailedPrecondition`. Any order. |
| `Commit` | nothing | Requires every chunk. Assembles the payload from the retained chunk commands, checking each reference's digest and length and the whole against `Begin`; decodes the retirement, checks it against the admitted digest, this authority, the operation and the resource; re-runs `LegacyControlCheckpoint::decode`; validates the supplement; applies the unified rows; records the receipt; deletes the references; releases the reservation. One transaction. |
| `Abort` | nothing | Deletes the references, releases the reservation, closes the workflow. |

Chunk capacity is derived from the configured `max_command_bytes`: the
encoded size of a maximal chunk command (1024-byte ids, `u64::MAX` revisions,
`u32::MAX` ordinal, a bytes field filled to the command bound) minus the bytes
themselves. A configuration that leaves fewer than 4096 bytes per chunk is
refused (`chunk_capacity`, `FailedPrecondition`). Other bounds: payload 1 byte
to 18 MiB, at most 4096 chunks, supplement at most 1 MiB, every unified row at
most 2 MiB (an import that cannot be represented under that bound refuses by
naming the row kind), and the assembled payload lives only for the commit
transaction.

### Accounting

Chunk commands are retained retry history like every other decision, so
their bytes are charged once and never duplicated by the reference row.
`Begin` reserves, in the control header, `chunk_count × (max_command_bytes +
key envelope) + 2 × payload_bytes + 2 × 64 KiB` bytes and `chunk_count + 2`
decisions. Every capacity check in the store — ordinary commands included —
subtracts the reservation, so a full store cannot strand a pending import
before its terminal `Commit` or `Abort`; the workflow's own steps spend from
their reservation. Terminal workflow rows remain as charged tombstones, so a
workflow id is never reusable. Recovery at open recomputes the reservation
sums, the per-workflow accounting, the staged reference count, every unified
row count and the payload bytes, and refuses on any difference.

### Ownership, CAS and concurrency

- A workflow is bound at `Begin` to the initiating principal and the
  resource. Another Admin cannot stage, commit or abort it (recorded
  `FailedPrecondition`); revoking the initiator's Admin blocks every further
  step (`PermissionDenied`) until rights are restored or the policy is
  replaced. A replacement administrator has no abort path in this release;
  a stranded workflow keeps its reservation, and that rule is deliberate
  until a retirement-scoped override exists.
- Every step carries `expected_control_revision` and
  `expected_policy_revision` against the current values: unrelated committed
  commands (grants, ownership) advance the revision and a stale step is a
  recorded refusal, re-issued with the current revision and the same content.
  An exact retry (same actor, id and bytes) returns the stored decision
  without a new revision.
- `Commit` re-checks that the resource holds no applied import and that the
  workflow is still staging; a late chunk, a second commit and an abort after
  a terminal step are recorded refusals.

## What applies, and what does not

The commit writes the unified rows for the resource: `CONTROL` (one
`ControlCollectionState`), `TOPOLOGIES` (current and history generations),
`NODES`, `REPLICAS`, `ACTIONS`, `COMPLETED`.

- **Allocators are preserved, never rebased.** `next_token`, `next_action`,
  topology generation and the ordered history import as they are.
- **The destination's control revision advances by one for the commit.** The
  legacy `revision` is recorded as `imported_legacy_revision` provenance and
  is never mirrored into the destination's revision.
- **Node registrations import as observations, not authority.** Residency is
  what the legacy authority declared, `UNSPECIFIED` where it declared
  nothing; it is never widened. Imported leases and ready flags authorize
  nothing: readiness, replacement and ownership need their own committed
  facts under the new authority.
- **Pending actions import in order with their exact payload** in state
  `IMPORTED_UNRECONCILED`; no worker consumes them until reconciliation runs
  under the new authority.
- **Completed-action ids import with provenance `LEGACY_IMPORT`.** They have
  no request fingerprint, so they are never verified retry receipts; a retry
  claiming one is refused as unsupported.
- **The supplement becomes committed collection configuration** with the
  import's revision: tree and codes on the current topology
  (`codes_available` true only there), declaration, geometry and planner
  policy on the state row. Historical generations carry no codes and
  `history_rollback_available` is false until trees for them are supplied by
  a later command. The planner policy is configuration for the planner; the
  destination's access policy is separate and is never derived from it.
- **Legacy identifiers are scoped by the resource and the import receipt.**
  Every row key carries the resource; nothing is promoted to a global id.
- **Nothing of a device enters.** The checkpoint holds control metadata only;
  the import carries no source, index, WAL or snapshot bytes. Staging never
  appears as an activated map or owner; only the committed rows are served.

## Receipt and recovery

The `Commit` decision carries the `ControlImportReceipt`: the only durable
statement that this destination imported this retirement. It records the
retirement SHA-256, the payload SHA-256, the imported legacy revision and
generation, the destination revision after commit, and counts of routes,
history generations, nodes, replicas, actions and completed ids. Recovery of
a lost reply is the same command under the same actor and id.

Crash windows (tested by process exit before and after the transaction of
every phase):

| Interruption | Durable state | Recovery |
|---|---|---|
| Before a step's transaction | previous step's state | the step's decision is absent; issue it |
| After a step's transaction, before its reply | the step is durable | the exact retry returns the stored decision |
| After `Commit`, before its reply | rows applied, references gone, receipt stored | retry returns the receipt; a second `Begin` for the resource is `AlreadyExists` |
| Process exit mid-workflow, indefinitely | staging retained and charged | resume the remaining chunks, or abort |

## Schema upgrade

Store format 1 (four tables, no control header) is adopted at open in one
transaction that creates the nine control tables and a format-2 header with
zero counts; that adoption is idempotent and the only defaulted state. A
store with any other table set, a missing header, or a header naming another
format refuses to open (`DataLoss`) and nothing is written. Every count in the
header is recomputed against the tables at open.

## Replication semantics

Under Raft each step is one log entry and the retained commands and staged
references are state-machine state, included in control snapshots. A
follower installs a snapshot taken mid-import and continues from the same
staging. Apply of `Commit` is deterministic over committed inputs: it reads
the chunks from the retained commands, never from disk or the network, and
the leader supplies no clock. The replay test applies the recorded command
sequence to a fresh store of the same identity and compares every table byte
for byte.

## The committed snapshot

`control_snapshot(principal, key)` serves the immutable committed view
(`ControlCollectionSnapshot`): state, current topology with codes, nodes
without lease tokens, replicas, actions, completed ids and a digest over the
canonical message. It is the interface for observation collection and the
advisory planner; nothing in it grants ownership or authorizes a move.

## Refusals named in the contract

- Retirement for another authority, actor, resource or operation; a `Begin`
  through the general command path; digests differing from the holder.
- Supplement absent or malformed; placement, derived or provider not
  explicit; code count differing from the current route count; a code naming
  no leaf; fingerprint differing from the declaration; planner policy
  differing from the checkpoint; history placement claimed available.
- Chunk ordinal outside the plan; length or digest differing from the plan;
  restage with different content; assembled payload differing from `Begin`;
  a chunk larger than the derived capacity; a configuration below 4096 bytes
  of capacity.
- A second pending import for the resource; a second applied import; a step
  by another actor; a stale revision; a step after a terminal state.
- Any unified row above 2 MiB; reservation that does not fit the remaining
  capacity.

## What this contract does not decide

- Managed readiness/admission joined through source commit, and the
  policy-pin/authority-lock ordering.
- Raft storage, transport and membership.
- Supplying trees for historical generations (rollback availability).
