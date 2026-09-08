# Raft hosting of the source authority

Status: implemented 2026-09-08 as the single-node stage of the accepted
[control-authority design](raft-control-design.md): `src/raft/` behind the
cargo feature `raft` (`openraft = "=0.9.25"`, `storage-v2`). Tests in
`src/raft/tests.rs`. Transport, membership changes beyond the bootstrap
voter and multi-node fault scenarios are the next stage; nothing here is
multi-node evidence.

## Envelopes

`raft_control.proto` types everything the consensus layer stores or moves:
`RaftProposal { principal, command }` (control, import, capacity configure or
observation transition), `RaftReply` (the matching decision, or a
`RaftRefusal` for an envelope or admission refusal at application),
`RaftLogId`, `RaftVote`, `RaftMembership` (joint configs and nodes),
`RaftEntry` (blank, proposal, membership), `RaftApplied`, `RaftLogHeader`
and `RaftSnapshotMeta`. `src/raft/types.rs` maps each to the library's
types explicitly and refuses malformed values by name (a voter with no node
record, unsorted ids, an entry without a log id). No Rust object is ever
serialized.

## Log store

`RaftLogStore` is a dedicated redb database beside the authority store:
a header (group identity, node id, vote, committed and last-purged
positions) and the consecutive entries keyed by index. `create` is the
explicit bootstrap; `open` refuses a missing or empty file and a header of
another group, node or format. Every write is one immediate transaction
under one write order, so votes and entries never interleave; the flush
callback fires after the commit. Append refuses a hole; truncate and purge
are protocol boundaries recorded in the header. The committed position is
saved, so a crash between commit and apply replays on restart.

## State machine

`ControlStateMachine` wraps the authority store and marks it Raft-hosted:
the direct command paths (`execute`, import steps, capacity commands) refuse
with "propose through the host"; only the committed-replay paths apply. For
each entry the applied position (`RaftApplied`: last log id and the stored
membership) is written in the same transaction as the command's effect —
the store's locked paths write the meta key `raft` from a pending position
set by the state machine — and blank, membership and refused entries write
the position alone. A store failure (`Internal`, `DataLoss`) is a storage
error that stops the node; a business rejection is a recorded decision; an
envelope or admission refusal is a recorded `RaftRefusal`. Recovery at open
validates the applied position when present.

Snapshots are consistent copies of the store file taken while every store
transaction is held off (`quiesced`), signed by length and SHA-256; the
snapshot id names both, and `current.meta` beside `current.redb` carries
group, last log id, membership and signature. Install writes into an
isolated incoming file, verifies length and digest against the announced
id, opens the image as a store of this group (running the full recovery
audit) and requires its applied position to equal the meta, keeps it as the
current snapshot, then swaps: the live store is closed, the image renamed
into place and reopened. The swap needs exclusive ownership of the store
handle and refuses by name otherwise; an outstanding clone keeps the old
file open. Restart sees the old complete state or the new complete state.

## Host

`RaftHost::bootstrap_single` is the explicit one-time creation of a group of
one voter; `RaftHost::start` opens existing state and never creates. The
host admits proposals the way the direct paths did — a `ConfirmReady` needs
the managed binding (`propose_confirm_ready`), a `Begin` needs the
retirement holder (`propose_import`) — then writes them through
`Raft::client_write` and returns the committed reply. `store()` hands out
the current store for reads (maps, snapshots, decisions). A proposal on a
non-leader is `Unavailable` naming the leader.

## Evidence (single node)

- Envelope round trips and malformed refusals.
- Log store: vote, consecutive append, hole refusal, ranges, truncate,
  purge, committed position, reopen, wrong node/group and missing file.
- Host: proposals apply through the log, the direct path refuses, an exact
  retry answers from the record, stale and unauthorized proposals are
  recorded outcomes, restart in place keeps the owner and applied position
  and adds one blank entry for the new term, a missing directory refuses.
- A committed but unapplied entry applies on restart with the position in
  the same transaction.
- A snapshot built on one node installs into a fresh replica of the group
  with the same owner rows; a corrupted image refuses before anything is
  replaced.

## Not yet

- The tonic transport with typed RPC envelopes, peer certificate identity
  and size/deadline limits; learners, promotion and voter replacement;
  three-voter fault scenarios; cross-process replay; the map feed from a
  learner's applied state. Kimi's harness stays on the local authority until
  these land.
