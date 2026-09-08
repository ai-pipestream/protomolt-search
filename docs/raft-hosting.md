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

An exact retry of a committed entry consumes the entry like any other: the
retry lookup and the applied position share one transaction, so the
position advances with no change to revisions, effects or receipt bytes.
After every entry the state machine reads the position back and stops the
node if it is not the entry just applied.

### Snapshots

Snapshots are immutable generations under `raft-snapshots/generations/<n>/`
(`image.redb`, `meta`), published through one small pointer file
(`current`, written to a temporary file, synced, renamed, directory synced).
The image is a copy of the store file taken while every store transaction
is held off (`quiesced`, which also reads the applied position under the
same hold), streamed through a bounded buffer and checksummed by length and
SHA-256 — an integrity check, not an authenticated signature; the snapshot
id names both and the meta carries group, last log id, membership and the
checksum. A store larger than `max_snapshot_bytes` refuses to snapshot by
name. Build writes a `build-*` directory, syncs, renames it into a new
generation, publishes the pointer and only then removes older generations;
an interruption anywhere leaves the previous generation published and
usable, and startup sweeps partial builds, abandoned receives and
unpublished generations while verifying the published one.

Install receives into `incoming-<token>/image.redb`, the exact directory the
receive was started in (one receive at a time; a superseded receive is
removed with its bytes). It refuses an announced length above the bound,
verifies length and digest streaming, then opens a **separate probe copy**
as a store of this group (the full recovery audit; the received bytes stay
identical to their checksum), requires the probe's applied position and
stored membership to equal the meta, writes the meta, renames the receive
into a generation, and only then swaps the live store for a fresh copy of
the image. The swap needs exclusive ownership of the store handle (short
read clones drain within a bounded wait); on refusal the old handle is kept,
the unpublished generation removed and the error named — the host keeps
its usable store and the previous snapshot. On success the pointer moves and
the policy/applied watch channels move to the reopened store, so
subscribers stay attached. Build and install never overlap.

## Host

`RaftHost::bootstrap_single` is the explicit one-time creation of a group of
one voter; `RaftHost::start` opens existing state and never creates. The
host admits proposals the way the direct paths did — a `ConfirmReady` needs
the managed binding (`propose_confirm_ready`), a `Begin` needs the
retirement holder (`propose_import`) — then writes them through
`Raft::client_write` and returns the committed reply. Raw submission and
committed application are not reachable by product callers: the submit
path is private, the replay paths, the log store's constructors and writes
and the state machine's constructor are crate-private, and a hosted store
refuses every direct command and every local `admission()`. `store()` hands
out the current store for reads (maps, snapshots, decisions);
`with_admission` grants the leased admission owner-side work needs
([admission under Raft](raft-admission.md)). A proposal on a non-leader is
`Unavailable` naming the leader.

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
  with the same owner rows; a corrupted image, a valid image of another
  group and a meta with another membership refuse before anything is
  replaced and leave no receive behind; a held handle makes the swap refuse
  by name while the old state stays readable; a subscriber attached before
  the install wakes after it; an image above the bound refuses; startup
  sweeps abandoned receives, partial builds and an unpublished generation
  and keeps the published one.
- Exact retries in every command family advance the applied position, a
  changed-content refusal and an unchanged observation transition consume
  their entries, and the position survives restart.
- A hosted store refuses direct mutation and local admission; the general
  proposal path refuses a readiness confirmation; a leased admission works
  on the leader and admits nothing once expired; a lease beyond the
  election floor is refused at configuration.

## Not yet

- The tonic transport with typed RPC envelopes, peer certificate identity
  and size/deadline limits; learners, promotion and voter replacement;
  three-voter fault scenarios; cross-process replay; the map feed from a
  learner's applied state. Kimi's harness stays on the local authority until
  these land.
