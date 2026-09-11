# Raft hosting of the source authority

Status: implemented 2026-09-08 from the accepted
[control-authority design](raft-control-design.md): `src/raft/` behind the
cargo feature `raft` (`openraft = "=0.9.25"`, `storage-v2`), the tonic
transport and membership operations behind `raft` + `tls`. Tests in
`src/raft/tests.rs` (single node) and `src/raft/transport_tests.rs` (three
voters over loopback mTLS). Member processes serve distributed owner
writes through the hosted write service below; the multi-node evidence
it holds to is in that section and in
[admission under Raft](raft-admission.md).

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
checksum. `max_snapshot_bytes` is a receiver's bound and does not apply
to a build of the node's own store. Build writes a `build-*` directory, syncs, renames it into a new
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
the image. The swap replaces the store's database in place
(`SourceAuthorityStore::replace_from`): the staged copy is opened, adopted
and validated as this identity, renamed over the store's path, and moved
into the store's backing slot under the admission lock held exclusively and
the operation lock every store operation runs under, so no admitted work
and no transaction spans it. A handle held across the install, and every
clone, serves the installed state on its next operation; the policy and
applied watches publish the installed revisions on the same channels. On
refusal nothing changed, the unpublished generation is removed and the
error named. On success the pointer moves. Build and install never overlap.

### Snapshot admission

The transport owns an incoming snapshot transfer end to end and hands the
library a validated image only (`RaftTransportService::stage`,
`SnapshotStaging` in `src/raft/state_machine.rs`):

- **Bounds.** Chunks go straight to the receive file, so memory is one
  chunk (`TransportLimits::max_message_bytes`); disk is up to three times
  the announced length while an install completes (the receive, the probe
  copy, the staged copy), and the announced length must be within
  `HostConfig::max_snapshot_bytes` before the first byte arrives; a chunk
  past the announcement drops the transfer with its bytes, and the next
  transfer from the peer begins at once
  (`a_chunk_past_the_announced_length_drops_the_transfer_and_the_next_one_installs`);
  one transfer at a time; a transfer with no chunk for
  `TransportLimits::snapshot_idle_timeout_ms` is dropped with its bytes.
- **Binding.** A transfer is bound to the authenticated peer that began
  it, the vote it came under, its snapshot id and its meta. Another peer
  cannot continue it (`FailedPrecondition`, naming the bound node) or start
  one under the same vote while it is open (`Unavailable`); the same peer,
  or a higher vote, supersedes it; a chunk with a changed meta or vote
  drops it; an out-of-order chunk is a mismatch the sender restarts from,
  and a chunk at offset zero restarts the same transfer, as the library's
  own sender does.
- **Validation before the library.** On the final chunk the complete
  image must have the announced length, then its digest is streamed, then
  a separate probe copy is opened as a store of this group (a redb open
  rewrites recovery state, so the received bytes stay untouched) and its
  applied position and membership must equal the meta. Any refusal is
  returned to the peer by name, the receive directory is removed, and the
  core is untouched.
- **Visibility.** A rejection a peer's service answers is recorded at
  the node that sent it, per peer, with the peer's code and message
  (`RaftHost::peer_rejections`); an install rejected at the announce is
  counted at the receiver with the announced length and the bound
  (`RaftHost::snapshot_rejections`); each build is reported
  (`RaftHost::last_snapshot_build`); and `RaftHost::add_learner` names a
  rejection the learner answers, with the learner's own code, instead of
  waiting out its window (the learner stays in the membership and the
  leader keeps retrying). A status is a rejection only when it carries the
  service's mark (`psearch-raft-answer` in the status metadata): the
  transport between two nodes makes statuses in the same code space (a
  connection closed is `Internal`, an end of stream and a message past
  the client's own decode bound are `OutOfRange`), and those are retried
  with the library's backoff, not recorded, and do not end a join. These
  accessors are in-process: no code in the serving binary reads them, no
  `/metrics` row and no log line carries them, so outside an `add_learner`
  call a peer's rejection is visible to a caller of the host and to no
  operator; the operator route for them is open ("Not yet"). A rejected
  snapshot is returned to the library as unreachable, so the leader backs
  off between attempts: the library's sender retries the chunk five times
  with 500 ms between, returns to the replication core, backs off 500 ms
  more and serves afresh, about 2.5 s between fresh serves
  (`the_leader_keeps_serving_a_peer_that_rejects_its_seed_with_backoff`).
- **Hand-off.** A validated image goes to `Raft::install_full_snapshot`,
  which applies the library's own rules: a vote older than the receiver's
  is answered without installing, a position the receiver already holds is
  declined, and a declined image is discarded. The state machine's install
  then swaps the image in without re-validating it, in place under the
  store's locks, so a held handle neither blocks nor refuses it; only a
  real storage failure in that swap is fatal, as the library's contract
  requires.

Evidence (`tests/control_raft_snapshots.rs`, "snapshot admission refuses
invalid transfers without stopping the core"): oversized, foreign,
truncated, wrong-membership, out-of-order, interrupted-then-superseded,
meta-changed and other-peer transfers are each refused by name with the
core running, the generation, owner row and applied position unchanged and
no receive left behind, and the genuine image then installs on the same
running core. The idle timeout is covered in `src/raft/transport_tests.rs`.

### Log and store agreement

The library purges the log for an incoming snapshot before the state
machine has installed it, and treats the state machine's refusal as a
fatal storage error. A purge therefore never passes the hosted store's
applied position (`RaftLogStore::bind_applied_floor`): a purge that would
is cut at the store's position, the library's position is recorded in the
log as the deferred purge, and the remainder is completed once the store
is at or past the recorded position: at start, before the library reads
the log state, and at the next append in the run that asked for it. The
library keeps its own purge point and reads entries above it, and it
guards a purge it did not ask for by assertion only (a release build would
send a follower entries with a gap, and that follower's core would stop
at the gap check), so a record left by an earlier run (a refused install,
a crash) is not completed at an append until the library asks for a purge
at or past it, or the next start; the log keeps the entries meanwhile,
contiguous from its purge point. A purge target below the purge point,
and a truncation reaching into a deferred purge, are rejected by name.
The library's `purged` metric reports the position it asked for, not the
log's, for as long as a purge is deferred. A refused install thus leaves
the log consistent with the store it kept, and the member restarts and is
seeded again. The
record is what makes the seed of a member with a log that advanced
without an apply hold: with the store at no position the purge moves no
entry, the install applies the snapshot's position, and the next append
completes the purge to it first, so the entries from before the seed go
and the append is written at the position after the snapshot. Without the record the
log kept those entries and the append opened a gap, which stopped the
member's core (`a_pending_member_with_an_advanced_log_and_no_apply_is_seeded_and_applies`).
A log holding entries after a gap is data loss and refuses to start. Startup also refuses a published
generation whose position is past the store's applied position (a forged
pointer, or a store restored from an older copy), naming both positions.

Two more refusals sit in the transport in front of the library: appends
and snapshot chunks under an uncommitted vote (both come only from a
leader, and the library asserts as much), and a snapshot chunk ending past
the announced length (enforced while receiving, not only at install).

Invalid incoming data never reaches the library (see "Snapshot
admission" above), and the swap holds no precondition a caller could
fail. What stops the core is a real local storage failure on one of the
state machine's three snapshot paths, by the library's contract, and the
member recovers by restart:

- **Install**: the swap (`SourceAuthorityStore::replace_from`) and the
  generation rename and publish around it.
- **Build**: quiescing and copying the store, hashing the copy,
  publishing. The image is not bounded here: `HostConfig::max_snapshot_bytes`
  is each receiver's bound, applied where an image arrives, so a store
  over a peer's bound is rejected at that peer by name
  (`ResourceExhausted`, naming the announced length, the bound and the
  rejecting node) before any byte is received, with that peer's core and
  the leader's running; the leader records the rejection, names it on the
  `add_learner` that met it, and keeps retrying the peer with backoff; the
  peer stays unseeded until its bound is raised or the store shrinks
  (`a_store_image_over_a_peers_bound_is_rejected_at_that_peer_with_both_cores_running`,
  `a_peer_rejected_at_its_bound_is_seeded_after_a_restart_under_a_larger_bound`).
  The build reports its image's length against this node's own receiver
  bound (`RaftHost::last_snapshot_build`): a node with a store image
  over its own bound cannot be seeded from a peer's image of that store
  after a wipe, nor caught up by a snapshot once it falls behind the
  leader's purge point, until its bound is raised. The bound is a
  configuration of each node, read at start. A peer that rejects the
  image by digest (`DataLoss`) names bytes that differ from the meta; the
  leader then reads its published image by digest
  (`RaftHost::verify_published_image`, under the pointer lock) and, when
  its own copy fails, builds afresh into a new generation, so the next
  serve is sound and the cycle ends; when its copy passes, the difference
  was on the way or at the peer, and the rejection stands as recorded.
  The one rejection left on this path is a condition, not a failure: a
  build with no applied position cannot happen through the host.
  `RaftHost::trigger_snapshot` rejects it on such a store before the
  library sees it, the library's own policy does not ask for one, and the
  builder's rejection is for the impossible state.
- **Serve**: reading the published generation for a lagging peer
  (`get_current_snapshot`). A serve is one step under the pointer lock,
  from reading the pointer to opening the image; a build or install
  publishing meanwhile waits for it, and once open the file outlives the
  directory's removal. A serve checks the meta and the image's length and
  does not read the image's digest: the receiver verifies the digest and
  rejects by name, this node's start verifies it in the sweep, and a
  serve is repeated for as long as a peer rejects. An image with bytes
  that changed on disk is therefore served, rejected at the receiver
  (`DataLoss`, the digest) and named at the leader, with the leader's
  core running; the leader's own check of the image fails and a fresh
  build replaces the generation, with no proposal in between
  (`a_served_image_with_changed_bytes_is_rejected_at_the_receiver_and_named_at_the_leader`).
  A continuation of a transfer the receiver dropped (a nonzero offset
  with no transfer in progress: after a rejection on the final chunk, or
  the idle timeout) is rejected by name rather than answered with a
  mismatch: on a mismatch the library's sender starts the same bytes over
  from offset zero on the same opened image, without end and with no
  fresh serve; on a rejection it spends its retry budget, returns to the
  core and serves afresh. A node with a published image that fails its
  digest at start does not start (the sweep names the generation); the
  repair is to remove the published generation and the pointer, after
  which the node starts on the store it has and is caught up or seeded by
  the leader. Before this lock a publish between the two reads
  removed the generation under the serve, the read failed as a storage
  error and the core stopped with no fault anywhere. The interleaving is
  pinned with two `fault-injection` gates, `RaftHost::arm_snapshot_build_gate`
  (paused before publish) and `arm_snapshot_serve_gate` (paused after the
  pointer read), and `RaftHost::serve_snapshot`, which reads what the
  state machine serves (`tests/control_raft_snapshots.rs`, "a serve under
  a concurrent publish returns the generation it read").

## Host

`RaftHost::bootstrap_single` is the explicit one-time creation of a group of
one voter with no transport; `RaftHost::start` opens existing state and
never creates. The networked forms are `bootstrap_cluster` (the first
member, listening), `prepare_member` (the durable state of a member that
will join, see below) and `start_member` (a prepared member, or any member
restarting, listening on its recorded address). The host admits proposals
the way the direct paths did — a `ConfirmReady` needs the managed binding
(`propose_confirm_ready`), a `Begin` needs the retirement holder
(`propose_import`) — then writes them through `Raft::client_write` and
returns the committed reply. Raw submission and committed application are
not reachable by product callers: the submit path is private, the replay
paths, the log store's constructors and writes and the state machine's
constructor are crate-private, and a hosted store refuses every direct
command and every local `admission()`. `store()` hands out the current
store for reads (maps, snapshots, decisions); `with_admission` grants the
leased admission owner-side work needs, with the linearizable read bounded
by the longest election so an isolated leader refuses instead of waiting
([admission under Raft](raft-admission.md)). `lease` performs the same
anchor, barrier, vote-hold extension and grant gate and returns them as
an owned `LeasedAdmission` for work that runs on a blocking worker; the
grant on that thread measures the interval from the anchor.
`with_admission` is that path with an immediate grant. A proposal on a
non-leader is `Unavailable` naming the leader.

## Transport

`raft_transport.proto` carries the library's three operations as typed
RPCs: `AppendEntries`, `Vote` and `InstallSnapshot` (one chunk per call in
the library's chunked protocol, with the snapshot meta naming group, last
log id, membership and the checksum its id encodes). Every request and
reply carries a `RaftRpcHeader`: protocol version, group identity, the
sender's and the receiver's node id.

Identity is bound on both sides (`src/raft/transport.rs`):

- A `PeerDirectory` binds each node id of one group to the SHA-256 of its
  certificate's DER (`certificate_sha256` over a PEM with exactly one
  leaf). A certificate binds to one node and a node to one certificate;
  rebinding refuses.
- The listener requires a client certificate from the cluster CA (mTLS), looks
  the leaf up in the directory, and refuses before the library sees the
  call when the certificate is unregistered (`Unauthenticated`), when the
  header claims another node than the certificate is registered to, names
  another group or is addressed to another node (`PermissionDenied`), or
  when the header is missing or of another protocol version
  (`InvalidArgument`). A certificate of another CA does not complete the
  handshake.
- The caller dials the address the membership records, presents its own
  certificate, names the target in every request and checks the responder
  named in every reply; a reply from another node or group is a network
  error, never a result.

Two more checks sit in front of the library. Every RPC presents the
group's timing agreement (`HostConfig::timing_agreement`: heartbeat,
election floor and ceiling, lease, skew) in metadata; a peer presenting
different values, or none, is refused (`FailedPrecondition`). And a
leader whose latest admission interval may still be open answers a vote
request with its current vote and grants nothing, without the library
seeing the request (`LeaseHold`; the reasons are in
[admission under Raft](raft-admission.md)).

Bounds: `TransportLimits { max_message_bytes, connect_timeout_ms }` is
enforced by both sides' codecs and validated to hold one snapshot chunk
(`HostConfig::snapshot_chunk_bytes`) with its envelope; an append batch
above the bound is reported to the library as too large so it halves the
batch, and a single entry above it is a named refusal. Each RPC carries the
library's hard deadline; a connect failure or refused connection is
`Unreachable`, a deadline is `Timeout`, and any other status is a network
error. `Isolation` (tests and the `fault-injection` feature) makes a node
refuse every RPC to and from chosen peers in both directions.

## Membership

Membership changes go through the library's joint-consensus procedure,
never through files:

1. `RaftHost::prepare_member(dir, identity, node_id, policy, limits)`
   creates the member's store and empty log. The store carries the meta key
   `member` ("awaiting-snapshot"): while it is present, no log entry applies
   to it (`FailedPrecondition`, "awaits the group's first snapshot"), and
   recovery refuses a store that carries both the marker and an applied
   position. A member therefore never replays the group's log onto its own
   genesis image, which could differ from the group's silently.
2. `add_learner(node_id, addr)` on the leader requires the node's
   certificate to be registered, builds a snapshot at or past the applied
   position it read (the store may be one entry ahead of the metric, and
   a build images the store where it is), purges the log to the
   snapshot's position, adds the learner and waits until the learner holds
   the membership entry that added it, or until the learner's service
   rejects the seed, which is returned at once with the learner's code (a
   failure of the transport between the nodes is retried inside the
   window, not returned). The seed repeats its build trigger every 2 s
   while a build is in flight: the library drops a trigger during a build
   and reports it taken, and a build begun before the position moved ends
   below it. A repeated `add_learner` for the same node writes one more
   membership entry (the library writes the entry whether or not the
   membership changes) and seeds again from the position after it. The
   learner is seeded by the verified image (group, position, membership
   checked on a probe copy), which replaces the marked store; the marker
   is gone with it.
3. `promote(learners)` upgrades caught-up learners to voters
   (`AddVoterIds`); `remove_member(node_id)` removes a voter
   (`RemoveVoters`, not retained as a learner) or a learner (`RemoveNodes`).
   A removed member proposes nothing and admits nothing.

A member restarts with `start_member` on the address the membership
records for it; the address is where it is dialed, its certificate is who
it is.

## Operator configuration

A serving process is a member when `--raft-dir` is given, and then needs
`--raft-node-id`, `--raft-group-id` and `--raft-authority-incarnation`
(32 hex digits each: the identity the store must carry), `--raft-listen`
and `--raft-peers`; `--raft-advertise` names the address peers dial when
it is not the bound one, and `--raft-heartbeat-ms`, `--raft-election-min-ms`,
`--raft-election-max-ms`, `--raft-lease-ms`, `--raft-skew-ms` and
`--raft-max-snapshot-bytes` set the `HostConfig` values (validated at
start; the timing values must be equal on every member, and the transport
refuses a peer whose values differ). The listener needs `--tls-cert`,
`--tls-key` and `--tls-client-ca`; the member's own identity is
`--tls-ca` with `--tls-client-cert` and `--tls-client-key`. Every option has
a `PIPESTREAM_SEARCH_RAFT_*` environment form and a `raft_*` file form.

The peers file is TOML:

```toml
[[peers]]
node_id = 1
certificate = "certs/node-1.pem"

[[peers]]
node_id = 2
certificate = "certs/node-2.pem"
```

Every member is listed, this node included; paths are relative to the file;
a node listed twice or a certificate bound to two nodes refuses
(`src/raft/operator.rs`).

A member serves owner writes for `--raft-managed-catalog=<collection>=<path>`
(repeatable; `PIPESTREAM_SEARCH_RAFT_MANAGED_CATALOG` comma-separated,
`raft_managed_catalog` in the file): an access-controlled source catalog
bound and activated for that collection of the member's authority.
`--raft-managed-principal` names the actor holding the Admin that
recovers the handles at start (`PIPESTREAM_SEARCH_RAFT_MANAGED_PRINCIPAL`,
`raft_managed_principal`). Both need `--raft-dir`; a catalog without its
actor is refused at parse. At start the binary recovers every catalog
through `hosted::recover_catalogs` and registers the hosted service on
the coordinator or relay listener; a node-only process naming catalogs
is refused. The service runs under compiled limits (1 MiB per write,
16 MiB admitted, 64 operations); the transport principals come from
`--bearer-tokens` as on the other surfaces.

Serving opens existing state only. `pipestream-search raft-prepare
--raft-dir=... --raft-node-id=... --raft-group-id=... --raft-authority-incarnation=...
--raft-listen=... --raft-peers=...` creates a member's durable state with a
placeholder genesis the group's snapshot replaces; the leader then adds it
with `add_learner` and promotes it. Bootstrapping the first member of a
group stays a programmatic step (`RaftHost::bootstrap_cluster` with the
group's policy and limits); an operator surface for authoring those is not
part of this checkpoint.

## Relay on the committed map

With `--relay` and `--raft-map-principal`, `--raft-map-workspace` and
`--raft-map-collection`, the relay routes on the member's committed map
instead of the file-polled shard map: `AuthorityMapSource`
(`src/source_authority/relay_map.rs`) reads `published_map` for the named
resource under the principal's Admin, validates every frame through
`MapConsumer` (digest, revision order, generation and content rules), and
hands the relay one frozen `MapSnapshot` per accepted revision — routes in
committed order with their placement codes and the validated tree,
`control_revision` from the map and `topology_generation` from its
generation. It wakes on the store's applied watch, which follows a
snapshot install. A frame the consumer refuses is kept beside the held map
as a named fault and the relay keeps routing on the last accepted map. A
resource with no applied control state refuses at attach. The map source
grants no admission and carries no lease; the relay routes on it and admits
nothing by it.

## Hosted owner writes

`HostedDocumentWriteService` (`src/document_write_service/hosted.rs`,
behind `raft`) serves the `DocumentWriteService` proto
(`GetDocumentWriteTarget`, `AcceptDocument`) over activated managed
catalogs, so clients do not change. A process serves the
single-authority service or this one, never both; the two share the
`Route` rows.

One `AcceptDocument` call, in order: the transport gate
(`Principals::authenticate` and `authorize(.., Ingest)`), the request
size with pending and byte permits plus ingest admission on the
principal (the shared `WriteBudget`), `host.lease` on the RPC future,
then a blocking worker holding the lease, the permits and the request,
where the grant runs and `ActiveManagedCatalog::accept` commits. The
lease and the permits are pinned in the worker, never in the RPC
future. `GetDocumentWriteTarget` follows the same shape with
`catalog.write_target` under the grant.

Two gates judge every write: the transport gate first, the authority
admission second and authoritative. The lease actor is the transport
principal's name, which the authority policy rows name by the same
string; a deployment must keep that equivalence. Every error from
`lease` passes through unchanged — `Unavailable` naming the leader
off-leader, `Unavailable` when no quorum acknowledged the barrier, and
`FailedPrecondition` when the interval elapsed before the grant — and
the service never retries, never forwards, and never falls back to the
local `admission()`. No lease check runs after the commit: a write
past its final check is durable under its epoch fence, and the pause
between the final check and durability is the residual named in
[admission under Raft](raft-admission.md).

Recovery at start (`hosted::recover_catalogs`, the function the binary
calls and tests call the same way) opens each configured path, takes
the file's binding bytes as they stand, and recovers the managed
handle under a fresh leased admission. A catalog that is prepared
rather than active, one with an authority identity that is not the
member's, or one with an activation the store does not record stops
the whole recovery by name; no partial service starts.

The evidence is `tests/control_raft_hosted_writes.rs` (eight tests:
admitted-and-durable on the leader, follower naming the leader, lease
kept past the interval with no durable change, isolation inside the
read bound with the healed leader refusing as a follower, restart
serving again, and the three start rejections) with the single-node
lease evidence in `src/raft/tests.rs`.

## Evidence

Single node (`src/raft/tests.rs`):

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

Three voters over the transport (`src/raft/transport_tests.rs`, loopback
mTLS with the fixtures under `tests/certs/raft`, regenerated by
`scripts/gen-raft-test-certs.sh`):

- Peer identity: a registered member is answered by the node it dialed; the
  same certificate cannot speak for another node, reach another node or
  name another group; a missing or unversioned header is malformed; a
  request without the timing agreement, or with other election or lease
  values, is refused; a
  CA-issued but unregistered certificate is unauthenticated whatever node it
  claims; a certificate of another CA does not complete the handshake; an
  unregistered node cannot be added; the directory refuses to rebind.
- Seeding and replication: members 2 and 3 join a bootstrapped node 1 by
  snapshot (marker gone, owner rows present, generation published beside
  each member's store), are promoted, and every later proposal applies on
  all three with the same position and rows; a follower's proposal names
  the leader; a prepared member store refuses to apply before its seed.
- Leader isolation and revocation: a lease granted on the leader with a
  quorum admits; with the leader isolated in both directions the survivors
  elect a successor no sooner than the election floor, by which time every
  lease the old leader issued has expired; the old leader's admission
  refuses within the read bound; the successor commits a revocation of the
  grant while the old side still shows the stale policy and grants nothing
  on it (local admission closed, leased admission refused); after healing
  the old leader catches up to the revoked policy, still admits nothing as
  a follower, and a fresh admission on the current leader sees the
  revocation.
- Restart: a stopped member restarted on its recorded address replays the
  entries it missed; a restarted leader yields a successor and proposals
  continue.
- Replacement: a fourth member joins by snapshot, is promoted, a voter is
  removed through the same procedure, proposals apply on the new voter set,
  and the removed member proposes nothing.

- Relay map (`src/source_authority/relay_map_tests.rs`): a resource with
  no control state refuses at attach; the reading carries the committed
  routes with their codes and the validated tree; a control change moves
  the change receiver and replaces the reading with the generation
  unchanged; a principal without Admin attaches nothing.
- Operator surfaces (`src/config.rs`, `src/raft/operator.rs` tests): the
  `--raft-*` options parse into a member, partial or malformed values are
  refused by name, and a peers file binds node ids to certificate
  fingerprints and refuses duplicates.
- Admission (`docs/raft-admission.md`): a grant paused after its barrier
  resumes into a grant within the interval and into a refusal after the
  survivors elected and revoked; a leader withholds its vote from a
  higher-term candidate while an interval may be open and stays leader.

## Not yet

- The map feed of a learner's applied state.
- An operator surface for authoring the first member's policy and limits
  (`bootstrap_cluster`), and for `add_learner`, `promote` and
  `remove_member` from the command line.
- An operator route for the rejection and build numbers
  (`peer_rejections`, `snapshot_rejections`, `last_snapshot_build`,
  `verify_published_image`): a `/metrics` row and a log line. Until then
  they are in-process accessors.
- Cross-process fault injection (a member killed at a transaction boundary
  while the others continue) is Kimi's harness, on `fault-injection` and the
  transport's `Isolation`.
