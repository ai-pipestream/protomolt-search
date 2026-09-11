# Admission under Raft: leases, isolation and revocation

Status: contract, 2026-09-08, revised the same day after Astra's review of
240be9f (A1, A2); the hosted write path landed 2026-09-11. The mechanism is
implemented (`RaftHost::with_admission`, `RaftHost::lease`,
`LeasedAdmission`, `AdmissionLease`, `LeaseHold`, `HostConfig::validate`,
the timing agreement on every transport RPC, the pre-commit fence in
`DocumentCatalog::accept_as`) with three-voter evidence over the tonic
transport. Member processes serve owner writes through the hosted write
service, whose every admission comes from the host's lease
([hosted owner writes](raft-hosting.md#hosted-owner-writes)).

It answers one question before any owner write is admitted through a
replicated authority: what makes an admission authoritative when the node
that granted it may be isolated, and when the surviving quorum may have
revoked the right it relied on.

## Why a local lock is not enough

`SourceAdmission` on a single-authority store is a shared lock: every
control command waits for admitted work to drain, so nothing an admission
relied on changes under it. On a replicated authority that lock is local to
one replica. A leader partitioned from the quorum keeps its lock, keeps its
last applied policy and owner rows, and would keep admitting work while the
surviving quorum elects a new leader and commits a revocation or a new
activation. The local view of a follower or learner is an applied view, not
a fresh read. Neither can establish that a right is current.

## What the pinned library does (openraft 0.9.25)

The argument below is made against these facts of the pinned source, not
against the general Raft literature:

- **Read barrier.** `Raft::ensure_linearizable` sends one empty
  `AppendEntries` to every voter *at the moment the core handles the
  request* and answers once a quorum (counting the leader itself) has
  replied; a `HigherVote` reply fails it (`core/raft_core.rs`,
  `handle_check_is_leader_request`). It then waits until the applied index
  reaches the read log id. It does **not** answer from an earlier quorum
  acknowledgement, and it does not refresh the leader's own lease
  bookkeeping.
- **Follower lease.** A follower that accepts an `AppendEntries` from its
  leader touches its vote timer at receive time
  (`vote_handler::update_vote`, `vote.touch(now)`). While
  `now <= vote_utime + election_timeout_max` it rejects every vote request
  (`engine_impl::handle_vote_req`; `leader_lease` is
  `election_timeout_max`), and it starts no election of its own before
  `vote_utime + election_timeout` with the timeout drawn from
  `[election_timeout_min, election_timeout_max]`.
- **Leader's own vote timer.** Only `update_vote` touches the timer, and a
  leader's vote does not change while it leads. Its timer therefore reads
  the instant it was elected, and once `election_timeout_max` has passed an
  established leader **grants** a vote to any candidate with a higher term
  and a log at least as long. The library's leader lease
  (`last_quorum_acked_time`, documented in `docs/data/leader-lease.md` as
  starting at *send* time) is metrics and heartbeat bookkeeping; it does
  not gate vote requests.

## The rule

An admission on a Raft-hosted authority is a **lease** granted by the host,
and only the host grants it:

1. `SourceAuthorityStore::admission` refuses on a hosted store
   (`FailedPrecondition`, "obtain a leased admission through the host").
   The direct command paths refuse too; only committed entries apply.
   The one crate-private exception is the recovery admission that opens
   a managed handle at start on the applied view; it admits no write, by
   name, and every write on the handle takes its own lease
   ([hosted owner writes](raft-hosting.md#hosted-owner-writes)).
2. `RaftHost::with_admission(principal, run)` takes the lease anchor —
   `AdmissionLease { anchor, anchor_wall, ttl }` — **before** it invokes
   the read barrier, then performs the barrier bounded by
   `election_timeout_max` (an isolated leader collects no quorum and
   refuses), then grants. The grant itself is refused when
   `anchor + ttl` has already passed: a continuation paused after the
   barrier, delayed acknowledgements, a slow catch-up to the read index or
   any scheduling delay consume the interval; none of them extends it.
   `RaftHost::lease` performs the same anchor, barrier, vote-hold
   extension and grant gate and returns them as an owned
   `LeasedAdmission` for work that runs on a blocking worker; the grant
   on another thread (`LeasedAdmission::admission`) measures the interval
   from the anchor, which is exactly the paused case above — the move
   between threads consumes the interval and never extends it.
   `with_admission` is that path with an immediate grant. The owned value
   itself takes no shared guard; only the granted `SourceAdmission`
   holds control commands off the store, from the grant to its drop.
3. Every admission method (`authorize`, `prepared_owner`,
   `activated_owner`, `admit_write`, `check_fresh`) checks the interval
   first; past it the admission admits nothing, by name.
4. `HostConfig::validate` requires
   `admission_lease_ms + clock_skew_ms ≤ election_timeout_min_ms`, and every
   transport RPC carries the group's timing agreement (heartbeat, election
   floor and ceiling, lease, skew); a peer presenting different values, or
   none, is refused before the library sees the call.
5. While an interval anchored at this node's latest barrier may still be
   open (`anchor + election_timeout_max` on this node's clock), the
   transport withholds this node's vote from every candidate
   (`LeaseHold`): the reply names the leader's current vote and grants
   nothing, and the request never reaches the library.

## Why the interval is defensible

Let `t0` be the anchor and `S` the instant the barrier's heartbeats were
sent. `t0 ≤ S` because the anchor is taken before the barrier is invoked
and the library sends only after the core handles the request; the
barrier never answers from an earlier acknowledgement. Every follower `f`
that acknowledged received the heartbeat at `r_f ≥ S` on its own clock,
offset from this node's by at most `clock_skew`, and from `r_f` on it
grants no vote before `r_f + election_timeout_max` and starts no election
before `r_f + election_timeout_min`.

A replacement leader needs votes from a majority `V`. `V` intersects the
acknowledging set `Q` (also a majority). If the intersection contains an
acknowledging follower, no member of `V` obtained that vote before
`S + election_timeout_max − clock_skew ≥ t0 + election_timeout_min − clock_skew
≥ t0 + admission_lease`. The only other way for `V` to intersect `Q` is
through the old leader itself, which the pinned library would let vote;
rule 5 closes that: the old leader withholds its vote until
`t0 + election_timeout_max`, past every lease it could have granted. A
revocation is committed by a replacement leader after its election, so no
revocation the surviving quorum commits precedes the expiry of any lease
the old leader issued. The bound holds whatever happens between the
barrier and the grant, because the interval is measured from `t0`, not
from the grant.

Assumptions, stated: monotonic clocks on every member run at the same rate
within the skew budget over one interval (a lease of at most one election
floor); a process or machine suspend is not counted by the monotonic
clock, so the lease also expires on the wall clock and a wall clock that
moved backwards under a lease refuses; election and lease values are equal
across the group (rule 4 enforces it on every RPC); membership is the
library's applied membership on each node (the barrier counts a quorum of
the effective configuration, joint configurations included).

## Read consistency

Three kinds of reads have three different guarantees; none is a substitute
for another:

| Read | Guarantee |
|---|---|
| Admission (`with_admission`, or `lease` with the grant on a worker) | linearizable, leased from an anchor taken before the barrier, refuses off-leader and refuses when the interval elapsed before the grant |
| Owner, policy and decision reads on `host.store()` | the applied view of this replica, which a snapshot install moves forward under the same handle; correct for audit and retry lookup, never for admitting a write |
| Map distribution (`published_map`, `subscribe_applied`) | revisioned and monotonic per consumer; may lag; a consumer refuses same-revision/different-content and generation conflicts |

## Source write boundary

A source write becomes authorized at exactly one point: the admission's
final check inside the source transaction, after the writer lock has been
acquired and every table write has been staged, immediately before the
commit (`DocumentCatalog::accept_as`, the `fence` argument;
`ActiveManagedCatalog::accept` passes `SourceAdmission::check_fresh`).

- Entry check: `admit_write(key, write_epoch, action)` at the start refuses
  early — current grant, owner ACTIVE under exactly this epoch, lease
  unexpired. It authorizes nothing by itself.
- Between entry and the final check the admission's shared guard holds
  every control command off this store, so policy and ownership cannot
  change under the write; only time passes. A pause, a wait for the writer
  lock or slow I/O in this window consumes the lease.
- Final check: `check_fresh` measures the interval again. A refusal leaves
  no durable change and no retry record, so a retry after a fresh admission
  is new work, not a replay.
- After the final check only the commit remains. A pause between the final
  check and durability is not bounded by anything in this process; a write
  that passed its final check may therefore become durable after the lease
  has lapsed. That is the residual, and it is named rather than hidden.
  The network path runs no lease check after the commit for the same
  reason: a write past its final check is durable under its epoch fence,
  and hiding its receipt would not make it less durable.
- The network path's entry check is `ActiveManagedCatalog::accept`'s
  `admit_write` under the worker's grant, after the transport gate
  (`Principals::authenticate` and `authorize(.., Ingest)`) and the
  request admission. The transport gate stays first; the authority
  admission is authoritative.

Contract for in-flight work, brought here for review: **an operation that
passed its final check may finish**; revocation takes effect for every
write whose final check happens after the interval has lapsed, and for
every new admission. The receipt discloses nothing about the lease; the
write is fenced against replacement by the owner's write epoch — a new
activation allocates a new epoch, and a former owner's writes are refused
by the epoch at every replica. Until replacement exists, expiry-only
replacement stays unavailable, and no path activates a second writer.
Closing the residual entirely needs a fencing token checked by the storage
at commit; the source catalog has none today, and adding one is a storage
contract change outside this checkpoint.

## Failure behaviour

- Leader isolated: the barrier fails or times out → admission refused
  (`Unavailable`). Leases granted before isolation expire within
  `admission_lease_ms` of their anchor; work admitted under them either
  passed its final check before expiry (and is fenced by the epoch on any
  later activation) or is refused at that check.
- Paused between barrier and grant: the grant is refused
  (`FailedPrecondition`, "interval elapsed before the grant").
- Revocation on the surviving quorum: applied on the new leader; the old
  leader cannot admit anything after its leases lapse; a retry of an old
  decision on any replica returns the recorded decision but discloses it
  only under the current grant (`read_policy` at each read).
- Attempted writes through the old side after a new leader exists: refused
  at admission (no barrier), and any in-flight lease expires before the new
  leader could have been elected.
- Vote request at a leader with an interval open: not granted, the leader
  stays leader, the request does not reach the library until the interval's
  ceiling has passed.

## Evidence and remaining gaps

Single node: the hosted store refuses local admission, the host grants
leased admissions after the read barrier, an elapsed interval is refused at
the grant, an expiring lease admits nothing once past, and
`HostConfig::validate` rejects a lease beyond the election floor.

Three voters over the tonic transport (`src/raft/transport_tests.rs`):

- "an isolated leader admits nothing and the surviving quorum revokes": a
  lease granted with a quorum admits; the leader isolated in both
  directions is replaced no sooner than the election floor; its barrier
  collects no quorum and refuses within `election_timeout_max_ms`; the
  survivors revoke while the old side shows the stale policy and grants
  nothing on it; after healing the old leader applies the revocation and
  still refuses as a follower.
- "a grant paused after the barrier is refused once its interval elapsed":
  a pause that resumes within the interval is granted with its anchor at
  or before the pause; the same pause held while the survivors elect and
  commit a revocation resumes into a refusal, never a fresh admission.
- "a leader withholds its vote while an admission interval may be open": a
  registered peer's vote request in a higher term with an equal log is not
  granted at the leader and the leader stays leader; past the ceiling the
  request reaches the library.
- Peer timing: a request without the timing agreement, or with different
  election or lease values, is refused by name.

Source write boundary (`src/document_catalog/managed/tests.rs`, "a write
paused before commit past its lease leaves no durable change"): a write
admitted at entry and paused inside its transaction past the lease is
refused at the final check, nothing is durable, no retry record exists, and
a pause that ends within the lease commits.

Hosted evidence (`src/raft/tests.rs`, `tests/control_raft_hosted_writes.rs`):
a lease taken on one thread grants on a blocking worker inside its
interval and admits an owner-side call; a lease kept past its interval
is refused at the grant with the interval elapsed before it and the
applied position unchanged. Over three voters through the hosted
service: a leader-served write commits with a receipt and its exact
retry replays; a follower names the leader in its refusal; a write
kept past its lease is denied at the grant and its retry commits as
new work, with no row and no retry record from the refused grant; an
isolated leader refuses inside the read bound and, healed, refuses as
a follower naming the successor. A restarted group recovers its
managed catalogs through the binary's recovery function with the first
member up, before any leader exists, and serves a write on the leader;
one member restarted while the others keep running recovers at start
and serves once it leads; a client that drops its call while the
worker is parked leaves the worker to commit under its permits, and
the exact retry replays the row; a transport policy changed under a
committed write withholds the receipt by name and the row stays
durable; recovery names a prepared source, a foreign authority and an
unrecorded activation, and a recovery admission admits no write, by
name.

Remaining: the residual between the final check and durability stays
open until a storage-side fencing token exists; Kimi's harness adds
deterministic scheduling around the pause hooks
(`RaftHost::arm_grant_gate`, `HostedDocumentWriteService::arm_grant_delay`,
`ActiveManagedCatalog::arm_precommit_pause`, all on `fault-injection`)
with crash and recovery variants.
