# Admission under Raft: leases, isolation and revocation

Specification version: 1

Status: contract, 2026-09-08, revised the same day after Astra's review of
240be9f (A1, A2); the hosted write path landed 2026-09-11. The mechanism is
implemented (`RaftHost::with_admission`, `RaftHost::lease`,
`LeasedAdmission`, `AdmissionLease`, `LeaseHold`, `HostConfig::validate`,
the timing agreement on every transport RPC, the pre-commit fence in
`DocumentCatalog::accept_as`) with three-voter evidence over the tonic
transport. Member processes serve owner writes through the hosted write
service, whose every admission comes from the host's lease
([hosted owner writes](raft-hosting.md#hosted-owner-writes)). Since the
commit fence (2026-09-11) the storage records every write's outcome at
durability: accepted, unconfirmed, or fenced by name
(`docs/document-writes.md`, "Write outcomes").

It answers one question before any owner write is admitted through a
replicated authority: what makes an admission authoritative when the node
that granted it may be isolated, and when the surviving quorum may have
revoked the right it relied on.

## Fault model and invariants

The argument below holds under crash-recovery with stable storage: a
member fails by stopping and restarts from its persisted log, store and
applied position. Members are not Byzantine: a member that answers runs
this code and its persisted state is what it wrote. Clocks on every
member run at the same rate within a skew bound `S` agreed at join and
validated on every RPC. The network may drop, delay, duplicate and
partition packets, but TLS means it does not corrupt or forge them: a
byte that arrives is a byte a member sent.

The invariants, each followed by the mechanism that maintains it and
the test that pins it:

- I1: No admission interval is ever extended; remaining time is
  computed only from the anchor on the anchoring member's monotonic
  clock, and a suspend detected by the wall clock, or a backwards step
  of the wall clock, refuses instead of extending. (`SourceAdmission::fresh`
  checks the monotonic deadline and the wall clock together in
  `src/source_authority.rs`; `src/raft/transport_tests.rs`, "a grant
  paused after the barrier is refused once its interval elapsed", and
  `tests/control_raft_hosted_writes.rs`, the lease kept past its
  interval.)
- I2: `election_timeout_max >= admission_lease + clock_skew` holds on
  every member, so every granted admission lies inside a window in
  which the granting leader withholds its vote; a forwarded lease is
  inside the leader's hold because the hold is extended from the
  request's receipt. (`HostConfig::validate` requires the stronger
  `admission_lease_ms + clock_skew_ms <= election_timeout_min_ms`, which
  implies the invariant since the ceiling is above the floor, and every
  transport RPC carries the timing agreement and refuses a peer that
  presents different values or none; `src/raft/transport_tests.rs`, the
  configuration and peer-timing refusals.)
- I3: No admission is granted on an applied view older than the
  position the barrier read; on a member that does not lead, the grant
  waits for that position inside the interval or rejects. (The
  `GrantLease` answer carries the position the leader's barrier read
  and the asking member applies up to it within its own interval in
  `src/raft/host.rs`, else `lease.read_position_behind`;
  `tests/control_raft_hosted_writes.rs`, the member behind the read
  position, and `src/raft/transport_tests.rs`, the linearizable-read
  refusal.)
- I4: A write is accepted only if its right is current when its
  acceptance is declared; a settlement fences only on a right found
  revoked under a lease still open, never on the lease's own lapse.
  (The final `check_fresh` inside the source transaction and the
  post-commit judgement under a fresh lease in
  `src/document_catalog/outcome.rs`; `tests/control_raft_hosted_writes.rs`,
  the unconfirmed and fenced settlements.)
- I5: The outcome of a write moves forward only: ACCEPTED and FENCED
  are final, and UNCONFIRMED becomes ACCEPTED or FENCED and nothing
  else. (The settlement transitions in `src/document_catalog/outcome.rs`,
  and the transition table in `docs/document-writes.md`, "Write outcomes";
  `src/document_catalog/managed/tests.rs` and
  `tests/control_raft_hosted_writes.rs`.)

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

1. `SourceAuthorityStore::admission` MUST refuse on a hosted store
   (`FailedPrecondition`, "obtain a leased admission through the host"),
   and the direct command paths MUST refuse too;
   only committed entries apply. The one crate-private exception is the
   recovery admission that opens a managed handle at start on the
   applied view; it MUST admit no write, by name, and every write on
   the handle MUST take its own lease
   ([hosted owner writes](raft-hosting.md#hosted-owner-writes)).
   (`src/raft/tests.rs`,
   `a_hosted_store_refuses_direct_mutation_and_local_admission`, fails if
   the direct path admits.)
2. `RaftHost::lease(principal)` (and `with_admission` over it) MUST
   take the lease anchor — `AdmissionLease { anchor, anchor_wall, ttl }`
   — **before** it invokes the read barrier, then perform the barrier
   bounded by `election_timeout_max` (an isolated leader collects no
   quorum and refuses), then grant. On a member that does not lead the
   barrier is the leader's, asked for over the transport
   (`RaftTransport.GrantLease`), and the grant waits until this member
   has applied the position that barrier read; "A lease forwarded from
   the leader" below. The grant itself MUST be refused when
   `anchor + ttl` has already passed (`lease.interval_elapsed`): a
   continuation paused after the barrier, delayed acknowledgements, a
   slow catch-up to the read index or any scheduling delay consume the
   interval; none of them extends it (I1). `RaftHost::lease` performs
   the same anchor, barrier, vote-hold extension and grant gate and
   returns them as an owned `LeasedAdmission` for work that runs on a
   blocking worker; the grant on another thread
   (`LeasedAdmission::admission`) MUST measure the interval from the
   anchor, which is exactly the paused case above — the move between
   threads consumes the interval and never extends it (I1).
   `with_admission` is that path with an immediate grant. The owned value
   itself takes no shared guard; only the granted `SourceAdmission`
   holds control commands off the store, from the grant to its drop.
   (`src/raft/transport_tests.rs`,
   `a_grant_paused_after_the_barrier_is_refused_once_its_interval_elapsed`,
   and `src/raft/tests.rs`,
   `a_lease_taken_on_one_thread_grants_on_another_within_its_interval`,
   fail if the anchor moves or the interval extends.)
3. Every admission method (`authorize`, `prepared_owner`,
   `activated_owner`, `admit_write`, `check_fresh`) MUST check the
   interval first; past it the admission MUST admit nothing, by name
   (`lease.interval_elapsed`, I1). (`src/raft/tests.rs`,
   `a_lease_kept_past_its_interval_is_rejected_at_the_grant`, fails if an
   expired admission admits.)
4. `HostConfig::validate` MUST require
   `admission_lease_ms + clock_skew_ms ≤ election_timeout_min_ms`, and every
   transport RPC MUST carry the group's timing agreement (heartbeat, election
   floor and ceiling, lease, skew); a peer presenting different values, or
   none, MUST be refused (`transport.timing_mismatch`) before the library
   sees the call (I2). (`src/raft/tests.rs`, the lease-beyond-the-election-floor refusal,
   and `src/raft/transport_tests.rs`,
   `peer_identity_binds_certificate_group_and_node`, fail if an
   unvalidated configuration or an untimed peer is admitted.)
5. While an interval anchored at this node's latest barrier may still be
   open (`anchor + election_timeout_max` on this node's clock), the
   transport MUST withhold this node's vote from every candidate
   (`LeaseHold`, I2): the reply names the leader's current vote and grants
   nothing, and the request never reaches the library.
   (`src/raft/transport_tests.rs`,
   `a_leader_withholds_its_vote_while_an_admission_interval_may_be_open`,
   fails if the vote is granted.)

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

## A lease forwarded from the leader

A member that does not lead cannot run the barrier: the library answers
its `ensure_linearizable` with the leader it believes in. `RaftHost::lease`
then asks that leader (`RaftTransport.GrantLease`, under the same
certificate, group and timing checks as every other RPC) and the leader
runs exactly the barrier it runs for its own lease, extends its own vote
hold from the instant it received the request, and answers with the
position the barrier read. The asking member grants once it has applied
that position, bounded by what remains of its interval, so the applied
view it grants on is at least as fresh as the leader's was at the
barrier; a member that has fallen behind and cannot get there inside the
interval MUST be rejected by name (`lease.read_position_behind`), and
no admission MUST be granted on a stale view (I3).
(`tests/control_raft_hosted_writes.rs`,
   `a_forwarded_lease_grants_only_once_the_member_applied_the_leaders_read_position`,
   fails if a stale view grants.)

The interval is the one anchored on the asking member **before** it
asked, on its own monotonic clock. Let `t0'` be that anchor, `t0` the
leader's anchor at receipt and `S` the send of the barrier's heartbeats:
`t0' ≤ t0 ≤ S`, so every bound in "Why the interval is defensible" holds
with `t0'` in place of `t0`, and the extra hop only shortens the useful
interval; no clock of the leader's enters the member's arithmetic, and
the skew budget is the one the group already agreed. The leader's hold
covers `t0 + election_timeout_max ≥ t0' + admission_lease`, so a lease
it forwarded is inside the interval its vote is withheld for. The asking
member extends its own hold too, which is inert while it does not lead
and correct the moment it does.

A leader that stopped leading between the library's answer and the
request MUST reject naming the leader it now believes in
(`lease.not_leader`), and the member's caller retries; a leader that
cannot be reached, or does not answer inside the read bound, MUST be
`Unavailable` naming the forwarded lease (`lease.leader_unreachable`).
No member forwards a proposal: writes to the authority still go to the
leader, and a member that does not lead MUST name it
(`lease.not_leader`, I3). (`tests/control_raft_hosted_writes.rs`,
   `a_write_through_a_follower_is_admitted_under_a_lease_forwarded_from_the_leader`,
   fails if the forward names nothing or a proposal is forwarded.)

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
  check and durability is not bounded by anything in this process (a
  stalled sync is the ordinary case), so a write that passed its final
  check may become durable after the lease has lapsed. Nothing inside one
  process can prevent that: the process that would check is the process
  that is stalled. What the storage can do is judge the write once more
  when the commit returns, and record the judgement with the row. That is
  the outcome: still inside the lease, the write is ACCEPTED, and the
  lease argument above makes its right current at durability; past it,
  the row is marked UNCONFIRMED in a transaction of its own, before the
  caller learns of it, and is settled under a fresh lease, whose grant is
  a linearizable read of the current policy and ownership. Settled with
  the actor's right current, the write is ACCEPTED. Settled with the right
  gone, the write is FENCED at the control revision this replica has
  applied: the row stays in the history, marked, and every retry replays
  the rejection by name. The receipt of an accepted write names the epoch
  it committed under.
- The network path's entry check is `ActiveManagedCatalog::accept`'s
  `admit_write` under the worker's grant, after the transport gate
  (`Principals::authenticate` and `authorize(.., Ingest)`) and the
  request admission. The transport gate stays first; the authority
  admission is authoritative.

Contract for in-flight work: **an operation that passed its final check
may become durable, and it MUST be accepted only if its right is current
when its acceptance is declared (I4).** Revocation takes effect for every
write whose final check happens after the interval has lapsed, for every
new admission, and for every write that became durable after its interval
lapsed and is settled after the revocation: such a write MUST be fenced
(`outcome.fenced`), its receipt MUST be a rejection, and its row MUST be
marked. The receipt discloses nothing about the lease beyond the epoch;
the write is fenced against replacement by the owner's write epoch,
which a new activation moves, and by the outcome at every replica that
reads the history. Until replacement exists, expiry-only replacement
stays unavailable, and no path activates a second writer; the fence
therefore bites on a revoked grant today and on a moved epoch when
replacement exists. (`tests/control_raft_hosted_writes.rs`,
   `an_unconfirmed_write_whose_actor_was_revoked_meanwhile_is_fenced`,
   fails if a write with a gone right is accepted.)

The residual that remains, named: a crash between the commit's return and
the UNCONFIRMED mark leaves a row recorded ACCEPTED whose lease may have
lapsed before durability. On restart the catalog recovers only if its
owner is still ACTIVE under the same epoch, in which case the row's right
was never lost; if the owner was replaced, recovery rejects the catalog
as a whole and no reader serves it. What is not decided in that window is
a late row under a grant revoked in it, on a catalog whose owner is
unchanged; that window is the instruction between the sync's return and
the next durable transaction, and it is stated here rather than closed.

## Failure behaviour

Every bullet is a MUST the evidence below pins; the lease lifecycle
table follows the last one.

- Leader isolated: the barrier MUST fail or time out → admission refused
  (`Unavailable`, `lease.no_linearizable_read`). Leases granted before
  isolation MUST expire within `admission_lease_ms` of their anchor (I1);
  work admitted under them either passed its final check before expiry
  (and is fenced by the epoch on any later activation, I4) or is refused
  at that check.
- Paused between barrier and grant: the grant MUST be refused
  (`FailedPrecondition`, `lease.interval_elapsed`, I1).
- Revocation on the surviving quorum: applied on the new leader; the old
  leader MUST NOT admit anything after its leases lapse; a retry of an old
  decision on any replica MUST return the recorded decision but disclose it
  only under the current grant (`read_policy` at each read).
- Attempted writes through the old side after a new leader exists: MUST be
  refused at admission (no barrier), and any in-flight lease MUST expire
  before the new leader could have been elected (I2).
- Vote request at a leader with an interval open: MUST NOT be granted, the
  leader stays leader, the request MUST NOT reach the library until the
  interval's ceiling has passed (I2). A lease the leader forwarded counts:
  its hold is extended from the request's receipt.
- A member that does not lead, cut from the leader: the forwarded lease
  cannot be asked for, `Unavailable` naming it
  (`lease.leader_unreachable`). Behind the leader's read position past its
  interval: MUST be rejected by name (`lease.read_position_behind`, I3),
  no grant on the stale view.

### Lease lifecycle

| State | How it is entered | How it is left | Test |
| ----- | ----------------- | -------------- | ---- |
| anchored | `RaftHost::lease` takes the anchor before the barrier | the barrier answers, or the interval elapses first | `src/raft/transport_tests.rs`, the paused grant |
| barrier passed | a quorum acknowledged inside `election_timeout_max`; on a member that does not lead, the leader's barrier answered over `GrantLease` | the grant runs, or the interval elapses first | `tests/control_raft_hosted_writes.rs`, the forwarded lease |
| granted | the grant gate found `anchor + ttl` still open and, off-leader, the read position applied | the interval elapses, or the admission is dropped | `src/raft/tests.rs`, the worker grant |
| lapsed | the monotonic deadline, the wall clock, or a backwards wall-clock step passed the anchor | terminal: no grant, no admission, no settlement moves it back (I1, I5) | `src/raft/tests.rs`, expired admission admits nothing |

A local admission and a forwarded admission share the lifecycle; only
the barrier differs (own vs. the leader's). The grant gate in both is
the anchor on the granting member's own monotonic clock (I1).

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
durable; recovery names a prepared source, a foreign authority, a foreign
collection and an unrecorded activation, and a recovery admission
admits no write, by name.

Write outcomes (`src/document_catalog/managed/tests.rs`,
`tests/control_raft_hosted_writes.rs`): a write whose commit returns after
its lease lapsed is unconfirmed, its record marked, until a fresh lease
settles it; a lapsed admission and another actor's admission settle
nothing, by name; settled with the right current it is accepted and the
retry replays it; a retry that finds the record unconfirmed settles it
itself; settled after the actor's grant was revoked it is fenced at the
applied control revision, the retry replays the rejection before any
entry check, the row stays in the history marked and remains the head of
its key, and the actor granted again writes the next version on from it.
A lease forwarded from the leader (`tests/control_raft_hosted_writes.rs`):
a write through a member that does not lead is admitted under a lease
forwarded from the leader and its retry replays, the member still not
leading; cut from the leader, the same member rejects naming the
forwarded lease; a member that stopped hearing the leader while the
leader and the third voter committed a revocation is rejected inside its
interval naming the position it has not applied, and once healed and
caught up denies the revoked actor on the merits; the healed old leader
of the isolation scenario denies the revoked actor under a lease
forwarded from its successor.

Over three voters through the hosted service: a write durable after its
lease lapsed is settled on the same call under a fresh lease; one the
leader cannot settle, every link cut, is named durable and unconfirmed
and the exact retry settles it once the member leads again; one whose
actor the surviving quorum revoked meanwhile is fenced at the revision
the healed leader applied, stays fenced after the actor is granted again,
and is marked in the history.

Remaining: the crash window between the commit's return and the
UNCONFIRMED mark, named under "Source write boundary"; Kimi's harness
adds deterministic scheduling around the pause hooks
(`RaftHost::arm_grant_gate`, `HostedDocumentWriteService::arm_grant_delay`,
`ActiveManagedCatalog::arm_precommit_pause`,
`ActiveManagedCatalog::arm_postcommit_pause`, all on `fault-injection`)
with crash and recovery variants.

## Changelog

- v1: the specification pass — requirement words on "The rule", the
  forwarded lease, the in-flight contract and "Failure behaviour", the
  fault model and invariants I1–I5, the lease lifecycle table, and the
  conformance declaration.
