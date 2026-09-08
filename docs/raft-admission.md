# Admission under Raft: leases, isolation and revocation

Status: contract, 2026-09-08, with the single-node mechanism implemented
(`RaftHost::with_admission`, `SourceAdmission` leases, `HostConfig::validate`).
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

## The rule

An admission on a Raft-hosted authority is a **lease** granted by the host,
and only the host grants it:

1. `SourceAuthorityStore::admission` refuses on a hosted store
   (`FailedPrecondition`, "obtain a leased admission through the host").
   The direct command paths refuse too; only committed entries apply.
2. `RaftHost::with_admission(principal, run)` first performs
   `ensure_linearizable()`: the read barrier proves this node is the leader
   with a quorum that acknowledged it *now*, and that its applied state
   includes everything committed. Then it grants a `SourceAdmission` with a
   deadline `granted + admission_lease_ms` and runs the owner-side work
   under it.
3. Every admission method (`authorize`, `prepared_owner`, `activated_owner`,
   `admit_write`) checks the deadline first; past it the admission admits
   nothing, by name. The source commit that an admission protects must
   finish inside the lease; a write that could not is refused at its own
   commit boundary, never admitted late.
4. `HostConfig::validate` requires `admission_lease_ms + clock_skew_ms ≤
   election_timeout_min_ms`. A leader that was proven current at `granted`
   cannot be replaced before `granted + election_timeout_min` on any
   member's clock, so every lease it issued has expired (with the skew
   budget) before a new leader can commit anything. An isolated former
   leader therefore admits nothing a surviving quorum could have revoked.

Revocation committed on the surviving quorum takes effect for every
admission the moment the old leader's leases expire; a grant restored on a
stale replica restores nothing, because the stale replica cannot pass the
read barrier. A grant to a requester is only as fresh as the linearizable
read that admitted it.

## Read consistency

Three kinds of reads have three different guarantees; none is a substitute
for another:

| Read | Guarantee |
|---|---|
| Admission (`with_admission`) | linearizable, leased, refuses off-leader |
| Owner, policy and decision reads on `host.store()` | the applied view of this replica; correct for audit and retry lookup, never for admitting a write |
| Map distribution (`published_map`, `subscribe_applied`) | revisioned and monotonic per consumer; may lag; a consumer refuses same-revision/different-content and generation conflicts |

## Fencing at the data owner

The lease bounds the *admission*; the write epoch bounds the *data*. An
`ActiveManagedCatalog` writes only under `admit_write(key, write_epoch)`,
which requires the owner ACTIVE under exactly that epoch in the applied
state the read barrier just refreshed. When replacement exists, a new
activation allocates a new epoch, and a former owner's writes are refused by
the epoch at every replica; until then, expiry-only replacement stays
unavailable, and no path activates a second writer.

## Failure behaviour

- Leader isolated: `ensure_linearizable` fails or times out → admission
  refused (`Unavailable`). Leases granted before isolation expire within
  `admission_lease_ms`; work admitted under them either committed to the
  source before expiry (and is fenced by the epoch on any later activation)
  or is refused at its commit.
- Revocation on the surviving quorum: applied on the new leader; the old
  leader cannot admit anything after its leases lapse; a retry of an old
  decision on any replica returns the recorded decision but discloses it
  only under the current grant (`read_policy` at each read).
- Attempted writes through the old side after a new leader exists: refused
  at admission (no barrier), and any in-flight lease expires before the new
  leader could have committed.

## Evidence and remaining gaps

Implemented and tested single-node: the hosted store refuses local
admission, the host grants leased admissions after the read barrier, an
expired lease admits nothing, `HostConfig::validate` rejects a lease beyond
the election floor. Multi-node evidence — leader isolation, revocation on
the surviving quorum, writes attempted through the old side — needs the
tonic transport and a three-voter harness; those tests are the gate before
distributed owner writes are enabled, and the mechanism above is the
contract they hold to.
