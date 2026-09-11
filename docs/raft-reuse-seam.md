# Where the generic Raft hosting ends and the source authority begins

Status: statement of the boundary as the code is on 2026-09-11, after Muse's review of the operator brief called for it. It changes no code. It names which layers a second domain would take as they are, which it would
replace, and where the cut would be made if a second domain were built.
No part of this is a plan to build one.

## The two contracts

The **wire contract** is what a client or a peer can rely on across
releases: the protos (`raft_transport.proto`, `raft_control.proto`,
`published_map.proto`, and `raft_operator.proto` once merged), the RPC
semantics the hosting and admission documents fix, and the status codes
and wording those documents name. Every request of the transport has a `protocol_version`; the operator service has one from Phase B on. A
change to any of these is a new protocol version, not an edit.

The **operational contract** is what an operator reads and may see change
between releases with a note in the README log: gauge names and labels,
log lines, CLI output shapes, the console pages.

## Layers that do not know the domain

These take `ControlRaft`'s types and no knowledge of source authorities, owners, epochs or catalogs. A second domain would keep them:

| Layer | Where | What it depends on |
|---|---|---|
| Log store | `src/raft/log_store.rs` | `RaftEntry`, `RaftVote`, `RaftLogId`, the group identity; entries are opaque bytes to it |
| Transport, identity, timing | `src/raft/transport.rs`: `PeerDirectory`, mTLS, the header check, the timing agreement, the lease hold, `GrantLease`, the isolation switch | the group identity, node ids, `HostConfig`'s timing values |
| Snapshot admission and staging | `src/raft/transport.rs` (`stage`), `src/raft/state_machine.rs` (`SnapshotStaging`, `PublishedImage`) | an image is a file with a digest, a length and a meta; validation of the image's *contents* is the domain's (below) |
| Host lifecycle and membership | `src/raft/host.rs`: `bootstrap_cluster`, `prepare_member`, `start_member`, `add_learner`, `promote`, `remove_member`, `shutdown`, the digest-rejection rebuild, `lease` and the forwarded lease | the library, the transport, a state machine that reports its applied position |
| Operator status and membership RPCs | `src/raft/operator_service.rs` (Phase A) | the host's accessors |
| The kit | `tests/control_adversarial/raft_kit.rs` | the above |

## Layers that are the source authority domain

| Layer | Where | What a second domain would replace it with |
|---|---|---|
| The state machine | `src/raft/state_machine.rs`: `ControlStateMachine` over `SourceAuthorityStore`, `apply` of `RaftProposal`, image build from the store, image validation on a probe copy | its own state machine over its own store, its own proposal type, its own image build and validation |
| Proposals and replies | `raft_control.proto`: `RaftProposal { principal, command }`, `RaftReply` | its own command and reply messages inside the same envelope |
| Admission, leases, epochs | `src/source_authority.rs` (`SourceAdmission`, `AdmissionLease`), `docs/raft-admission.md`, the write outcomes | whatever its writes need; the lease *mechanism* (anchor, barrier, hold, forward) is generic, the *rule* it enforces (owner ACTIVE under an epoch, a grant current) is the domain's |
| Managed catalogs and the hosted write service | `src/document_catalog/managed.rs`, `src/document_write_service/hosted.rs` | its own writers |
| The published map and the relay's source | `src/source_authority/map_feed.rs`, `relay_map.rs` | its own feed, if it has consumers |
| The owner lifecycle RPCs and the registry | Phase B | its own |

## Where the cut is today, and where it is not

The cut is real at the log store, the transport and the host: none of
them reads a `SourceAuthorityStore`. It is not yet a Rust boundary at the
state machine: `ControlRaft` binds the library's type config to
`RaftProposal` and `RaftReply`, and `RaftHost` keeps a `SharedStore` of the source authority. A second domain would generalize `ControlRaft` over
its proposal and reply types and give `RaftHost` a state-machine trait
with four obligations: apply an entry and return the reply; report the
applied position; build an image to a directory and report its digest;
validate and install an image. Everything above that trait stays.

Two places would need care in that generalization, and are named so the
work is not underestimated: the host's `store()` and `store_handle()` are
how the hosted write service and the relay's map source reach the domain,
and would become a domain-typed accessor; and the operator status message has no domain fields today, which is why it can stay generic, so a
domain's own status belongs on its own RPC, not on this one.

## What this requires of Phase B and C

No change that moves the boundary. Phase B's registry, lifecycle RPCs and
`ListRejections` sit on the domain side except `ListRejections`, which is
generic and belongs next to `GetMemberStatus`. Phase C's page reads both.
