# Transactional source-owner control state

The foundations branch implements a server-side `SourceAuthorityStore` with a
dedicated redb database and the protobuf contract in
`proto/ai/protomolt/search/storage/v1/source_authority.proto`. It is the first
transactional part of the accepted [source activation](source-authority-activation.md)
and [replicated control](raft-control-design.md) design. It has no public RPC,
CLI construction, Raft adapter, source-catalog admission or fleet wiring.

## Identity and preparation

Bootstrap explicitly supplies an immutable group identity and authority
incarnation, committed access policy and capacity limits. Recovery requires that
same identity and an existing valid database. Missing, empty, wrong-format or
inconsistent state never creates a new authority. The parent directory must
already exist. Creation uses a private file and holds an exclusive open-file
lock for the lifetime of all store clones.

An owner key contains workspace, collection and the logical source-owner ID.
Two devices can therefore retain independent owners within one collection.
The intended target separately identifies its host node, storage incarnation,
source history and residency. It has no filesystem path or document bytes.
A process incarnation is a different future readiness input; it is not folded
into storage identity. The current access-policy collection-name resolution
rules continue to apply.

`PrepareSourceOwner` creates a pending record. `CancelPreparedSourceOwner`
retains its generation and history. A new preparation after cancellation
advances the generation and needs a new workflow ID. Used workflows are never
released for reuse. History and residency remain fixed; a device-local owner
cannot move to a server or another device, even after cancellation.

Neither phase admits writes, authorizes installation, retires a live catalog
or publishes a shard map. Initial readiness must still bind verified storage
to a committed decision before any managed writer can open. Replacement still
requires retirement, seal, verified staging, activation and readiness; there
is no timeout or lease-expiry shortcut.

## Atomic decisions and permissions

The embedding server supplies the authenticated stable principal. Passing an
arbitrary client-supplied actor to this Rust API would bypass authentication;
the API itself authenticates no credential and exposes no network route.
Every operation checks that actor's current explicit Admin grant against the
committed policy. Search and Ingest grants do not imply administration.

A command carries the authority identity, exact resource/owner key, command ID,
expected control revision, expected policy revision and expected ownership
generation. The retry key adds the authenticated actor. Workflow IDs link
multiple phase commands and are separate from each command's idempotency ID.
The server hashes the canonical version-1 command encoding under the
`protomolt.source-authority.command.v1` domain; caller-supplied hashes are not
trusted. These internal storage records require canonical encodings and refuse
unknown fields, unlike the preserved original document payloads.

One `Durability::Immediate` transaction records the transition, its response,
retry key, workflow reservation and accounting. Business precondition failures
are stored responses with a nonzero gRPC status code; they do not advance the
control revision. Malformed envelopes, failed admission, wrong authority,
capacity and storage errors return a `Status` instead of a recorded decision.
Exact retries return the original response without reapplying it. Changed
content under a used actor/resource/command key refuses.

Current permission is checked before looking up a retry. Revocation therefore
prevents disclosure of its saved result. A policy command that revokes its own
issuer still commits its decision, but the response is suppressed until the
issuer again has current administration permission. A retry after regrant
returns that decision without applying the old policy again.

`ReplaceSourceCollectionGrants` changes only its authorized collection's
grants. It cannot rebind resources or edit another collection. Existing
document and field restrictions use the same validator as the search policy.
The policy and control revisions advance in the same transaction. The pure
transition function uses committed policy and command inputs only: no clock,
filesystem, live policy callback, network request or catalog operation.
This committed policy governs the control store's operations. Publication to
serving authorizers and remote enforcement acknowledgements remain integration
work.

## Recovery and capacity

Each store clone shares serialization and a failure latch. A storage failure
closes the instance; old clones do not reactivate when another instance opens.
An error after commit may have persisted the result. Recovery reopens and
validates that result rather than inventing a new command or restoring an old
in-memory clone. The existing JSON control adapter remains separately fenced.

Recovery checks versions, canonical messages, identities, request digests,
response semantics, table counts and payload accounting. Owner records point
to their last durable decision and retained workflow reservation. A private
8 MiB database cache and individually bounded records avoid loading the whole
owner/decision history into memory during validation.

Limits bound owner count, retained decision count, command size and logical
payload bytes. Payload accounting includes policy bytes and all owner,
decision and workflow table keys and values; it excludes the fixed metadata
header and redb page overhead. This is not a process-RSS or physical-disk quota.
Capacity exhaustion refuses new work without reserving the command ID or
evicting retry history. Existing authorized retries remain readable at capacity.

## Remaining integration

The source-owner store's revision does not claim atomicity with the legacy JSON
topology authority. Their states must converge into the same transactional
control machine before managed activation and map publication are exposed.
The source database remains separate: durable intents and completion facts
must cover every cross-database crash boundary.

Still required are verified initial readiness, managed catalog bindings and
admission guards, owner retirement execution and evidence, replacement phases,
published owner metadata, remote revocation enforcement, OpenRaft log/application
storage and transport, and runtime integration. Phones remain outside the
control consensus group and keep all source/index/WAL/snapshot bytes locally.
The preparation kernel is not a completed managed-write or Raft implementation.

## Validation (2026-09-08)

The full gate on the foundations worktree based on `f327f69` passed: 709 library
tests, 150 integration targets (903 top-level tests and one nested worker),
13 embedded tests, two IVF tests and nine Python protobuf-oracle tests. All five
Android/iOS target checks, tests/examples compilation, formatting and vendored
protobuf checks passed. The three mobile-only unused-item warnings were already
present in the baseline. All 658 tracked and untracked inputs remained identical
through the gate; only this validation record and the README were amended after it.

The new authority file is the only protobuf addition. All 21 prior proto files,
the original wire fixtures and the normalized pre-existing descriptors remain
identical to `f327f69`. Literal owner-key and actor-scoped operation-key byte
fixtures pin both encoding and decoding of the version-1 storage layout.

The 15 authority tests cover retained workflows, device residency, actor/owner
namespaces, current permissions on retries, self-revocation, capacity rollback,
policy-revision exhaustion, concurrent exact retries, abrupt process exit around
commit, shared failure closure and inconsistent recovery records. A regression
first reproduced the pure transition accepting state from another logical owner;
the transition now refuses that mismatched key before evaluating a command.

The full suite ran under `MemoryMax=8G`, `MemorySwapMax=0` and two Cargo build
jobs. Its cgroup peak reached 8,589,934,592 bytes with memory-limit reclaim events,
zero swap and zero OOM events. This is validation-scope accounting, not a measured
production bound. Raw evidence is retained locally under
`/tmp/psearch-source-authority-{red,green,full}-*`; no fleet was changed.
