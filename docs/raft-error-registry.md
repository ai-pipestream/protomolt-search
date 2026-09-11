# Raft error registry

Specification version: 1

Every named rejection the Raft documents describe, with the identifier
it carries as the `psearch-reason` metadata on its `Status`
(`src/raft/reasons.rs`). The gRPC code and the identifier are frozen;
the human message text is informative and may change. A test that
checks a rejection checks the code and the identifier, not the
sentence; where the message names a value (a position, a revision,
a node id), the test checks that value too.

The registry equality test (`src/raft/reasons.rs`) keeps the
identifier column below equal to the constants, in both directions:
a row without a constant and a constant without a row both fail.

Stability: `stable` rows never change their identifier or code.
`provisional` rows are reserved for the Phase A operator service,
which is not on this base yet; no site attaches them here.

## Transport answers (`src/raft/transport.rs`)

| reason | code | refusal | stability |
| ------ | ---- | ------- | --------- |
| transport.no_client_certificate | Unauthenticated | no client certificate from the cluster CA; the listener's TLS requires one, so this is a second check behind the handshake and no client reaches it | stable |
| transport.unregistered_certificate | Unauthenticated | certificate bound to no member | stable |
| transport.bad_header | InvalidArgument | no header, or a protocol version this node does not speak | stable |
| transport.wrong_group | PermissionDenied | another group named | stable |
| transport.misaddressed | PermissionDenied | addressed to another node | stable |
| transport.identity_mismatch | PermissionDenied | claimed node is not the registered one | stable |
| transport.isolated | Unavailable | isolated peer under fault injection | stable |
| transport.not_heard | Unavailable | deaf peer under fault injection | stable |
| transport.timing_mismatch | FailedPrecondition | no or differing timing agreement | stable |
| transport.no_linearizable_read | Unavailable | no quorum for the lease barrier | stable |
| snapshot.over_bound | ResourceExhausted | announced image beyond the receiver bound | stable |
| snapshot.digest | DataLoss | image bytes differ from the announced digest | stable |
| snapshot.stale_continuation | FailedPrecondition | chunk continues a transfer the receiver dropped | stable |
| snapshot.transfer_bound | FailedPrecondition | another peer's chunk on a bound transfer; names the bound node | stable |
| snapshot.over_announced | InvalidArgument | a chunk past the announced length, or a transfer that ended short of it; the transfer is dropped | stable |
| snapshot.membership_mismatch | DataLoss | image membership differs from the snapshot meta | stable |
| snapshot.position_mismatch | DataLoss | image applied position differs from the snapshot meta | stable |

## Leases (`src/raft/host.rs`, `src/raft/transport.rs`, `src/source_authority.rs`)

| reason | code | refusal | stability |
| ------ | ---- | ------- | --------- |
| lease.not_leader | Unavailable | addressed member is not the leader; names the leader | stable |
| lease.no_known_leader | Unavailable | no leader known to forward to | stable |
| lease.no_transport | Unavailable | build or host has no transport to forward with | stable |
| lease.no_linearizable_read | Unavailable | no quorum for the admission barrier | stable |
| lease.leader_unreachable | Unavailable | forwarded ask timed out or never connected | stable |
| lease.read_position_behind | Unavailable | member had not applied the read position inside the interval | stable |
| lease.interval_elapsed | FailedPrecondition | grant or check ran past the anchored interval | stable |

## Membership (`src/raft/host.rs`, `src/source_authority.rs`)

| reason | code | refusal | stability |
| ------ | ---- | ------- | --------- |
| membership.not_networked | FailedPrecondition | member has no directory to change | stable |
| membership.unregistered_certificate | FailedPrecondition | no certificate bound before the join | stable |
| membership.bad_address | InvalidArgument | dial address missing or overlong | stable |
| membership.no_learners | InvalidArgument | promotion named no learners | stable |
| membership.change_rejected | FailedPrecondition | the library refused the change | stable |
| membership.awaiting_snapshot | FailedPrecondition | prepared store awaits the group's first snapshot | stable |

## Admission and recovery (`src/source_authority.rs`, `src/document_catalog/managed.rs`, `src/document_write_service/hosted.rs`)

| reason | code | refusal | stability |
| ------ | ---- | ------- | --------- |
| admission.epoch_moved | FailedPrecondition | owner not ACTIVE under the write epoch | stable |
| admission.not_active | FailedPrecondition | catalog prepared, not active at recovery | stable |
| admission.foreign_authority | FailedPrecondition | admission or binding names another authority | stable |
| admission.foreign_collection | FailedPrecondition | binding names another collection | stable |
| admission.unrecorded_activation | FailedPrecondition | activation the store does not record | stable |

## Write outcomes (`src/document_catalog/`)

| reason | code | refusal | stability |
| ------ | ---- | ------- | --------- |
| outcome.fenced | FailedPrecondition | durable write settled against a gone right; names version, sequence, revision, epoch | stable |
| outcome.no_operation | NotFound | no operation with this id to settle | stable |
| outcome.unknown_outcome | DataLoss | record carries an outcome the code does not know | stable |
| outcome.version_unknown | DataLoss | operation names a version the catalog does not hold | stable |
| outcome.version_mismatch | Aborted | expected version is not the head | stable |
| outcome.operation_reused | AlreadyExists | operation id used for another write | stable |
| outcome.unconfirmed | Unavailable | durable write no fresh lease settles; names version and sequence | stable |

## Receipt delivery (`src/document_write_service/hosted.rs`)

| reason | code | refusal | stability |
| ------ | ---- | ------- | --------- |
| receipt.policy_changed | PermissionDenied | transport policy changed under a committed write; receipt withheld | stable |

## Operator routes (provisional)

| reason | code | refusal | stability |
| ------ | ---- | ------- | --------- |
| operator.no_membership | Unauthenticated | no verified cluster membership | provisional |
| operator.not_leader | Unavailable | addressed member is not the leader; names the leader | provisional |
