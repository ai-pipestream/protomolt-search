# Collection control state

Status: implemented in `src/source_authority/control.rs` (store format 3),
records in `proto/ai/protomolt/search/storage/v1/control_state.proto`, audit
at open in `src/source_authority/recovery/control.rs`, tests in
`src/source_authority/control_tests.rs` and the `tests/control_*` targets.

A collection's control state is its current topology, retained history,
node registrations, shard replicas, pending placement actions, completed
action ids and configuration (placement tree, derived columns, provider
geometry and planner policy). The transactional `SourceAuthorityStore` holds
it as rows; the capacity planner, the map feed and observers read those rows.
Nothing in this state activates an owner, publishes a map or reconciles an
action.

This contract replaced the retire/checkpoint/chunked-import path that moved
state out of the JSON `DurableControlPlane`. That path had no clients and was
deleted with no compatibility reader: a store older than format 3 refuses to
open.

## The install command

`ControlInstallCommand` writes one collection's control state in one bounded
command and one durable redb transaction:

- `key` is the collection resource (an empty `owner_id`).
- `command_id` is 1..1024 bytes. It is actor-scoped and retryable exactly
  like a `SourceAuthorityCommand`, in the same namespace as source and
  capacity commands: an id used by one path cannot be reused by the other
  (a direct `FailedPrecondition`, nothing recorded).
- `expected_control_revision` and `expected_policy_revision` must equal the
  store's current revisions.
- `state` is a `ControlInstallState`, the whole fact set, inside the store's
  `max_command_bytes`.

The direct path is `SourceAuthorityStore::install_control`; a store hosted by
Raft refuses it and the caller proposes through `RaftHost::propose_install`,
whose committed entry applies through the same code (`replay_control_install`).

### Direct refusals

These return a `Status` and record nothing, because the command could never
apply:

- the command exceeds `max_command_bytes` (`ResourceExhausted`);
- the format, key, command id or authority identity is wrong;
- the state is invalid (`InvalidArgument`, prefixed `control install:`):
  zero allocators, a missing topology, a repeated generation, a reversed
  route range, unordered or duplicate nodes or replicas, a lease token that
  is zero, reused or reaches `next_token`, an action id that reaches
  `next_action`, unordered completed ids, or a configuration that leaves a
  choice implicit (a tree needs one route code per current route,
  `no_placement` needs none; a derived declaration needs its exact
  fingerprint);
- the principal lacks current Admin on the resource;
- the command id is already used with different content, or by another
  command path.

### Recorded decisions

Everything else is a durable `ControlInstallDecision` under the command's
operation key, so an exact retry returns it verbatim:

- **Accepted** (`code` 0): the rows are written, the control revision
  advances by one, and the decision carries a `ControlInstallReceipt` that
  counts what was written.
- **Stale** (`FailedPrecondition`): a stale install is a recorded refusal.
  The control or policy revision differs; nothing else changes.
- **Installed once** (`AlreadyExists`): a resource is installed once; a
  second install is a recorded AlreadyExists, whatever its content.
- **Full** (`ResourceExhausted`): the rows would pass `max_payload_bytes`.

Retry history is never evicted. A new command refuses directly when the
decision or payload capacity is full.

## Rows

| Table | Key | Value |
| --- | --- | --- |
| `source_authority_control_install_operations` | operation key | `ControlInstallOperation` (digest, command, decision) |
| `source_authority_control` | collection key | `ControlCollectionState` (receipt, allocators, generations, configuration) |
| `source_authority_control_topologies` | `ControlTopologyKey` | `ControlTopology`, one per current and retained generation |
| `source_authority_control_nodes` | `ControlNodeKey` | `ControlNode` (registration and residency) |
| `source_authority_control_replicas` | `ControlReplicaKey` | `ControlReplica` |
| `source_authority_control_actions` | `ControlActionKey` | `ControlAction`, always `UNRECONCILED` |
| `source_authority_control_completed` | `ControlActionKey` | `ControlCompletedAction` |

The meta key `control` holds the `ControlStoreHeader` counts. Installed
leases are observations: a lease token here authorizes nothing, and a
pending action is not consumed until reconciliation runs under this
authority. `control_snapshot` returns an immutable `ControlCollectionSnapshot`
without lease tokens and with a SHA-256 digest over its canonical bytes;
`control_install_decision` reads a stored decision.

## Audit at open

Recovery recomputes everything against the headers and refuses with
`DataLoss` on any disagreement; nothing is repaired. Each stored operation's
command must validate and hash to its digest, its decision must be well
formed (codes 0, 6, 8 or 9; a receipt only on an accepted install), and at
most one install per resource is accepted. Each control row must match its
accepted receipt, every listed generation must have its topology row, and the
per-resource node, replica, action and completed counts must equal the
receipt and, in total, the control header.
