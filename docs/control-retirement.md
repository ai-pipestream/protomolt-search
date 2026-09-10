# Legacy authority retirement

The local retirement adapter fences the JSON control authority before its
checkpoint can be imported into a transactional or replicated authority. It
does not implement that import, Raft, map publication, source activation or
document durability. No RPC or fleet command invokes it yet.

`SourceAuthorityStore::retire_legacy_control` requires an authenticated stable
principal supplied by the hosting adapter and current committed Admin rights
for the exact workspace/collection. Its format-1 request names an empty-owner
resource key, a 1..1024-byte operation ID, the SHA-256 of the exact canonical
`LegacyControlImport`, and nonzero destination control/policy revisions.
The first retirement checks both revisions and the live legacy checkpoint
under their respective locks. A memory-only legacy plane refuses.

The destination authority guard is held through the legacy commit, so a
concurrent policy revocation cannot cross that commit boundary. Lock order is
destination authority, then legacy state. Only the legacy file changes: the
destination's tables, policy, owners, decisions and revision remain unchanged.
Legacy I/O failures latch the legacy instance when publication may have
occurred; they do not latch an otherwise healthy destination store.

## Durable record and recovery

The adapter replaces the original JSON inode using a private create-new
candidate, file sync, atomic rename and directory sync. The persistent sidecar
ownership lock remains held across replacement. The frame is at most 17 MiB:
eight bytes `PCTRLRET`, 32 bytes of SHA-256 over the payload, then canonical
`LegacyControlRetirement` protobuf bytes. The record carries the destination
identity/incarnation, actor/resource/operation, exact request and complete
checkpoint (at most 16 MiB and 262,144 checkpoint wire fields). Existing proto
fields and legacy JSON semantics are unchanged.

The checkpoint retains lease credentials, actions, counters, policy and
history. Keep it private; it is not suitable for diagnostics, public map feeds
or logs. The opaque `RetiredLegacyControl` holder has no Debug/Clone and retains
the sidecar lock. Its record/bytes accessors are privileged local interfaces.
Checksums detect corruption; they do not authenticate administrator-controlled
files against malicious replacement.

After a successful retirement all legacy clones refuse authority operations.
Both legacy open modes refuse the retired path. An exact same-actor/resource/
operation/request retry may retrieve the record while the instance is alive,
provided current Admin rights still hold. Initial destination revision checks
are not repeated after retirement: unrelated committed work cannot strand an
already retired authority. Changed payload under the same operation refuses
with AlreadyExists; a different actor, resource, operation or target authority
refuses with FailedPrecondition (or PermissionDenied at policy admission).
These are retries of this legacy file's transition, not a destination-global
operation receipt. A future import must commit its own unique decision.

`recover_legacy_retirement` requires current resource Admin rights and the same
request/actor/authority. First drop every legacy clone and retirement holder.
Recovery reacquires exclusive file ownership, bounds the read, verifies the
checksum, canonical encoding, required fields, checkpoint schema/digest and
collection binding, then syncs the exact opened inode and parent directory.
It never bootstraps a missing path or converts live JSON into retirement.
Unknown/future/noncanonical records and corrupt or truncated frames refuse.

| Interruption | Legacy state | Recovery |
|---|---|---|
| Before rename | Original JSON remains authoritative | Reopen legacy state if necessary, check exact checkpoint again, retry retirement |
| Rename attempted, outcome uncertain | Current clones are fenced | Close them; inspect durable state through existing-open or retirement recovery, never bootstrap |
| Marker visible, before directory sync | Destination has not imported anything | Retirement recovery validates and syncs the visible marker before returning evidence |
| After sync, before caller receives result | Durable retirement record exists | Exact authorized recovery returns the same record |

An abrupt process exit before directory sync may recover either old JSON or
the retirement frame depending on filesystem durability. New import must wait
for a successfully synced retirement holder, so this uncertainty does not
authorize a second writer. Fault tests exercise process interruption, not a
claim of simulated power-loss durability on every filesystem.

## Remaining import obligations

Retirement is preparation. The next transactional import must bind this exact
record and its destination incarnation to one committed retry decision,
preserve all allocators/history, and include the full placement tree, derived
declarations and provider geometry absent from legacy JSON. The checkpoint's
16 MiB bound must not bypass the destination's existing 1 MiB command/2 MiB
record bounds: a separately bounded staged-import contract is required.

Before activation, hosting must quiesce old workers, reconcile pending actions
and verify per-shard epochs/readiness. Retirement cannot recall work already
sent to a data node. Automatic replacement on lease expiry remains unavailable
in the first managed-authority release. Phone source/index/WAL/snapshot bytes
remain on the phone; this checkpoint contains control metadata only.
