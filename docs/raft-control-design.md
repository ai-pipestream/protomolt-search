# Replicated control authority with OpenRaft

Status: proposed for Fable's review, 2026-09-05. Design baseline: main
`c094125ca2572e47e383a0060ee8c6f7e6233570`, including the revised scale-out
reservation at `f2ff569`. No Raft dependency, wire allocation, migration or
runtime implementation is introduced by this note.

The foundations work owns the replicated state machine, durable storage,
OpenRaft transport adapter and recovery. Fable owns relay consumers and their
generation checks. Implementation starts after the budget branch merges and
this design is reviewed. The current relay and scan-rate work keeps its single
control authority and does not wait for Raft.

## Scope and deployment

Use an established OpenRaft release, initially evaluated against **0.9.25**,
the version of the official API documentation inspected for this design.
Pin the selected dependency and storage/wire format before implementation;
changing either requires compatibility tests. The library supplies consensus;
our code supplies application semantics, storage and transport
([integration guide](https://docs.rs/openraft/latest/openraft/docs/getting_started/index.html)).

The initial production shape is one control group per administrative search
cluster, with three explicitly configured server voters in separate failure
domains. Collections are namespaced state within that group. Multiple isolated
administrative clusters use separate group IDs and stores. Do not create a Raft
group per data shard or add voters automatically as collections grow. Five
voters is an operator choice; one voter is useful for tests and migration but
does not provide machine-failure availability.

Phones are neither voters nor control learners. Their documents, catalog
payloads, indexes, vectors, WAL and snapshots remain on their originating
device. Device residency and participation metadata may be control metadata;
the source bytes never enter the control log or its snapshots. A device's
absence cannot trigger a copy or replacement-owner action.

Make the integration an optional server feature, with the network adapter
requiring `net`. The embedded library retains its existing no-network dependency
gate and does not acquire OpenRaft, a background consensus task or a listener.

## Current code that must be separated

`DurableControlPlane` in [control_plane.rs](../src/control_plane.rs) combines
an in-memory mutex, JSON persistence and `ControlPolicy`. `StoredState` owns
revision/token/action counters, topology/history, node leases, replica reports,
pending actions and completed action IDs. The service handlers sometimes
perform several durable changes (`ReportShard`, then reconciliation), then
publish to a coordinator. This becomes one deterministic command transaction
where the public operation requires atomic behavior.

Two existing limitations matter to the handoff:

- `StoredTopology`/`StoredRoute` omit the placement tree and route placement
  codes. `publish_current_topology` therefore refuses placed topologies.
  `PublishTopologyRequest` carries the full tree, but `ClusterPlan` does not.
  Replication must preserve the complete map, not reproduce this omission.
- The coordinator's write freeze is a process-local guard and random token.
  It is not a durable, distributed cutover transaction and cannot survive
  control leader failover as an authority to publish or move bytes.

Extract a pure transition function over an explicit state value. Neither that
function nor OpenRaft's apply callback may perform network calls, read clocks,
inspect live shard files, publish a coordinator map or launch placement work.
The existing single-authority adapter should use the same transition function
so its behavior remains testable without Raft.

## Replicated state and commands

Each group has an immutable cluster ID and authority incarnation. Each
collection binds its workspace/resource identity explicitly. Replicate:

- The complete published topology, predicate-partition tree and shard codes,
  topology generation, bounded history and canonical map digest.
- Control revision, token/action/write-epoch allocators and policy version.
- Node identities/incarnations, registration leases, residency restrictions,
  capacity observations and replica facts with their generations/watermarks.
- Pending placement intents, durable preparation/completion facts and terminal
  decisions. Preserve completed action payload fingerprints, not only an ID set.
- Control-operation deduplication results, retry fences and the committed
  policy/configuration inputs needed to replay the same decisions.

Keep document source records, document history, index images and scan statistics
samples out of this log. Only the bounded capacity summary needed for planning
belongs here. [DocumentCatalog](document-writes.md) remains a separate logical
authority. A replicated control transaction must never claim that a document
write was accepted, searchable or durable. Catalog replication/publication
needs its own transaction and recovery design; matching revision numbers do
not make two independent databases atomic.

Application commands use versioned protobuf messages under `ai.protomolt.search`
in the search repository's `proto/ai/protomolt/search/` tree. Define typed
alternatives for bootstrap/import, register,
renew, drain, report-and-reconcile, reconcile, action preparation/completion,
topology prepare/activate/abort, rollback-as-a-new-generation and policy updates.
The exact package suffix, RPC names and field numbers are reserved only after
review, alongside Fable's merged budget additions. Do not serialize Rust enum
debug strings or use unversioned JSON as the long-term command contract.

Every mutating request needs an explicit client operation identity, collection
scope, canonical payload fingerprint and required revision/generation where
applicable. The same operation and payload replay the stored response;
different content under the same identity refuses. A retried renewal must not
silently extend the lease again. A timeout means outcome unknown: retry the
same operation, not a freshly generated one. Raft mode must refuse legacy
mutation requests that cannot identify retries safely.

Persist deduplication with the effect and response. Start without automatic
expiry. A later bounded-retention mechanism requires acknowledged client
sequence floors and persistent rejection of older requests, so garbage
collection cannot turn an old retry into a second mutation. Capacity exhaustion
must refuse new work rather than evict safety-critical history silently.

Apply all entries in order, including membership and no-op entries. Business
rejections return deterministic recorded results; storage errors are fatal
storage failures, not ordinary rejected requests. Advance the applied log
position even for a committed command that fails its precondition. Control
revision advances for accepted state transitions; topology generation advances
only when publishing a different map. Neither is the Raft term or log index.

## Time, authorization and determinism

The leader supplies a bounded observation time as command input. Apply advances
a stored logical observation time monotonically and evaluates lease deadlines
against that value. Followers do not sample their own wall clocks while
replaying. Policy thresholds and planner version travel in committed state;
machine-local flags cannot make replicas choose different actions.

Clock skew may cause conservative unavailability or suspicion of a node. It
must not authorize two data owners. Registration lease expiry is a planning
input; leader election and control commit use OpenRaft's quorum rules.

Authenticate the control peer/admin before proposal and bind the resolved
workspace, collection, actor and authorization revision into the command.
Never log bearer credentials or private keys. Deterministic authorization
preconditions must use a committed policy revision/binding; an external authority
update needs an explicit ordered integration, not a live network lookup in
apply. Unknown or stale authority bindings refuse. Control membership is not
a substitute for eventual document/field permissions.

Reconciliation computes from one applied state and recorded observations.
Sort every otherwise unordered input, use checked integer arithmetic, and pin
the planner's tie-break and numeric behavior. A dry run establishes a
linearizable read point and returns its control revision and measurement
provenance without allocating actions or changing state.

## Commit, persistence and snapshots

Proposed storage is a dedicated redb database using the already pinned redb
version, separate from every document catalog and index path. A single storage
worker serializes log/vote operations and state-machine transactions without
blocking Tokio workers. Tables store group identity/format, vote, consecutive
log entries, committed/purged positions, applied membership, application state,
deduplication results and snapshot metadata.

Implement OpenRaft's log store contract literally: persist votes before returning;
make appended entries readable in order and signal their flush callback only
after durable storage. Serialize writes and preserve contiguous log history.
Treat truncate/purge boundaries as protocol state, never as file cleanup guessed
from an index number
([log storage contract](https://docs.rs/openraft/latest/openraft/storage/trait.RaftLogStorage.html)).

Use a persistent state machine: one durable transaction writes application
changes, retry response, last-applied log ID and membership before apply returns.
A committed client response follows application; publishing a map follows that
durable transaction. A crash between commit and apply replays; a crash after
apply but before reply returns the saved response on retry. Apply must not
return success while the corresponding state exists only in memory
([state-machine contract](https://docs.rs/openraft/latest/openraft/storage/trait.RaftStateMachine.html)).

Snapshots capture a consistent state-machine view with format version, group
identity, authority incarnation, last applied log ID, membership, all collection
state and retry fences. Build/transfer them in bounded chunks with length and
digest verification. Publish a completed snapshot atomically after syncing it
and its directory. Only then may retained log history be purged under OpenRaft's
rules. Keep the last usable snapshot until replacement is fully accepted.

Receive into an isolated staging file. Reject wrong group, unsupported format,
bad digest, inconsistent membership or incomplete content. Install state and
the applied/membership position atomically before advertising availability.
Restart must see either the old complete state or the new complete state, never
a new applied index paired with old application data. Do not reset a corrupt or
unreadable store to an empty cluster. Fail closed and recover that peer from a
verified retained snapshot/log or a healthy group member.

Control snapshots are not `NodeService` index snapshots. Give their paths,
formats and RPCs separate names so no future worker copies a data shard into
the control replication channel. Control snapshots may contain lease material;
their readers and backups need the same trust as voters.

## Transport and membership

Implement a tonic adapter for OpenRaft vote, append and snapshot operations.
Use a separate internal service with typed, versioned protobuf envelopes and
explicit mappings to the pinned library types. Include group ID, source/target
node IDs and protocol version. Verify the authenticated certificate identity
matches the expected configured peer, not merely that a CA signed it. Apply
size, concurrency, snapshot-byte and deadline limits before allocation.

Do not deserialize arbitrary untrusted Rust objects. Round-trip every library
message variant, including membership changes and errors, through the adapter's
conformance tests. Unsupported versions refuse rather than dropping unknown
semantics. This remains separate from the public search service and phone
query-session transport.

Bootstrap is an explicit one-time action with one recorded group ID and initial
voter set. Never infer a new cluster from an empty/missing file during normal
startup. Add a server as a learner, catch it up, then promote it through the
library's membership procedure. Never edit membership by rewriting local files
or counting currently reachable machines. Remove/replace a voter through the
same procedure and never reuse its persistent identity for unrelated storage.

## Fencing ownership and external work

A replicated decision alone cannot stop a partitioned old shard owner that
continues accepting writes. The existing coordinator-local freeze and WAL
binding are insufficient evidence for that guarantee. Introduce a durable
per-shard write epoch scoped by authority incarnation and index generation;
data mutations and action execution validate it at their actual commit boundary.
An old epoch remains invalid after node restart.

Ownership changes use committed prepare -> fenced-ready facts -> activate (or
abort) transitions. The old owner durably stops new writes, drains admitted
writes and records its final watermark before acknowledging preparation. The
replacement proves the exact required generation and catch-up watermark. Only
then does a committed activation publish the new owner. Stale action IDs,
epochs, source generations and mismatched completion payloads refuse.

If the old owner is unreachable and cannot be externally fenced, the initial
implementation must **not** activate another writable owner merely because its
lease expired. Automatic data-owner failover needs a separately proved storage
fencing or replicated-write protocol. A control leader may fail over while the
existing data ownership remains unchanged. This is a mandatory safety gate,
not an assumption that Raft solves data replication.

Apply writes placement intents to an outbox in state. Workers consume only
committed intents, perform idempotent copy/prepare operations and report fenced
results as commands. Replay never launches a second uncontrolled side effect.
A new leader resumes outstanding intents. Replica copies are invisible until
activation; drop/cleanup follows explicit safe retirement and query-reader
release. Device residency is checked in both intent creation and execution.

## Published map interface for Fable

Use one transport-neutral `PublishedMap` value at the consumer boundary. This
is a proposed envelope around the existing map meaning, not a claim that all
fields already exist on `ClusterPlan`:

- Cluster ID and authority incarnation; explicit workspace/collection identity.
- Control revision and topology generation, with distinct documented meanings.
- Complete ordered routes, stable logical shard identities, hash ranges,
  placement codes and the full predicate-partition tree.
- Canonical map digest and schema/format version. Include activated ownership
  epochs needed by data-plane requests, without inventing segment-subset fields
  before that separate contract is accepted.

The producer exposes a current committed/applied snapshot and a bounded
subscription resuming after a revision. The single-authority adapter can provide
the same interface before Raft exists. A local Raft learner emits values only
after apply; a remote subscriber receives them from that same applied feed.
No consumer observes an uncommitted proposal.

Consumers validate identity, format, digest and complete routing constraints,
then swap the map atomically. Same revision with different content is an error;
older revisions are ignored/refused. Capacity-only changes may advance control
revision without changing topology generation. A topology generation cannot
name two different maps. Keep a query's original map until its readers finish;
never splice two generations into one fan-out or BM25 statistics set.

Reconnect with a retained revision replays available updates. If history was
compacted, send a complete snapshot with an explicit reset marker; never claim
that a missing range of revisions was delivered. A slow consumer may coalesce
full map snapshots; it cannot coalesce or execute placement commands through
this interface. Expose applied/served revisions and lag. Mutation admission
must enforce the activated epoch rather than assuming a connected watch means
the consumer is current.

Authoritative control reads and dry runs use OpenRaft's linearizable read
barrier before reading applied state. A follower/learner's local map view is
explicitly an applied view, not a fresh linearizable read. Quorum loss stops
control mutations; existing queries may use their pinned readable generations
under the product's availability rules
([read barrier API](https://docs.rs/openraft/latest/openraft/raft/struct.Raft.html#method.ensure_linearizable)).

**Learner trust and scale:** a true OpenRaft learner receives the group's
replicated control state, not just a collection's published map. Only explicitly
trusted server relays may run one. Use the map subscription for relays with
narrower access and to avoid making thousands of relays direct replication
targets of one leader. Fable owns the consumer of `PublishedMap` in either
case; this work owns the learner runtime and applied-state producer. Phones
use their permitted query/session metadata interface, never a control learner.

## Migration and recovery rules

1. After the budget merge, audit the final state/config schema and capture the
   legacy file, policy and full live map together. Quiesce the old control
   authority and its placement workers. Record a digest and explicit recovery
   point; reconcile unresolved actions before enabling automatic execution.
2. Validate one import payload containing every counter, lease, action/result,
   collection binding and full map. Preserve allocated IDs and generations.
   Missing placement state is an import error until supplied and verified from
   the quiesced map. Do not silently discard it.
3. Explicitly bootstrap the agreed voter set and commit the import once.
   Compare all replicas' applied state/digests and exercise restart/catch-up.
   Adopt existing data owners only after their epoch enforcement is verified.
4. Switch consumers to the applied map feed. Persist a consumed/import marker
   so the old JSON authority cannot restart and compete with the Raft group.
   Retain the original file and import record as recovery evidence.
5. Before Raft accepts new operations, migration can be rolled back under a
   deliberate shutdown. Afterwards, restarting the old authority from that
   file would lose acknowledged decisions and is forbidden.

Ordinary restart reopens the same group and store, restores applied state and
membership, replays committed unapplied entries, and catches up before serving
authoritative operations. Losing a minority requires replacing/catching up
peers, not reinitialization. Losing quorum stops progress until a quorum is
restored. Disaster recovery from an older backup is an explicit operator
operation with a new authority incarnation, fencing of the old cluster and a
declared recovery point; it cannot promise preservation of later acknowledgments.
Clients must rebind rather than comparing revision numbers across incarnations.

## Implementation sequence and acceptance

1. Fable reviews this note and the map handoff. No dependency/proto allocation
   until that review and the budget merge establish the shared base.
2. Extract deterministic transitions and full-map state. Run the existing
   single-authority control, placement and budget tests through that adapter.
3. Implement storage and its conformance/fault tests; then the tonic adapter
   and single-node replay tests. Keep deployment opt-in.
4. Add three-voter fault scenarios and the applied map feed; Fable integrates
   relay consumers. Add durable ownership fencing before enabling activation
   of replacement writers. The first control HA release may keep that action
   explicitly unavailable where the old owner cannot be fenced.
5. Exercise migration and disaster recovery with disposable state, then perform
   a separately authorized deployment with recorded backup and rollback limits.

Required gates include deterministic replay across processes; duplicate commands
before/after failover and snapshot installation; persistence failures around
vote, append flush, apply, snapshot publication and log purge; corrupted and
wrong-cluster snapshots; dropped/reordered RPCs; minority isolation and stale
leaders; voter removal/rejoin; learner lag/reconnect/reset; repeated equal map
revisions; authorization revision changes; partial topology preparation and
old-owner writes; and proof that device data cannot enter control snapshots or
placement actions. Include cross-architecture replay and Android/iOS dependency
checks. Passing ordinary happy-path cluster tests is insufficient.

## Questions for Fable

1. Does the `PublishedMap` envelope cover the relay's full placed map without
   requiring access to leases, action execution or catalog state?
2. Can the relay consume the same applied snapshot/subscription abstraction
   whether hosted beside a trusted learner or connected as a narrower client?
3. Which budget policy/config and exclusion fields must be carried in the
   deterministic state after that branch merges?
4. Does any current action assume an unreachable primary may be replaced as a
   writer without a durable fence? Those actions need the activation gate above.
5. Are map coalescing, reset-after-compaction and the distinction between
   control revision and topology generation sufficient for the consumer tests?

The operator's decision on splitting server segment ownership remains separate.
The single authority remains in place during the current relay/budget build.
