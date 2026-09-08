# Legacy control checkpoint

The foundations branch provides a typed, bounded input for migrating the
single-file control authority into the transactional control state machine.
`DurableControlPlane::checkpoint_for_import` captures the complete current
state and constructor policy while holding the authority mutex. A closed or
uncertain authority refuses export. This is a privileged local capability;
there is no public, node-facing or diagnostics RPC for it.

The storage contract is `ai.protomolt.search.storage.v1.LegacyControlImport`,
format 1. It contains lease tokens and must receive the same protection as the
original authority file. Do not log its bytes or expose them in the console,
public map subscriptions, search results or error messages. The validated Rust
wrapper deliberately does not implement `Debug`.

## Preserved meaning

The protobuf carries every persisted JSON field: collection, control revision,
token and action allocators, full current topology and historical topologies,
node map keys and lease credentials, all capacity/residency observations,
replica map keys and metadata, ordered pending actions and completed-action IDs.
History, route and pending-action order are preserved. Map keys use unsigned UTF-8 byte order; completed IDs use unsigned numeric
order. Both have a canonical sorted, unique representation. Optional replica strings
preserve absent versus present-empty; a plain protobuf string is insufficient.

The legacy completion set retains at most 4,096 IDs and has no original
request fingerprints or responses. Capturing it cannot recover evicted retries
or prove that a repeated ID has the same payload. The transactional importer
must retain this provenance and refuse unsupported legacy retry claims; it
must not synthesize a complete idempotency journal from these IDs.

The seven constructor policy values are captured separately. They describe
runtime configuration at export, not the policy that produced old decisions.
The original JSON contains no workspace binding, authority incarnation,
placement tree, per-route placement codes, derived declaration, provider
geometry, source-owner readiness or policy history. The checkpoint does not
invent those fields or turn an expired registration lease into replacement
writer authority. A future transactional importer requires explicit trusted
inputs and proof for the missing information.

The legacy reader now refuses unknown fields at every persisted object level,
duplicate node/replica map keys and duplicate completed-action IDs. Duplicate
keys cannot silently replace an earlier decision while parsing.
Existing defaults for collection, completed actions, peer generation and added
capacity observations remain accepted. Missing originally required fields and
unknown enum names refuse. This prevents a newer JSON authority from being
opened and overwritten with fields silently removed by an older reader.

## Bounds and refusals

`LegacyControlCheckpoint::decode` checks the 16 MiB wire limit and walks known
message boundaries before allocating decoded records. It permits at most
262,144 wire fields, counting nested fields and each packed completed-action ID.
Malformed lengths/varints, groups, unknown/noncanonical fields and unsupported
formats refuse. Error messages identify the violated rule without echoing lease
credentials, map keys or arbitrary stored strings.

Export first counts the JSON representation against a 16 MiB limit without
allocating another state-sized JSON buffer, then checks the protobuf length
and field budget. Exceeding either representation limit refuses; the writer
does not truncate history or retry records to fit. These are explicit migration
limits, not an assertion that the legacy authority itself has bounded growth.

Validation also requires nested state/topology/policy/capacity records, known
enums, consistent node/replica keys, valid inclusive ranges, nonzero control
revision and allocators, distinct lease tokens below the token allocator, and
unique pending/completed action IDs below the action allocator. Completed and
pending IDs cannot overlap. Validated checkpoints expose immutable references
and deterministic re-encoding; callers cannot mutate their validated contents.

## Convergence boundary

Export is a snapshot, not a transfer of authority. It does not stop the old
writer after returning bytes, install a new store, prove a copy is ready or
activate managed writes. Keep the old writer quiesced throughout the eventual
import and revalidate the snapshot against that authority before commit. The
exclusive sidecar lock protects one cooperating local writer; copying its JSON
or protobuf does not transfer that lock or establish distributed fencing.

The next integration consumes this typed input in the transactional authority,
with explicit resource/policy/topology inputs and an actor-scoped import retry
record committed atomically with the imported state. Runtime admission and
published-map consumers must then read that same applied state. See the
[single-authority convergence boundary](raft-control-design.md#single-authority-convergence-boundary-2026-09-08).

## Validation (2026-09-08)

The focused gate passed 37 control-plane tests, including a literal minimal
wire fixture, complete nondefault state round trips with `u64::MAX` counters,
optional replica presence, legacy defaults, duplicate/unknown JSON refusals,
wire budgets and frozen export after later state mutations. Uncertain storage
refuses export. The first compile exposed missing Rust package re-exports;
a later malformed fixture accidentally tested ordering before identity. Both
were corrected, and their failed logs retained before the successful run.

The combined gate above `6439a91` passed 739 library tests and 150 integration
targets (903 top-level passes plus one nested worker; one existing external
OpenNLP test ignored), 13 embedded tests, two IVF tests and nine Python reference
tests. All five mobile compilation targets, tests/examples compilation,
formatting, vendored-proto and diff checks passed. Baseline/current protoc
outputs prove exactly the new checkpoint descriptor was added: all 22 existing
proto files, their descriptors and the original reference fixtures are unchanged.
No service or existing RPC field was added or changed.

All 669 tracked and untracked inputs, HEAD and dirty status matched before and
after the full gate. Only documentation is updated afterward to record these
results, clarify ordering and incorporate the independently amended capacity-tier
contract and review. The run used an 8 GiB, swap-disabled scope, reached that
cap, and recorded no OOM events. These are correctness checks, not performance
measurements or a fleet rollout.
