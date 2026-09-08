# Closed managed-source binding

The foundations branch can bind an existing access-controlled source catalog
to an exact committed owner preparation. This implements the source-side durable
boundary between preparation and readiness in the
[activation design](source-authority-activation.md). It does not activate a
writer, publish a map, verify a host's live process or replace another source.

`AccessControlledCatalog::bind_prepared_owner` consumes the local catalog handle.
It pins its current Admin permit, requires the same authenticated actor to have
current Admin permission in the control store, and compares the complete pending
owner record with the supplied preparation. The resource and history must match
the existing source. Actor attribution must be complete, retirement/sealing must
not have begun, and pending source/index maintenance decisions must already be
resolved. The explicit metadata byte budget covers both the existing checkpoint
and the resulting checkpoint with its larger managed header; an over-budget
result refuses before the source commit.

One immediate source transaction changes the header to format 9 and records
`SourceManagedBinding`: authority identity, the exact owner preparation and the
accepted sequence at binding. It retains the source history, every accepted
version and retry decision, the actor namespace and the index journals. No
source history is created, reconstructed or renamed by this operation. A new
header field is not a document wire change.

The lock order is existing source policy pin, control authority, source database
writer. The control preparation already exists durably before source binding;
the pure control transition never performs source IO. Failure before source
commit leaves the prior catalog format. Failure after commit leaves a closed
managed catalog. Either outcome is recovered by inspecting the exact existing
file, never by creating a replacement. A source-side failure does not close a
healthy control-store instance. The consumed source handle is dropped on error.

`PreparedManagedCatalog` exposes current-authorized binding and checkpoint
metadata inspection only. It exposes no catalog handle, acceptance, index
mutation, raw source retrieval or backup/export operation. Exact managed reopen
requires current committed Admin permission and the expected authority and local
binding. After a lost response, `PreparedManagedCatalog::recover` requires only
the expected authority and complete preparation. It discovers the accepted
sequence from the validated existing header while retaining the same exclusive
file lock through opening and inspection; it never guesses a sequence or
releases the lock between discovery and validation. Malformed caller
expectations return `InvalidArgument`; malformed persisted metadata returns
`DataLoss`. Missing state refuses. Cancellation can leave this local completion
available for inspection, but cannot reopen its admission.

All existing catalog writer paths refuse a managed binding at the database
writer boundary, including retirement, sealing, legacy actor assignment and
prepared-decision recovery. Existing receipt lookup remains a read. Public
standalone and access-controlled open paths refuse a managed source, even when
the caller has local Admin permission. Header format, owner, history, resource
and actor metadata must agree. Source headers have a 256 KiB decode bound, and
format-9 headers require canonical bytes so unknown managed semantics cannot
silently disappear. Checkpoint validation retains the full binding and includes
the actor retry table; a copied binding still grants no admission.

The target in a preparation is intended storage, not proof that this process
is running on that host or owns a unique physical copy. The next readiness step
must verify authenticated host and storage/process identity, retain exclusive
local ownership, and reconcile the source completion with current committed
control state. A binding does not fence an earlier independent catalog copy.
Managed admission and topology must share the transactional authority before a
serving adapter exposes writable activation. Remote revocation enforcement and
the complete retirement/replacement sequence remain required.

Device-local residency stays in the durable binding. This adapter carries only
ownership metadata; it neither exports source/index/WAL/snapshot bytes nor
authorizes a server to take over a phone's shard. Phone hosting and its local
authority adapter remain separate from control consensus membership.

## Validation (2026-09-08)

The full gate on the working tree based on `a9ec459` passed: 720 library tests,
150 integration targets (903 top-level passes plus one nested worker),
13 embedded tests, two IVF tests and nine Python protobuf-reference tests.
All five Android/iOS target checks, tests/examples compilation, formatting and
vendored protobuf checks passed. The existing external
`native_matches_opennlp_contract` test remained ignored because it requires an
OpenNLP endpoint and scans every Unicode scalar. The three mobile-only
unused-item warnings were unchanged from the baseline.

The 11 managed-binding tests cover source history and actor retry preservation,
current administration rights, mismatched owner/resource/history/authority,
legacy-writer refusal, missing/corrupt state, exclusive recovery locking,
cancellation, metadata-budget rollback and process exit before/after the source
commit. After-commit recovery uses only the expected authority and preparation,
without supplying the source-selected accepted sequence. The initial negative
run reproduced malformed caller metadata returning `DataLoss` and an oversized
result committing despite the input metadata budget. Both now refuse with the
intended error and preserve the prior source state.

The protobuf gate proves the exact additive `SourceManagedBinding`, header
field 10 and import delta. All other proto bytes, prior normalized descriptors
and original wire fixtures remain identical to `a9ec459`.

All 662 tracked and untracked inputs remained identical throughout validation.
Only this validation record and the README were amended afterward. The suite
ran with `MemoryMax=8G`, `MemorySwapMax=0` and two Cargo build jobs. Its cgroup
peak reached 8,589,934,592 bytes with memory-limit reclaim events, zero swap and
zero OOM events. This is test-scope accounting, not a production memory claim.
Raw local evidence is retained under `/tmp/psearch-managed-binding-red-*`,
`/tmp/psearch-managed-binding-focused-green-r2-*` and
`/tmp/psearch-managed-binding-full-*`. No main merge or fleet action was performed.
