# Source-index maintenance

Status: preservation comparison and the durable maintenance journal are
implemented on the foundation branch; runtime cutover integration remains
proposed. Legacy compaction and index-only
snapshots still refuse source-owned catalogs. This note does not enable them.

## Two sequences with different meanings

Accepted sequence orders source versions. A compaction consumes no new accepted
version and must not manufacture an acceptance receipt or change an idempotency
result. Catalog epoch orders physical manifest publications. It advances for
source replacement and for maintenance. Source history, logical index key,
collection, source version and optional chunk identity survive physical rewrites.

The projection journal retains immutable source decisions. Its active manifest
is checked against the latest source decision or the maintenance decision that
follows it; compaction never rewrites the source decision to appear successful.

The format-2 extension stores immutable `MaintenanceIntent` records keyed by
logical index and after-catalog epoch. Each binds its source owner, accepted
sequence, last source intent, before/after manifest hashes and epochs, previous
maintenance cursor, and locally computed preservation certificate. The state
retains the latest maintenance cursor even after another source write. A later
source publication starts from the maintenance manifest and becomes the new
physical tip while retaining that history link.

At most one source or maintenance intent may be pending for an index. Preparation
retries with the same before/after views return the existing pending intent;
a different transition refuses. Recovery records a decision only after checking
the synced actual manifest under its publication lock. The before manifest
aborts maintenance, the after manifest commits it, and any third manifest keeps
the intent unresolved. None of these operations changes source acceptance or
retry receipts.

`enable_index_maintenance` atomically creates the decision table and upgrades
the journal header from format 1 to 2. Every pending source publication must be
resolved before migration. Source decisions are preserved. Older readers refuse
the new header; downgrading the header while retaining its table also refuses.
Existing format-1 journals remain readable and do not migrate implicitly.

`prepare_index_maintenance` takes held before/after catalogs and computes the
preservation proof itself. It also recomputes supplied pruning summaries from
the stored columns, so an incorrect range cannot hide rows after publication.
The owner must fence publication across preparation and commit; it should run
the expensive comparison while old queries remain readable, before acquiring
the shard's final write guard. This method records artifact intent only.
`recover_index_maintenance` reconciles the durable decision, and
`index_maintenance_decision` reads immutable history without claiming visibility.
The node's source activation observation now reports the current catalog epoch
while preserving the original source intent and accepted sequence.

## Implemented preservation comparison

`OpenedSegmentSet::verify_source_rewrite` compares two held immutable catalogs
and returns a protobuf `SourceRewriteCertificate`. It checks:

- the same source owner and complete mapped binding;
- the same declared field/column tables, analyzer fingerprints, position and
  sentence capabilities, derived declaration, vector dimension and backend state;
- each live document's exact key, version and optional chunk ordinal;
- stored text, lineage, original descriptor/message/payload bytes, source ordinal,
  every stored scalar and map column including presence, and exact FP32 rows;
- per-field lengths, terms, term frequencies, spans, positions and sentence spans.

Physical row numbers, segment ids, dictionary ordinals and tombstoned rows are
excluded from the logical comparison. Each identity must occur exactly once in
each live view; equal row totals do not excuse a duplicate or missing identity.
No whole-field transpose is needed. A batch holds 32 bytes per row, capped at
1,048,576 rows. An 8 MiB redb cache backs temporary identity/digest tables on disk.
The reader visits posting skip runs only over the batch's row interval. It also
holds one reconstructed source row or posting at a time; the batch limit is not
a total-process memory quota. Scratch directories must be new, are private on
Unix, and are removed after all database handles close. Existing paths are never
reused or cleaned up.

The format-1 transcript uses SHA-256 with length-prefixed byte strings and
fixed-width little-endian counts. Row content starts with the reconstructed
`AddDocumentsRequest` protobuf. Stored f64 bits are included separately because
protobuf default elision normalizes negative zero. FP32 vector bits are included
explicitly. Ordered field metadata and postings extend each row digest. The
identity table is sorted by encoded `DocumentIdentity`; its complete key/digest
sequence determines the content digest. Schema and backend construction state
have a separate digest. Batch size and physical row order do not enter either.

This is an artifact-content comparison, not proof of source acceptance,
authorization, durability, or runtime activation. The publisher must compute it
from its own held views, never trust a caller-provided certificate. It compares
exact vectors and backend construction state; the maintenance builder must still
rebuild provider images through the same backend and derive/validate segment
summaries. A certificate alone does not certify opaque provider code bytes or
soundness of caller-supplied pruning metadata.

## Empty generations must keep declarations

Source publication now writes a format-4 segment catalog with a checksummed,
canonical protobuf `IndexGenerationDeclaration`. It retains the complete ordered
column tables (including empty columns), field names, analyzer fingerprints,
position and sentence capabilities, derived declaration, and vector dimension
and backend construction state. Each segment must agree with the declaration.

The first source write may establish an analyzer fingerprint or vector backend
that was previously unset. Once established, later source writes cannot change
or remove it. Rewrites use strict equality, including when every physical row is
reclaimed. Reopen restores analyzer metadata into the empty configured tail,
validates the column and derived declarations, and constructs the empty vector
provider with the persisted scoring configuration. A stale legacy vector file
cannot override a declared generation.

Formats 1, 2 and 3 remain readable. Format 4 requires the declaration; older
readers refuse it. The optional JSON field is omitted from older manifests so
the source journal's existing typed-manifest hashes remain unchanged. The
format-1 rewrite transcript represents declared metadata identically to matching
segment metadata; reclaiming the final tombstoned segment preserves its proof.

Reindexing or changing a derived expression remains a separate operation from
compaction. This metadata work does not enable source-owned maintenance cutover;
the runtime activation sequence below is still required.

## Runtime publication and recovery integration still required

Build outputs under private staging while the old sealed view remains readable.
At publication, hold the owning node's ingest/mutation/seal fences, verify the
captured read version and owner, and prepare the complete new provider, exact,
lexical and live-document state. Record the maintenance intent before the
manifest, publish the manifest, activate the complete state and advance the read
version, then acknowledge the durable maintenance decision. Keep source sequence
and acceptance receipts unchanged.

A reopened before-manifest aborts only maintenance; an after-manifest commits its
decision; any third manifest retains the intent and refuses reconciliation.
Uncertain publication retains artifacts. Runtime activation and journal recovery
must have fault-injection coverage on both sides of the manifest commit. Held
old views remain usable, while old public read claims are rejected after cutover.
The activation observation must report the current catalog epoch, not the older
source intent's epoch.

Required integration evidence includes repeated compactions between source
versions, updates and deletes after compaction, an all-deleted generation,
zero-row sources, per-field permission filtering before and after, both crash
windows, source-catalog and index reopen, and stable document/chunk identities
through lexical, dense, hybrid, Boolean, browse and streaming results. Coherent
backup/restore must capture source history, journal and the referenced manifest
as one recoverable authority; copying only index artifacts is insufficient.

## Validation scope

Storage-level tests cover consecutive maintenance between source writes,
source updates/deletes after maintenance, final-segment reclamation, exact retry
receipt preservation, pending-source migration refusal, exclusive pending
transactions, both durable manifest outcomes after source-catalog reopen,
unexpected-manifest retention, and corrupted history links (including recomputed
checksums). Node reopen reports the current catalog epoch and retains document
identity. The fixture models manifest cutover explicitly; it does not enable the
legacy compactor or prove the complete live query/permission matrix above.

Validation remains local under an enforced 8 GiB scope with swap disabled.
No fleet operations or production compaction runs are part of this checkpoint.
