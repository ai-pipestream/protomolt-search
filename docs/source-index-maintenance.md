# Source-index maintenance

Status: preservation comparison, durable maintenance journal, and the trusted
local node's journaled cutover/recovery are implemented on the foundation branch.
The bounded source-owned rewrite builder and its product integration remain
unfinished. The WAL compactor and index-only snapshots still refuse source-owned
catalogs; this does not enable those legacy routes.

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
The standalone storage method records artifact intent only. The node path
separates verification from preparation using a private, non-serializable
verified object bound to the held before/after views. It copies and verifies
outputs before acquiring mutation fences; source writes and old queries can
continue, and a changed read version rejects the stale output before intent.
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
compaction. The production rewrite builder still has to use the activation
sequence below; writing generation metadata alone cannot publish a rewrite.

## Implemented node cutover

`NodeServiceImpl::publish_document_maintenance_blocking` accepts a trusted
owner's rebuilt segment files and captured node read claim. It copies them into
fresh immutable directories and verifies row preservation and pruning metadata
without holding mutation fences. A private staged object owns the copied files,
and a private verified object binds their complete manifests and preservation
result. A caller cannot manufacture either object from a protobuf certificate.
Staging exclusively creates its temporary directory. A competing build or a
leftover directory is refused without removing its files; cleanup only owns a
directory this attempt successfully created.

At cutover the node takes its ingest/mutation/seal fences, checks the captured
read claim and catalog again, and explicitly enables the maintenance journal.
It prepares the complete vector, exact-vector, lexical, binding and live-row
view before acquiring the final shard write guard. The verified intent precedes
the manifest swap. The node then installs all serving components together,
invalidates its parent cache, advances the read epoch, and acknowledges the
maintenance decision while the serving guard is still held. Physical row
renumbering starts with the new layout's tombstones, not the old row bitmap.

`DocumentMaintenanceActivation` identifies the maintained source sequence,
source and maintenance intents, current catalog/read versions and live row
count. It is an observation of this node, not a source acceptance or a
collection-wide write receipt. A later source publication supersedes it.
`recover_document_maintenance_blocking` joins a reopened exact serving view to
journal recovery and returns the observation only if maintenance is still the
current physical tip. Source recovery continues to return the immutable source
decision paired with the current catalog epoch.

An unresolved durable intent fences node ingestion. A manifest-sync failure
retains staged files even if the rename succeeded. Reopen distinguishes before,
after and third manifests through the journal; any third manifest remains
unresolved. Normal successful cutover retires the known old segment directories
only after the journal decision. Existing query snapshots retain their opened
images. Ambiguous outcomes retain files for recovery; automatic orphan cleanup
after recovery is not implemented and must not guess which private files to
remove.

## Remaining integration

Build source-owned rewrites in bounded batches from the retained segment rows,
including mixed vector/no-vector segments, every typed value family and analyzed
postings. Feed those outputs through the node cutover above. The WAL compactor
cannot supply source-owned rows: these indexes deliberately have no WAL. Expose
an owned candidate/operation through the embedded runtime without borrowing
caller paths across cancellation, and cover the full query/permission matrix.

Required additional evidence includes repeated real row rebuilding across
source versions, all-deleted and zero-row sources, stable document/chunk identity
across lexical, dense, hybrid, Boolean, browse and streaming surfaces, and
coherent backup/restore of source history, journal and the referenced manifest.
Complex public query shapes that do not yet expose source identity still need
that separate identity plumbing; stored-identity preservation alone is not
complete coverage of the public API.

## Validation scope

Storage tests cover consecutive maintenance, source writes after maintenance,
final-segment reclamation, exact retry-receipt preservation, migration refusal,
exclusive pending transactions, unexpected manifests and corrupted history
links. Actual node tests interrupt after intent, after activation and after an
uncertain manifest rename, reopen source and node, and reconcile the serving
view. A deterministic concurrent source write proves verification holds no
mutation fence and stale verified output cannot commit.

Integration tests physically renumber surviving rows, retain lexical scores and
source identity, enforce document visibility and identity-disclosure grants,
reject stale read claims/public cursors, preserve empty vector configuration,
and accept later source writes. Held old readers remain usable after normal
file retirement. These tests use rebuilt layouts assembled from immutable live
segments; the bounded row-rebuilding component remains on the list above.

Validation is local under an enforced 8 GiB scope with swap disabled. No fleet
operations or production compaction runs are part of this checkpoint.
