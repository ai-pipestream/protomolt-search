# Source-index maintenance

Status: preservation comparison, durable maintenance journal, and the trusted
local node's journaled cutover/recovery are implemented on the foundation branch.
Bounded stored-row rebuilding and the embedded owner operation are implemented
on this branch. The WAL compactor and index-only snapshots still refuse
source-owned catalogs; those legacy routes are not maintenance entry points.

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
- the vector provider's own stored encoding of each live row: the bytes its
  scorer reads for that row and the per-row scoring metadata beside them
  (`VectorProvider::row_transcript`; for the embedded TurboVec adapter the bit
  width, dimension, the row's bit-plane packed codes and its correction scale);
- per-field lengths, terms, term frequencies, spans, positions and sentence spans.

Physical row numbers, segment ids, dictionary ordinals and tombstoned rows are
excluded from the logical comparison. Each identity must occur exactly once in
each live view; equal row totals do not excuse a duplicate or missing identity.

### What a format-2 certificate guarantees

A `SourceRewriteCertificate` with `format_version = 2` states that, for the
two catalogs the publisher held while computing it, the multiset of
(document identity, row content) over live rows is equal, where row content
is everything listed above including the dense encoding the provider actually
serves from. Equal FP32 source rows alone do not satisfy it: an image whose
stored codes were built from other vectors, or under other calibration, has
different row transcripts and refuses, even when every exact row and the
backend configuration agree. Valid row reordering and different segment cuts
still certify, because the transcript follows the identity, not the slot.

A provider that cannot expose its stored representation refuses
`row_transcript` by name, and the proof refuses with it; no certificate is
issued for a dense image the proof could not read. Format 1 certificates,
which older journals hold, remain valid records of stored content and exact
rows only; they say nothing about the dense image, and nothing rewrites them.
The journal accepts both formats on recovery (`validate_maintenance`); new
publications always write format 2.

The certificate is still an artifact-content comparison, not proof of source
acceptance, authorization, durability, or runtime activation. The publisher
must compute it from its own held views, never trust a caller-provided
certificate. The builder must still derive and validate segment summaries
(the publisher recomputes pruning summaries against stored rows), and a
certificate does not certify the soundness of caller-supplied pruning metadata.

### Cost

Memory: [`BATCH_ROW_BYTES`] (32) per batch row, at most 1,048,576 rows
(32 MiB), plus one reconstructed source row or posting at a time and an 8 MiB
redb cache for the on-disk identity/digest tables. Provider row transcripts
are read in pieces of `TRANSCRIPT_BLOCK_ROWS` (1,024) rows —
`rows × (16 + bits × dim / 8)` bytes, 256 KiB at 384 dimensions and 4 bits —
and the embedded engine (`TurboQuantIndex::stored_rows`, chain s21) converts
one 32-row block at a time from the layout it already serves, assembling a
mapped block straight from its pages outside the search's chunk cache. No
packed image is materialized or retained by the proof: `packed_ready()`
stays false on every loaded and mapped segment afterwards
(`mapped_proof_leaves_packed_codes_unmaterialized`), and at a fixed batch the
proof's own allocation does not grow with the image
(`proof_allocation_is_independent_of_vector_image_size`: 16,384 rows at 8
versus 512 dimensions, a 64× larger image, measured 3,913,010 versus 3,929,138 bytes of peak allocation on the proof thread, 16 KiB apart). Two
proofs running at once cost two of everything above; the batch limit is not
a total-process memory quota.

Time: every live row is reconstructed once; every field's vocabulary is
walked once per batch of each segment, with posting cursors skipped to the
batch's row interval. The number of vocabulary passes is therefore
`fields × Σ_segments ceil(rows_segment / batch)`, and the batch bounds digest
memory and nothing else. Measured on the 280-row test fixture (two fields, a
one-segment before and a three-segment after), passes by batch size:
1 → 1,120; 7 → 164; 64 → 22; 280 → 8; 1,024 and above → 8. The certificate
is byte-identical at every size. `proof_batch_rows = 0` selects the default,
the largest batch; `proof_batch_rows_for_budget(bytes)` derives a batch from
a digest budget for callers that need a smaller one. It returns an error below
32 bytes; a hard zero-byte budget never becomes a one-row allocation. Choose a smaller batch
only when 32 MiB of digests is unaffordable.

Scratch: the directory must not exist. It is created with mode 0700 and its
database with mode 0600 at creation, independent of the process umask, and
removed after all database handles close. Existing paths are never reused or
cleaned up.

### Transcript

The product wrapper requires every requested row exactly once, in increasing
order, before accepting a successful provider transcript call. Missing,
duplicate, reordered and out-of-range callbacks refuse; invalid indices never
reach the proof's bitmap or digest array. Single-row requests use checked range
arithmetic. Provider failure remains a proof failure even after partial output.

The format-2 transcript uses SHA-256 with length-prefixed byte strings and
fixed-width little-endian counts, under the domain strings
`protomolt.source-rewrite.row.v2` and `protomolt.source-rewrite.rows.v2`, so
a format-1 digest never equals a format-2 digest. Row content starts with the
reconstructed `AddDocumentsRequest` protobuf. Stored f64 bits are included
separately because protobuf default elision normalizes negative zero. FP32
vector bits are included explicitly, then the provider row transcript.
Ordered field metadata and postings extend each row digest. The identity
table is sorted by encoded `DocumentIdentity`; its complete key/digest
sequence determines the content digest. Schema and backend construction state
have a separate digest. Batch size and physical row order do not enter either.

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

## Bounded stored-row rebuilding

`NodeServiceImpl::compact_document_index_blocking` captures an owned immutable
view and read claim, builds private outputs, and invokes the journaled cutover.
`CompactDocumentIndexRequest` carries the logical index key and required row,
byte, staging-file and preservation-proof budgets. It contains no filesystem
paths and introduces no network RPC.

The builder walks posting skip runs over a bounded input row interval, retaining
only live rows. It copies original protobuf sources, identities, lineage, all
nine stored column families and presence, field lengths, terms, frequencies,
spans, positions and sentence tables. It does not invoke an analyzer, CEL or an
embedding service. Complete generation declarations restore empty columns and
analysis metadata. Vector-bearing cuts rebuild provider images from the exact
FP32 rows and the persisted backend configuration; a change in vector presence
starts a separate cut so vector-less documents remain vector-less.

Compatible rows combine across source segments. Row and byte limits cut outputs;
a read interval that exceeds the byte budget is halved and retried. One row that
cannot fit refuses the entire operation. Accounting includes row protobufs,
analysis structures, terms, occurrences and vectors. An input batch and an
output batch can coexist. These are content budgets, not a total-process heap
quota: immutable readers, one decoded row/posting, allocation overhead and
provider build buffers also consume memory. Output metadata grows with the
number of cuts, as does the eventual catalog. Mapped serving remains the default.
The staging-file budget is checked after each completed cut; publication also
copies those files into the active catalog's fresh output directories.

Private files live in one exclusively created directory owned by the blocking
operation. Budget failures discard private work without changing source or
serving state. A concurrent source publication makes the captured read version
stale; cutover refuses it rather than losing that write. This operation does not
tail concurrent writes or promise convergence under continuous ingestion.

`EmbeddedSearch::compact_document_index` uses this same operation with owned
node/catalog references on a blocking worker. Cancelling its awaiter does not
cancel an operation already running or remove files from under it. The owner
can use `recover_document_maintenance` to observe its durable result. The Rust
embedded API is available; adding this operation to the mobile C/Kotlin/Swift
bridge remains separate packaging work.

## Remaining integration

Public `Query` and `QueryStream` already resolve stable document/chunk identity
across lexical, dense, hybrid, Boolean and browse results, including collapse
inner hits and provisional revisions. Resolution uses the admitted read version
and enforces document visibility and identity disclosure; physical row IDs remain
generation-local. See [query result identity](query-result-identity.md) for the
public contract and its maintenance coverage.

Coherent backup/restore must join source history, journal and
the referenced manifest. The [pinned source checkpoint](source-index-backup.md)
now preserves the complete source transaction and index anchors; coherent
artifact capture, bundle completion and restore activation remain outstanding.
Recovery-aware orphan reclamation and corpus-scale
rewrite measurements remain outstanding. Index-only snapshots and WAL compaction
must continue to refuse owned catalogs until their contracts are joined.

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
file retirement. The row-builder tests also merge source segments, replace a
source inside the merged segment, reclaim its partial tombstones, retain a zero-row source,
reclaim all deleted rows and accept subsequent versions. Independent fixtures
compare mixed vector/no-vector cuts, all typed column families, negative zero,
missing versus zero values and analyzed postings through the preservation proof.
Budget failures preserve the old view; smaller input intervals complete under
a tighter byte budget. Embedded tests cover query equality, reopen/recovery and
an awaiter cancelled while the private build is held before publication.

Validation is local under an enforced 8 GiB scope with swap disabled. No fleet
operations or production compaction runs are part of this checkpoint.
