# Source-index maintenance

Status: preservation comparison implemented on the foundation branch; journal
and runtime cutover integration remain proposed. Legacy compaction and index-only
snapshots still refuse source-owned catalogs. This note does not enable them.

## Two sequences with different meanings

Accepted sequence orders source versions. A compaction consumes no new accepted
version and must not manufacture an acceptance receipt or change an idempotency
result. Catalog epoch orders physical manifest publications. It advances for
source replacement and for maintenance. Source history, logical index key,
collection, source version and optional chunk identity survive physical rewrites.

The current projection journal checks that its committed manifest equals the
latest `ProjectionIntent.after_manifest_sha256`. That is correct for source-only
publication, but a compaction would break it. Do not weaken that comparison or
rewrite the historical source decision to make compaction appear to succeed.

The maintenance extension should retain a separately keyed immutable maintenance
decision, using its after-catalog epoch as the index-local revision. Its intent
binds the owner, accepted sequence and last source intent, before/after manifest
hashes and epochs, previous maintenance decision (if any), and the preservation
comparison below. At most one source or maintenance intent may be pending for an
index. A later source publication starts from the maintenance manifest and then
becomes the new source tip. Acceptance and retry records remain unchanged.

Adding those records requires a new journal-header version. Older readers must
refuse it before writing; otherwise a reader unaware of the maintenance pointer
could erase it when serializing the existing state. Migration must preserve old
source decisions and resolve pending source publication before changing the
extension. New readers must validate both the immutable maintenance record and
its connection to the last source decision, not trust an epoch supplied by a
caller.

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

Current segment catalogs keep mapped binding and owner metadata even with no
rows, but the complete column/derived declaration and surviving empty vector
backend state can still live only in segment files. Dropping the last segment
must not erase those declarations. The comparison currently refuses that loss.

Before enabling full compaction, persist a protobuf generation declaration in the
catalog, including empty columns, field capabilities, derived declaration and
vector construction state. Validate each segment against that declaration and
retain it when every physical row is reclaimed. Treat the new manifest format as
an explicit reader compatibility boundary. Reindexing or changing a derived
expression is a different operation from compaction, never an exception to this
preservation rule.

## Publication and recovery integration still required

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

## Checkpoint validation

The combined local run passed 592 library tests, 831 integration tests across
142 targets, 13 embedded tests and two IVF adapter tests. One existing live
OpenNLP comparison remains ignored. All five Android/iOS compile checks,
examples, formatting, vendored contracts and the full search-protobuf descriptor
comparison against `3a32b7f` passed. Validation used an enforced 8 GiB scope with
swap disabled. No fleet operations or production compaction runs were performed.
