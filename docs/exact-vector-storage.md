# Exact-vector storage

The product-owned FP32 store supplies reranking and the vector rows copied into
sealed segments. It is separate from the quantized provider image. These changes
are on the foundation branch; logical document publication remains unfinished.

## Appending to a persisted view

`ExactVectorStore::append` retains a mapped base and puts only the new rows in a
private disk builder. It neither reads nor copies the base payload. Base and
delta form one positional row space for serial/parallel scoring and range reads.
The delta view reports mapped pages touched in the base, preserves caller result
order, and uses the same scalar FP32 accumulation as the heap and mapped paths.

`write` is still an explicit full snapshot: it streams base and delta into the
existing `PMEXACT1` file format, syncs the file, renames it and syncs its parent.
It returns a mapped instance. Existing mapped readers retain their original
payloads. Continuing to append through the delta view after a snapshot does not
modify that snapshot. A finalized plain builder refuses further appends; callers
continue through the mapped instance it returned. The builder is fenced as soon
as rename succeeds, even if the parent-directory sync then fails. The mapped
same-path shortcut checks dimension, row count and payload identity rather than
acknowledging a different snapshot now occupying that path. These checks do not
replace the owner's fencing of independent writers. No storage or protobuf field
numbers change.

The private delta is scratch state, not a durable publication record or receipt.
Normal drop removes its scratch file. A process crash can leave an unreferenced
scratch file; recovery still depends on the source/WAL and committed images.
Single-image snapshots still copy all FP32 rows. Segmented serving now shares
its sealed files directly, as described below. The document publisher must
still join source versions, manifests, row masks and read revisions coherently.

## Immutable segment views

A segment snapshot holds its exact-vector mapping alongside its provider image,
BM25 image and tombstone overlay. Bitmap-only publications share those mappings;
older readers retain their original images and masks. Opening and staging check
exact dimensions against the provider, as well as row alignment.

`ExactVectorStore::from_segments` shares these validated mappings without copying
or hashing the corpus again. Its row space is the catalog's contiguous physical
range. Document-only segments retain their positions: scoring skips missing
vectors, while range reads across a gap refuse rather than invent coordinates.
`contains_row` reports vector presence, not visibility. Parallel results preserve
request order and repeated slots, and page accounting distinguishes mappings.
An append spills only new rows after the physical end, including any gaps.

Node startup, segmented compaction and snapshot installation use that view.
A sidecar with the right length cannot override different committed segment
values. An obsolete sidecar is ignored when sealed vector images exist. A flush
persists FP32 rows with the sealed segments and can release the private builder
when no newer tail remains; it does not rewrite a redundant whole-shard file.
Concurrent tail writes still prevent exporting a supposedly complete snapshot.
Single-image and product-owned sidecars for remote providers keep their existing
persistence path.

Segmented snapshot exports carry the original segment files and omit the
redundant root FP32 sidecar. Existing file formats and protobuf definitions stay
the same. Optional legacy sidecars on incoming snapshots are still validated.
Older receivers may need to reopen a dense catalog to rebuild their sidecar;
they cannot serve catalogs with vector gaps using their concatenation path.
Use a build supporting segment views on every receiver of those catalogs.
A sparse view refuses a dense single-file export; persisting it through its
catalog preserves both the values and the missing rows.

When a node attaches, reopens or installs a catalog, committed segment tombstones
are merged into its serving bitmap. This preserves newer generation-wide deletes
and held readers, and prevents a missing or older root bitmap from resurrecting
retired rows. The merge visits bitmap words and deleted bits rather than scanning
every document. Live activation of accepted document replacements still requires
one coherent publication decision and remains unfinished.

## Read failures

Scoring now returns an I/O error if any requested spill read fails, in both
serial and parallel paths. It does not return partial scores or fill unread
coordinates with zeros. `row_values` returns `io::Result<Vec<f32>>`; invalid
ranges and short reads refuse the whole range. Sealing, transplant and child
reconciliation propagate those errors. No source can be declared successfully
reconstructed from an invented vector.

## Evidence

Three regressions failed on the prior implementation: one appended 4-dimensional
row wrote 32,864 scratch bytes instead of 96 (an 80-byte header plus 16 bytes), a
short spill read returned a score, and a finalized builder allowed a mutation.
The corrected tests pass. Additional tests cross the base/delta boundary, compare
parallel scores bit for bit, retain old snapshots during continued appends,
compare exported files to canonical heap output, and verify that a failed export
from a truncated delta leaves the previously published base unchanged. These are
local storage tests, not fleet throughput or end-to-end publication evidence.

Further regressions reproduce an uncertain finalization that left its published
file mutable, and a stale mapped handle acknowledging a different snapshot at
the same path. The tests inject a failure after rename and verify fencing and
reopen, and replace a path with a same-shaped, different-valued image to verify
that the shortcut refuses it. This is process-level failure evidence, not a
hardware power-loss test.

The preceding append-view checkpoint `49709e8` includes main through `8d32a72`.
Its local validation passed
1,390 tests: 566 library tests, 809 integration tests across 140 targets,
13 embedded tests and two IVF benchmark tests. One external OpenNLP contract
test remains ignored because it requires a running analysis service and scans
every Unicode scalar. The five Android/iOS target checks, protobuf descriptor
preservation checks, vendored-proto check, examples/test compilation, formatting
and diff checks also passed. This change does not edit protobuf definitions.
Cargo invocations ran serially inside one 8 GiB memory scope with swap disabled
and two Cargo build jobs; the full suites used four test threads. These results do not establish fleet deployment or
complete the document publication lifecycle.

Segment-view regressions reproduced two earlier startup defects: a document-only
gap made reopen fail with four exact rows against six physical rows, and a
same-sized stale sidecar replaced all sealed scores with zero. A further regression
showed a manifest-retired row visible again after reopen without a root bitmap.
The lifecycle tests cover corrected scores, physical gaps, explicit byte budgets,
shared mappings and independent masks, parallel duplicate-slot scoring, append
boundaries, dimension mismatch refusal, and sparse flush/export/install/reopen.

An empty configured catalog also ignores exact rows from a retired sidecar.
Its regression covers reopen and snapshot export after complete removal of the
indexed rows. The full library run additionally exposed a streaming-metrics test
sharing global counters with concurrent query tests; that test now runs in an
isolated process while retaining its exact assertions. Production metrics are
unchanged.

The combined segment-view change passed 1,397 local tests: 566 library tests,
816 integration tests across 141 targets, 13 embedded tests and two IVF benchmark
tests. The external OpenNLP contract test remains ignored for the prerequisite
noted above. All five Android/iOS target checks, descriptor preservation checks,
vendored-proto check, examples/test compilation, formatting and diff checks
passed. Cargo invocations were serialized with two build jobs under an 8 GiB
memory limit and zero swap allowance. This is local validation on main's
`8d32a72` base plus the foundation branch, not fleet deployment evidence.
