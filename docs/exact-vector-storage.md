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
Startup reconstruction and explicit whole-shard flushes can still copy all FP32
rows. The document publisher must avoid those operations for each version and
join exact vectors, segment manifests, row masks and read versions coherently.

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

The combined branch includes main through `8d32a72`. Local validation passed
1,390 tests: 566 library tests, 809 integration tests across 140 targets,
13 embedded tests and two IVF benchmark tests. One external OpenNLP contract
test remains ignored because it requires a running analysis service and scans
every Unicode scalar. The five Android/iOS target checks, protobuf descriptor
preservation checks, vendored-proto check, examples/test compilation, formatting
and diff checks also passed. This change does not edit protobuf definitions.
Builds and tests ran serially inside one 8 GiB memory scope with swap disabled
and two Cargo build jobs. These results do not establish fleet deployment or
complete the document publication lifecycle.
