# Replay from segments

A re-placement split moves documents between shards. It changes which
shard a document lives on and the placement code it carries, and it
changes no term of its analysis: the postings a sealed segment already
stores for the document are the postings the child needs. The first
splits replayed the WAL and sent each document's text back through the
analysis sidecar, which made the child build a re-ingest: on the 66M
document archive of 2026-09-06 that was about 3,700 documents a second
on one thread, the sidecar at a core and a half, the disks idle, six
hours for the six bands. This document describes the replay that takes
each document's analyzed data from the source segments instead, and
the year cut of the spill that makes a fresh split come out
partitioned so no compaction has to follow it.

## What a sealed segment stores, per document

The BM25 store of a sealed segment (`src/postings.rs`, the v8 container)
keeps, for each field of the field table:

- the doc-length table (one `u32` per row) and the field's total length;
- a directory of terms, sorted, each with a document run (`doc_id`,
  `tf`, occurrence start) and an occurrence run of `(start, end)`
  UTF-16 spans (`docs/highlighting.md`), when the file was written with
  skip runs (v5 and later; the archive is v8);
- for a positional field, a kind-7 positions section: one ordinal per
  occurrence, parallel to the occurrence run (`docs/phrase-proximity.md`);
- for a sentence field, a kind-8 sentence table per row
  (`docs/highlighting.md`);
- the field's analysis fingerprint.

Outside the fields: the stored body text and lineage per row, the
original protobuf source and document identity per row when the
document had them, the kinded column tables (facet dictionaries and
ordinals, f64 and i64 and u64 columns, geo points, map facet and map
numeric columns), the live-document overlay, the FP32 exact-vector
sidecar and the provider image (one row per vector slot), and the
segment's metadata (base label, rows, fingerprints, summary).

The WAL still carries the text of each document, and the transplant
changes what is persisted in no way: a later re-analysis under a new
analyzer remains a WAL replay.

## The transpose

The postings are keyed by term; the child build wants them keyed by
document. `FieldView::transpose` (`src/postings.rs`) walks the field's
directory once, in term order, and for each term's document run copies
`(doc_id, tf, occurrences, positions)` into flat arrays, then groups the
entries by document with a counting sort over the row ids (rows are
dense, `0..rows`). The result, `FieldTranspose`, materializes one
document's `AnalyzedField` on demand: its terms in directory order, the
tf and the occurrence spans from the runs, the ordinals from the
positions section when the field has one, the sentence table from the
kind-8 section when the field has one, and the length from the
doc-length table.

The bound is one field of one segment: every posting entry (16 bytes),
every occurrence (8 bytes), every ordinal (4 bytes), plus the row
prefix table. For a 350,000-row archive segment with about 200
occurrences per document that is about 1.2 GB for the body field, freed
before the next segment. A file without skip runs (v3, v4) cannot be
transposed cheaply and is refused by name.

## The replay

`reshard --from-segments` takes the same `--logs` as the WAL replay. Each
log names its generation directory; the catalog is the sibling
`<index>.tv.segments` of the log's `<index>.tv.wal`, refused by name when
it is absent (a single-image source has no segments to read). Before
anything is written the sources are checked:

- every source's WAL has no row past its catalog (the catalog's row
  count is the WAL's high watermark plus one): an unsealed tail would
  be lost, so the split refuses and names the source; flush it first;
- every source shares one BM25 field table, the same positional and
  sentence fields, and the same analysis fingerprint per field, and one
  set of column tables: a child is one store and cannot mix analyzers;
- every source's BM25 files carry skip runs.

Pass 1 walks each source's segments in label order. Per segment it
transposes every field, opens the exact-vector sidecar and the
live-document overlay, and for each live row reconstructs the logged
document from the store (text, lineage, original source and identity,
every column value, the positional and sentence field names) and the
FP32 row from the sidecar. The placement tree is evaluated on the
reconstructed columns, exactly as the WAL replay evaluates it on the
logged ones, and the row goes to its child's spill: the document and
the vector as WAL records under the source id, as before, and the
transplanted fields in an analysis sidecar beside the spill bucket
(`analysis-<bucket>.bin`, one framed entry per row: field count, then
per field the terms with tf, spans and ordinals, the sentence table,
and the length). The spill's WAL half is what the child build has
always replayed; the sidecar is what replaces the analyzer.

Pass 2 is the segmented child build as before, one spill bucket at a
time: the bucket's WAL records replay into memory, the bucket's analysis
sidecar loads beside them (one bucket of rows, the same bound), and
`build_child` takes each document's fields from the sidecar keyed by
source id instead of calling the analyzer. A document without an entry,
or an entry without a document, is refused by name. The child's field
table, positional and sentence fields, analysis fingerprints, and column
tables are the sources' own, pinned before the first document.

What is copied verbatim: term spans, ordinals, sentence tables, field
lengths, vectors, column values, text, lineage, sources and identities.
What is recomputed by the build from the copied data: the child's
postings, dictionaries, skip runs, document frequencies, total lengths,
column dictionaries and summaries, the provider image (from the FP32
rows under the same calibration). What is not carried: stable routing
keys, which live in WAL envelopes and not in segments, so a leaf with
several shards is refused by name under this mode (split it from the
logs, or give the leaf one shard).

The spill sidecar rather than a pull from the source at build time: the
build handles one spill bucket, whose rows come from every source
segment, so pulling would need every source's transpose resident or a
transpose per (segment, bucket) pair; the sidecar is written once while
each segment's transpose is resident and read once per bucket, and the
bound stays at one transpose plus one bucket.

## Cutting the spill by the partition column

With a hash cut, a child's segments each hold every year, and the
partitioned compaction (`docs/mutations.md`) has to rewrite the child
to give segment pruning a year range to skip. `--cut-column=year
--cut-rows=<n>` cuts each child's spill by the column's value instead:
a first pass over the sources counts rows per (child, value); the cut
points per child are chosen in ascending value order so that a cut
holds at most `n` rows, a run of one value moving to the next cut as a
unit when it would overflow (a run longer than `n` is split and its
neighbours share the value, as the partitioned compaction does); rows
without the value go to the last cut. The spill's WAL writer appends
each record to the bucket the cut names (`WalWriter::append_to_bucket`)
instead of the bucket the id hashes to, and the child build replays the
spill without the hash-routing check that a node's log is held to (the
spill is the split's own). Each sealed segment is named as partitioned
by the column, so its summary records the value range and the catalog
manifest carries the partition key: a fresh split comes out in the
layout a compaction would have produced, and the compaction step is
unnecessary. The answers are the same as the hash cut's: the same rows
with the same postings under the same shard statistics; only the
segment membership differs.

The first pass reads columns only (the transposes are not needed), so
it costs a scan of the column tables under `--from-segments`, and a WAL
replay of the columns under the log replay.

## Partitioned compaction of a catalog without a log

The children of a segmented split have no WAL: the spill logs build the
catalog and are removed. `CompactShard` replays the log into its
outputs and refuses a shard without one by name (`src/compaction.rs`),
so the partitioned compaction cannot run on such a child today. The
year cut above removes the need for a fresh split. For a catalog that
is served and takes writes (a tail sealed by row count, in ingest
order), the compaction wants the same transplant: build the outputs
from the shard's sealed segments through `FieldTranspose`, rows keyed
by the partition column, the tail that arrives after the cutoff sealed
unordered as it is now. That keeps the shadow and cutover contract
(`docs/mutations.md`): the outputs are staged under the work directory,
the tail is caught up through the same apply functions ingest uses,
and the cutover publishes one manifest. The pieces that differ from the
log replay: the row source (segments, the live overlay applied, instead
of log records), the analysis (transplanted, never the sidecar), and
the precondition (a catalog with a generation binding and sealed
segments, instead of a complete log). It is not in this change; the
plan is recorded here so the next step starts from the same transpose.

## Reference

`src/postings.rs` (`FieldView::transpose`, `FieldTranspose`),
`src/reshard.rs` (`TreeRowSource`, `SpillCut`, the analysis sidecar,
`build_child` with transplanted fields), `src/wal.rs`
(`WalWriter::append_to_bucket`), `src/segments.rs`
(`SegmentCatalog::publish_partition_key`), `examples/reshard.rs`
(`--from-segments`, `--cut-column`, `--cut-rows`),
`tests/replay_from_segments.rs`.
