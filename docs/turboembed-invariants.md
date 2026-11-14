# TurboEmbed storage, phrase, and replay invariants

The Epic #102 integration stores lattice arc lengths in column kind 18 and
SPLADE impacts in column kind 19. Both remain optional: older `.bm25` images
without these columns use unit-length positional arcs and no sparse impacts.
The enclosing section CRC covers both payloads.

## Sparse scales and traversal

Uniform-scale fields retain the original `TVSPLD01` bytes. A field containing
multiple document scales uses `TVSPLD02`: each block header appends a positive
finite maximum scale (`f32` bits), and each posting appends its original
document scale (`f32` bits). Integer impacts are never requantized or clipped
when documents are grouped into segments or compacted. This reader accepts
both versions; readers predating v2 cannot serve newly written mixed-scale
sections. Upgrade readers before publishing those sections.

A mixed-scale block bound is query weight times maximum integer impact times
maximum scale. The maxima may come from different documents, which makes the
bound conservative. Block and global pivot sums cover the worst-case `f32` summation error when
traversal and scoring orders differ. Scores accumulate in term order.
Block bounds include every active query term, even those not yet aligned at
the candidate document. They apply only through the earliest current block
boundary; the traversal stops its skip there. Admission filters run before heap insertion;
equal scores prefer the lower document id.

Field transposes, bounded source-owned rewrites, and the reshard/partitioned
compaction analysis sidecar preserve impacts, scales, and arc lengths. New
sidecar records set bit 7 of the position flags; bit 1 declares arc lengths.
The reader also accepts legacy sidecars with only position/sentence flags.
Rewrite certificate format 3 hashes sparse impacts/scales and positional arc
lengths, treating an absent length column as unit arcs. It certifies live rows
independently of physical order and segment boundaries.

## Phrase queries

A query lattice is expanded into complete alternative term sequences, capped
at 4,096 arcs and 256 paths. Excessive expansion is rejected. `PhraseLeg`
retains its original sequence/slop and adds alternative sequences, evaluated
with OR semantics through the same positional predicate. Alternative paths
require positions rather than the two-term bigram shortcut. Coordinators and
shards must both understand the alternatives before serving lattice queries.

## NLP replay

Before WAL append, NLP materialization writes `AddDocumentsRequest` tag 30,
`nlp_materialization`, as `v1:` plus the SHA-256 of the serialized column
mapping. The marker is written even when extraction produces no column values.
Replay retains the mapping but requests no NLP layer, preserves logged values,
and rejects an unknown marker version or a changed enabled mapping. A stream
switches analysis sessions when it crosses between fresh and materialized rows.
Lexical analysis is independent of this NLP marker.

Old records without the marker remain unmaterialized inputs; there is no safe
way to infer completed empty extraction from absent column values.
