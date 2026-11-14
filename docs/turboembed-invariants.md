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

SPLADE requests above the configured `--max-k` are refused before field or
query-term shortcuts, both by the coordinator and by `NodeService.ScoreSplade`.
The coordinator retains its live `max_k` knob; a node's startup `NodeConfig.max_k`
is exposed as the fixed `score_splade_max_k` diagnostic. Other node RPC limits
are unchanged. SPLADE `k=0` still returns no hits. The low-level reader bounds
heap reservation by the matching query terms' posting counts, using saturating
arithmetic even for `usize::MAX`; admission and lower-document-id ties retain
their existing behavior.

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

## Retained vector tiers

The provider and its FP32 sidecar contain the active indexing tier. Retained
full vectors, prefixes, norms, optional codes, and sentence vectors belong to
the same shard generation. They survive flush, restart,
snapshot installation, source backup/restore, and row renumbering. Recovery
loads their original float bits; it does not normalize the active tier again
or invoke an embedder. Tombstones apply before tier rescoring.

A single-image generation writes `<index>.multi` with an authoritative
`<index>.multi.json` declaration. Snapshot generations use `vectors.multi`
and `vectors.multi.json`. Single-image retained generations use live-doc
version 2 (unchanged bitmap geometry), so older node readers refuse even
when WAL is disabled. This CRC-protected version also requires a retained
declaration on recovery. Ordinary generations continue writing version 1.
Each immutable segment declares its optional
`vectors.multi` artifact in the catalog, including its byte count and SHA-256.
The private `PMMRES01` payload also verifies its row span and integrity footer.
Each record is bounded to 64 MiB including its enclosing row message, and a
separate decoded-allocation budget is checked before protobuf allocation.
This does not impose a whole-corpus heap limit on retained vectors.

Missing declared files, mismatched hashes, out-of-range rows, and absent
configured tiers refuse recovery or installation before replacing serving
state. A generation written before retention cannot recover a discarded full
vector from its active prefix; rebuild it from full-resolution input. Explicit
tier queries report unavailable retained data rather than silently omitting a
live vector candidate. Document-only positions remain valid gaps.

`AddVectorsRequest.materialized_rows` (tag 3) carries optional retention records
parallel to its active vectors. Admission checks their cardinality and exact
active-tier float bits. Replication negotiates
`HealthResponse.multi_res_vector_version >= 1` before sending them. Ordinary
vector batches leave this field empty. The WAL publishes format 9 before
appending retained records; this reader continues accepting prior WAL formats.
Retained segment catalogs use format 5, and repository snapshots carrying
retention use format 2, so older readers refuse them. Streamed snapshots append
the retained payload after the live bitmap and carry its bounded declaration
in the manifest. The `.bm25` column layout and vector-provider image formats
are unchanged.
