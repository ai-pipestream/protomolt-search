# Multi-field BM25: format v6 (TVBM2506)

Status: design. Follows the pattern of `block-max.md`: spec first, then
implementation gated on the contracts at the bottom.

## Why

The lexical index scores exactly one field today: `AddDocumentsRequest.text`,
which in the court corpus is the chunk body. Everything else we know about a
document (case name, court, docket caption, headnote) is either a fast-field
candidate or invisible to lexical scoring. Multi-field BM25 makes those
signals scoreable with per-field weights, without giving the engine a schema:
the field set comes from a registered protobuf descriptor (protomolt indexing
hints), and the engine stores only field names and analysis fingerprints.

Naming note: v5 (`TVBM2505`) is the block-max format and is taken. Multi-field
is v6, magic `TVBM2506`. That counter is the BM25 file's own lineage; the
corpus rebuild that ships it is the v7 event at the bundle level (vector
format v7 per-block calibration + BM25 v6 + block-aligned shard cuts), named
by the manifest epoch, not by either file magic.

## Scoring model: weighted per-field sum

```
score(q, d) = sum over fields f of  w_f * bm25_f(q, d)
bm25_f(q, d) = sum over terms t of idf(N, df_f(t)) * tf_norm(k1_f, b_f, tf_f, dl_f, avgdl_f)
```

Each field is, structurally, its own single-field BM25 index over the shared
positional slot space: its own postings, document lengths, df, avgdl, and its
own `(tf, dl)` Pareto frontiers in the skip runs. The fused score is a
weighted sum with query-time weights.

Why per-field saturation rather than true BM25F (one saturation over a
weighted tf sum):

1. Every existing contract survives per field unchanged: the block-max
   exactness proof, the skip-run frontier collapse argument, the
   pruned-equals-exhaustive bit identity. True BM25F needs frontier tuples
   across fields, or independent per-field bounds that are valid but looser.
2. Floors decompose: an upper bound on the fused score is the weighted sum of
   per-field upper bounds, so MaxScore partitioning and the hybrid emission
   floors work with no new math.
3. Weights, k1_f and b_f are query-time knobs. Nothing about them is baked
   into the file. True BM25F with index-time weighted tf would cost a rebuild
   per retune.
4. It is what Lucene and Elasticsearch do by default (per-field queries
   combined by sum or dismax); combined_fields / true BM25F is the exception,
   not the baseline.

True BM25F stays on the table as a later scoring mode behind the same format
(the file stores raw per-field tf and dl; only the combiner changes). Adopt it
only if a relevance A/B on the corpus says so. A dismax combiner (max over
fields plus tie-break) is a cheap variant to include in that A/B.

Determinism rule: the fused sum is accumulated in field-id order, and within a
field in term-index order. IEEE addition is not associative; the existing
scorers already pin term order for exactly this reason, and v6 extends the
pinned order across fields.

## File layout

```
magic "TVBM2506"
header:
  u32 n_fields
  u32 n_slots
  u64 texts_off, text_index_off, lineages_off   <- shared sections, unchanged
  field table, n_fields entries:
    u16 name_len + name bytes                    <- field name from the schema
    u64 analysis_fingerprint                     <- hash of the AnalysisSpec
    u64 total_length_f                           <- avgdl_f numerator
    u64 doc_lengths_off_f
    u64 postings_off_f
    u64 directory_off_f
shared sections (texts, text_index, lineages): v5 bytes (index rebased)
per field f:
  doc_lengths_f (n_slots x u32)
  postings_f    (v5 per-term doc run / occurrence run / skip run)
  directory_f   (v5 34 B entries + term blob, run offsets section-relative)
```

Lessons from v3/v4 encoded:

- v5's header has no version integer and no spare bytes, and one section
  offset is derived by arithmetic. v6 gets an explicit section table; every
  section is located by an absolute u64 in the header, nothing derived.
- Section-internal pointers are RELATIVE to their section's start: directory
  run offsets to the field's postings section, text_index entries to the
  texts section (v5 stored both absolute, which is why only the other four
  sections are bit-identical across the two formats). Sections survive
  relocation, so a future compactor can move a field's group by byte copy.
- `blob_off` stays u32 but is per-field and blob-relative; per-field blobs
  are strictly smaller than today's single blob, so the v4 lesson holds.
- Occurrence pair counters stay u32 per (field, term): cap is 4 G pairs per
  term per field, an explicit `try_from` panic, never a silent wrap.
- Fields with no occurrence data (scoring-only analysis) write an empty
  occurrence run, same as MODE_SCORING_ONLY does today.

Stored text stays one section, shared: the engine stores the chunk body once
(field 0), because stored text serves get_documents and rerank, not scoring.
Additional fields are indexed-only; their raw values live in the document
plane (Postgres) and, where filterable, in fast fields. This is the #427
boundary: the engine owns scoring structures, not documents.

There is no migration path, because there is no migration: multi-field ships
with the v7 rebuild event, a full re-ingest from stored raw materials. The
vector side re-encodes from the raw embeddings files (kept exactly for
format breaks like this); the lexical side replays the WAL, which holds
every document with the exact AnalysisSpec it was ingested under, now
extracting the new fields. Nothing about the current index shape needs to
survive. A v4/v5 file still opens as the degenerate v6 (one field named
"body", weight 1.0) because those readers already exist; that is free
rollback and development convenience, not a maintained compatibility
surface.

## Wire and WAL

`AddDocumentsRequest` gains:

```proto
message DocumentField {
  string field = 1;                  // name, must match the shard field table
  string text = 2;
  AnalysisSpec analysis = 3;         // per-field spec (title unstemmed, body Porter, ...)
}
repeated DocumentField fields = N;
```

The existing `text` + `analysis` pair remains valid and means field "body".
This is a durable-record change: the WAL persists AddDocumentsRequest, so old
records replay as field 0 and reshard replay is the format migration lever,
exactly as it was for v5.

Per-field analysis specs are part of the field table fingerprint. Query
analysis must use each field's spec or term identity breaks; the shard
rejects a query whose per-field fingerprints do not match its field table.
Explicit mapped analysis also requires a nonzero query fingerprint, including
on fields with no rows and metadata-only requests. Legacy unknown identities
retain their existing behavior. The same identity travels with flat BM25,
hybrid legs, rescoring and lexical membership/sorting; see
[query analysis identity](descriptor-mappings.md#query-analysis-identity-2026-09-05-feature-branch).

## Stats: per-field globals, same invariant

The load-bearing invariant is unchanged: no scorer ever reads shard-local
stats. `CorpusStats` becomes per-field:

- `N` shared (a document is a document),
- `total_doc_length_f` and `avgdl_f` per field,
- `df_f(t)` per (field, term).

`TermStats` is keyed by (field, term); `merge_stats` stays an elementwise sum.
The four request protos that carry globals grow the field dimension.

A shard that lacks a named field answers zeros for it, and `FieldStats`
also says `known: false` outright. The flag is what makes the zeros
readable: an empty shard, a shard whose documents never filled the field,
and a shard that has never heard of the field all produce identical
numbers, and one of those is a typo. Shards still SKIP a leg naming a
field they lack, which is right for a heterogeneous fleet, but the
coordinator refuses a field NO shard knows — otherwise a misspelled field
returns the remaining fields' ranking with no error, and an A/B arm with
a typo reads as "makes no difference".

Folded-in fix: the hybrid leg path (`compute_legs`) hardcodes default k1/b
today; `ShardLegsRequest` and `HybridShardRequest` gain k1/b (per field in
v6) so tuning reaches every path, not just Bm25Search.

## The analytics artifact (epoch stats)

Today the coordinator re-derives global stats with a TermStats fan-out on
every query. That is correct but costs a round trip and gives each query its
own snapshot. v6 adds a persisted, epoch-sealed stats artifact:

```
stats-epoch-NNNN:
  N, per-field total_length
  per-field global term directory: term -> df
  per-field per-term dominating corner (max tf, min dl)
```

The dominating corner merges across shards by the same Pareto collapse the
skip runs use, so the artifact is a pure fold over shard directories. It
buys three things:

1. Phase (b) elision: queries score against the frozen epoch, no stats
   fan-out on the hot path. A shard drifting mid-epoch does not change the
   scoring space, so scores stay comparable by construction. This is the
   lexical twin of v7 per-block calibration on the vector side.
2. Coordinator-side upper bounds: `B_max(q)`, the best possible lexical
   score for a query, is computable from the artifact alone (idf from global
   df, tf_norm from the dominating corner). No shard touched.
3. The lexical scoring-space digest: hash(field table, analysis
   fingerprints, k1/b, stats epoch). It goes in the shard manifest next to
   the chunk-recipe digest, and the coordinator refuses to merge result
   streams from shards whose digests differ. Weights are excluded: they are
   a query-time knob and do not change the per-field score spaces.

New documents between epochs score under the frozen stats (slightly stale
idf, bounded and honest) until the next epoch seals, which is a metadata
publish, not a rebuild.

## Hybrid streaming interplay

The fused hybrid score is `w_v * v(d) + w_b * b(d)` over calibrated spaces
(v7 blocks on the vector side, a frozen stats epoch on the lexical side).
With `B_max(q)` from the artifact and the vector side bounded by calibrated
unit-vector dot product, the coordinator derives per-source emission floors
from the fused floor `s`:

```
vector floor  = (s - w_b * B_max(q)) / w_v
lexical floor = (s - w_v * V_max)    / w_b
```

The lexical index typically finishes first (postings over a few terms), so
its k-th best seeds the fused floor within milliseconds and the vector
side's streaming scan (`search_streaming` on the fork) runs floored from
near the start. Both floors tighten monotonically as fused results
accumulate; both sides certify exactness with their done frames.

One subtlety the landed implementation had to face: a BM25-only bound can
never floor the vector scan, because the top lexical doc could sit at any
vector score — the decomposed floor `(s - w_b * B_max(q)) / w_v` is only
non-vacuous once `s` incorporates real vector knowledge. So the landed
FUSION_MODE_DECOMPOSED inserts a phase between the legs: `VectorRescore`
(the vector twin of `Bm25Rescore`, a masked candidate-scoped scan whose
scores are bitwise the streaming scan's) pins v(d) for the BM25 leg's top
k docs, and their true fused scores are the first floor. `B_max(q)` is the
leg's own top score `b_1` — exact, no artifact needed — until the epoch
stats artifact supplies per-(field,term) corners for a coordinator-side
bound with no shard touched.

## Schema binding (protomolt)

The field table is compiled from a registered descriptor at shard create:

- `FieldIndexHint` TEXT fields become BM25 fields; `analyzer` maps to an
  AnalysisSpec; `engine_params["bm25_weight"]` is the default weight
  (query-time overridable).
- The `block_role` chunks field's text is field 0 (stored body).
- `sortable`/`facetable` fields go to fast fields, not BM25.
- The recipe digest (chunking + embedding) and the lexical digest together
  define stream-merge compatibility.

The engine never sees the descriptor; ingest resolves it to the field table
and per-field specs. Two shards built from the same schema at the same epoch
are comparable end to end, by construction.

## Contracts (all inherited, now per field)

1. Dual-writer byte identity: `Bm25Store::save` and `SpillBuilder::finish`
   produce identical v6 bytes for the same corpus.
2. Pruned equals exhaustive, bit-identical, per field and for the fused
   weighted sum under the pinned accumulation order.
3. Distributed equals monolithic exactly, with per-field global stats.
4. `validate_structure` extends to the section table and per-field run
   arithmetic; malformed files error, never panic.
5. v4/v5 files open and serve as single-field v6 with no byte movement.

## Build order

1. Root types: `AnalyzedDoc`, `Posting`, `doc_lengths` grow the field
   dimension; v6 writer/reader with n_fields = 1 proving byte-level parity
   of sections and identical query results against v5. LANDED: `AnalyzedDoc`
   is per-field (`AnalyzedField`), the store is `Vec<FieldStore>` behind the
   unchanged single-field surface, the v6 writer and `Bm25Reader::open`
   round-trip the format, and `tests/v6_format.rs` plus the postings
   section-parity test pin the contract.
2. Multi-field store + reader + exhaustive scorer; contract 3 tests.
   LANDED, with the writer half of step 4 pulled forward: `with_fields` /
   `create_with_fields` construction, per-field reader views
   (`Bm25Store::field` / `Bm25Reader::field`, each its own `Bm25Index`
   served by the unchanged v5 machinery), the fused weighted-sum scorer
   `top_k_fused_exhaustive` (accumulation pinned field-id then
   term-index; contract 3 proven at the store level against per-field
   merged global stats), the per-field `SpillBuilder`, and v6 IS the
   format: `save` and `finish` write v6 everywhere, dual-writer byte
   identity holds on multi-field corpora, and the v5 writer survives
   only as `save_v5`, the oracle for the parity and query-identity
   tests.
3. Per-field skip runs + pruned fused scorer; contract 2 tests. LANDED:
   the v6 writer already emits v5-shaped skip runs per field (pulled
   forward in step 2), so this step is the scorer: `top_k_fused_pruned`
   generalizes the block-max/MaxScore machinery from terms to (field,
   term) pairs, each cursor's bounds scaled by `w_f * idf`, every bound
   sum and candidate score accumulated in the pinned
   field-id-then-term-index order (bounds dominate the true score in
   IEEE arithmetic exactly; full evaluations reproduce the exhaustive
   bits). `tests/fused_pruned.rs` gates contract 2: bitwise
   pruned-equals-exhaustive over random corpora / weights / floors, the
   weight-1.0 degenerate identity with `top_k_pruned`, tie survival at
   the floor and the k-th slot, real block skips under a seeded floor,
   exhaustive fallbacks (no impacts, negative weight), and distributed
   equals monolithic on the pruned path, seeded and unseeded.
4. Wire + WAL + ingest + reshard replay; corpus extraction grows a second
   real field (case name from the cluster metadata) to have something
   honest to score. This step lands as part of the v7 rebuild event:
   vector v7 re-encode from the raw embeddings, lexical v6, block-aligned
   shard cuts (multiples of 8192). NOTE on the replay path: an existing
   WAL cannot carry this step's payload, because its records predate
   `DocumentField` and replay body-only by construction — a WAL replay
   would rebuild the corpus with `case_name` missing. The rebuild
   therefore re-ingests from the chunk texts, which is the same work
   (replay re-analyzes every document anyway) and picks the field up
   natively; WAL replay stays the migration lever for shape changes that
   do not add a field. See `deploy/v7-rebuild/` for the runbook and the
   measured disk model. LANDED (the engine wire; the corpus re-ingest
   itself IS the rebuild event):
   `DocumentField` on AddDocumentsRequest (the WAL's durable record, so
   old logs replay body-only and reshard replay is the migration
   lever), per-field TermStats shares, fused `Bm25FieldLeg` legs on
   Bm25Query (list order = the pinned accumulation order; shards skip
   legs naming fields they lack — exact, since their documents hold no
   postings there), `QueryField` on the client-facing Bm25Search,
   per-field ingest analysis (body on the streaming session, extras on
   concurrent unary calls, positional assembly against the shard's
   `--bm25-fields` table), reshard children deriving or taking the
   field table, the `compute_legs` hardcoded-k1/b fix (ShardLegs and
   HybridShard now carry params), the v5/v6 `Bm25Shard::open` resident
   fix (restarts heap-loaded current-format shards), and
   `court_ingest --case-names=<tsv>` emitting the case_name field
   (unstemmed) from cluster metadata. Gates: `tests/multi_field_wire.rs`
   (fused distributed == monolithic over the wire on both storage
   shapes, reweight-without-reindex, fused kth-best seed round trip,
   per-field stats shares, ingest validation, resident-open formats,
   legs params) and the reshard multi-field gate (children derive both
   fields, conserve per-field postings, fused ranking survives
   bitwise). Field-table fingerprints stay 0 until the ingest layer
   wires real AnalysisSpec hashes; name equality is the current check.
5. Epoch stats artifact + digests; phase (b) elision behind a flag.
6. Hybrid floor integration with the streaming vector side. LANDED as
   FUSION_MODE_DECOMPOSED: the exact fused weighted-sum top-k
   `w_v * v(d) + w_b * b(d)` with no leg truncation, executed
   BM25-first — leg to depth leg_k (its top score IS the exact
   `B_max(q)` for floor purposes), `VectorRescore` pins v(d) for the
   leg's top k docs (see the interplay note above for why a BM25-only
   bound cannot floor the vector scan), every vector stream opens with
   the decomposed floor, re-decomposed raises chase the k-th best
   known fused lower bound mid-scan, and a candidate-scoped
   `Bm25Rescore` close-out pins b(d) for emitted docs the leg did not
   cover (docs an unfilled leg proves absent score exactly 0). The
   same increment landed document-mode streaming (the second search
   mode): `StartStreamSearch.collapse_parents` tags every emission
   with its parent (20-byte records), the coordinator owns the whole
   parent aggregation — floors are k-th best PARENT scores, the
   response carries per-parent chunk groups filtered to the final
   floor (cross-shard chunk retrieval, no colocation) — and the
   sign-agnostic floor seed fix (vector scores are signed; clamping a
   negative k-th best to zero was a latent recall bug on the plain
   streaming path). Gates: `tests/decomposed.rs` (bitwise equality
   with the exhaustive fused oracle across weightings, leg depths,
   and the min_vector_score gate; rescore-equals-scan bitwise;
   degenerate empty leg; weight validation) and
   `tests/stream_collapse.rs` (representatives equal the bidi
   collapse path, groups exactly the chunks at or above the returned
   floor, straddler parents retrieve from both shards, self-parent
   fallback).
