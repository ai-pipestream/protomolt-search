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

## Stats: per-field globals, same invariant

The load-bearing invariant is unchanged: no scorer ever reads shard-local
stats. `CorpusStats` becomes per-field:

- `N` shared (a document is a document),
- `total_doc_length_f` and `avgdl_f` per field,
- `df_f(t)` per (field, term).

`TermStats` is keyed by (field, term); `merge_stats` stays an elementwise sum.
The four request protos that carry globals grow the field dimension.

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
3. Per-field skip runs + pruned fused scorer; contract 2 tests.
4. Wire + WAL + ingest + reshard replay; corpus extraction grows a second
   real field (case name from the cluster metadata) to have something
   honest to score. This step lands as part of the v7 rebuild event:
   vector v7 re-encode from the raw embeddings, lexical v6 from WAL
   replay, block-aligned shard cuts (multiples of 8192).
5. Epoch stats artifact + digests; phase (b) elision behind a flag.
6. Hybrid floor integration with the streaming vector side.
