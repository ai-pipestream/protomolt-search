# Server-side highlighting

Implemented on branch 2026-09-02 (roadmap item 9). A hit already carried every
occurrence span of every query term in original-text coordinates, and
the texts are in the index. What was missing was snippet assembly on the
shard: window selection, merging of overlapping spans, and a boundary
rule that is not "mid-clause". This increment stores the analyzer's
sentence spans at ingest and cuts snippets against them at query time,
so a client gets the sentence a term was found in rather than the whole
document, at zero query-time analysis cost.

## Ingest: the sentence table

`--sentence-fields=body` (config `sentence_fields`) declares that the
body keeps sentence spans. Every document then carries the analyzer's
sentence layer:

- The sidecar: `sentence_detection` is requested on the ingest session
  (`SessionLayers.sentences`), one traversal of text the sidecar already
  holds. The spans come back in **original-text UTF-16 units**, the one
  coordinate system every persisted span uses — before any char filter
  (accent folding, invisible stripping, case folding) touched the text.
  This is the same layer the sidecar's `SOURCE_SENTENCES` embeddings are
  computed from ("parallel to the annotations of the requested source
  layer, anchored to original-text spans"), so sentence `k` of the stored
  table is embedding `k` of the same text.
- Native analysis: the newline detector — each maximal run of text
  between line breaks, trimmed — runs in the same pass as tokenization.
  It is the sidecar's model-free default rule, and every token lies
  inside exactly one of its sentences.

The node fills `AddDocumentsRequest.sentence_fields` from its
configuration before the document is analyzed or logged, exactly as it
does `position_fields`: a WAL replay stores the same spans, a replay on
a node with a different setting refuses by name, and a reshard child
inherits the table from the record alone.

Ingest checks the table before anything is written: sorted,
non-overlapping, no empty span, and **every occurrence of every term
inside one sentence**. A document the analyzer returned without the
layer, or with a sentence table that leaves a term uncovered, is
`FAILED_PRECONDITION` naming the field — never indexed without spans,
never a snippet-less hit later. Only the body qualifies: snippets are
cut from stored text, and only the body's text is stored, so
`--sentence-fields` accepts `body` alone.

### Format

A kind-8 entry in the v7 column table, named `sentences:<field>`,
`u64 off | u64 total`, and a section after the positions sections:

```text
u32 n_slots | (n_slots + 1) x u32 base | total x (u32 start, u32 end)
```

`base[d]..base[d + 1]` are document `d`'s sentences in text order. The
heap writer and the bounded spill builder produce byte-identical files
(the spill builder streams spans to a stage file as documents arrive and
keeps 4 B per slot in heap, the doc-length table's own budget). The
validator ties the entry to a field of the table, checks the slot
count, the base table's monotonicity from 0 to `total`, and the exact
section size; the spans are payload, CRC-covered, and left on disk at
open. The reader answers `field_doc_sentences(f, doc)` from the map with
two base reads and one slice — touched only for returned hits.

Cost: 4 B per slot plus 8 B per sentence, and nothing on the postings.
Measured on 20,000 CourtListener chunks under `body_spec` with the
newline detector (`examples/bigram_cost.rs`): 3.04 sentences per
document, `column:sentences:body:vals` 566,720 bytes = **28.3 B/doc**
against a body index of 3,806.5 B/doc — +0.74% of the body index, +0.56%
of the file (5,079.4 → 5,107.8 B/doc). The OpenNLP sentence model cuts
finer than line breaks on this corpus, so a sidecar-ingested shard pays
8 B for each extra sentence it finds and no more; the per-document
count is the only variable. For comparison, token positions cost 847
B/doc and the bigram column 9,146 B/doc on the same chunks
(`docs/phrase-proximity.md`).

## Query: snippets

`HighlightSpec` on `Bm25SearchRequest` (flat and fused routes alike),
and on `QueryRequest` for the single lexical selection:

| field | meaning | default / bounds |
|---|---|---|
| `fields` | fields to cut from; only the body qualifies | `["body"]` |
| `max_snippets` | per hit per field | 3, at most 64 |
| `max_chars` | snippet width in UTF-16 units | 300, 16 to 4096 |
| `mode` | `SENTENCE` (default) or `WINDOW` | |

Each returned `Bm25Hit` (and `QueryHit`) then carries `snippets`, in
text order, each with:

- `text`: the UTF-8 slice of the stored text at `[start, end)`;
- `start`, `end`: UTF-16 units of the **original** text;
- `highlights`: the hit's occurrence spans inside the snippet, merged
  (no two overlap or touch), ascending, in the same units — subtract
  `start` for snippet-relative positions;
- `cut`: how the bounds were chosen;
- `sentence_index`: for a sentence cut, the sentence's ordinal in the
  stored table.

The hit keeps its full `terms` occurrence list either way; snippets are
in addition, never instead.

### Sentence mode

The unit is a stored sentence. Sentences holding at least one
occurrence are ranked by distinct query terms, then occurrence count,
then position; the top `max_snippets` come back in text order. A
sentence within `max_chars` is returned whole (`SNIPPET_CUT_SENTENCE`).
One wider than the budget is trimmed to a window inside it around its
first highlight (`SNIPPET_CUT_TRUNCATED_SENTENCE`). A field without a
stored table refuses this mode by name (`FAILED_PRECONDITION`, naming
`--sentence-fields=body` and the window alternative); nothing
approximates sentences from punctuation at query time.

### Window mode

No sentence spans are consulted. Highlights within `max_chars` of each
other form one cluster; each cluster gets a window of at most
`max_chars` around it, and windows that overlap after cutting merge.
Every snippet is `SNIPPET_CUT_WINDOW`. This is the documented
non-sentence cut, and the only one: it exists for fields ingested
before sentence spans and for callers that want fixed-width context.

### The cut rule

A window edge (in either mode) lands on whitespace or on the text's
edge, never inside a token; trailing whitespace is trimmed. If honoring
that would push the anchoring highlight out of the window — a highlight
wider than the budget — the window becomes the anchor's own
whitespace-delimited run and may then exceed `max_chars`. A window
start at a stored sentence's start, or an end at its end, counts as an
edge.

Offsets are resolved through one walk of the stored text per hit
(character boundaries to UTF-16 units). An occurrence that outruns the
text or splits a surrogate pair is a contract break and refuses as
`INTERNAL` naming the span; it cannot come from an analyzer that reports
code-point-aligned spans over the same text.

### Refusals

`INVALID_ARGUMENT`: `max_chars` below 16 or above 4096, `max_snippets`
above 64, an empty or repeated field name, an unknown mode, a field
other than the body ("snippets are cut from stored text, and only the
body's text is stored"). The coordinator checks the spec before any
shard is asked, so an empty fleet refuses like a full one.
`FAILED_PRECONDITION`: sentence mode over a field whose storage declares
no sentence spans. On `Query`, any shape but the single lexical
selection refuses by name: only that leg carries the occurrence spans
snippets are cut around.

### Multi-field

On the fused route the occurrence spans of every leg come back on the
hit, but snippets are cut only from the body — the stored text. A hit
matched only in another field is a hit with occurrences and no
snippets; asking for that field refuses by name.

### Offsets, normalization, and lineage

Every offset in a snippet is the original text's, in UTF-16 units. A
term whose surface form the char filters shorten (`Rodríguez​`
folds to `rodriguez`) still highlights its original characters. A
chunk's `DocLineage.span_start` composes with a snippet's offsets when
the pipeline's chunk spans are UTF-16 units of the parent (Java string
indices, as the CourtListener chunker emits): `span_start + start` is
the snippet's position in the parent document. `tests/highlighting.rs`
pins both.

## What this does not do

- No query-time analysis: the query text is analyzed once, as before,
  and highlighting adds no call (metered against the mock sidecar).
- No whole documents as the mechanism: a snippet is at most `max_chars`
  wide unless a single highlight is wider.
- No sentence inference at query time: a field without stored spans
  gets the named window cut or a refusal, never guessed sentences.
- No stored text for non-body fields; that is a separate cost decision.
- Sentence-level snippets for dense hits are not served: the engine
  holds one vector per document, and which sentence a vector matched is
  not a fact it has. `sentence_index` exists so a pipeline that keeps
  per-sentence embeddings can name the sentence it matched against the
  same table.

## Tests

`tests/highlighting.rs`: the wire happy path (bounds, merging, order,
ranking, `max_snippets`), truncation and window cuts on token edges,
UTF-16 offsets with non-BMP characters, the offsets-before-normalization
and lineage composition pin, the refusal table, multi-field, distributed
== monolithic bitwise (hits and snippets), the `Query` adapter and its
refusal, the no-analysis meter and the layer-less sidecar refusal,
flush / reopen / WAL replay (reshard) round trip, an old file without
the section (serves, refuses sentence mode, refuses sentence ingest, no
silent upgrade), and the exact price of the column with heap and spill
writers byte-identical. `src/highlight.rs` unit-tests the cutting
arithmetic; `crates/protomolt-analyzer` tests the newline detector.
