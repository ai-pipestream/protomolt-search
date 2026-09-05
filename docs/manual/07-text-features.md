# Text features

## Analysis

Every term in the index came out of an analyzer. The `AnalysisSpec` on a request
picks the tokenizer, the stemmer, the term-vector mode, the term-vector source,
and the character filter chain, as the numeric values of the analysis sidecar's
own enums. A query must use the same spec the documents were ingested under, or
it scores terms the index does not contain.

Two providers serve it:

- **Native** (`--analysis-addr=native`). An in-process Rust analyzer, no network
  call and no JVM. It supports whitespace and UAX29 tokenizers, no-stemmer and
  Porter, full and scoring-only term vectors, the tokens, stems, and
  normalized-stems sources, and the strip-invisible, whitespace, accent-fold,
  and full-case-fold character filters. Text is capped at 1 MiB. An absent
  `AnalysisSpec` is rejected here, because "the sidecar's configured default" is
  not something the native provider can know. Any option outside that table
  fails by name instead of being approximated.
- **The OpenNLP analysis sidecar** (`--analysis-addr=host:port`). The wider
  surface: static embeddings, sentence detection, quality and geography layers,
  model NER, and model- or dictionary-backed tokenizers and stemmers.

Offsets are stored as UTF-16 code units of the original text, one unit for the
entire index. A provider that answers in another unit when UTF-16 was requested
is rejected, so one generation cannot mix coordinate systems.

Changing an analyzer changes persisted term identity. Treat it as an index
rebuild, not a setting.

## Dual-cased terms

`AddDocumentsRequest.cased_field` names a second declared BM25 field that
receives the body's **cased** identity from the same analysis pass: the same
chain minus case folding. `COURT court Court` is one term with 3 occurrences on
the body and 3 distinct terms on the cased field. No second analysis runs, at
ingest or at replay, so the cased column costs postings and no more.

The named field must be in the field table, must not be `body`, and must not
also appear in `fields`. The body's analysis must be an explicit spec with a
step-chain source (tokens or normalized stems); the plain stems source ignores
the character filter chain, so it has no case-insensitive form to contrast
with, and is rejected. The sidecar must advertise dual term identity, or ingest
is rejected by name. A query on the cased field must set that field's analysis
to the matching spec.

## Phrases and proximity

`PhraseMatch { slop }` requires the analyzed terms to occur in the field in
query order, in a window holding at most `slop` extra token positions.
`slop = 0` is an exact phrase. Positions are token ordinals from the ingest
analysis, not character offsets, so a token that normalized away still counts as
a gap.

Set it on `Bm25SearchRequest.phrase` (the body, flat route), on
`QueryField.phrase` (per field, fused route), or on `LexicalQuery.phrase` (the
single lexical leaf of `Query`).

A field serves a phrase only through a payload it declared at ingest:

- `--bigram-fields=body` derives a field `body.bigrams` of adjacent token pairs.
  A two-term exact phrase becomes one term lookup. It answers no other shape:
  three adjacent pairs do not certify a three-term phrase.
- `--position-fields=body` keeps one token ordinal per occurrence and answers
  longer phrases and any slop.

A field that declared no payload is rejected by name, naming what it lacks. A fleet
where only some shards keep positions is rejected too, instead of matching the
phrase on part of the corpus. The response reports `phrase_routing`, so you can
tell a bigram answer from a positional one.

Cost on a corpus of court chunks: positions add about 22% to the body index,
bigrams about 240%, because nearly every pair in a chunk is distinct. Start with
positions.

Phrases are served on the flat and fused lexical routes and on the single
lexical leaf. Boolean clauses, composite strategies, and boosts reject them.
Score stages, stats, cardinality, and projections are rejected alongside a
phrase.

There is also a separate glossary feature: `SearchService.PhraseSearch` adds
BM25 evidence for **registered** multiword concepts loaded from a TSV file
(`--phrase-glossary`, `--phrase-field`, optional `--entity-map-field` and
`--phrase-ner`). It synthesizes no arbitrary n-grams, and a document takes only
the strongest matching concept from that field, so nested concepts do not stack.

## Prefix terms and string ranges

`TermPrefix { prefix, max_expansions }` expands a prefix against the field's
byte-sorted term dictionary and adds every matching term to the query, each
given the score of the ordinary term it is. Set it on `Bm25SearchRequest.prefixes`,
`QueryField.prefixes`, or `LexicalQuery.prefixes`. The response reports
`prefix_expansions`, so the terms that got scores are not a guess.

The prefix is normalized under the field's character filters and is not stemmed,
because the dictionary has stems. An absent `AnalysisSpec` is rejected: the
coordinator does not know the sidecar's default chain.

`max_expansions` defaults to 128 and the maximum is 1024. A prefix that expands
past the cap on any shard, or in the fleet-wide union, is INVALID_ARGUMENT
naming the count. The set is not truncated.

The same byte order serves CEL string comparisons. `court < "b"`,
`court >= "a" && court < "b"`, `court.startsWith("ca")`, and the same on a
map-facet value all compile to one ordinal range per shard. A shard with a
dictionary written in an older first-seen order serves equality and rejects
ordering by name.

`endsWith`, `contains`, regular expressions, and wildcard matching are rejected.
A byte-sorted dictionary resolves prefixes and ranges; fuzzy matching is served
as did-you-mean instead.

## Highlighting

`HighlightSpec` on `Bm25SearchRequest.highlight` or `QueryRequest.highlight`
returns snippets cut on the shard from the stored text and the occurrence spans
it already had. No analyzer runs on the query path.

- `fields`: only the body has stored text; any other name is rejected.
- `max_snippets`: 3 by default, at most 64.
- `max_chars`: 300 by default, at least 16 and at most 4096, in UTF-16 units.
- `mode`: `HIGHLIGHT_MODE_SENTENCE` (what the unset value resolves to) or
  `HIGHLIGHT_MODE_WINDOW`.

Sentence mode needs sentence spans stored at ingest (`--sentence-fields=body`);
a field without them is rejected by name, and window mode cuts at whitespace
instead. Each `Snippet` has the field, the text, `start`/`end` in UTF-16 units
of the original text, merged ascending `highlights` inside it, a `cut` kind, and
for a sentence cut the sentence's ordinal in the stored table. A window edge
falls on whitespace or on the text's edge, and not inside a token.

On `Query`, only the single lexical selection serves snippets; other shapes are
rejected, since only that side includes occurrence spans.

## Autocomplete

`SearchService.Suggest` completes a prefix over one indexed BM25 field's
dictionary, ranked by document frequency summed over the collection's shards,
ties in term bytes.

- `field`: any indexed field: the body, a per-field column, the cased counterpart, the
  glossary phrase field. An empty or unknown name is rejected.
- `prefix`: non-empty, normalized under the spec's character filters and not
  stemmed.
- `limit`: 10 by default, maximum 100.
- `max_scan`: 100,000 by default, maximum 1,000,000, applied per shard and
  fleet-wide. A prefix with more matches is INVALID_ARGUMENT naming the count.
- `analysis`: required.

The response has the suggestions, `dictionary_terms_with_prefix` (the fleet-wide
count under the prefix whether or not each term came back), and
`df_includes_tombstoned_rows`.

A stemmed field suggests stems. On a Porter-stemmed body, `courtes` completes to
`courtesi`, and `courtesy` completes to no term, because no dictionary term
starts with it. Surface-form completion needs a field indexed without a stemmer.

The df here is posting df, so a deleted row still counts until compaction
rewrites the segment; `df_includes_tombstoned_rows` states when that applies.

## Synonyms and did-you-mean

Synonym rules rewrite the query at request time. No posting is added at ingest.

A `SynonymRule` is symmetric when `to` is empty (every entry expands to every
other), and one-way when `to` is set (each entry in `terms` expands to the `to`
entries and not back). Rules come from the coordinator's table
(`--synonyms=<file>`) and from the request
(`Bm25SearchRequest.synonyms`, `QueryField.synonyms`, `LexicalQuery.synonyms`),
with `synonyms_off` skipping the table for that request.

Entries are written as surface words. The coordinator analyzes them under the
field's spec, stemmer included, so a rule written as `automobile` matches the
stem the dictionary has. Each added term is an ordinary query term with its
own statistics, so a query with the rule scores bit for bit like the expanded
query typed out. `synonym_expansions` on the response reports what each term
expanded to. A rule with fewer than 2 entries, or a blank entry, is rejected
before any shard is contacted.

`SearchService.TermSuggest` is did-you-mean. It analyzes the text under the
field's spec and, for each term, proposes dictionary terms within `max_edits`
optimal string alignment edits sharing the term's first `prefix_length`
characters, ranked by distance, then summed df, then term bytes.
`TERM_SUGGEST_MODE_MISSING` (the default) proposes only for terms no shard's
dictionary has; `TERM_SUGGEST_MODE_ALWAYS` proposes for every term, excluding
its own entry. `max_edits` defaults to 1 and stops at 2. `prefix_length`
defaults to 1; 2 is a better setting on a large corpus, because a one-character
prefix scans wide. `limit` defaults to 5 per term, maximum 100. `max_scan`
behaves as it does for `Suggest`, and past the bound the request is rejected
naming the count and `prefix_length` as the knob.

## Several fields at once

`Bm25SearchRequest.fields` (a list of `QueryField`) replaces the flat
single-field query with a weighted per-field sum. Each entry has its field,
its own analysis, a `weight` (0 selects 1.0), and its own `k1` and `b`. The list
order is the accumulation order, because floating-point addition is not
associative, and it must be the same on every shard.

Setting `Bm25SearchRequest.analysis` together with a non-empty `fields` is
rejected instead of ignored. Score stages, stats, and projections are flat-route
only.

A shard that lacks a named field skips it, which is correct for a fleet in the
middle of a migration. The coordinator rejects a field that **no** shard knows,
because a misspelled field would otherwise return the remaining fields' ranking
with no error.

Reference: `docs/native-analysis.md`, `docs/phrase-proximity.md`,
`docs/highlighting.md`, `docs/suggest.md`, `docs/synonyms.md`.
