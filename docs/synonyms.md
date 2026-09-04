# Synonyms and did-you-mean

Landed 2026-09-04 (L8 and the term suggester). Both are query-time
rewrites over the dictionary and the analyzed query: nothing changes at
ingest, no posting is added, and every result is the ordinary BM25 result
of the terms the rewrite named.

## Synonyms

A rule is a line of words (`SynonymRule`):

- **symmetric** — `terms = ["car", "automobile", "motor vehicle"]`: every
  entry expands to every other;
- **one-way** — `terms = ["nyc"]`, `to = ["new york city"]`: each `terms`
  entry expands to the `to` entries and never back.

Rules live in the coordinator's table (`--synonyms=<file>`, a TOML file
of `[[rules]]`, also per collection) and on the request:
`Bm25SearchRequest.synonyms` (the flat route), `QueryField.synonyms`
(that field on the fused route), and `LexicalQuery.synonyms` (the single
lexical leaf of `Query`). The table applies to every lexical query of the
collection unless the request says `synonyms_off`; the request's rules
apply on top either way.

Entries are surface words. At expansion time the coordinator analyzes
each entry under the field's analysis spec — the same chain the query
text goes through, stemmer included — so a rule written as `automobile`
matches the stem `automobil` the dictionary holds. The analyzed forms of
the table are cached per spec. A query term matches an entry that
analyzed to exactly that one term; the rule's other entries (or its `to`
entries) contribute every term they analyzed to, so a phrase entry adds
each of its terms. An added term joins the query once and is **scored as
the ordinary term it is**: BM25 sums the contributions of every term
under the global statistics of each, exactly as a prefix expansion does
(`docs/prefix-terms.md`). `car` with the rule above therefore scores
bitwise the same as the query `car automobile motor vehicle` typed out —
that identity is what `tests/synonyms.rs` pins — and the hit's occurrences
name the term that matched.

`Bm25SearchResponse.synonym_expansions` (and
`QueryResponse.synonym_expansions` on the lexical leaf) report one entry
per matched query term: the field, the analyzed term, and the analyzed
terms added. A rule with fewer than two entries, or a blank entry,
refuses by name before any shard is asked. A sorted lexical leaf computes
no relevance and refuses rules on the leaf, as it refuses every other
relevance shape.

What this does not do: no multi-term matching on the query side (a
phrase entry contributes expansions but is not matched as a phrase in the
query), no weights on expansions (an expansion is a term; weight it with
a `QueryField` if a field-level weight is what is meant), no expansion
inside a `PhraseMatch` window, no ingest-time synonyms.

## Did you mean

`SearchService.TermSuggest` analyzes `text` under the field's spec and
proposes, for each distinct term, dictionary terms within `max_edits`
edits — the optimal string alignment distance: an insertion, a deletion,
a substitution, or an adjacent transposition is one edit — that share the
term's first `prefix_length` characters, ranked by distance, then by df
summed over the shards, then by term bytes. Under the default mode
`MISSING` only a term no shard's dictionary holds gets candidates; under
`ALWAYS` every term does, its own entry excluded. Each term reports its
own summed df and how many dictionary terms the scan read.

The scan is the bounded prefix scan `Suggest` runs (`docs/suggest.md`):
one `SuggestTerms` per term per shard under the term's prefix, the
shards' entries unioned by term with df summed, and the same `max_scan`
contract — past the bound on any shard, or fleet-wide, the request
refuses naming the count and the lever (`prefix_length`), never a quieter
answer. `prefix_length` is that lever: 1 by default (a one-character
prefix is a wide scan on a large corpus; 2 is a fair default there), and
a term shorter than it gets no candidates. `max_edits` is 1 by default
and at most 2; `limit` is 5 per term and at most 100. Two shards equal one
shard bitwise, as for `Suggest`, and `tests/synonyms.rs` holds the fleet
to a brute-force ranked scan of the analyzed corpus.

A stemmed field proposes stems, for the reason `docs/suggest.md` gives:
the dictionary holds stems, and the engine will not guess the surface
form. df is posting df, so `df_includes_tombstoned_rows` says when a
tombstoned row still counts.

Cost: no new section, column, or payload; one bounded dictionary scan per
term per shard, one analysis call for the text, and an edit-distance
computation per scanned entry (cut off at the bound). Metrics count the
route as `term_suggest`.
