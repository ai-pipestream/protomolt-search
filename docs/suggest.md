# Autocomplete over the sorted dictionary

Landed 2026-09-04 (five-item build, L11). `SearchService.Suggest` completes a
prefix against one indexed BM25 field's term dictionary, ranked by document
frequency summed over the collection's shards. It is the suggester Lucene and
OpenSearch ship and no vector database does, and it costs nothing new on
disk: every dictionary in a `.bm25` file is already in byte order
(`docs/prefix-terms.md`), and the df it ranks by is the number the directory
entry already holds.

## Contract

```
Suggest(SuggestRequest{collection, field, prefix, limit, max_scan, analysis})
  -> SuggestResponse{suggestions: [{term, df, shards}],
                     dictionary_terms_with_prefix,
                     df_includes_tombstoned_rows}
```

- `field` names any indexed BM25 field: the body, a per-field column, the
  dual-cased twin (`docs/dual-cased.md`), the glossary phrase field
  (`docs/phrase-search.md`). A field no shard indexes is `INVALID_ARGUMENT`
  naming it; an empty field name refuses too (there is no default field to
  guess).
- `prefix` is non-empty, in the field's surface form; see normalization.
- `limit`: how many suggestions come back. 0 selects 10; above 100 refuses
  naming the maximum. The request is never clamped.
- `max_scan`: how many dictionary terms under the prefix the request may
  scan, per shard and fleet-wide. 0 selects 100,000; above 1,000,000
  refuses. A prefix with more matches than the bound refuses naming the
  count (see the cap).
- `analysis`: the field's `AnalysisSpec`. Required, for the same reason a
  `TermPrefix` requires it: the sidecar's default chain is not known to the
  coordinator, and a prefix normalized under the wrong chain completes to
  the wrong terms.
- `collection` follows `docs/collections.md`: empty names the unnamed
  dataset or the configured default; an unknown name, or no name on a named
  set without a default, refuses by name. Bearer principals gate the route
  like every other public route (`docs/security.md`); `limit` is capped
  absolutely, so a principal's `max_k` does not apply, while its concurrency
  quota does.

Each suggestion carries the dictionary term, its df summed over the shards
that hold it, and how many shards those were. `dictionary_terms_with_prefix`
is the size of the fleet-wide union under the prefix whether or not every
term was returned, so a client can tell "ten of twelve" from "ten of ten
thousand".

## Normalization

The coordinator normalizes the prefix exactly as it normalizes a
`TermPrefix`: the field's char filters — invisible stripping, whitespace,
accent folding, case folding, whichever the spec lists — and never its
stemmer. `Cour`, `COUR`, and `coür` all complete as `cour` on the folded
body. `SOURCE_STEMS` ignores char filters at ingest, so on the cased twin
the prefix is compared as written: `Cou` completes to `Court`, `cou` to
`court`, `COU` to `COURT`.

The stemmer is never applied because the dictionary holds stems and a
prefix of a stem is what the caller typed. The consequence is worth
stating plainly: **a stemmed field suggests stems.** On the body,
`courtes` completes to `courtesi`, and `courtesy` — the surface word, which
Porter would have stemmed to `courtesi` — completes to nothing, because no
dictionary term starts with `courtesy`. Surface-form completion needs a
field indexed without a stemmer: the cased twin (whose stems keep case but
are still stems), a `SOURCE_TOKENS` field, or a per-field column declared
for the purpose. The engine does not re-stem the prefix or un-stem the
dictionary; either would be a guess about the analyzer.

## Ranking

Every shard answers `NodeService.SuggestTerms` from its own dictionary: a
binary search to the prefix's lower bound, then a scan while the prefix
holds, returning every matching term with its posting df. The heap builder
walks its ordered map and reads each posting list's length; the file reader
reads the df field of each directory entry; a segmented shard sums the same
term across its sealed parts and its tail. The coordinator unions the
shards' entries by term, summing df, and orders by df descending, then term
bytes ascending.

That union is exactly the dictionary one image of the same rows would hold,
and the summed df is exactly that image's posting df, so two shards equal
one shard bitwise (df, count, order); the `shards` tally is the only field
that depends on layout. `tests/suggest.rs` pins the fleet against a
monolithic node and both against a brute-force ranked scan of the analyzed
corpus, and a two-segment-plus-tail shard against a single image before and
after an mmap reopen.

## The cap

`max_scan` is the cost contract. A shard whose count under the prefix
exceeds it reports the count and no entries; the coordinator refuses
`INVALID_ARGUMENT` naming the count, the shard, and the bound. A union that
exceeds it while every shard was within it refuses the same way naming the
fleet count — the count the monolith would have refused with. The engine
never truncates a suggestion list to a quieter match set, never
linear-scans a dictionary to serve one, and never returns "the top ten of
the first thousand it happened to read". Lengthen the prefix or raise the
bound.

Past the bound nothing is materialized: the file reader compares directory
entries in place without copying a term out, the heap store counts map
entries without cloning them, and the wire carries the count alone. The
cost gate in `tests/suggest.rs` holds a 5,000-term dictionary to that.

## The tombstone flag

df is **posting df**: the length of the term's posting list as the shard
stores it. A deleted document is a tombstone in the live-docs bitmap
(`docs/mutations.md`), not a posting removed, so its terms keep counting
until compaction rewrites the segment. Search results never include it; the
suggestion's df still does.

The alternative — walking every posting list under the prefix and masking
each document against the bitmap — costs one posting walk per candidate
term, which for a wide prefix is the whole inverted index for that
neighbourhood, per keystroke. That is the wrong trade for an autocomplete
box. Instead the response says what it did: every `SuggestTerms` answer
carries the shard's tombstone count, and `df_includes_tombstoned_rows` is
true when any shard reported one. A client that needs exact live df for a
handful of terms has `TermStats`, which does mask; a fleet that wants the
flag off compacts.

`SearchService.TermSuggest` (`docs/synonyms.md`, "Did you mean") runs
the same scan under a term's leading characters and ranks the entries
within an edit bound of the term.

## Costs

- Disk and RAM: nothing. No new section, no new column, no new posting
  payload. The dictionary was already byte-sorted and the df was already in
  the directory entry.
- Per request: one RPC per shard, `O(log n)` directory reads to the lower
  bound plus one per matching term, and `O(m log m)` on the coordinator to
  rank `m` union terms. No sidecar call on the query path — normalization is
  the native char-filter chain the coordinator already runs for prefix
  terms.
- Wire: every matching term crosses from every shard that holds it, up to
  `max_scan` per shard. The default bound (100,000) is generous for a
  dictionary; a one-letter prefix on a large corpus is where it bites, and
  the refusal names the number.

## Metrics

`turbovec_requests_total{rpc="suggest_terms"}` on the node and
`{rpc="suggest"}` on the coordinator (`docs/metrics.md`).

## Tests

`tests/suggest.rs`: ranked brute-force exactness on heap shards with two
shards against one; the limit, its default, and the refusal above the
maximum; the refusal table (per-shard and fleet-wide bound with counts, the
absolute maximum, empty prefix, a prefix that normalizes to nothing, unknown
and empty field, unknown collection, absent spec); case and accent folding
and the stemmer not applied; the cased twin and the glossary phrase field;
a segmented shard against a single image and both reopened from disk
through the mmap reader; the tombstone flag before and after a delete with
df unchanged; two collections with different df for the same term and the
unnamed-request refusal on a named set; the bearer gate; and the cost gate.
`src/postings.rs` pins the heap store and the file reader to one
`(term, df)` table and the bound refusal at unit level.
