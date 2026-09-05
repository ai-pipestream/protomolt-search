# Facets: dictionary-encoded columns and count-then-rank

Landed 2026-08-03 (track 1, `plans/track-1-features.md` section 2).
"How many results per court, per year, per opinion type" — exact counts
over the full match set, priced and measured before adoption.

## The two decisions

**Count-then-rank, not approximate.** Facet counts are computed over
every document matching at least one query term, independent of `k`,
`min_score`, and block-max pruning — those bound what is SURFACED,
never what matched. The alternative (count only what survives the
floor) is cheap and wrong in a way users notice on broad queries, and
this engine's argument is exactness. The cost is a full doc-run walk
per scored term, paid only by queries that ask for facets
(`facet_fields` empty = no counting, no cost).

**Priced before committing** (`examples/facet_walk_probe.rs`, release
build, mmap reader, page-cache warm): the union walk runs at ~1.2
ns/posting and the ordinal counting at ~2 ns/matched-doc. Scaled to a
production shard (~10.8M docs), a worst-case stopword-heavy query
prices at roughly 15–35 ms per shard, in parallel across the fleet.
The plan's own threshold was "if a full BM25 walk is tens of
milliseconds the argument is over" — it is.

## Storage: the v7 format (`TVBM2507`)

The track-1 plan assumed "the v6 section table has room for new section
types". That premise was WRONG: v6 has no typed section table — every
section is located by a positional header slot, and
`validate_structure_v6` pins an exact contiguous tiling. Adding a
column is therefore a format change, and per the standing policy
(rebuild-not-migrate, no external clients) it is a new magic, not a
migration:

```text
magic "TVBM2507"
u32 n_fields | u32 n_slots
u64 texts_off | u64 text_index_off | u64 lineages_off
field table, n_fields entries          <- v6 bytes, unchanged
u32 n_columns
column table, n_columns entries:
  u16 name_len | name bytes | u8 kind
  kind 0 (facet): u32 n_values | u64 dict_off | u64 ords_off
  kind 1 (f64):   u64 min_bits | u64 max_bits | u64 vals_off
texts | text_index | lineages          <- v6 bytes, unchanged
per field: doc_lengths | postings | directory   <- v6 bytes, unchanged
per kind-0 column: dict | ords
per kind-1 column: vals (n_slots x f64, NaN = absent)
```

(REVISED 2026-08-03, same day and before any v7 file existed outside
test artifacts: the facet-only table became a kinded column table when
numeric columns landed for score functions — `docs/score-functions.md`.
An unknown kind refuses at open by number, so the next column kind
needs no new magic — proven the same day when kinds 2 and 3 (map
columns, `docs/map-columns.md`) joined without touching kinds 0/1.
Facet semantics below are unchanged.)

- `dict`: the distinct values in ordinal (first-seen) order, `u16 len |
  bytes` each. Decoded eagerly at open (one entry per distinct value —
  court is ~2,000, year ~250).
- `ords`: `n_slots x u32`, one ordinal per document slot;
  `u32::MAX` (`FACET_ABSENT`) = the document has no value. Fixed
  stride, stays in the map; a facet lookup is one 4 B read.

**The format break is opt-in per shard.** A store with no declared
facet fields still writes v6, byte-identical to every pre-facet build
(the writers gate the additions rather than forking, and the dual-writer
byte-identity test pins both cases). v6/v5/v4/v3 files keep opening
exactly as before; a v7 file with zero facet fields is refused as
corruption. Existing shards and the track-2 rebuild are untouched until
a shard declares `--facet-fields`.

Facet values are opaque identifiers: never analyzed, counted exactly as
ingested. Cardinality is bounded by what ingest sends; the columns are
built for enumerable fields (court, year, type), not per-document keys.

## Ingest and durability

`AddDocumentsRequest.facets` carries `(field, value)` pairs. The WAL
persists the request verbatim, so facet values ride the same
durable-record lever as multi-field: old logs replay facet-less,
reshard replay re-applies values and derives the child's facet table
from the records themselves. Validation refuses unknown facet fields
(typo protection, `--facet-fields` is the schema), repeated fields, and
empty values — before anything mutates.

## Wire and merge

`Bm25QueryRequest.facet_fields` asks a shard to count;
`Bm25QueryResponse.facets` answers per-field `(value, count)` lists
plus a `known` flag. Counts are additive — no node's count depends on
another's, so there is NO analog of the global-df trap — and the
coordinator's merge is the plain per-value sum, sorted count-descending
(ties by value ascending). The known-flag rules mirror multi-field
scoring exactly: a shard without the field contributes nothing
(legitimate heterogeneous fleet; its documents hold no values), but a
field NO shard knows is refused — zeros everywhere would make a typo
read as "no results per anything".

`Bm25SearchRequest.facet_fields` / `Bm25SearchResponse.facets` expose
the same on the public route, flat and fused alike (a fused query's
match set is the union over every leg's terms). Facet counting rides
the scoring RPC, so the stats-epoch refusal-retry covers it with no
extra machinery.

Hybrid queries carry their counts through the public Query route:
`QueryRequest.aggregate` (`docs/aggregations.md`, "Aggregating a
query's pool") folds group-by counts, stats, histograms, and
cardinality over the candidate pool a hybrid page was drawn from, and
over a browse's exact filter match set. The vector leg matched the
corpus, so the honest scope of a hybrid count is the pool the page
ranked, and that is the scope the fold reports (`matched` is its
size). The legacy `HybridSearchRequest` carries no facets.

## Aggregations beyond counting (2026-08-24)

`Bm25SearchRequest.stats_fields` aggregates numeric and integer columns
over the FILTERED match set: per column, the count of documents holding
a value, min, max, sum, and (computed at the coordinator, so clients
cannot get it wrong) mean = sum / count. All of it rides the same one
bitmap every facet kind shares — the traversal is the expensive half
and a second aggregate must not pay for it twice — and merges the way
counts do: counts and sums add, mins and maxes fold, no shard's answer
depends on another's. Absence contributes nothing: `count` is documents
that HELD a value, which is what keeps the mean honest, and a
zero-count column reports min = max = 0 with the count saying so.

`Bm25SearchRequest.cardinality_fields` counts DISTINCT facet values
over the match set — exactly, not estimated. Ordinals are shard-local,
so value strings are the only union-able currency: each shard reports
the values present in its match set and the coordinator unions them.
The cost is those strings on the wire, proportional to per-shard
distinct counts, and it is the caller's explicit choice per field —
the same made-visible trade count-then-rank settled.

Both take the flat single-field route only (the fused route refuses
them by name, like score stages), and both refuse a column no shard
declares, naming the column and the knob. `tests/aggregations.rs`
holds all of it, heterogeneous fleet included.

## What this deliberately does not do

- No facet FILTERING yet ("court=scotus" narrowing the result set) —
  that must apply before the floor check to keep pruning sound, and it
  ships with the public-API filter syntax (plan section 5).
- No numeric/range facets — the column storage is the shared mechanism
  (plan section 3, functions on columns); a typed column kind slots
  into the same facet-table shape when that lands.
- No per-shard count caps or truncation: every non-zero value crosses
  the wire. Bounded by dictionary cardinality, which is
  operator-declared; if a huge-cardinality facet ever ships, cap it
  loudly, never silently.
