# Map columns: first-class map types in the index

Landed 2026-08-03 (track 1). `map<string, string>` and
`map<string, f64>` as first-class column kinds — the thing Lucene
cannot do, and the reason it cannot is the design lesson this document
starts from.

## Why Lucene can't, structurally

Lucene's document model is the flat field: to index a map, every
distinct key must become a dynamic field NAME, and each field name
mints its own term dictionary, field infos, and docvalues. **Data
cardinality becomes schema cardinality** — the Elasticsearch "mapping
explosion" failure — and the official patch (`flattened`) gives up
typed values to contain it. The fix here inverts the relationship: a
map is ONE column whose KEY SPACE is dictionary-encoded, exactly like
its values. A million distinct keys cost a million dictionary entries
in one column, not a million schemas.

## Prior art: what the tantivy JSON field taught us

Before building, we deep-read tantivy's JSON field (MIT; the one real,
battle-tested implementation of typed dynamic-path indexing —
`/work/reference-code/tantivy`, v0.26). What we adopted:

- **Two-phase dictionary interning** (unordered id during build,
  ordered at flush) is proven at scale; our intern-at-ingest with
  first-seen ordinals is the same family, and per-key/per-value
  dictionaries per column mirror their per-(path, type) columns.
- **Dictionary-encoded string ranges resolve to ordinal ranges** —
  kept in the back pocket for CEL string-range predicates.
- **Cardinality auto-detection with a zero-cost dense case** — future
  work for our offsets sections (see costs, below).

What we deliberately did differently, each answering one of their
documented regrets:

- **Structured wire, no separators.** Tantivy encodes paths as
  `[path][0x00][type_tag][value]` byte strings; the `0x00` needed three
  layers of defense after a production panic, and the `0x01` segment
  separator is UNESCAPABLE to this day — a key containing it silently
  aliases a nested path. Our wire carries (column, key) as separate
  proto fields; there is no separator to collide with, no escaping
  rule, no `expand_dots` ambiguity (their own comments call that
  feature a mistake).
- **One statically typed column per name.** Tantivy's schemaless
  typing puts ~90 lines of saturating bound arithmetic in every range
  query because a path's column type is data-dependent, and its term
  dictionary and columnar sides disagree on types by construction. Our
  map columns are declared (`--map-facet-fields` / `--map-numeric-fields`)
  and typed; a value of the wrong type is refused at ingest, not
  coerced at query time.
- **Per-key statistics are first-class.** Tantivy cannot score by a
  JSON numeric (fieldnorms are structurally impossible for JSON fields;
  per-path stats need a full term-dictionary scan — their comment says
  "very expensive"). Our map-numeric key dictionary carries per-key
  min/max, so map-keyed score stages lift bounds exactly like plain
  columns.

## Storage: kinds 2 and 3 in the v7 column table

```text
kind 2 (map<string,string>) table entry:
  u16 name_len | name | u8 kind=2 | u32 n_keys | u32 n_values
  u64 keys_off | u64 values_off | u64 offsets_off | u64 pairs_off
sections: keys dict | values dict
          offsets ((n_slots + 1) x u32 prefix sums)
          pairs (total x (u32 key_ord | u32 value_ord))

kind 3 (map<string,f64>) table entry:
  u16 name_len | name | u8 kind=3 | u32 n_keys
  u64 keys_off | u64 offsets_off | u64 pairs_off
sections: keys dict WITH per-key metadata
          (u16 len | key | u64 min_bits | u64 max_bits)
          offsets | pairs (total x (u32 key_ord | f64 bits))
```

Pair lists are strictly key-ordered per document (validated at open,
like everything else: dict walks, offset monotonicity, ordinal ranges,
finiteness, and per-key min/max agreement against a full scan). A
document's lookup is two offset reads plus a binary search of ITS pair
list — O(log entries-per-doc), independent of corpus key cardinality.
At most one value per (document, key): map semantics, enforced at
ingest ("repeats in one document" is a refusal, not a last-write-wins).

Cost note: each map column pays `4 x (n_slots + 1)` bytes of offsets
(~43 MB on a 10.8M-doc shard) even when sparse. Tantivy's rank/select
optional index is the known optimization; a sparse-offsets kind can
join the table later without a format break — unknown kinds refuse by
number, which is the whole point of the kinded table.

## Semantics

- Keys and values are opaque, exact, never analyzed. Full-text lives in
  named BM25 fields; maps are the metadata plane. (Key-scoped term
  positions inside map values are possible later — tantivy's
  per-path-id position offsets are the design to copy — but blurring
  term identity now would compromise everything built on it.)
- Absence = the key simply missing from the document's pair list. No
  NaN sentinel in map pairs; non-finite numeric values are refused at
  ingest. Empty keys are refused (almost always a producer bug).
- **Counting**: `MapFacetField { column, key }` entries ride the same
  count-then-rank pass as plain facets — resolve (column, key) to a
  shard-local key ordinal once, then one binary search per matched
  document. Counts are additive; the coordinator merges positionally
  and sorts count-desc, value-asc. Answered in `facets` after the plain
  entries with `FacetFieldCounts.key` set.
- **Scoring**: `ScoreStage.key` selects a map-numeric entry; bounds
  lift from THAT KEY's min/max. All score-function contracts unchanged.
- **The typo rule goes down to the key.** A shard lacking the column or
  the key answers known=false and contributes nothing (its documents
  genuinely hold no entries — exact, not degraded). A (column, key)
  pair NO shard knows is refused naming `column[key]` and the knob — a
  typo'd drill-down or a typo'd chain must never read as "zero results"
  or a silent no-op.

## The protobuf story

With maps, the column plane covers most of the proto data model:

| proto type | lands as |
|---|---|
| string / enum | facet column (kind 0) |
| double/float/ints (< 2^53) | f64 column (kind 1) |
| int64/uint64 exact, Timestamp | i64 column (kind 4, `range-facets.md`) |
| map<string, string> | kind 2 |
| map<string, numeric> | kind 3 |
| nested message paths | dotted column names (declared) |
| repeated scalars | list column (queued; same pair-list encoding) |
| bytes, Any, repeated-inside-repeated | out of scope, refused loudly |

A descriptor-driven extractor ("FileDescriptorSet + message name →
column schema") is the consumer-side one-liner that makes "index any
protobuf" real; the engine's wire stays explicit typed values.

## Queued behind this

- **Increment 2** — LANDED 2026-08-03 (`docs/range-facets.md`): i64
  columns as kind 4 (exact past 2^53, `i64::MIN` the refused absence
  sentinel; Timestamp is ingest sugar converting to epoch micros on
  the node), and range facets with explicit bucket edges over
  f64/i64/map values, sharing the count-then-rank bitmap with the two
  facet kinds above. Half-open buckets, no implicit tails, edges
  validated rather than repaired. Range FILTERS still arrive with CEL;
  if selective pure-range filters matter, the 1-D analog of Lucene's
  trie/BKD trick is a static value-sorted (value, doc) section per
  column — shards are immutable per generation, so a plain sorted
  array does what tries do.
- **Increment 3**: geo columns (lat/lon points; bbox = two range
  predicates, haversine radius and Manhattan distance as filters and
  as monotone-decay score stages). Road-network semantics (travel
  time/energy) stay in an enrichment sidecar (routee-compass, BSD-3,
  in reference-code) — routing is not indexing.
- **CEL filters** compile map access natively (`meta["color"] ==
  "red"`, `has(meta.color)`) to (key_ord, value-ordinal set) predicates
  — the map design was shaped so that layer needs nothing new.
