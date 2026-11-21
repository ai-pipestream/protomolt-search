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
- Absence = the key missing from the document's pair list. A present empty
  string is a value, including when protobuf omits the default `value` field
  inside a present `MapFacetEntry`. Equality, ordering, projected values and
  counts retain it. No NaN sentinel exists in map pairs; non-finite numeric
  values are refused at ingest.
- Empty keys are literal keys in ordinary `AddDocuments` map entries.
  Optional `map_key` fields on string predicates, score stages and range
  facets, together with `MapRead`, distinguish empty map keys from plain
  columns.
- **Counting**: `MapFacetField { column, key }` entries ride the same
  count-then-rank pass as plain facets — resolve (column, key) to a
  shard-local key ordinal once, then one binary search per matched
  document. Counts are additive; roots and relays validate each response's
  column and key before merging, then sort count-desc, value-asc. Map entries
  follow plain entries in `facets`, with `FacetFieldCounts.map_key` present
  even for an empty key.
- **Scoring**: `ScoreStage.map_key` selects a map-numeric entry, including an
  empty key; bounds use that key's min/max. Omitting `map_key` selects a plain
  column. See [score input](score-functions.md).
- **The typo rule goes down to the key.** A shard lacking the column or
  the key answers known=false and contributes nothing (its documents
  genuinely hold no entries — exact, not degraded). A (column, key)
  pair NO shard knows is refused naming `column[key]` and the knob — a
  typo'd drill-down or a typo'd chain must never read as "zero results"
  or a silent no-op.

## Protobuf preservation and projection

The column API and descriptor-driven extraction are separate. The ordinary
`AddDocuments` API accepts explicit `MapFacetEntry` and `MapNumericEntry`
records. It does not infer their contents from an attached original source.
Unhinted maps retain their source-only inference. Explicit map KEYWORD/BOOLEAN
and FLOAT/DOUBLE projections now bind to the matching map column families; see
[protobuf map projection](map-projection.md). Original protobuf bytes preserve
every wire entry, including duplicates and empty keys and values.
Describing or retaining a source is not proof of a queryable map projection.

Current scalar storage has full-domain signed and unsigned integer columns
with independent presence. Map-numeric storage is f64, so it is not an exact
replacement for protobuf int64/uint64 maps. Map-facet values are strings with
entry presence; a missing key and a present empty string are distinct. The
current [schema report](schema-report.md) and [index contract](index-definition.md)
are the entry points for what descriptor-based planning actually supports.

### Empty string values (2026-09-06)

Ingestion previously rejected a present empty map value and told the caller to
omit it, which changed its meaning to absence. Both existing storage encodings
already distinguish those cases. The rejection is removed without changing the
file format or any protobuf wire declaration. Older ingestion services can
still refuse these values; existing image readers can retain them.

`tests/map_value_presence.rs` checks heap/spill byte equality, heap and mapped
readers, and both sides of the public gRPC boundary. It distinguishes present
empty, present nonempty and absent entries under equality, membership, ordering,
prefixes, projections and facet counts. One- and two-shard runs cover both
storage layouts, restart and compaction that moves rows. Existing duplicate-key,
unknown-column and non-finite numeric refusals remain in force.

Combined validation on main `483be73` plus this change passed 507 library tests,
719 integration tests across 123 targets, 12 embedded tests and two IVF tests
(1,240 total; one existing live OpenNLP test ignored). All five mobile Rust
checks, test/example compilation, formatting and vendored-proto checks passed.
Descriptor comparison confirms byte-identical protobuf wire declarations to
`483be73`. This is local validation; no fleet rollout was performed.

### Explicit map string selectors (2026-09-06)

`FilterExpr.string_range` (tag 11) and `FilterExpr.string_prefix` (tag 12) carry
an optional `map_key` on `StringRangePredicate` (tag 5) and
`StringPrefixPredicate` (tag 4). An absent `map_key` selects a plain facet
column; a present `map_key`, including the empty string, selects that literal
key in the map dictionary. CEL emits `map_key` for map ordering and prefixes
such as `meta[''] >= 'm'` and `meta[''].startsWith('m')`. Evaluation reads the
map column, and placement cannot exclude or remove these leaves using
same-named scalar bounds. Leaf order for knowledge checks and field-use
authorization covers both plain and map forms.

Tests cover heap and mapped dictionaries, absent keys and empty values, Kleene
negation, direct and two-level relay queries over gRPC, placement evaluation and
pruning, and field-use denial before statistics.

### Explicit map score input (2026-09-06)

`ScoreStage.map_key` (optional string, tag 11) preserves a literal map key,
including empty. Scoring, bounds, column knowledge, fetched signals and
explanations carry that distinction through relays.
[Score input](score-functions.md#explicit-map-input-2026-09-06) describes the
contract. The count contract below enables empty-key ingestion through the
ordinary column API.

### Explicit map counts and ingestion (2026-09-06)

Ordinary `AddDocuments` accepts empty map keys for strings and finite
numbers. A present empty string or numeric zero remains distinct from an
omitted entry. Duplicate keys, unknown columns and non-finite values still
refuse before mutation. The [descriptor projection](map-projection.md)
increment connects supported protobuf map values to these columns.

`RangeFacetField` carries `optional string map_key` (tag 7) and
`repeated FilterBound edges` (tag 8), selecting a literal map key (including
empty) when `map_key` is present and a plain column when `map_key` is absent.
The same half-open, exact typed-edge rules apply.
See [range facets](range-facets.md#explicit-map-input-2026-09-06).

`FacetFieldCounts.map_key` and `RangeFacetCounts.map_key` retain map context
with protobuf presence. Nodes echo them on known and unknown map responses;
plain responses omit them. Roots and relays reject a different column/key,
missing context for map counts, unknown responses carrying counts, and
count overflow. Range merges also validate every interval edge before adding
counts.

`tests/map_count_presence.rs` covers typed intervals and boundaries, forged
response identity and overflow.
`tests/map_value_presence.rs` covers named and empty keys, missing entries,
string and numeric projections, facet and range counts, and Aggregate count,
sum and mean through direct and two-level relay gRPC queries. It exercises
both storage layouts, explicit WAL rebuild, image reopen, and compaction that
renumbers rows. Field-grant tests deny map count requests before statistics.
The WAL check uses the explicit reshard replay path after the flush persistence
barrier; the rebuild reads log records independently of the source images.
Automatic WAL replay on node open is not implemented: an interrupted bulk
build refuses to open. This remains a durability/recovery integration gap.
Storage encodings are unchanged.

**Simultaneous rollout requirement.** Coordinators, relays, nodes and
regenerated gRPC/JSON clients must switch to the single-form `search.proto`
schema together. `StringRangePredicate` and `StringPrefixPredicate` carry map
context on `optional string map_key` (tags `5` and `4`, with retired tag `2`
reserved), and lexical stream completion (`Bm25StreamCompletion`, with
`Bm25QueryStreamResponse` tag `2` reserved) plus `FetchValuesResponse` map-type
validation (tag `11` reserved) assume matching builds; mixed-fleet peers would
silently misread a map-context string range or prefix predicate as a plain
facet-column predicate without an error. Under proto3 JSON (`WIRE_JSON`),
`FilterExpr.mapNumber`/`mapHasKey` and `ValueExpr.map` bind to the unified
all-plane map variants (tags `15`, `16`, and `16`).

### Remaining map work

Scalar [map projection](map-projection.md) now applies protobuf defaults and
decoder-defined duplicate-key handling before emitting unique entries. Exact
numeric integer map storage and message-valued map projection remain unfinished.
Original bytes remain independent of selected projections. Direct `AddDocuments`
entries require unique keys because they are already materialized values.

The legacy `stats_fields` and `cardinality_fields` request lists name plain
columns. They are not CEL expressions or map selectors. Map statistics use
`Aggregate` with an explicit `MapRead` expression, such as `metrics['']`.
Message-valued map projection and exact numeric integer map storage remain
part of the search foundation goal.

## Original sequencing notes (2026-08-03)

- **Increment 2** — LANDED 2026-08-03 (`docs/range-facets.md`): i64
  columns as kind 4 (exact past 2^53, `i64::MIN` the refused absence
  sentinel; Timestamp is ingest sugar converting to epoch micros on
  the node), and range facets with explicit bucket edges over
  f64/i64/map values, sharing the count-then-rank bitmap with the two
  facet kinds above. Half-open buckets, no implicit tails, edges
  validated rather than repaired. Range FILTERS arrived with CEL
  (`docs/cel-filters.md`);
  if selective pure-range filters matter, the 1-D analog of Lucene's
  trie/BKD trick is a static value-sorted (value, doc) section per
  column — shards are immutable per generation, so a plain sorted
  array does what tries do.
- **Increment 3**: geo columns (lat/lon points; bbox = two range
  predicates, haversine radius and Manhattan distance as filters and
  as monotone-decay score stages). Road-network semantics (travel
  time/energy) stay in an enrichment sidecar (routee-compass, BSD-3,
  in reference-code) — routing is not indexing.
- **CEL filters — arrived** (`docs/cel-filters.md`): map access
  compiles natively (`meta["color"] == "red"`, `"color" in meta`) to
  (key_ord, value-ordinal set) predicates — the map design was shaped
  so that layer needed nothing new, and it didn't. (`has()` stays a
  column-level test; KEY presence is `"k" in m`, CEL's own idiom.)
