# Vector-leg filters: one allowlist, both legs

Status: implemented. The lexical leg has filtered since the CEL
increment (`docs/cel-filters.md`); the vector leg could not, so the
hybrid route refused filters outright rather than filter one half and
misdescribe the result set. This increment gives the vector scan the
same predicate, resolved from the same tree, and removes that refusal.

What it unlocks, in one sentence each: `SearchRequest` and
`HybridSearchRequest` now carry `geo_filters` and a CEL `filter`; every
fusion mode filters both legs; and the vector-plus-CEL shape the public
query contract lists as unsupported (`docs/query-api.md`) now has an
ordinary engine path underneath it.

## The mechanism is an allowlist, not a post-filter

A shard resolves the request's filters ONCE against its own tables,
producing the same [`DocFilter`] the lexical heap gate uses, then
evaluates it over every slot into a `Vec<bool>`: `allow[slot]` is
whether that document survives. The vector scan takes it as
turbovec's `SearchOptions::mask`, so the kernel never scores a removed
slot.

The slot-to-document identity that makes this legal is the one the
engine already relies on everywhere: a shard's vector slot and its
BM25 local doc id are the same number (`global id = slot_offset +
slot`), which is why `VectorRescore` can route global candidate ids
into mask positions and why hybrid fusion can rank one id space with
two legs. Documents are ingested before vectors so the two stay
aligned.

Filtering as a mask rather than as a post-filter over emitted
candidates is the difference between the floors meaning the right
thing and merely being safe. Both are exact — a filter only removes
documents, so a floor computed over the survivors is a lower bound on
the same k-th best either way — but a mask makes the shard's published
k-th best the FILTERED k-th best, which is higher, so floors rise
faster and every other shard prunes harder. Post-filtering would leave
each shard pruning against an unfiltered boundary and pay the full
scan on a query the filter made small.

## Why no pruning math changes

The same argument the geo increment used, unchanged: a filter only
REMOVES documents. Every score bound that dominated the full corpus
still dominates a subset of it, so block-max bounds, MaxScore, the
shared floor protocol, the decomposed floor algebra, and the
completion certificates all stay valid with nothing rewritten. The
discipline is that the test gates ADMISSION only — it decides what
enters a heap, never what a cursor skips or how a bound is computed.

Two consequences worth naming:

- **A skipped chunk is a saving, not an approximation.** When no slot
  in a chunk survives the filters, the scan makes no kernel call at
  all. That is where a selective filter pays for itself, above the
  kernel's own all-masked block short-circuit.
- **The completion certificate still means what it meant.** A
  streaming node certifies that every live slot at or above the floor
  was emitted. With an allowlist "live" ranges over the survivors, so
  the certificate covers the filtered corpus exactly.

## Absence, typos, and the heterogeneous fleet

All inherited from the lexical leg, because it is the same resolved
predicate:

- A document with no value for a filtered column FAILS the filter.
  Absence is Unknown under the Kleene rules, and only True admits.
- A column this shard lacks resolves to the absent case for every one
  of its documents, which is exact — its documents genuinely hold no
  value.
- A column NO shard resolves is a typo and REFUSES by name. This is
  the sharpest case of the rule: a misspelled column would remove
  every document on every shard and return an empty result set that
  looks exactly like an honest "nothing matched". Every vector route
  carries the handshake flags that make the refusal possible —
  `SearchShardDone`, `StreamSearchSummary`, `HybridShardResponse`, and
  `ShardLegsResponse` each answer `geo_columns_known` and
  `filter_columns_known` positionally over `walk_leaves` order.
- A shard with no lexical half has no columns, so it admits nothing.
  A shard still bulk-building REFUSES rather than answering empty:
  "no columns yet" is a transient state, and reporting it as a result
  would be a silent lie.

## Batching: queries with different filters do not share a kernel call

The chunked scan coalesces concurrent queries into one pass over the
packed codes. Floors are safe to share (the kernel gets the batch
minimum, and each query re-applies its own before merging), but masks
are not. The kernel returns each query's top `chunk_k` over the MASKED
set; under a union mask, a query's whole chunk quota could be filled
by documents its own filter rejects while its real candidates sit
below the cut.

So queries are grouped by allowlist identity, and each group gets its
own kernel call with a mask that is exactly the allowlist of every
query in it. Unfiltered queries all share the `None` group, so a fleet
that sends no filters batches exactly as it did before this increment
existed — and a filterless request takes a path bit-identical to its
pre-filter form, which is why `None` and an all-true allowlist are
deliberately different things.

## Cost

Two costs, both visible and neither hidden behind a heuristic:

1. **Building the allowlist** is one filter evaluation per slot: array
   reads and integer compares, the same per-document work the facet
   walk already prices at a couple of nanoseconds.
2. **Masked scans take turbovec's serial path.** `serial_required`
   is true whenever a mask is present, so a mask forfeits the
   multi-query SIMD batch kernel. The chunked scan has ALWAYS passed a
   mask (its chunk range is an allowlist), so this increment adds no
   new forfeit on that path; the streaming route gains one.

A weak filter can therefore cost more than it saves, and a selective
one saves a great deal by skipping whole chunks. The engine does not
guess which: it applies what was asked for and reports what it did.
When the saving needs a number, turbovec's `mask-skip-counter` feature
exposes `blocks_skipped_by_mask()`; it is feature-gated because the
per-skip atomic sits in the masked hot loop.

On a segmented shard a third saving comes first: a sealed segment whose
column summary cannot meet the filter's range predicates is ruled out
before the allowlist is built, its slots are `false` without a per-row
evaluation, and its vector image is never opened
(`docs/segment-pruning.md`). `ShardScanStats.segments_total` and
`segments_skipped` report it per shard.

## Routes

| Route | Filters |
|---|---|
| `Search` (bidi `SearchShard`) | `geo_filters` + `filter` |
| `Search` (streaming `StreamSearch`) | same |
| `Search` collapse-by-parent, both coordinators | same; a parent is represented by its best SURVIVING chunk |
| `HybridSearch` GLOBAL_RANK / SCORE_BLEND (`ShardLegs`) | both legs |
| `HybridSearch` TWO_LEVEL (`HybridShard`) | both legs |
| `HybridSearch` DECOMPOSED (lexical leg + vector stream) | both legs |
| `HybridSearch` CASCADE | phase 1 filters; phase 2 reranks that pool and never widens it |
| `Bm25Search` flat and fused | unchanged (`docs/cel-filters.md`) |

`VariantSearch` carries whole `Bm25SearchRequest`s and
`HybridSearchRequest`s, so A/B over filters — including
filtered-versus-not on the hybrid route — works with no new machinery.

The debug console no longer refuses a filter combined with the vector
leg. With the vector leg on, a filter rides the hybrid route; with it
off, the lexical route still wins because it also carries facet counts.

## Tests

- `src/chunked.rs`: the exactness oracle at scan level — a filtered
  scan equals the unfiltered scan narrowed afterwards, for every
  chunking and three selectivities; an all-true allowlist changes
  nothing; an empty one makes zero kernel calls; queries with
  different allowlists batch without disturbing each other's results;
  collapse under an allowlist collapses the survivors.
- `tests/vector_filters.rs`: the same oracle through the public RPC on
  both the bidi and streaming coordinators, including at a truncating
  `k` (the top-k of the survivors, not the survivors of the top-k);
  an empty result stays empty rather than widening; the typo refusal
  by name on the vector route; collapse mode; and every fusion mode
  admitting only documents that passed the filter.

## What this increment deliberately does not do

- **Hybrid facets.** Counts over the vector leg's match set are now
  well-DEFINED (the filtered set is a set), but counting them is its
  own increment.
- **Filter-only browse.** Both routes still require a query — terms or
  a vector. A match-all selection is public-API work.
- **Allowlist caching.** The mask is rebuilt per request per shard.
  Keyed on (filter, stats_epoch) it would be exactly invalidatable,
  the same discipline as the term-stats cache; that is a measurement
  away, not a design question.
