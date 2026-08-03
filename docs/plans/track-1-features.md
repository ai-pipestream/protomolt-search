# Track 1: search engine features

Written 2026-08-02. This is the feature track: what the engine grows next,
now that the format, the analyzer, and the distributed exactness work are
settled. It is written for whoever picks the track up cold. Companion
documents: `architecture.md` for how the system fits together,
`work-queue.md` for the queue this track was carved out of.

The theme of the track is that the engine is exact and fast but private.
Everything it can do today is expressed through an internal gRPC surface
shaped by the console and the ingest pipelines. The features below are
what turn it into something a product can sit on.

## 1. Layer separation

This comes first because every other feature lands somewhere, and today
there are only two somewheres: the index and the console. The system
wants four named layers with owned interfaces:

1. **Document store.** The full record: source text, metadata, lineage,
   and NLP annotations. Keyed on stable identity (source document plus
   chunk ordinal), never on index position. This is the repo service
   from `architecture.md` section 8.1, designed and not wired.
2. **Index.** What turbovec owns: postings, vectors, columns. Rebuildable
   from the store at any time, and treated as disposable. The rebuild
   habit we already have is this principle in practice.
3. **Analysis.** The NLP sidecar. Already separate, already versioned by
   fingerprint. The missing piece is that its richer output (entities,
   lemmas, PII) has nowhere durable to land until the store exists.
4. **Search.** Coordinator and nodes. Returns lean hits plus lineage
   keys; the store answers "give me the whole document."

The dependency to be honest about: facets, functions on columns, and the
public API all get easier if the layer boundaries exist first, but none
of them strictly require it. The store can lag. What cannot lag is the
rule that search hits carry stable identity, because every layer joins
on it.

TODO: decide whether the document store is a new service in this repo, a
schema in the existing Postgres, or an object layout on the NAS. The
open question from `architecture.md` still stands: a per-document store
answers "give me this document" well and "count entities by court and
year" badly, and the answer may be both a row store and the index's own
columns.

## 2. Facets and aggregations

The first user-visible feature. "How many results per court, per year,
per opinion type" is the standard shape of legal search, and nothing in
the engine computes it today.

The mechanics are friendly to our architecture. Facet counts are
additive, so each node counts over its own matches and the coordinator
sums. There is no analog of the global-df trap here: no node's count
depends on another's. The shared-floor stream needs one change of
contract, because facets are computed over the full match set while the
floor exists to avoid materializing the full match set.

Two designs to price:

1. **Count-then-rank.** Nodes run the match iterator to exhaustion,
   counting facet values as they go, and apply the floor only to what
   they surface as candidates. Costs full postings traversal per query;
   BM25 already walks most of it, the vector leg does not.
2. **Approximate facets.** Count only over documents that survive the
   floor, and label the counts as computed over the candidate pool.
   Cheap and wrong in a way users notice on broad queries.

The exactness stance of this engine argues for design 1, with the cost
made visible: a request flag chooses whether facets are wanted, and a
query that wants them pays the traversal.

LANDED 2026-08-03 (`docs/facets.md`): design 1, exactly as argued. The
traversal was priced first (`examples/facet_walk_probe.rs`): ~1.2
ns/posting for the union walk, ~2 ns/matched-doc for counting — a
worst-case stopword query on a 10.8M-doc shard is 15–35 ms, so the
argument was over. Two of this section's premises resolved differently
than written: (a) "the v6 section table has room for new section
types" was wrong — v6 locates sections by positional header slots and
validation pins an exact tiling, so facet columns are a new magic
(`TVBM2507`), opt-in per shard via `--facet-fields` (facet-less shards
still write byte-identical v6); (b) sidecar-vs-in-file was decided
in-file — the stale-sidecar trap from the v7 rebuild event is real and
recorded. Counts ride the Bm25Search route, flat and fused; hybrid
waits for filters (the vector leg matches everything, so "counts over
the matches" is ill-defined there). Facet FILTERING is deliberately
not in this cut; it lands with the public-API filter syntax and must
apply before the floor check.

## 3. Functions on columns, and the two-language split

Scoring today is BM25, cosine, or a fixed hybrid blend. The next step is
letting a query shape the score with column values: recency decay on
decision date, court-level boosts, page-rank style citation weight when
we have it.

DESIGN PINNED 2026-08-03 (`docs/score-functions.md`), superseding the
first draft of this section. The column features split into two
languages with a principled boundary:

- **CEL selects.** Filters ("court == 'scotus' && year >= '1990'") use
  CEL as the surface syntax, compiled PER SHARD into dictionary-resolved
  ordinal predicates — never interpreted per document. A filter only
  removes documents, so every block-max bound stays a valid upper bound
  for free; no new pruning math. Constructs that do not compile to
  dictionary predicates plus boolean algebra are refused by name, never
  interpreted slowly. (Not yet implemented; lands with the public-API
  filter syntax, and it is what makes hybrid facets well-defined.)
- **First-class function chains score.** The final score is a chain of
  named stages applied in request-list order to the BM25 score, on the
  node, before the floor test and heap insertion. Every stage signs one
  contract: monotone non-decreasing in the incoming score, with a
  computable upper bound given the column's min/max metadata. That
  single condition makes chaining sound: the chain's bound is the chain
  applied to the block-max bound, so MaxScore, the shared floors, and
  kth_best keep working on FINAL scores with no new theorems. List
  order is the pinned evaluation order (IEEE math is not associative),
  identical on every shard — distributed == monolith bitwise, and the
  A/B machinery (SearchVariant carries whole requests) compares
  chain-vs-no-chain for free. A stage that cannot state its bound does
  not ship.

Typed numeric columns (f64 values, NaN = absent, min/max in the column
table metadata) are the shared prerequisite: facets, CEL filters, and
score chains all read the same per-document columns — one mechanism,
three features. A document without a value passes through every stage
unchanged (identity), which is exact, not a degradation; a column NO
shard knows is refused, the same typo rule as fields and facets.

TODO: whether function parameters are per-request or registered named
profiles. Per-request is simpler and is enough for the console.

## 4. Caching

Nothing in the serving path caches today except the OS page cache, and
measurements say that is mostly right: the index is mmapped, the hot
postings stay resident, and the first-query-after-restart cost is cold
pages, not missing caches. Caching work should be evidence-first, in
this order:

1. **Term statistics cache.** The coordinator re-fetches per-shard df
   for every query. Frozen per epoch, tiny, trivially correct to cache,
   already implicated in the df-pruning work. Do first.
2. **Query result cache.** Keyed on (normalized request, epoch). Epoch
   keying makes invalidation exact rather than heuristic: a rebuild
   changes the epoch and the cache empties itself. Worth it for the
   console's repeated-query pattern; unknown value for real traffic.
   TODO: measure repeat rates once there is real traffic to measure.
3. **Vector leg candidates.** The roughly 95 ms gap between the hybrid
   vector leg and the standalone vector path is a measured oddity in
   `work-queue.md` section 4. Understand it before caching around it;
   it may be a bug wearing a latency costume.

What we will not do is cache inside the scoring path where entries
could survive an index swap. Every cache keys on epoch or it does not
ship. This is the loud-failure principle applied to staleness.

## 5. The public search API

Last because it consumes all of the above. The internal gRPC surface is
shaped by trusted callers: it exposes raw k, shard debugging, variant
arms. A public surface needs:

1. Paging with stable cursors. The floor protocol gives exact top-k,
   which makes offset paging honest, but deep offsets still cost k.
   Cursor is (score, stable id) of the last hit; the max-k cap already
   bounds the worst case.
2. Facet and filter syntax over the facet columns from section 2.
3. Auth, quotas, and TLS, which fold in the membership work already
   queued in `work-queue.md`.
4. A versioned response shape that hides index internals. No local doc
   ids, no shard numbers, lineage keys only.

TODO: REST gateway versus public gRPC versus both. The console would be
the first consumer of whichever ships, which is the cheapest way to find
out the shape is wrong.

## 6. Sequencing and what this track does not touch

A workable order inside the track: term-stats cache (small, pays back
immediately), facet columns and count-then-rank facets, functions on
columns reusing the same column storage, then the public API over the
lot. The layer-separation design runs alongside as a document first,
service second.

This track deliberately does not touch the index format beyond adding
section types, does not touch the analyzer, and does not depend on the
v7 rebuild (track 2) except that facet columns land in newly built
shards, so facet work meets the rebuild at the column-writing step.
If both tracks run at once, that seam is the coordination point.
