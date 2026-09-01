# Block-max bounds

A block-max index stores, next to every fixed-size block of an index, an
upper bound on the score any entry in that block can produce. A top-k
search that already holds a floor (the current k-th best score) tests the
bound first: if the block cannot beat the floor, the block is skipped
without being decoded. The result set is unchanged, because everything
skipped was provably non-competitive.

The technique is Lucene's; the attribution is at the end of this
document. What follows is where it pays in this system, where it does
not, and the design for the half where it does.

## Where it pays here

Two legs, two answers, both measured.

**The lexical leg is the bottleneck and block-max is its fix.** Timings
below are a single `Bm25Query` RPC against one live shard (10,829,174
documents, avgdl 204.7), k=10, warm:

| query | postings walked | wall |
|---|---:|---:|
| `habea` | 214,134 | 0.21 s |
| `summari` | 603,938 | 0.41 s |
| `appeal` | 1,562,888 | 0.81 s |
| `court` | 4,938,028 | 2.81 s |
| `court state appeal` | 9,016,923 | 4.61 s |

Wall time is linear in postings walked at roughly 550 ns per posting, and
a repeat of the `court` query returns 2.61 s, so this is CPU per posting,
not I/O. For comparison the vector leg answers k=10 across all eight
shards in 313 ms. One common term costs nine times the entire vector
cluster, and nothing in the current path can avoid it: `bm25::top_k`
walks every posting of every query term into a `HashMap`, allocating the
occurrence list of each posting on the way, then sorts and truncates to
k.

**The vector leg cannot use block-max at all.** Measured, not assumed:
see [Part 2](#part-2-the-vector-scan-a-measured-negative). Geometric
bounds over 32-vector blocks in dim 256 prune 0.2-0.5% of blocks, which
is less than the sidecar they cost. The bytes lever on that side is
cascade quantization, not block skipping.

---

# Part 1: impact-blocked BM25

## Why the bound is tight on this side

A BM25 term contribution is `idf(N, df) * tf_norm(tf, dl, avgdl)`, and
`tf_norm` is monotone: increasing in `tf`, decreasing in `dl`. The
largest contribution a block can produce is therefore attained by a
document that actually exists in that block, at the maximum of the block
over the `(tf, dl)` Pareto frontier. There is no slack term and no
dimension count in the bound. That is the whole reason block-max works
for lexical retrieval and fails for dense vectors.

One subtlety falls straight out of this system's design: shards score
with the **global** corpus stats the coordinator supplies, never
shard-local ones (`bm25.rs`). `avgdl`, `N` and `df` are therefore not
known at build time, so a per-block bound cannot be stored as a float.
Store the `(tf, dl)` pairs and evaluate them per query. Lucene reaches
the same conclusion for the same reason: `Impacts.getImpacts()` returns
`(freq, norm)` pairs rather than scores.

The frontier is also parameter-agnostic. Because `tf_norm` is
non-decreasing in `tf` and non-increasing in `dl` for every `k1 >= 0`,
`b >= 0`, a pair dominated by another (`tf' >= tf` and `dl' <= dl`) can
never be the block maximum under any parameter choice, so pruning the
dominated pairs at build time is safe for any `k1`/`b` a query later
picks.

## Format: `.bm25` v5 (`TVBM2505`)

v4 stores each term's postings as one variable-stride byte run: per
posting `doc_id u32, tf u32, n_offsets u32`, then `n_offsets` pairs of
`u32`. Occurrences are interleaved with the scoring fields, so a scorer
cannot step over a posting without reading its occurrences, and it cannot
seek to posting *i* at all. Both properties have to change before any
skipping is possible.

v5 splits each term into three runs and adds a skip run:

```
per term:
  doc run        fixed 12 B stride: doc_id u32 | tf u32 | occ_start u32
                 (occ_start relative to the term's occurrence run; the
                  occurrence count is the next posting's occ_start minus
                  this one, with one trailing sentinel occ_start per term
                  so the last posting's count is derivable too, and no
                  scan is needed to locate any posting's occurrences)
  occurrence run u32 start, u32 end pairs, in posting order
  skip run       u64 level1_region_off (level-0 records are variable
                     stride, so the level-1 region's start is not
                     derivable from df alone)
                 level 0, one record per 128 postings:
                     last_doc_id u32
                     n_pairs u8, then n_pairs x (tf u32, dl u32)
                 level 1, one record per 32 level-0 blocks (4096 postings):
                     last_doc_id u32
                     skip_run_offset u64
                     n_pairs u8, then n_pairs x (tf u32, dl u32)
```

The doc run needs no pointers because it is fixed stride: block *i*
starts at `12 * 128 * i`, and `advance(target)` inside a block is a
binary search. Only the skip run is variable stride, which is why the
level-1 record carries one offset into it.

The pair lists are the Pareto frontier of `(tf, dl)` over the block,
capped at 8 entries. Truncation has to be done by **collapsing, not
dropping**: a group of adjacent frontier entries is replaced by its
dominating corner `(max tf, min dl)` of the group. Dropping entries
outright would be a correctness bug, because which frontier entry
maximizes `tf_norm` depends on the query's `avgdl`, so a dropped entry
can be the maximum. Collapsing only ever raises the bound, which costs
tightness and never exactness. Cost is at most 69 B per 128 postings
(5 + 8x8), about 0.5 B per posting against a 12 B doc run, plus 37 B
per 4096 postings at level 1.

The directory entry grows from 18 B to 34 B: `doc_run_off u64,
skip_run_off u64, occ_run_off u64, df u32, blob_off u32, term_len u16`.
Everything else in the file (header, doc lengths, texts, text index,
lineages, term blob) is unchanged, and the loader keeps accepting v3/v4
as it does today, with the exhaustive scorer as their path.

Fixed 12 B stride is deliberate. Delta plus varint would cut the doc run
three- to four-fold, but the measurement above says this leg is CPU-bound
at 550 ns per posting, not byte-bound; fixed stride keeps `advance(target)`
a binary search inside a block instead of a decode. Compression is an
orthogonal later step, and it composes: skipped blocks are never
decompressed either.

## Build side

`SpillBuilder` already merges runs per term in doc-id order, which is
exactly the order impacts must be accumulated in. The merge loop
carries:

- a running Pareto frontier over the current 128 postings, flushed to the
  skip run at each block boundary and merged into the level-1 accumulator;
- the occurrence bytes diverted to the term's occurrence run, with
  `occ_start` recorded in the doc run.

Both are single-pass and hold O(1) state per term, so the sub-1 GB build
memory the spill builder buys is untouched. Lucene structures this the
same way: `CompetitiveImpactAccumulator` is fed per document and drained
at each block boundary by the postings writer.

## Query side: the pruned scorer

`bm25::top_k_pruned` is a block-max MaxScore. Per term, an
`ImpactCursor` (returned by `Bm25Index::impacts`, `None` for the heap
store and v3/v4 files, which keep the exhaustive path) holds the current
128-posting block's frontier — `idf * max over the frontier of
tf_norm(.., avgdl)` upper-bounds that term's contribution anywhere in
the block — a bound for the current level-1 group, and a static
whole-term bound. Each iteration:

1. **Termination**: the sum of the static whole-term bounds can no
   longer clear the floor — nothing remaining can enter, stop.
2. **Level skips**: if the sum of level-1 group bounds cannot clear the
   floor, every cursor leaps past the shallowest group end (4096
   postings per term at one test); else if the sum of level-0 block
   bounds cannot, every cursor shallow-advances past the shallowest
   block end.
3. **MaxScore partition**: inside a competitive window (up to the
   shallowest block end, where every block bound is valid), the largest
   prefix of terms — sorted by block max — whose maxes sum inert is
   non-essential. Only essential terms generate candidates.
4. **Candidate test**: a candidate's bound is its essential (true)
   contributions plus the non-essential block maxes; if inert, the doc
   is dropped unevaluated. Otherwise the doc is fully evaluated, scored
   in term order, and inserted into the heap on the exact contract.

On every heap replacement the floor rises and every bound test
re-evaluates against it.

Occurrences are read only for the k survivors, through `occ_start`. That
alone removes the per-posting `Vec` allocation that dominates the 550 ns,
and it is independent of any skipping.

**Legacy formats.** v3/v4 files load and serve, but they are kept for
migration only: they serve on the unoptimized exhaustive path, and the
occurrence-split scorer does O(k·df) survivor-offset lookups on them, so
large-k `Bm25Search` against an unmigrated v4 file is slower than it was
before v5 (crossover around k≈300). There is no backward-compatibility
commitment to that path — production shards should be migrated to v5
(via a WAL `reshard` replay plus `InstallSnapshot`, or simply by taking
new documents, which flush v5).

## Exactness and its gate

The skip test is `bound <= cutoff` against the current k-th best, and
`<=` rather than `<` is load-bearing on the tie case, so it is worth
spelling out. `top_k` sorts by score descending, doc id ascending, so
at equal scores the smaller doc id wins. A doc-ordered scan reproduces
that ordering exactly when the heap replaces only on a strictly greater
score: the incumbent is always the smaller doc id. A block whose bound
equals the cutoff therefore lies later in doc-id order than the
incumbent and can contain nothing that displaces it, so `<=` is safe.
Reverse either half of that (scan out of doc order, or let ties
displace) and the test has to weaken to `<`.

Doc order is maintained **structurally**: on the inert-drop path every
cursor — essential and non-essential — advances past the dropped doc, so
no cursor ever lags the wavefront and candidate selection is strictly
doc-id increasing (a `debug_assert!` checks it on every selection). The
advance is sound: an unconsumed doc behind the wavefront has postings
only in currently non-essential terms (every essential cursor sits at or
past the wavefront, so its earlier postings are consumed), the partition
proved the sum of those terms' block bounds inert over the whole window,
and inertness only strengthens as the floor and the k-th best rise —
such docs can never become insertable later. (An earlier revision let
non-essential cursors lag and relied on that argument alone to keep
results exact; the argument held, but the *stated* doc-order invariant
was false, so the code now enforces it.)

One more subtlety that is load-bearing: every bound sum — termination,
both skip tiers, the partition, the candidate test — is accumulated in
**term-index order**. IEEE addition is not associative, and only the
identical association order provably dominates the true (term-ordered)
score; a reordered bound can dip an ULP below a tied floor and prune a
doc that must survive.

Skipping then only removes candidates that would have been rejected, so
the returned top-k is bit-for-bit the exhaustive result. This is the same
contract the vector side already carries for `initial_threshold`: ties at
the floor survive, and a seeded floor above the true k-th best returns
the unseeded result filtered to that floor. The wire floor is f32 while
scoring is f64, and `f32(kth)` rounds up half the time — so every
`kth_best` is **emitted one f32 ULP down** (`bm25::floor_seed`), which
can never exceed the true k-th best; seeding with the emitted value is
provably lossless (the round-trip gate below).

Gates, in the project's existing style:

- a property test scoring a random corpus both ways and asserting equal
  `(doc_id, score.to_bits())` sequences, over random `k`, term counts,
  and `k1`/`b`;
- a test asserting the truncated frontier never under-bounds: for every
  block, the stored frontier's max is `>=` the max over the block's real
  postings, at several `avgdl` values;
- a seeded round-trip test: for random corpora and every `k`, seeding
  with the emitted `kth_best` and re-querying returns the unseeded
  result exactly (zero lost boundary hits), including engineered tie
  clusters at the boundary;
- a level-1-scale fuzz (20k-44k docs, clone tie pressure, duplicate and
  absent terms, floors including exact-kth and mid-range) that runs with
  the doc-order `debug_assert!` armed;
- `cluster_sweep`'s cross-cell hit-signature gate extended to the lexical
  leg, so a fleet sweep with pruning on is compared against pruning off
  the way sharing on/off already is.

## Exact lexical candidate streaming

Most of what the vector leg built for floors applies unchanged:

- `Bm25QueryRequest` gains `min_score`, the lexical twin of
  `SearchOptions::initial_threshold`, so a coordinator can seed a shard
  with the merged k-th best it already holds;
- in cascade fusion mode, phase 2's `score_candidates` gets the skip run
  for free: a merge-join against a sorted candidate list becomes
  `advance(target)` over blocks instead of a full postings walk.

The mid-query protocol is now more than a floor relay. `Bm25QueryStream`
is a bidirectional candidate stream:

1. The coordinator sends the ordinary globally scored request, including
   any caller-supplied `min_score`.
2. On the block-max path, each shard emits every fully evaluated candidate at
   or above its current inclusive floor as packed 12-byte
   `(u64 doc_id, f32 score)` records. An exhaustive compatibility fallback may
   emit its complete local top-k frontier after scoring; that remains exact
   because a global top-k winner cannot fall outside its shard's top-k.
3. The coordinator maintains the only authoritative global top-k heap. Its
   emission-safe k-th score is conflated into one watch cell and relayed to
   every shard. `bm25::LiveFloorHook` adopts raises between block-bound tests.
4. Each shard terminates with `completed=true`, a non-empty scoring
   fingerprint, the emitted-candidate count, and its ordinary local response.
   The coordinator rejects EOF, cancellation, an obsolete uncertified terminal
   response, count drift, score drift, or a mismatched fingerprint.
5. The local responses enrich the certified global winners with offsets,
   projections, facets, and column handshakes. They do not define the global
   result heap.

Exactness is structural. A relayed floor is a proven lower bound on the final
global k-th score, the seed is one f32 ULP below that score, and scorer tests
use inclusive comparisons so boundary ties survive. A result exists only when
every configured shard certifies a complete scan in the same score space.
`tests/bm25_live_floor.rs` pins the scorer hook,
`tests/bm25_search.rs::bm25_stream_relay_matches_unary_exactly` pins exact
streamed-versus-unary fleet results,
`bm25_stream_candidates_end_in_a_scoring_certificate` pins candidate and
certificate accounting, and `bm25_stream_stop_is_an_incomplete_certificate`
pins cancellation semantics. `tests/multi_field_wire.rs` applies the same gate
to fused multi-field scoring.

The candidate protocol is the default (`bm25_stream = true`). Operators can
force the legacy unary route with `--bm25-stream=false` or
`TURBOVEC_BM25_STREAM=false` for an exact A/B comparison. Nodes always serve
the stream RPC. Phrase-aware BM25 currently remains on its unary exact scorer;
ordinary flat and fused multi-field BM25 use the candidate stream. The earlier
floor-only relay measurement on the v9 court fleet remains useful historical
evidence: at k=100, BM25 p90 fell from 262 to 231 ms and max from 360 to 298 ms
on eight same-host shards. It is not a measurement of this newer
coordinator-only heap protocol; the OpenSearch challenge suite is the current
measurement path.

The caller-seeded floor composes with live streaming:
`Bm25SearchRequest.min_score` forwards a lower bound the caller already holds
(for example, the `kth_best` of a previous identical query re-issued after
appends), and the coordinator raises it from there.

Before block-max, a raised floor only skipped heap insertions, which is
why the five-machine round measured a 51% candidate cut at cost parity: a
cheaper way to reject candidates is worth nothing when the scan reads
every byte regardless. Block-max is the mechanism that converts a floor
into bytes not read. The floor work is the input; this is the multiplier.

Note also what the RPC does not promise: `Bm25QueryResponse` carries hits
only, no total match count. The usual dynamic-pruning caveat (Lucene has
to downgrade `TotalHits.Relation` to `GREATER_THAN_OR_EQUAL_TO` once
pruning is on) does not bite, because nothing here ever counted matches.

## Staging

Landed in this order, each measurable on its own:

1. **Occurrence split, no skipping** — landed. v5 (`TVBM2505`) writer
   plus reader, occurrences fetched only for the k survivors, scorer
   accumulates with an allocation-free membership bitmask. Measured
   (1M-doc court-shaped corpus, k=10): the high-df `court` query fell
   from 162 ms (old v4 scorer) to 40 ms, with allocations from ~2 per
   posting to ~40 per query — the per-posting allocation was indeed the
   dominant cost.
2. **Level-0 impacts, block skips** — landed. `ImpactCursor`,
   `top_k_pruned` with range skips and static-bound termination,
   bit-exact against the exhaustive oracle over the property gate,
   `min_score` on `Bm25QueryRequest`. Measured: mixed-shape query 184 →
   3.8 ms; the large-df terms fell by more than the small-df ones as
   predicted, rare terms barely moved.
3. **Level-1 skips + MaxScore partition** — landed. Level-1 leaps in
   `advance_shallow` and the essential / non-essential partition inside
   competitive windows (exactness held: every bound sum in term-index
   order). Measured: full evaluations on the mid shape dropped from
   644k to 1.5k (k=10), wall 13.3 → 2.9 ms; level-1 group leaps
   concentrated in the highest-df term. Also: the HybridSearch lexical
   leg and `Bm25Rescore` route through the pruned paths, and the v4
   reader's `posting_offsets` early-exits (its k=1000 column fell from
   16 s to 1.5 s).
4. **Seeded lexical floor across the fleet** — landed, client-seeded
   and unary. `kth_best` on
   `Bm25QueryResponse` / `Bm25SearchResponse`, `min_score` on
   `Bm25SearchRequest`, the `--block-max` node flag
   (`PIPESTREAM_SEARCH_BLOCK_MAX`, default true) for A/B, and `cluster_sweep
   --bm25-terms` running the `{floor seeding off, on} x {block-max off,
   on}` factorial with the hit-signature gate on every cell (the
   seeded cell seeds one f32 ULP below the merged k-th best, so seeded
   must equal unseeded exactly). A live-fleet 2x2 at court scale
   remains an operational run; the gate logic and semantics are covered
   by `tests/bm25_search.rs::bm25_search_min_score_factorial_across_the_fleet`
   and the `lexical_sweep_smoke` example.
5. **Mid-query live-floor relay** — landed first as an opt-in floor-only
   protocol. It established the monotone `LiveFloorHook` and exactness gates.
6. **Coordinator-owned lexical heap and certificates** — landed. The bidi
   stream now carries compact candidates and a terminal score-space
   certificate. The coordinator alone selects the global top-k, refuses every
   incomplete shard set, and uses the local terminal responses only to enrich
   winners. It is on by default for flat and fused multi-field BM25 and is the
   collector behind public `QueryStream` lexical revisions.

## Migrating existing shards to v5

v3/v4 files keep loading on the exhaustive path forever (with the
allocation-light scorer and the early-exit `posting_offsets`, so legacy
is slower but never broken). Upgrading a live shard needs no pipeline
rerun: every persisted shard's full document history is in its WAL, so
a v4→v5 rebuild is a `reshard` replay (merge 1→1, or any split/merge
that was going to happen anyway) followed by `InstallSnapshot` — the
same machinery as resharding, with the same calibration invariants. A
shard that simply receives more documents also upgrades itself: the
heap-builder reload path writes v5 on its next flush.

---

# Part 2: the vector scan, a measured negative

The original idea was per-block score upper bounds so a floor could skip
*reading* blocks in the quantized scan, since the fleet is
memory-bandwidth-bound and nothing else touches bytes read. It does not
work, for a reason that is structural rather than fixable by tuning.

Probe: 1,048,576 consecutive corpus vectors (dim 256, unit norm) in
32-vector blocks, 64 in-distribution queries, floor taken from a 4M-vector
stride sample so it approximates the real 86.6M-chunk k=10 floor (0.909).
Two bound families, both exact:

- **box**: per-dimension `[min, max]` over the block;
  `bound = sum_d (q_d > 0 ? q_d * hi_d : q_d * lo_d)`. Evaluable through
  the existing nibble LUT, 256 B per block of sidecar.
- **ball**: centroid and radius; `bound = q . c + r`. 132 B per block.

| layout | bound | mean bound | true block max | pruned |
|---|---|---:|---:|---:|
| corpus order | box | 1.4325 | 0.8097 | 0.32% |
| corpus order | ball | 1.4934 | 0.8097 | 0.25% |
| rp-tree reorder | box | 1.4049 | 0.8027 | 0.49% |
| rp-tree reorder | ball | 1.3798 | 0.8027 | 0.37% |

At those rates the sidecar costs more bytes than the skips save: 0.94x,
i.e. 6% *more* traffic, not less.

The reason is a one-line calculation. Mean true block max is 0.81 against
a floor of 0.909, so 90%+ of blocks genuinely contain nothing
competitive; the potential is enormous. But the ball bound's slack is
essentially the block radius, and the margin available is
`floor - block max = 0.099`. Measured block radius is 0.789 in corpus
order and 0.675 after a recursive random-projection reorder. A bound
would need blocks eight times tighter, meaning all 32 members within 0.1
of their centroid, which is cosine 0.995 and means near-duplicates. Box
slack behaves the same way and grows as `sqrt(d) * sigma`, so it gets
worse with dimension while the score stays O(1). Better clustering moves
0.789 to 0.675, not to 0.1.

Reordering also is not free: physical order defines the slot, and a
global id is `slot_offset + local id`, so a reorder pass has to run at
ingest (or as a WAL replay, which `reshard` already makes derivable)
rather than over a built shard. Given that the ceiling it buys is 0.49%,
none of that is worth building.

What does move bytes on the vector side is reading fewer bytes per
vector rather than skipping vectors: a 1- or 2-bit first pass over all
vectors followed by an exact rescore of the survivor pool, which is the
already-queued 2-bit + rerank item. The measured recall of the same
shape, quantized top-10k rescored against fp32, is 1.0000 at k=10 and
k=100. Bytes go 128 B/vector to 32 or 64 B plus the pool. That is the
lever; block-max is not.

Probe script: [`benchmarks/blockmax_probe.py`](benchmarks/blockmax_probe.py)
(numpy only, reads the embeddings file directly, no cluster needed).

---

# Attribution

Block-max indexing is Lucene's, and the reference checkout is
`/work/reference-code/lucene` at `4965e8d4d96`. Paths below are relative
to `lucene/`.

**The bound itself, stored per block at index time**

| file | what it is |
|---|---|
| `core/src/java/org/apache/lucene/codecs/Impact.java` | one `(freq, norm)` pair: the bound is a pair, not a score, because scoring parameters are not known at write time |
| `core/src/java/org/apache/lucene/codecs/CompetitiveImpactAccumulator.java` | build-time accumulation of the Pareto-competitive pairs for a block |
| `core/src/java/org/apache/lucene/codecs/lucene104/Lucene104PostingsWriter.java:392-408, 501-508` | level-0 and level-1 impacts written into the postings file |
| `core/src/java/org/apache/lucene/codecs/lucene104/Lucene104PostingsFormat.java:343-353` | `BLOCK_SIZE = 256`, `LEVEL1_FACTOR = 32`, so level 1 covers 8192 docs |
| `core/src/java/org/apache/lucene/codecs/lucene104/Lucene104PostingsReader.java:277-448` | `BlockPostingsEnum`, the two-level cursor with separate doc/pos/pay file pointers per level |

**The read API**

| file | what it is |
|---|---|
| `core/src/java/org/apache/lucene/index/Impacts.java` | `numLevels()`, `getDocIdUpTo(level)`, `getImpacts(level)`: the multi-level bound contract |
| `core/src/java/org/apache/lucene/index/ImpactsSource.java:39` | `advanceShallow(target)`, positioning without decoding, which is what makes a skip cheaper than a read |
| `core/src/java/org/apache/lucene/index/SlowImpactsEnum.java` | the degenerate implementation, useful as the shape of a fallback |

**The consumers**

| file | what it is |
|---|---|
| `core/src/java/org/apache/lucene/search/MaxScoreCache.java:96-151` | impacts to a max score per level, cached, plus `getSkipUpTo(minScore)` |
| `core/src/java/org/apache/lucene/search/ImpactsDISI.java:56-99` | the canonical skip loop: `if (maxScore >= minCompetitiveScore) return target;` else advance past the block |
| `core/src/java/org/apache/lucene/search/MaxScoreBulkScorer.java` | MaxScore, with the essential / non-essential partition (the model for `top_k_pruned` above) |
| `core/src/java/org/apache/lucene/search/WANDScorer.java:31-40` | WAND, and the header citing Broder et al. and Ding and Suel |
| `core/src/java/org/apache/lucene/search/BlockMaxConjunctionScorer.java`, `BlockMaxConjunctionBulkScorer.java` | the conjunction case |

**The floor that drives it**, which is the part this project already built
on the vector side:

| file | what it is |
|---|---|
| `core/src/java/org/apache/lucene/search/TopScoreDocCollector.java:147,162` | the collector pushing `setMinCompetitiveScore` down into the scorer |
| `core/src/java/org/apache/lucene/search/MaxScoreAccumulator.java` | a `LongAccumulator(Math::max)` sharing the floor across concurrently searched segments |

`MaxScoreAccumulator` is worth reading next to `search.rs`'s
`shared_floor`: both are a monotone max over a total-order key in one
atomic, for the same reason. Lucene shares between segment searches;
the fork shares between scan ranges, and pipestream-search shares between
machines.

**Ordering, the companion technique**: block-max gets better when blocks
are homogeneous, which is why Lucene ships reorderers.
`misc/src/java/org/apache/lucene/misc/index/BPIndexReorderer.java`
implements recursive graph bisection (Dhulipala et al., with the
Mackenzie et al. tradeoffs), and
`misc/src/java/org/apache/lucene/misc/index/BpVectorReorderer.java`
applies the same partitioning to vectors, with a header deriving why the
centroid is the right partition representative for dot products. Part 2's
rp-tree probe is a cheap stand-in for that, and its result is why no
reorder pass is proposed here.

**Papers**: Broder, Carmel, Herscovici, Soffer, Zien, *Efficient Query
Evaluation using a Two-Level Retrieval Process* (CIKM 2003) for WAND;
Turtle and Flood, *Query Evaluation: Strategies and Optimizations* (1995)
for MaxScore; Ding and Suel, *Faster Top-k Document Retrieval Using
Block-Max Indexes* (SIGIR 2011) for the block-max refinement.

# Is it in Qdrant?

Partly, and only on the sparse side. Reference checkout
`/work/reference-code/qdrant` at `db8fa43fc`, version 1.18.3.

`lib/sparse/src/index/posting_list_common.rs:34` gives every sparse
posting a `max_next_weight`: the maximum weight over the remainder of
that posting list. `lib/sparse/src/index/search_context.rs:361-422`
(`prune_longest_posting_list`) uses it the way MaxScore uses a max score:
if `max_weight_from_list * query_weight <= min_score`, the rest of the
list cannot contribute and is skipped.

Three differences from block-max proper:

- it is a **suffix** maximum per element, not a per-block maximum, so it
  is the MaxScore family rather than the block-max refinement; a single
  heavy posting late in a list weakens every bound before it, which is
  exactly what per-block bounds fix;
- it is **gated on sign**: `search_context.rs:81` disables pruning
  entirely when any query weight is negative, since the bound assumes
  non-negative contributions;
- it prunes **only the longest posting list**, not an essential /
  non-essential partition across all of them.

On the dense side Qdrant has nothing of the kind, and there is no reason
it would: dense search there is HNSW plus quantization with rescoring, so
it never runs the exhaustive scan that a block bound would prune. A grep
for block/max-score bounds across `lib/segment/src/vector_storage/` and
`lib/quantization/` returns nothing. Part 2 above is the measured reason
that absence is the right call rather than an omission.
