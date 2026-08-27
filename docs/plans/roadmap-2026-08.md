# Roadmap, 2026-08-24: twenty things to build next

Written cold against the tree, not from the older plans. Tracks 1-3
(`track-1-features.md`, `track-2-reindex.md`, `track-3-ner-performance.md`)
described the world of early August; most of track 1 has since landed
(facets, the whole typed column plane through geo, CEL filters, v8
integrity, term-stats cache). This document is the next twenty items,
with the reasoning that picks them and, where it matters, the
verification that says the item is smaller or larger than it looks.

The framing question is the one the project keeps answering: what makes
this engine something other people would run instead of OpenSearch, and
what does exactness cost when we say yes. Three constraints govern every
item below and are stated once here rather than repeated:

1. **Page cache, not disk, is the budget.** Measured 2026-08-02: `.tv`
   vectors 10.7 GB, `.bm25` postings plus texts 394.8 GB, RAM 121 GB.
   The vector leg's full linear scan touches only what fits in cache and
   runs at a stable ~216 ms p50; BM25 sits near 6.5:1 size-to-cache.
   Anything that grows postings evicts the only cache that matters. Cost
   a proposal in bytes per document, against RAM.
2. **Removal is free, admission is not.** Every filter family so far
   (geo, then CEL) landed with no new pruning math because a filter only
   removes documents, so every block-max bound stays an upper bound over
   what survives. The moment a feature can *admit* a document, or can
   reorder by something other than the scored quantity, it owes a new
   certificate. Items below say which side of that line they are on.
3. **Refusal beats degradation.** A shape the engine cannot certify is
   `INVALID_ARGUMENT` naming the reason. That rule already produced the
   typo rule, the antimeridian refusal, and the hybrid-filter refusal,
   and it is what makes the approximate-vector item (14) designable at
   all.

Items are grouped by what they unblock, and the tiers at the end give a
suggested order.

---

## A. Close the gaps that block a public surface

### 1. Vector-leg filters (the "increment C" gap) — LANDED

**Landed 2026-08-24 (`docs/vector-filters.md`).** It came in about where
this item predicted: an allowlist built from the same resolved
`DocFilter` the lexical heap gate uses, handed to the kernel as
`SearchOptions::mask`, with no pruning math touched. Two things the
plan below did not anticipate. The chunked scan ALREADY passes a mask
on every call (its chunk range is an allowlist), so the serial-path
forfeit was already paid on that route and this increment adds one only
to the streaming route. And batching needed a real change: queries with
different allowlists cannot share a kernel call, because the kernel
returns each query's top `chunk_k` over the masked set and a union mask
would let one query's rejected documents fill another's quota — so the
batch groups by allowlist identity, and unfiltered queries all share the
`None` group and batch exactly as before. Original analysis follows.

`Bm25Search` takes a CEL filter; `HybridSearch` refuses one, because the
vector leg has no filter machinery. That refusal is what makes hybrid
facets ill-defined, blocks vector-plus-CEL in the public contract, and
makes the engine's best feature unavailable on its best route.

**This is smaller than it has been recorded as.** The vector kernel
already supports it upstream. `turbovec::SearchOptions` carries
`mask: Option<&'a [bool]>` (a per-slot allowlist) alongside
`initial_threshold`, `search_streaming`/`try_search_streaming` take
`SearchOptions`, and `search.rs` short-circuits whole 32-vector blocks
that the mask excludes (`block_has_allowed`, counted behind the
`mask-skip-counter` feature). The work is therefore plumbing, not
kernel: resolve the `FilterExpr` IR per shard exactly as
`Bm25Shard::resolve_filter` already does, materialize the survivors as a
slot mask, and hand it to the streaming scan.

Two things to get right. A mask forces the serial path
(`serial_required(mask_present, ..)` returns true whenever a mask is
present), so a *weak* filter costs the SIMD batch path and buys few
skipped blocks; the mask should be worth its own cost, and the block-skip
counter is how that gets measured rather than assumed. And the mask is
exact, which means the floor protocol and the completion certificates
survive untouched: masked slots simply never enter the heap, which is
the same removal-only argument every filter family has used.

Unblocks: hybrid + filters, hybrid facets, vector + CEL in the public
contract, and item 2.

### 2. Filter-only browse (match-all selection) — LANDED

**Landed 2026-08-24 (`NodeService.BrowseShard`, `src/query.rs`,
`tests/query_api.rs`).** A match-all iterator over the document space,
gated by the resolved filter at the same Kleene admission rule every
scored route uses, ordered by global doc id ascending — the
deterministic order the absence of a relevance score requires. Deep
paging costs one page (an `after` id floor), and browse cursors are
inherently stable because the engine is append-only. On the public
Query route, one bare FilterQuery (or an AND of filters) IS a browse.
Item 7's column sort landed on top of the same walk.

The original rationale, kept:

There is no way today to ask for documents without a scoring leaf. Every
route starts from analyzed terms or a query vector. "Show me every
opinion from this court in this date range, newest first" is the most
ordinary request a search product receives and the engine cannot express
it.

Needs a match-all iterator over the slot space, gated by the resolved
filter at the same heap-insertion point, and a deterministic order in
the absence of a relevance score. It is the natural pair to item 7
(sort), and together they are the "structured browse" half of what
people use OpenSearch for.

### 3. The public `Query` RPC, increment 1 — LANDED (and the LTR path, 2026-08-26)

**Landed 2026-08-24 (`src/query.rs`, `tests/query_api.rs`).**
**2026-08-26: the LTR path completed the contract** (PRs #32-#35,
`src/ltr.rs`, `tests/ltr.rs`): the generic composite scorer (six
operations, pool normalization, missing policies, per-dimension
provenance precise enough to recompute every score client-side), the
generalized boost phase (lexical and dense, any scored shape, several
under the scorer, through the candidate-scoped rescore seams), stored-
value dimensions and projections on every shape (the `FetchValues`
seam), and the profile surface. The remaining unsupported shape is
arbitrary nested boolean search.

The original increment-1 scope, kept: the adapter and nothing more, as
scoped: seven shapes execute bitwise
through their ordinary routes, everything else refuses by name. One
design point settled during implementation: cascade is expressed as a
strategy whose membership is the gate's own (operator UNSPECIFIED
required), and the candidate-scoped lexical boost rides only composite
selections until the single-leaf boost has an engine path.

The original rationale, kept:

`docs/query-api.md` is a finished contract with nothing behind it: the
selection / boost / composite-scorer phase split, the rule that a boost
never admits a document, `k <= selection_k <= max_k`, per-dimension score
provenance, and the mapping table from public concepts to the routes
that already exist (`Search`, `Bm25Search`, `HybridSearch`,
`BoostRescore`, `ScoreStage`).

Increment 1 is the adapter and nothing more: execute the shapes the
mapping table already proves, refuse everything else by name. That gets
the external shape under test — the console is the first consumer and the
cheapest way to learn the shape is wrong — without inventing engine math.
Each later item in this document then lands as one more row in that
table rather than as a new RPC.

### 4. Paging with stable cursors — LANDED

**Landed 2026-08-24 (`src/query.rs`, `tests/query_api.rs`).** Search-after
semantics on the public Query route: the token embeds the boundary hit's
(rank, score bits, doc id), and resumption re-finds that exact hit —
bitwise, which the engine's determinism makes a real corpus-state check —
refusing with FAILED_PRECONDITION when the corpus changed under it. The
subtlety this item predicted (epoch stability) landed as that bitwise
boundary check rather than an epoch token: cheaper, and it validates the
one thing paging actually depends on. Single-leaf queries page by
deepening (exact prefix property); composites page within their fixed
selection_k pool, because RRF ranks, blend normalization, and the cascade
gate all move with depth — an exhausted pool refuses and names the knob.

The original rationale, kept:

The floor protocol gives exact top-k, which is what makes offset paging
honest here in a way it is not in engines that approximate. What is
missing is the cursor: `(score, stable id)` of the last hit, resumed by
seeding `min_score` (which both `Bm25QueryRequest` and
`Bm25SearchRequest` already carry) and skipping the tie prefix. Deep
offsets still cost k, and `max_k` is the existing guardrail.

The subtlety worth writing down before coding: a cursor is only stable
across an index epoch. Paging across a rebuild must refuse rather than
silently return a page from a different corpus, the same way every cache
in this engine keys on epoch or does not ship.

---

## B. Query features the engine cannot currently express

### 5. Phrase and proximity queries

The largest missing *retrieval* feature, and the one where the honest
answer costs something. Postings store occurrence spans in
**original-text character coordinates** (`OffsetSpan { start, end }`,
returned per hit in `TermOccurrences`), not token ordinals. The sidecar's
`TermVector` likewise carries `repeated Span occurrences`, and token
ordinals exist only in the separate `tokens` layer.

Character offsets can express adjacency, but they cannot express slop
honestly: "no intervening token" is not a question a pair of character
ranges can answer, because whitespace and a dropped stopword look
identical. So the two routes are:

- **Token positions as an opt-in per-field payload.** The sidecar emits a
  token ordinal alongside each occurrence (a small addition to
  `TermVector`, reported through `GetCapabilities` per ground rule 2),
  and the postings gain a position payload for fields that ask for it.
  Exact phrase, exact slop, and the standard proximity vocabulary follow.
  The cost is postings growth on the field that opts in, which is
  constraint 1 in its sharpest form and must be measured on a real shard
  before it is adopted, not after.
- **A bigram column.** Already in `work-queue.md` section 4. Answers most
  of the phrase demand at a known, bounded cost and needs no new query
  machinery, because a bigram is just a term. It does not answer slop.

Recommendation: price the bigram column first because it is measurable in
an afternoon, and treat positions as the increment that follows only if
the measurement says the corpus needs slop. Either way, a field without
positions must **refuse** a phrase query rather than approximate it.

### 6. Prefix terms, and the sorted term dictionary underneath

Prefix queries need an ordered term dictionary. So do string range
filters, which `docs/cel-filters.md` already records as blocked on
exactly that. One structural change, two features, and the second one
closes a named gap in a shipped feature.

Expansion must be capped and the cap must refuse rather than truncate: a
prefix that expands past the limit is an `INVALID_ARGUMENT` naming the
term count, not a silently narrower result set. Wildcard and fuzzy
matching are deliberately **not** proposed here — they are where lexical
engines spend their worst tail latency, and the honest substitute is item
20's projections plus a real reranker.

### 7. Sort by column, with its own certificate — LANDED (browse route)

**Landed 2026-08-24 (`BrowseSort`, `QueryRequest.sort`,
`tests/query_api.rs`).** The full-traversal route this item predicted:
the shard walks its whole admitted set with a column-keyed k-heap
(exhaustive, so the certificate is trivial rather than argued against
pruning bounds), per-shard exact top-k by key makes the merged union
exact, and i64/f64/asc/desc all reduce to one ascending u64 comparison
via order-preserving key bits — which is also what the sorted cursor
pages on. Documents without a value are EXCLUDED: absence has no honest
position in a column order. Sorting a SCORED selection remains refused
by name — that is the half of this item where the certificate argument
still has to be made, and it did not get a free pass.

The original rationale, kept:

Sorting by score is the only order the engine can produce. Sorting by
`decision_date desc` is the second most common thing a legal search UI
asks for, and it is where an exactness argument has to be made rather
than inherited: block-max pruning bounds the *score*, so ordering by a
column value invalidates the pruning certificate outright. This is
constraint 2's other side, and it does not get a free pass.

Two sound routes, and the item should ship whichever is measured cheaper:
full traversal with a column-keyed heap (the count-then-rank walk already
exists and is priced at ~1.2 ns/posting), or a monotone precomputed
ordering that lets the scan stop early. What must not happen is sorting
the top-k-by-score and calling it sorted.

### 8. Aggregations beyond counting — LANDED

**Landed 2026-08-24 (`Bm25SearchRequest.stats_fields` /
`.cardinality_fields`, `tests/aggregations.rs`).** As priced: stats
(count of value-holding docs, min, max, sum, mean computed at the
coordinator) over numeric and integer columns, on the same one bitmap
all facet kinds share, merged additively. Cardinality took the stance
this document argued: EXACT, via per-shard distinct-value unions
(values, not ordinals — ordinals are shard-local), with the cost being
the value strings on the wire and the caller's explicit choice. Flat
route only, like score stages; the fused route refuses by name.

The original rationale, kept:

Facets count. Nothing computes min, max, sum, average, or a stats bundle
over the f64 and i64 columns, and nothing estimates cardinality. This is
unusually cheap here because the machinery exists: all three facet kinds
already share one match bitmap over the filtered match set, aggregates
are additive across shards exactly as counts are, and the coordinator
merge is the same positional sum. Sum and count give mean; min/max are
already in the column table metadata for the *whole* column and need only
be recomputed over the match set.

Cardinality is the one that needs a decision, because exact distinct
counts across shards are not additive. Either pay for a full ordinal
bitmap union, or declare the estimator and its error in the response. The
project's stance argues for the first with the cost made visible, which is
the same argument that settled count-then-rank.

### 9. Server-side highlighting

`Bm25Hit` already returns every occurrence span in original-text
coordinates, and the texts are in the index. What is missing is snippet
assembly: window selection, merge of overlapping spans, and a boundary
rule. The console does something client-side today.

Doing it server-side saves shipping whole documents to clients, and it
gets better for free from item 17's neighbour: sentence spans are one of
the sidecar's *free* layers (measured within noise of a term-vectors-only
pass), so storing them at ingest means snippets cut at sentence
boundaries instead of mid-clause, at zero query-time cost.

---

## C. What a general-purpose engine has and this one does not

### 10. Deletes and updates

**There is no delete path at all.** No `DeleteDocuments` RPC, no
tombstone, no version field: grep the tree and the concepts do not
appear. Every index is append-only and every correction is a rebuild.
That is coherent for a corpus we regenerate at will, and it is
disqualifying for anyone else — it is the single largest gap between this
engine and something a product team would adopt.

The design writes itself from the rules already in place. A per-shard
tombstone bitmap applied at the same heap-insertion gate as every filter
is removal-only, so no bound and no certificate changes. It rides the WAL
as a record so replay reproduces it, and it becomes a v8 section with its
own CRC like everything else. Update is delete-plus-add. The one thing
that must be declared rather than fixed is statistics: deleted documents
still sit in df and in the length normalizer until the next compaction,
so the response says so, and compaction is the rebuild habit this project
already has, now scoped to a shard.

### 11. Collections, or at least a namespace

One cluster serves one corpus. There is no index name, no collection, no
tenant. Everything — shard layout, the column table, the analysis
fingerprint, the vocabulary — is global to the process. Anyone running
this for more than one dataset has to run more than one cluster.

This is deliberately listed without a design, because the cheap version
(a name on every request, validated against the shard's bound name) and
the real version (multiple column tables and vocabularies per node) are
very different amounts of work, and which one is right depends on whether
item 19 lands. A descriptor-bound index has a natural identity already.

### 12. The operational surface: metrics, TLS, membership, quotas — METRICS LANDED

**Metrics landed 2026-08-24 (`docs/metrics.md`):** a hand-rolled
Prometheus text exporter on `--metrics-listen` — request counters by
RPC route, cumulative scan stats (including the floors offered/published
split this document asked to be measurable), ingest counters, and
per-shard gauges sampled from live state at scrape time. TLS, auth,
membership, and quotas remain unbuilt, as scoped below.

The original rationale, kept:

Zero of these exist: no Prometheus, no OpenTelemetry, no TLS, no auth.
`ClusterHealth` and the debug blocks are what there is. Membership and
TLS have been queued since the earliest work-queue with no design behind
them.

Metrics first and separately, because it is small and because several
other items on this list are supposed to be justified by measurement that
currently has to be produced by hand-run examples. The engine already
counts the interesting things internally (scan stats, skipped blocks,
floors offered versus published); they need an exporter, not new
instrumentation.

---

## D. Vectors: approximate search, honestly

### 13. A quantized cascade before any graph index

Worth stating plainly, because it inverts the usual assumption: the
vector leg is *not* the problem. `turbovec` is a flat TurboQuant index
with hand-written SIMD kernels and no graph or cell structure at all, the
whole 86.6M-vector corpus is 10.7 GB, it lives in page cache, and it
scans exhaustively at ~216 ms p50. The measured ladder already shows
recall 1.0 after rerank.

So the first approximate-shaped win is not HNSW, it is a **bit-width
cascade**: scan at 2-bit for a candidate pool, rerank exactly at full
width, and keep a provable certificate by seeding the exact pass's floor
from the cheap pass. That reuses `initial_threshold`, which exists for
precisely this ("a cheap first pass in a cascade" is in its own doc
comment), needs no new index structure, and cannot silently lose recall
because the second pass is exact over the pool it is given.

### 14. HNSW or IVF as a *declared* approximate leaf

When it is justified — and the honest trigger is the corpus outgrowing
page cache, not a benchmark table — an approximate index enters as a
selection leaf that names its exactness domain, which `query-api.md`
already requires of every strategy. It must never be substituted for an
exact leaf silently; that is the same rule that makes the engine refuse a
truncated blend in place of a decomposed sum.

Two design constraints that should be recorded now, while nothing is
built:

- The streaming floor protocol and the completion certificates assume an
  exhaustive scan. A graph traversal cannot issue the same certificate,
  so it needs its own — a recall guarantee under stated parameters, or an
  explicit "approximate" marker on the response that propagates all the
  way to fusion. Fusing an approximate leg with an exact one produces an
  approximate result, and the response must say so.
- **Filtered ANN is where graph indexes fail and this engine's flat scan
  wins.** A mask over a flat scan is exact at any selectivity; a mask over
  a graph traversal degrades recall exactly when the filter is selective,
  which is when people use filters. Item 1 makes filtered vector search
  exact and fast today. That is an argument for keeping the flat path as
  the default forever and letting the graph serve unfiltered recall at
  scale, rather than replacing anything.

---

## E. Everything the sidecar knows, at zero query cost

The governing rule for this whole group, and the answer to "how do we
combine features with the sidecar without losing speed":

> **All NLP cost moves to ingest. The query path keeps exactly one
> sidecar call — analyze and embed the query — and never gains a second.**

Every layer the sidecar can produce becomes a *column* or a *term* at
ingest time, after which filters, facets, and score-function chains read
it for free, because they already read columns. Nothing in this group
puts a model on the query path. That also settles the standing worry in
`work-queue.md` section 4 about entity terms: a model that tags a mention
at ingest and misses it at query time breaks matching, so the model never
runs at query time.

The counterweight is constraint 1: each of these costs bytes per document
against page cache. They are listed cheapest-per-value first.

### 15. Noise and artifact scores as columns, and a quality-decay stage — LANDED

**Landed 2026-08-24 (`docs/quality-columns.md`).** As predicted, no new
scoring math: `QualitySpec` on `AddDocumentsRequest` asks the sidecar
for the noise/artifact layers in the same analysis pass, the node
reduces them to three scalars (worst noise score, union span coverage,
artifact count) and materializes them into the ordinary `numerics` /
`integers` lists before the apply — so filters, facets, decay stages,
and WAL replay all take paths that already existed. The one design
trap dodged: the layers live on the ingest request, NOT on
`AnalysisSpec`, because folding them into the analysis fingerprint
would have invalidated every shard in the fleet for a change that
cannot affect term identity.

The original rationale, kept:

The highest value-per-byte item in this document. The sidecar's noise and
artifact layers are measured **free** relative to a term-vectors-only
pass, they produce a scalar per chunk, and the corpus is known to carry
roughly 1.5% hard garbage including shift-ciphered PDF text. Eight bytes
per document buys a column that a `MULT_*` stage can demote by.

It needs no new scoring math at all: a decay in a quality score is
monotone non-decreasing in the incoming score with a computable bound
under the column's min/max metadata, which is exactly the contract every
`ScoreStage` already signs, so MaxScore, the shared floors, and
`kth_best` keep working on final scores. It is also directly A/B-able,
since `VariantSearch` carries whole requests and chain-versus-no-chain is
what it was built to compare.

### 16. Glossary and entity columns, not entity terms

The sidecar's glossary matcher is the multi-word entity answer, and its
NER layers are deployed (person plus location chosen, all seven models on
disk). Land them as **facet-ordinal columns**, not as tokens in the
postings. As a column, "opinions mentioning this organization" is a CEL
filter and a facet count at zero query cost and eight-ish bytes per
document; as tokens, it grows the one structure that is already at 6.5:1
against RAM, and it drags the analyzer's term-identity contract into
territory where a model's miss becomes a silent zero-result query.

The entity-terms-as-an-A/B-column idea from `work-queue.md` section 4
survives this as a separate, later experiment. The column comes first
because it is cheap and cannot break matching.

### 17. Wire the geography layer into the geo columns that already exist — LANDED

**Landed 2026-08-24 (`docs/geography-columns.md`).** `GeographySpec`
mirrors the quality-column shape: same-pass geocoding, reduced to best
point / top region vote / confidence, materialized into the ordinary
`geo_points` / `facets` / `numerics` lists. Absence stayed honest (a
place-less document is nowhere, never (0,0)), and the no-NER sidecar
state is refused at session open on `GetCapabilities.ner_available` —
structured evidence, not warning-string parsing — because empty layers
without that check are indistinguishable from a genuinely place-less
corpus.

The original rationale, kept:

The engine has geo-point columns (kind 5), bbox and radius filters, and
haversine and Manhattan distance-decay stages, all shipped. The sidecar
produces `GeoLocation` and `RegionVote` layers. Nothing connects them.

Connecting them gives spatial search over a corpus that has no explicit
coordinates anywhere in its source data, using two features that are both
already built and tested. This is the single best ratio of new capability
to new code in the document.

### 18. Dual-cased term identity in one analysis pass

`AnalysisResult` already carries `cased_term_vectors` alongside
`term_vectors`, and `TermVectorOptions` already has `dual_cased`. The
rebuild wanted a cased body column as a standing A/B arm and the open
question in `work-queue.md` 1.1 was whether to pay for it. Emitting both
identities from one pass is how it stops being a second pass, and the
sidecar is the rebuild's throughput ceiling, so a pass avoided is the
whole cost avoided.

---

## F. What makes it a protobuf search engine rather than our corpus's engine

### 19. Descriptor-derived mappings, increments 1 and 2 — LANDED

**Increment 2 landed 2026-08-25**: bind plus protobuf-native ingest,
all protobuf/CEL, no JSON. `NodeService.IngestMapped` is a client
stream whose first message BINDS a plan — the client's reviewed
fingerprint is required, the node re-derives locally and refuses a
mismatch naming both sides, and every landing column the shard does
not declare refuses up front in one message — and whose later messages
are the serialized protobuf documents themselves. The extractor
(`src/mapping.rs`) walks each document's wire bytes against a
field-number trie compiled from the descriptor (unknown fields skip,
merge semantics honored) and reduces them to the ORDINARY
`AddDocumentsRequest` value lists plus the vector, so the analysis
session, the column validation, the CEL materialization from the bind,
and the WAL records are the ones ordinary ingest already has — replay
never needs the descriptor. Each document's vector applies in LOCKSTEP
at the same id under the same lock (its own AddVectors WAL record); a
shard whose document leg ran ahead refuses by name. The durable
shard-level binding landed the same day: the first bind pins the shard
to (plan fingerprint, body path, materialize hash) as the kind-6 entry
of the kinded column table inside the v8 integrity envelope, plus a
WAL marker record — restarts adopt it from the file, rebinds under a
different mapping refuse by name, reshard replay carries it onto
children and refuses mixed-plan inputs, snapshot installs replace it.
Chunk scopes landed the same
day: one engine row per chunk, parent scalars and parent TEXT
denormalized onto every row, a chunk-scope body, per-chunk vectors,
required CHUNK_IDs, zero-chunk documents legitimate (the response
counts parents and rows separately), and lineage carrying the reduced
parent id so the existing parent-collapse groups mapped chunks with no
new machinery. Deliberately deferred: per-field analyzer resolution.
Pinned in `tests/mapped_ingest.rs`.

**Increment 1 landed 2026-08-25** exactly at the scoped increment:
derivation plus dry-run planning plus the fingerprint, no ingest.
`SearchService.PlanIndex` (`src/mapping.rs`) derives the plan, maps
each field onto its engine column family (repeated scalars and
OBJECT/NESTED/BINARY visibly land on FAMILY_NONE), reads protomolt's
`(index)` hints off the raw descriptor bytes (prost drops extensions;
the walk is hand-rolled), and fingerprints the canonical encoding with
the new hand-rolled SHA-256 (`src/sha256.rs`, NIST-vector-pinned). The
refusal table — ambiguous vector, missing id, chunk-scope violations,
contradictory or unsupported hints, conflicting extension declarations
— is implemented and pinned in `tests/descriptor_mappings.rs`. The
exchange contract and hint vocabulary are vendored byte-identical from
protomolt rev 74d172d9, gated by `scripts/check-vendored-protos.sh`.

The original rationale, kept:

`docs/descriptor-mappings.md` is a finished design note with nothing
implemented, and it is the item that changes what the project *is*: bring
your own `FileDescriptorSet`, get a deterministic fingerprinted index
plan, and ingest your protobuf messages as the bytes they already are —
decoded against the bound descriptor, extracted onto the same typed
column plane, with no JSON and no intermediate document model for field
types to drift through.

The reference implementation is frozen in turbovec-grpc history
(immediately before `68910cb`) and proves plan derivation, fingerprinting,
protobuf-native ingest, chunk scopes, and a durable stored format. The
port is not a drop-in: the reference *interpreted* CEL per document and
gave unset fields their proto3 defaults, and both are things this engine
refuses. Mapped fields inherit the compiled IR and the Kleene absence
rule.

Increment 1 should be derivation plus dry-run planning plus the
fingerprint — no ingest — because that is the part that has to be argued
about, and refusing to guess (one vector field, one document id, named
refusal otherwise) is the behavior the whole feature is judged on.

### 20. First-class CEL: ingest materialization and query projections — LANDED

**Landed 2026-08-25** (`docs/cel-values.md`): `cel::compile_value` is
the value front-end of the same hand-rolled compiler (arithmetic,
literals, column and map reads, `double()`, everything else refused by
name), `src/values.rs` resolves per shard with stock CEL's no-coercion
typing and evaluates per RETURNED hit; `Bm25SearchRequest.projections`
and `QueryRequest.projections` (single-lexical-leaf shape) carry the
public surface, with the filter rules for missing columns carried over
(shard-absent = exact, fleet-unknown = refused by name).
`AddDocumentsRequest.materialize` computes derived columns at ingest —
explicit target kind, materialize-then-ordinary-path like quality and
geography, WAL logs post-materialization so replay never re-evaluates.
The differential oracle (`tests/cel_values.rs`) holds the engine to
bitwise agreement with `cel-interpreter` wherever stock CEL yields a
value; the two deviations (absence for missing inputs and for integer
arithmetic errors) are documented and pinned.

The original rationale, kept:

The last item, and the one that makes the CEL investment compound. Today
CEL selects and function chains score. Two more uses of the *same*
compiler, with no new evaluation machinery:

- **Ingest-time materialization.** A mapping declares a derived value as
  a CEL expression over the document's own fields, computed once per
  document at ingest and stored as an ordinary typed column. Because the
  output is an ordinary column, filters, facets, and score chains need
  nothing new and no new bounds math. The expression text joins the
  mapping fingerprint, so changing it is an index compatibility event —
  a rebuild, never a silent behavior change.
- **Query-time projections.** Computed values per hit, compiled once per
  request by the same front end, evaluated over resolved columns after
  selection. This is a new leaf family in the IR (column reads plus pure
  scalar functions) and not a new execution model: compile once, resolve
  per shard, never interpret per document. It is also the honest answer
  to half of what people reach for wildcards and scripts for in other
  engines.

Both inherit the compiler's refusal list, and both extend the existing
differential oracle against `cel-interpreter` — which stays a dev
dependency the serving binary never links.

---

## Tiers

**Tier 1, do first.** 1 (vector-leg filters, because it is plumbing over
an upstream kernel feature and it unblocks four other things), 15 (noise
columns and a quality stage, free layer plus a stage contract that
already exists), 17 (geography into the geo columns, two shipped features
that have never been connected), 12-metrics-only (so the rest of this
list can be argued from numbers).

**Tier 2, the public surface.** 3 (the `Query` adapter), 4 (cursors), 2
(match-all browse), 7 (sort with its certificate), 8 (aggregations over
the bitmap that already exists).

**Tier 3, the identity bet.** 19 then 20. This is where the project stops
being a court-corpus engine. It is also the largest single block of work
here and should not start until tier 1 is done, because it lands on the
column plane and the column plane should be finished first.

**Tier 4, adoption gaps.** 10 (deletes — the largest single "cannot adopt
this" gap, and cheap by the removal-only argument), 5 or the bigram
column, 6 (sorted dictionary, prefix, string ranges), 9 (highlighting),
11 (collections), 12-rest (TLS, membership, quotas).

**Tier 5, vectors at scale.** 13 (the quantized cascade) whenever the
vector leg starts mattering; 14 (a graph index) only once the corpus
outgrows page cache, and only as a declared approximate leaf.
