# Ten-item build goal, 2026-09-02

Operator instructions for a long-running implementation loop on
Pipestream Search (`protomolt-search`). This is the full spec. The
loop's 4000-character goal should point here and add only extra
strictness; it must not replace this file.

Work in `/work/worktrees/turbovec-workspace`. Canonical product repo is
`protomolt-search`. Read that repo's `AGENTS.md`, `README.md`,
`docs/plans/roadmap-2026-08.md`, `docs/query-api.md`,
`docs/cel-filters.md`, `docs/immutable-segments.md`,
`docs/cluster-control.md`, `docs/embedded-mobile.md`, and workspace
`AGENTS.md` before coding. Code and tests are the source of truth when
prose disagrees.

Implement all ten items as real product behavior. No stubs, no "cheap
now / real later," no silent conversion of existing indexes, no fake
exactness, no second sidecar pass, no query-time NLP. Finish an item
(tests green, docs matching code) before starting the next unless a
listed dependency forces overlap.

Do not rebuild the CourtListener corpus, stop the live cluster, cut over
shards, delete a generation, force-push, or rewrite an existing
`turbovec-pipestream-sN` branch. Do not add AI attribution or co-author
trailers. Push only if the operator asks; if pushing, Forgejo `forgejo`
first, GitHub `origin` second. English only. Fail loud.

## Current pins and facts

Verify live. Do not trust stale prose.

- `protomolt-search` `main` is `eae9362`. In-flight branch
  `feat/segments-planner-control-plane` is `9359be0` (one commit ahead),
  on both remotes.
- `Cargo.toml` pins turbovec branch `turbovec-pipestream-s17`, not s15.
- Sidecar work is `grpc-opennlp-analysis` `main` (`b3a8a65`).
  `SOURCE_NORMALIZED_STEMS` and `dual_cased` are already on that main.
  Local `normalized-stems` and the historical worktrees
  `grpc-opennlp-unlock` / `grpc-opennlp-analysis-nlp` are not targets.
- `protomolt-search/src/analyzer.rs` still sends `dual_cased: false`.
- Glossary `PhraseSearch` already exists and must not be confused with
  item 1. Item 1 is arbitrary phrase/proximity, not another glossary.
- Do not reopen 2-bit production search, residual IVF as a production
  provider, healthy-fleet hedging, tiny scan chunks, or local BM25
  statistics. IVF screening failed; keep it out of the production pin.

## How to work

1. Merge `feat/segments-planner-control-plane` into `main` first
   (Forgejo PR, then GitHub). Item 6 builds on it. Then do the rest on a
   new branch from that merged `main`.
2. One logical commit family per item (or a small stacked set).
   Conventional commits. No drive-by refactors, no bulk format, no
   unrelated README rewrites.
3. Vendored sidecar/TEI/client-example protos are copy-only. Prove byte
   identity after any proto copy.
4. New retrieval features are removal-or-score only unless the item says
   otherwise. A shape you cannot certify is `INVALID_ARGUMENT` naming the
   reason, never a silent narrower set.
5. Page cache is the RAM budget. Cost every posting/column growth in
   bytes per document. Measure a real shard before adopting a payload
   that grows postings.
6. Query path may make at most one sidecar call: analyze/embed the
   query. Every other NLP layer is ingest-time storage. Highlighting,
   dual-case, sentence bounds, and prefix expansion must not add a
   sidecar round trip.
7. Distributed results must equal the same monolithic computation under
   the documented total order. Pin ids, scores, ranks, or explicit ULP
   bounds.
8. After each item: focused tests while developing, then `cargo test` in
   `protomolt-search` before calling it done. If you touch the sidecar,
   `./gradlew test` and `./gradlew bufLint`. If you touch the turbovec
   fork, `cargo test -p turbovec --locked` on the new chain branch. If
   you touch `turbovec-grpc`, keep the product boundary: no BM25, CEL,
   schemas, or highlighting in that crate.

## Performance anti-hacks

These are defects, not optimizations. Do not do them.

- **Do not fake phrase/slop from character offsets.** Offsets cannot tell
  a stopword from whitespace. Fields without a position payload refuse
  phrase/slop by name.
- **Do not index arbitrary shingles as the phrase implementation.**
  Glossary phrases already cover registered concepts. The first
  measurement is a **bigram column** (a bigram is a term). Positions are
  a later opt-in per field, only if the bigram measurement says slop is
  required. Price postings growth on a real shard before enabling
  positions on a corpus field.
- **Do not linear-scan the term dictionary for prefixes.** Prefix and
  string-range need an ordered dictionary (sort-at-flush, binary search
  plus bounded scan). A prefix past the expansion cap is
  `INVALID_ARGUMENT` naming the term count, never a truncated match set.
- **Do not interpret CEL per document** to get string ranges or prefixes.
  Compile once, resolve per shard to ordinal/term ranges, evaluate as
  predicates at the existing heap gate.
- **Do not call the sidecar (or re-analyze text) at query time for
  snippets.** Store sentence spans at ingest. Highlight from stored body
  text plus occurrence spans plus stored sentence bounds. Native newline
  "sentences" are not a substitute for sidecar sentence spans; if the
  ingest analyzer cannot store sentence bounds, refuse
  sentence-boundary highlighting by name rather than splitting on `'.'`.
- **Do not implement collections as a name check on one global
  vocabulary / column table / df table.** That still makes one cluster
  one corpus and it makes every query touch the wrong stats. Real
  collections are isolated score spaces. Routing fans out only to that
  collection's shards. Never merge BM25 df, avgdl, calibration, or
  vocabularies across collections. Never scan every collection and
  filter hits after the fact.
- **Do not put TLS on the UDP floor lane.** gRPC gets rustls/mTLS. UDP
  stays a typed frame plus an HMAC tag. A forged floor must not be
  applied (it would under-emit). Lost UDP never changes a successful
  result. HMAC verification is on the hot path: constant-time, no heap,
  no string compares, no JSON.
- **Do not clamp over-quota `k` or ingest.** Refuse by name with both
  numbers. Clamping is a silent correctness change.
- **Do not memcpy a mmapped vector index into a heap `Vec` on open.**
  That defeats page cache, which is the whole point. One packed-bytes
  accessor: owned `Vec` or mmap, paged blocked cache, bitwise-identical
  scores to the heap path. Do not change `.tv` encoding just to make
  mmap easier.
- **Do not silently convert single-image shards into segment catalogs at
  startup.** Conversion is an explicit operator action. Queries snapshot
  one `Arc<OpenedSegmentSet>`. Segments score with **global** live
  df/avgdl for that collection, never local per-segment stats.
- **Do not let `AUTO` pick ANN, a fixed `nprobe`, 2-bit, or residual
  IVF.** `AUTO` may resolve to `EXACT` when the live provider proves
  exhaustive completion. An unqualified ANN provider is refused by name
  until a generation-bound, benchmark-qualified policy exists. That
  policy must take requested k, filter selectivity, candidate depth, and
  provider controls. Record the resolved mode on the response. ANN fused
  with exact is approximate, and the response must say so.
- **Do not pay a second analysis pass for the cased A/B column.** One
  `Analyze` with `dual_cased=true` returns both identities. Quality and
  geography layers stay on the ingest request, not in the analysis
  fingerprint. Each BM25 field still has its own fingerprint.
- **Do not leave tonic / HTTP/2 / Tokio `net` in the embedded crate
  "because generated code shares handlers."** Item 9 is to stop linking
  unused networking. Duplex in-process I/O may remain. DNS, TCP
  listeners, UDP, and the h2 stack must not be pulled into the mobile
  link. After the trim, `cargo tree -p protomolt-search-embedded` must
  not show `h2`, `hyper`, or Tokio `net` as live deps of that crate.
  Exactness must remain the same handlers, not a second ranking
  implementation.
- **Do not treat provider mismatch as a log line.** Health / startup /
  first query preflight rejects mixed provider kind, dimension, quality
  contract, or scoring fingerprint with `FAILED_PRECONDITION` before
  search traffic.
- **Do not batch vector queries with different allowlists into one
  kernel call.** Masks take turbovec's serial path; that is already paid.
  A union mask is incorrect. `None` and an all-true mask stay different.
- **Do not grow the UDP frame, add per-query heap allocations on the
  BM25 scorer, or add per-document locks.** Collections add a routing
  key and per-collection tables, not a process-wide mutex around search.

## Test standard

Match existing style: real gRPC where the feature is an RPC, bitwise or
rank-identical assertions, refusal tests that pin the reason string,
heap-builder and mmap-reader both covered when storage changes, WAL
replay when ingest/layout changes, coordinator + node + `both`, and
native analysis plus sidecar when the feature uses analysis.

Every item needs all of:

1. **Happy path** through the public surface (`Query` / `Bm25Search` /
   `Search` as appropriate), not only a unit function.
2. **Distributed equals monolithic** for any retrieval change (two-node
   harness or existing coordinator tests). Pin hit ids and scores.
3. **Refusal table** for illegal shapes, missing payloads, unknown
   collection, expired/missing auth, over-cap prefix,
   phrase-without-positions, AUTO+unqualified ANN, mixed providers,
   collection stats mixing.
4. **Persistence round-trip:** flush, drop process, reopen mmap, same
   results. If WAL exists for the new payload, replay equals live ingest.
5. **Format/compat:** old shards without the new section still load and
   serve old queries; they refuse the new query shape by name. No silent
   upgrade on open.
6. **Isolation / no bleed:** collections do not share hits or df; auth
   does not accept the wrong token; UDP HMAC reject does not change gRPC
   results; dual-case folded vs cased fields do not pollute each other's
   terms.
7. **Performance / cost gates** that fail if a hack comes back:
   bytes-per-doc or posting-size assertion for bigram/positions; prefix
   expansion hits the cap; mmap open RSS stays far below heap-load RSS
   for the same image (follow `tests/mmap_store.rs` spirit); embedded
   `cargo tree` gate; dual-case uses one Analyze (mock or counter);
   highlighting does not call analysis on the query path.

Prefer adding cases to the existing files (`tests/query_api.rs`,
`tests/bm25_search.rs`, `tests/cel_filters.rs`, `tests/embedded.rs`,
`tests/vector_backend.rs`, `tests/mmap_store.rs`,
`tests/phrase_search.rs`) over parallel suites. New files are fine when
the feature is a new surface. Do not weaken an existing pin to make a
new feature pass.

## Task list

Do in this order.

### 0. Merge segments control plane

Merge `feat/segments-planner-control-plane` (`9359be0`) to `main` via
PR. Full `cargo test`. Existing single-image indexes keep serving. Do
not add "segments are default" in this merge; that is item 6.

### 1. Phrase and proximity (roadmap 5)

- Add a measurable **bigram column** as ordinary terms (not glossary
  `$phrase:` ids, not all shingles).
- Measure posting growth vs body-only on a real fixture shard large
  enough to see bytes/doc; record the number in the test or a short
  `docs/` note next to the feature.
- Phrase queries on a field that has the bigram column use it.
- Exact slop/proximity requires an **opt-in per-field token-position
  payload**. If the measurement does not justify positions for default
  body, still implement the payload, wire, WAL, and refuse path, and
  leave default fields without positions.
- Field without positions + phrase/slop request → `INVALID_ARGUMENT`
  naming the field. Never approximate from character offsets.
- Tests: adjacent phrase hits, non-adjacent does not, slop=1 vs slop=0,
  stopword/whitespace pair that offsets would confuse, multi-field
  only-on-opt-in-field, distributed merge, mmap reopen, WAL replay,
  glossary `PhraseSearch` still bitwise as today.

### 2. Prefix terms + sorted term dictionary (roadmap 6)

- Ordered term dictionary at flush (this is also the CEL string-range
  unlock).
- Prefix query capped; over cap refuses with the term count in the
  message.
- Enable CEL string ordering / prefix filters that
  `docs/cel-filters.md` currently refuses (`court < "b"`, and the
  documented dictionary-range case). Keep Kleene absence. Compile to
  ordinal/term ranges, no per-doc string walks.
- Still refuse `matches()`, regex, unbounded fuzzy/wildcard.
- Tests: prefix exactness vs brute-force term list on a fixture, cap
  refusal, string range vs sorted values, unordered old files refuse
  string-range by name, CEL differential oracle updated only where stock
  CEL is defined, distributed BM25 with prefix, mmap reader binary-search
  path.

### 3. Server-side highlighting (roadmap 9)

- Ingest stores sentence spans when the analyzer provides them (sidecar
  sentence layer is free at ingest).
- Query returns sentence-bounded snippets with merged overlapping
  occurrence spans. Do not ship whole documents as the highlight
  mechanism.
- No query-time sidecar. Console can consume the new fields; do not
  leave highlighting client-side only.
- Tests: overlap merge, sentence boundary vs mid-token cut, missing
  sentence spans refuse sentence mode and still allow a documented
  non-sentence window only if you implement one and name it, UTF-16
  offsets stay original-text, multi-field, WAL/mmap.

### 4. Real collections (roadmap 11)

Not a name on the request. One cluster serves many corpora.

Each collection owns: shard set / topology records, column tables,
vocabularies, analysis fingerprints, BM25 global df and avgdl, vector
calibration pair, WAL/segments. A shard belongs to exactly one
collection. Identity is operator collection name plus the
descriptor-bound plan fingerprint already stored on mapped shards.

Routing, stats, floors, fusion, facets, aggregations, and browse never
mix collections. Unknown collection → `INVALID_ARGUMENT`. Empty
collection is empty, not "the other corpus."

Tests: two collections on one node and on a coordinator; same term has
different df; a query without collection name refuses once collections
exist (or uses an explicit default only if configured, never a silent
pick); mapped ingest bind cannot write across collections; reshard/WAL
stay inside the collection; ClusterControl placement records carry
collection id; health lists collections without mixing row counts.

### 5. TLS, mTLS, bearer, UDP HMAC, quotas (roadmap 12 rest)

- rustls, operator-supplied certs. Plaintext remains for loopback tests
  behind an explicit flag; once TLS flags are set, plaintext gRPC is
  refused.
- **mTLS for cluster-internal** (coordinator ↔ node, ClusterControl
  register/lease/plan/complete). A shared bearer is not membership.
- **Bearer on public SearchService/Query** for clients. Unauthenticated
  calls fail. Wrong token fails.
- **HMAC on every UDP floor/cancel datagram**, key distinct from the
  bearer. Forged/truncated/replayed tag is ignored; gRPC twin still
  governs. Do not enlarge the packet into a parser playground.
- **Quotas** per principal: concurrency, max_k, ingest rate. Exceed →
  named refusal, no clamp.
- Tests: tls accept/reject, mTLS missing client cert, bearer interceptor,
  forged UDP floor does not lower emitted candidates vs authentic run,
  quota refusal, metrics bind still works, embedded/mobile still has no
  network TLS requirement (in-process duplex).

### 6. Segments as the default layout

After item 0: new nodes get the segment catalog unless they opt into
single-image. Startup never converts. Old single-image files still open.
Compaction/split/merge stay on the catalog path.

Tests: fresh node writes catalog; old fixture still serves; no
conversion log/path on open; query exactness across two segments vs one
image of the same rows (global df, live bitmap); control-plane compact
still conserves live rows.

### 7. Adaptive AUTO for dense search

Keep `EXACT` exhaustive. `ANN` only on a provider that actually exposes
configured ANN. `AUTO` chooses only through a generation-bound policy
object keyed on embedding model, corpus generation, row count,
dimensions, provider kind, scoring fingerprint, requested k, filter
selectivity, and candidate depth. No interpolation. No hidden `nprobe`.
Residual IVF stays out of the production pin unless a new host-valid
matrix passes the existing gates (it has not).

If no ANN provider is qualified, complete work is: policy type plus
persistence of measurement profiles plus `AUTO`→`EXACT` on exhaustive
providers plus loud refusal on unqualified ANN plus response
`dense_execution` provenance. That is the real item; a hardcoded
heuristic is not.

Tests: AUTO+TurboVec exhaustive = EXACT bitwise; AUTO+fake ANN refuses
by name; profile mismatch refuses; ANN response marked approximate;
FP32 rerank still does not upgrade ANN to global exact.

### 8. Dual-cased term identity in one analysis pass (roadmap 18)

Probe the running sidecar `GetCapabilities.dual_term_identity_available`.
Open port ≠ jar. If false, fix `grpc-opennlp-analysis` **main**, copy
proto, prove bytes. If true, do not invent a sidecar branch.

Search ingest: set `dual_cased=true` for the A/B pair, persist folded
and cased bodies as two fields from **one** Analyze, fingerprint each
field. Native analyzer: emit both identities in one pass too if native
ingest is used; do not special-case a second native pass.

Tests: one Analyze counter/mock; folded recall hits case variants that
cased field does not; spans align; fingerprint mismatch refuses; WAL
replay does not re-analyze; quality/geo still same-pass and not in the
term fingerprint.

### 9. Mobile dependency trimming

`protomolt-search-embedded` must not link tonic-transport HTTP/2, `h2`,
or Tokio networking it does not use. Keep protobuf plus in-process
duplex plus native analyzer. No-egress construction stays (no DNS, no
UDP, no sidecar URL).

Tests: existing `tests/embedded.rs` exactness still passes; a build/tree
test fails if `h2` or Tokio `net` reappears on that crate; binary/link
size assertion if you can do it stably in CI-less local tests.

### 10. mmap vector index (fork) + provider verification

- mmap is a **third turbovec fork patch** on a new immutable
  `turbovec-pipestream-sN`. Never rewrite s17. Packed-bytes abstraction,
  paged blocked cache, scores bitwise-identical to heap `Vec`. Classify
  rebuild from format code: serving existing `.tv` v5/v6 through mmap is
  not a corpus rebuild. Pin the new branch in `protomolt-search`
  `Cargo.toml` and `cargo update -p turbovec`.
- Product-owned mmap FP32 sidecar already exists; do not reimplement it.
- Provider verification: ingest/health/`GetVectorBackend` fingerprints
  must match across the fleet before queries run. Clustered backend
  included.

Tests: fork crate tests; product load mmap vs heap identical top-k;
RSS/open test; mixed fingerprint rejected before a Search/Query returns
hits; snapshot install rejects foreign provider.

## Done

All ten items implemented, not sketched. `cargo test` green in
`protomolt-search`. Sidecar/fork tests green if those trees changed.
README TODO list and roadmap items marked landed with dates only where
the code actually landed. No live-cluster cutover. No shortcuts in the
anti-hack list.
