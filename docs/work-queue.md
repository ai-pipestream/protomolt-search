# Work queue

Written 2026-08-02. This is the engineering queue, ordered by what blocks
what, for someone picking the work up cold. Each item says what is known,
what is decided, and what is still open, because several of these have a
measurement behind them that is easy to re-derive wrongly.

Companion documents: `architecture.md` for how the system fits together,
`multi-field.md` for the index format, and the sidecar's own
`NLP-SIDECAR-ENHANCEMENTS.md` in the workspace root.

## 1. Blocking the corpus

### 1.1 The v7 rebuild, with the new corpus analyzer

Everything else about the corpus waits on this. The analyzer changed and
the index on disk has not caught up.

`analyzer::body_spec()` now applies ACCENT_FOLD, and the pinned corpus
fingerprint moved to `0x55eb_d3a6_febd_2ac3`. The reason is measured
rather than stylistic: sampling 200,000 real chunks, the corpus writes
the same surname both ways and neither spelling reaches the other's
documents. `Rodriguez` 1,114 occurrences against the accented form's 21,
`Garcia` 9 against 1,131, `Nunez` 0 against 116. 1,120 word types are
split this way.

Nothing on disk breaks in the meantime: the live v7 shards carry
`analysis_fingerprint = 0` on every field, which the query path reads as
"no claim" and accepts. The rebuild is what stamps the real value, and
from then on a query analyzed under a different spec is refused rather
than quietly scored against terms that do not exist.

DEHYPHENATE and NFKC were considered and deliberately NOT adopted, on the
same sampling: line-break hyphenation appears in 0.1% of chunks because
this corpus was converted from XML and HTML rather than extracted from
PDFs. Both are exposed as constants so an A/B arm can use them without
touching the corpus spec. A test pins the adopt/skip decision so a later
edit has to argue with the measurement.

Open: whether the rebuild also builds a second body column under the
cased analyzer as a standing A/B arm.

### 1.2 The NLP capture pass

Separate pass, separate decision, does not compete with the rebuild.

Settled: annotations are held in the repo service, not the index, and
search returns a lean hit while the full annotated record is fetched by
lineage. See `architecture.md` section 8.1.

Measured, at concurrency 32 on 1.3 KB chunks, for an 86.6M-chunk pass:

| NER models | docs/s | pass |
|---|---|---|
| person only | 5,039 | ~5 h |
| person + location | 2,235 | ~11 h |
| person + location + organization | 1,590 | ~15 h |
| all seven | 749 | ~32 h |

Chosen: person + location. All seven models are on disk at
`/work/court-corpus/models/ner-en-all/`. Nothing loads unless
`OPENNLP_NER_MODEL` names it, so the default costs nothing.

Other layers, relative to a term-vectors-only pass: sentence detection,
noise and artifacts are free within measurement noise; lemmatization is
about 1.4x; POS tagging is about 7x. Those numbers were taken through
the annotator lock and should be re-measured now that it is gone.

Two constraints that are easy to get wrong. Annotations must key on
stable document identity, meaning source document and chunk ordinal,
never the index's document id, which is a storage position that
resharding changes. And a failed annotation write must not fail the
index build but must stay visible in the ledger, so "which chunks lack
annotations" has an answer.

Tools: `examples/annotation_cost` prices layers, `annotation_throughput`
sweeps concurrency over unary and streams, `ner_probe` shows which
entity types a deployment actually finds.

### 1.3 Repo service integration

Designed, not wired. The proto shape is expected to change. The open
question recorded in `architecture.md` is the bulk-analytics access
pattern: a per-document object store answers "give me this document"
well and "count entities by court and year" badly.

## 2. Operational

Small, real, and each one makes later work more honest.

- **The live cluster is stale.** Running since 01:55 on 2026-08-02, on a
  binary that predates the day's work. It still scans the vector leg on
  BM25-only queries. A restart is a couple of minutes and makes every
  subsequent measurement trustworthy.
- **`opinions.ndjson` has no current backup.** 116 GB local, and the NAS
  copy is older and smaller. This is the one worth not leaving.
- **Port 8600 is ufw-blocked** from the browser:
  `sudo ufw allow from 192.168.0.0/16 to any port 8600`.
- **Sidecar PR 2** is open as a draft awaiting review.

## 3. Known defects, elsewhere

- `dependency_parse` and `pii` return zero annotations and no error from
  the analysis sidecar. `PiiAnnotator` is added unguarded, so this is
  independent of the locking work. A layer that silently produces
  nothing would go into a capture run and yield an empty column.
- NER throughput is parked, not solved. What is ruled out: the annotator
  lock (removed, worth 6.7x), GC (about 2 ms pauses), processor
  visibility (the JVM sees all 32), pipeline rebuilding (it is cached),
  the stream worker pool (32 workers already matches the unary path),
  and the client (four independent clients aggregate to the same
  number). What remains: throughput plateaus near 8 cores, and four
  separate processes used 18 cores to deliver slightly less total
  throughput than one process using 8. Per-core efficiency fell 58%.
  That is the signature of contention on a shared resource rather than
  anything in the service's plumbing, and it wants a profiler.

## 4. Unstarted design

- **The public search API.** The largest unstarted piece. Facets,
  categories, paging, aggregations and a stable external query surface
  are all open. What exists today is the internal gRPC surface the
  console and pipelines use.
- **Entity terms as an A/B column.** Indexing "New York" as a single
  term is worth testing, but as an extra column rather than a
  replacement for ordinary tokens. Replacing them would put NER on the
  query path and make matching brittle: a mention the model tags at
  ingest but misses at query time stops matching altogether, where
  today the parts still match independently.
- Bigram column, `terms` on `HybridHit` and `HybridLegHit`, the roughly
  95 ms gap between the hybrid vector leg and the standalone one, epoch
  stats, membership and TLS, Pi fleet redeploy.

## 5. Upstream watch

Ryan's per-block calibration (turbovec PR 457, issue 455) was closed
unmerged, so it is still not on upstream main and our chain stays based
on the branch. When it lands, roughly half the carried fork drift
becomes deletable: per-block calibration makes cross-shard score
comparability free by construction, which removes the whole seeded
calibration patch class.

The fork chain currently sits on `fd851e5` with four carried commits.
One seam worth remembering: `FORCE_SCALAR_FALLBACK` is a process-global
test flag, and upstream treats that as safe because either path returns
a correct top-k. That holds for their set-membership tests and not for
ours, which compare scores bitwise. Both now serialize on a module
mutex.
