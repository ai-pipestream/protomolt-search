# The v7 rebuild

Building a fresh shard set from the raw chunk texts and embeddings, with
block-aligned cuts and the multi-field BM25 index.

## Why this is a rebuild and not a migration

Two format breaks land together:

- **`.tv` v7** — TQ+ calibration is now per 8192-vector block. Each block
  fits and freezes its own shift/scale, so cross-shard score comparability
  is free by construction. v6 files do not load.
- **`.bm25` v6** — per-field sections sharing one slot space, so a shard
  can score `body` and `case_name` separately and fused. v5 and earlier
  hold one field.

Neither reader accepts the older file, and back-compat is deliberately not
a goal here: these formats have no external clients. The resolution for
any break is to rebuild from the inputs we keep for exactly this purpose
(`chunks-full.ndjson` + `embeddings-full.bin`, both mirrored to the NAS),
never to migrate in place.

## What you need before starting

| Input | Default | Notes |
|---|---|---|
| chunk texts | `/work/court-corpus/chunks-full.ndjson` | one JSON record per chunk, file order == id order |
| embeddings | `/work/court-corpus/embeddings-full.bin` | 12-byte header + fixed-stride records; its length defines the corpus size |
| case names | `/work/court-corpus/case-names.tsv` | `<cluster_id>\t<name>`, see below |
| analysis sidecar | `grpc-opennlp-analysis`, JVM dist | **must implement `AnalyzeStream`** |
| embedding model | `/work/court-corpus/models/minilm-l6-v2-static` | query-side embeddings only |

Export the case-name table straight from the CourtListener clusters table
(the `\t`/newline/backslash scrub is what keeps every line exactly two
fields, which is all `load_case_names` accepts):

```sql
COPY (
  SELECT id,
         translate(COALESCE(NULLIF(btrim(case_name), ''),
                            NULLIF(btrim(case_name_full), ''),
                            NULLIF(btrim(case_name_short), '')),
                   E'\t\r\n\\', '   ')
    FROM opinion_clusters
   WHERE COALESCE(NULLIF(btrim(case_name), ''),
                  NULLIF(btrim(case_name_full), ''),
                  NULLIF(btrim(case_name_short), '')) IS NOT NULL
   ORDER BY id
) TO STDOUT WITH (FORMAT text, DELIMITER E'\t');
```

Coverage on the current database: 9,833,660 clusters, 9,833,656 named
(9,753,371 from `case_name`, 80,283 from the long caption, 2 from the
short form, 4 with no name at all).

## Running it

```bash
WAVE=2 ./rebuild.sh plan       # corpus math, cut plan, disk model
WAVE=2 ./rebuild.sh up         # sidecar + N empty nodes
WAVE=2 ./rebuild.sh calibrate  # fit the seed calibration once
WAVE=2 ./rebuild.sh ingest     # the long one
WAVE=2 ./rebuild.sh down       # nodes flush on SIGTERM
WAVE=2 ./rebuild.sh serve      # nodes on the built shards + coordinator
```

`serve` will refuse to start a shard that still has a `.bm25.build`
directory with no `.bm25` beside it, because `Flush` removes that
directory on success and the pair can only mean an interrupted build. A
shard like that would come up healthy and answer every lexical query
with silence, putting a one-eighth hole in every BM25 result with
nothing anywhere saying so. If you meant it, `--allow-missing-bm25`.

Then the acceptance matrix:

```bash
cargo run --release --example v7_verify -- \
  --coord=127.0.0.1:59291 --analysis-addr=http://127.0.0.1:59202 \
  --shards=8 --offset-stride=21659648
```

## Why the cuts are block-aligned

Under per-block calibration a sealed block refits on exactly its own rows,
which makes its quantization a deterministic function of its content. So a
distributed scan is bitwise-equal to a monolithic one **only when every
shard holds whole blocks** — an arbitrary cut leaves a shard's first and
last blocks holding different rows than the monolith's, and they quantize
differently.

`rebuild.sh` derives the cuts from the corpus size: whole blocks
everywhere, the leftover tail on the LAST shard. The tail matters because
an unsealed (open) block rides its index's most recent fit; putting it on
the final shard means it rides the same last seal the monolith's tail
would. For 86,633,399 chunks over 8 shards that is 1322 blocks each for
shards 0-6 and 1321 blocks + 2999 rows for shard 7.

Slot offsets are block-aligned too (`i * 21659648` by default, 100%
headroom over the largest shard), so `global_id / 8192` stays a unique
(shard, block) pair.

## Disk: the part that actually constrains the run

Measured on the 827k-chunk canary, bytes per chunk:

| | B/chunk | 86.6M chunks |
|---|---|---|
| `.tv` vectors | 133 | 11 GB |
| `.bm25` postings + texts | 5,178 | 448 GB |
| WAL | 2,480 | 214 GB |
| **`.bm25.build` (transient)** | **7,630** | **661 GB** |

The build directory is the trap. While a shard ingests, its spilled texts
and sort runs sit on disk at roughly 1.5x the size of the `.bm25` they
become, and during `Flush` **both exist at once**. Eight shards building
simultaneously need about 1.4 TB; the finished set needs 674 GB.

`WAVE=N` bounds how many shards build concurrently, which bounds the peak:

```
peak = (SHARDS - WAVE) x finished_shard + WAVE x flushing_shard
```

WAVE=2 puts the peak at 839 GB for the full corpus; WAVE=4 at 1006 GB.
`ingest` refuses to start a wave that would leave less than
`DISK_MARGIN_GB` (default 100) free, so a bad estimate fails fast instead
of wedging the filesystem hours in. Total work is unchanged by WAVE — the
shared analysis sidecar is the throughput ceiling, not shard parallelism.

## Four things that will bite you

**A stale analysis sidecar.** If the sidecar predates the `AnalyzeStream`
RPC, the node silently falls back to one unary call per document; its gRPC
server then GOAWAYs the connection after ~70 streams and the bulk driver
dies with an opaque `h2 protocol error` a few seconds in, while the node
logs nothing and stays healthy. Check with
`strings <sidecar-bin> | grep AnalyzeStream` and rebuild with
`./gradlew installDist` if it is missing. The general lesson: a driver
that is still streaming when the server aborts sees only the h2 reset, so
reproduce with `examples/ingest_probe.rs`, which closes its send side
first and gets the real status back.

**Node ports are inside the ephemeral range.** `ip_local_port_range` is
32768-60999 here, which covers 59300-59307, so an unrelated outbound
connection can be holding a node's port at bind time and that node dies
with `AddrInUse` while its siblings come up fine. `rebuild.sh` retries the
bind; the permanent fix is
`sysctl -w net.ipv4.ip_local_reserved_ports=59290-59310`.

**The sidecar needs its embedding model for queries, not for ingest.**
Ingest takes vectors from the embeddings file, so a sidecar started
without `OPENNLP_EMBEDDINGS_DIR` builds a perfectly good index and then
cannot embed a single query. `rebuild.sh` always sets it.

**Do not rebuild the sidecar's jars while it is running.** This one was
self-inflicted mid-rebuild and is worth the warning. A JVM loads classes
lazily, so `./gradlew installDist` against a live sidecar leaves the
process serving everything it had already touched and unable to load
anything it had not. Ingest kept working for hours across six shards
because the analysis path's classes were all resident; embedding, which
ingest never calls, died with
`ClassNotFoundException: EmbeddingOptions$1` — a *classpath* error, even
though `OPENNLP_EMBEDDINGS_DIR` was correctly set and the jar on disk was
perfectly good. The failure is invisible until the first query, which is
after the rebuild has finished.

An open port therefore does not mean a working sidecar: analysis and
embedding are separate capabilities of one process, and ingest exercises
only the first. `sidecar_up` now probes embedding specifically
(`analyze_probe --embed`) and restarts a sidecar that answers but cannot
embed, rather than adopting it because the port was listening.

## A/B-ing an analysis change without a second rebuild

Any change to term identity (tokenizer, stemmer, normalizer, term source)
invalidates the BM25 index, and a second full rebuild peaks at ~840 GB
against a 674 GB finished set: you cannot hold both. That makes "just try
it and compare" prohibitively expensive at corpus scale.

Multi-field BM25 dissolves this. Every field gets its own postings over
one shared slot space, so the same body text can be indexed twice under
different analysis as two COLUMNS of one index, and the comparison
becomes a query-time choice of which field to score. Both columns see
byte-identical input and identical ids, which is a cleaner control than
two separate ingests could give.

```bash
OUT=/work/court-corpus/ab-slice SHARDS=2 \
FIELDS=body,body_norm,case_name BODY_COLUMNS=body_norm:1:2:3 \
  ./rebuild.sh up calibrate ingest down serve
```

`body` keeps the corpus spec (whitespace, porter, SOURCE_STEMS) and
`body_norm` takes SOURCE_NORMALIZED_STEMS, which runs the normalizer rung
chain before the stemmer. Query one field, then the other, over the same
documents.

Then ask the engine for the comparison rather than assembling it
yourself. `SearchService.VariantSearch` takes N labelled arms, each a
complete ordinary request, runs them one after another over the same
corpus, and returns each ranking plus the diffs against the first arm:
overlap, Kendall tau-b over the union, truncated RBO, and score regret in
the reference's own units. Read tau and regret together — a big tau
change at zero regret is the near-duplicate shuffle, while real regret
means the arm reached for worse documents. Set `interleave` with exactly
two arms to also get a team-draft merge of the two, ready to serve, with
per-position attribution so a selection credits one arm.

This is only worth serving from the engine because search here is
bitwise deterministic and layout-invariant: two arms of one query differ
only by the arm, so a single query is already an observation rather than
a sample. Where recall varies run to run, the same diff would mostly be
measuring the index's own noise.

The catch is honest: a body column is most of the postings, so each one
roughly adds the whole `.bm25` again. Run this on a slice (`SHARDS=2`
over a 1M-chunk corpus is minutes of ingest), then let the winner ride
the full rebuild alone.

Why this matters for THIS corpus: under `SOURCE_STEMS` the Porter stemmer
only transforms lower-case input, so `COURT` is indexed as its literal
surface form while `court` stems normally. A caption reading "SUPREME
COURT OF THE UNITED STATES" and the query "supreme court certiorari
united states" share exactly one term. `SOURCE_NORMALIZED_STEMS` folds
first and they share all five. That is a term-identity claim, not a
relevance claim, which is exactly what the A/B is for.

## Rollback

The previous shard set is on the NAS at
`/mnt/nas-corpus/turbovec/shards-full-8x/` (verified byte-for-byte before
deletion), and the inputs are at `/mnt/nas-corpus/turbovec/corpus/`.
Restoring the old set means restoring the old binary too — the current
engine cannot read v6 `.tv` files.
