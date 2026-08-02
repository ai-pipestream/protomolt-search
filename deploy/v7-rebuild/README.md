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

## Three things that will bite you

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

## Rollback

The previous shard set is on the NAS at
`/mnt/nas-corpus/turbovec/shards-full-8x/` (verified byte-for-byte before
deletion), and the inputs are at `/mnt/nas-corpus/turbovec/corpus/`.
Restoring the old set means restoring the old binary too — the current
engine cannot read v6 `.tv` files.
