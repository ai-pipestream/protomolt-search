# Fleet rebuild with a placement tree, 2026-09-05

The four-machine fleet rebuilt as generation 10: the corpus split into
two placement groups by filing year, the recent group on the two Pis
behind a relay coordinator, the archive group on krick-1, and a root
coordinator over a placed shard map with shard pruning on. The old
generation (ports 19291 and 19300-19307, `~/protomolt-search/shards`)
was left in place; the cutover is the operator's decision.

## Layout

| Piece | Host | Port | Directory | Binary |
|---|---|---|---|---|
| archive shards 0-5 (`year < 2015`, code 18014398509481984 = 1 << 54) | krick-1 | 19400-19405 | `/work/court-corpus/shards-v10/archive` | main 973dcf6 |
| recent shard 0 (`year >= 2015`, code 0) | pi5v3 | 19406 | `~/protomolt-search/shards-v10/recent` | main 973dcf6 (aarch64) |
| recent shard 1 | pi5v1 | 19407 | `~/protomolt-search/shards-v10/recent` | main 973dcf6 (aarch64) |
| relay over the recent shards | pi5v1 | 19390 | `~/protomolt-search/start-relay-v10.sh`, `relay-map-v10.toml` | main 5b42636 (aarch64) |
| root coordinator, shard pruning on | krick-1 | 19391 | `~/protomolt-search/start-coord-v10.sh`, `root-map-v10.toml` | main 973dcf6 |
| root, shard pruning off (comparison) | krick-1 | 19392 | same script, `NAME=noprune` | main 973dcf6 |
| root over the Pi nodes directly (relay check; stopped after the run) | krick-1 | 19393 | `root-map-direct.toml`, `NAME=direct` | main 973dcf6 |
| the direct map with pruning off (stopped after the run) | krick-1 | 19394 | `NAME=directnoprune` | main 973dcf6 |
| analysis sidecar (shared) | krick-1 | 19202 | `~/protomolt-search/sidecar` | grpc-opennlp-analysis |
| console (transcoder to the root) | krick-1 | 127.0.0.1:8610 | `bin-v10/console` | main 973dcf6 |

Inputs: `/work/court-corpus/inputs-v10/{recent,archive}` on krick-1,
partitioned from the full corpus by filing year at 2015 (the first copy
sat on krick-1's root filesystem and filled it to 100%; it was moved to
`/work`, which also holds the archive shards). Cluster metadata
`~/protomolt-search/cluster-meta.tsv` (9,833,656 clusters: filing date
and court) gave every chunk its `year`, `decided`, and `court` columns
through `court_ingest --cluster-meta`. Calibration reused from the old
generation (`shards/calibration.json`), so scores stay comparable.

Slot ranges. The archive keeps the runbook's default stride
(22,151,168 per shard, 100% headroom): offsets 0, 22151168, 44302336,
66453504, 88604672, 110755840. The recent group is contiguous, as the
relay requires of its children: `SLOT_BASE` = 6 x stride = 132,907,008,
shard 0 at 132907008 (10,108,928 rows), shard 1 at 143015936
(10,102,590 rows). The relay reports slots 132907008..153118526.

The root map (`root-map-v10.toml`):

```toml
generation = 10
[[shards]]                      # x6, one per archive node
addr = "192.168.1.195:19400"
slot_offset = 0
placement = 18014398509481984
[[shards]]                      # the relay stands in for the recent group
addr = "192.168.1.216:19390"
slot_offset = 132907008
placement = 0
[placement]
column = "placement"
level_bits = 9
[[placement.nodes]]
name = "recent"
cel = "year >= 2015"
shards = 1
[[placement.nodes]]
name = "archive"
shards = 6
```

Node flags beyond the runbook's: `--facet-fields=court
--integer-fields=year,decided --placement-column=placement
--placement-leaf=<code>`. Root flags: `--shard-map=root-map-v10.toml
--shard-pruning=true --max-k=100000 --bearer-tokens=tls/principals-v10.toml`
plus the mTLS and UDP-key material; the relay takes `--role=coordinator
--relay --shard-map=relay-map-v10.toml` and the same TLS files.

The bearer file: the current binary requires a `[policy]` block with
explicit grants (`docs/security.md`), which the old `principals.toml`
lacks, so `tls/principals-v10.toml` carries the tools principal with
`admin = true` and `actions = ["search", "ingest", "admin"]` on the
unnamed collection of workspace `court`.

## Runbook

`deploy/v7-rebuild/rebuild.sh` gained the knobs a per-group build needs
(commit 39f1997): `SLOT_BASE`, `CONTIGUOUS_SLOTS`, `CLUSTER_META`,
`NODE_EXTRA_ARGS`, `COORD_EXTRA_ARGS`, `SHARD_MAP`, `CALIBRATION`. The
env files sit next to the old ones: `fleet-v10-archive.env` and
`fleet-v10-recent-drivers.env` on krick-1, `fleet-v10-recent.env` on
each Pi. Sequence, per host, under `setsid nohup`:

```sh
# krick-1
source fleet-v10-archive.env; source fleet-tls.env
rebuild.sh sidecar calibrate up      # then, per Pi: up
rebuild.sh ingest                    # WAVE=6, ~1 TB projected peak on /work
source fleet-v10-recent-drivers.env; source fleet-tls.env
rebuild.sh calibrate ingest          # drivers here, nodes on the Pis
rebuild.sh down serve                # archive nodes (RUN_COORD=0)
# each Pi
rebuild.sh down serve
# pi5v1
./start-relay-v10.sh
# krick-1
./start-coord-v10.sh --shard-pruning=true
```

## Ingest

| Group | Rows | Drivers | Wall clock | Rows per second | On disk |
|---|---|---|---|---|---|
| archive (6 shards, krick-1 -> krick-1) | 66,421,881 | 6 in one wave | 3,130 s | 21,200 | 583 GB |
| recent (2 shards, krick-1 -> Pis) | 20,211,518 | 2 | 7,074 s | 2,860 | 104 GB per Pi |

Both groups ran at once. krick-1 held six nodes at about 8 GB anonymous
memory each (the heap tail seals at 500,000 documents), the sidecar,
and eight drivers, with 10 GB to spare of 61. A Pi node ingests at
about 1,400 rows per second with `SEAL_TAIL_DOCS=100000`. Serve-mode
open times: krick-1 1,200 s for six shards, pi5v3 584 s, pi5v1 627 s.

## Verification

Readiness and counts (`v7_verify --shards=7` against the root): 7 of 7
ready in 0 s, 86,633,399 vectors and 86,633,399 BM25 documents, every
shard finished and consistent. The relay stands in for 20,211,518 of
them. The full 14-point acceptance matrix passed against the direct map
(8 shards on :19393); against the root it stopped at the verifier's
per-shard `GetVectorBackend` probe, which the relay did not serve at the
time (fixed below for the dense route; the verifier's own probe still
goes shard by shard).

`GetShardDiagnostics` from the root: the six archive shards report
layout `segments`, 22 segments each, `has_placement` true with code
18014398509481984, segment pruning on. The relay shard reports
`Unimplemented`: the relay does not compose the diagnostics node route
yet. `GetMetricsSnapshot` counts the routes taken so far
(`turbovec_requests_total` by rpc). `ClusterHealth` lists all seven
targets reachable with one scoring fingerprint (`fe22e151...`).

The console (`bin-v10/console --coordinator=192.168.1.195:19391`)
transcodes a dense `Query` to the root in 1.1 s cold; `/api/config`
answers with the body spec, the methods, and the TLS and bearer state.

### What the relay composes, and what it does not

The relay forwards `StreamSearch`, `TermStats`, `Health`, the keyword
leg (`Bm25Query`, `Bm25PhraseQuery`, `Bm25QueryStream`, `Bm25Rescore`,
`ShardLegs`), and, since 5b42636, `GetVectorBackend`: the root's dense
preflight calls it on every shard before a public query scores anything,
so a relay that did not serve it blocked every dense query through the
root. The relay answers with the descriptor and configuration its
children share, rows summed, and errors by name when a child differs.

Not composed: the bitmap routes (`ResolveLexicalBitmap`,
`ResolveVectorBitmap`, `ResolveFilterBitmap`) and the diagnostics node
route. A boolean `Query` with a `FilterQuery` leaf resolves the filter
as a per-shard bitmap, so every filtered query through the root on
:19391 fails with `Unimplemented` naming the route. Filtered queries
were therefore measured on the direct map (:19393 and :19394), where
the placement tree and shard pruning apply exactly as they would over
the relay once those routes compose.

### Queries

k = 10, `profile = true`, one cold pass; the dense vectors are corpus
rows 7, 300, and 900 (each finds itself at score 1.000). Root :19391
goes through the relay; :19393 lists the Pi nodes directly with shard
pruning on; :19394 is the same map with shard pruning off. Hits (ids,
scores, order) were identical across every root that answered, in every
case. Times are the coordinator's `total_ms`.

| Query | :19391 relay | :19393 direct, pruning | :19394 direct, no pruning | Segments (visited / skipped) |
|---|---|---|---|---|
| lexical "qualified immunity" | 19 ms | 12 ms | 34 ms | 322 / 0 |
| lexical "grandfathered status" | 284 ms | 9 ms | 9 ms | 322 / 0 |
| lexical "firearm drugs payment" | 416 ms | 73 ms | 68 ms | 322 / 0 |
| lexical "grandfathered status", `court == "scotus"` | route not relayed | 670 ms | 689 ms | 644 / 0 |
| lexical "firearm drugs payment", `year >= 2024` | route not relayed | 16.7 s | 16.9 s | 644 / 236 |
| lexical "qualified immunity", `year >= 2018` | route not relayed | 54.1 s | 56.0 s | 644 / 165 |
| lexical "qualified immunity", `year >= 2015` | route not relayed | 73.5 s | 73.3 s | 644 / 132 |
| lexical "qualified immunity", `year < 2015` | route not relayed | 35.7 s | 38.5 s | 454 / 0 with pruning (recent leaf skipped); 644 / 190 without |
| dense row 7 | 286 ms | 252 ms | 265 ms | scan |
| dense row 300 | 255 ms | 256 ms | 253 ms | scan |
| dense row 900 | 256 ms | 253 ms | 252 ms | scan |
| dense row 7, warm repeat | 294 ms | (stopped) | 255 ms | scan |
| dense row 7, `court == "scotus"` | route not relayed | 8.7 s | 10.2 s | 322 / 0 |
| dense row 7, `year >= 2024` | route not relayed | 11.7 s | 17.5 s | 322 / 236 |
| dense row 7, `year >= 2018` | route not relayed | 36.0 s | 41.4 s | 322 / 165 |
| dense row 7, `year < 2015` | route not relayed | coordinator OOM-killed | 192 s | 322 / 190 |

Readings.

- Unfiltered dense through the relay costs the same as the direct
  fan-out (the relay forwards the packed stream untouched): 255-294 ms
  either way, over 86.6M rows on eight nodes.
- Unfiltered lexical through the relay adds the relayed keyword leg's
  round trips: 284-416 ms cold against 9-73 ms direct for terms the Pis
  had not paged in; "qualified immunity", asked earlier, took 19 ms.
- Shard pruning shows in the segment counters: with `year < 2015` the
  recent leaf is not visited at all (454 segments seen instead of 644),
  while without pruning the two Pi shards are visited and their 190
  segments ruled out by summaries. The mirror case `year >= 2015` does
  not skip the archive, which is the default leaf (no predicate of its
  own to contradict).
- The filtered boolean path is the problem at this scale on the 973dcf6
  binary: the filter travels as a per-shard bitmap and becomes a
  coordinator-side id set, so cost and memory follow the match count.
  A court filter (small set) costs 0.7 s lexical and 8.7 s dense (the
  dense clause resolves the full 86.6M-row vector membership first);
  `year >= 2018` costs 36-56 s; `year < 2015` (66M rows) took 192 s
  and, on the pruning-on instance, grew the coordinator to 49.9 GB
  anonymous memory until the kernel killed it (`oom-kill` at 18:47,
  pid 126306). The nodes were untouched. Main after 49ea0f6 no longer
  resolves the dense membership over the wire; the id-set arithmetic is
  the next measurement, and the two comparison coordinators were
  stopped after the run to give the memory back to the nodes.


## What remains

- Cutover: nothing was moved off the old ports. The old generation on
  :19291 and :19300-19307 is untouched and can be stopped by the
  operator once the new one is accepted; the Pis still run the old
  nodes from `~/protomolt-search/shards` next to the new ones.
- Relay composition of the bitmap routes and the diagnostics node
  route, so filtered queries and shard diagnostics go through the root
  over the relay (docs/relay-coordinators.md, "What is not composed yet").
- A partitioned compaction per shard: the segments sealed in ingest
  order, so the year summaries overlap and segment pruning has little to
  skip inside a leaf (`docs/segment-pruning.md`).
- The filtered boolean path on this binary is slow at 86M rows: the
  match set travels as a bitmap and becomes a coordinator id set per
  query (35-75 s for a year range over tens of millions of rows, 1 s for
  a court filter). Main after the dense-mask merge (49ea0f6) changes the
  candidate scoring; the coordinator-side set arithmetic is the next
  measurement.
- Placement pruning only excludes a leaf whose own predicate
  contradicts the filter: `year < 2015` skips the recent leaf, but
  `year >= 2015` does not skip the archive, because the archive is the
  default leaf and carries no predicate of its own.
- Control-plane leases for the scan rate, and a uniform binary once the
  root and nodes move to the current main (the relay alone runs 5b42636;
  the proto is unchanged between the two, comments aside).
