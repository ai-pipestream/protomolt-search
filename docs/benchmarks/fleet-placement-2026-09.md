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

### The same filters as a shard allowlist

The table above sent each filter as a `FilterQuery` leaf inside a
`BooleanQuery`, which is the membership-bitmap path. The public route
also takes a filter as a clause of an AND `CompositeSearchStrategy`
around one search leaf, and that shape ships the predicate tree to the
shards, where it is an allowlist on the postings walk or the scan
(`docs/manual/03-filters.md`, "The vector branch"). Same root :19391
through the relay, same terms and vector, k = 10, one cold pass, later
the same evening:

| Query | :19391 relay | Shards (asked / skipped) | Segments (visited / skipped) |
|---|---|---|---|
| lexical "grandfathered status", `court == "scotus"` | 369 ms | 7 / 0 | 322 / 0 |
| lexical "firearm drugs payment", `year >= 2024` | 68 ms | 7 / 0 | 322 / 236 |
| lexical "qualified immunity", `year >= 2018` | 16 ms | 7 / 0 | 322 / 165 |
| lexical "qualified immunity", `year >= 2015` | 15 ms | 7 / 0 | 322 / 132 |
| lexical "qualified immunity", `year < 2015` | 66 ms | 7 / 1 (recent leaf) | 132 / 0 |
| dense row 7, `court == "scotus"` | 555 ms | 7 / 0 | scan |
| dense row 7, `year >= 2024` | 537 ms | 7 / 0 | scan |
| dense row 7, `year >= 2018` | 735 ms | 7 / 0 | scan |
| dense row 7, `year < 2015` | 258 ms | 7 / 1 (recent leaf) | scan |

The relay forwards this shape (the filter rides inside `Bm25Query` and
the packed stream), so the root answers it; `year < 2015` prunes the
relay's leaf at the root and asks six shards. Row 7 comes back first at
1.000 under `year < 2015`, as it should, since it sits in the archive.
The boolean-route hits were not kept by id in that run and the direct
roots are stopped, so the two shapes were not compared at this scale;
`tests/query_api.rs` and `tests/boolean_masked.rs` pin their agreement
on small corpora.

Filtered search over 86.6M rows is therefore a millisecond operation
on the public route. The boolean route's cost is not the filter, it is
the coordinator materializing a filter leaf's membership as an id set,
which at 66M rows is the 50 GB above. That is the design item: a filter
leaf under MUST should reach its sibling clauses as a shard allowlist,
and boolean set algebra between search clauses should stay on the shard
that holds the bitmaps, with the coordinator merging ranked candidates
only.


### The boolean shape by id, on one binary

The next morning (2026-09-06) the fleet moved to one binary: main
6141ca5 on the six archive nodes, the roots, and the Pi nodes (the
relay's 5b42636 build is the same code, since the commits between the
two touch documentation only). A direct root :19393 (the Pi nodes
listed instead of the relay, pruning on) then took the boolean shape
and the allowlist shape back to back, k = 10, with the root's peak
resident size (`VmHWM`) read after each query:

| Query | Boolean shape | Allowlist shape | Same ids | Root peak RSS after |
|---|---|---|---|---|
| lexical "grandfathered status" AND dense row 7 | 5.4 s | no such shape | | 1.5 GB |
| lexical "qualified immunity" AND dense row 7 | 9.4 s | no such shape | | 2.6 GB |
| lexical "qualified immunity", `court == "scotus"` | 866 ms | 79 ms | yes | 2.6 GB |
| lexical "qualified immunity", `year >= 2018` | 3.4 s | 47 ms | yes | 2.6 GB |
| dense row 7, `court == "scotus"` | 1.9 s | 588 ms | yes | 2.6 GB |
| dense row 7, `year >= 2018` | 39.0 s | 830 ms | yes | 13.0 GB |
| lexical "qualified immunity", `year < 2015` | 7.2 s | 66 ms (earlier run) | | 13.0 GB |
| dense row 7, `year < 2015` | 177 s | 258 ms (earlier run) | | 49.6 GB |

The top ten agree by id wherever the two shapes were run together, so
the cost is the shape, not the answer. The two AND(search, dense)
rows are the coordinator's set arithmetic over the lexical clause's
membership (600k ids for "qualified immunity") followed by the
per-shard rescore calls; the filter rows are the filter leaf's
membership crossing the wire as a bitmap and living at the root as an
id set, 49.6 GB for the 66M rows under `year < 2015`. The root was
stopped after the run.

With main 774da20 on the roots and the relay (the relay now forwards
the bitmap and rescore routes, `docs/relay-coordinators.md`), the same
boolean shapes go through the relay root :19391 and answer with the
same ids as the direct root: 897 ms, 4.9 s, 2.1 s, and 47 s for the
four filter rows above, measured while the archive re-placement split
below was using the sidecar and most of krick-1's cores, so those
times carry that load.

With main a9bf470 on every process (the boolean planner's pushdown of
5fdedf3, the dense-membership rule of 7c44e28, GPT's scoped read
contracts, the relay's fetch and fold routes), verified by checksum on
each host, the same shapes through the relay root :19391, k = 10, idle
fleet, the root's peak resident size read after each query:

| Query | Boolean shape | Allowlist shape | Same ids | Root peak RSS after |
|---|---|---|---|---|
| lexical "grandfathered status" AND dense row 7 | 623 ms | no such shape | | 24 MB |
| lexical "qualified immunity" AND dense row 7 | 421 ms | no such shape | | 24 MB |
| lexical "qualified immunity", `court == "scotus"` | 621 ms | 96 ms | yes | 24 MB |
| lexical "qualified immunity", `year >= 2018` | 667 ms | 70 ms | yes | 24 MB |
| dense row 7, `court == "scotus"` | 597 ms | 556 ms | yes | 24 MB |
| dense row 7, `year >= 2018` | 881 ms | 782 ms | yes | 24 MB |
| lexical "qualified immunity", `year < 2015` | 209 ms (relay shard skipped) | | | 24 MB |
| dense row 7, `year < 2015` | 321 ms (relay shard skipped) | | | 24 MB |

The root's resident size no longer moves with the query: the tree is
evaluated on the shards and only ranked candidates cross the wire. The
`year < 2015` rows are the first where the placement tree prunes a
shard on the boolean route (the relay over the recent group is not
asked). The lexical boolean rows still cost more than the allowlist
shape (about 600 ms against 70-100 ms): a boolean lexical clause is
scored by the candidate walk over the members, while the allowlist
shape runs the block-max search; that gap is the next thing to close
on this route. The trimmed verification (`v10-verify.py`, root against
the pruning-off root) agrees on every shape.

### The archive in year bands

The archive is one 66.4M-row default leaf. Its year histogram through
the root (`Aggregate`, `double(year)`, `year < 2015`) puts 15M rows in
2005-2014, 21.7M in 2000-2014, and 32.7M in 1990-2014, with 4.1M
before 1900. Cut into six bands of about 11M each, the generation-11
tree is `recent` (`year >= 2015`, the Pis), then `year >= 2008`,
`year >= 2000`, `year >= 1990`, `year >= 1976`, `year >= 1940`, and the
default leaf, one shard per band because `court_ingest` documents
carry no stable key and a hash-tiled leaf needs one. The offline
re-placement split (`reshard --logs=<six WALs> --placement-tree=...`,
`docs/placement.md`, "Changing the tree") replays the six archive WALs
(27 GB each), evaluates the new tree on each document, rewrites the
placement column, and writes one image per band under
`/work/court-corpus/shards-v11/archive` with a slot stride of
16,777,216 so the bands sit below the recent group's slots at
132,907,008. A first attempt with the old stride of 22,151,168 would
have put the seventh child on the Pi range and was stopped after its
routing pass (about seven minutes for the six logs, 155 GB of spill).

The second attempt wrote its spill with one bucket per child and then
replayed a 10.6M-row band into one image: 50 GB resident plus 43 GB of
swap on the 61 GB machine, stopped. The split now spills with the
sources' bucket count (64 here) and builds each child as a segment
catalog one bucket at a time (`docs/placement.md`, "Changing the
tree"): the third attempt held 10 GB resident and built a 166k-row
bucket segment in about 56 seconds, which puts the six bands at about
six hours. It stopped twenty minutes in when the sidecar closed the
connection with ENHANCE_YOUR_CALM: the bulk analysis path opened six
streams per 32-entry batch and let each go with its trailers unread,
a RST_STREAM on the wire, about 600 a second, past grpc-netty's
rapid-reset guard. With the stream drained to the server's end
(b0ee87c) the fourth attempt ran to the end: 4 h 45 min, 26 GB
resident at the peak, six bands of 10.6M to 12.2M documents in 64
segments each, 66,421,881 documents in all, the generation-10 archive
to the document. krick-1 ran at a load of 1.2 on 32 cores while it
did: the reshard's one thread at 0.6 of a core and the sidecar at
1.5, the child build a serial round trip of 32 documents at a time.

### Generation 11 on the bands

The bands serve on krick-1 (:19411-:19416) under the split's own map
(`root-map-v11.toml`, the relay on pi5v1 for `recent`) behind a root
on :19393. One trap on the way: a child of the log replay declares its
integer columns in the order its records list them (`year, placement,
decided`), the source segments in the node's flag order (`year,
decided, placement`), and a node compares a segment's tables with its
own by position, so the bands open only under
`--integer-fields=year,placement,decided`. The transplant replay
below pins the sources' tables on the children instead.

A re-placement renumbers: an archive document's id is its new band's
slot, so the id-level comparison with the generation-10 root differs
on every archive hit while the scores agree to the digit, and a
fetched document is the same text under both ids (1085983 on the old
shard 0, 39447484 on the `a2000` band, one SHA-256). The boolean shapes
through :19393 cost what they cost through :19391 (0.2 to 0.9 s, a
23 MB root). What the bands add is shard pruning on the archive, k =
10, warm:

| Filter | Generation 10 root | Generation 11 root |
|---|---|---|
| lexical, `year < 1990` | 90 ms, 7 shards, 197 of 322 segments skipped | 53 ms, 4 of 7 shards skipped, 192 segments consulted |
| lexical, `year < 1940` | 100 ms, 7 shards | 42 ms, 6 of 7 shards skipped, 64 segments |
| lexical, `year >= 2008 && year < 2015` | 49 ms, 7 shards | 58 ms, 1 shard skipped, 320 of 384 segments skipped |
| lexical, `year >= 1976 && year < 1990` | 55 ms, 7 shards | 42 ms, 4 of 7 shards skipped |
| dense, `year < 1990` | 255 ms, 7 shards | 286 ms, 4 of 7 shards skipped |

The lexical shapes halve; the dense shapes do not move, because a
dense scan's wall time is its slowest shard's and the surviving bands
still scan every segment: the year cut inside a band is what the
segment summaries need, and that is the transplant run below.

### The transplant

`reshard --from-segments --cut-column=year --cut-rows=1000000`
(12e624c, `docs/replay-from-segments.md`) replays the same six logs
with each document's analyzed fields copied from the source segments
through a per-field transpose, the analyzer never called, and cuts
each band's spill by year so the segments come out partitioned with
summaries, no compaction step. Three attempts on the archive: the
first refused shard 1 on a global-versus-local id mistake in the tail
check (fixed with a test, a6e21f4); the second, with million-row cuts,
put 42 GB into swap and was stopped (the doc now sizes the cut); the
third, at 300,000 rows per cut, spilled in 34 minutes with no analysis
and built a 300,000-row cut every 37 seconds on one thread, five bands
in 2 h 51 min, and was killed by the kernel on the sixth when the
machine ran out of memory under its other tenants (a 30B model server,
a CI container, twelve nodes). That kill also took the six
generation-10 archive nodes and the analysis sidecar; both were
restarted on the pinned binary. `--only-child` (dcc1529) rebuilds the
sixth band alone.

The sixth band was rebuilt alone (`--only-child=6`, 150,000 rows per
cut: 12,203,725 documents in 112 segments in an hour, one 33-row
unkeyed segment for the documents without a year), after two more
stopped attempts whose causes were not memory at all: the split needed
about 700 open files under the shell's 1024 (the split now sizes its
plan against the limit and the reshard raises its own, 483be73), and
every fleet process on krick-1 was being SIGKILLed whenever the last
ssh session closed, because the host had no lingering for the fleet
user and systemd tears the user manager down with everything in it.
`loginctl enable-linger` on krick-1 ended that; the Pis already had it,
which is why their nodes survived the same nights.

All six bands serve year-cut under the same map: every segment covers
one year (the early centuries grouped where years are sparse) at
150,000 to 300,000 rows, the catalogs name `year` as their partition
key, the children carry the sources' column order, and the identity
check across generations (documents paired by text through both roots,
lineage, document key, version and chunk ordinal compared) finds no
difference. Through the generation-11 root, k = 10, warm, against the
generation-10 root on the same binary:

| Filter | Generation 10 | Generation 11, six bands year-cut |
|---|---|---|
| lexical, `year >= 2012 && year < 2013` | 291 ms, 236 of 322 segments skipped | 35 ms, 321 of 326 skipped |
| lexical, `year >= 1985 && year < 1986` | 241 ms | 37 ms, 202 of 205 skipped, 4 shards skipped |
| lexical, `year < 1940` | 410 ms | 41 ms, 6 of 7 shards skipped |
| dense, `year >= 2012 && year < 2013` | 2.9 s | 79 ms |
| dense, `year >= 1985 && year < 1986` | 2.2 s | 65 ms |
| dense, `year >= 1995 && year < 1998` | 714 ms | 137 ms |
| dense, `year < 1990` | 854 ms | 257 ms, 4 of 7 shards skipped |
| dense, `year >= 2008 && year < 2015` (one whole band) | 249 ms | 239 ms |

A filter narrower than a band is where the year cut pays: the dense
scan reads only the segments whose year range the filter admits, so a
one-year filter costs a fortieth of what it did and a three-year range
a fifth; a filter that is a whole band gains nothing inside it, as
expected, and shard pruning is what it gains. The boolean shapes
through this root cost what they cost before (0.24 to 0.88 s, a 23 MB
root). The generation-10 column was taken right after its nodes
reopened, so its dense times are on the cold side.

## What remains

- Cutover: nothing was moved off the old ports. The old generation on
  :19291 and :19300-19307 is untouched (its processes did not survive
  the reboot of 2026-09-05) and the operator decides when the new one
  replaces it; the Pis still hold the old nodes' files next to the new
  ones.
- The year-band split of the archive is running again on a9bf470 with
  the segmented child layout; after it, each band serves under
  `--placement-leaf` and `--placement-tree`, the root map moves to
  generation 11, and a partitioned compaction by `year` inside each
  band gives segment pruning something to skip within a leaf
  (`docs/segment-pruning.md`).
- The boolean lexical clause's candidate walk (600 ms where the
  allowlist shape's block-max search takes 70-100 ms).
- The log replay's children declare their column tables in record
  order; they should pin the sources' order as the transplant does.
- The partitioned compaction of a served catalog that has no log (the
  split's children): the transplant fed into the shadow build,
  designed in `docs/replay-from-segments.md`.
- Control-plane leases for the scan rate.
