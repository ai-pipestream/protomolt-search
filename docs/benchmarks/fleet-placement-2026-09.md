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

| Filter | Generation 10, warm | Generation 11, six bands year-cut |
|---|---|---|
| lexical, `year >= 2012 && year < 2013` | 42 ms, 236 of 322 segments skipped | 35 ms, 321 of 326 skipped |
| lexical, `year >= 1985 && year < 1986` | 49 ms | 37 ms, 202 of 205 skipped, 4 shards skipped |
| lexical, `year < 1940` | 100 ms | 41 ms, 6 of 7 shards skipped |
| dense, `year >= 2012 && year < 2013` | 160 ms (2.9 s cold) | 79 ms |
| dense, `year >= 1985 && year < 1986` | 244 ms (2.2 s cold) | 65 ms |
| dense, `year >= 1995 && year < 1998` | 714 ms cold | 137 ms |
| dense, `year < 1990` | 255 ms | 257 ms, 4 of 7 shards skipped |
| dense, `year >= 2008 && year < 2015` (one whole band) | 249 ms | 239 ms |

A filter narrower than a band is where the year cut pays: the dense
scan reads only the segments whose year range the filter admits, so a
one-year dense filter costs a half to a quarter of the warm
generation-10 shape and a twentieth to a fortieth of its cold one; the
lexical shapes were already cheap on generation 10 through the
per-column summaries and gain less. A filter that is a whole band, or
a wide range across bands, gains nothing inside a band, as expected,
and shard pruning is what it gains. The boolean shapes through this
root cost what they cost before (0.24 to 0.88 s, a 23 MB root).

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
- The boolean lexical clause's MUST filter leaf now resolves over its
  narrower exact siblings' members instead of every row of the shard,
  and an empty MUST intersection short-circuits before the dense
  membership. Fleet-proven on 2026-09-07/08 (the rollout section
  below): the lexical+filter boolean shapes fell 532 to 59 ms and 619
  to 130 ms warm over 86.6M rows with bitwise-identical answers, and
  the dense+filter surcharge the first roll introduced is closed.
- The log replay's children declare their column tables in record
  order; they should pin the sources' order as the transplant does.
- Control-plane leases for the scan rate.

## The backfill proof (2026-09-07)

`docs/derived-columns.md` on the court data: one generation-10 archive
shard (shard 5, 22 sealed segments, 11,068,537 live documents) was
re-placed under the generation-11 tree from its segments with the
declaration `year_d = calendar.year(decided)` computed on each document
(`reshard.cea2c63 --from-segments --only-child=6 --derived-columns
--derive=year_d`), building only the archive child (the tree's default
leaf: `year` before 1940, plus the undated rows the band predicates
cannot place).

| Measure | Value |
|---|---|
| documents transplanted and derived | 11,068,537 |
| child built (the archive child) | 1,954,825 documents, 64 segments |
| elapsed | 12 min 24 s |
| peak resident set | 41.5 GB |
| documents with `decided` | 1,954,816 |
| documents with `decided` and no `year_d` | 0 |
| documents with `year_d` and no `decided` | 0 |
| years checked (exact filtered counts, one per year) | 234 |
| documents with `year_d` unequal to the stored `year` | 0 |

The check ran through a one-shard root over the served child (node
`--derived-columns`, root over a map with the `[[derived]]` table):
per year, the count of `year == y && year_d == y` equals the count of
`year == y`. The few documents without `decided` have no `year_d`, the
documented absence. Projections read the column like any other
(`year`, `year_d`, `decided` under a 1930 filter agree on each hit),
and `hash.fnv64(court)` at query time is turned away by name. The
child's log manifest records the fingerprint, the `[[derived]]` table
and the complete column table; the shard map the tool wrote has the
table too.

The peak resident set is file-backed, not allocated: the source
segments mapped and read once, the spill written and read back. The
anonymous memory of the run peaks at 2.8 GB (measured below, "The
transplant's memory"); the box had just lost the serving nodes to a
kernel out-of-memory event (03:27), so the run had the memory to
itself.

### Reconciling the proof child (2026-09-07)

The per-year counts above check one column. `examples/reconcile.rs`
(`docs/derived-columns.md`, "Reconciling a child") reads the child
back against the source's 22 sealed segments document for document:
each live source row goes through the split's own transformation
(rederived under the declaration, the placement code rewritten) and
is digested with its text, lineage, identity, original source, every
column and its FP32 vector; each child row is digested the same way;
the two multisets must be equal. Two checks stay outside the
declaration's evaluator: `year_d == year` (the corpus's own year) and
`year_d` equal to the civil year of `decided` counted day by day from
1970.

| Measure | Value |
|---|---|
| live source rows read | 11,068,537 |
| routed to the archive leaf by the tree | 1,954,825 |
| child rows (64 segments, 0 deleted) | 1,954,825 |
| matched by digest | 1,954,825 (0 unmatched either way) |
| rows with `decided`: `year_d` equals the civil year, and the stored `year` | 1,954,816 of 1,954,816 |
| rows without `decided` | 9, none with `year_d` |
| child tables | the source's, `year_d` appended; the declaration on all 64 segments |
| wall, 8 threads | 209 s |

The 9 rows without `decided`, identified read-only against the child
(`examples/dump_missing.rs`, which walks the catalog and prints every
live row lacking the column), are the complete chunk sets of two
source opinions whose cluster ids are absent from the source cluster
metadata, so the rows carry no `court` facet, no stored `year`, and no
`decided`: CourtListener opinion 10297883 (cluster 9831271), five
chunks of one District of Nevada order, and opinion 10658737 (cluster
10192143), four chunks of another. They sit in the archive leaf
because the tree's bands all predicate on `year`, which is absent for
them, so the default leaf takes them; `calendar.year(decided)`
computes absence, so they store no `year_d` — the documented absence
rule, not a derivation error.

The limits of the check: it covers what the store reconstructs (the
columns, the stored text, lineage, identity, original source, the
FP32 row) and not the postings themselves, which the transplant copies
as transposed spans and the served answers cover (`tests/replay_from_
segments.rs`, bit for bit against the re-analyzed split); the civil
year is the only derived expression with an independent form here, a
declaration with another expression gets the digest check and
`--equal` against a stored column; and sealed segments carry no stable
routing keys, so a leaf tiled by them (more than one shard) is not
reconciled from segments.

### The transplant's memory (2026-09-07)

The 41.5 GB "peak resident set" of the proof run was measured with
`/usr/bin/time`; it counts the mapped pages of the source segments and
the page cache of the spill, not allocations. The same run inside a
transient systemd scope (`systemd-run --user --scope -p
MemoryAccounting=yes`, the cgroup's `memory.stat` sampled every 5 s):

| Run (`reshard --from-segments --only-child=6 --derive=year_d`) | Wall | Anonymous peak | Resident peak | Scope peak |
|---|---|---|---|---|
| cea2c63, no limit | 11 min 45 s | 2.8 GB | 34.9 GB | 55.3 GB |
| cea2c63, `MemoryMax=8G` `MemorySwapMax=0` | 12 min 2 s | 2.8 GB | 7.9 GB | 8.0 GB, swap 0 |
| page release, no limit | 11 min 51 s | 2.8 GB | 14.9 GB | 26.7 GB |
| page release, `MemoryMax=8G` `MemorySwapMax=0` | 11 min 50 s | 2.8 GB | 9.7 GB (pages the serving nodes had charged elsewhere) | 8.0 GB, swap 0 |
| page release, `--build-threads=4`, `MemoryMax=8G` `MemorySwapMax=0` | 8 min 5 s | 3.8 GB | 8.8 GB | 8.0 GB, swap 0 |
| 16305a3, one thread, `--build-memory=8192`, `MemoryMax=8G` | 11 min 48 s | 2.8 GB | 8.4 GB | 8.0 GB, swap 0 |
| 16305a3, `--build-threads=4`, `--build-memory=16384`, `MemoryMax=8G` | 8 min 12 s | 3.8 GB | 8.4 GB | 8.0 GB, swap 0 |

Every run's catalog is byte for byte the proof's (sha256 over the 321
files of `shard-6.tv.segments`); the two 16305a3 rows are the bounded
build (a queue of sealed buckets between the workers and the ordered
appender, the memory budget checked against the spill's per-bucket
counts) and they match the unlimited threaded row above. Throughput is
the source read plus the child build: 11.07 million rows read and 1.95
million sealed in about 12 minutes, 15,700 source rows per second
single-threaded; four build threads seal the child in 8 min 5 s inside
the same 8 GiB budget (the routing pass is unchanged, the child build
goes from about 6.5 minutes to under 3; anonymous peak 3.8 GB, one
bucket's replay per thread).

The budget rule is now the tool's, not the operator's:
`--build-memory=<MiB>` is checked after the routing pass against the
spill's own counts at the conservative 70 KiB a row
(`docs/replay-from-segments.md`). The same proof input with
`--build-threads=4 --build-memory=4096` ran the routing pass (5 min
47 s) and then refused, before any bucket was built:

```text
child 6 (archive) bucket 31: 30922 documents at 70 KiB a row is about
2114 MiB, and 4 build threads need 8456 MiB at once, over
--build-memory=4096 MiB; lower --build-threads, raise the budget, or
cut finer (--cut-rows) -- the thread count is not lowered for you
```

(The estimate is about twice the 1 GB a 30,000-row bucket actually
replays into, which is the point of taking the band's conservative
end: the 16 GiB row above passes the check while the 8 GiB scope does
the real bounding.)

### The boolean lexical clause: where the 600 ms is (2026-09-07)

"What remains" above named the boolean lexical clause's candidate walk
(600 ms against the allowlist shape's 70-100 ms). Measured offline on
the proof child (1,954,825 rows, 64 segments, krick-1, third warm
round, `examples/boolean_walk.rs`), the shard-side phases in
milliseconds:

| shape | members | lexical membership walk | filter column scan | scorer, candidate walk | scorer, postings walk | top-k |
|---|---|---|---|---|---|---|
| `grandfath` (df 591) | 591 | 0.0 | | 0.0 | 0.0 | 0.0 |
| `court` (df 815,750) | 815,750 | 1.4 | | 17.9 | 11.2 | 3.9 |
| `court`, `year >= 1900` | 545,077 | 1.5 | 14.9 | 11.6 | 8.5 | 2.9 |
| `qualifi immun` | 26,001 | 0.1 | | 1.0 | 0.6 | 0.3 |
| `qualifi immun`, `year >= 1900` | 17,068 | 0.1 | 14.7 | 1.0 | 0.9 | 0.2 |

With the filter-leaf narrowing landed (the filter phase reads the
column over the narrower MUST sibling's members only;
`examples/boolean_walk.rs` runs it by default, `--filter-scan=full`
is the A/B baseline, and the old binary `boolean_walk.wt1f2b706`
re-ran for the identical conditions), third warm round on the same
proof child, milliseconds:

| shape | members | filter scan before | filter scan after | total before | total after |
|---|---|---|---|---|---|
| `grandfath`, `year >= 1900` | 405 | 14.5 | 0.0 | 15.3 | 0.8 |
| `court`, `year >= 1900` | 545,077 | 14.6 | 6.7 | 31.1 | 23.3 |
| `qualifi immun`, `year >= 1900` | 17,068 | 14.6 | 0.4 | 15.8 | 1.5 |

The gain tracks the sibling's width: `grandfath`'s 591 postings leave
405 rows to test, `court`'s 815,750 leave over half a million, so the
common-term shape keeps most of its scan while the selective shapes
drop to nothing. The members, the ranked page, and the scores are
identical in every round (the old binary's `top:` lines diff clean
against the narrowed ones; peak resident memory is unchanged at 6.9
GB, the shard's own pages). The node side applies the same rule in
`evaluate_boolean_membership`: a filter leaf the tree reaches only
under MUST, beside exact (lexical or dense) siblings, resolves over
the intersection of those siblings' memberships narrowed by every
ancestor MUST bound; a SHOULD or MUST_NOT filter, a filter without an
exact MUST sibling, and a leaf shared between a bounded and an
unbounded position keep the whole-shard scan. Membership stays
exhaustive — no row outside the bound can satisfy the group, so the
verdict there is never consulted. `tests/boolean_filter_domain.rs`
pins the two modes identical over fixed and randomized trees,
tombstones, vector gaps, and root aggregates;
`PIPESTREAM_SEARCH_BOOLEAN_FILTER_FULL_SCAN` forces the old scan.

The candidate walk costs at most 18 ms on this hardware even over
815,000 candidates; the filter leaf's column scan (about 15 ms per 2
million rows, every row of the shard) is the largest shard-side phase
in a filtered shape. The change that landed (`bm25::score_candidates_
plain` picks per term between positioning the impact cursor on each
candidate and merge-joining the term's doc run against the candidates,
by df against the candidate count, each candidate taking at most one
contribution per term in term order so the scores are bitwise the
same; the shard route's accumulators sized to the members instead of
the shard's rows) is sound and measurable offline (`court` 17.9 to
11.2 ms) and flat end to end on one krick-1 shard (eight shapes through
the proof root, before 3.0-83.3 ms, after 2.8-94.3 ms, ids and scores
identical over 1,070 hits; `tests/blockmax.rs` holds the bitwise
equality over random corpora and a three-part catalog with a heap
tail).

Where the fleet's 600 ms is, measured on the recovered fleet with the
same binary (a9bf470) through three roots, warm:

| shape | all 7 shards (`:19391`) | the 6 krick-1 shards only | plain shape, krick-1 only |
|---|---|---|---|
| lexical "qualified immunity", `court == "scotus"` | 568 ms | 168 ms | 56 ms |
| lexical "qualified immunity", `year >= 2018` | 651 ms | 47 ms (no rows) | 37 ms |
| lexical "qualified immunity" AND dense row 7 | 425 ms | 210 ms | |
| dense row 7, `court == "scotus"` | 600 ms | 174 ms | 182 ms |

Two thirds of the boolean lexical shape is the two Pi shards (20
million rows of the recent group behind the relay), where the filter
leaf's column scan over every row costs what the walk never did; the
krick-1 third is the scan too (the plain shape reads the column only
for the postings' candidates and prunes segments by their summaries,
the boolean shape resolves the filter leaf over the whole shard before
the group rule). The narrowing above attacks exactly that scan; the
fleet table has not been re-run against it.

## 2026-09-07: the out-of-memory event and the recovery

At 03:27 krick-1 (61.4 GiB, 64 GiB swap) ran out of memory. The
kernel's reports (`journalctl -k`, eight dumps between 03:27 and
03:42) show the memory in one cgroup: `system.slice:docker:x6zmx...`,
a BuildKit build inside a privileged `docker buildx` builder
(root, no memory limit), about 2,000 concurrent `cc1plus` processes
of 100-180 MB each, `memory.peak` 59.5 GiB; the machine's other
tenant (the `protomolt-glimmer-vllm` container) has a recorded peak
of 45.2 GiB. `systemd-oomd` did not act (its swap threshold is 90 %,
swap use peaked near 33 GB). The kernel chose its victims by badness,
which counts swapped pages: the search nodes (65 GB of virtual memory
each, most of it mapped segments, the idle heap swapped) went first,
then `llama-server`, the vLLM engine and `uvicorn`, then the build's
own compilers. Every search process in the user slice was gone by
03:42; the Pi nodes and the relay were untouched.

The budget the fleet came back under: a node's anonymous memory is
2-35 MB (the rest is mapped segments), a root's under 100 MB, the
analysis sidecar's JVM 8 GB at most. Each group runs in a transient
user scope with `MemoryAccounting=yes`, `MemoryLow=8G`,
`MemoryHigh=20G` and `MemoryMax=28G` for the six generation-10
archive nodes (`search-v10.scope`) and the six generation-11 bands
(`search-v11.scope`), `MemoryMax=6G` per root, so the fleet's own
footprint (cache included) is bounded and reclaimed inside its scopes
before it presses on anything else. The limit of the budget: it
bounds the fleet, not the tenant; a root build with no limit peaks at
the machine's size again and the kernel's choice of victim does not
change. `docker update --memory` on the builder container is the
tenant's call.

The recovery, in order, each step verified before the next: the
sidecar (already back, `:19202`); the six band nodes
(`serve-v11-fs.sh up` in its scope; `readlink /proc/<pid>/exe` and the
md5 of the running text against `bin-v11/pipestream-search.a9bf470`,
`cf03f4bd…`, on every process); the generation-11 root `:19393`; the
generation-10 nodes (`rebuild.sh down serve`, 1,192 s to open); the
roots `:19391` and `:19392`. Counts and partitions through
`DiagnosticsService/GetShardDiagnostics`: generation 11 reports the
six bands at 10,636,780 / 11,088,325 / 10,973,089 / 10,737,302 /
10,782,660 / 12,203,725 live rows with their year ranges (2008-2014,
2000-2007, 1990-1999, 1976-1989, 1940-1975, 202-1939; 39 to 112
keyed segments each) and the relay at 20,211,518, 86,633,399 in all;
generation 10 reports the same total over its six hash shards. The
cross-generation identity pairing (`v11-identity-check.py`, 79
documents, 237 fields) has 0 problems; the band-filter shapes on
`:19393` and the boolean shapes on `:19391` answer with the ids of the
tables above (boolean lexical with `court == "scotus"` 568 ms against
the plain shape's 95 ms, the lexical `year >= 2018` pair 651 against
47 ms, the same top ids on each pair).

## 2026-09-07: the boolean rollout, prepared (cutover awaits authorization)

The narrowed MUST filter leaf (`1a74f97`, "The boolean lexical clause"
above) is proven offline; the fleet proof waits on an authorized
cutover of the serving binary. Everything short of the cutover is done.

**The build.** Current `main` `92b11fb`, staged on krick-1 as
`bin-v11/pipestream-search.92b11fb` (md5 `a600b23b…`); the serving
fleet is verified at `a9bf470` (md5 `cf03f4bd…`, all 15 processes).
The new binary carries the filter-domain narrowing plus the merged
foundations work; the A/B switch
`PIPESTREAM_SEARCH_BOOLEAN_FILTER_FULL_SCAN` forces the old whole-shard
scan on the new binary.

**Reversibility, from the code.** A serve-only roll to `92b11fb` is
reversible to `a9bf470` without touching shard data:

- WAL manifests of populated generations are not re-stamped on open
  (the re-stamp fires only when the manifest already declares derived
  columns or a column table, `src/wal.rs`); an EMPTY generation would
  be stamped format 8 and refused by the old binary, so the runbook
  checks for empty `gen-*` dirs first. The fleet's generations are all
  populated.
- No ingest during the window: new-feature appends (index policy,
  derived, integer map, identity) are what bump the WAL format past
  what `a9bf470` reads. Flushes and seals trip no gate.
- A segment sealed during the window stays within the column kinds the
  old binary knows, because the fleet's mapping declares no integer
  maps, derived columns, or index policies (the kinds added by the
  merge are emitted only when those are present).
- The replay journal has no production caller on this main; the
  coordinator persists nothing versioned; the turbovec pin
  (`turbovec-pipestream-s20`) and the `.tv`/`PMEXACT1` constants are
  unchanged between the two builds.

**The driver.** `examples/fleet_boolean.rs` runs the documented shapes
through a root over mTLS, k = 10, printing per-round wall time, the
profile's selection/total ms and segment/shard pruning counters, the
hits as `doc_id:score_bits:rank`, and per-shape FNV-1a digests over
every round's hits. Equal digests are the answer-equality gate; the
boolean and allowlist variants of one filter already digest equal on
both roots.

**Before, `a9bf470`, warm (round 3 of 3), from krick:**

| shape | all 7 shards (`:19391`) | the 6 krick-1 shards (`:19394`) | Pi/relay share |
|---|---|---|---|
| lexical "grandfathered status" AND dense row 7 | 308 ms | 136 ms | 172 ms |
| lexical "qualified immunity" AND dense row 7 | 387 ms | 175 ms | 212 ms |
| lexical "qualified immunity", `court == "scotus"` | 532 ms | 130 ms | 402 ms |
| lexical "qualified immunity", `year >= 2018` | 619 ms | 8 ms (no rows) | 611 ms |
| dense row 7, `court == "scotus"` | 565 ms | 139 ms | 426 ms |
| dense row 7, `year >= 2018` | 848 ms | 15 ms | 833 ms |
| allowlist "qualified immunity", `court == "scotus"` | 51 ms | 18 ms | 33 ms |
| allowlist "qualified immunity", `year >= 2018` | 10 ms | 1 ms | 9 ms |

Roots' VmHWM 24 MB before and after the runs; the `search-v10` scope
sits at 15.2 GB current / 21.5 GB peak. Digests on `:19391`:
`lex-qualified+scotus` = `9ed31c7d…`, `lex-qualified+year2018` =
`3deb03a0…`, `dense+scotus` = `0b08d071…`, `dense+year2018` =
`67b67745…`, the two AND shapes `f8cc2722…` / `50dafed0…`.

**The cutover plan, when authorized.** krick-1 first, Pis second,
roots last, one scope at a time, each step verified before the next:
(1) confirm no empty `gen-*` WAL dir on any shard; (2) `rebuild.sh
down`, swap `bin-v11/pipestream-search` to the staged file by rename
(the old file stays as `pipestream-search.a9bf470`), `rebuild.sh
serve`, wait for health on every shard; (3) the same on pi5v1 and
pi5v3 with their binary built on pi5v1 from the pinned commit; (4)
restart the three roots and the krick-only root on the new binary;
(5) re-run the driver against `:19391` and `:19394`, compare digests
per shape first, then warm and cold (first round after the restart)
latencies and the pruning counters; (6) read every scope's
`memory.stat` and each root's VmHWM. Reversal is the same procedure
with the old file renamed back; the conditions above keep the shards
untouched either way. Any digest mismatch stops the roll at the step
that introduced it, and the discrepancy is investigated before any
improvement is claimed.

**After, `92b11fb`, measured 2026-09-07 23:25-23:31 under operator
authorization.** The roll followed the plan above: roots stopped,
`rebuild.sh down`, binary swapped by rename (the old file stays as
`pipestream-search.a9bf470.serving`), `serve` (1,179 s), the Pis on an
aarch64 build of the same commit (pi5v3 then pi5v1 with the relay), the
three roots last. Every node process verified by `/proc/<pid>/exe` md5
against the staged file. The v11 bands and their root stayed on
`a9bf470` and are not part of this measurement. Every shape's digest
matches the before run on both roots — ids, scores, and ranks bitwise
identical over all 240 hits.

| shape (warm, round 3) | all 7 shards before | after | krick-1 only before | after |
|---|---|---|---|---|
| lexical "grandfathered status" AND dense row 7 | 308 ms | 295 ms | 136 ms | 107 ms |
| lexical "qualified immunity" AND dense row 7 | 387 ms | 377 ms | 175 ms | 151 ms |
| lexical "qualified immunity", `court == "scotus"` | 532 ms | **59 ms** | 130 ms | **23 ms** |
| lexical "qualified immunity", `year >= 2018` | 619 ms | **130 ms** | 8 ms | 10 ms |
| dense row 7, `court == "scotus"` | 565 ms | 588 ms | 139 ms | 155 ms |
| dense row 7, `year >= 2018` | 848 ms | 896 ms | 15 ms | 31 ms |
| allowlist "qualified immunity", `court == "scotus"` | 51 ms | 49 ms | 18 ms | 18 ms |
| allowlist "qualified immunity", `year >= 2018` | 10 ms | 10 ms | 1 ms | 1 ms |

The lexical+filter boolean shapes — the ones whose cost was the filter
leaf's whole-shard column scan — drop 5.7 to 9 times on the krick-1
shards and 4.8 to 9 times across the fleet, landing at the allowlist
shape's cost (59 ms against 49 ms), because the filter now reads its
column only over the lexical sibling's members. The AND-dense shapes
gain a little (the filter leaf is absent there; the residual is the
dense scan and the coordinator). The two dense+filter shapes gain
nothing and pay a small, stable surcharge — +23 to +48 ms on the full
root, +16 ms on the krick-1 set where the filter matches no rows at
all: the new evaluation order resolves the exact leaves first, so the
dense membership bitmap (rows with a vector, a per-shard structure the
segment pruning does not shrink) is paid even when the filter leaf
would have emptied the group first. An empty-MUST short-circuit is the
fix; the allowlist controls (unchanged route) hold at 51/49 and 10/10,
so the conditions are comparable. Cold rows (first round after the
restart) have no before counterpart on the previously warm fleet; the
after cold rows run 1.1 to 11 times the warm figures
(lex-grandfathered+dense 3,295 ms cold against 295 ms warm is the
dense scan's cold read), recorded here so the next roll measures cold
against cold. Roots' VmHWM after: 24.7 MB (`:19391`), 24.2 MB
(`:19394`); the search-v10 scope peaks at 21.5 GB of its 28 GB cap,
all from the fresh segment read at serve.

**The surcharge fix, rolled and re-measured (2026-09-08 01:25-02:00).**
The dense+filter surcharge was root-caused offline: the domain fill
walked every domain bit with a per-slot pruned-range check even inside
fully pruned spans, and a domain as wide as the shard paid that fill
for no narrowing. `9c7f0d9` walks domain bits by word within admitted
spans only, fills by domain only when the domain is under half the
admitted set, and resolves each group's MUST children cheapest-first
with an empty-intersection short-circuit (the dense membership is
skipped once a MUST group is provably empty). The fleet rolled to
`9c7f0d9` by the same procedure (krick-1 serve 1,179 s, the Pis, the
roots); every shape's digest matches the before run again on both
roots. Warm, round 3, against the `a9bf470` before column above:

| shape | before | 92b11fb | 9c7f0d9 |
|---|---|---|---|
| `:19391` lexical, `court == "scotus"` | 532 ms | 59 ms | 57 ms |
| `:19391` lexical, `year >= 2018` | 619 ms | 130 ms | 129 ms |
| `:19391` dense, `court == "scotus"` | 565 ms | 588 ms | 572 ms |
| `:19391` dense, `year >= 2018` | 848 ms | 896 ms | 870 ms |
| `:19394` dense, `year >= 2018` (no rows) | 15 ms | 31 ms | 6 ms |
| `:19394` dense, `court == "scotus"` | 139 ms | 155 ms | 140 ms |
| `:19391` allowlist, `court == "scotus"` (control) | 51 ms | 49 ms | 51 ms |

The zero-match shape lands under the old baseline (the dense membership
is skipped outright); the non-empty dense+filter shapes return to
within 1-3% of baseline, the deliberate residual of resolving the dense
membership before the filters in a non-empty group. Roots' VmHWM 24 MB;
the search-v10 scope peak 21.5 GB, unchanged.
