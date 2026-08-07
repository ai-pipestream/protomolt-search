# The bench matrix

What to run after every engine update: four cluster shapes, one driver,
one history file. Everything reads `inventory.env` in this directory --
that file is the single source of truth for hosts, ports, niceness, and
paths.

```bash
# one-time, or whenever the binary changes: put an aarch64 build on the pis
./deploy_fleet.sh all            # or named hosts: ./deploy_fleet.sh pi5v1 cm5v1
./deploy_fleet.sh doctor         # ssh/disk/mem/port health, no changes

# the matrix itself (run from krick)
./run_matrix.sh solo             # everything on krick-1
./run_matrix.sh duo              # krick-1 + krick, split evenly
./run_matrix.sh fleet            # krick-1 + krick + the live pis
./run_matrix.sh fleetpi          # pis only, no fast machine at all
./run_matrix.sh all              # all three, in that order
./run_matrix.sh teardown         # stop every bench process on every host
```

Useful overrides: `--shard-set=PATH` (default
`/work/court-corpus/shards-v9`), `--k=10,100` (default
`10,100,1000,10000`), `--concurrency=1` (default `1,8`). `SHARD_SET`,
`SLOT_STRIDE`, `SIDECAR_ADDR`, `BENCH_OUT`, `NICE_FAST`, `NICE_PI` are
environment variables documented in `inventory.env`.

## What each setup measures

- **solo** -- every shard on krick-1 (32 cores), coordinator on krick-1.
  The fast-box ceiling: engine throughput and latency with no network fan
  out and no slow collaborator. An engine regression shows up here with
  nothing else to blame.
- **duo** -- shards split evenly across krick-1 and krick, coordinator on
  krick. Adds exactly one network hop and one more fast machine: isolates
  the cost of distribution itself.
- **fleet** -- shard 0 on krick-1, shard 1 on krick, shards 2.. round-robin
  over whichever pis are alive, coordinator on krick. This is the
  floor-scout measurement the engine exists for: two fast collaborators
  plus a fleet of slow machines, and the question of whether mid-query
  floor sharing keeps the slow nodes from gating every query.
- **fleetpi** -- every shard on a pi, coordinator on cm5ai1: no fast
  machine anywhere. `FLEETPI_HOSTS` in `inventory.env` is ordered to match
  the shards the pis already have staged (pi5v1..cm5v1 keep shards 2..7
  from the fleet setup; pi5ai1 and cm5v2 take shards 0 and 1), so only new
  hosts ever rsync. Answers the "what if krick and krick-1 went away"
  question, and how much floor sharing matters when there is no fast leg
  to lean on.

Every setup starts TWO nodes per shard over the SAME shard files: one with
`--floor-sharing=true` (port 59700+2n on its host) and one twin with
`--floor-sharing=false` (59701+2n). `cluster_sweep` A/Bs the two clusters
and applies its bitwise correctness gate: sharing on and off must return
identical hit signatures at every k, or the run is invalid. The twin
doubles as a hedge replica.

Each setup runs two sweep cells: concurrency 1 (40 queries + 5 warmup,
the latency cell) and concurrency 8 (64 + 5, the throughput cell), over
k = 10, 100, 1000, 10000, with probe vectors drawn from
`/work/court-corpus/embeddings-full.bin`.

## Reading the output

Per run, per setup:

- `/work/court-corpus/bench/<setup>-<date>.jsonl` -- the raw
  `cluster_sweep` records: one line per (label, k, floor_sharing on/off)
  with `qps`, `wall_p50_ms`, `wall_p90_ms`, `wall_p99_ms`, candidate and
  floor counters.
- `/work/court-corpus/bench/history.jsonl` -- one summary line per
  finished setup: `{setup, date, engine_rev, turbovec_rev, cells: [...]}`.
  `engine_rev` is this repo's `git rev-parse --short HEAD`;
  `turbovec_rev` is the turbovec commit pinned in `Cargo.lock`. Compare
  runs with jq, e.g. qps for the sharing-on k=100 cell across history:

  ```bash
  jq -c '{setup, date, engine_rev,
          cell: [.cells[] | select(.k==100 and .floor_sharing=="on")
                 | {label, qps, wall_p50_ms}]}' \
    /work/court-corpus/bench/history.jsonl
  ```

A setup that passes appends its history line and tears down everything it
started. A setup whose correctness gate fails exits 1 and deliberately
leaves every process up and every log in place (logs under each host's
`<bench-root>/logs/`, pidfiles under `<bench-root>/run/`); inspect, then
`run_matrix.sh teardown`.

## Operational traps

Inherited from `deploy/v7-rebuild/README.md`, plus the ones this suite
adds:

- **An open port is not readiness.** A node binds its listener before it
  has opened its `.bm25`, and opening tens of GB of postings takes
  minutes. `run_matrix.sh` therefore gates on `v7_verify --ready-only`
  against the coordinator (the same poll `rebuild.sh serve` uses), not on
  ports. Do not point a sweep at a fleet that has not passed it.
- **Bench ports live inside the ephemeral range too.**
  `ip_local_port_range` here is 32768-60999, which covers 59700-59714 and
  59295, so an unrelated outbound connection can hold a bench port at bind
  time and that node dies with `AddrInUse` while its siblings come up
  fine. The permanent fix, as for the rebuild ports:
  `sysctl -w net.ipv4.ip_local_reserved_ports=59295,59700-59715` (and the
  equivalent on the pis if it bites there).
- **Pin data-plane addresses to IPv4 literals.** Never put a hostname in
  `--nodes`: a multi-homed IPv6 answer once routed a gRPC channel to the
  wrong machine. The scripts only ever pass `hostname -I` first-address
  literals; keep it that way.
- **The sidecar is shared and stateful in ways you cannot see.** Every
  bench node analyzes and embeds against krick's sidecar
  (`SIDECAR_ADDR`, default `http://192.168.1.242:59202`) -- the same
  process the live fleet uses. That is read-only query traffic and is
  safe, but it means a bench run adds load to a production sidecar, and a
  sidecar restarted without its embedding model serves ingest fine and
  fails the first hybrid query. `rebuild.sh`'s probe-gated reuse
  (`analyze_probe --embed`) is the check; an open port proves nothing.
- **This suite never touches the live v7 fleet.** The bench coordinator
  is 59295, not 59291; bench nodes are 59700+, not 59300-59307. Teardown
  only ever kills pidfiles named `bench-*.pid` that this suite wrote.
  Do not point `--shard-set` at the directory the live fleet is serving
  from unless you mean to: the shards are mmapped read-only so it works,
  but the bench's page-cache pressure competes with the live queries.
- **`SLOT_STRIDE` must match the build.** Nodes are started with
  `--slot-offset=i*SLOT_STRIDE`; the default 21659648 is the full-corpus
  rebuild default. A custom slice built with a different stride (check
  `rebuild.sh plan` output) needs `SLOT_STRIDE=... ./run_matrix.sh ...`
  or every shard answers in the wrong global id space.
- **Pi disk is small.** A full-corpus shard is ~1.45 GB `.tv` plus ~56 GB
  `.bm25`; a 57 GB pi root cannot hold one. `deploy_fleet.sh doctor`
  prints free disk per host -- check it against the shard sizes before a
  fleet run, and use a smaller `--shard-set` slice for the pis if needed.
- **krick stages nothing.** krick serves its shards straight from
  `SHARD_SET` (it is the rsync source); every other host gets its own
  copy under its bench root. rsync's default size+mtime quick check is
  what skips files a host already has -- do not add `--checksum` unless
  you enjoy reading 56 GB per shard per run.
- **The cross build is your problem, once.** `deploy_fleet.sh` cross
  builds only when the aarch64 target and linker are already installed,
  and otherwise prints the exact one-time setup. It never installs
  anything itself. `BINARY=/path/to/binary` bypasses the check entirely.
