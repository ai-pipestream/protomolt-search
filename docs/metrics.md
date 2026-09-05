# Metrics: the Prometheus exporter

`--metrics-listen=host:port` (TOML `metrics_listen`, env
`PIPESTREAM_SEARCH_METRICS_LISTEN`) serves a Prometheus text-format metrics page
over plain HTTP. Off by default; when unset, nothing listens and the
counters still count (they are relaxed atomic adds, cheap enough to be
always on).

The metrics page is its own listener and stays plain HTTP with no auth:
it is a scrape target, not a gRPC surface, and the TLS and bearer
principals of `docs/security.md` cover the gRPC listeners, not this one.
**Bind it to a trusted interface** (`--metrics-listen=127.0.0.1:9100`,
or the address of a private scrape network); leave it unset on a host
that has none. The exporter is hand-rolled (`src/metrics.rs`) because the
exposition format is a few lines of `name{label="value"} 123` text and
the scrape protocol is one GET; neither a metrics framework nor an HTTP
framework earns a place in the serving binary for that — the same
dependency argument that keeps CEL and regex crates out.

## The two lifetimes

- **Counters and histograms** are process-wide statics, fed at one
  instrumentation seam every handler passes through (below) and at the
  few places the engine already counted things. Monotone; use `rate()`
  and `histogram_quantile()`.
- **Gauges** are sampled at SCRAPE time from live shard state, through
  closures the binary hands to the server. Nothing has to remember to
  update a gauge on every mutation, so a gauge cannot go stale — it is
  the state, read under the shard lock when the scraper asks. The one
  exception is `turbovec_requests_in_flight`, which is arrivals minus
  departures and lives with the counters.

## The seam

Every counted handler wraps its body in `metrics::timed(route, request,
|request| async move { .. })` — or `metrics::timed_stream` when the
answer is a response stream — and nothing else. The seam counts the
arrival, holds the in-flight slot, times the body, and records the
outcome by the gRPC status code that left the handler. A handler future
dropped before it answers (the client went away, or the server is
shutting down) is recorded as `CANCELLED`, so the in-flight gauge always
returns to zero.

One arrival is one count however many trait methods it passes through:
the coordinator delegating to its topology snapshot, the public `Query`
adapter running its leaf through `Search` / `Bm25Search` /
`HybridSearch`, and a variant search running its arms, all mark the
re-dispatched request `metrics::nested` and the seam runs it uncounted.
The collections router counts at the member coordinator it resolves
to, so a request through the router is counted once, at the route it
reached.

**Cost.** Per request: one `Instant::now()` at arrival, one per phase
end, a type-map lookup for the nested marker, and a few relaxed atomic
adds (arrival, in-flight up and down, one histogram bucket, the sum,
and an error row on failure). No allocation and no formatting on the
request path: every label string is rendered at scrape time only, and
the route set is fixed at compile time (`REQUEST_ROUTES` in
`src/metrics.rs`) so every table is a plain static indexed by the
route's discriminant — no registry, no map, no lock. A streaming route
pays the same plus one bool check per polled message.

## What is exported

### Requests, by route

Counted at ARRIVAL (a shard erroring under load shows as traffic, not
silence):

    turbovec_requests_total{rpc="..."}

with one row per route. Node (shard) routes:

    search_shard  stream_search  browse_shard  resolve_filter_bitmap
    hybrid_shard  shard_legs  bm25_query  bm25_phrase_query
    bm25_query_stream  read_wal  term_stats  expand_term_prefix
    vector_rescore  exact_vector_rescore  bm25_rescore  fetch_values
    aggregate_shard  quantile_counts  get_documents  resolve_parents
    add_documents  add_vectors  ingest_mapped  delete_documents
    commit_replacements  suggest_terms  compact_shard  export_snapshot
    stream_snapshot  install_snapshot_from

Coordinator public routes:

    search  bm25_search  phrase_search  hybrid_search  query  suggest  term_suggest
    query_stream  plan_index  routed_ingest_mapped
    freeze_topology_writes  publish_topology  abort_topology_cutover
    aggregate  cluster_health  broadcast_vector_backend
    broadcast_calibration  variant_search

Cluster control routes (`docs/cluster-control.md`):

    register_node  renew_node_lease  drain_node  report_shard
    complete_placement_action  reconcile_cluster  get_cluster_plan
    rollback_cluster

(`bm25_query` counts both the unary and the streaming transport of the
same query. `suggest_terms` is the node's dictionary scan and `suggest`
the coordinator's public route, `docs/suggest.md`; a coordinator process
exports the latter, a node process the former.)

Each transport of a BM25 query is its own route (`bm25_query`,
`bm25_phrase_query`, `bm25_query_stream`) because each has its own
latency profile and the streaming one reports phases; `expand_term_prefix`
is its own route rather than borrowing `term_stats`. The node routes not
listed (health, flush, calibration and backend get/set, snapshot
install, WAL binding) are not counted.

### In flight

    turbovec_requests_in_flight{rpc="..."}

A gauge: requests inside a handler, or with an open response stream,
right now. A unary request departs when the handler returns; a
streaming request departs at the stream's terminal event (below).

### Latency

    turbovec_request_duration_seconds_bucket{rpc="...",le="..."}
    turbovec_request_duration_seconds_sum{rpc="..."}
    turbovec_request_duration_seconds_count{rpc="..."}

A Prometheus histogram per route, seconds from arrival. The buckets are
fixed, chosen once, and the same for every route:

    0.001 0.002 0.005 0.01 0.02 0.05 0.1 0.2 0.5 1 2 5 10 +Inf

`le` is inclusive, as Prometheus defines it (an observation of exactly
1ms lands in the 0.001 bucket); the compare is done in integer
nanoseconds. `_count` equals the `+Inf` bucket. `_sum` renders as an
exact decimal from the nanosecond total (`0.0015`, never `1.5e-3` and
never a float rounding tail); bucket counts and `_count` are integers.

**Streaming routes report two phases.** `search_shard`, `stream_search`,
`bm25_query_stream`, `read_wal`, `stream_snapshot`, and `query_stream` carry a `phase`
label on every histogram row and unary routes carry none:

    turbovec_request_duration_seconds_count{rpc="query_stream",phase="first_response"}
    turbovec_request_duration_seconds_count{rpc="query_stream",phase="complete"}

- `first_response`: arrival to the first message handed to the
  transport. This is the streaming search's product claim — the first
  hit's latency — measured where the transport takes the message, not
  where the handler returned its stream.
- `complete`: arrival to the stream's terminal event — exhausted, ended
  by a terminal `Err` status, or dropped unread.

Both phases count every request on the route: a refusal before the
stream opens, or a stream that ends with no message, records the two
phases at the same instant. A `sum by (rpc)` over a streaming route
therefore sums two phases; select the phase you mean.

### Errors

    turbovec_request_errors_total{rpc="...",code="..."}

Requests that ended in a gRPC error, by the canonical status code that
left the handler (for a stream, the terminal `Err` it ended with):

    invalid_argument  failed_precondition  not_found  resource_exhausted
    unavailable  deadline_exceeded  unauthenticated  permission_denied
    internal  other

`other` folds every remaining code (cancelled, unknown, already_exists,
aborted, out_of_range, unimplemented, data_loss) — including a handler
future or a response stream dropped before it produced a status, which
is recorded as `CANCELLED`. Every (route, code) row is pre-declared, so
the series exist from the first scrape and a `rate()` over one never
starts from an absent series. Arrivals minus errors is the success
count; the histogram observes both.

### Vector-scan work

Folded in once per completed scan on every route through the scheduler
(batched or solo):

    turbovec_scan_chunk_calls_total          per-chunk kernel calls
    turbovec_scan_candidates_total           real candidates collected
    turbovec_scan_floors_offered_total       floors the scan offered
    turbovec_scan_floors_published_total     floors actually on the wire
    turbovec_scan_floor_updates_applied_total  chunks run under a pushed floor
    turbovec_scan_batches_total              batched kernel passes
    turbovec_scan_batched_jobs_total         jobs that rode a batch

The offered/published split exists because they were once one counter
and a knob that suppressed nine tenths of the broadcasts read as having
done nothing (`docs/optimizations.md`); `candidates_total` is where
floor sharing's savings show.

### Ingest

    turbovec_documents_added_total
    turbovec_vectors_added_total

### Per-shard gauges

Labeled by `slot_offset` (the shard's name in the global id space):

    turbovec_shard_vectors{slot_offset="..."}
    turbovec_shard_documents{slot_offset="..."}
    turbovec_shard_stats_epoch{slot_offset="..."}

A process serving several shards (one `[[shards]]` entry each) exports
one labeled sample per shard on one page.

## Page size

49 routes × (14 buckets + `_sum` + `_count`), twice for the five
streaming routes, plus 49 × 10 error rows and the request and in-flight
rows: 1,499 lines and 111 KB of text per scrape (measured by the
page-shape unit test's render, no gauges), rendered on demand into one
string. Nothing is retained between scrapes.

## What is deliberately NOT here

- **Per-principal labels.** The bearer principals of `docs/security.md`
  are not a label dimension; a principal's traffic is not separable on
  this page. A label per principal is a series per principal per route,
  and the principal set is operator data, not a compile-time table —
  it needs the label registry this exporter does not have.
- **Per-collection labels.** Same reason. The collections router counts
  at the member coordinator it resolves to, and every member's counts
  land in the same process-wide statics, so a process serving several
  collections cannot split a route's rows by collection.
- **Sidecar and fleet client latency.** The seam times the handler, which
  includes its fan-out and its one sidecar call; the split between the
  engine's own work and the time spent waiting on a shard or the sidecar
  is in the per-request `debug` profile blocks, not on this page.
- **Membership, quotas** — the rest of roadmap item 12, tracked there.

## Tests

`src/metrics.rs` unit tests pin the route table against the enum's
discriminants, inclusive bucket boundaries and cumulative rendering
(`_count` equals `+Inf`), the exact-decimal float rendering, the status
code mapping including `other`, the page shape (every sample numeric,
`_sum` a float, every metric under exactly one HELP/TYPE header,
multi-shard gauge samples grouped under one header, every route with a
histogram and all ten error rows, phases on streaming routes only), and
the seam itself: outcomes by code, the in-flight gauge returning to zero
on success, refusal, and a dropped handler future, nested re-dispatch
counted once, and both streaming phases across a completed stream, a
stream ending in `Err`, a stream dropped unread, and a refusal before
the stream opened.

`tests/metrics.rs` scrapes a live exporter over real HTTP and asserts as
deltas (the counters are process-wide, so absolute assertions would race
sibling tests): a served ingest moves its route's `_count`, `+Inf`
bucket, and `_sum`; an `INVALID_ARGUMENT` browse moves exactly the
`invalid_argument` row and no other code; and over a served coordinator
in front of a served shard, `Search` and `Query` move their unary rows
once each with no double count through the `Query` adapter,
`QueryStream` moves both phases with `complete` no earlier than
`first_response`, and the shard underneath shows the fan-out's
transport: two `search_shard` streams and one `stream_search` stream per
phase, every route back to zero in flight.
