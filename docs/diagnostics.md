# The diagnostics service

`DiagnosticsService` is the operator's window into a running node or
coordinator: the runtime knobs and their live values, the metrics
registry as values and as a stream, the layout of the shards, and the
coordinator's log of recent requests. It is served on the same listener
as the data services, so it needs no extra port and inherits the
listener's TLS.

With bearer principals configured (`docs/security.md`), a call on the
coordinator needs a principal with `admin = true` on its entry; any
other principal gets `PERMISSION_DENIED` naming itself, and a missing
token `UNAUTHENTICATED`. Without principals the service is open, like
the rest of the surface. Nodes have no principals: their listeners are
reached over mTLS by the coordinator and by operators.

The only call that changes an answer is `SetRuntimeKnob`. Everything
else reads.

## Runtime knobs

`GetRuntimeKnobs` lists every setting the process has, live ones first.
Each entry names the knob, its scope (node or coordinator), its kind,
the current value as text, the value the process started with, whether
`SetRuntimeKnob` may change it, and one line of description.

A live knob is an atomic the request path loads each time, so a change
takes effect on the next request and needs no restart. A setting the
code still reads from its startup configuration is listed with
`mutable = false`, and setting it returns `FAILED_PRECONDITION` naming
it. An unknown name returns `INVALID_ARGUMENT` with the known names; a
value that does not parse for the knob's kind returns
`INVALID_ARGUMENT` naming the knob, and the knob is unchanged.

| Knob | Scope | Kind | Live | What it does |
|---|---|---|---|---|
| `floor_sharing` | node | bool | yes | Publish the shard's k-th best score and prune candidates under the shared cutoff (`--floor-sharing`). |
| `segment_pruning` | node | bool | yes | Skip sealed segments with a column summary that rules out the filter (`--segment-pruning`, `docs/segment-pruning.md`). |
| `floor_delta` | node | float | yes | Smallest score movement that publishes a new floor. |
| `floor_warmup_chunks` | node | int | yes | Scan chunks to finish before the first floor is published. |
| `floor_min_interval_ms` | node | int | yes | Shortest gap between floor publications; 0 publishes on each movement. |
| `max_k` | coordinator | int | yes | Largest `k` a request may ask for, and the depth an omitted `k` runs at (`--max-k`). Zero is rejected. |
| `hedge_delay_ms` | coordinator | int | yes | Wait on a shard's primary before racing its replica; 0 disables hedging. |
| `shard_pruning` | coordinator | bool | yes | Skip shards whose placement leaf rules out the request's filter before fan-out (`--shard-pruning`, `docs/placement.md`). |
| `chunk_blocks`, `block_max`, `coalesce`, `scan_parallel`, `rerank_parallel`, `layout`, `vector_mmap`, `seal_tail_docs`, `bit_width`, `slot_offset`, `collection`, `vector_backend`, `placement_column`, `placement_leaf` | node | | no | Read at startup. |
| `collection`, `nodes`, `replicas`, `stream_search`, `bm25_stream`, `max_rerank_bytes`, `shard_deadline_ms`, `dense_execution_policy` | coordinator | | no | Read at startup. |

A coordinator serving several collections shares one process-wide set
of caps: `SetRuntimeKnob` applies to every collection's coordinator and
the list is the default collection's.

The `startup_value` is there so an A/B run can be put back: flip
`segment_pruning` off, run the queries, flip it back to the startup
value.

```sh
grpcurl -plaintext -d '{}' 127.0.0.1:19300 \
  ai.protomolt.search.v1.DiagnosticsService/GetRuntimeKnobs
grpcurl -plaintext -d '{"name":"segment_pruning","value":"false"}' 127.0.0.1:19300 \
  ai.protomolt.search.v1.DiagnosticsService/SetRuntimeKnob
```

## Metrics snapshot and stream

`GetMetricsSnapshot` returns the metrics registry as values: the same
request counters, in-flight gauges, latency histograms, error counters,
scan-work and ingest counters, and per-shard gauges the Prometheus page
renders (`docs/metrics.md`), with the same names and labels, in the
same order. A histogram sample lists cumulative bucket counts with
their upper bounds in seconds, the sum in seconds, and the count. Both
are views of one registry reading (`metrics::read`): the page and the
snapshot built from the same reading are equal by construction, and a
test asserts it on one reading rather than on two taken moments apart.

`StreamMetrics` sends one snapshot every `interval_ms` (0 selects 1000;
below 100 is rejected) until the client hangs up; the producer stops
once the receiver has closed.

```sh
grpcurl -plaintext -d '{"interval_ms":500}' 127.0.0.1:19291 \
  ai.protomolt.search.v1.DiagnosticsService/StreamMetrics
```

The Prometheus page remains the export for scrapers and history; the
stream is for a live dashboard. Both read the same atomics, so they
cannot differ.

## Shard diagnostics

`GetShardDiagnostics` describes the shards a process serves. A node
answers for its own shard: the layout (`segments` or `single-image`),
the catalog epoch, rows, live rows, tombstones, rows in the unsealed
tail, the partition key when a partitioned compaction set one, the
scoring fingerprint, the live values of `segment_pruning` and
`floor_sharing`, the placement code the shard serves and whether its
segments carry more than one (`docs/placement.md`), and every sealed
segment with its id, generation, base
row, rows, live rows, whether it has a summary, the summary's
column ranges (integer columns with `lo`/`hi`, double columns with
`lo_f`/`hi_f` and `floating = true`, each with the count of rows that have a
value), its partition range when it has one, and whether its vector
image is memory-mapped.

A coordinator answers for every node in its shard map, in shard order:
an in-process node directly, a remote node through that node's own
diagnostics service, with the shard index and address written on it. A
node that does not serve the service yet, or does not answer within
five seconds, is listed with the status it returned in `layout`
(`Unimplemented: ...`, `DeadlineExceeded: ...`). `shard` limits the
answer to one shard index. The response also includes the served
topology generation.

```sh
grpcurl -plaintext -d '{"shard":7}' 127.0.0.1:19291 \
  ai.protomolt.search.v1.DiagnosticsService/GetShardDiagnostics
```

## Recent queries

The coordinator keeps a ring of the last 256 public requests: search,
BM25 search, phrase search, hybrid search, variant search, query,
streaming query, aggregate, suggest, and did-you-mean. An entry records
the wall clock, the route name from the metrics table, the `executed`
label a query reports, `k`, the wall time in milliseconds, the gRPC
status name (`OK` on success), the principal's name when principals are
configured, the segment totals and skips when the response includes
them, the candidate count when a hybrid debug block includes one, the
number of hits, and the collection. The push happens after the handler
answers and reads the response in place; no request or response body
is copied.

`RecentQueries` returns the newest `limit` entries first (0 selects 50,
256 at most) and the number of requests seen since the process started,
including those the ring no longer holds. A node has no ring and
answers with an empty list.

```sh
grpcurl -plaintext -d '{"limit":10}' 127.0.0.1:19291 \
  ai.protomolt.search.v1.DiagnosticsService/RecentQueries
```

## Metrics of the service itself

The six RPCs are routes in the metrics table (`get_runtime_knobs`,
`set_runtime_knob`, `get_metrics_snapshot`, `stream_metrics`,
`get_shard_diagnostics`, `recent_queries`), so a dashboard's own polling
shows up on the page it polls.

## Tests

`tests/diagnostics.rs`: the knob list and a live flip of
`segment_pruning` observed on the next query's profile, with the answer
unchanged; the other live knobs taking their values; immutable, unknown,
and malformed settings rejected by name; the snapshot equal to the
rendered page; the stream at its interval and a fresh stream after a
hang-up; shard diagnostics on a segmented shard with summaries and on a
single-image shard, through the node and through the coordinator; the
ring's contents and order; the admin rule on every coordinator RPC and
the open service without principals.
