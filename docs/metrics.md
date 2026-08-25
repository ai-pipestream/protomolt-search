# Metrics: the Prometheus exporter

`--metrics-listen=host:port` (TOML `metrics_listen`, env
`TURBOVEC_METRICS_LISTEN`) serves a Prometheus text-format metrics page
over plain HTTP. Off by default; when unset, nothing listens and the
counters still count (they are relaxed atomic adds, cheap enough to be
always on).

This is the first piece of the operational surface — metrics only, on
purpose. There is still no TLS and no auth anywhere in the engine,
including on this page: **bind it to a trusted interface.** The
exporter is hand-rolled (`src/metrics.rs`) because the exposition
format is a few lines of `name{label="value"} 123` text and the scrape
protocol is one GET; neither a metrics framework nor an HTTP framework
earns a place in the serving binary for that — the same dependency
argument that keeps CEL and regex crates out.

## The two lifetimes

- **Counters** are process-wide statics, incremented where the engine
  already counted things. Monotone; use `rate()`.
- **Gauges** are sampled at SCRAPE time from live shard state, through
  closures the binary hands to the server. Nothing has to remember to
  update a gauge on every mutation, so a gauge cannot go stale — it is
  the state, read under the shard lock when the scraper asks.

## What is exported

Requests, counted at ARRIVAL (a shard erroring under load shows as
traffic, not silence):

    turbovec_requests_total{rpc="search_shard" | "stream_search" |
        "hybrid_shard" | "shard_legs" | "bm25_query" | "term_stats" |
        "vector_rescore" | "bm25_rescore" | "get_documents" |
        "add_documents" | "add_vectors"}

(`bm25_query` counts both the unary and the streaming transport of the
same query.)

Vector-scan work, folded in once per completed scan on every route
through the scheduler (batched or solo):

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

Ingest:

    turbovec_documents_added_total
    turbovec_vectors_added_total

Per-shard gauges, labeled by `slot_offset` (the shard's name in the
global id space):

    turbovec_shard_vectors{slot_offset="..."}
    turbovec_shard_documents{slot_offset="..."}
    turbovec_shard_stats_epoch{slot_offset="..."}

A process serving several shards (one `[[shards]]` entry each) exports
one labeled sample per shard on one page.

## What is deliberately NOT here (yet)

- **Latency histograms.** The roadmap's rule for this item was "an
  exporter, not new instrumentation"; the interesting counters already
  existed. Latency belongs to a follow-up that decides bucket
  boundaries once, deliberately.
- **Error counters.** Same reason. Arrival counts plus the scrape
  target's own `up` metric cover liveness; per-code error counting is
  new instrumentation.
- **TLS, auth, membership, quotas** — the rest of roadmap item 12,
  explicitly unbuilt and tracked there.

## Tests

`src/metrics.rs` unit tests pin the page shape (every sample numeric,
every metric under exactly one HELP/TYPE header, multi-shard gauge
samples grouped under one header) and route-counter independence;
`tests/metrics.rs` scrapes a live server over real HTTP and asserts the
counters move with real ingest traffic, as deltas (the counters are
process-wide, so absolute assertions would race sibling tests).
