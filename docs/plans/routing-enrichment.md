# Routing enrichment: routee-compass as a sidecar (its own session)

Written 2026-08-03 for a FUTURE session — this is a handoff brief in the
track-2/track-3 pattern, not queued work in the feature track. Read
`track-1-features.md`, `docs/map-columns.md`, and (once landed) the geo
columns doc before starting.

## The pipeline this completes

1. **Place mentions** — the OpenNLP sidecar's NER finds place names in
   opinions; the openmap tooling in the OpenNLP fork geocodes them to
   coordinates. (That half lives in the user's OpenNLP branches, not
   this repo — `project-boundaries` applies: turbovec-search stays a
   standalone engine, the CourtListener pipeline is a consumer.)
2. **Geo columns** (feature-track increment 3) — coordinates land on
   documents as a geo-point column kind; bbox, haversine radius, and
   Manhattan distance work as filters and monotone-decay score stages.
3. **Routing enrichment** (THIS session) — road-network semantics:
   "travel time / energy from X", which straight-line distance cannot
   answer. routee-compass (NREL, BSD-3, Rust core; cloned at
   `/work/reference-code/routee-compass`) is the engine for that.

## The boundary decision, made up front

routee-compass is a ROUTING engine over road-network graphs — big,
mutable datasets with their own update lifecycle. It must NOT become an
index dependency; it is an enrichment SIDECAR, exactly the boundary the
NLP sidecar draws: the engine owns scoring structures, sidecars own
their domains.

The consequence that makes this session cheap on the engine side:
**precomputed routing costs are just columns.** Travel time from a
fixed anchor set (courthouse locations, circuit boundaries, population
centers — the anchor design is this session's real decision) computed
at ingest/enrichment time lands as plain f64 columns or map-numeric
columns keyed by anchor name (`travel_min["scotus"]`), and then range
facets, score-function chains with per-key bounds, and (future) CEL
filters all work UNCHANGED. Zero new engine machinery.

Query-time routing ("travel time from an arbitrary user location") is
the expensive variant: it cannot participate in block-max pruning (no
precomputable bound for an arbitrary origin) and would gate every
candidate on a routing call. If wanted, it belongs in a rerank stage
over the surfaced top-k, never in the scoring loop — the cascade
machinery is the seam.

## The session's work list

1. Evaluate the routee-compass Rust API for embedding in a sidecar
   service (it ships a Rust core with Python bindings; we want the Rust
   core behind a small gRPC surface, batch-oriented:
   (origins, destinations) -> cost matrix).
2. Design the anchor set with the consumer (which fixed origins make
   "travel time to X" a useful legal-search signal).
3. Wire the enrichment into the CourtListener pipeline: geocoded
   coordinates -> batch routing calls -> `NumericValue` /
   `MapNumericEntry` ingest. The WAL carries the enriched values, so
   rebuilds replay them without re-routing.
4. Decide graph sourcing and refresh cadence (OSM extracts; a stale
   graph is a data-quality question for the consumer, not an engine
   invariant).
5. Measure: batch routing throughput against ingest throughput; if
   routing is the bottleneck, it batches offline ahead of ingest.

## Prerequisites

- Geo columns landed (increment 3).
- OpenNLP geocoding output shape pinned (the openmap work).
- An owner for the anchor-set decision.
