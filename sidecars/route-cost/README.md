# route-cost: the routing enrichment sidecar

Batch travel-cost matrices over road networks, for enriching documents
with anchor travel times before ingest. This is work item 1 of
`docs/plans/routing-enrichment.md` made concrete: routee-compass
(NREL, BSD-3, https://github.com/NREL/routee-compass) embedded behind
a small batch surface, built with `default-features = false` so the
ONNX energy stack never comes along for travel-time work. When
energy-aware costs are wanted (`trip_energy` in the config), build
with `--features energy` to pull in routee-compass's powertrain
models; ONNX inference is already routine in this infrastructure
(OpenVINO/Intel serve grparse), so that build is an option, not a
hazard — it is simply not the default for travel-time matrices.

## The boundary

Routing is an ENRICHMENT SIDECAR, never an index dependency — the same
line the NLP sidecar draws. This crate is deliberately NOT a workspace
member of turbovec-search: the engine builds without it, it builds
without the engine (own lockfile, own target dir), and the only thing
that crosses the boundary is data. Costs it computes land in ordinary
map-numeric columns (`travel_min["scotus"]`) through normal ingest,
where range facets, score chains with per-key bounds, and future CEL
filters work unchanged. The engine needs zero routing machinery.

## Usage

```
route-cost --config <compass.toml> --anchors <anchors.json> \
           --points <points.jsonl> --out <costs.jsonl> \
           [--cost-pointer </route/traversal_summary/trip_time/value>]
```

- `--config`: a routee-compass configuration (graph files, traversal
  models, `[cost.weights]`). The config owns what "cost" means (time,
  distance, an energy blend) and the parallelism; queries stay pure
  coordinates. `fixtures/downtown-denver/travel-time.toml` is a
  complete working example.
- `--anchors`: JSON array of `{"name", "lat", "lon"}` — the fixed
  origins (courthouses, circuit seats). Names become map-numeric keys
  at ingest, so they must be unique and non-empty.
- `--points`: JSONL of `{"id", "lat", "lon"}` — usually geocoded
  document places. Ids are echoed on every record for the join back.
- `--out`: JSONL, one record per (point, anchor) pair:
  `{"id", "anchor", "cost", "unit"}` or `{"id", "anchor", "error"}`.
  A point that map-matches to the same spot as the anchor routes to
  the empty path; its cost is the empty sum, exactly `0.0`, emitted
  with `"empty_route": true` (and no unit, since the empty summary
  carries none).
- `--cost-pointer`: RFC 6901 pointer to the cost inside a
  routee-compass result. Explicit because the config decides the
  summary shape; a pointer that matches nothing refuses by name.

Queries run FROM the anchor TO the point (one-way streets make
direction real); swap inputs to ask the other direction.

Exit codes: `0` every pair routed; `2` some pairs failed, with an
explicit error record per failed pair (points outside the graph are
expected in real corpora — the record says which and why, nothing is
silently dropped); `1` refused before routing (malformed inputs,
duplicate anchor names, impossible coordinates, bad config).

## Graph data

The fixture graph covers downtown Denver only and exists for the smoke
tests (`fixtures/downtown-denver/ATTRIBUTION.md`). A real deployment
compiles its own region from OSM extracts with routee-compass's
tooling; graph sourcing and refresh cadence are the consumer's
data-quality question (`docs/plans/routing-enrichment.md` items 2-5:
anchor-set design, pipeline wiring, graph lifecycle, throughput
measurement).

## What this deliberately does not do

- No query-time routing in the search path: an arbitrary origin has no
  precomputable bound, so it can never sit inside block-max scoring;
  if wanted it belongs in a rerank stage over the surfaced top-k.
- No gRPC service yet: enrichment is batch-shaped (compute offline,
  ingest once, WAL replays forever). A service wrapper can hold this
  same binary's logic if the pipeline ever wants it online.
