# The console: REST facade and web UI

The `console` binary is the operator's front end for a running cluster:
a small HTTP server that speaks JSON on one side and the cluster's gRPC
on the other, plus the static web UI it serves. It is a client only. It
holds the TLS material and the bearer token (`docs/security.md`), so a
browser never carries either.

## The transcoding endpoint

`POST /api/rpc/<Service>/<Method>` takes the request message in proto3
JSON (the same JSON `grpcurl` accepts: field names in `camelCase` or
`snake_case`, enums by name or number, 64-bit integers as strings or
numbers) and answers with the response message in proto3 JSON with
every field rendered, defaults included (what `grpcurl -emit-defaults`
prints), so a document id of 0 is a value and not an omission. Every
unary method of `SearchService` and `DiagnosticsService` is reachable
this way; the mapping is built from the compiled descriptor set at run
time, so an RPC added to an exposed service needs no facade change.
`NodeService` is the cluster's internal surface and is not exposed.
`ClusterControl` is internal too, and the facade lists one of its
methods by name: `PlanBalance`, the read-only balance dry run the
dashboard renders (`docs/bandwidth-budget.md`). The wire still applies
cluster trust to that call, so the facade's own credentials must be a
member's; the other control methods answer 404 from the facade, naming
the exposed surface. A gRPC status becomes an HTTP status (`INVALID_ARGUMENT` 400,
`UNAUTHENTICATED` 401, `PERMISSION_DENIED` 403, `NOT_FOUND` 404,
`RESOURCE_EXHAUSTED` 429, `UNIMPLEMENTED` 501, anything else 502) with
the status message in a JSON `error` field and its name in `code`,
unchanged. Malformed request JSON is the facade's own 400 naming the
message type.

`?target=` picks the process: `coordinator` (the default) or `node<i>`
from the configured `--nodes` list, so a dashboard can read and set a
node's runtime knobs. No other address is reachable from a browser.

Server-streaming methods are exposed as server-sent events under
`/api/stream/<Service>/<Method>`: one `data:` line per message, each a
proto3 JSON object, then `event: end`; a status mid-stream arrives as
`event: error` with the same `error`/`code` body. The request message
comes from a POST body, from a `request=<url-encoded JSON>` query
parameter, or from scalar query parameters mapped onto top-level fields
(`GET /api/stream/DiagnosticsService/StreamMetrics?interval_ms=1000`).

## Convenience routes

- `GET /api/health`: `ClusterHealth`, shaped for the UI (per target:
  reachability, live and deleted documents, slot offset, fingerprint).
- `GET /api/config`: what the facade was started with (coordinator,
  nodes, whether TLS, a bearer token, and an analysis sidecar are
  configured), the exposed method list with streaming flags, and the
  corpus analysis spec the suggesters need.
- `POST /api/embed` `{"text"}`: the analysis sidecar's embedding, when
  `--analysis` is given; 501 otherwise.
- `POST /api/documents` `{"doc_ids": [...]}`: stored text and lineage
  from the owning nodes, routed by the cluster's slot ranges (at most
  1000 ids; ids no shard covers are listed under `unrouted`).
- `GET /` and the static files under it: the UI.

## The UI

Two pages, plain ES modules and CSS embedded in the binary, no build
step and no external resources; light and dark follow the system, and
reduced motion is respected.

**Search** (`/`) builds the unified `Query` from a form: the selection
shape (lexical, dense, hybrid composite with the fusion strategy and
weights, a boolean tree of lexical, dense, and CEL filter clauses, or a
filter-only browse), a CEL filter, `k` and `selection_k`, sort keys,
collapse with inner hits, highlight snippets, an aggregation rail
(group-by, folds, a fixed or calendar histogram, percentiles), the
explain and profile flags, and cursor paging. Hits show scores, signals,
scorer dimensions, matched clauses, snippets or the stored text, and a
foldable explain tree; the profile line shows phase timings, the shards
the coordinator skipped from their placement, and the segments skipped. Typeahead comes from `Suggest` on the last word,
did-you-mean from `TermSuggest` after each query. The Stream button
runs the same request through `QueryStream` and logs each revision. The
A/B panel runs `VariantSearch` with two configurations and shows both
rankings with movement markers, the rank-difference figures, and the
interleaved list. The raw view shows the request, the response, and a
working `grpcurl` line (the token spelled as `$BEARER_TOKEN`). Dense
shapes need an embedding: the sidecar's when `--analysis` is given, a
pasted vector otherwise.

**Dashboard** (`/dashboard`) streams `StreamMetrics` from the chosen
process into tiles with sparklines (requests per second, windowed p99,
in flight, errors, scan candidates, floors published, scan bytes per
second with the rate while scanning, and the kernel's active share) and
a per-route table with p50 and p99 from the latency histograms over the
stream's window; the table's rows are the snapshot's route labels, so a
new route appears on its own. A knobs panel from `GetRuntimeKnobs` has
inputs that call `SetRuntimeKnob` and show the startup value. The shard
map from `GetShardDiagnostics` opens with the placement groups (one line
per code served, the path it decodes to, the shards in it, a warning
when a shard's rows carry more than one code) and draws each shard's
sealed segments as bars, in green when a partitioned compaction gave
them a range, with the summary on hover; a shard whose layout came back
as a relay's refusal is marked as a relay, since a relay serves the
node-facing surface only and its children are its own map
(`docs/relay-coordinators.md`). The placement dry run panel takes a tree
in the shard map's `[placement]` shape (or as `PlacementTree` JSON) and
an optional filter, calls `SearchService.PlanPlacement`, and lists rows
and moving rows per shard and leaf with the totals; the balance dry run
panel calls `ClusterControl.PlanBalance` with the gain threshold, the
move budget, and the rate age, and lists the loads, the moves with the
estimate after each, and the exclusions with their reasons. A
recent-queries table from `RecentQueries` has a drill-down. Cluster
health tiles come from `/api/health`. A diagnostics call the cluster
does not serve renders as "not served by this cluster" and is retried
every 30 seconds.

## Configuration

The same flags as every tool (`--coordinator`, `--nodes` in shard order,
`--analysis`, `--tls-ca`, `--tls-client-cert`, `--tls-client-key`,
`--tls-domain`, `--bearer-token` or `--bearer-token-file`) plus
`--listen` (default `127.0.0.1:8600`). The facade refuses to bind a
non-loopback address without `--allow-remote`, because whoever reaches
it acts as its principal. The HTTP side is hand-rolled HTTP/1.1 with
`Connection: close`; it is an operator's tool, not a product server.

## Tests

`tests/console.rs` starts two nodes, a coordinator with the lexical
backend, and the facade in-process, then exercises the health and
config routes; a lexical `Query` whose JSON equals the descriptor's
rendering of the typed client's response bytes; a boolean `Query` with a
filter, explain, and an aggregation; the 400 mapping with the gRPC
message and the facade's own 400 for malformed JSON; 501 for the unserved
diagnostics on the coordinator and on a node, and 400 for a node index
out of range; the placement dry run (a tree with no default refused by
name, a planned tree's rows per leaf) and the balance dry run against an
in-memory control plane, with the other control methods answering 404;
404 for internal services and unknown methods; document
text routed by slot range; the 501 embedding route; the UI assets with
their content types; the suggesters; the server-sent event stream for
`QueryStream` with its end frame and the query-parameter form; and the
loopback rule. `src/console.rs` unit-tests the URL decoding, the query
parsing, the descriptor's method list, and the JSON round trip.
