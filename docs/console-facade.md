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
A client that takes more than ten seconds to accept one frame is
dropped: its unread gRPC stream would otherwise hold flow-control
window on the channel all calls share.

## Convenience routes

- `GET /api/health`: `ClusterHealth`, shaped for the UI (per target:
  reachability, live and deleted documents, slot offset, fingerprint).
- `GET /api/config`: what the facade was started with (coordinator,
  nodes, whether TLS, a bearer token, and an analysis sidecar are
  configured), the exposed method list with streaming flags, and the
  corpus analysis spec the suggesters need.
- `GET /api/describe/<Service>/<Method>`: the method's comment,
  streaming flag, request and response type names, and every message
  (fields with JSON name, number, type, repeated, oneof, map, comment)
  and enum reachable from them, from the compiled descriptor set.
- `POST /api/embed` `{"text"}`: the analysis sidecar's embedding, when
  `--analysis` is given; 501 otherwise.
- `POST /api/documents` `{"doc_ids": [...]}`: stored text and lineage
  from the owning nodes, routed by the cluster's slot ranges (at most
  1000 ids; ids no shard covers are listed under `unrouted`).
- `GET /` and the static files under it: the UI.

## The UI

Three pages, plain ES modules and CSS embedded in the binary, no build
step and no external resources. Light and dark follow the system until
the header's theme control picks one (kept per browser), and reduced
motion is respected.

**Search** (`/`) builds the unified `Query` from a form: the selection
shape (lexical, dense, hybrid composite with the fusion strategy and
weights, a boolean tree of lexical, dense, and CEL filter clauses, or a
filter-only browse), a CEL filter, `k` and `selection_k`, sort keys,
collapse with inner hits, the columns to show on each hit (sent as
`projections`), highlight snippets, an aggregation rail (group-by,
folds, a fixed or calendar histogram, percentiles), the explain and
profile flags, and cursor paging. Two folded panels carry the rest of
the lexical leaf and the request: phrase with slop, prefix with its
expansion cap, synonym rules for this query (one per line, `a, b` for
an equivalence and `a => b, c` for a one-way rule) or the cluster's
table switched off, score stages over a column (exponential decay with
origin and scale, a log multiplier, a linear addend), and a boost query
with its window and weights. A panel that holds a value opens by itself
and says how many it holds.

A hit shows its score with a bar against the top score on the page, the
requested columns as labelled values (a projected value carries no name;
the request's projection at the same index names it), snippets or the
stored text clamped to a few lines, one bar per leg for a multi-leg
shape (each against that leg's best on the page), scorer dimensions,
matched clauses, and a foldable explain tree. The profile line shows the
wall and total times, the shards the coordinator skipped from their
placement and the segments skipped, over a stacked bar of the phase
timings with the remainder up to the total as fan-out and merge.
Snippets are requested for an unfiltered lexical query only, the one
shape the engine cuts them for, and the result line says so otherwise.

The filter draws as one chip per top-level `&&` condition; the text
input stays the source. A click on a group-by value in the rail adds
`column == "value"` (shift-click `!=`), a click on a string column value
of a hit does the same, and a click on a histogram column adds its
half-open range; each undoes itself on a second click. A filter compares
a column in its own type and does not compile `double()`, so the range
of a `double(year)` histogram goes on `year` with integer literals, a
bare column of a fixed histogram takes double literals, a calendar
histogram's column takes integer microseconds, and any other expression
is not clickable. A fixed histogram draws its empty buckets.

The form is written to the URL fragment on every run (what differs from
the defaults; a pasted vector is left out), so a link reproduces the
query, back and forward step through queries, and the History panel
keeps the last thirty in `localStorage`. `/` focuses the query box,
`Ctrl` or `⌘` with `Enter` runs from any field, `[` and `]` page, the
arrow keys choose a typeahead suggestion, and `?` lists these.

Typeahead comes from `Suggest` on the last word, did-you-mean from
`TermSuggest` after each query. The Stream button runs the same request
through `QueryStream` without `explain` (the unary route serves it),
replaces the hit list with each revision as it arrives, and logs each.
The A/B panel runs `VariantSearch` with two configurations and shows
both rankings with movement markers, the rank-difference figures, and
the interleaved list in place of the hit list. A failed query keeps the
previous results on screen, dimmed and labelled, and outlines the field
the engine's message names. **Pin baseline** keeps the current ranking
(for the tab's session); every later run then marks each hit with its
movement against it (up, down, new), states the overlap and whether the
top hit changed, and lists the hits that dropped out with their
baseline ranks. The **csv** and **json** buttons download the page of
hits (rank, id, score, each signal, each column, text) and the response.
The single-column inputs (collapse, group-by, an aggregation's
expression, a stage's column) suggest the column names of earlier
successful queries, since no RPC lists a cluster's columns. The raw view shows the request, the
response, and a working `grpcurl` line (the token spelled as
`$BEARER_TOKEN`). Dense shapes need an embedding: the sidecar's when
`--analysis` is given, a pasted vector otherwise.

**API** (`/explorer`) lists every exposed method with a filter. A
method shows its request and response shapes from `GET /api/describe`,
each field with its type and the comment written over it in the
`.proto`, nested messages unfolding in place and oneof members marked.
A click on a request field adds it to the JSON body with a starting
value (the first named value of an enum, an element for a repeated
field), creating the messages above it. Run sends the body to the
coordinator or a chosen node, a streaming method lists its messages
newest first until stopped (as many as fit 200,000 characters), and the tabs show the response and
the `grpcurl` line. The URL fragment holds the method, target and body.

**Dashboard** (`/dashboard`) streams `StreamMetrics` from the chosen
process into tiles with sparklines (requests per second, windowed p99,
in flight, errors, scan candidates, floors published, scan bytes per
second with the rate while scanning, and the kernel's active share) and
a per-route table with p50 and p99 from the latency histograms over the
stream's window; the table lists the routes that have served a request
since the page opened or carry errors, sorted by the column heading
clicked, with the idle labels behind a toggle, so a new route appears on
its own. A knobs panel from `GetRuntimeKnobs` has
inputs that call `SetRuntimeKnob` and show the startup value. The shard
map from `GetShardDiagnostics` opens with the placement groups (one line
per code served, the path it decodes to, the shards in it, a warning
when a shard's rows carry more than one code) and draws each shard's
sealed segments as bars, in green when a partitioned compaction gave
them a range, with the summary on hover; a relay's shard is the
children's layouts merged into one, named `relay over N children`, with
the children's segments under it and the child index in front of each
segment id (`docs/relay-coordinators.md`). The placement dry run panel takes a tree
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
text routed by slot range; the 501 embedding route; the describe route
(fields, a oneof, an enum, the streaming flag, 404 for an internal
service); the UI assets with
their content types; the suggesters; the server-sent event stream for
`QueryStream` with its end frame and the query-parameter form; and the
loopback rule. `src/console.rs` unit-tests the URL decoding, the query
parsing, the descriptor's method list, and the JSON round trip.
`node tests/console_ui.mjs` covers the UI's pure module
(`query-state.js`): the filter's split into conditions, the clauses a
click adds, the calendar bucket bounds, the URL encoding, the synonym
rules, the baseline comparison, the CSV writer, and the API page's
field inserts.

## A demo cluster

`cargo run --release --example console_demo` starts the console over a
cluster that lives in the one process: four shards of 150 generated
court-opinion paragraphs each (facets `court` and `topic`, integers
`year` and `filed`, the numeric `citations`, three chunks per opinion),
a coordinator with diagnostics and an in-memory control plane, and the
mock analysis sidecar with a hashed bag-of-words embedding, so every
panel of the pages has something to show. The embedding is a toy: two
texts are close when they share stemmed words. Nothing is written to
disk. `--listen=` moves it off `127.0.0.1:8600`.
