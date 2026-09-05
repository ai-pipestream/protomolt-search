# Embedded and mobile use

The `protomolt-search-embedded` package runs the same product in one process:
your application calls the same `SearchService` contract, an in-process
coordinator talks to local shards through a direct call into the same handlers
the network serves, and no socket is bound or dialed.

There is no second ranking implementation and no second schema model. The shards
are the ordinary node implementation and the coordinator is the ordinary
coordinator; only the link between them differs. A client-streaming call is
framed into the handler's stream body in memory, and a server stream comes back
as the handler's own receiver.

## What it serves

The public routes: `Search`, `Bm25Search`, `PhraseSearch`, `HybridSearch`,
`VariantSearch`, `Query`, the terminally certified `QueryStream`, `PlanIndex`,
the exact `Aggregate`, the provider broadcasts, and cluster health.

Locally: create or open one or more shards, ordinary document and vector
streams, descriptor-planned mapped ingest, deletes and atomic replacements,
per-shard health, and flush.

The same column tables are declared here as on a server. Global BM25 statistics,
vector score identity, hybrid fusion, tie order, live-document admission, CEL,
facets, and stream completion all stay coordinator behavior, so embedded results
match the network service bit for bit.

## The no-egress boundary

It has by construction, not by policy:

1. The configuration takes shard configurations, not node addresses.
2. Every coordinator link is an in-process link to a shard it opened itself.
3. A missing link is rejected instead of falling back to TCP, and both DNS
   resolution and the UDP signal lane are disabled.
4. Every shard and the coordinator are forced to the in-process native analyzer.
   A sidecar URL is rejected before any service task starts.
5. Embeddings enter as client-supplied vectors or as fields in mapped protobuf
   documents. Startup does not configure a remote embedding provider.

The package depends on the search crate with its network feature off and on the
core of the gRPC crate only, so the linked binary has no HTTP/2, hyper,
axum, tower, rustls, or socket layer, and no Tokio networking or signals. A
dependency check in its test suite fails the build the moment one of them comes
back.

## Create and open

`create` rejects overwriting an existing provider image, sidecar, snapshot
generation, or write-ahead log. `open` is normal startup: an absent path starts
empty, and an existing path loads the same generation the server binary would.

A shard is persistent (with a log) or in-memory (for tests and disposable
indexes, where flush reports that no file was written).

The native analyzer requires an explicit `AnalysisSpec`, on purpose: the
analyzer is persisted term identity, and the historical "server default" can
name sidecar-only behavior. Use the same explicit spec at ingest and at query
time.

## Android and iOS

The crate builds as a Rust library, an Android shared library, and an Apple
static library. The mobile ABI exposes create and open, mapped ingest, `Query`,
a pull-based `QueryStream`, flush, and close. Inputs and outputs are protobuf
bytes, and the lifecycle proto imports the ordinary search types, so no
host wrapper contains a second query or schema model. The Rust library owns its
async runtime; Android and Apple callers do not manage one.

- Android: one Java class moving entire protobuf messages per call, with no
  product logic. The packaged manifest declares no internet permission.
- Apple: the same functions as a C module, with an optional Swift facade.

Stream rules on the byte ABI:

- A stream permits one pending read; a second concurrent read is
  FAILED_PRECONDITION.
- Closing a stream or its runtime wakes a pending read with CANCELLED.
- A call on an already-removed handle is NOT_FOUND, and a repeated close reports
  that it closed no stream.
- Run blocking calls off the application's UI thread.

Both build scripts refuse to overwrite an existing artifact. Compile checks
cover arm64 and x86-64 Android and arm64 and x86-64 iOS targets, and the device
suites check the lifecycle, persistence across close and reopen, the absent
internet permission, and socket descriptors before and after.

Two dependency pins matter to you: the Unicode data and the string matcher are
exact pins because their output is persisted term identity, so moving them is an
analyzer-fingerprint and corpus-rebuild decision.

## Shards owned by devices

Sharing a search across indexes that stay on phones is a design under review,
not a feature you can turn on. The embedded package remains networkless; a
separate opt-in transport would own connection, authentication, session state,
and what a result is allowed to reveal.

Three points from that review are useful to know even if you do not build it.
Keeping a shard on a device does not by itself make the scores, identifiers, or
snippets it returns private. Global BM25 needs shared statistics for the pinned
corpus, and those statistics say something about each local corpus. And an exact
answer needs a completion certificate from every pinned participant: a device
that suspends or disconnects makes the result incomplete, and a retry against a
smaller set is a new computation, not a repair of the old one.

Reference: `docs/embedded-mobile.md`, `docs/device-shards.md`.
