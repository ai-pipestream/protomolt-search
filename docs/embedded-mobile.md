# Embedded and mobile Protomolt Search

`protomolt-search-embedded` is the linkable form of the full search product.
It runs a complete private cluster inside one process:

```text
mobile application
       |
       v
EmbeddedSearch / SearchService
       |
       v
local coordinator
       |
       +---- in-memory HTTP/2 ---- local shard 0
       +---- in-memory HTTP/2 ---- local shard 1
       `---- in-memory HTTP/2 ---- local shard N
```

This is a local cluster, not a federated query. The server never receives the
phone's index, documents, queries, vectors, analyzer output, or rankings.
Each shard is the ordinary `NodeServiceImpl`; the coordinator is the ordinary
`CoordinatorServiceImpl`. Tonic carries the existing protobuf messages over
Tokio duplex streams so the embedded runtime does not maintain a second
ranking or schema implementation.

## Contract

The runtime exposes all public search routes:

- `Search`, `Bm25Search`, `PhraseSearch`, `HybridSearch`, and `VariantSearch`;
- `Query` and terminally certified `QueryStream`;
- `PlanIndex` and exact `Aggregate`;
- vector-backend/calibration broadcasts and cluster health.

It also provides the local lifecycle needed to own a private index:

- create or open one or more shards;
- ordinary document and vector streams;
- descriptor-planned `IngestMapped` streams;
- delete and atomic replacement overlays;
- per-shard health and flush;
- a generated in-memory `NodeServiceClient` for the remaining admin routes,
  including snapshot and encoded-row operations.

`NodeConfig` owns the same BM25 field, facet, numeric, map, integer, and geo
tables on server and device. `PlanIndex` runs the same deterministic mapping
derivation, and mapped ingest binds the reviewed plan fingerprint before it
accepts a document. Global BM25 statistics, vector score identity, hybrid
fusion, tie order, live-document admission, CEL, facets, and query-stream
completion all remain coordinator behavior.

## No-egress boundary

No-egress is enforced by construction rather than by a convention:

1. `EmbeddedSearchConfig` accepts shard configurations, not node addresses.
2. Every coordinator channel is preloaded with an in-memory duplex connector.
3. An embedded coordinator rejects a missing channel instead of falling back
   to TCP, and both DNS resolution and the UDP floor-hint lane are disabled.
4. Every shard and the coordinator are forced to the in-process `native`
   analyzer. A sidecar URL is rejected before any service task starts.
5. Embeddings enter as caller-supplied vectors or fields in mapped protobuf
   documents. Embedded startup never configures a remote embedding provider.

The dependency graph still contains tonic, HTTP/2, and Tokio networking code
because the embedded and network products intentionally share generated
services and handlers. The embedded runtime constructs only duplex streams;
it needs no reachable host, listener, DNS, UDP, or remote service.

## Create and open

Use `create` when a host expects a new private index. It refuses to overwrite
an existing provider image, BM25/exact/live sidecar, snapshot generation, or
WAL. Use `open` for normal application startup; an absent path starts empty,
while an existing path loads the same active snapshot and sidecars as the
server binary.

```rust,no_run
use protomolt_search_embedded::{
    analyzer::body_spec, EmbeddedSearch, EmbeddedSearchConfig,
    EmbeddedShardConfig,
};
use protomolt_search_embedded::pb::AddDocumentsRequest;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let mut shard = EmbeddedShardConfig::persistent("private-search.index", 0);
shard.node.facet_fields = vec!["kind".into()];
shard.node.integer_fields = vec!["created_at".into()];

let search = EmbeddedSearch::open(EmbeddedSearchConfig::single(shard)).await?;
search
    .add_documents(
        0,
        vec![AddDocumentsRequest {
            text: "New York City hot dog".into(),
            analysis: Some(body_spec()),
            ..Default::default()
        }],
    )
    .await?;
search.flush_all().await?;
# Ok(())
# }
```

The native analyzer requires an explicit `AnalysisSpec`. This is deliberate:
the fingerprinted analyzer is persisted term identity, while the historical
`server` default can name sidecar-only behavior. Use the same explicit spec at
ingest and query time.

`EmbeddedShardConfig::persistent` enables the WAL. `in_memory` is available
for tests and disposable indexes; its flush response reports `written=false`
like an ordinary node.

## Android and iOS

The product-named crate is a small facade over the shared implementation:

```toml
[dependencies]
protomolt-search-embedded = { path = "crates/protomolt-search-embedded" }
```

It is an `rlib` so an application-specific Rust host bridge can expose the API
through JNI, UniFFI, Swift/C, Flutter, or another framework without choosing a
framework in the search engine. The host owns its async runtime and app
lifecycle; it should call `flush_all` before normal shutdown. Dropping the
runtime aborts only the in-memory service tasks.

Run every supported compile gate with:

```bash
scripts/check-mobile.sh
```

The script checks:

```bash
cargo check --locked -p protomolt-search-embedded --target aarch64-linux-android
cargo check --locked -p protomolt-search-embedded --target aarch64-apple-ios
cargo check --locked -p protomolt-search-embedded --target x86_64-apple-ios
```

These are Rust compile gates. Packaging an Android AAR or Apple XCFramework
belongs to the selected host bridge and should add device-level lifecycle,
backgrounding, disk-quota, and power tests.

## Dependency audit

The 2026-09-01 mobile enablement audit refreshed all versions allowed by the
workspace's compatibility constraints and upgraded direct unpinned consumers
to TOML 1.1, Tower 0.5, and bzip2 0.6. The `turbovec-grpc` revision now names
its current `main` merge while retaining the exact same source tree and
TurboVec `turbovec-pipestream-s17` scoring dependency.

The standalone `sidecars/route-cost` crate is now explicitly excluded from the
root workspace, matching its manifest's ownership comment. Its independent
lockfile was also refreshed to all compatible versions and its test suite was
run under that lock.

Some apparent older versions are compatibility pins, not forgotten updates:

- tonic 0.12, prost/prost-types 0.13, and tonic-build 0.12 must move together
  with `turbovec-grpc`; mixing their generated message/trait versions breaks
  the in-process clustered backend contract;
- ICU4X 2.0 and aho-corasick 1.1.4 are exact analyzer pins because their output
  affects persisted term identity; changing them is an analyzer-fingerprint
  and corpus-rebuild decision;
- TurboVec's random/statistical dependencies remain engine-owned exact pins
  because they affect persisted encoded bytes.

`cargo update --dry-run -v` is the freshness check. A future major update must
retain the server/embedded parity test and classify analyzer or index-format
changes before accepting the new lockfile.

## Verification

`tests/embedded.rs` proves:

- multi-shard embedded results equal the network service response exactly for
  lexical and dense public `Query` shapes;
- `QueryStream` emits exactly one successful terminal completion whose final
  response equals ordinary `Query`;
- descriptor planning and mapped ingest use the configured local schema;
- delete/replace state and rankings survive flush/reopen;
- create refuses existing private data;
- remote analysis configuration is rejected before startup.
