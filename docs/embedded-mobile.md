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

## Android and iOS packages

The product-named crate is a small facade over the shared implementation and
emits all three forms needed by its consumers: `rlib` for Rust applications,
`cdylib` for Android, and `staticlib` for Apple:

```toml
[dependencies]
protomolt-search-embedded = { path = "crates/protomolt-search-embedded" }
```

The stable mobile ABI exposes create/open, mapped ingest, `Query`, pull-based
`QueryStream`, flush, and close. Inputs and outputs are protobuf bytes.
`mobile.proto` owns only lifecycle envelopes and imports the ordinary
`search.proto` request and response types, so neither host wrapper contains a
second query or schema model. The Rust library owns its Tokio runtime; Android
and Apple callers do not manage one.

Android uses the platform JNI boundary through one Java class,
`ai.pipestream.search.mobile.ProtomoltSearch`. The class moves whole protobuf
messages per call, not individual hits or fields, and contains no product
logic. The AAR manifest intentionally declares no `INTERNET` permission. Build
the arm64 device and x86-64 emulator package on a host with the Android NDK:

```bash
scripts/build-android-aar.sh target/mobile/ProtomoltSearch.aar
```

Apple imports the same functions as the C module `ProtomoltSearch`; the
included `ProtomoltSearch.swift` is an optional `Data` facade. Build the arm64
device plus arm64/x86-64 simulator package for iOS 15 or newer on macOS with
Xcode:

```bash
scripts/build-apple-xcframework.sh target/mobile/ProtomoltSearch.xcframework
```

Both scripts refuse to overwrite an existing artifact. A `mobile-v*` tag or a
manual `mobile-sdk` GitHub workflow run builds both packages as downloadable
artifacts. The Apple artifact contains the XCFramework, Swift facade, and both
protobuf contracts. A branch push does not claim a mobile release.

Device suites live beside each host package. Android's instrumentation suite
loads the AAR and covers mapped ingest, unary and streaming query, flush,
close/reopen persistence, overwrite refusal, a fixture disk budget, absence of
the `INTERNET` permission, and before/after socket descriptors. Its
`@RequiresDevice` probe records CPU time, wall time, and index bytes for 100
queries. Run it with a connected arm64 device or x86-64 emulator after building
the AAR:

```bash
mobile/android/device-tests/gradlew -p mobile/android/device-tests \
  -PprotomoltAar=../../../target/mobile/ProtomoltSearch.aar \
  connectedDebugAndroidTest
```

Apple's Swift package runs the same lifecycle and socket checks through the
XCFramework. Its XCTest probe records `XCTCPUMetric`, `XCTClockMetric`, and
`XCTStorageMetric`. Build the XCFramework, open
`mobile/apple/device-tests/Package.swift` in Xcode, and run the
`ProtomoltSearchDeviceTests` scheme on both an iPhone and a simulator. Power
baselines are accepted only from repeated physical-device runs on the same
hardware and OS; simulator measurements are functional evidence, not power
evidence.

Run every supported compile gate with:

```bash
scripts/check-mobile.sh
```

The script checks:

```bash
cargo check --locked -p protomolt-search-embedded --target aarch64-linux-android
cargo check --locked -p protomolt-search-embedded --target x86_64-linux-android
cargo check --locked -p protomolt-search-embedded --target aarch64-apple-ios
cargo check --locked -p protomolt-search-embedded --target aarch64-apple-ios-sim
cargo check --locked -p protomolt-search-embedded --target x86_64-apple-ios
```

These remain fast Rust compile gates. The package workflow performs the native
link and archive steps with the real NDK and Xcode toolchains.

## Dependency audit

The 2026-09-01 mobile enablement audit refreshed all versions allowed by the
workspace's compatibility constraints and upgraded direct unpinned consumers
to TOML 1.1, Tower 0.5, and bzip2 0.6. The `turbovec-grpc` revision now names
its current `main` merge while retaining the exact same source tree and
TurboVec `turbovec-pipestream-s17` scoring dependency.

The standalone `sidecars/route-cost` crate is now explicitly excluded from the
root workspace, matching its manifest's ownership comment. Its independent
lockfile was also refreshed to Arrow/Parquet 59.3 and all compatible versions,
and its test suite was run under that lock. Forgejo and the mobile packaging
workflow use Rust 1.98.0. Their checkout and artifact actions are on the current
v7 lines and are pinned by full commit SHA rather than mutable tags. Android
packaging pins stable NDK r29; the device harness pins Gradle 9.6.1 by checked
wrapper checksum, AGP 9.2.0, API 37, and protobuf 4.36.1.

Some apparent older versions are compatibility pins, not forgotten updates:

- tonic 0.12, prost/prost-types 0.13, and tonic-build 0.12 must move together
  with `turbovec-grpc`; mixing their generated message/trait versions breaks
  the in-process clustered backend contract;
- ICU4X 2.0 and aho-corasick 1.1.4 are exact analyzer pins because their output
  affects persisted term identity; changing them is an analyzer-fingerprint
  and corpus-rebuild decision;
- TurboVec's random/statistical dependencies remain engine-owned exact pins
  because they affect persisted encoded bytes.

`cargo update --locked --dry-run -v` is the freshness check. A future major
update must retain the server/embedded parity test and classify analyzer or
index-format changes before accepting the new lockfile.

The release-level gate is `scripts/check-dependencies.sh`. It checks the root,
independent residual-IVF experiment, and route-cost lockfiles for compatible
updates; verifies that the locked TurboVec, distributed facade, and experimental
IVF revisions still match their live branch tips; rejects Python bindings from
the search/IVF graph; verifies the exact action SHAs against the newest live
major tags; and audits all three lockfiles against RustSec. Forgejo runs this
gate before the full product and experimental-provider tests in
`.forgejo/workflows/ci.yml`.

The 2026-09-01 audit used cargo-audit 0.22.2 and the current RustSec database
(1,235 advisories). It found no vulnerabilities in any lockfile. It did report
informational unmaintained warnings: `paste` through TurboVec's
statrs/nalgebra chain and the test-only CEL oracle, plus `bincode` and
`atomic-polyfill` entries in the independently owned route-cost lockfile.
Those are visible upstream-dependency risks, not silently described as a clean
bill of health; cargo-audit does not classify them as vulnerabilities.

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
