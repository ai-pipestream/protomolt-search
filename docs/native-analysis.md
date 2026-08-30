# Native lexical analysis

`protomolt-analyzer` is a small Rust crate under
[`crates/protomolt-analyzer`](../crates/protomolt-analyzer). It has no gRPC,
Tokio, TurboVec, filesystem, or operating-system dependency. The search
server uses it directly when `analysis_addr` is `native`, and a mobile host can
link the same crate without running a JVM or making a network call.

## Supported contract

The native provider implements the exact subset used by the product's named
`ingest`/`folded` and `cased` analyzers:

| Contract field | Native values |
|---|---|
| Tokenizer | `TOKENIZER_WHITESPACE` |
| Stemmer | `STEMMER_NONE`, classic `STEMMER_PORTER` |
| Term-vector mode | `MODE_FULL`, `MODE_SCORING_ONLY` |
| Term-vector source | `SOURCE_TOKENS`, `SOURCE_STEMS`, `SOURCE_NORMALIZED_STEMS` |
| Normalizers | `STRIP_INVISIBLE`, `WHITESPACE`, `ACCENT_FOLD`, `FULL_CASE_FOLD` |
| Offsets | Half-open UTF-16 offsets into the original request text |

Terms retain first-occurrence order. Full vectors retain every occurrence;
scoring-only vectors retain frequencies without offsets. Input is capped at
1 MiB, matching the sidecar.

An absent `AnalysisSpec` is not accepted by the native provider because the
named `server` analyzer means "use the sidecar's configured defaults". Any
unsupported tokenizer, stemmer, normalizer, optional quality layer, or
geography layer also fails explicitly. The native analyzer does not silently
substitute a similar algorithm.

## Server configuration

For a single-shard `both` process:

```bash
cargo run --release -- \
  --role=both \
  --index=/data/search/shard-0.tv \
  --node-listen=127.0.0.1:50051 \
  --coord-listen=127.0.0.1:50050 \
  --nodes=127.0.0.1:50051 \
  --analysis-addr=native
```

The equivalent top-level TOML value is:

```toml
analysis_addr = "native"
```

The top-level value is shared with the shard only for the single-shard
configuration. A node serving multiple shards sets `analysis_addr = "native"`
inside each `[[shards]]` entry. The coordinator also needs a top-level native
backend for query analysis.

The same backend dispatch is used by unary query analysis, streamed ingest,
mapped ingest, replay, and resharding. There is no local network service and no
loopback hop.

## OpenNLP boundary

Keep the OpenNLP sidecar when a workload needs any of these:

- static embeddings or another sidecar embedding configuration;
- sentence detection, annotations, quality, or geography layers;
- model-backed or dictionary-backed tokenizers, stemmers, and normalizers;
- an `AnalysisSpec` outside the native table above;
- the sidecar-configured `server` analyzer.

A deployment can use native lexical analysis for BM25 and a separate embedding
provider for vectors. The current `embed_text` compatibility helper uses the
OpenNLP address, so selecting `native` there fails with a message that names the
missing embedding provider.

## Compatibility and index safety

The native implementation is checked against the real OpenNLP service for the
two product analyzers, a generated Porter suffix corpus, and every Unicode
scalar. JDK 25 uses Unicode 16 normalization, category, and script behavior;
OpenNLP additionally bundles Unicode 17 full-case-fold mappings. The crate pins
the matching ICU4X data and carries the small case-fold delta explicitly.

Run the differential oracle against a live sidecar:

```bash
OPENNLP_ANALYSIS_ADDR=http://127.0.0.1:59222 \
  cargo test --test native_opennlp_conformance -- --ignored --nocapture
```

Analysis output is persisted term identity. Keep the ICU dependencies exact,
and treat any tokenizer, normalizer, or stemmer change as an index
compatibility event. Switching an existing generation between providers is
safe only after the differential test and corpus canaries pass for its exact
spec. A changed spec or changed term output requires a rebuild.

## Android and iOS

Rust supplies standard targets for both platforms. The portable crate is
validated with:

```bash
rustup target add aarch64-linux-android aarch64-apple-ios x86_64-apple-ios
cargo check -p protomolt-analyzer --target aarch64-linux-android
cargo check -p protomolt-analyzer --target aarch64-apple-ios
cargo check -p protomolt-analyzer --target x86_64-apple-ios
```

Those checks prove the analysis core compiles for Android ARM64, iOS devices,
and the Intel iOS simulator. Packaging it into an application still needs the
host bridge selected by that application, such as JNI/UniFFI for Android and a
C/UniFFI or Swift-facing wrapper for iOS. That bridge is intentionally outside
the term-analysis crate so it does not impose one mobile framework on every
consumer.
