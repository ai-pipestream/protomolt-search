//! The vocabulary index: streaming corpus statistics accumulated inline in
//! the ingest path, from the AnalyzeStream responses the node already
//! receives — no new gRPC channel, no extra analysis pass.
//!
//! Persisted wire contract: `proto/ai/protomolt/search/storage/v1/vocab_snapshot.proto`
//! (FNV-1a + splitmix64 sketch layouts, byte-compatible with the Java
//! listener). Term identity is counted
//! exactly as produced by `turbo-client` / `protomolt-analyzer`; this module
//! accumulates, stores, aggregates, and exports corpus DF (`TCIDF001`) to
//! `TurboVocabularyService`. Every analyzed document feeds two channels —
//! TERMS (term-vector entries with their frequencies, exactly the BM25 index
//! identity) and TOKENS (raw token surface forms) — each holding a HyperLogLog
//! (cardinality), a count-min sketch (point frequencies), a space-saving top-K
//! (heavy hitters), and counters. Memory is bounded by configuration, never by
//! corpus size (~11 MiB per channel per window).
//!
//! A window rolls over at [`DEFAULT_WINDOW_DOCS`] documents (or on an
//! explicit snapshot) and is sealed as a protobuf `VocabSnapshot` at
//! `<index path>.vocab/snapshot-<seq>-<millis>.pb`, a stable on-disk
//! contract (hashes, parameters and byte layouts are pinned by the
//! cross-language vector tests).
//!
//! The listener is analytics, not a ledger: [`VocabularyListener::feed`]
//! never fails — a sketch or persistence failure loses one document's
//! counts or keeps the window unsealed, never the document's ingest.

mod count_min;
mod drift;
#[cfg(feature = "net")]
mod export;
mod hash;
mod heavy_hitters;
mod hyper_log_log;
mod listener;
mod snapshots;
mod window;

#[cfg(test)]
mod interchange_tests;
#[cfg(test)]
mod listener_tests;
#[cfg(test)]
mod sketch_tests;

pub use count_min::CountMinSketch;
pub use drift::{
    compute_channel_drift, compute_drift, load_embedding_vocabulary, states_by_channel,
    ChannelDrift, DriftMetrics,
};
#[cfg(feature = "net")]
pub use export::export_corpus_statistics_to_kserve;
pub use heavy_hitters::{HeavyHitter, HeavyHitters};
pub use hyper_log_log::HyperLogLog;
pub use listener::{VocabularyListener, DEFAULT_WINDOW_DOCS, LIVE};
pub use snapshots::{load_snapshot_file, scan_snapshot_dir, vocab_dir, SnapshotMeta};
pub use window::{merge_snapshots, WindowState};
