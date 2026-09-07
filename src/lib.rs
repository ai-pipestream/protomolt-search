//! Pipestream Search: provider-neutral distributed lexical, vector, and
//! hybrid search.
//!
//! Hub-and-spoke architecture:
//!
//! - [`node`] serves [`NodeService`](pb::node_service_server::NodeService)
//!   on shard owners. Each node holds one provider-backed vector index (a
//!   disjoint partition of the corpus) and scans it in chunks via
//!   [`chunked::chunked_topk`].
//! - [`coordinator`] serves the client-facing
//!   [`SearchService`](pb::search_service_server::SearchService), fans each
//!   query out to every node, and merges shard top-k lists with
//!   [`merge::merge_topk`].
//!
//! Collaborative mid-query floor sharing: nodes publish their current k-th
//! best score once their top-k heap fills; the coordinator aggregates the
//! maximum ([`merge::FloorTracker`]) and pushes it back; nodes seed the next
//! chunk's `initial_threshold` with it. Because a shard's k-th best is a
//! lower bound on the global k-th best, pruning at-or-below the shared floor
//! never drops a true global top-k hit — the mechanism is lossless.

// tonic Status is the natural error type throughout this gRPC crate; the
// generated service traits themselves return Result<_, Status>, so boxing
// it to satisfy result_large_err would only add allocations.
#![allow(clippy::result_large_err)]

pub mod analyzer;
pub mod authorization;
pub mod bm25;
pub mod boolean_bits;
pub mod calendar;
pub mod cel;
pub mod chunked;
pub mod clustered_turbovec;
pub mod collections;
mod column_stats;
pub mod compaction;
pub mod config;
#[cfg(feature = "net")]
pub mod console;
pub mod control_plane;
pub mod coordinator;
pub mod demo;
pub mod dense_policy;
pub mod diagnostics;
pub mod document_catalog;
pub mod document_contract;
pub mod embedded;
pub mod error_disclosure;
pub mod exact_vectors;
pub mod explain;
mod field_permissions;
pub mod filter;
pub mod fusion;
pub mod geo;
#[cfg(feature = "net")]
pub mod harness;
pub mod highlight;
pub mod index_contract;
mod integer_map;
pub mod interleave;
mod lineage;
pub mod link;
pub mod live_docs;
pub mod ltr;
mod mapped_analysis;
pub mod mapped_vector;
pub mod mapping;
pub mod merge;
pub mod metrics;
pub mod node;
#[cfg(feature = "net")]
pub mod node_agent;
pub mod pb;
pub mod phrases;
pub mod placement;
pub mod placement_plan;
pub mod postings;
mod protobuf;
pub mod proximity;
pub mod quality;
pub mod query;
mod query_cursor;
mod query_disclosure;
mod query_identity;
mod rangefacet;
pub mod rankdiff;
pub mod relay;
#[cfg(feature = "net")]
pub mod replication;
pub mod reshard;
pub mod scorefn;
pub mod security;
pub mod segment_prune;
pub mod segmented;
pub mod segmented_vectors;
pub mod segments;
pub mod sha256;
#[cfg(feature = "net")]
pub mod snapshot;
pub mod snapshot_repository;
pub mod sortkeys;
pub mod source_archive;
pub mod stats_cache;
pub mod stats_identity;
mod stream_signal;
pub mod synonyms;
pub mod values;
pub mod vector;
pub mod visibility;
pub mod vocab;
pub mod wal;

/// Max gRPC message size (encoding and decoding) applied to every client
/// and server this crate builds. Sized by the analysis path, not search:
/// an Analyze response carries per-sentence embeddings plus every token
/// span, roughly 10-15x the input text, and the corpus holds opinions of
/// several MB. Search responses are tiny by comparison (k=10000 hits is
/// ~160 KiB).
pub const MAX_MESSAGE_BYTES: usize = 256 * 1024 * 1024;

/// HTTP/2 flow-control windows, applied to every server and client
/// channel this crate builds. The defaults (64 KiB stream window) sit
/// BELOW one full pre-floor stream batch (12 B x 8192-row calibration
/// block = 96 KiB), so every burst stalled on window-update round
/// trips — pure chattiness. Sized to the batch geometry: the stream
/// window carries ~20 full-block batches without a round trip, the
/// connection window two such streams.
pub const H2_STREAM_WINDOW: u32 = 2 * 1024 * 1024;
/// See [`H2_STREAM_WINDOW`].
pub const H2_CONN_WINDOW: u32 = 4 * 1024 * 1024;

pub(crate) mod vector_read;
