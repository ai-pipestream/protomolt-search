//! turbovec-search: distributed top-k search over turbovec shard indexes.
//!
//! Hub-and-spoke architecture:
//!
//! - [`node`] serves [`NodeService`](pb::node_service_server::NodeService)
//!   on shard owners. Each node holds one turbovec index (a disjoint
//!   partition of the corpus) and scans it in chunks of SIMD blocks via
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
pub mod bm25;
pub mod chunked;
pub mod config;
pub mod coordinator;
pub mod court;
pub mod dataset;
pub mod fusion;
pub mod harness;
pub mod merge;
pub mod node;
pub mod pb;
pub mod postings;
pub mod reshard;
pub mod snapshot;
pub mod wal;

/// Max gRPC message size (encoding and decoding) applied to every client
/// and server this crate builds. Sized by the analysis path, not search:
/// an Analyze response carries per-sentence embeddings plus every token
/// span, roughly 10-15x the input text, and the corpus holds opinions of
/// several MB. Search responses are tiny by comparison (k=10000 hits is
/// ~160 KiB).
pub const MAX_MESSAGE_BYTES: usize = 256 * 1024 * 1024;
