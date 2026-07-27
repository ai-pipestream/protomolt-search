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

pub mod chunked;
pub mod config;
pub mod coordinator;
pub mod merge;
pub mod node;
pub mod pb;
