//! Client side of `NodeService.InstallSnapshot`: push a centrally built
//! shard image (opaque provider bytes plus optional exact-vector and BM25
//! sidecars) to a node
//! over one client stream. This is the bulk-load path for pre-computed corpora:
//! build compatible provider images once, then push each artifact to its shard
//! owner instead of re-embedding per node.
//! See the proto comments on the RPC for the install rules.
//!
//! The second half is the snapshot repository's network sources: the
//! node-side fetchers behind `InstallSnapshotFrom` for a peer's
//! `StreamSnapshot` and for an HTTP(S) repository, plus the client helpers
//! for `ExportSnapshot` and `InstallSnapshotFrom`.

mod fetch;
mod http;
mod peer;
mod push;

pub use fetch::stage_from_url;
pub use http::{artifact_url, build_get};
pub use peer::{export_snapshot, install_snapshot_from, stage_from_peer};
pub use push::{
    install_snapshot, install_snapshot_generation, install_snapshot_generation_with_retained,
    install_snapshot_with_exact, SNAPSHOT_CHUNK_BYTES,
};
