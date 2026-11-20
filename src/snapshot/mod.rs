//! The snapshot repository's network sources: the node-side fetchers behind
//! `InstallSnapshotFrom` for a peer's `StreamSnapshot` and for an HTTP(S)
//! repository, plus the client helpers for `ExportSnapshot` and
//! `InstallSnapshotFrom`.

mod fetch;
mod http;
mod peer;

pub use fetch::stage_from_url;
pub use http::{artifact_url, build_get};
pub use peer::{export_snapshot, install_snapshot_from, stage_from_peer};
