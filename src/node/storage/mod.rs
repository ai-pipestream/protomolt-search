//! On-disk shard paths, write-ahead log binding, and segment tail sealing.

mod paths;
mod seal;
mod wal;

pub use self::paths::{
    exact_vector_sidecar_path, live_docs_sidecar_path, recover_segments_swap, segments_root,
};
pub(in crate::node) use self::paths::{export_staging_dir, probe_path_exists, segments_old_dir};
pub(crate) use self::paths::{generation_tmp_dir, heap_store};
pub(in crate::node) use self::wal::{
    check_wal_tables, internal_backend_config, open_wal, wire_backend_config,
    wire_backend_descriptor,
};
pub(crate) use self::wal::{wal_append_or_degrade, wal_manifest};
