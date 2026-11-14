//! On-disk shard paths, write-ahead log binding, and segment tail sealing.

mod paths;
mod seal;
mod wal;

pub use self::paths::{
    bm25_build_dir, bm25_sidecar_path, exact_vector_sidecar_path, generation_bm25, generation_dir,
    generation_exact_vectors, generation_live_docs, generation_vector, live_docs_sidecar_path,
    recover_generation, recover_segments_swap, refuse_retired_generation, segments_root,
};
pub(in crate::node) use self::paths::{export_staging_dir, probe_path_exists, segments_old_dir};
pub(crate) use self::paths::{
    generation_old_dir, generation_tmp_dir, heap_store, live_docs_storage_path, storage_paths,
};
pub(in crate::node) use self::wal::{
    check_wal_tables, internal_backend_config, open_wal, wire_backend_config,
    wire_backend_descriptor,
};
pub(crate) use self::wal::{wal_append_or_degrade, wal_manifest};
