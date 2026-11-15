//! Replace a validated segment catalog while its publication lock is held.

use super::segment_prepare::{PreparedSegments, SegmentPaths};
use crate::node::*;

pub(super) fn install(
    service: &NodeServiceImpl,
    guard: &mut ShardState,
    staged: &SegmentPaths,
    path: &Path,
    manifest: &RepositoryManifest,
) -> Result<InstallSnapshotResponse, Status> {
    let installed = SegmentPaths::installed(path);
    let old = segments_old_dir(path);
    replace_catalog(staged, &installed, &old)?;
    replace_sidecars(staged, &installed)?;
    crate::postings::fsync_parent(&installed.catalog)
        .map_err(|e| Status::internal(format!("fsync installed catalog: {e}")))?;
    let prepared = PreparedSegments::open(&service.config, &installed)?;
    let counts = prepared.activate(guard);
    NodeServiceImpl::rotate_wal_after_install(guard, &service.config, path, counts)?;
    cleanup_retired(&old);
    Ok(InstallSnapshotResponse {
        path: installed.catalog.display().to_string(),
        num_vectors: counts.0,
        num_documents: counts.1,
        manifest: Some(manifest.to_pb()),
    })
}

fn replace_catalog(
    staged: &SegmentPaths,
    installed: &SegmentPaths,
    old: &Path,
) -> Result<(), Status> {
    crate::postings::fsync_parent(&staged.catalog.join("segments.json"))
        .map_err(|e| Status::internal(format!("fsync staging catalog: {e}")))?;
    if old.try_exists().map_err(io_status)? {
        std::fs::remove_dir_all(old).map_err(io_status)?;
    }
    if installed.catalog.try_exists().map_err(io_status)? {
        std::fs::rename(&installed.catalog, old).map_err(io_status)?;
    }
    if let Err(error) = std::fs::rename(&staged.catalog, &installed.catalog) {
        if old.try_exists().map_err(io_status)? {
            std::fs::rename(old, &installed.catalog).map_err(|rollback| {
                Status::internal(format!(
                    "install catalog: {error}; restore prior catalog: {rollback}"
                ))
            })?;
        }
        return Err(io_status(error));
    }
    Ok(())
}

fn replace_sidecars(staged: &SegmentPaths, installed: &SegmentPaths) -> Result<(), Status> {
    let staged_declaration = crate::multi_res_storage::declaration_path(&staged.retained);
    let installed_declaration = crate::multi_res_storage::declaration_path(&installed.retained);
    for (source, destination) in [
        (&staged.vector, &installed.vector),
        (&staged.exact, &installed.exact),
        (&staged.live, &installed.live),
        (&staged.retained, &installed.retained),
        (&staged_declaration, &installed_declaration),
    ] {
        if source.try_exists().map_err(io_status)? {
            std::fs::rename(source, destination).map_err(io_status)?;
        } else if destination.try_exists().map_err(io_status)? {
            std::fs::remove_file(destination).map_err(io_status)?;
        }
    }
    Ok(())
}

fn io_status(error: std::io::Error) -> Status {
    Status::internal(format!("install segment generation: {error}"))
}

pub(super) fn cleanup_retired(path: &Path) {
    if let Err(error) = std::fs::remove_dir_all(path) {
        if error.kind() != std::io::ErrorKind::NotFound {
            eprintln!(
                "snapshot installed; cleanup {} failed: {error}",
                path.display()
            );
        }
    }
}
