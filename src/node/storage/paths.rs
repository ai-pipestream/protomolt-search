//! Sidecar, segment catalog, and snapshot generation filesystem paths and recovery.

use crate::node::*;

/// The segment catalog root of a persisted shard: `<index>.segments/`.
pub fn segments_root(index_path: &std::path::Path) -> PathBuf {
    let mut name = index_path
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_default();
    name.push(".segments");
    index_path.with_file_name(name)
}

/// The heap store a node's configuration declares: the tail of a
/// segmented shard, or the whole store of an in-memory one.
pub(crate) fn heap_store(config: &NodeConfig) -> Result<Bm25Store, String> {
    validate_column_tables(config)?;
    let names: Vec<&str> = config.bm25_fields.iter().map(String::as_str).collect();
    for name in config.position_fields.iter().chain(&config.sentence_fields) {
        if !names.contains(&name.as_str()) {
            return Err(format!(
                "field {name:?} is not in this shard's BM25 field table {names:?}"
            ));
        }
    }
    let facets: Vec<&str> = config.facet_fields.iter().map(String::as_str).collect();
    let numerics: Vec<&str> = config.numeric_fields.iter().map(String::as_str).collect();
    let map_facets: Vec<&str> = config.map_facet_fields.iter().map(String::as_str).collect();
    let map_numerics: Vec<&str> = config
        .map_numeric_fields
        .iter()
        .map(String::as_str)
        .collect();
    let map_integers: Vec<&str> = config
        .map_integer_fields
        .iter()
        .map(String::as_str)
        .collect();
    let map_unsigned_integers: Vec<&str> = config
        .map_unsigned_integer_fields
        .iter()
        .map(String::as_str)
        .collect();
    let integers: Vec<&str> = config.integer_fields.iter().map(String::as_str).collect();
    let unsigned_integers: Vec<&str> = config
        .unsigned_integer_fields
        .iter()
        .map(String::as_str)
        .collect();
    let geos: Vec<&str> = config.geo_fields.iter().map(String::as_str).collect();
    let positions: Vec<&str> = config.position_fields.iter().map(String::as_str).collect();
    let sentences: Vec<&str> = config.sentence_fields.iter().map(String::as_str).collect();
    let mut store = Bm25Store::with_fields(&names)
        .with_facets(&facets)
        .with_numerics(&numerics)
        .with_map_facets(&map_facets)
        .with_map_numerics(&map_numerics)
        .with_map_integers(&map_integers)
        .with_map_unsigned_integers(&map_unsigned_integers)
        .with_integers(&integers)
        .with_unsigned_integers(&unsigned_integers)
        .with_geos(&geos)
        .with_positions(&positions)
        .with_sentences(&sentences);
    store.set_derived(stored_derived(config));
    Ok(store)
}

/// The product-owned FP32 sidecar the tail's exact vectors spill into.
pub fn exact_vector_sidecar_path(index_path: &std::path::Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".exact");
    PathBuf::from(p)
}

/// The persisted live-row overlay beside the segment catalog.
pub fn live_docs_sidecar_path(index_path: &std::path::Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".live");
    PathBuf::from(p)
}

/// Where a repository snapshot is staged before it installs
/// (`<index path>.snap-tmp/`).
pub(crate) fn generation_tmp_dir(index_path: &Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".snap-tmp");
    PathBuf::from(p)
}

/// Where a segment-layout install parks the previous catalog during
/// its swap (`<index path>.segments.snap-old`); see
/// [`recover_segments_swap`].
pub(in crate::node) fn segments_old_dir(index_path: &Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".segments.snap-old");
    PathBuf::from(p)
}

/// The private directory a `StreamSnapshot` exports into before
/// streaming (`<index path>.snap-export-<n>/`): one per stream, removed
/// when the stream ends.
pub(in crate::node) fn export_staging_dir(index_path: &Path) -> PathBuf {
    static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let mut p = index_path.as_os_str().to_owned();
    p.push(format!(
        ".snap-export-{}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    PathBuf::from(p)
}

fn probe_path_exists_io(path: &Path, context: &str) -> std::io::Result<bool> {
    probe_path_exists(path, context)
        .map_err(|message| std::io::Error::new(std::io::ErrorKind::InvalidData, message))
}

fn remove_dir_if_present(path: &Path, context: &str) -> std::io::Result<()> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(std::io::Error::new(
            error.kind(),
            format!("{context} {}: {error}", path.display()),
        )),
    }
}

/// Crash recovery for a segment-layout snapshot install: the swap renames the live catalog to
/// `.segments.snap-old` and the staged one into place. `snap-old`
/// present with no live catalog means the crash fell between the two
/// renames — the previous catalog is whole, rename it back; both present
/// means the new catalog is live — delete the old one. Stray export
/// staging directories are unreceived garbage and are removed.
pub fn recover_segments_swap(index_path: &Path) -> std::io::Result<()> {
    let root = segments_root(index_path);
    let old = segments_old_dir(index_path);
    if probe_path_exists_io(&old, "segment catalog backup")? {
        if probe_path_exists_io(&root.join("segments.json"), "segment catalog manifest")? {
            remove_dir_if_present(&old, "remove retired segment catalog backup")?;
        } else {
            remove_dir_if_present(&root, "remove incomplete segment catalog")?;
            std::fs::rename(&old, &root).map_err(|error| {
                std::io::Error::new(
                    error.kind(),
                    format!(
                        "restore segment catalog backup {} to {}: {error}",
                        old.display(),
                        root.display()
                    ),
                )
            })?;
        }
    }
    if let (Some(parent), Some(name)) = (index_path.parent(), index_path.file_name()) {
        let prefix = format!("{}.snap-export-", name.to_string_lossy());
        match std::fs::read_dir(parent) {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry?;
                    if entry.file_name().to_string_lossy().starts_with(&prefix) {
                        remove_dir_if_present(&entry.path(), "remove stray snapshot export")?;
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

pub(in crate::node) fn probe_path_exists(path: &Path, context: &str) -> Result<bool, String> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                std::fs::metadata(path).map_err(|error| {
                    format!(
                        "cannot inspect {context} {} (broken or unreadable symlink target): {error}",
                        path.display()
                    )
                })?;
            }
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!(
            "cannot inspect {context} {}: {error}",
            path.display()
        )),
    }
}
