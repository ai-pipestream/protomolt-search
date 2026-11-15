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

/// The persistence path of a shard's BM25 store: `<index path>.bm25`.
pub fn bm25_sidecar_path(index_path: &std::path::Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".bm25");
    PathBuf::from(p)
}

/// The product-owned FP32 sidecar beside a legacy provider image.
pub fn exact_vector_sidecar_path(index_path: &std::path::Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".exact");
    PathBuf::from(p)
}

/// The persisted live-row overlay beside a legacy provider image.
pub fn live_docs_sidecar_path(index_path: &std::path::Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".live");
    PathBuf::from(p)
}

/// Where a bulk BM25 build spills while it runs: `<bm25 path>.build`.
///
/// A successful `Flush` removes it, so finding one beside a MISSING
/// `.bm25` is unambiguous evidence of an interrupted build — as opposed
/// to a shard that simply has no postings, which is what a vector-only
/// deployment legitimately looks like.
pub fn bm25_build_dir(bm25_path: &std::path::Path) -> PathBuf {
    let mut p = bm25_path.as_os_str().to_owned();
    p.push(".build");
    PathBuf::from(p)
}

/// Snapshot generation layout, next to the shard's configured index path.
/// Generations use `vector.index` and `documents.bm25`; a generation that
/// still holds the retired `index.tv` pair refuses to open
/// ([`refuse_retired_generation`]).
pub fn generation_dir(index_path: &Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".snap");
    PathBuf::from(p)
}

/// The provider image inside a generation directory.
pub fn generation_vector(dir: &Path) -> PathBuf {
    dir.join("vector.index")
}
/// The product-owned FP32 sidecar inside a generation directory.
pub fn generation_exact_vectors(dir: &Path) -> PathBuf {
    dir.join("vectors.f32")
}
/// The BM25 sidecar path inside a generation directory.
pub fn generation_bm25(dir: &Path) -> PathBuf {
    dir.join("documents.bm25")
}

/// Product-owned live-row overlay inside a snapshot generation.
pub fn generation_live_docs(dir: &Path) -> PathBuf {
    dir.join("live-docs.bin")
}

pub(crate) fn live_docs_storage_path(index_path: &Path, generation: Option<&PathBuf>) -> PathBuf {
    generation.map_or_else(
        || live_docs_sidecar_path(index_path),
        |dir| generation_live_docs(dir),
    )
}

/// Receive staging (`<index path>.snap-tmp/`) and swap-out
/// (`<index path>.snap-old/`) directories for the generation swap.
pub(crate) fn generation_tmp_dir(index_path: &Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".snap-tmp");
    PathBuf::from(p)
}
pub(crate) fn generation_old_dir(index_path: &Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".snap-old");
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

/// Crash recovery for a segment-layout snapshot install, the catalog
/// twin of [`try_recover_generation`]: the swap renames the live catalog to
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

/// Where the shard's files live: the active snapshot generation when one
/// was installed, else the legacy `<index path>` (+`.bm25`) layout.
/// Returns `(provider index, exact vectors, bm25)` paths.
pub(crate) fn storage_paths(
    index_path: &Path,
    generation: Option<&PathBuf>,
) -> (PathBuf, PathBuf, PathBuf) {
    match generation {
        Some(dir) => (
            generation_vector(dir),
            generation_exact_vectors(dir),
            generation_bm25(dir),
        ),
        None => (
            index_path.to_path_buf(),
            exact_vector_sidecar_path(index_path),
            bm25_sidecar_path(index_path),
        ),
    }
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

/// Fallible crash recovery for the snapshot generation swap.
///
/// Propagates retired-layout/metadata refusals and any filesystem error while
/// removing `.snap-tmp` / `.snap-old` or restoring `.snap-old` to `.snap` so
/// shard open fails closed rather than serving over half-recovered state.
pub fn try_recover_generation(index_path: &Path) -> std::io::Result<Option<PathBuf>> {
    refuse_retired_generation(index_path)
        .map_err(|message| std::io::Error::new(std::io::ErrorKind::InvalidData, message))?;
    // Roll back any interrupted compaction cutover before snapshot swap rules
    // run, so recovery never retires files the compaction rollback restores.
    crate::compaction::recover_interrupted(index_path);
    let snap = generation_dir(index_path);
    let old = generation_old_dir(index_path);
    let tmp = generation_tmp_dir(index_path);
    remove_dir_if_present(&tmp, "remove stray snapshot staging")?;
    if probe_path_exists_io(&old, "snapshot backup")? {
        if probe_path_exists_io(&snap, "snapshot generation")? {
            remove_dir_if_present(&old, "remove retired snapshot backup")?;
        } else {
            std::fs::rename(&old, &snap).map_err(|error| {
                std::io::Error::new(
                    error.kind(),
                    format!(
                        "restore snapshot backup {} to {}: {error}",
                        old.display(),
                        snap.display()
                    ),
                )
            })?;
        }
    }
    let has_vector = probe_path_exists_io(&generation_vector(&snap), "snapshot vector")?;
    let has_bm25 = probe_path_exists_io(&generation_bm25(&snap), "snapshot bm25")?;
    Ok((has_vector || has_bm25).then_some(snap))
}

/// Crash recovery for the generation swap, returning `None` when no active
/// generation exists or when [`try_recover_generation`] refuses recovery.
pub fn recover_generation(index_path: &Path) -> Option<PathBuf> {
    match try_recover_generation(index_path) {
        Ok(generation) => generation,
        Err(error) => {
            eprintln!("recover_generation {}: {error}", index_path.display());
            None
        }
    }
}

/// File names of the retired snapshot generation layout. Nothing reads
/// them; their presence means the generation was written by an older
/// binary.
const RETIRED_GENERATION_FILES: [&str; 2] = ["index.tv", "index.tv.bm25"];

/// Refuse a snapshot generation in the retired layout or with unreadable or
/// dangling entries. Without this check [`recover_generation`] would report no
/// generation and the shard would open from its root paths: empty, or from a
/// stale root image, silently abandoning the installed data. Check both the
/// active and backup generation before any recovery cleanup: the backup may be
/// a rollback source, and an unsupported or damaged path must leave all
/// recovery files for the operator.
pub fn refuse_retired_generation(index_path: &Path) -> Result<(), String> {
    for generation in [generation_dir(index_path), generation_old_dir(index_path)] {
        for name in RETIRED_GENERATION_FILES {
            let retired = generation.join(name);
            match std::fs::symlink_metadata(&retired) {
                Ok(_) => {
                    return Err(format!(
                        "{} is a snapshot generation in the retired layout ({name}); this binary \
                         reads only vector.index and documents.bm25 and does not convert it",
                        retired.display()
                    ));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(format!(
                        "cannot inspect snapshot generation entry {} before recovery: {error}",
                        retired.display()
                    ));
                }
            }
        }
        if !probe_path_exists(&generation, "snapshot generation before recovery")? {
            continue;
        }
        let meta = std::fs::metadata(&generation).map_err(|error| {
            format!(
                "cannot inspect snapshot generation {} before recovery: {error}",
                generation.display()
            )
        })?;
        if !meta.is_dir() {
            return Err(format!(
                "cannot inspect snapshot generation {} before recovery: not a directory",
                generation.display()
            ));
        }
        let entries = std::fs::read_dir(&generation).map_err(|error| {
            format!(
                "cannot inspect snapshot generation {} before recovery: {error}",
                generation.display()
            )
        })?;
        for entry in entries {
            let entry = entry.map_err(|error| {
                format!(
                    "cannot inspect snapshot generation {} before recovery: {error}",
                    generation.display()
                )
            })?;
            probe_path_exists(&entry.path(), "snapshot generation entry before recovery")?;
        }
    }
    Ok(())
}
