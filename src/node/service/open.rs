//! Fallible persisted shard open and crash-recovery orchestration.

use crate::node::*;

struct LoadedArtifacts {
    index: Option<VectorIndex>,
    exact_vectors: Option<ExactVectorStore>,
    bm25: Option<Bm25Shard>,
    live_docs: LiveDocs,
}

/// Recover an interrupted catalog swap and compaction cutover, then refuse
/// a source-managed catalog opened under another collection or with a WAL.
/// Returns whether the catalog declares its generation.
/// Refuse a shard directory still holding retired single-image
/// artifacts, by name. Opening past them would serve an empty catalog
/// over data the operator can see on disk: the fix is a rebuild through
/// the reshard tool, never silent ignorance.
fn refuse_retired_artifacts(path: &Path) -> Result<(), String> {
    for suffix in [".bm25", ".bm25.build", ".snap", ".snap-old"] {
        let mut candidate = path.as_os_str().to_owned();
        candidate.push(suffix);
        let candidate = PathBuf::from(candidate);
        if probe_path_exists(&candidate, "retired shard artifact")? {
            return Err(format!(
                "{} is a retired single-image artifact; this build serves only the segment \
                 catalog at {}, so rebuild the shard through the reshard tool and remove the \
                 retired file",
                candidate.display(),
                segments_root(path).display()
            ));
        }
    }
    Ok(())
}

fn recover_open_paths(config: &NodeConfig) -> Result<bool, String> {
    let Some(path) = config.index_path.as_ref() else {
        return Ok(false);
    };
    refuse_retired_artifacts(path)?;
    recover_segments_swap(path)
        .map_err(|error| format!("recover segment catalog swap {}: {error}", path.display()))?;
    crate::compaction::recover_interrupted(path);
    let Some(manifest) = crate::segments::SegmentCatalog::read_manifest(&segments_root(path))?
    else {
        return Ok(false);
    };
    if let Some(owner) = &manifest.source_owner {
        let owner = owner.decode()?;
        if owner.collection != config.collection || config.wal {
            return Err(
                "source-managed index requires its original collection and WAL disabled".into(),
            );
        }
    }
    Ok(manifest.generation_declaration.is_some())
}

/// The empty tail image a flush persists at the index path so a catalog
/// without sealed vectors keeps its dimension and calibration.
fn load_root_vector_index(
    config: &NodeConfig,
    declared_generation: bool,
) -> Result<Option<VectorIndex>, String> {
    if declared_generation {
        return Ok(None);
    }
    let Some(path) = config.index_path.as_ref() else {
        return Ok(None);
    };
    if !probe_path_exists(path, "vector index")? {
        return Ok(None);
    }
    let mut loaded = VectorIndex::load(&config.vector_backend, path)
        .map_err(|error| format!("load {}: {error}", path.display()))?;
    loaded
        .prepare()
        .map_err(|error| format!("prepare {}: {error}", path.display()))?;
    Ok(Some(loaded))
}

fn open_segmented_layout(
    config: &NodeConfig,
    root: &Path,
    exact_path: &Path,
    declared_generation: bool,
    mut index: Option<VectorIndex>,
) -> Result<(Option<VectorIndex>, Option<ExactVectorStore>, Bm25Shard), String> {
    let tail = heap_store(config)?;
    let shard = SegmentedShard::open_with(root, tail, config.vector_load())
        .map_err(|error| format!("segment catalog {}: {error}", root.display()))?;
    let set = shard.snapshot().clone();
    if let Some(owner) = &set.manifest().source_owner {
        let owner = owner.decode()?;
        if owner.collection != config.collection || config.wal {
            return Err(
                "source-managed index requires its original collection and WAL disabled".into(),
            );
        }
    }
    let declared_backend = set
        .generation_declaration()
        .and_then(crate::segments::generation::backend);
    let stored_backend = match (0..set.len()).find_map(|i| set.vector(i)) {
        Some(first) => Some((
            first
                .dim_opt()
                .ok_or("segment vector image has no dimension")?,
            first
                .backend_config()
                .map_err(|error| format!("segment vector backend: {error}"))?,
        )),
        None => None,
    };
    let mut exact_vectors = None;
    if let Some((dim, backend)) = declared_backend.or(stored_backend) {
        if declared_generation && backend.backend_kind != config.vector_backend {
            return Err("generation vector backend differs from node configuration".into());
        }
        exact_vectors = Some(
            ExactVectorStore::from_segments(&set, dim)
                .map_err(|error| format!("segment exact-vector view: {error}"))?,
        );
        let tail_image = VectorIndex::from_backend_config(dim, &backend)
            .map_err(|error| format!("segment tail image: {error}"))?;
        let provider = SegmentedProvider::open(set, tail_image)
            .map_err(|error| format!("segment vectors: {error}"))?;
        index = Some(VectorIndex::from_provider(provider));
    } else if declared_generation {
        index = None;
    } else if let Some(image) = index.take() {
        if !image.is_empty() {
            return Err(format!(
                "the vector image at the index path holds {} rows outside the segment catalog \
                 {}; vectors live only in sealed segments and their tail",
                image.len(),
                root.display(),
            ));
        }
        exact_vectors = Some(ExactVectorStore::empty(image.dim_opt()));
        let provider = SegmentedProvider::adopt(set, image)
            .map_err(|error| format!("segment tail image: {error}"))?;
        index = Some(VectorIndex::from_provider(provider));
    } else if probe_path_exists(exact_path, "exact vector sidecar")? {
        exact_vectors = Some(
            ExactVectorStore::open(exact_path)
                .map_err(|error| format!("load {}: {error}", exact_path.display()))?,
        );
    }
    Ok((index, exact_vectors, Bm25Shard::Segmented(shard)))
}

fn open_persisted_live_docs(index_path: &Path) -> Result<LiveDocs, String> {
    let live_path = live_docs_sidecar_path(index_path);
    if !probe_path_exists(&live_path, "live-docs sidecar")? {
        return Ok(LiveDocs::default());
    }
    LiveDocs::open(&live_path).map_err(|error| format!("load {}: {error}", live_path.display()))
}

fn load_persisted_artifacts(
    config: &NodeConfig,
    declared_generation: bool,
    index: Option<VectorIndex>,
) -> Result<LoadedArtifacts, String> {
    let Some(index_path) = config.index_path.as_ref() else {
        return Ok(LoadedArtifacts {
            index,
            exact_vectors: None,
            bm25: None,
            live_docs: LiveDocs::default(),
        });
    };
    let (index, exact_vectors, bm25) = open_segmented_layout(
        config,
        &segments_root(index_path),
        &exact_vector_sidecar_path(index_path),
        declared_generation,
        index,
    )?;
    Ok(LoadedArtifacts {
        index,
        exact_vectors,
        bm25: Some(bm25),
        live_docs: open_persisted_live_docs(index_path)?,
    })
}

fn load_retained_vectors(
    config: &NodeConfig,
    artifacts: &LoadedArtifacts,
) -> Result<crate::multi_res::MultiResVectors, String> {
    let Some(path) = config.index_path.as_ref() else {
        return Ok(crate::multi_res::MultiResVectors::default());
    };
    let physical = artifacts.index.as_ref().map_or(0, VectorIndex::len).max(
        artifacts
            .bm25
            .as_ref()
            .map_or(0, |s| s.next_doc_id() as usize),
    );
    let payload = crate::multi_res::sidecar_path(path);
    let multi_res_vectors = if let Some(provider) =
        artifacts.index.as_ref().and_then(VectorIndex::as_segmented)
    {
        provider.snapshot().retained_vectors()?
    } else {
        crate::multi_res_storage::open_generation(&payload, physical as u64, &artifacts.live_docs)
            .map_err(|error| format!("load retained vectors {}: {error}", payload.display()))?
    };
    crate::multi_res_wal::require_coverage(
        &multi_res_vectors,
        (0..artifacts.index.as_ref().map_or(0, VectorIndex::len)).filter(|&slot| {
            artifacts
                .exact_vectors
                .as_ref()
                .is_none_or(|exact| exact.contains_row(slot))
        }),
        &artifacts.live_docs,
        &config.matryoshka_prefix_dims,
        config.matryoshka_active_dim,
    )?;
    Ok(multi_res_vectors)
}

impl NodeServiceImpl {
    /// Open a local shard using the same persisted-catalog rules as the
    /// network server. A path without a catalog starts an empty one.
    pub fn open(
        mut config: NodeConfig,
        phrase_index: Option<Arc<crate::phrases::PhraseIndex>>,
    ) -> Result<Self, String> {
        declare_placement_column(&mut config);
        declare_derived_columns(&mut config)?;
        check_placement_columns(&config)?;
        validate_column_tables(&config)?;
        let declared_generation = recover_open_paths(&config)?;
        let root_index = load_root_vector_index(&config, declared_generation)?;
        let artifacts = load_persisted_artifacts(&config, declared_generation, root_index)?;
        if let Some(store) = artifacts.bm25.as_ref() {
            check_attached_derived(&config, store.derived(), store.doc_count(), "the store")?;
        }
        check_wal_tables(artifacts.index.as_ref(), &config)?;
        if let (Some(idx), Some(expected_dim)) =
            (artifacts.index.as_ref(), config.matryoshka_active_dim)
        {
            if expected_dim > 0 && idx.dim_opt().is_some_and(|d| d != expected_dim as usize) {
                return Err(format!(
                    "persisted vector index dim {:?} disagrees with configured matryoshka dim {}",
                    idx.dim_opt(),
                    expected_dim
                ));
            }
        }
        let multi_res_vectors = load_retained_vectors(&config, &artifacts)?;
        let LoadedArtifacts {
            index,
            exact_vectors,
            bm25,
            live_docs,
        } = artifacts;
        let service = Self::new(index, config)
            .with_bm25(bm25)
            .with_exact_vectors(exact_vectors)?
            .with_live_docs(live_docs)?
            .with_phrase_index(phrase_index);
        write_shard(&service.state).multi_res_vectors = multi_res_vectors;
        Ok(service)
    }
}
