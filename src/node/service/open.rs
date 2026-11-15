//! Fallible persisted shard open and crash-recovery orchestration.

use crate::node::*;

struct RecoveredOpenPaths {
    generation: Option<PathBuf>,
    declared_generation: bool,
}

struct LoadedArtifacts {
    index: Option<VectorIndex>,
    exact_vectors: Option<ExactVectorStore>,
    bm25: Option<Bm25Shard>,
    live_docs: LiveDocs,
}

fn recover_open_paths(config: &NodeConfig) -> Result<RecoveredOpenPaths, String> {
    let Some(path) = config.index_path.as_ref() else {
        return Ok(RecoveredOpenPaths {
            generation: None,
            declared_generation: false,
        });
    };
    refuse_retired_generation(path)?;
    recover_segments_swap(path)
        .map_err(|error| format!("recover segment catalog swap {}: {error}", path.display()))?;
    let mut declared_generation = false;
    if let Some(manifest) = crate::segments::SegmentCatalog::read_manifest(&segments_root(path))? {
        declared_generation = manifest.generation_declaration.is_some();
        if let Some(owner) = &manifest.source_owner {
            let owner = owner.decode()?;
            if owner.collection != config.collection
                || config.wal
                || config.layout != Layout::Segments
                || probe_path_exists(&generation_dir(path), "snapshot generation")?
                || probe_path_exists(&generation_old_dir(path), "snapshot backup")?
            {
                return Err("source-managed index requires its original collection, segmented layout, WAL disabled and no competing snapshot generation".into());
            }
        }
    }
    let generation = try_recover_generation(path).map_err(|error| error.to_string())?;
    Ok(RecoveredOpenPaths {
        generation,
        declared_generation,
    })
}

fn load_root_vector_index(
    config: &NodeConfig,
    recovered: &RecoveredOpenPaths,
) -> Result<Option<VectorIndex>, String> {
    if recovered.declared_generation {
        return Ok(None);
    }
    let Some(index_path) = config.index_path.as_ref() else {
        return Ok(None);
    };
    let path = recovered
        .generation
        .as_ref()
        .map_or_else(|| index_path.clone(), |dir| generation_vector(dir));
    if !probe_path_exists(&path, "vector index")? {
        return Ok(None);
    }
    let mut loaded = VectorIndex::load(&config.vector_backend, &path)
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
        if owner.collection != config.collection || config.wal || config.layout != Layout::Segments
        {
            return Err("source-managed index requires its original collection, segmented layout and WAL disabled".into());
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
    } else if index.as_ref().is_some_and(VectorIndex::is_empty) {
        exact_vectors = Some(ExactVectorStore::empty(
            index.as_ref().and_then(VectorIndex::dim_opt),
        ));
    } else if probe_path_exists(exact_path, "exact vector sidecar")? {
        exact_vectors = Some(
            ExactVectorStore::open(exact_path)
                .map_err(|error| format!("load {}: {error}", exact_path.display()))?,
        );
    }
    Ok((index, exact_vectors, Bm25Shard::Segmented(shard)))
}

fn open_persisted_live_docs(
    index_path: &Path,
    generation: Option<&PathBuf>,
) -> Result<LiveDocs, String> {
    let live_path = generation.map_or_else(
        || live_docs_sidecar_path(index_path),
        |dir| generation_live_docs(dir),
    );
    if !probe_path_exists(&live_path, "live-docs sidecar")? {
        return Ok(LiveDocs::default());
    }
    LiveDocs::open(&live_path).map_err(|error| format!("load {}: {error}", live_path.display()))
}

fn load_persisted_artifacts(
    config: &NodeConfig,
    recovered: &RecoveredOpenPaths,
    mut index: Option<VectorIndex>,
    allow_missing_bm25: bool,
) -> Result<LoadedArtifacts, String> {
    let mut exact_vectors = None;
    let mut bm25 = None;
    let Some(index_path) = config.index_path.as_ref() else {
        return Ok(LoadedArtifacts {
            index,
            exact_vectors,
            bm25,
            live_docs: LiveDocs::default(),
        });
    };
    let (_, exact_path, bm25_path) = storage_paths(index_path, recovered.generation.as_ref());
    let root = segments_root(index_path);
    let catalog_exists =
        probe_path_exists(&root.join("segments.json"), "segment catalog manifest")?;
    if probe_path_exists(&exact_path, "exact vector sidecar")?
        && (!catalog_exists || recovered.generation.is_some())
    {
        exact_vectors = Some(
            ExactVectorStore::open(&exact_path)
                .map_err(|error| format!("load {}: {error}", exact_path.display()))?,
        );
    }
    let build_dir = bm25_build_dir(&bm25_path);
    let build_dir_exists = probe_path_exists(&build_dir, "BM25 build directory")?;
    if probe_path_exists(&bm25_path, "BM25 sidecar")? {
        if catalog_exists && recovered.generation.is_none() {
            return Err(format!(
                "{} has both a single-image file and a segment catalog at {}; a shard has \
                 one layout, and nothing converts on open",
                bm25_path.display(),
                root.display()
            ));
        }
        bm25 = Some(
            Bm25Shard::open(&bm25_path)
                .map_err(|error| format!("load {}: {error}", bm25_path.display()))?,
        );
    } else if recovered.generation.is_none()
        && (catalog_exists
            || (config.layout == Layout::Segments && !build_dir_exists && index.is_none()))
    {
        let (seg_idx, seg_exact, seg_bm25) = open_segmented_layout(
            config,
            &root,
            &exact_path,
            recovered.declared_generation,
            index,
        )?;
        index = seg_idx;
        exact_vectors = seg_exact;
        bm25 = Some(seg_bm25);
    } else if build_dir_exists && !allow_missing_bm25 {
        return Err(format!(
            "BM25 build directory {} exists but {} does not. The bulk build was \
             interrupted; this shard would answer lexical queries with silence, \
             which is indistinguishable from a corpus that genuinely lacks those \
             terms. Re-run ingest for this shard, or pass --allow-missing-bm25 to \
             serve it vector-only on purpose.",
            build_dir.display(),
            bm25_path.display()
        ));
    }
    let live_docs = open_persisted_live_docs(index_path, recovered.generation.as_ref())?;
    Ok(LoadedArtifacts {
        index,
        exact_vectors,
        bm25,
        live_docs,
    })
}

fn load_retained_vectors(
    config: &NodeConfig,
    generation: Option<&PathBuf>,
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
    let payload = crate::multi_res::storage_path(path, generation);
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
    /// Open a local shard using the same persisted-generation rules as the
    /// network server. A missing vector/BM25 image starts an empty shard;
    /// an unfinished BM25 spill is refused unless the caller explicitly
    /// allows a vector-only recovery.
    pub fn open(
        mut config: NodeConfig,
        phrase_index: Option<Arc<crate::phrases::PhraseIndex>>,
        allow_missing_bm25: bool,
    ) -> Result<Self, String> {
        declare_placement_column(&mut config);
        declare_derived_columns(&mut config)?;
        check_placement_columns(&config)?;
        validate_column_tables(&config)?;
        let recovered = recover_open_paths(&config)?;
        let root_index = load_root_vector_index(&config, &recovered)?;
        let artifacts =
            load_persisted_artifacts(&config, &recovered, root_index, allow_missing_bm25)?;
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
        let multi_res_vectors =
            load_retained_vectors(&config, recovered.generation.as_ref(), &artifacts)?;
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
            .with_phrase_index(phrase_index)
            .with_generation(recovered.generation);
        write_shard(&service.state).multi_res_vectors = multi_res_vectors;
        Ok(service)
    }
}
