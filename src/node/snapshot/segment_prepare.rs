//! Validate every incoming segment view before replacing the serving generation.

use crate::node::*;
use crate::segments::OpenedSegmentSet;

pub(super) struct SegmentPaths {
    pub(super) catalog: PathBuf,
    pub(super) vector: PathBuf,
    pub(super) exact: PathBuf,
    pub(super) live: PathBuf,
    pub(super) retained: PathBuf,
}

impl SegmentPaths {
    pub(super) fn staged(directory: &Path) -> Self {
        Self {
            catalog: directory.join(CATALOG_DIR),
            vector: directory.join("vector.index"),
            exact: directory.join("vectors.f32"),
            live: directory.join("live-docs.bin"),
            retained: directory.join(crate::multi_res::FILE_NAME),
        }
    }

    pub(super) fn installed(path: &Path) -> Self {
        Self {
            catalog: segments_root(path),
            vector: path.to_path_buf(),
            exact: exact_vector_sidecar_path(path),
            live: live_docs_sidecar_path(path),
            retained: crate::multi_res::sidecar_path(path),
        }
    }
}

pub(super) struct PreparedSegments {
    shard: SegmentedShard,
    index: Option<VectorIndex>,
    exact: Option<ExactVectorStore>,
    live: LiveDocs,
    retained: crate::multi_res::MultiResVectors,
}

impl PreparedSegments {
    pub(super) fn open(config: &NodeConfig, paths: &SegmentPaths) -> Result<Self, Status> {
        if !paths
            .catalog
            .join("segments.json")
            .try_exists()
            .map_err(io_status)?
        {
            return Err(Status::invalid_argument(
                "segment snapshot has no catalog/segments.json",
            ));
        }
        let tail = heap_store(config).map_err(Status::failed_precondition)?;
        let shard = SegmentedShard::open_with(&paths.catalog, tail, config.vector_load())
            .map_err(|e| Status::failed_precondition(format!("snapshot catalog: {e}")))?;
        let set = shard.snapshot();
        if set.manifest().source_owner.is_some() {
            return Err(Status::failed_precondition(
                "source-managed index snapshots require a coherent source-catalog restore",
            ));
        }
        check_attached_derived(
            config,
            shard.derived(),
            shard.doc_count(),
            "snapshot catalog",
        )
        .map_err(Status::failed_precondition)?;
        let plain = load_plain(config, &paths.vector)?;
        let index = open_index(set, plain)?;
        let exact = open_exact(set, index.as_ref(), &paths.exact)?;
        let mut live = if paths.live.try_exists().map_err(io_status)? {
            LiveDocs::open(&paths.live).map_err(io_status)?
        } else {
            LiveDocs::default()
        };
        let rows = shard
            .doc_count()
            .max(index.as_ref().map_or(0, |image| image.len() as u64));
        if live.persisted_rows() > rows {
            return Err(Status::invalid_argument(
                "snapshot live-row overlay exceeds its artifacts",
            ));
        }
        set.merge_tombstones(&mut live)
            .map_err(Status::invalid_argument)?;
        let retained = open_retained(config, paths, set, index.as_ref(), &live, rows)?;
        Ok(Self {
            shard,
            index,
            exact,
            live,
            retained,
        })
    }

    pub(super) fn check_provider(&self, guard: &ShardState) -> Result<(), Status> {
        let Some(incoming) = self.index.as_ref().map(VectorIndex::descriptor) else {
            return Ok(());
        };
        let serving_kind = guard
            .index
            .as_ref()
            .map(|i| i.descriptor().backend_kind)
            .or_else(|| {
                guard
                    .wal
                    .as_ref()
                    .map(|w| w.manifest().vector_backend.clone())
                    .filter(|kind| !kind.is_empty())
            });
        if serving_kind.is_some_and(|kind| kind != incoming.backend_kind) {
            return Err(Status::failed_precondition(
                "snapshot vector provider differs from this shard",
            ));
        }
        if let Some(serving) = guard.index.as_ref().map(VectorIndex::descriptor) {
            if serving.scoring_fingerprint != incoming.scoring_fingerprint {
                return Err(Status::failed_precondition(format!(
                    "snapshot image scores under {}/{} but this shard serves {}/{}; a snapshot installs only from the same provider state",
                    incoming.backend_kind, incoming.scoring_fingerprint,
                    serving.backend_kind, serving.scoring_fingerprint,
                )));
            }
        }
        Ok(())
    }

    pub(super) fn activate(self, guard: &mut ShardState) -> (u64, u64) {
        let counts = (
            self.index.as_ref().map_or(0, |index| index.len() as u64),
            self.shard.doc_count(),
        );
        guard.mapped_binding = self.shard.binding().cloned();
        guard.bm25 = Some(Bm25Shard::Segmented(self.shard));
        guard.index = self.index;
        guard.exact_vectors = self.exact;
        guard.multi_res_vectors = self.retained;
        guard.live_docs = self.live;
        guard.parents = None;
        guard.generation = None;
        guard.advance_stats_epoch();
        counts
    }
}

fn io_status(error: std::io::Error) -> Status {
    Status::invalid_argument(format!("snapshot artifact: {error}"))
}

fn load_plain(config: &NodeConfig, path: &Path) -> Result<Option<VectorIndex>, Status> {
    if !path.try_exists().map_err(io_status)? {
        return Ok(None);
    }
    let mut image = VectorIndex::load(&config.vector_backend, path)
        .map_err(|e| Status::invalid_argument(format!("snapshot vector image: {e}")))?;
    image
        .prepare()
        .map_err(|e| Status::invalid_argument(format!("snapshot image: {e}")))?;
    Ok(Some(image))
}

fn open_index(
    set: &Arc<OpenedSegmentSet>,
    plain: Option<VectorIndex>,
) -> Result<Option<VectorIndex>, Status> {
    let Some(first) = (0..set.len()).find_map(|i| set.vector(i)) else {
        return Ok(plain);
    };
    let backend = first
        .backend_config()
        .map_err(|e| Status::invalid_argument(format!("segment backend: {e}")))?;
    let dim = first
        .dim_opt()
        .ok_or_else(|| Status::invalid_argument("segment vector image has no dimension"))?;
    let tail = VectorIndex::from_backend_config(dim, &backend)
        .map_err(|e| Status::invalid_argument(format!("segment tail image: {e}")))?;
    let provider = SegmentedProvider::open(set.clone(), tail)
        .map_err(|e| Status::invalid_argument(format!("segment vectors: {e}")))?;
    Ok(Some(VectorIndex::from_provider(provider)))
}

fn open_exact(
    set: &OpenedSegmentSet,
    index: Option<&VectorIndex>,
    path: &Path,
) -> Result<Option<ExactVectorStore>, Status> {
    let exact = if path.try_exists().map_err(io_status)? {
        let exact = ExactVectorStore::open(path).map_err(io_status)?;
        if exact.len() != index.map_or(0, VectorIndex::len)
            || exact.dim() != index.and_then(VectorIndex::dim_opt)
        {
            return Err(Status::invalid_argument(
                "snapshot exact-vector sidecar shape disagrees with provider images",
            ));
        }
        exact.verify_payload().map_err(io_status)?;
        Some(exact)
    } else {
        None
    };
    match index.and_then(VectorIndex::as_segmented) {
        Some(provider) => {
            let dim = provider
                .tail()
                .dim_opt()
                .ok_or_else(|| Status::invalid_argument("segment provider has no dimension"))?;
            ExactVectorStore::from_segments(set, dim)
                .map(Some)
                .map_err(|e| Status::invalid_argument(format!("snapshot exact-vector view: {e}")))
        }
        None => Ok(exact),
    }
}

fn open_retained(
    config: &NodeConfig,
    paths: &SegmentPaths,
    set: &OpenedSegmentSet,
    index: Option<&VectorIndex>,
    live: &LiveDocs,
    rows: u64,
) -> Result<crate::multi_res::MultiResVectors, Status> {
    let sealed = (0..set.len()).any(|i| set.vector(i).is_some());
    let retained = if sealed {
        set.retained_vectors().map_err(Status::invalid_argument)?
    } else {
        crate::multi_res_storage::open_generation(&paths.retained, rows, live)
            .map_err(|e| Status::invalid_argument(format!("snapshot retained vectors: {e}")))?
    };
    let ranges: Vec<_> = if sealed {
        set.manifest()
            .segments
            .iter()
            .filter(|meta| !meta.vector.file.is_empty())
            .map(|meta| meta.base_label as usize..(meta.base_label + meta.rows) as usize)
            .collect()
    } else {
        std::iter::once(0..index.map_or(0, VectorIndex::len)).collect()
    };
    crate::multi_res_wal::require_coverage(
        &retained,
        ranges.into_iter().flatten(),
        live,
        &config.matryoshka_prefix_dims,
        config.matryoshka_active_dim,
    )
    .map_err(Status::failed_precondition)?;
    Ok(retained)
}
