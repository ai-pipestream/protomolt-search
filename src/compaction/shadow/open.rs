//! Staging and shadow-state opening for single-image and segmented compaction.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tonic::Status;

use crate::exact_vectors::ExactVectorStore;
use crate::live_docs::LiveDocs;
use crate::node::{Bm25Shard, NodeServiceImpl, ShardState};
use crate::segments::{
    OpenedSegmentSet, SegmentCatalog, SegmentMetadata, SegmentSetManifest, SegmentSource,
};
use crate::vector::VectorIndex;
use crate::wal::WalWriter;

use super::super::{Preflight, Shadow};

pub(in crate::compaction) struct StageImagesSpec<'a> {
    pub root: &'a Path,
    pub images: &'a [crate::reshard::ChildImage],
    pub id_generation: u64,
    pub source_generation: u64,
    pub backend_kind: &'a str,
    pub partition_column: Option<&'a str>,
}

pub(in crate::compaction) fn stage_compacted_images(
    spec: StageImagesSpec<'_>,
) -> Result<Vec<SegmentMetadata>, Status> {
    let mut live_paths = Vec::with_capacity(spec.images.len());
    for image in spec.images {
        if image.num_vectors != 0 && image.num_vectors as usize != image.row_parent_ids.len() {
            return Err(Status::failed_precondition(format!(
                "compaction output {} has {} vectors over {} rows; a segment's artifacts \
                 cover the same rows",
                image.vector_path.display(),
                image.num_vectors,
                image.row_parent_ids.len()
            )));
        }
        if image.bm25_path.is_none() {
            return Err(Status::failed_precondition(format!(
                "compaction output {} has no BM25 image; the segment layout seals documents",
                image.vector_path.display()
            )));
        }
        let path = crate::node::live_docs_sidecar_path(&image.vector_path);
        LiveDocs::default()
            .write(&path, image.row_parent_ids.len() as u64)
            .map_err(|e| Status::internal(format!("write {}: {e}", path.display())))?;
        live_paths.push(path);
    }
    let ids: Vec<String> = (0..spec.images.len())
        .map(|i| format!("cmp-{:06}-{i:04}", spec.id_generation))
        .collect();
    let sources: Vec<SegmentSource<'_>> = spec
        .images
        .iter()
        .zip(&ids)
        .zip(&live_paths)
        .map(|((image, id), live)| SegmentSource {
            multi_res_path: image.multi_res_path.as_deref(),
            segment_id: id,
            generation: spec.source_generation,
            base_label: image.slot_offset,
            backend_kind: spec.backend_kind,
            vector_path: (image.num_vectors > 0).then_some(image.vector_path.as_path()),
            exact_vector_path: (image.num_vectors > 0).then_some(image.exact_vector_path.as_path()),
            bm25_path: image.bm25_path.as_deref().expect("checked above"),
            live_docs_path: live,
            partition_column: spec.partition_column,
        })
        .collect();
    crate::segments::stage_segments(spec.root, sources)
        .map_err(|e| Status::internal(format!("stage compacted segments: {e}")))
}

pub(in crate::compaction) fn verify_compacted_vector_descriptor(
    first: &VectorIndex,
    backend_kind: &str,
    scoring_fingerprint: &str,
    provider_label: &str,
) -> Result<(), Status> {
    let d = first.descriptor();
    if d.backend_kind != backend_kind || d.scoring_fingerprint != scoring_fingerprint {
        return Err(Status::failed_precondition(format!(
            "the compacted segments score under {}/{} but the shard serves {}/{}; \
             {provider_label} does not reproduce the live generation",
            d.backend_kind, d.scoring_fingerprint, backend_kind, scoring_fingerprint
        )));
    }
    Ok(())
}

pub(in crate::compaction) fn open_segmented_vectors(
    set: &Arc<OpenedSegmentSet>,
) -> Result<Option<(VectorIndex, ExactVectorStore)>, Status> {
    let Some(first) = (0..set.len()).find_map(|i| set.vector(i)) else {
        return Ok(None);
    };
    let backend = first
        .backend_config()
        .map_err(|e| Status::internal(format!("segment vector backend: {e}")))?;
    let dim = first
        .dim_opt()
        .ok_or_else(|| Status::internal("segment vector image has no dimension"))?;
    let tail_image = VectorIndex::from_backend_config(dim, &backend)
        .map_err(|e| Status::internal(format!("segment tail image: {e}")))?;
    let provider = crate::segmented_vectors::SegmentedProvider::open(set.clone(), tail_image)
        .map_err(|e| Status::internal(format!("segment vectors: {e}")))?;
    let exact = ExactVectorStore::from_segments(set, dim)
        .map_err(|error| Status::internal(format!("compacted exact-vector view: {error}")))?;
    Ok(Some((VectorIndex::from_provider(provider), exact)))
}

fn move_artifact(from: &Path, to: &Path) -> Result<(), Status> {
    std::fs::rename(from, to)
        .map_err(|e| Status::internal(format!("move {} -> {}: {e}", from.display(), to.display())))
}

fn stage_single_image_dir(
    pre: &Preflight,
    image: &crate::reshard::ChildImage,
) -> Result<(PathBuf, crate::multi_res::MultiResVectors), Status> {
    let gen = pre.work_dir.join("generation");
    std::fs::create_dir_all(&gen)
        .map_err(|e| Status::internal(format!("mkdir {}: {e}", gen.display())))?;
    let rows = image.row_parent_ids.len() as u64;
    if image.num_vectors > 0 {
        move_artifact(&image.vector_path, &crate::node::generation_vector(&gen))?;
        move_artifact(
            &image.exact_vector_path,
            &crate::node::generation_exact_vectors(&gen),
        )?;
    }
    if let Some(path) = &image.bm25_path {
        move_artifact(path, &crate::node::generation_bm25(&gen))?;
    }
    let retained_path = gen.join(crate::multi_res::FILE_NAME);
    if let Some(path) = &image.multi_res_path {
        move_artifact(path, &retained_path)?;
        move_artifact(
            &crate::multi_res_storage::declaration_path(path),
            &crate::multi_res_storage::declaration_path(&retained_path),
        )?;
    }
    let multi_res_vectors = crate::multi_res_storage::open(&retained_path, rows)
        .map_err(|error| Status::internal(format!("open compacted retained vectors: {error}")))?;
    let live_path = crate::node::generation_live_docs(&gen);
    let live = if multi_res_vectors.is_empty() {
        LiveDocs::default()
    } else {
        LiveDocs::default().with_retained_vectors()
    };
    live.write(&live_path, rows)
        .map_err(|e| Status::internal(format!("write {}: {e}", live_path.display())))?;
    crate::postings::fsync_parent(&live_path)
        .map_err(|e| Status::internal(format!("fsync {}: {e}", gen.display())))?;
    Ok((gen, multi_res_vectors))
}

impl NodeServiceImpl {
    fn open_single_shadow_vectors(
        &self,
        pre: &Preflight,
        has_vectors: bool,
        vector_path: &Path,
        exact_path: &Path,
    ) -> Result<(Option<VectorIndex>, Option<ExactVectorStore>), Status> {
        let index = if !has_vectors {
            self.empty_configured_index(pre)?
        } else {
            let mut loaded = VectorIndex::load(&self.config.vector_backend, vector_path)
                .map_err(|e| Status::internal(format!("load {}: {e}", vector_path.display())))?;
            loaded
                .prepare()
                .map_err(|e| Status::internal(format!("prepare {}: {e}", vector_path.display())))?;
            let d = loaded.descriptor();
            if d.backend_kind != pre.backend_kind
                || d.scoring_fingerprint != pre.scoring_fingerprint
            {
                return Err(Status::failed_precondition(format!(
                    "the compacted image scores under {}/{} but the shard serves {}/{}; the WAL \
                     manifest's provider state does not reproduce the live generation",
                    d.backend_kind,
                    d.scoring_fingerprint,
                    pre.backend_kind,
                    pre.scoring_fingerprint
                )));
            }
            Some(loaded)
        };
        let exact_vectors = if has_vectors {
            Some(
                ExactVectorStore::open(exact_path)
                    .map_err(|e| Status::internal(format!("open {}: {e}", exact_path.display())))?,
            )
        } else {
            index
                .as_ref()
                .and_then(VectorIndex::dim_opt)
                .map(|dim| ExactVectorStore::spilling(exact_path, Some(dim)))
                .transpose()
                .map_err(|e| {
                    Status::internal(format!(
                        "exact-vector builder {}: {e}",
                        exact_path.display()
                    ))
                })?
        };
        Ok((index, exact_vectors))
    }

    pub(super) fn open_single_image_shadow(
        &self,
        pre: &Preflight,
        build: &crate::reshard::CompactionBuild,
        new_wal: WalWriter,
    ) -> Result<Shadow, Status> {
        let [image] = build.images.as_slice() else {
            return Err(Status::internal(format!(
                "a single-image compaction built {} images",
                build.images.len()
            )));
        };
        let (gen, multi_res_vectors) = stage_single_image_dir(pre, image)?;
        let vector_path = crate::node::generation_vector(&gen);
        let exact_path = crate::node::generation_exact_vectors(&gen);
        let bm25_path = crate::node::generation_bm25(&gen);
        let live_path = crate::node::generation_live_docs(&gen);
        let (index, exact_vectors) =
            self.open_single_shadow_vectors(pre, image.num_vectors > 0, &vector_path, &exact_path)?;
        let bm25 = self.open_single_shadow_bm25(pre, image.bm25_path.is_some(), &bm25_path)?;
        let live_docs = LiveDocs::open(&live_path)
            .map_err(|e| Status::internal(format!("open {}: {e}", live_path.display())))?;
        let mapped_binding = bm25.as_ref().and_then(|b| b.binding().cloned());
        Ok(Shadow {
            state: ShardState {
                multi_res_vectors,
                index,
                exact_vectors,
                bm25,
                live_docs,
                generation: Some(gen),
                wal: Some(new_wal),
                parents: None,
                mapped_binding,
                stats_epoch: 0,
                files_current: false,
                stats_incarnation: Default::default(),
                pending_compaction: None,
            },
            id_map: BTreeMap::new(),
            staged: Vec::new(),
            replaced: Vec::new(),
            tail_records: 0,
            epoch_at_open: 0,
        })
    }

    fn open_single_shadow_bm25(
        &self,
        pre: &Preflight,
        has_bm25: bool,
        bm25_path: &Path,
    ) -> Result<Option<Bm25Shard>, Status> {
        if has_bm25 {
            let shard = Bm25Shard::open(bm25_path)
                .map_err(|e| Status::internal(format!("open {}: {e}", bm25_path.display())))?;
            self.check_shadow_fingerprints(pre.fields.as_ref(), &shard)?;
            Ok(Some(shard))
        } else {
            let mut store =
                crate::node::heap_store(&self.config).map_err(Status::failed_precondition)?;
            store.set_binding(
                crate::reshard::read_generation_binding(&pre.gen_dir)
                    .map_err(Status::failed_precondition)?,
            );
            Ok(Some(Bm25Shard::Building(store)))
        }
    }

    pub(super) fn open_segmented_shadow(
        &self,
        pre: &Preflight,
        build: &crate::reshard::CompactionBuild,
        new_wal: WalWriter,
    ) -> Result<Shadow, Status> {
        let root = crate::node::segments_root(&pre.index_path);
        let live_manifest = SegmentCatalog::read_manifest(&root)
            .map_err(Status::internal)?
            .unwrap_or_default();
        let replaced: Vec<String> = live_manifest
            .segments
            .iter()
            .map(|s| s.segment_id.clone())
            .collect();
        let epoch_at_open = {
            let guard = crate::node::read_shard(&self.state);
            match guard.bm25.as_ref() {
                Some(Bm25Shard::Segmented(shard)) => shard.snapshot().epoch(),
                _ => live_manifest.epoch,
            }
        };
        let staged = stage_compacted_images(StageImagesSpec {
            root: &root,
            images: &build.images,
            id_generation: pre.cutoff_generation + 1,
            source_generation: epoch_at_open + 1,
            backend_kind: &pre.backend_kind,
            partition_column: pre.partition.as_deref(),
        })?;
        match self.open_staged_segmented_shadow(
            pre,
            &root,
            &staged,
            replaced,
            epoch_at_open,
            new_wal,
        ) {
            Ok(shadow) => Ok(shadow),
            Err(status) => {
                crate::segments::remove_segment_dirs(&root, &staged);
                Err(status)
            }
        }
    }

    fn open_staged_segmented_shadow(
        &self,
        pre: &Preflight,
        root: &Path,
        staged: &[SegmentMetadata],
        replaced: Vec<String>,
        epoch_at_open: u64,
        new_wal: WalWriter,
    ) -> Result<Shadow, Status> {
        let manifest = SegmentSetManifest {
            epoch: epoch_at_open,
            segments: staged.to_vec(),
            partition_key: pre.partition.clone(),
            ..Default::default()
        }
        .with_binding(
            crate::reshard::read_generation_binding(&pre.gen_dir)
                .map_err(Status::failed_precondition)?
                .as_ref(),
        )
        .map_err(Status::failed_precondition)?;
        let catalog = SegmentCatalog::open_staged(root, manifest, self.config.vector_load())
            .map_err(|e| Status::internal(format!("open the compacted set: {e}")))?;
        let tail = crate::node::heap_store(&self.config).map_err(Status::failed_precondition)?;
        let shard = crate::segmented::SegmentedShard::open_catalog(catalog, tail)
            .map_err(|e| Status::internal(format!("open the compacted shard: {e}")))?;
        let set = shard.snapshot().clone();
        let bm25 = Bm25Shard::Segmented(shard);
        self.check_shadow_fingerprints(pre.fields.as_ref(), &bm25)?;
        let mut index = None;
        let mut exact_vectors = None;
        if let Some(empty) = self.empty_configured_index(pre)? {
            exact_vectors = Some(ExactVectorStore::empty(empty.dim_opt()));
            let provider = crate::segmented_vectors::SegmentedProvider::open(set.clone(), empty)
                .map_err(|e| Status::internal(format!("segment vectors: {e}")))?;
            index = Some(VectorIndex::from_provider(provider));
        }
        if let Some(first) = (0..set.len()).find_map(|i| set.vector(i)) {
            verify_compacted_vector_descriptor(
                first,
                &pre.backend_kind,
                &pre.scoring_fingerprint,
                "the WAL manifest's provider state",
            )?;
        }
        if let Some((vec_index, exact)) = open_segmented_vectors(&set)? {
            index = Some(vec_index);
            exact_vectors = Some(exact);
        }
        let mapped_binding = bm25.binding().cloned();
        Ok(Shadow {
            state: ShardState {
                multi_res_vectors: set.retained_vectors().map_err(Status::internal)?,
                index,
                exact_vectors,
                bm25: Some(bm25),
                live_docs: LiveDocs::default(),
                generation: None,
                wal: Some(new_wal),
                parents: None,
                mapped_binding,
                stats_epoch: 0,
                files_current: false,
                stats_incarnation: Default::default(),
                pending_compaction: None,
            },
            id_map: BTreeMap::new(),
            staged: staged.to_vec(),
            replaced,
            tail_records: 0,
            epoch_at_open,
        })
    }

    fn empty_configured_index(&self, pre: &Preflight) -> Result<Option<VectorIndex>, Status> {
        let dim = pre.manifest.dim as usize;
        if dim == 0 {
            return Ok(None);
        }
        let Ok(config) = pre.manifest.backend_config() else {
            return Ok(None);
        };
        VectorIndex::from_backend_config(dim, &config)
            .map(Some)
            .map_err(|e| Status::internal(format!("construct the shadow's empty index: {e}")))
    }
}
