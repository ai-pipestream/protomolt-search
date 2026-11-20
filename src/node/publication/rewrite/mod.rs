//! Bounded source-owned row rebuilding. Publication stays in the journaled path.
use super::*;
use crate::pb::{CompactDocumentIndexRequest, DocumentMaintenanceActivation};
use crate::postings::{AnalyzedDoc, AnalyzedField, Bm25Index, Bm25Store};
use crate::reshard::{reconstruct_document, segment_tables, ColumnTables};
use crate::segments::generation;
use prost::Message;

mod build;
mod read;
use build::{Builder, Output};
use read::read_batch;
#[cfg(test)]
mod hooks {
    use super::*;
    type Callback = Box<dyn FnOnce() + Send>;
    struct Hooks {
        built: Option<Callback>,
        finished: Callback,
    }
    static HOOKS: std::sync::Mutex<std::collections::BTreeMap<PathBuf, Hooks>> =
        std::sync::Mutex::new(std::collections::BTreeMap::new());
    pub(crate) fn install(root: PathBuf, built: Callback, finished: Callback) {
        assert!(HOOKS
            .lock()
            .unwrap()
            .insert(
                root,
                Hooks {
                    built: Some(built),
                    finished
                }
            )
            .is_none());
    }
    pub(super) fn run(root: &Path, finished: bool) {
        let callback = {
            let mut hooks = HOOKS.lock().unwrap();
            if finished {
                hooks.remove(root).map(|h| h.finished)
            } else {
                hooks.get_mut(root).and_then(|h| h.built.take())
            }
        };
        if let Some(callback) = callback {
            callback();
        }
    }
}
#[cfg(test)]
pub(crate) use hooks::install as set_rewrite_test_hooks;

struct Directory(PathBuf);
impl Directory {
    fn create(parent: &Path) -> Result<Self, Status> {
        let mut random = [0u8; 16];
        getrandom::fill(&mut random).map_err(failure)?;
        let suffix: String = random.iter().map(|b| format!("{b:02x}")).collect();
        let path = parent.join(format!(".document-rewrite-{suffix}"));
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&path).map_err(failure)?;
        Ok(Self(path))
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn failure(error: impl std::fmt::Display) -> Status {
    Status::failed_precondition(format!("source row rebuild: {error}"))
}
fn charge(used: &mut usize, bytes: usize, limit: usize) -> Result<(), Status> {
    *used = used.checked_add(bytes).ok_or_else(|| {
        Status::resource_exhausted("source row rebuild batch accounting overflow")
    })?;
    if *used > limit {
        return Err(Status::resource_exhausted(
            "source row rebuild batch_bytes exceeded",
        ));
    }
    Ok(())
}
struct Row {
    document: crate::pb::AddDocumentsRequest,
    fields: Vec<AnalyzedField>,
    vector: Option<Vec<f32>>,
    materialized: Option<Vec<u8>>,
    bytes: usize,
}

/// The request's proof batch size after validating every bound. Zero asks
/// for the default: the largest batch, 32 MiB of digests. A smaller batch
/// only repeats vocabulary passes (rewrite_proof.rs).
fn checked_proof_batch_rows(request: &CompactDocumentIndexRequest) -> Result<usize, Status> {
    if !(1..=65536).contains(&request.batch_rows)
        || !(1..=64 * 1024 * 1024).contains(&request.batch_bytes)
        || request.max_staged_bytes == 0
        || request.proof_batch_rows as usize > crate::segments::rewrite_proof::MAX_BATCH_ROWS
    {
        return Err(Status::invalid_argument("source compaction requires batch_rows 1..65536, batch_bytes 1..64MiB, positive max_staged_bytes and proof_batch_rows 0..1048576 (0 selects the 1048576-row default)"));
    }
    Ok(match request.proof_batch_rows as usize {
        0 => crate::segments::rewrite_proof::DEFAULT_PROOF_BATCH_ROWS,
        rows => rows,
    })
}

/// The vector, exact-vector, postings, live-docs and retained-vector paths
/// of each rebuilt output, in that order.
fn output_paths(outputs: &[Output]) -> Vec<[PathBuf; 5]> {
    outputs
        .iter()
        .map(|o| {
            [
                o.directory.join("vector.index"),
                o.directory.join("vectors.f32"),
                o.directory.join("documents.bm25"),
                o.directory.join("live-docs.bin"),
                o.directory.join(crate::multi_res::FILE_NAME),
            ]
        })
        .collect()
}

impl NodeServiceImpl {
    /// Rebuild live stored rows in private bounded batches, then use the source
    /// journal and serving fence for publication. Call on a blocking worker.
    /// This owner operation does not re-analyze or re-evaluate derived values.
    /// A concurrent source publication causes the finished rewrite to refuse.
    pub fn compact_document_index_blocking(
        &self,
        source: &DocumentCatalog,
        request: CompactDocumentIndexRequest,
    ) -> Result<DocumentMaintenanceActivation, Status> {
        let proof_batch_rows = checked_proof_batch_rows(&request)?;
        if source.collection()? != self.config.collection {
            return Err(failure("source and target collections differ"));
        }
        let owner = source.index_owner(&request.index_key)?;
        let (before, catalog, claim) = self.snapshot_for_rewrite(&owner)?;
        source
            .current_index_publication_decision(&request.index_key, &catalog)?
            .ok_or_else(|| failure("source compaction requires a committed source projection"))?;
        let directory = Directory::create(
            before
                .root()
                .parent()
                .ok_or_else(|| failure("catalog root has no parent"))?,
        )?;
        let mut builder = Builder {
            node: self,
            before: &before,
            directory,
            outputs: Vec::new(),
            pending: Vec::new(),
            bytes: 0,
            written: 0,
            next_row: 0,
            request: &request,
        };
        builder.build()?;
        #[cfg(test)]
        hooks::run(before.root(), false);
        let paths = output_paths(&builder.outputs);
        let next_epoch = before
            .epoch()
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("maintenance catalog epoch exhausted"))?;
        let sources = builder
            .outputs
            .iter()
            .zip(&paths)
            .map(|(output, paths)| SegmentSource {
                multi_res_path: output.retained.then_some(paths[4].as_path()),
                segment_id: &output.id,
                generation: next_epoch,
                base_label: output.base,
                backend_kind: &self.config.vector_backend,
                vector_path: output.vector.then_some(paths[0].as_path()),
                exact_vector_path: output.vector.then_some(paths[1].as_path()),
                bm25_path: &paths[2],
                live_docs_path: &paths[3],
                partition_column: before.manifest().partition_key.as_deref(),
            })
            .collect();
        let result = self.publish_document_maintenance_blocking(
            source,
            &request.index_key,
            claim,
            before.epoch(),
            sources,
            &builder.directory.0.join("proof"),
            proof_batch_rows,
        );
        drop(builder);
        #[cfg(test)]
        hooks::run(before.root(), true);
        result
    }

    /// The owned, declared generation a rewrite reads, with its catalog and
    /// the read version the finished rewrite must still hold.
    fn snapshot_for_rewrite(
        &self,
        owner: &crate::pb::storage::SourceIndexOwner,
    ) -> Result<
        (
            Arc<OpenedSegmentSet>,
            crate::segments::SegmentCatalog,
            StatsClaim,
        ),
        Status,
    > {
        let guard = read_shard(&self.state);
        let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
            return Err(failure("source compaction requires a segmented index"));
        };
        let before = shard.snapshot().clone();
        let claim = StatsClaim::required(guard.stats_epoch, &guard.stats_incarnation.bytes()?)?;
        self.check_segment_publication(&guard, claim, before.epoch())?;
        check_source_live_view(&before, &guard.live_docs)?;
        if before
            .manifest()
            .source_owner
            .as_ref()
            .map(|o| o.decode())
            .transpose()
            .map_err(failure)?
            .as_ref()
            != Some(owner)
            || before.generation_declaration().is_none()
        {
            return Err(failure(
                "source compaction requires the exact owned, declared generation",
            ));
        }
        Ok((before, shard.catalog().clone(), claim))
    }
}

#[cfg(test)]
mod tests;
