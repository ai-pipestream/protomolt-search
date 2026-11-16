//! A trusted local segment transaction and its coherent serving-state switch.
use super::*;
mod backup;
mod maintenance;
mod rewrite;
mod rows;
use crate::document_catalog::{DocumentCatalog, ProjectionRecovery};
use crate::pb::{storage::ProjectionIntent, DocumentProjectionActivation};
use crate::segments::{OpenedSegmentSet, SegmentRowRetirement, SegmentSource};
use crate::stats_identity::StatsClaim;
pub use maintenance::DocumentMaintenanceRequest;
#[cfg(test)]
pub(crate) use maintenance::{AFTER_MAINTENANCE_VERIFIED, INTERRUPT_MAINTENANCE};
#[cfg(test)]
pub(crate) use rewrite::set_rewrite_test_hooks;
use rows::{install_prepared_serving_state, read_backend_and_next_claim, PreparedServingState};

#[cfg(test)]
thread_local! { pub(crate) static FAIL_PROJECTION_DECISION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[derive(Clone, Copy)]
struct SourcePublication<'a> {
    catalog: &'a DocumentCatalog,
    index_key: &'a [u8],
    candidate: &'a StagedDocumentCandidate,
}

type PublicationBackend = Option<(usize, crate::vector::VectorBackendConfig)>;

pub(super) fn check_source_live_view(
    set: &OpenedSegmentSet,
    live: &LiveDocs,
) -> Result<(), Status> {
    let mut durable = LiveDocs::default();
    set.merge_tombstones(&mut durable)
        .map_err(Status::failed_precondition)?;
    let actual = live.words();
    if live.deleted_count() != durable.deleted_count()
        || durable.words().is_some_and(|words| {
            words.iter().enumerate().any(|(i, word)| {
                *word
                    != actual
                        .as_ref()
                        .and_then(|words| words.get(i))
                        .copied()
                        .unwrap_or(0)
            })
        })
    {
        return Err(Status::failed_precondition(
            "source publication cannot certify uncommitted runtime tombstones",
        ));
    }
    Ok(())
}

fn activation(
    intent: &ProjectionIntent,
    claim: StatsClaim,
    catalog_epoch: u64,
) -> DocumentProjectionActivation {
    let source = intent
        .source
        .as_ref()
        .expect("validated publication source");
    DocumentProjectionActivation {
        history_id: intent.history_id.clone(),
        index_key: intent.index_key.clone(),
        document_key: source.document_key.clone(),
        version: source.version,
        accepted_sequence: source.accepted_sequence,
        deleted: source.deleted,
        rows: intent.rows,
        intent_id: intent.intent_id.clone(),
        catalog_epoch,
        stats_epoch: claim.epoch,
        stats_incarnation: claim.incarnation().to_vec(),
    }
}

impl NodeServiceImpl {
    /// Join the reopened serving view with source-journal recovery. The result
    /// identifies this node's current read version, including a new incarnation
    /// after restart. A pending/dirty tail or uncommitted deletion cannot qualify.
    pub fn recover_document_projection_blocking(
        &self,
        source: &DocumentCatalog,
        index_key: &[u8],
    ) -> Result<Option<DocumentProjectionActivation>, Status> {
        if source.collection()? != self.config.collection {
            return Err(Status::failed_precondition(
                "publication source and target collections differ",
            ));
        }
        let _ingest = self.claim_ingest()?;
        let _mutation = self.mutation_gate.blocking_write();
        let _seal = self
            .seal_lock
            .lock()
            .map_err(|_| Status::internal("seal lock poisoned"))?;
        let guard = write_shard(&self.state);
        let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
            return Err(Status::failed_precondition(
                "source recovery requires a segmented target",
            ));
        };
        let claim = StatsClaim::required(guard.stats_epoch, &guard.stats_incarnation.bytes()?)?;
        if let Some(owner) = &shard.snapshot().manifest().source_owner {
            if owner.decode().map_err(Status::data_loss)? != source.index_owner(index_key)? {
                return Err(Status::failed_precondition(
                    "source recovery belongs to another index owner",
                ));
            }
        }
        self.check_segment_publication(&guard, claim, shard.snapshot().epoch())?;
        check_source_live_view(shard.snapshot(), &guard.live_docs)?;
        match source.recover_index_publication(index_key, shard.catalog()) {
            Ok(_) => {}
            Err(status) if status.code() == tonic::Code::NotFound => {}
            Err(status) => return Err(status),
        }
        source
            .current_index_publication_decision(index_key, shard.catalog())
            .map(|intent| intent.map(|intent| activation(&intent, claim, shard.snapshot().epoch())))
    }

    /// Activate one complete private candidate and record its artifact decision.
    /// Call on a blocking worker. The target read version captured at staging
    /// must still hold. This is a trusted local owner operation, not a public
    /// ingest route or a collection-wide searchable write receipt.
    pub fn publish_document_projection_blocking(
        &self,
        source: &DocumentCatalog,
        index_key: &[u8],
        candidate: &StagedDocumentCandidate,
    ) -> Result<DocumentProjectionActivation, Status> {
        if index_key.is_empty() || index_key.len() > 1024 {
            return Err(Status::invalid_argument(
                "projection index_key must contain 1 to 1024 bytes",
            ));
        }
        if source.collection()? != self.config.collection {
            return Err(Status::failed_precondition(
                "publication source and target collections differ",
            ));
        }
        let info = candidate.info();
        let expected =
            StatsClaim::required(info.target_stats_epoch, &info.target_stats_incarnation)?;
        let before = {
            let guard = read_shard(&self.state);
            guard.check_stats_epoch(expected.epoch, &expected.incarnation())?;
            let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
                return Err(Status::failed_precondition(
                    "source publication requires a segmented target",
                ));
            };
            shard.snapshot().clone()
        };
        let retired = before
            .document_retirements(&info.document_key, 65536)
            .map_err(Status::failed_precondition)?;
        let staged = candidate.segments();
        let ids: Vec<_> = (0..staged.map_or(0, OpenedSegmentSet::len))
            .map(|i| {
                format!(
                    "source-{}-{}-{i}",
                    crate::sha256::hex_digest(index_key),
                    info.accepted_sequence
                )
            })
            .collect();
        let paths = candidate_paths(staged);
        let base = before
            .manifest()
            .segments
            .last()
            .map(|m| m.end_label_exclusive())
            .transpose()
            .map_err(Status::failed_precondition)?
            .unwrap_or(0);
        let sources = candidate_sources(staged, &ids, &paths, info.accepted_sequence, base)?;
        let (claim, intent) = self.publish_rows_inner(
            expected,
            before.epoch(),
            &retired,
            sources,
            Some(SourcePublication {
                catalog: source,
                index_key,
                candidate,
            }),
        )?;
        let intent = intent.expect("source publication produced an intent");
        Ok(activation(&intent, claim, intent.after_epoch))
    }
}

/// The vector, exact-vector, postings, live-docs and retained-vector files
/// of one staged candidate segment, in that order.
type CandidatePaths = [PathBuf; 5];

fn candidate_paths(staged: Option<&OpenedSegmentSet>) -> Vec<CandidatePaths> {
    staged
        .into_iter()
        .flat_map(|set| {
            (0..set.len()).map(move |i| {
                let meta = set.metadata(i);
                let dir =
                    crate::segments::SegmentCatalog::segment_dir(set.root(), &meta.segment_id);
                [
                    dir.join(&meta.vector.file),
                    dir.join(&meta.exact_vectors.file),
                    dir.join(&meta.bm25.file),
                    dir.join(&meta.live_docs.file),
                    dir.join(
                        meta.multi_res
                            .as_ref()
                            .map_or("", |artifact| artifact.file.as_str()),
                    ),
                ]
            })
        })
        .collect()
}

/// One segment source per candidate segment, labelled contiguously from `base`.
fn candidate_sources<'a>(
    staged: Option<&'a OpenedSegmentSet>,
    ids: &'a [String],
    paths: &'a [CandidatePaths],
    generation: u64,
    mut base: u64,
) -> Result<Vec<SegmentSource<'a>>, Status> {
    let mut sources = Vec::with_capacity(paths.len());
    for (i, paths) in paths.iter().enumerate() {
        let meta = staged
            .expect("candidate paths require segments")
            .metadata(i);
        sources.push(SegmentSource {
            multi_res_path: meta.multi_res.as_ref().map(|_| paths[4].as_path()),
            segment_id: &ids[i],
            generation,
            base_label: base,
            backend_kind: &meta.backend_kind,
            vector_path: (!meta.vector.file.is_empty()).then_some(paths[0].as_path()),
            exact_vector_path: (!meta.exact_vectors.file.is_empty()).then_some(paths[1].as_path()),
            bm25_path: &paths[2],
            live_docs_path: &paths[3],
            partition_column: None,
        });
        base = base
            .checked_add(meta.rows)
            .ok_or_else(|| Status::resource_exhausted("publication row range overflows"))?;
    }
    Ok(sources)
}

#[cfg(test)]
mod tests;
