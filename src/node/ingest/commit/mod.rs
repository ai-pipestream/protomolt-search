//! Locked per-document row, posting, and vector commit orchestration.

mod analysis;
mod columns;
mod store;
mod vector;

use self::columns::ValidatedColumns;
use crate::node::*;
use crate::postings::AnalyzedDoc;

/// Inputs and output counters for one locked single-document commit.
struct DocumentCommit<'a> {
    doc: AddDocumentsRequest,
    analyzed: AnalyzedDoc,
    vector: Option<Vec<f32>>,
    stable_routing_key: Option<Vec<u8>>,
    added: &'a mut u64,
    first_id: &'a mut u64,
}

impl<'a> DocumentCommit<'a> {
    fn new(
        doc: AddDocumentsRequest,
        analyzed: AnalyzedDoc,
        vector: Option<Vec<f32>>,
        stable_routing_key: Option<Vec<u8>>,
        counters: (&'a mut u64, &'a mut u64),
    ) -> Self {
        Self {
            doc,
            analyzed,
            vector,
            stable_routing_key,
            added: counters.0,
            first_id: counters.1,
        }
    }
}

impl NodeServiceImpl {
    /// Apply a materialized document against a caller-held [`ShardState`]:
    /// id assignment, BM25 store insert, mapped vector commit, WAL manifest
    /// lock, and WAL record appends in lockstep.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn apply_document_locked(
        &self,
        guard: &mut ShardState,
        doc: AddDocumentsRequest,
        analyzed: AnalyzedDoc,
        vector: Option<Vec<f32>>,
        stable_routing_key: Option<Vec<u8>>,
        added: &mut u64,
        first_id: &mut u64,
    ) -> Result<(), Status> {
        let req = DocumentCommit::new(doc, analyzed, vector, stable_routing_key, (added, first_id));
        self.commit_document(guard, req)
    }

    fn commit_document(
        &self,
        guard: &mut ShardState,
        req: DocumentCommit<'_>,
    ) -> Result<(), Status> {
        guard.check_index_owned_write()?;
        validate_source_and_identity(&req.doc)?;
        // Invalidate before any fallible mutation, including builder/provider
        // creation; a refused append must not leave old files marked current.
        guard.advance_stats_epoch();

        let vector_tip = guard.index.as_ref().map_or(0, |i| i.len() as u32);
        let prepared_vector = self.prepare_mapped_vector(guard, req.vector, vector_tip)?;
        self.ensure_bm25_builder(guard)?;
        let doc_id = assign_doc_id(guard, vector_tip, prepared_vector.is_some())?;
        self.validate_and_record_analysis(guard, &req.doc, &req.analyzed)?;

        let global_id = self.config.slot_offset + u64::from(doc_id);
        {
            let shard = guard.bm25.as_ref().expect("builder just ensured");
            let columns = ValidatedColumns::validate(shard, &req.doc)?;
            if *req.added == 0 {
                *req.first_id = global_id;
            }
            let shard = guard.bm25.as_mut().expect("builder just ensured");
            store::apply_bm25_row(shard, doc_id, &req.doc, req.analyzed, &columns)?;
        }

        let logged_vector = vector::apply_and_lock_mapped_vector(guard, doc_id, prepared_vector)?;
        vector::append_committed_wal_ops(
            &mut guard.wal,
            global_id,
            req.doc,
            req.stable_routing_key,
            logged_vector,
        );
        *req.added += 1;
        Ok(())
    }

    /// Ensure `guard.bm25` holds an active builder, adopting any pre-existing
    /// plain vector index as the segmented provider's tail on a persisted shard.
    pub(in crate::node) fn ensure_bm25_builder(
        &self,
        guard: &mut ShardState,
    ) -> Result<(), Status> {
        if guard.bm25.is_some() {
            return Ok(());
        }
        let builder = self.new_builder()?;
        guard.bm25 = Some(builder);
        let snapshot = match guard.bm25.as_ref() {
            Some(Bm25Shard::Segmented(g)) => Some(g.snapshot().clone()),
            _ => None,
        };
        if let Some(set) = snapshot {
            if guard
                .index
                .as_ref()
                .is_some_and(|index| index.as_segmented().is_none())
            {
                let plain = guard.index.take().expect("checked above");
                let provider = SegmentedProvider::adopt(set, plain)
                    .map_err(|e| Status::failed_precondition(format!("{e}")))?;
                guard.index = Some(VectorIndex::from_provider(provider));
            }
        }
        Ok(())
    }
}

fn validate_source_and_identity(doc: &AddDocumentsRequest) -> Result<(), Status> {
    match &doc.original_source {
        Some(source) if source.descriptor_set.is_empty() || source.message_type.is_empty() => {
            return Err(Status::invalid_argument(
                "original source requires descriptor bytes and message type",
            ));
        }
        None if doc.source_chunk_ordinal.is_some() => {
            return Err(Status::invalid_argument(
                "source chunk ordinal requires an original source",
            ));
        }
        _ => {}
    }
    if let Some(identity) = &doc.identity {
        if doc.original_source.is_none() {
            return Err(Status::invalid_argument(
                "document identity requires an original source",
            ));
        }
        crate::source_archive::validate_identity(identity, doc.source_chunk_ordinal)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
    }
    Ok(())
}

/// Assign the next positional `doc_id` across both legs, enforcing lockstep
/// alignment when a mapped vector accompanies the document.
fn assign_doc_id(guard: &ShardState, vector_tip: u32, has_vector: bool) -> Result<u32, Status> {
    let doc_id = vector_tip.max(
        guard
            .bm25
            .as_ref()
            .expect("builder just ensured")
            .next_doc_id(),
    );
    if has_vector && u64::from(doc_id) != u64::from(vector_tip) {
        return Err(Status::failed_precondition(format!(
            "the shard's document leg is ahead of its vector leg ({doc_id} documents, \
             {vector_tip} vectors); mapped ingest appends both legs in lockstep — \
             rebuild the shard or backfill vectors with AddVectors first"
        )));
    }
    Ok(doc_id)
}
