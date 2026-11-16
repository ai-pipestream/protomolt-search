//! Document deletion and atomic row replacement against a held shard state.

use crate::node::*;

mod binding;

impl NodeServiceImpl {
    pub(in crate::node) fn validate_vector_column_name(
        &self,
        name: &str,
        store: Option<&Bm25Shard>,
    ) -> Result<(), Status> {
        let configured = self
            .config
            .bm25_fields
            .iter()
            .chain(&self.config.facet_fields)
            .chain(&self.config.numeric_fields)
            .chain(&self.config.integer_fields)
            .chain(&self.config.unsigned_integer_fields)
            .chain(&self.config.map_facet_fields)
            .chain(&self.config.map_numeric_fields)
            .chain(&self.config.map_integer_fields)
            .chain(&self.config.map_unsigned_integer_fields)
            .chain(&self.config.geo_fields)
            .any(|column| column == name);
        let persisted = store.is_some_and(|store| {
            (0..store.field_count()).any(|field| store.field_name(field) == name)
                || store.numeric_index(name).is_some()
                || store.integer_index(name).is_some()
                || store.unsigned_integer_index(name).is_some()
                || store.facet_index(name).is_some()
                || store.map_numeric_index(name).is_some()
                || store.map_integer_index(name).is_some()
                || store.map_unsigned_integer_index(name).is_some()
                || store.map_facet_index(name).is_some()
                || store.geo_index(name).is_some()
        });
        if configured || persisted {
            return Err(Status::failed_precondition(format!("vector column {name:?} conflicts with a declared or stored text, scalar, map, or geo column; use distinct column names")));
        }
        Ok(())
    }
}

// The mutation handlers' bodies against a state the caller holds (see
// `NodeServiceImpl::apply_batch_locked`).
impl NodeServiceImpl {
    /// Refuse an ID-addressed mutation whose IDs were issued under an older WAL
    /// generation before compaction or snapshot installation renumbered rows.
    pub(in crate::node) fn check_wal_generation(
        guard: &ShardState,
        expected: Option<u64>,
    ) -> Result<u64, Status> {
        let held = guard.wal.as_ref().map_or(0, WalWriter::generation);
        if expected.is_some_and(|expected| expected != held) {
            return Err(Status::failed_precondition(format!(
                "stale WAL generation: the request's ids were issued under WAL generation \
                 {} but this shard is at {held}; a compaction or snapshot install renumbered \
                 its rows since, so those ids no longer name the same documents — resolve them \
                 again",
                expected.expect("checked above")
            )));
        }
        Ok(held)
    }

    pub(crate) fn delete_documents_locked(
        &self,
        guard: &mut ShardState,
        doc_ids: &[u64],
        expected_wal_generation: Option<u64>,
    ) -> Result<DeleteDocumentsResponse, Status> {
        guard.check_legacy_mutation()?;
        let wal_generation = Self::check_wal_generation(guard, expected_wal_generation)?;
        let offset = self.config.slot_offset;
        let rows = physical_rows(guard);
        let mut slots = Vec::with_capacity(doc_ids.len());
        for id in doc_ids {
            let local = id.checked_sub(offset).ok_or_else(|| {
                Status::invalid_argument(format!("document id {id} is below shard offset {offset}"))
            })?;
            if local >= rows {
                return Err(Status::invalid_argument(format!(
                    "document id {id} is outside this shard's {rows} physical rows"
                )));
            }
            slots.push((*id, local as usize));
        }
        let mut deleted = 0u64;
        let mut already_deleted = 0u64;
        for (id, slot) in slots {
            if guard.live_docs.delete(slot) {
                deleted += 1;
                wal_append_or_degrade(
                    &mut guard.wal,
                    wal_record::Op::DeleteDocument(LoggedDeleteDocument { doc_id: id }),
                );
            } else {
                already_deleted += 1;
            }
        }
        if deleted > 0 {
            guard.advance_stats_epoch();
            guard.parents = None;
        }
        Ok(DeleteDocumentsResponse {
            deleted,
            already_deleted,
            live_revision: guard.live_docs.revision(),
            wal_generation,
        })
    }

    pub(crate) fn commit_replacements_locked(
        &self,
        guard: &mut ShardState,
        replacements: &[Replacement],
        expected_wal_generation: Option<u64>,
    ) -> Result<CommitReplacementsResponse, Status> {
        guard.check_legacy_mutation()?;
        let wal_generation = Self::check_wal_generation(guard, expected_wal_generation)?;
        let offset = self.config.slot_offset;
        let artifact_rows = active_artifact_rows(guard);
        let rows = artifact_rows.iter().copied().max().unwrap_or(0);
        let mut seen = std::collections::HashSet::new();
        let mut pairs = Vec::with_capacity(replacements.len());
        for replacement in replacements {
            if replacement.old_doc_id == replacement.new_doc_id {
                return Err(Status::invalid_argument(
                    "replacement old and new ids must differ",
                ));
            }
            if !seen.insert(replacement.old_doc_id) || !seen.insert(replacement.new_doc_id) {
                return Err(Status::invalid_argument(
                    "a replacement batch may mention each id only once",
                ));
            }
            let old = replacement.old_doc_id.checked_sub(offset).ok_or_else(|| {
                Status::invalid_argument("replacement old id is below this shard's offset")
            })?;
            let new = replacement.new_doc_id.checked_sub(offset).ok_or_else(|| {
                Status::invalid_argument("replacement new id is below this shard's offset")
            })?;
            if old >= rows || new >= rows {
                return Err(Status::invalid_argument(
                    "replacement ids must name existing rows on this shard",
                ));
            }
            if artifact_rows
                .iter()
                .any(|artifact_rows| old >= *artifact_rows || new >= *artifact_rows)
            {
                return Err(Status::failed_precondition(
                    "replacement ids must exist in every active provider, exact-vector, and document artifact",
                ));
            }
            if guard.live_docs.is_deleted(new as usize) {
                return Err(Status::failed_precondition(format!(
                    "replacement row {} is already deleted",
                    replacement.new_doc_id
                )));
            }
            pairs.push((*replacement, old as usize));
        }
        let mut committed = 0u64;
        let mut already_committed = 0u64;
        for (replacement, old) in pairs {
            if guard.live_docs.delete(old) {
                committed += 1;
                wal_append_or_degrade(
                    &mut guard.wal,
                    wal_record::Op::Replacement(LoggedReplacement {
                        old_doc_id: replacement.old_doc_id,
                        new_doc_id: replacement.new_doc_id,
                    }),
                );
            } else {
                already_committed += 1;
            }
        }
        if committed > 0 {
            guard.advance_stats_epoch();
            guard.parents = None;
        }
        Ok(CommitReplacementsResponse {
            committed,
            already_committed,
            live_revision: guard.live_docs.revision(),
            wal_generation,
        })
    }
}
