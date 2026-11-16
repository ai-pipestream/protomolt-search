//! The shard write fence that commits segment rows and installs the
//! prepared serving state.
use super::*;

pub(super) struct PreparedServingState {
    pub(super) retained: crate::multi_res::MultiResVectors,
    pub(super) index: Option<VectorIndex>,
    pub(super) exact: Option<ExactVectorStore>,
    pub(super) bm25: Bm25Shard,
    pub(super) live: LiveDocs,
}

pub(super) type PublicationSnapshot = (
    crate::segments::SegmentCatalog,
    Arc<OpenedSegmentSet>,
    LiveDocs,
    PublicationBackend,
    Option<crate::postings::StoredBinding>,
    StatsClaim,
);

pub(super) fn read_backend_and_next_claim(
    guard: &ShardState,
    exhausted_msg: &'static str,
) -> Result<(PublicationBackend, StatsClaim), Status> {
    let backend = guard
        .index
        .as_ref()
        .map(|index| {
            Ok::<_, Status>((
                index
                    .dim_opt()
                    .ok_or_else(|| Status::failed_precondition("provider has no dimension"))?,
                index
                    .backend_config()
                    .map_err(|error| Status::failed_precondition(error.to_string()))?,
            ))
        })
        .transpose()?;
    let next = guard
        .stats_epoch
        .checked_add(1)
        .ok_or_else(|| Status::resource_exhausted(exhausted_msg))?;
    let next_claim = StatsClaim::required(next, &guard.stats_incarnation.bytes()?)?;
    Ok((backend, next_claim))
}

pub(super) fn install_prepared_serving_state(
    guard: &mut ShardState,
    prepared: PreparedServingState,
    stats_epoch: u64,
) {
    guard.index = prepared.index;
    guard.exact_vectors = prepared.exact;
    guard.multi_res_vectors = prepared.retained;
    guard.mapped_binding = prepared.bm25.binding().cloned();
    guard.bm25 = Some(prepared.bm25);
    guard.live_docs = prepared.live;
    guard.parents = None;
    guard.stats_epoch = stats_epoch;
    guard.files_current = false;
}

impl NodeServiceImpl {
    /// Publish immutable segment rows and activate both search legs under one
    /// shard write fence. Call on a blocking worker, not a Tokio worker thread.
    /// This privileged local operation is not a document API: the source
    /// authority must still certify accepted versions and coordinate shards.
    /// No authentication boundary or document durability receipt is introduced.
    ///
    /// The node must have a persistent segment catalog, no mutable/frozen tail,
    /// and no WAL or pending compaction. A WAL-backed generation needs a joined
    /// source/WAL transaction; bypassing its replay history is refused.
    pub fn publish_segment_rows_blocking(
        &self,
        expected: StatsClaim,
        catalog_epoch: u64,
        retirements: &[SegmentRowRetirement],
        sources: Vec<SegmentSource<'_>>,
    ) -> Result<StatsClaim, Status> {
        self.publish_rows_inner(expected, catalog_epoch, retirements, sources, None)
            .map(|(claim, _)| claim)
    }

    pub(super) fn resolve_projection_declaration(
        &self,
        source: SourcePublication<'_>,
        old_set: &OpenedSegmentSet,
        backend: &PublicationBackend,
    ) -> Result<crate::pb::storage::IndexGenerationDeclaration, Status> {
        use crate::segments::generation;
        let mut declared = generation::from_store(
            &heap_store(&self.config).map_err(Status::failed_precondition)?,
            backend.clone(),
        )
        .map_err(Status::failed_precondition)?;
        if let Some(previous) = old_set.generation_declaration() {
            generation::check_upgrade(&declared, previous).map_err(Status::failed_precondition)?;
            declared = previous.clone();
        } else if !old_set.is_empty() {
            let previous = generation::from_reader(old_set.bm25(0), backend.clone())
                .map_err(Status::failed_precondition)?;
            generation::check_upgrade(&declared, &previous).map_err(Status::failed_precondition)?;
            declared = previous;
        }
        if let Some(staged) = source.candidate.segments().filter(|set| !set.is_empty()) {
            let mut selected = generation::backend(&declared);
            if selected.is_none() {
                if let Some(vector) = (0..staged.len()).find_map(|i| staged.vector(i)) {
                    selected = Some((
                        vector.dim_opt().ok_or_else(|| {
                            Status::failed_precondition("candidate vector dimension is absent")
                        })?,
                        vector
                            .backend_config()
                            .map_err(|e| Status::failed_precondition(e.to_string()))?,
                    ));
                }
            }
            let candidate = generation::from_reader(staged.bm25(0), selected)
                .map_err(Status::failed_precondition)?;
            generation::check_upgrade(&declared, &candidate)
                .map_err(Status::failed_precondition)?;
            declared = candidate;
        }
        Ok(declared)
    }

    pub(super) fn verify_projection_decision(
        &self,
        source: SourcePublication<'_>,
        catalog: &crate::segments::SegmentCatalog,
        intent: Option<&ProjectionIntent>,
    ) -> Result<(), Status> {
        #[cfg(test)]
        if FAIL_PROJECTION_DECISION.with(|fail| fail.replace(false)) {
            self.fence_ingest("injected interruption after activation; reopen for recovery".into());
            return Err(Status::unavailable(
                "injected interruption before source decision",
            ));
        }
        match source
            .catalog
            .recover_index_publication(source.index_key, catalog)
        {
            Ok(ProjectionRecovery::Committed(ref decided)) if Some(decided) == intent => Ok(()),
            result => {
                self.fence_ingest(
                    "source publication activation lacks a matching durable decision; reopen for recovery"
                        .into(),
                );
                Err(match result {
                    Err(status) => status,
                    Ok(_) => Status::data_loss(
                        "source publication resolved a different artifact decision",
                    ),
                })
            }
        }
    }

    pub(super) fn snapshot_for_publication(
        &self,
        expected: StatsClaim,
        catalog_epoch: u64,
        is_projection: bool,
    ) -> Result<PublicationSnapshot, Status> {
        let guard = read_shard(&self.state);
        if !is_projection {
            guard.check_index_owned_write()?;
        }
        self.check_segment_publication(&guard, expected, catalog_epoch)?;
        let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
            unreachable!()
        };
        let (backend, next_claim) =
            read_backend_and_next_claim(&guard, "publication statistics epoch exhausted")?;
        Ok((
            shard.catalog().clone(),
            shard.snapshot().clone(),
            guard.live_docs.clone(),
            backend,
            guard.mapped_binding.clone(),
            next_claim,
        ))
    }

    pub(super) fn publish_rows_inner(
        &self,
        expected: StatsClaim,
        catalog_epoch: u64,
        retirements: &[SegmentRowRetirement],
        sources: Vec<SegmentSource<'_>>,
        projection: Option<SourcePublication<'_>>,
    ) -> Result<(StatsClaim, Option<ProjectionIntent>), Status> {
        StatsClaim::required(expected.epoch, &expected.incarnation())?;
        let _ingest = self.claim_ingest()?;
        let _mutation = self.mutation_gate.blocking_write();
        let _seal = self
            .seal_lock
            .lock()
            .map_err(|_| Status::internal("seal lock poisoned"))?;
        let (catalog, old_set, old_live, backend, old_binding, next_claim) =
            self.snapshot_for_publication(expected, catalog_epoch, projection.is_some())?;
        if projection.is_some() {
            check_source_live_view(&old_set, &old_live)?;
        }
        let source_owner = projection
            .map(|s| s.catalog.index_owner(s.index_key))
            .transpose()?;
        let declaration = projection
            .map(|s| self.resolve_projection_declaration(s, &old_set, &backend))
            .transpose()?;
        let mut prepared_commit = false;
        let prepare = |set: &Arc<OpenedSegmentSet>| {
            let prepared = self.prepare_serving_state(&catalog, set, old_live, backend)?;
            if (!old_set.is_empty() || old_binding.is_some())
                && prepared.bm25.binding() != old_binding.as_ref()
            {
                return Err("publication changes the current mapped binding".into());
            }
            let guard = write_shard(&self.state);
            self.check_segment_publication(&guard, expected, catalog_epoch)
                .map_err(|status| status.to_string())?;
            prepared_commit = true;
            let intent = projection
                .map(|s| {
                    s.catalog
                        .prepare_index_publication(s.index_key, s.candidate, &old_set, set)
                        .map_err(|status| status.to_string())
                })
                .transpose()?;
            Ok((guard, prepared, intent))
        };
        let result = match projection {
            Some(s) => catalog.commit_projection_prepared(
                catalog_epoch,
                retirements,
                sources,
                s.candidate.segments().and_then(OpenedSegmentSet::binding),
                source_owner
                    .as_ref()
                    .expect("source publication has an owner"),
                declaration
                    .as_ref()
                    .expect("source publication has a declaration"),
                prepare,
            ),
            None => catalog.commit_rows_prepared(catalog_epoch, retirements, sources, prepare),
        };
        let (_, (mut guard, prepared, intent)) = result.map_err(|error| {
            if prepared_commit {
                self.fence_ingest(
                    "segment publication did not acknowledge its manifest; reopen for recovery"
                        .into(),
                );
            }
            Status::failed_precondition(error)
        })?;
        install_prepared_serving_state(&mut guard, prepared, next_claim.epoch);
        if let Some(source) = projection {
            self.verify_projection_decision(source, &catalog, intent.as_ref())?;
        }
        Ok((next_claim, intent))
    }

    pub(super) fn check_segment_publication(
        &self,
        guard: &ShardState,
        expected: StatsClaim,
        epoch: u64,
    ) -> Result<(), Status> {
        guard.check_stats_epoch(expected.epoch, &expected.incarnation())?;
        if self.config.index_path.is_none()
            || self.config.wal
            || guard.wal.is_some()
            || guard.pending_compaction.is_some()
            || self.compacting.load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Status::failed_precondition("segment publication requires a persistent catalog without WAL or active compaction"));
        }
        let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
            return Err(Status::failed_precondition(
                "segment publication requires the segmented layout",
            ));
        };
        if shard.snapshot().epoch() != epoch || shard.catalog().snapshot().epoch() != epoch {
            return Err(Status::failed_precondition(
                "segment publication catalog epoch changed",
            ));
        }
        if shard.tail().next_doc_id() != 0 || shard.frozen().is_some() {
            return Err(Status::failed_precondition(
                "segment publication requires a sealed document tail; Flush first",
            ));
        }
        if let Some(index) = guard.index.as_ref() {
            match index.as_segmented() {
                Some(provider) if !provider.tail().is_empty() || provider.frozen().is_some() => {
                    return Err(Status::failed_precondition(
                        "segment publication requires a sealed vector tail; Flush first",
                    ))
                }
                None if !index.is_empty() => {
                    return Err(Status::failed_precondition(
                        "segment publication cannot replace an unsealed vector image",
                    ))
                }
                _ => {}
            }
        } else if guard
            .exact_vectors
            .as_ref()
            .is_some_and(|exact| !exact.is_empty())
        {
            return Err(Status::failed_precondition(
                "segment publication cannot bypass a remote provider's exact-vector sidecar",
            ));
        }
        Ok(())
    }

    pub(super) fn prepare_serving_state(
        &self,
        catalog: &crate::segments::SegmentCatalog,
        set: &Arc<OpenedSegmentSet>,
        mut live: LiveDocs,
        mut backend: Option<(usize, crate::vector::VectorBackendConfig)>,
    ) -> Result<PreparedServingState, String> {
        let shard =
            SegmentedShard::from_snapshot(catalog.clone(), set.clone(), heap_store(&self.config)?)?;
        check_attached_derived(
            &self.config,
            shard.derived(),
            shard.doc_count(),
            "the publication catalog",
        )?;
        if let Some(declaration) = set.generation_declaration() {
            let declared = crate::segments::generation::backend(declaration);
            if backend
                .as_ref()
                .is_some_and(|held| declared.as_ref() != Some(held))
            {
                return Err("publication changes the established vector backend".into());
            }
            backend = declared;
        }
        if backend.is_none() {
            if let Some(first) = (0..set.len()).find_map(|i| set.vector(i)) {
                if first.descriptor().backend_kind != self.config.vector_backend {
                    return Err("publication vector backend differs from node configuration".into());
                }
                backend = Some((
                    first
                        .dim_opt()
                        .ok_or("publication provider has no dimension")?,
                    first.backend_config().map_err(|error| error.to_string())?,
                ));
            }
        }
        let (index, exact) = if let Some((dim, config)) = backend {
            if config.backend_kind != self.config.vector_backend {
                return Err("publication vector backend differs from node configuration".into());
            }
            let tail = VectorIndex::from_backend_config(dim, &config)
                .map_err(|error| error.to_string())?;
            let provider =
                SegmentedProvider::open(set.clone(), tail).map_err(|error| error.to_string())?;
            let exact =
                ExactVectorStore::from_segments(set, dim).map_err(|error| error.to_string())?;
            (Some(VectorIndex::from_provider(provider)), Some(exact))
        } else {
            (None, None)
        };
        set.merge_tombstones(&mut live)?;
        let retained = set.retained_vectors()?;
        crate::multi_res_wal::require_coverage(
            &retained,
            (0..index.as_ref().map_or(0, VectorIndex::len))
                .filter(|&slot| exact.as_ref().is_none_or(|exact| exact.contains_row(slot))),
            &live,
            &self.config.matryoshka_prefix_dims,
            self.config.matryoshka_active_dim,
        )?;
        Ok(PreparedServingState {
            retained,
            index,
            exact,
            bm25: Bm25Shard::Segmented(shard),
            live,
        })
    }
}
