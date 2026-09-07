//! A coherent source-owned physical cutover and its recovery observation.
use super::*;
use crate::document_catalog::MaintenanceRecovery;
use crate::pb::{storage::MaintenanceIntent, DocumentMaintenanceActivation};

#[cfg(test)]
thread_local! {
    // 1: durable intent, before manifest. 2: activated manifest, before decision.
    pub(crate) static AFTER_MAINTENANCE_VERIFIED: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    pub(crate) static INTERRUPT_MAINTENANCE: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
}
fn observation(intent: &MaintenanceIntent, claim: StatsClaim) -> DocumentMaintenanceActivation {
    let owner = intent.owner.as_ref().expect("validated maintenance owner");
    DocumentMaintenanceActivation {
        history_id: owner.history_id.clone(),
        index_key: owner.index_key.clone(),
        accepted_sequence: intent.accepted_sequence,
        source_intent_id: intent.source_intent_id.clone(),
        maintenance_intent_id: intent.intent_id.clone(),
        catalog_epoch: intent.after_epoch,
        stats_epoch: claim.epoch,
        stats_incarnation: claim.incarnation().to_vec(),
        live_rows: intent
            .preservation
            .as_ref()
            .expect("validated preservation proof")
            .live_rows,
    }
}

impl NodeServiceImpl {
    /// Publish rebuilt, immutable segments through the source owner's journal
    /// and one serving-state fence. All live source identities and stored query
    /// content must survive. The owner calls this on a blocking worker; this is
    /// not a public ingest/administration RPC or a source write receipt.
    ///
    /// Source writes may continue during the caller's private build. Its captured
    /// read version must still hold here. Copying and preservation verification
    /// run without mutation fences; both reads and source writes can continue.
    /// This operation explicitly enables journal format 2, refusing migration
    /// while another index in the source catalog has a pending source intent.
    #[allow(clippy::too_many_arguments)]
    pub fn publish_document_maintenance_blocking(
        &self,
        source: &DocumentCatalog,
        index_key: &[u8],
        expected: StatsClaim,
        catalog_epoch: u64,
        sources: Vec<SegmentSource<'_>>,
        proof_scratch: &Path,
        proof_batch_rows: usize,
    ) -> Result<DocumentMaintenanceActivation, Status> {
        if !(1..=1_048_576).contains(&proof_batch_rows) {
            return Err(Status::invalid_argument(
                "maintenance proof_batch_rows must be 1..=1048576",
            ));
        }
        if source.collection()? != self.config.collection {
            return Err(Status::failed_precondition(
                "maintenance source and target collections differ",
            ));
        }
        let owner = source.index_owner(index_key)?;
        StatsClaim::required(expected.epoch, &expected.incarnation())?;
        let (catalog, before, backend, next_claim) = {
            let guard = read_shard(&self.state);
            self.check_segment_publication(&guard, expected, catalog_epoch)?;
            let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
                unreachable!()
            };
            let held = shard.snapshot();
            if held
                .manifest()
                .source_owner
                .as_ref()
                .map(|o| o.decode())
                .transpose()
                .map_err(Status::data_loss)?
                .as_ref()
                != Some(&owner)
            {
                return Err(Status::failed_precondition(
                    "maintenance requires this source history's owned index",
                ));
            }
            check_source_live_view(held, &guard.live_docs)?;
            let backend = guard
                .index
                .as_ref()
                .map(|index| {
                    Ok::<_, Status>((
                        index.dim_opt().ok_or_else(|| {
                            Status::failed_precondition("provider has no dimension")
                        })?,
                        index
                            .backend_config()
                            .map_err(|error| Status::failed_precondition(error.to_string()))?,
                    ))
                })
                .transpose()?;
            let next = guard.stats_epoch.checked_add(1).ok_or_else(|| {
                Status::resource_exhausted("maintenance statistics epoch exhausted")
            })?;
            (
                shard.catalog().clone(),
                held.clone(),
                backend,
                StatsClaim::required(next, &guard.stats_incarnation.bytes()?)?,
            )
        };
        source
            .current_index_publication_decision(index_key, &catalog)?
            .ok_or_else(|| {
                Status::failed_precondition("maintenance requires a committed source projection")
            })?;
        let staged = catalog
            .stage_maintenance(before.clone(), sources)
            .map_err(Status::failed_precondition)?;
        let verified = source.verify_index_maintenance(
            index_key,
            &before,
            staged.snapshot(),
            proof_scratch,
            proof_batch_rows,
        )?;
        #[cfg(test)]
        if let Some(callback) = AFTER_MAINTENANCE_VERIFIED.with(|hook| hook.borrow_mut().take()) {
            callback();
        }
        let _ingest = self.claim_ingest()?;
        let _mutation = self.mutation_gate.blocking_write();
        let _seal = self
            .seal_lock
            .lock()
            .map_err(|_| Status::internal("seal lock poisoned"))?;
        {
            let guard = read_shard(&self.state);
            self.check_segment_publication(&guard, expected, catalog_epoch)?;
            check_source_live_view(&before, &guard.live_docs)?;
        }
        source.current_index_publication_decision(index_key, &catalog)?;
        source.enable_index_maintenance()?;
        let mut journal_prepared = false;
        let result = catalog.commit_maintenance_prepared(&staged, &owner, |after| {
            // Compaction renumbers rows: old positional tombstones must not be
            // carried into the new layout. Its own validated overlays apply.
            let prepared =
                self.prepare_serving_state(&catalog, after, LiveDocs::default(), backend)?;
            let intent = source
                .prepare_verified_index_maintenance(&verified)
                .map_err(|status| status.to_string())?;
            staged.retain_for_recovery();
            journal_prepared = true;
            #[cfg(test)]
            if INTERRUPT_MAINTENANCE.with(|phase| phase.get() == 1) {
                INTERRUPT_MAINTENANCE.with(|phase| phase.set(0));
                return Err("injected interruption after maintenance intent".into());
            }
            let guard = write_shard(&self.state);
            self.check_segment_publication(&guard, expected, catalog_epoch)
                .map_err(|status| status.to_string())?;
            check_source_live_view(&before, &guard.live_docs)
                .map_err(|status| status.to_string())?;
            Ok((guard, prepared, intent))
        });
        let (_, (mut guard, prepared, intent)) = match result {
            Ok(value) => value,
            Err(error) => {
                if journal_prepared {
                    self.fence_ingest(
                        "maintenance has an unresolved durable intent; reopen for recovery".into(),
                    );
                }
                return Err(Status::failed_precondition(error));
            }
        };
        guard.index = prepared.index;
        guard.exact_vectors = prepared.exact;
        guard.mapped_binding = prepared.bm25.binding().cloned();
        guard.bm25 = Some(prepared.bm25);
        guard.live_docs = prepared.live;
        guard.parents = None;
        guard.stats_epoch = next_claim.epoch;
        guard.files_current = false;
        #[cfg(test)]
        if INTERRUPT_MAINTENANCE.with(|phase| phase.get() == 2) {
            INTERRUPT_MAINTENANCE.with(|phase| phase.set(0));
            self.fence_ingest(
                "maintenance activation lacks its durable decision; reopen for recovery".into(),
            );
            return Err(Status::unavailable(
                "injected interruption after maintenance activation",
            ));
        }
        match source.recover_index_maintenance(index_key, &catalog) {
            Ok(MaintenanceRecovery::Committed(decision)) if decision == intent => {}
            outcome => {
                self.fence_ingest(
                    "maintenance activation lacks a matching durable decision; reopen for recovery"
                        .into(),
                );
                return Err(match outcome {
                    Err(status) => status,
                    Ok(_) => {
                        Status::data_loss("maintenance resolved a different artifact decision")
                    }
                });
            }
        }
        drop(guard);
        // Held query snapshots retain their opened images. Reclamation is only
        // attempted after the source journal acknowledges the committed layout.
        crate::segments::remove_segment_dirs(before.root(), &before.manifest().segments);
        Ok(observation(&intent, next_claim))
    }

    /// Join a reopened node's exact serving view to source maintenance recovery.
    /// Returns None after an aborted first maintenance or a later source write.
    /// A pending source publication is recovered through its source API first.
    pub fn recover_document_maintenance_blocking(
        &self,
        source: &DocumentCatalog,
        index_key: &[u8],
    ) -> Result<Option<DocumentMaintenanceActivation>, Status> {
        if source.collection()? != self.config.collection {
            return Err(Status::failed_precondition(
                "maintenance source and target collections differ",
            ));
        }
        let owner = source.index_owner(index_key)?;
        let _ingest = self.claim_ingest()?;
        let _mutation = self.mutation_gate.blocking_write();
        let _seal = self
            .seal_lock
            .lock()
            .map_err(|_| Status::internal("seal lock poisoned"))?;
        let guard = write_shard(&self.state);
        let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
            return Err(Status::failed_precondition(
                "maintenance recovery requires a segmented target",
            ));
        };
        let claim = StatsClaim::required(guard.stats_epoch, &guard.stats_incarnation.bytes()?)?;
        self.check_segment_publication(&guard, claim, shard.snapshot().epoch())?;
        if shard
            .snapshot()
            .manifest()
            .source_owner
            .as_ref()
            .map(|o| o.decode())
            .transpose()
            .map_err(Status::data_loss)?
            .as_ref()
            != Some(&owner)
        {
            return Err(Status::failed_precondition(
                "maintenance recovery belongs to another index owner",
            ));
        }
        check_source_live_view(shard.snapshot(), &guard.live_docs)?;
        source.recover_index_maintenance(index_key, shard.catalog())?;
        source
            .current_index_maintenance_decision(index_key, shard.catalog())
            .map(|intent| intent.map(|intent| observation(&intent, claim)))
    }
}
