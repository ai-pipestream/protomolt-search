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

/// One maintenance publication: the source catalog and index it rewrites,
/// the stats claim and catalog epoch its build read, the rebuilt segments,
/// and where and in what batches their preservation proof runs.
pub struct DocumentMaintenanceRequest<'a> {
    pub source: &'a DocumentCatalog,
    pub index_key: &'a [u8],
    pub expected: StatsClaim,
    pub catalog_epoch: u64,
    pub sources: Vec<SegmentSource<'a>>,
    pub proof_scratch: &'a Path,
    pub proof_batch_rows: usize,
}

struct MaintenanceSnapshot {
    owner: crate::pb::storage::SourceIndexOwner,
    catalog: crate::segments::SegmentCatalog,
    before: Arc<OpenedSegmentSet>,
    backend: PublicationBackend,
    next_claim: StatsClaim,
}

impl NodeServiceImpl {
    fn validate_maintenance_request(
        &self,
        req: &DocumentMaintenanceRequest<'_>,
    ) -> Result<(), Status> {
        if !(1..=1_048_576).contains(&req.proof_batch_rows) {
            return Err(Status::invalid_argument(
                "maintenance proof_batch_rows must be 1..=1048576",
            ));
        }
        if req.source.collection()? != self.config.collection {
            return Err(Status::failed_precondition(
                "maintenance source and target collections differ",
            ));
        }
        StatsClaim::required(req.expected.epoch, &req.expected.incarnation())?;
        Ok(())
    }

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
    pub fn publish_document_maintenance_blocking(
        &self,
        req: DocumentMaintenanceRequest<'_>,
    ) -> Result<DocumentMaintenanceActivation, Status> {
        self.validate_maintenance_request(&req)?;
        let owner = req.source.index_owner(req.index_key)?;
        let snap = self.snapshot_for_maintenance(owner, req.expected, req.catalog_epoch)?;
        req.source
            .current_index_publication_decision(req.index_key, &snap.catalog)?
            .ok_or_else(|| {
                Status::failed_precondition("maintenance requires a committed source projection")
            })?;
        let staged = snap
            .catalog
            .stage_maintenance(snap.before.clone(), req.sources)
            .map_err(Status::failed_precondition)?;
        let verified = req.source.verify_index_maintenance(
            req.index_key,
            &snap.before,
            staged.snapshot(),
            req.proof_scratch,
            req.proof_batch_rows,
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
        self.recheck_maintenance_fence(&snap, req.expected, req.catalog_epoch)?;
        req.source
            .current_index_publication_decision(req.index_key, &snap.catalog)?;
        req.source.enable_index_maintenance()?;
        let mut journal_prepared = false;
        let result = snap
            .catalog
            .commit_maintenance_prepared(&staged, &snap.owner, |after| {
                let prepared = self.prepare_serving_state(
                    &snap.catalog,
                    after,
                    LiveDocs::default(),
                    snap.backend,
                )?;
                let intent = req
                    .source
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
                self.check_segment_publication(&guard, req.expected, req.catalog_epoch)
                    .map_err(|status| status.to_string())?;
                check_source_live_view(&snap.before, &guard.live_docs)
                    .map_err(|status| status.to_string())?;
                Ok((guard, prepared, intent))
            });
        let intent = self.activate_maintenance_result(
            result.map(|(_, payload)| payload),
            journal_prepared,
            req.source,
            req.index_key,
            &snap.catalog,
            snap.next_claim.epoch,
        )?;
        crate::segments::remove_segment_dirs(snap.before.root(), &snap.before.manifest().segments);
        Ok(observation(&intent, snap.next_claim))
    }

    fn recheck_maintenance_fence(
        &self,
        snap: &MaintenanceSnapshot,
        expected: StatsClaim,
        catalog_epoch: u64,
    ) -> Result<(), Status> {
        let guard = read_shard(&self.state);
        self.check_segment_publication(&guard, expected, catalog_epoch)?;
        check_source_live_view(&snap.before, &guard.live_docs)
    }

    fn snapshot_for_maintenance(
        &self,
        owner: crate::pb::storage::SourceIndexOwner,
        expected: StatsClaim,
        catalog_epoch: u64,
    ) -> Result<MaintenanceSnapshot, Status> {
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
        let (backend, next_claim) =
            read_backend_and_next_claim(&guard, "maintenance statistics epoch exhausted")?;
        Ok(MaintenanceSnapshot {
            owner,
            catalog: shard.catalog().clone(),
            before: held.clone(),
            backend,
            next_claim,
        })
    }

    fn activate_maintenance_result(
        &self,
        result: Result<
            (
                std::sync::RwLockWriteGuard<'_, ShardState>,
                PreparedServingState,
                MaintenanceIntent,
            ),
            String,
        >,
        journal_prepared: bool,
        source: &DocumentCatalog,
        index_key: &[u8],
        catalog: &crate::segments::SegmentCatalog,
        stats_epoch: u64,
    ) -> Result<MaintenanceIntent, Status> {
        let (mut guard, prepared, intent) = result.map_err(|error| {
            if journal_prepared {
                self.fence_ingest(
                    "maintenance has an unresolved durable intent; reopen for recovery".into(),
                );
            }
            Status::failed_precondition(error)
        })?;
        install_prepared_serving_state(&mut guard, prepared, stats_epoch);
        self.verify_maintenance_decision(source, index_key, catalog, &intent)?;
        drop(guard);
        Ok(intent)
    }

    fn verify_maintenance_decision(
        &self,
        source: &DocumentCatalog,
        index_key: &[u8],
        catalog: &crate::segments::SegmentCatalog,
        intent: &MaintenanceIntent,
    ) -> Result<(), Status> {
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
        match source.recover_index_maintenance(index_key, catalog) {
            Ok(MaintenanceRecovery::Committed(decision)) if &decision == intent => Ok(()),
            outcome => {
                self.fence_ingest(
                    "maintenance activation lacks a matching durable decision; reopen for recovery"
                        .into(),
                );
                Err(match outcome {
                    Err(status) => status,
                    Ok(_) => {
                        Status::data_loss("maintenance resolved a different artifact decision")
                    }
                })
            }
        }
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
