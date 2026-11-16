//! One coalesced kernel pass: per-job admission under the shard read
//! guard, the batched scan, and delivery of every job's outcome.

use super::*;

struct QueuedQuerySpec {
    vector: Vec<f32>,
    k: usize,
    keep_ties: bool,
    include_rerank_candidate: bool,
    enumerate_max_rows: Option<usize>,
    patience: Option<crate::vector::PatienceSpec>,
    floor_prune: Option<crate::vector::FloorPruneSpec>,
}

/// The shard snapshot one batch scans.
struct BatchContext<'a> {
    guard: &'a ShardState,
    index: &'a VectorIndex,
    segment_pruning: bool,
}

/// What admission resolved for one job.
struct AdmittedJob {
    patience: Option<crate::vector::PatienceSpec>,
    floor_prune: Option<crate::vector::FloorPruneSpec>,
    allow: Option<crate::boolean_bits::Bits>,
    prune: crate::segment_prune::PruneStats,
}

impl BatchContext<'_> {
    /// Admit `job` against the CURRENT index: the shard may have been
    /// swapped (InstallSnapshot) between the RPC's validation and this
    /// batch winning a slot. A scoped job's read receipt is sent here.
    fn admit(&self, job: &ScanJob) -> Result<AdmittedJob, Status> {
        let index = self.index;
        if Some(job.vector.len()) != index.dim_opt() {
            return Err(Status::failed_precondition(format!(
                "query dim {} no longer matches the index",
                job.vector.len()
            )));
        }
        check_enumerate_threshold(index, job.enumerate_max_rows)?;
        let patience =
            check_patience_options(index, job.patience_saturation_ppm, job.patience_hops)?;
        let floor_prune = check_floor_prune_options(
            index,
            job.floor_prune,
            job.floor_k,
            job.k as u32,
            job.floor_share_ppm,
            job.floor_greediness_ppm,
            job.floor_clamp_min,
        )?;
        let ((_, allow, prune), receipt) = self.guard.vector_scan_filters(
            index.len(),
            &job.geo_filters,
            &job.geo_regions,
            job.filter.as_ref(),
            job.read_context.as_ref(),
            self.segment_pruning,
        )?;
        if let Some(receipt) = receipt {
            if job
                .ready_tx
                .blocking_send(Ok(SearchShardResponse {
                    payload: Some(search_shard_response::Payload::ReadReady(receipt)),
                }))
                .is_err()
            {
                return Err(Status::cancelled("scoped scan response closed"));
            }
        }
        Ok(AdmittedJob {
            patience,
            floor_prune,
            allow,
            prune,
        })
    }
}

/// The admitted jobs of one batch, parallel by position.
struct AdmittedBatch {
    specs: Vec<QueuedQuerySpec>,
    allows: Vec<Option<crate::boolean_bits::Bits>>,
    prunes: Vec<crate::segment_prune::PruneStats>,
    knowns: Vec<(Vec<bool>, Vec<bool>)>,
    externals: Vec<Box<dyn FnMut() -> Option<f32> + Send>>,
    publishers: Vec<Box<dyn FnMut(f32) -> bool + Send>>,
    dones: Vec<tokio::sync::oneshot::Sender<Result<ScanOutcome, Status>>>,
}

impl AdmittedBatch {
    fn with_capacity(jobs: usize) -> Self {
        Self {
            specs: Vec::with_capacity(jobs),
            allows: Vec::with_capacity(jobs),
            prunes: Vec::with_capacity(jobs),
            knowns: Vec::with_capacity(jobs),
            externals: Vec::new(),
            publishers: Vec::new(),
            dones: Vec::new(),
        }
    }

    /// Admit `job`, or answer it with its refusal.
    fn push(&mut self, ctx: &BatchContext<'_>, job: ScanJob) {
        let admitted = match ctx.admit(&job) {
            Ok(admitted) => admitted,
            Err(refused) => {
                let _ = job.done.send(Err(refused));
                return;
            }
        };
        self.prunes.push(admitted.prune);
        self.knowns.push(filter_known_flags(
            ctx.guard.bm25.as_ref(),
            &job.geo_filters,
            job.filter.as_ref(),
        ));
        self.allows.push(admitted.allow);
        self.specs.push(QueuedQuerySpec {
            vector: job.vector,
            k: job.k,
            keep_ties: job.tie_complete,
            include_rerank_candidate: job.include_rerank_candidate,
            enumerate_max_rows: job.enumerate_max_rows.map(|rows| rows as usize),
            patience: admitted.patience,
            floor_prune: admitted.floor_prune,
        });
        self.externals.push(job.external);
        self.publishers.push(job.publish);
        self.dones.push(job.done);
    }

    /// Score every admitted query in one pass over the index.
    fn scan(
        &mut self,
        index: &VectorIndex,
        chunk_blocks: usize,
    ) -> Vec<(Vec<ChunkHit>, ScanStats)> {
        let queries: Vec<BatchQuery> = self
            .specs
            .iter()
            .zip(&self.allows)
            .map(|(spec, allow)| BatchQuery {
                vector: &spec.vector,
                k: spec.k,
                keep_ties: spec.keep_ties,
                allow: allow.as_ref().map(crate::vector::Mask::Bits),
                enumerate_max_rows: spec.enumerate_max_rows,
                patience: spec.patience,
                floor_prune: spec.floor_prune,
            })
            .collect();
        let (externals, publishers) = (&mut self.externals, &mut self.publishers);
        let mut pass_bytes = 0u64;
        let mut pass_nanos = 0u64;
        let results = crate::chunked::chunked_topk_batch_observed(
            index,
            &queries,
            chunk_blocks,
            &mut |qi| (externals[qi])(),
            &mut |qi, floor| (publishers[qi])(floor),
            &mut |pass| {
                pass_bytes = pass_bytes.saturating_add(pass.bytes);
                pass_nanos = pass_nanos.saturating_add(pass.nanos);
            },
        );
        // Record one sample per batched shard pass regardless of batch width.
        observe_scan(pass_bytes, pass_nanos);
        results
    }

    /// Send every job its hits, stats and handshake flags.
    fn deliver(
        self,
        guard: &ShardState,
        results: Vec<(Vec<ChunkHit>, ScanStats)>,
        identities: crate::source_archive::IdentitySnapshot,
    ) {
        for (
            (((done, (hits, mut stats)), (geo_columns_known, filter_columns_known)), prune),
            spec,
        ) in self
            .dones
            .into_iter()
            .zip(results)
            .zip(self.knowns)
            .zip(self.prunes)
            .zip(self.specs)
        {
            stats.segments_total = prune.segments_total;
            stats.segments_skipped = prune.segments_skipped;
            crate::metrics::record_scan(&stats);
            let mut rerank_candidates = HashMap::new();
            if spec.include_rerank_candidate {
                for h in &hits {
                    if let Some(payload) = guard.rerank_candidate_for(h.slot) {
                        rerank_candidates.insert(h.slot, payload);
                    }
                }
            }
            let _ = done.send(Ok(ScanOutcome {
                hits,
                identities: identities.clone(),
                rerank_candidates,
                stats,
                geo_columns_known,
                filter_columns_known,
            }));
        }
    }
}

/// Run one batched scan under the shard read lock and deliver every job's
/// result. Blocking-pool context.
pub(in crate::node) fn run_scan_batch(
    state: &std::sync::RwLock<ShardState>,
    chunk_blocks: usize,
    segment_pruning: bool,
    batch: Vec<ScanJob>,
) {
    let guard = read_shard(state);
    let index = match guard.index.as_ref() {
        Some(index) => index,
        None => {
            for job in batch {
                let _ = job.done.send(Err(Status::failed_precondition(
                    "shard has no index yet (set calibration or add vectors)",
                )));
            }
            return;
        }
    };
    let ctx = BatchContext {
        guard: &guard,
        index,
        segment_pruning,
    };
    let mut admitted = AdmittedBatch::with_capacity(batch.len());
    for job in batch {
        admitted.push(&ctx, job);
    }
    if admitted.dones.is_empty() {
        return;
    }
    let identities = match guard
        .bm25
        .as_ref()
        .map(Bm25Shard::identity_snapshot)
        .transpose()
    {
        Ok(identities) => identities.unwrap_or_default(),
        Err(error) => {
            for done in admitted.dones {
                let _ = done.send(Err(Status::internal(error.clone())));
            }
            return;
        }
    };
    let results = admitted.scan(index, chunk_blocks);
    admitted.deliver(&guard, results, identities);
}
