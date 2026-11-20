//! Shared dispatch of one unary vector fan-out and its reply stream.

use super::*;

type ShardReply = (u32, f32, Result<SearchShardDone, Status>);

/// One vector fan-out in flight. The workers abort when this drops.
pub(in crate::coordinator) struct ShardDispatch {
    admission: Option<Arc<crate::vector_read::VectorReadBarrier>>,
    done_rx: mpsc::Receiver<ShardReply>,
    _workers: tokio::task::JoinSet<()>,
    pub(in crate::coordinator) known: FilterKnown,
    /// Shards the mask did not skip; each sends exactly one reply.
    pub(in crate::coordinator) active: usize,
    pub(in crate::coordinator) hedges: Arc<AtomicU64>,
    pub(in crate::coordinator) hedge_wins: Arc<AtomicU64>,
    pub(in crate::coordinator) shard_hits: Vec<(u32, Vec<(u64, f32)>)>,
    pub(in crate::coordinator) shard_stats: Vec<Option<ShardScanStats>>,
    pub(in crate::coordinator) shard_wall_ms: Vec<(u32, f32)>,
}

impl ShardDispatch {
    /// The next shard's terminal reply with its filter flags merged; a shard
    /// failure or a lost worker ends the fan-out.
    pub(in crate::coordinator) async fn next_done(
        &mut self,
    ) -> Result<(u32, f32, SearchShardDone), Status> {
        match self.done_rx.recv().await {
            Some((shard, wall_ms, Ok(done))) => {
                self.known.merge_shard(
                    shard as usize,
                    &done.geo_columns_known,
                    &done.filter_columns_known,
                )?;
                Ok((shard, wall_ms, done))
            }
            Some((shard, _, Err(e))) => Err(Status::new(
                if self.admission.is_some() {
                    e.code()
                } else {
                    tonic::Code::Internal
                },
                format!("shard {shard} failed: {e}"),
            )),
            None => Err(Status::internal("fan-out completed without all shards")),
        }
    }
}

impl CoordinatorServiceImpl {
    /// Start one `SearchShard` attempt (hedged, deadline-bound) per shard the
    /// mask keeps.
    pub(in crate::coordinator) async fn dispatch_vector_shards(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        tie_complete: bool,
        collapse: bool,
        filters: &RequestFilters,
    ) -> Result<ShardDispatch, Status> {
        let n_nodes = self.node_addrs.len();
        if n_nodes == 0 {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }

        let admission = self.vector_read_barrier()?;
        self.ensure_shard_vector_rows().await;
        let mask = self.vector_scan_mask(filters.tree.as_ref());
        let ctx = ShardQueryCtx {
            admission: admission.clone(),
            request_id: Arc::from(request_id),
            vector: Arc::new(vector.to_vec()),
            k,
            tie_complete,
            collapse,
            filters: Arc::new(filters.clone()),
            shard_filters: Arc::new(self.shard_filter_trees(filters, mask.as_ref())),
            shard_floor_shares: Arc::new(self.shard_floor_share_ppms(filters, mask.as_ref())),
            tracker: Arc::new(Mutex::new(FloorTracker::new())),
            gfloor: Arc::new(watch::channel(f32::NEG_INFINITY).0),
            hedges: Arc::new(AtomicU64::new(0)),
            hedge_wins: Arc::new(AtomicU64::new(0)),
        };
        let (hedges, hedge_wins) = (Arc::clone(&ctx.hedges), Arc::clone(&ctx.hedge_wins));
        let known = Self::filter_known(filters, mask.as_ref());
        let active = (0..n_nodes)
            .filter(|shard| !mask.as_ref().is_some_and(|m| m.skipped[*shard]))
            .count();

        let (done_tx, done_rx) =
            mpsc::channel::<(u32, f32, Result<SearchShardDone, Status>)>(n_nodes);
        let workers = self.spawn_shard_workers(&ctx, mask.as_ref(), done_tx)?;

        let mut shard_hits: Vec<(u32, Vec<(u64, f32)>)> = Vec::with_capacity(n_nodes);
        let mut shard_stats: Vec<Option<ShardScanStats>> = Vec::with_capacity(n_nodes);
        let mut shard_wall_ms: Vec<(u32, f32)> = Vec::with_capacity(n_nodes);
        // A skipped shard is a shard with no matching row: no hits, no
        // stats, no wall time.
        for shard in 0..n_nodes {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                shard_hits.push((shard as u32, Vec::new()));
                shard_stats.push(None);
                shard_wall_ms.push((shard as u32, 0.0));
            }
        }
        Ok(ShardDispatch {
            admission,
            done_rx,
            _workers: workers,
            known,
            active,
            hedges,
            hedge_wins,
            shard_hits,
            shard_stats,
            shard_wall_ms,
        })
    }

    /// One hedged worker per kept shard; each sends its terminal reply on
    /// `done_tx`, which closes once every worker has.
    fn spawn_shard_workers(
        &self,
        ctx: &ShardQueryCtx,
        mask: Option<&crate::placement::ShardMask>,
        done_tx: mpsc::Sender<ShardReply>,
    ) -> Result<tokio::task::JoinSet<()>, Status> {
        let mut workers = tokio::task::JoinSet::new();
        for shard in 0..self.node_addrs.len() {
            if mask.is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let primary = self.node_client(&self.node_addrs[shard])?;
            let replica = match self.replica_addrs.get(shard).and_then(|r| r.as_deref()) {
                Some(addr) => Some(self.node_client(addr)?),
                None => None,
            };
            let ctx = ctx.clone();
            let limits = self.limits();
            let done_tx = done_tx.clone();
            workers.spawn(async move {
                let t0 = std::time::Instant::now();
                let admission = ctx.admission.clone();
                let result =
                    run_shard_with_hedge(shard as u32, primary, replica, ctx, limits).await;
                if let (Some(admission), Err(error)) = (admission, &result) {
                    admission.fail(error.clone());
                }
                let wall_ms = t0.elapsed().as_secs_f32() * 1e3;
                let _ = done_tx.send((shard as u32, wall_ms, result)).await;
            });
        }
        Ok(workers)
    }
}
