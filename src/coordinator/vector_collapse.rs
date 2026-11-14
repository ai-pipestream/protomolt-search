//! Vector collapse.

use super::*;

impl CoordinatorServiceImpl {
    /// [`Self::fanout_search`] in collapse-by-parent mode: `k` means k
    /// distinct parent documents, each represented by its best chunk.
    /// Shards collapse locally (their floors are k-th best PARENT
    /// scores, so collaborative pruning strengthens); the coordinator
    /// dedupes parents that appear on multiple shards (opinions
    /// straddling a shard cut) keeping the better representative, then
    /// takes the global top-k parents.
    pub async fn fanout_search_collapse(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        filters: &RequestFilters,
    ) -> Result<FanoutResult, Status> {
        self.check_vector_scan(filters, true)?;
        if self.scoped_vector_scan() && self.query_read_versions.is_none() {
            let (pinned, reads) = self.pin_read_versions().await?;
            let result =
                Box::pin(pinned.fanout_search_collapse(request_id, vector, k, filters)).await;
            pinned.validate_read_versions(&reads).await?;
            return result;
        }
        let n_nodes = self.node_addrs.len();
        if n_nodes == 0 {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }
        let admission = self.vector_read_barrier()?;
        self.ensure_shard_vector_rows().await;
        let mut workers = tokio::task::JoinSet::new();
        let mask = self.vector_scan_mask(filters.tree.as_ref());
        let ctx = ShardQueryCtx {
            admission: admission.clone(),
            request_id: Arc::from(request_id),
            vector: Arc::new(vector.to_vec()),
            k,
            tie_complete: false,
            collapse: true,
            filters: Arc::new(filters.clone()),
            shard_filters: Arc::new(self.shard_filter_trees(filters, mask.as_ref())),
            shard_floor_shares: Arc::new(self.shard_floor_share_ppms(filters, mask.as_ref())),
            tracker: Arc::new(Mutex::new(FloorTracker::new())),
            gfloor: Arc::new(watch::channel(f32::NEG_INFINITY).0),
            hedges: Arc::new(AtomicU64::new(0)),
            hedge_wins: Arc::new(AtomicU64::new(0)),
        };
        let (hedges, hedge_wins) = (Arc::clone(&ctx.hedges), Arc::clone(&ctx.hedge_wins));
        let mut known = Self::filter_known(filters, mask.as_ref());
        let active = (0..n_nodes)
            .filter(|shard| !mask.as_ref().is_some_and(|m| m.skipped[*shard]))
            .count();

        let (done_tx, mut done_rx) =
            mpsc::channel::<(u32, f32, Result<SearchShardDone, Status>)>(n_nodes);
        for shard in 0..n_nodes {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
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
        drop(done_tx);

        let mut shard_hits: Vec<(u32, Vec<(u64, f32)>)> = Vec::with_capacity(n_nodes);
        let mut shard_stats: Vec<Option<ShardScanStats>> = Vec::with_capacity(n_nodes);
        let mut shard_wall_ms: Vec<(u32, f32)> = Vec::with_capacity(n_nodes);
        // Parent -> best hit. Tie-break inside a parent: score desc, then
        // vector id asc (globally unique), deterministic across arrival
        // orders.
        let mut best: HashMap<u64, ScoredHit> = HashMap::new();
        // A skipped shard is a shard with no matching row: no hits, no
        // stats, no wall time.
        for shard in 0..n_nodes {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                shard_hits.push((shard as u32, Vec::new()));
                shard_stats.push(None);
                shard_wall_ms.push((shard as u32, 0.0));
            }
        }
        for _ in 0..active {
            match done_rx.recv().await {
                Some((shard, wall_ms, Ok(done))) => {
                    known.merge_shard(
                        shard as usize,
                        &done.geo_columns_known,
                        &done.filter_columns_known,
                    )?;
                    shard_hits.push((
                        shard,
                        done.hits.iter().map(|h| (h.vector_id, h.score)).collect(),
                    ));
                    for hit in done.hits {
                        match best.entry(hit.parent_id) {
                            std::collections::hash_map::Entry::Vacant(entry) => {
                                entry.insert(hit);
                            }
                            std::collections::hash_map::Entry::Occupied(mut entry) => {
                                let previous = entry.get();
                                if hit.score > previous.score
                                    || (hit.score == previous.score
                                        && hit.vector_id < previous.vector_id)
                                {
                                    entry.insert(hit);
                                }
                            }
                        }
                    }
                    shard_stats.push(done.stats);
                    shard_wall_ms.push((shard, wall_ms));
                }
                Some((shard, _, Err(e))) => {
                    return Err(Status::new(
                        if admission.is_some() {
                            e.code()
                        } else {
                            tonic::Code::Internal
                        },
                        format!("shard {shard} failed: {e}"),
                    ));
                }
                None => {
                    return Err(Status::internal("fan-out completed without all shards"));
                }
            }
        }

        known.refuse_unknown(filters)?;

        let mut hits: Vec<ScoredHit> = best.into_values().collect();
        hits.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.vector_id.cmp(&b.vector_id))
        });
        hits.truncate(k as usize);
        Ok(FanoutResult {
            hits,
            shard_stats,
            shard_hits,
            shard_wall_ms,
            hedges_fired: hedges.load(AtomicOrdering::Relaxed),
            hedge_wins: hedge_wins.load(AtomicOrdering::Relaxed),
            rerank_candidates: HashMap::new(),
        })
    }
}
