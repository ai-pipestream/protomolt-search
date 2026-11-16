//! Parent-document vector result aggregation.

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
        let mut scan = self
            .dispatch_vector_shards(request_id, vector, k, false, true, filters)
            .await?;
        // Parent -> best hit. Tie-break inside a parent: score desc, then
        // vector id asc (globally unique), deterministic across arrival
        // orders.
        let mut best: HashMap<u64, ScoredHit> = HashMap::new();
        for _ in 0..scan.active {
            let (shard, wall_ms, done) = scan.next_done().await?;
            scan.shard_hits.push((
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
                            || (hit.score == previous.score && hit.vector_id < previous.vector_id)
                        {
                            entry.insert(hit);
                        }
                    }
                }
            }
            scan.shard_stats.push(done.stats);
            scan.shard_wall_ms.push((shard, wall_ms));
        }

        scan.known.refuse_unknown(filters)?;

        let mut hits: Vec<ScoredHit> = best.into_values().collect();
        hits.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.vector_id.cmp(&b.vector_id))
        });
        hits.truncate(k as usize);
        Ok(FanoutResult {
            hits,
            shard_stats: scan.shard_stats,
            shard_hits: scan.shard_hits,
            shard_wall_ms: scan.shard_wall_ms,
            hedges_fired: scan.hedges.load(AtomicOrdering::Relaxed),
            hedge_wins: scan.hedge_wins.load(AtomicOrdering::Relaxed),
            rerank_candidates: HashMap::new(),
        })
    }
}
