//! Cascade.

use super::*;

impl CoordinatorServiceImpl {
    /// Cascade hybrid (FUSION_MODE_CASCADE): no score fusion.
    ///
    /// Phase 1 — candidate generation through the EXISTING floor-sharing
    /// bidi vector path with the tie-complete option: cross-shard early
    /// termination applies (the cutoff is the savings), and every doc
    /// tied at the boundary score survives on every shard. The pool is
    /// `{score >= s_k}` where `s_k` is the global k-th vector score, so
    /// it can exceed k by the boundary tie-group size — score-defined,
    /// hence layout-invariant.
    ///
    /// Phase 2 — BM25 rerank behind the rescore seam: analyze the query,
    /// route each candidate to its owning shard (Bm25Rescore with the
    /// global stats), then rerank the pool by BM25 desc, vector desc,
    /// doc id asc, and return the top `k`. More rankers plug in behind
    /// this same seam later (one stage, no framework).
    #[allow(clippy::too_many_arguments)]
    pub async fn fanout_cascade(
        &self,
        request_id: &str,
        text: &str,
        vector: &[f32],
        k: u32,
        spec: Option<&crate::pb::AnalysisSpec>,
        min_vector_score: f32,
        debug: bool,
        filters: &RequestFilters,
    ) -> Result<(Vec<CascadeHit>, Option<HybridDebug>), Status> {
        self.fanout_cascade_with_terms(
            request_id,
            text,
            &[],
            vector,
            k,
            spec,
            min_vector_score,
            debug,
            filters,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn fanout_cascade_with_terms(
        &self,
        request_id: &str,
        text: &str,
        pre_analyzed_terms: &[String],
        vector: &[f32],
        k: u32,
        spec: Option<&crate::pb::AnalysisSpec>,
        min_vector_score: f32,
        debug: bool,
        filters: &RequestFilters,
    ) -> Result<(Vec<CascadeHit>, Option<HybridDebug>), Status> {
        let analysis_fingerprint = crate::analyzer::analysis_fingerprint(spec);
        if k == 0 || vector.is_empty() {
            return Ok((Vec::new(), None));
        }
        let t_total = std::time::Instant::now();
        // Phase 1: floor-shared, tie-complete vector candidates.
        let t_legs = std::time::Instant::now();
        // Phase 1 carries the filters, so the candidate gate is the
        // filtered corpus; phase 2 reranks that pool and never widens
        // it, so no unfiltered document can reappear.
        #[cfg(feature = "net")]
        let clustered_phase1: Option<FanoutResult> = if self.clustered_vectors.is_some() {
            Some({
                let candidates = self
                    .clustered_vector_candidates(request_id, vector, k, None, true, filters)
                    .await?;
                let ranges = self.product_label_ranges().await?;
                let mut shard_hits: Vec<(u32, Vec<(u64, f32)>)> = (0..self.node_addrs.len())
                    .map(|shard| (shard as u32, Vec::new()))
                    .collect();
                for (doc_id, score) in candidates.hits {
                    let owner = Self::product_owner(&ranges, doc_id)?;
                    shard_hits[owner as usize].1.push((doc_id, score));
                }
                FanoutResult {
                    shard_stats: vec![None; self.node_addrs.len()],
                    shard_wall_ms: (0..self.node_addrs.len())
                        .map(|shard| (shard as u32, 0.0))
                        .collect(),
                    shard_hits,
                    hits: Vec::new(),
                    hedges_fired: 0,
                    hedge_wins: 0,
                    rerank_candidates: HashMap::new(),
                }
            })
        } else {
            None
        };
        #[cfg(not(feature = "net"))]
        let clustered_phase1: Option<FanoutResult> = None;
        let mut phase1 = match clustered_phase1 {
            Some(phase1) => phase1,
            None => {
                self.fanout_search(request_id, vector, k, true, filters)
                    .await?
            }
        };
        let phase1_ms = t_legs.elapsed().as_secs_f32() * 1e3;
        let mut all: Vec<(u64, u32, f64)> = Vec::new(); // (doc_id, shard, score)
        for (shard, hits) in &phase1.shard_hits {
            for &(doc_id, score) in hits {
                all.push((doc_id, *shard, f64::from(score)));
            }
        }
        all.sort_by(|a, b| b.2.total_cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
        let boundary = if all.len() >= k as usize {
            all[k as usize - 1].2
        } else {
            f64::NEG_INFINITY
        };
        // The vector-score floor tightens the gate: the effective cutoff
        // is max(k-th score, floor), applied before the rescore fan-out
        // so filtered-out candidates cost nothing in phase 2.
        let boundary = boundary.max(if min_vector_score > 0.0 {
            f64::from(min_vector_score)
        } else {
            f64::NEG_INFINITY
        });
        let pool: Vec<(u64, u32, f64)> = all.into_iter().filter(|h| h.2 >= boundary).collect();

        // Query analysis + global BM25 stats for phase 2.
        let (terms, analysis_ms) = if !pre_analyzed_terms.is_empty() {
            let mut terms: Vec<String> = Vec::new();
            for term in pre_analyzed_terms {
                if !terms.contains(term) {
                    terms.push(term.clone());
                }
            }
            (terms, 0.0f32)
        } else {
            let addr = self.analysis_addr.clone().ok_or_else(|| {
                Status::unavailable(
                    "no analysis backend configured on the coordinator (analysis_addr)",
                )
            })?;
            let t = std::time::Instant::now();
            let analyzed = crate::analyzer::analyze_document(&addr, text, spec).await?;
            let analysis_ms = t.elapsed().as_secs_f32() * 1e3;
            let mut terms: Vec<String> = Vec::new();
            for (term, _, _) in analyzed.into_body().terms {
                if !terms.contains(&term) {
                    terms.push(term);
                }
            }
            (terms, analysis_ms)
        };
        // Phase 2: route candidates to their owning shards for
        // rescoring. Stats + rescore run as a round (a stale-stats
        // refusal reruns them once with fresh stats and a new fenced claim).
        let t = std::time::Instant::now();
        let mut by_shard: std::collections::HashMap<u32, Vec<u64>> =
            std::collections::HashMap::new();
        for (doc_id, shard, _) in &pool {
            by_shard.entry(*shard).or_default().push(*doc_id);
        }
        let mut fresh = false;
        let (stats_ms, t_rescore, bm25_of, rescore_debug) = loop {
            let (global, epochs) = self.body_stats(&terms, fresh).await?;
            let claims = epochs;
            let stats_ms = t.elapsed().as_secs_f32() * 1e3;
            let t_rescore = std::time::Instant::now();
            match self
                .bm25_rescore_round(
                    &terms,
                    analysis_fingerprint,
                    &global,
                    &claims,
                    &by_shard,
                    &[],
                )
                .await
            {
                Err(e) if !fresh && is_stale_stats(&e) => {
                    self.stats_cache.invalidate_all();
                    fresh = true;
                }
                Err(e) => return Err(e),
                Ok((bm25_of, rescore_debug)) => {
                    break (stats_ms, t_rescore, bm25_of, rescore_debug);
                }
            }
        };
        let rescore_ms = t_rescore.elapsed().as_secs_f32() * 1e3;

        // Rerank: BM25 desc, vector score desc, doc id asc. Top k of the
        // (possibly larger) tie-extended pool.
        let t_fusion = std::time::Instant::now();
        let mut ranked: Vec<CascadeHit> = pool
            .into_iter()
            .map(|(doc_id, shard, vector_score)| CascadeHit {
                doc_id,
                rank: 0,
                shard,
                vector_score: vector_score as f32,
                bm25_score: bm25_of.get(&doc_id).copied().unwrap_or(0.0) as f32,
                boost_score: 0.0,
                rerank_candidate: phase1.rerank_candidates.remove(&doc_id),
            })
            .collect();
        ranked.sort_by(|a, b| {
            b.bm25_score
                .total_cmp(&a.bm25_score)
                .then_with(|| b.vector_score.total_cmp(&a.vector_score))
                .then_with(|| a.doc_id.cmp(&b.doc_id))
        });
        ranked.truncate(k as usize);
        for (i, hit) in ranked.iter_mut().enumerate() {
            hit.rank = i as u32 + 1;
        }
        let dbg = debug.then(|| {
            // Per-shard: phase-1 wall + this shard's rescore wall, the
            // phase-1 candidate count, the rescored count, and the vector
            // scan's stats. Vectors from fanout_search are in completion
            // order; re-key by shard.
            let mut walls: std::collections::HashMap<u32, f32> =
                phase1.shard_wall_ms.iter().copied().collect();
            let mut scans: std::collections::HashMap<u32, Option<ShardScanStats>> = phase1
                .shard_hits
                .iter()
                .zip(&phase1.shard_stats)
                .map(|((shard, _), stats)| (*shard, *stats))
                .collect();
            let mut shards: Vec<HybridShardDebug> = phase1
                .shard_hits
                .iter()
                .map(|(shard, hits)| {
                    let (rescore_wall, rescored) =
                        rescore_debug.get(shard).copied().unwrap_or((0.0, 0));
                    HybridShardDebug {
                        shard: *shard,
                        rpc_ms: walls.remove(shard).unwrap_or(0.0) + rescore_wall,
                        vector_hits: hits.len() as u32,
                        bm25_hits: rescored,
                        scan: scans.remove(shard).flatten(),
                    }
                })
                .collect();
            shards.sort_by_key(|s| s.shard);
            HybridDebug {
                fusion_mode: FusionMode::Cascade as i32,
                leg_k: k,
                terms,
                analysis_ms,
                stats_ms,
                legs_ms: phase1_ms + rescore_ms,
                fusion_ms: t_fusion.elapsed().as_secs_f32() * 1e3,
                total_ms: t_total.elapsed().as_secs_f32() * 1e3,
                shards,
                boost_ms: 0.0,
                boost_terms: Vec::new(),
            }
        });
        Ok((ranked, dbg))
    }
}
