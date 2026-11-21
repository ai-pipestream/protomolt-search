//! Tie-complete vector candidate selection followed by lexical cascade reranking.

use super::hybrid_context::{CascadeQuery, HybridInput};
use super::*;

type CascadePool = Vec<(u64, u32, f64)>;

struct CascadeRescore {
    stats_ms: f32,
    started: std::time::Instant,
    scores: HashMap<u64, f64>,
    shards: HashMap<u32, (f32, u32)>,
}

struct CascadeTiming {
    total: std::time::Instant,
    fusion: std::time::Instant,
    analysis_ms: f32,
    legs_ms: f32,
}

impl CoordinatorServiceImpl {
    /// Vector candidates rescored by the lexical leg; see [`CascadeQuery`].
    pub async fn fanout_cascade_context(
        &self,
        query: CascadeQuery<'_>,
    ) -> Result<(Vec<CascadeHit>, Option<HybridDebug>), Status> {
        let (hits, debug, _) = self.fanout_cascade_with_terms(query).await?;
        Ok((hits, debug))
    }

    pub(super) async fn fanout_cascade_with_terms(
        &self,
        query: CascadeQuery<'_>,
    ) -> Result<(Vec<CascadeHit>, Option<HybridDebug>, DenseScanExecution), Status> {
        let input = query.input();
        if input.k() == 0 || input.vector().is_empty() {
            return Ok((Vec::new(), None, DenseScanExecution::default()));
        }
        let total = std::time::Instant::now();
        let legs_started = std::time::Instant::now();
        let (mut candidates, exec) = self.cascade_candidates(input).await?;
        let phase1_ms = legs_started.elapsed().as_secs_f32() * 1e3;
        let pool = cascade_pool(&candidates, query);
        let (terms, analysis_ms) = self.analyze_hybrid_input(input, false).await?;
        let rescore = self.cascade_rescore(&pool, &terms, input).await?;
        let legs_ms = phase1_ms + rescore.started.elapsed().as_secs_f32() * 1e3;
        let fusion = std::time::Instant::now();
        let hits = rank_cascade(
            pool,
            &rescore.scores,
            &mut candidates.rerank_candidates,
            input.k(),
        );
        let debug = cascade_debug(
            query,
            &candidates,
            &rescore,
            terms,
            CascadeTiming {
                total,
                fusion,
                analysis_ms,
                legs_ms,
            },
        );
        Ok((hits, debug, exec))
    }

    async fn cascade_candidates(
        &self,
        input: HybridInput<'_>,
    ) -> Result<(FanoutResult, DenseScanExecution), Status> {
        #[cfg(feature = "net")]
        if self.clustered_vectors.is_some() {
            let rows = self
                .clustered_quality_identity()
                .await?
                .map_or(0, |id| id.rows);
            let candidates = self
                .clustered_vector_candidates(
                    input.request_id(),
                    input.vector(),
                    input.k(),
                    None,
                    true,
                    input.filters(),
                )
                .await?;
            let ranges = self.product_label_ranges().await?;
            let mut shard_hits: Vec<(u32, Vec<(u64, f32)>)> = (0..self.node_addrs.len())
                .map(|shard| (shard as u32, Vec::new()))
                .collect();
            for (doc_id, score) in candidates.hits {
                let owner = Self::product_owner(&ranges, doc_id)?;
                shard_hits[owner as usize].1.push((doc_id, score));
            }
            return Ok((
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
                },
                clustered_scan_execution(rows),
            ));
        }
        self.ensure_shard_vector_rows().await;
        let result = self
            .fanout_search(
                input.request_id(),
                input.vector(),
                input.k(),
                true,
                input.filters(),
            )
            .await?;
        let refs: Vec<(usize, Option<&ShardScanStats>)> = result
            .shard_hits
            .iter()
            .zip(&result.shard_stats)
            .map(|((shard, _), stats)| (*shard as usize, stats.as_ref()))
            .collect();
        let exec = self.evidence_from_shard_stats(&refs, 0);
        Ok((result, exec))
    }

    async fn cascade_rescore(
        &self,
        pool: &[(u64, u32, f64)],
        terms: &[String],
        input: HybridInput<'_>,
    ) -> Result<CascadeRescore, Status> {
        let started = std::time::Instant::now();
        let mut by_shard: HashMap<u32, Vec<u64>> = HashMap::new();
        for (doc_id, shard, _) in pool {
            by_shard.entry(*shard).or_default().push(*doc_id);
        }
        let fingerprint = crate::analyzer::analysis_fingerprint(input.spec());
        let mut fresh = false;
        loop {
            let (global, claims) = self.body_stats(terms, fresh).await?;
            let stats_ms = started.elapsed().as_secs_f32() * 1e3;
            let rescoring = std::time::Instant::now();
            match self
                .bm25_rescore_round(terms, fingerprint, &global, &claims, &by_shard, &[])
                .await
            {
                Err(error) if !fresh && is_stale_stats(&error) => {
                    self.stats_cache.invalidate_all();
                    fresh = true;
                }
                Err(error) => return Err(error),
                Ok((scores, shards)) => {
                    return Ok(CascadeRescore {
                        stats_ms,
                        started: rescoring,
                        scores,
                        shards,
                    })
                }
            }
        }
    }
}

pub(super) fn cascade_pool(candidates: &FanoutResult, query: CascadeQuery<'_>) -> CascadePool {
    let k = query.input().k();
    let mut all = Vec::new();
    for (shard, hits) in &candidates.shard_hits {
        for &(doc, score) in hits {
            all.push((doc, *shard, f64::from(score)));
        }
    }
    all.sort_by(|a, b| b.2.total_cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
    let boundary = if all.len() >= k as usize {
        all[k as usize - 1].2
    } else {
        f64::NEG_INFINITY
    };
    let boundary = boundary.max(if query.min_vector_score() > 0.0 {
        f64::from(query.min_vector_score())
    } else {
        f64::NEG_INFINITY
    });
    all.into_iter().filter(|hit| hit.2 >= boundary).collect()
}

pub(super) fn rank_cascade(
    pool: CascadePool,
    scores: &HashMap<u64, f64>,
    candidates: &mut HashMap<u64, crate::pb::RerankCandidatePayload>,
    k: u32,
) -> Vec<CascadeHit> {
    let mut ranked: Vec<CascadeHit> = pool
        .into_iter()
        .map(|(doc_id, shard, vector_score)| CascadeHit {
            doc_id,
            rank: 0,
            shard,
            vector_score: vector_score as f32,
            bm25_score: scores.get(&doc_id).copied().unwrap_or(0.0) as f32,
            boost_score: 0.0,
            rerank_candidate: candidates.remove(&doc_id),
        })
        .collect();
    ranked.sort_by(|a, b| {
        b.bm25_score
            .total_cmp(&a.bm25_score)
            .then_with(|| b.vector_score.total_cmp(&a.vector_score))
            .then_with(|| a.doc_id.cmp(&b.doc_id))
    });
    ranked.truncate(k as usize);
    for (index, hit) in ranked.iter_mut().enumerate() {
        hit.rank = index as u32 + 1;
    }
    ranked
}

fn cascade_debug(
    query: CascadeQuery<'_>,
    candidates: &FanoutResult,
    rescore: &CascadeRescore,
    terms: Vec<String>,
    timing: CascadeTiming,
) -> Option<HybridDebug> {
    query.debug().then(|| {
        let mut walls: HashMap<u32, f32> = candidates.shard_wall_ms.iter().copied().collect();
        let mut scans: HashMap<u32, Option<ShardScanStats>> = candidates
            .shard_hits
            .iter()
            .zip(&candidates.shard_stats)
            .map(|((shard, _), stats)| (*shard, *stats))
            .collect();
        let mut shards: Vec<HybridShardDebug> = candidates
            .shard_hits
            .iter()
            .map(|(shard, hits)| {
                let (rescore_wall, rescored) =
                    rescore.shards.get(shard).copied().unwrap_or((0.0, 0));
                HybridShardDebug {
                    shard: *shard,
                    rpc_ms: walls.remove(shard).unwrap_or(0.0) + rescore_wall,
                    vector_hits: hits.len() as u32,
                    bm25_hits: rescored,
                    scan: scans.remove(shard).flatten(),
                }
            })
            .collect();
        shards.sort_by_key(|shard| shard.shard);
        HybridDebug {
            fusion_mode: FusionMode::Cascade as i32,
            leg_k: query.input().k(),
            terms,
            analysis_ms: timing.analysis_ms,
            stats_ms: rescore.stats_ms,
            legs_ms: timing.legs_ms,
            fusion_ms: timing.fusion.elapsed().as_secs_f32() * 1e3,
            total_ms: timing.total.elapsed().as_secs_f32() * 1e3,
            shards,
            boost_ms: 0.0,
            boost_terms: Vec::new(),
        }
    })
}
