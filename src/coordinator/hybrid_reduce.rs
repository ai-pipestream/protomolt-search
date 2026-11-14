//! Shared hybrid ranking and provenance reduction.

use super::hybrid_context::HybridQuery;
use super::*;

#[derive(Default)]
pub(super) struct GlobalHybrid {
    pub(super) vector: Vec<(u32, Vec<(u64, f64)>)>,
    pub(super) bm25: Vec<(u32, Vec<(u64, f64)>)>,
    pub(super) owners: HashMap<u64, u32>,
    pub(super) candidates: HashMap<u64, crate::pb::RerankCandidatePayload>,
}

impl GlobalHybrid {
    pub(super) fn collect_raw(
        &mut self,
        shard: u32,
        hits: Vec<crate::pb::RawLegHit>,
        vector: bool,
    ) {
        let hits = hits
            .into_iter()
            .map(|hit| {
                self.owners.entry(hit.doc_id).or_insert(shard);
                if let Some(candidate) = hit.rerank_candidate {
                    self.candidates.entry(hit.doc_id).or_insert(candidate);
                }
                (hit.doc_id, f64::from(hit.score))
            })
            .collect();
        if vector {
            self.vector.push((shard, hits));
        } else {
            self.bm25.push((shard, hits));
        }
    }

    pub(super) fn finish(mut self, query: HybridQuery<'_>) -> Vec<HybridHit> {
        let legs = query.legs();
        let mut vector = fusion::merge_legs_by_score(self.vector);
        let mut bm25 = fusion::merge_legs_by_score(self.bm25);
        if legs.min_vector_score > 0.0 {
            vector.retain(|&(_, score, _)| score >= f64::from(legs.min_vector_score));
            let allowed: std::collections::HashSet<u64> =
                vector.iter().map(|&(id, _, _)| id).collect();
            bm25.retain(|&(id, _, _)| allowed.contains(&id));
        }
        let inputs = [
            Leg {
                hits: vector.iter().map(|&(id, score, _)| (id, score)).collect(),
                weight: f64::from(legs.vector_weight),
            },
            Leg {
                hits: bm25.iter().map(|&(id, score, _)| (id, score)).collect(),
                weight: f64::from(legs.bm25_weight),
            },
        ];
        let fused = if legs.fusion_mode == FusionMode::ScoreBlend {
            fusion::blend_fuse(
                &inputs,
                legs.leg_k as usize,
                legs.normalization,
                legs.combination,
                query.input().k() as usize,
            )
        } else {
            fusion::rrf_fuse(&inputs, legs.rrf_k, query.input().k() as usize)
        };
        fused
            .into_iter()
            .map(|hit| HybridHit {
                doc_id: hit.doc_id,
                fused_score: hit.fused_score as f32,
                shard: self.owners.get(&hit.doc_id).copied().unwrap_or(0),
                vector_rank: hit.leg_ranks[0],
                vector_score: hit.leg_scores[0].unwrap_or(0.0) as f32,
                bm25_rank: hit.leg_ranks[1],
                bm25_score: hit.leg_scores[1].unwrap_or(0.0) as f32,
                boost_score: 0.0,
                vector_normalized: hit.leg_norms.first().copied().flatten(),
                bm25_normalized: hit.leg_norms.get(1).copied().flatten(),
                rerank_candidate: self.candidates.remove(&hit.doc_id),
            })
            .collect()
    }
}

pub(super) fn fuse_clustered_shard(
    vector: Vec<(u64, f64)>,
    bm25: Vec<crate::pb::RawLegHit>,
    legs: HybridLegs,
) -> Vec<crate::pb::HybridLegHit> {
    let mut candidates = HashMap::new();
    let bm25 = bm25
        .into_iter()
        .map(|hit| {
            if let Some(candidate) = hit.rerank_candidate {
                candidates.insert(hit.doc_id, candidate);
            }
            (hit.doc_id, f64::from(hit.score))
        })
        .collect();
    fusion::rrf_fuse(
        &[
            Leg {
                hits: vector,
                weight: f64::from(legs.vector_weight),
            },
            Leg {
                hits: bm25,
                weight: f64::from(legs.bm25_weight),
            },
        ],
        legs.rrf_k,
        legs.leg_k as usize,
    )
    .into_iter()
    .map(|hit| crate::pb::HybridLegHit {
        doc_id: hit.doc_id,
        fused_score: hit.fused_score as f32,
        vector_rank: hit.leg_ranks[0],
        vector_score: hit.leg_scores[0].unwrap_or(0.0) as f32,
        bm25_rank: hit.leg_ranks[1],
        bm25_score: hit.leg_scores[1].unwrap_or(0.0) as f32,
        rerank_candidate: candidates.remove(&hit.doc_id),
    })
    .collect()
}

pub(super) fn finish_two_level(
    lists: Vec<(u32, Vec<crate::pb::HybridLegHit>)>,
    query: HybridQuery<'_>,
) -> Vec<HybridHit> {
    let legs: Vec<Leg> = lists
        .iter()
        .map(|(_, hits)| Leg {
            hits: hits
                .iter()
                .map(|hit| (hit.doc_id, f64::from(hit.fused_score)))
                .collect(),
            weight: 1.0,
        })
        .collect();
    fusion::rrf_fuse(&legs, query.legs().rrf_k, query.input().k() as usize)
        .into_iter()
        .map(|hit| {
            let (shard, source) = lists
                .iter()
                .find_map(|(shard, hits)| {
                    hits.iter()
                        .find(|source| source.doc_id == hit.doc_id)
                        .map(|source| (*shard, source))
                })
                .expect("fused hit comes from some shard");
            HybridHit {
                doc_id: hit.doc_id,
                fused_score: hit.fused_score as f32,
                shard,
                vector_rank: source.vector_rank,
                vector_score: source.vector_score,
                bm25_rank: source.bm25_rank,
                bm25_score: source.bm25_score,
                boost_score: 0.0,
                vector_normalized: None,
                bm25_normalized: None,
                rerank_candidate: source.rerank_candidate.clone(),
            }
        })
        .collect()
}

pub(super) fn hybrid_debug(
    query: HybridQuery<'_>,
    mut shards: Vec<HybridShardDebug>,
    legs_ms: f32,
    fusion_started: std::time::Instant,
) -> Option<HybridDebug> {
    query.debug().then(|| {
        shards.sort_by_key(|shard| shard.shard);
        HybridDebug {
            fusion_mode: match query.legs().fusion_mode {
                FusionMode::TwoLevel => FusionMode::TwoLevel as i32,
                FusionMode::ScoreBlend => FusionMode::ScoreBlend as i32,
                _ => FusionMode::GlobalRank as i32,
            },
            leg_k: query.legs().leg_k,
            terms: Vec::new(),
            analysis_ms: 0.0,
            stats_ms: 0.0,
            legs_ms,
            fusion_ms: fusion_started.elapsed().as_secs_f32() * 1e3,
            total_ms: 0.0,
            shards,
            boost_ms: 0.0,
            boost_terms: Vec::new(),
        }
    })
}
