//! Fused-hit trees: the rank, blend, decomposed, and cascade modes.

use tonic::Status;

use super::{leaf, node};
use crate::pb::{
    CascadeHit, Explanation, FusionMode, HybridHit, HybridLegOptions, ScoreCombination,
    ScoreNormalization,
};

/// The resolved fusion parameters, defaulted the way the hybrid route
/// defaults them (`CoordinatorServiceImpl::hybrid_search`).
#[derive(Debug, Clone, Copy)]
pub struct Fusion {
    pub mode: FusionMode,
    pub rrf_k: f64,
    pub vector_weight: f64,
    pub bm25_weight: f64,
    pub normalization: ScoreNormalization,
    pub combination: ScoreCombination,
}

impl Fusion {
    pub fn resolve(mode: FusionMode, legs: &HybridLegOptions) -> Self {
        Fusion {
            mode,
            rrf_k: if legs.rrf_k == 0.0 {
                crate::fusion::DEFAULT_RRF_K
            } else {
                f64::from(legs.rrf_k)
            },
            vector_weight: f64::from(legs.vector_weight.unwrap_or(1.0)),
            bm25_weight: f64::from(legs.bm25_weight.unwrap_or(1.0)),
            normalization: match legs.normalization() {
                ScoreNormalization::ZScore => ScoreNormalization::ZScore,
                ScoreNormalization::None => ScoreNormalization::None,
                _ => ScoreNormalization::MinMax,
            },
            combination: match legs.combination() {
                ScoreCombination::Geometric => ScoreCombination::Geometric,
                ScoreCombination::Harmonic => ScoreCombination::Harmonic,
                _ => ScoreCombination::Arithmetic,
            },
        }
    }
}

/// A legacy hybrid boost's parameters (`BoostRescore`): the window is
/// ordered by `base_weight * fused + boost_weight * boost`, and the
/// served score is the fused score.
#[derive(Debug, Clone)]
pub struct LegacyBoost {
    pub id: String,
    pub base_weight: f64,
    pub boost_weight: f64,
}

fn legacy_boost_leaf(boost: &LegacyBoost, base: f64, boost_score: f32) -> Explanation {
    let key = boost.base_weight * base + boost.boost_weight * f64::from(boost_score);
    leaf(
        f64::from(boost_score),
        format!(
            "boost {:?}: BM25 of the boost text over the window; orders the window by \
             base_weight {} * score + boost_weight {} * boost = {key}, and is not part of the \
             served score",
            boost.id, boost.base_weight, boost.boost_weight
        ),
    )
}

/// One fused hit's tree for the rank, blend, and decomposed modes.
pub fn composite(
    hit: &HybridHit,
    fusion: &Fusion,
    dense_id: &str,
    lexical_id: &str,
    boost: Option<&LegacyBoost>,
) -> Result<Explanation, Status> {
    let mut details = Vec::new();
    let description = match fusion.mode {
        FusionMode::GlobalRank | FusionMode::TwoLevel => {
            rank_legs(hit, fusion, dense_id, lexical_id, &mut details)
        }
        FusionMode::ScoreBlend => blend_legs(hit, fusion, dense_id, lexical_id, &mut details)?,
        FusionMode::Decomposed => decomposed_legs(hit, fusion, dense_id, lexical_id, &mut details),
        FusionMode::Cascade | FusionMode::Unspecified => {
            return Err(Status::internal(
                "explain: a cascade hit reached the fused-hit tree builder",
            ));
        }
    };
    if let Some(boost) = boost {
        if hit.boost_score != 0.0 {
            details.push(legacy_boost_leaf(
                boost,
                f64::from(hit.fused_score),
                hit.boost_score,
            ));
        }
    }
    Ok(node(f64::from(hit.fused_score), description, details))
}

/// Pushes one reciprocal-rank node per leg the hit is in and returns
/// the root's description.
fn rank_legs(
    hit: &HybridHit,
    fusion: &Fusion,
    dense_id: &str,
    lexical_id: &str,
    details: &mut Vec<Explanation>,
) -> String {
    if let Some(rank) = hit.vector_rank {
        details.push(node(
            fusion.vector_weight / (fusion.rrf_k + f64::from(rank)),
            format!(
                "leg {dense_id:?}: weight {} / (rrf_k {} + rank {rank})",
                fusion.vector_weight, fusion.rrf_k
            ),
            vec![leaf(
                f64::from(hit.vector_score),
                "the leg's raw score, which fixed the rank and is not an addend",
            )],
        ));
    }
    if let Some(rank) = hit.bm25_rank {
        details.push(node(
            fusion.bm25_weight / (fusion.rrf_k + f64::from(rank)),
            format!(
                "leg {lexical_id:?}: weight {} / (rrf_k {} + rank {rank})",
                fusion.bm25_weight, fusion.rrf_k
            ),
            vec![leaf(
                f64::from(hit.bm25_score),
                "the leg's raw score, which fixed the rank and is not an addend",
            )],
        ));
    }
    "reciprocal rank fusion: sum of the leg nodes".to_string()
}

/// Pushes one weighted normalized-score node per leg the hit is in and
/// returns the root's description.
fn blend_legs(
    hit: &HybridHit,
    fusion: &Fusion,
    dense_id: &str,
    lexical_id: &str,
    details: &mut Vec<Explanation>,
) -> Result<String, Status> {
    let kind = match fusion.normalization {
        ScoreNormalization::ZScore => "z-score",
        ScoreNormalization::None => "identity",
        _ => "min-max",
    };
    let mut legs: Vec<(&str, f64, Option<f64>, f32)> = Vec::new();
    if hit.vector_rank.is_some() {
        legs.push((
            dense_id,
            fusion.vector_weight,
            hit.vector_normalized,
            hit.vector_score,
        ));
    }
    if hit.bm25_rank.is_some() {
        legs.push((
            lexical_id,
            fusion.bm25_weight,
            hit.bm25_normalized,
            hit.bm25_score,
        ));
    }
    for (id, weight, normalized, raw) in legs {
        let normalized = normalized.ok_or_else(|| {
            Status::internal(format!(
                "explain: the blend route returned document {} in leg {id:?} without \
                 its normalized score",
                hit.doc_id
            ))
        })?;
        details.push(node(
            weight * normalized,
            format!("leg {id:?}: weight {weight} * normalized score"),
            vec![
                leaf(
                    normalized,
                    format!("{kind} normalization of the raw score over the leg's list"),
                ),
                leaf(f64::from(raw), "the leg's raw score"),
            ],
        ));
    }
    let total = if fusion.vector_weight != 0.0 {
        fusion.vector_weight
    } else {
        0.0
    } + if fusion.bm25_weight != 0.0 {
        fusion.bm25_weight
    } else {
        0.0
    };
    Ok(match fusion.combination {
        ScoreCombination::Geometric => "score blend, geometric: exp(sum of weight * \
             ln(normalized) / sum of the weights used) over the leg nodes (a leg at or \
             below zero is left out)"
            .to_string(),
        ScoreCombination::Harmonic => "score blend, harmonic: sum of the weights used / \
             sum of weight / normalized over the leg nodes (a leg at or below zero is \
             left out)"
            .to_string(),
        _ => {
            format!("score blend, arithmetic: sum of the leg nodes / total weight {total}")
        }
    })
}

/// Pushes one weighted raw-score node per leg and returns the root's
/// description.
fn decomposed_legs(
    hit: &HybridHit,
    fusion: &Fusion,
    dense_id: &str,
    lexical_id: &str,
    details: &mut Vec<Explanation>,
) -> String {
    details.push(node(
        fusion.vector_weight * f64::from(hit.vector_score),
        format!(
            "leg {dense_id:?}: weight {} * raw score",
            fusion.vector_weight
        ),
        vec![leaf(f64::from(hit.vector_score), "the leg's raw score")],
    ));
    details.push(node(
        fusion.bm25_weight * f64::from(hit.bm25_score),
        format!(
            "leg {lexical_id:?}: weight {} * raw score",
            fusion.bm25_weight
        ),
        vec![leaf(f64::from(hit.bm25_score), "the leg's raw score")],
    ));
    "decomposed fusion: sum of the leg nodes (exact full-corpus weighted sum)".to_string()
}

/// One cascade hit's tree: the served score is the rerank leg's BM25
/// relevance; the dense score is the gate that admitted the document
/// to the pool.
pub fn cascade(
    hit: &CascadeHit,
    dense_id: &str,
    lexical_id: &str,
    boost: Option<&LegacyBoost>,
) -> Explanation {
    let mut details = vec![
        leaf(
            f64::from(hit.bm25_score),
            format!("leg {lexical_id:?}: phase-2 BM25 rescore over the phase-1 pool"),
        ),
        leaf(
            f64::from(hit.vector_score),
            format!(
                "leg {dense_id:?}: phase-1 dense score that admitted the document to the pool \
                 (gate, not an addend)"
            ),
        ),
    ];
    if let Some(boost) = boost {
        if hit.boost_score != 0.0 {
            details.push(legacy_boost_leaf(
                boost,
                f64::from(hit.bm25_score),
                hit.boost_score,
            ));
        }
    }
    node(
        f64::from(hit.bm25_score),
        "cascade: the served score is the rerank leg's relevance",
        details,
    )
}
