//! The per-document explain tree (`docs/explain.md`).
//!
//! An [`Explanation`] is a node with a value, a description of what
//! that value is, and the nodes it was computed from. The rule for
//! every node this module builds: the description states the function
//! of the children, and the root's value is the hit's served score
//! (its f32) so a reader can check the tree against the response
//! without a calculator. The tree is assembled from numbers the
//! engine already computed on the path that produced the hit; it
//! never re-scores anything, so a request with `explain` on returns
//! the same hits in the same order, bitwise, as one with it off.

use crate::pb::{
    Bm25Hit, CascadeHit, DenseScoreMode, Explanation, FusionMode, HybridHit, HybridLegOptions,
    PrefixExpansion, QueryHit, ScoreCombination, ScoreNormalization, SynonymExpansion,
};
use tonic::Status;

/// A node with children.
pub fn node(value: f64, description: impl Into<String>, details: Vec<Explanation>) -> Explanation {
    Explanation {
        value,
        description: description.into(),
        details,
    }
}

/// A node without children.
pub fn leaf(value: f64, description: impl Into<String>) -> Explanation {
    node(value, description, Vec::new())
}

/// The lexical leaf's tree from the shard's breakdown: one node per
/// (field, term) contribution with the BM25 inputs under it, prefix
/// and synonym expansions grouped under the term that produced them,
/// the sum, then the score stages in evaluation order, then the leaf's
/// served f32.
pub fn lexical(
    id: &str,
    hit: &Bm25Hit,
    prefixes: &[PrefixExpansion],
    synonyms: &[SynonymExpansion],
) -> Result<Explanation, Status> {
    let breakdown = hit.explain.as_ref().ok_or_else(|| {
        Status::internal(format!(
            "explain: the lexical route returned document {} without its scoring breakdown",
            hit.doc_id
        ))
    })?;
    // Which source produced each scored term: a prefix, a synonym
    // rule, or the query text itself. A term can be both a query term
    // and an expansion of another; the query text wins, so a term the
    // user typed is never filed under a rule.
    let mut term_nodes: Vec<Explanation> = Vec::new();
    let mut groups: Vec<(String, Vec<Explanation>)> = Vec::new();
    for term in &breakdown.terms {
        let inputs = vec![
            leaf(
                term.tf_norm,
                format!(
                    "tf_norm = tf * (k1 + 1) / (tf + k1 * (1 - b + b * dl / avgdl)) with tf={}, \
                     dl={}, avgdl={}, k1={}, b={}",
                    term.tf, term.doc_length, term.avgdl, term.k1, term.b
                ),
            ),
            leaf(
                term.idf,
                format!(
                    "idf = ln(1 + (N - df + 0.5) / (df + 0.5)) with N={}, df={}",
                    term.doc_count, term.df
                ),
            ),
            leaf(term.weight, "field weight"),
        ];
        let mut inputs = inputs;
        let field = if term.field.is_empty() {
            "body".to_string()
        } else {
            term.field.clone()
        };
        let description = if term.phrase_group {
            inputs.push(leaf(term.phrase_weight, "phrase term weight"));
            format!(
                "term {:?} in field {field}: weight * idf * tf_norm * phrase weight (phrase \
                 group, combined by maximum)",
                term.term
            )
        } else {
            format!(
                "term {:?} in field {field}: weight * idf * tf_norm",
                term.term
            )
        };
        let term_node = node(term.contribution, description, inputs);
        let source = prefixes
            .iter()
            .find(|p| p.field == term.field && p.terms.contains(&term.term))
            .map(|p| format!("expansions of prefix {:?} in field {field}", p.prefix))
            .or_else(|| {
                synonyms
                    .iter()
                    .find(|s| s.field == term.field && s.terms.contains(&term.term))
                    .map(|s| format!("synonyms of {:?} in field {field}", s.term))
            });
        match source {
            Some(label) => match groups.iter_mut().find(|(l, _)| *l == label) {
                Some((_, list)) => list.push(term_node),
                None => groups.push((label, vec![term_node])),
            },
            None => term_nodes.push(term_node),
        }
    }
    for (label, list) in groups {
        let sum: f64 = list.iter().map(|n| n.value).sum();
        term_nodes.push(node(
            sum,
            format!("{label}: sum of the expansions' contributions"),
            list,
        ));
    }
    let sum_description = if breakdown.phrase {
        format!(
            "BM25 sum over {} (field, term) contributions: the base fields summed plus the \
             phrase group's maximum ({})",
            breakdown.terms.len(),
            breakdown.phrase_max
        )
    } else {
        format!(
            "BM25 sum over {} (field, term) contributions",
            breakdown.terms.len()
        )
    };
    let mut current = node(breakdown.bm25, sum_description, term_nodes);
    for stage in &breakdown.stages {
        let name = if stage.key.is_empty() {
            stage.column.clone()
        } else {
            format!("{}[{}]", stage.column, stage.key)
        };
        let description = if stage.present {
            format!(
                "score stage {} on column {name}: input {} gives contribution {} applied to \
                 the node below (docs/score-functions.md)",
                stage.stage, stage.input, stage.contribution
            )
        } else {
            format!(
                "score stage {} on column {name}: the document has no value, identity",
                stage.stage
            )
        };
        current = node(stage.output, description, vec![current]);
    }
    Ok(node(
        f64::from(hit.score),
        format!("lexical leaf {id:?}: BM25 relevance, served as f32 of the arithmetic below"),
        vec![current],
    ))
}

/// The dense leaf's tree. `estimate` is the candidate score the
/// provider produced before an exact FP32 rerank replaced it.
pub fn dense(id: &str, score: f32, mode: DenseScoreMode, estimate: Option<f32>) -> Explanation {
    match (mode, estimate) {
        (DenseScoreMode::Fp32Rerank, Some(estimate)) => node(
            f64::from(score),
            format!(
                "dense leaf {id:?}: exact FP32 dot product of the query with the stored \
                 vector (docs/query-api.md \"Dense FP32 rerank\")"
            ),
            vec![leaf(
                f64::from(estimate),
                "the provider's candidate score that selected this document for the rerank \
                 (not part of the served score)",
            )],
        ),
        _ => leaf(
            f64::from(score),
            format!(
                "dense leaf {id:?}: the provider's native similarity score for the stored \
                 vector (docs/dense-execution-policy.md)"
            ),
        ),
    }
}

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
        FusionMode::ScoreBlend => {
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
            match fusion.combination {
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
            }
        }
        FusionMode::Decomposed => {
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

/// A boolean root's tree: the sum of its positive scoring clauses'
/// relevance, each clause a leaf under its id.
pub fn boolean(hit: &QueryHit) -> Explanation {
    let details = hit
        .signals
        .iter()
        .map(|signal| {
            leaf(
                f64::from(signal.score),
                format!(
                    "clause {:?}: the leaf's relevance for this document",
                    signal.id
                ),
            )
        })
        .collect();
    node(
        f64::from(hit.score),
        "boolean root: sum of the positive scoring clauses (docs/query-api.md \"Recursive \
         boolean execution\")",
        details,
    )
}

/// A request-level boost that reorders its window without changing
/// the served score (`BoostQuery` without a composite scorer).
#[derive(Debug, Clone)]
pub struct WindowBoost {
    pub id: String,
    pub base_weight: f64,
    pub boost_weight: f64,
}

/// Finish each hit's tree after the boost and scorer phases. With a
/// composite scorer the root becomes the scorer's operation over its
/// dimension nodes, the selection tree kept under it as the provenance
/// of the selection signal; with window boosts and no scorer the root
/// keeps the selection's value and gains one leaf per boost signal
/// that says how the window was ordered.
pub fn finish(
    hits: &mut [QueryHit],
    boosts: &[WindowBoost],
    scorer: Option<&str>,
) -> Result<(), Status> {
    for hit in hits.iter_mut() {
        let selection = hit.explain.take().ok_or_else(|| {
            Status::internal(format!(
                "explain: document {} reached the finish pass without a selection tree",
                hit.doc_id
            ))
        })?;
        hit.explain = Some(match scorer {
            Some(operation) => {
                let mut details: Vec<Explanation> = hit
                    .dimensions
                    .iter()
                    .map(|dim| {
                        let mut inputs = Vec::new();
                        if let Some(raw) = dim.raw {
                            inputs.push(leaf(raw, "the dimension's raw signal"));
                        }
                        inputs.push(leaf(
                            dim.normalized,
                            "the raw signal after the dimension's normalization over the pool",
                        ));
                        let description = if dim.skipped {
                            format!(
                                "dimension {:?}: skipped (disabled, or missing under the SKIP \
                                 policy); contributes no term",
                                dim.id
                            )
                        } else if dim.raw.is_none() {
                            format!(
                                "dimension {:?}: missing signal scored as 0 under the ZERO \
                                 policy, times weight",
                                dim.id
                            )
                        } else {
                            format!("dimension {:?}: normalized signal times weight", dim.id)
                        };
                        node(dim.contribution, description, inputs)
                    })
                    .collect();
                details.push(node(
                    selection.value,
                    "the selection score this document entered the scorer with (provenance \
                     of the selection signal, not a term of the operation)",
                    vec![selection],
                ));
                node(
                    f64::from(hit.score),
                    format!(
                        "composite scorer {operation} over the dimension nodes \
                         (docs/query-api.md \"Composite scorer\")"
                    ),
                    details,
                )
            }
            None => {
                let mut details = vec![selection];
                for boost in boosts {
                    if let Some(signal) = hit.signals.iter().find(|s| s.id == boost.id) {
                        let key = boost.base_weight * f64::from(hit.score)
                            + boost.boost_weight * f64::from(signal.score);
                        details.push(leaf(
                            f64::from(signal.score),
                            format!(
                                "boost {:?}: the boost query's relevance; orders the window by \
                                 base_weight {} * score + boost_weight {} * boost = {key}, and is \
                                 not part of the served score",
                                boost.id, boost.base_weight, boost.boost_weight
                            ),
                        ));
                    }
                }
                if details.len() == 1 {
                    details.pop().expect("the selection tree")
                } else {
                    node(
                        f64::from(hit.score),
                        "the selection score, unchanged by the boost; the boost leaf says how \
                         the window was ordered",
                        details,
                    )
                }
            }
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lexical_tree_recomposes_its_sum_and_groups_expansions() {
        let hit = Bm25Hit {
            doc_id: 7,
            score: 1.5,
            terms: Vec::new(),
            projected: Vec::new(),
            snippets: Vec::new(),
            explain: Some(crate::pb::Bm25Explain {
                terms: vec![
                    crate::pb::Bm25TermExplain {
                        term: "zebra".into(),
                        tf: 1,
                        doc_length: 3,
                        avgdl: 3.0,
                        k1: 1.2,
                        b: 0.75,
                        tf_norm: 1.0,
                        doc_count: 8,
                        df: 4,
                        idf: 1.0,
                        weight: 1.0,
                        contribution: 1.0,
                        ..Default::default()
                    },
                    crate::pb::Bm25TermExplain {
                        term: "zebras".into(),
                        contribution: 0.5,
                        weight: 1.0,
                        ..Default::default()
                    },
                ],
                bm25: 1.5,
                stages: Vec::new(),
                phrase: false,
                phrase_max: 0.0,
            }),
        };
        let prefixes = vec![PrefixExpansion {
            field: String::new(),
            prefix: "zeb".into(),
            terms: vec!["zebras".into()],
        }];
        let tree = lexical("lex", &hit, &prefixes, &[]).unwrap();
        assert_eq!(tree.value, 1.5);
        let sum = &tree.details[0];
        assert_eq!(sum.value, 1.5);
        assert_eq!(sum.details.len(), 2, "one plain term and one prefix group");
        let group = sum
            .details
            .iter()
            .find(|n| n.description.contains("prefix \"zeb\""))
            .expect("the prefix group");
        assert_eq!(group.value, 0.5);
        assert_eq!(
            group.details[0].description,
            "term \"zebras\" in field body: weight * idf * tf_norm"
        );
    }

    #[test]
    fn a_lexical_hit_without_a_breakdown_is_an_internal_error() {
        let hit = Bm25Hit {
            doc_id: 1,
            score: 1.0,
            terms: Vec::new(),
            projected: Vec::new(),
            snippets: Vec::new(),
            explain: None,
        };
        let err = lexical("lex", &hit, &[], &[]).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
    }

    #[test]
    fn rrf_legs_sum_to_the_fused_score() {
        let fusion = Fusion {
            mode: FusionMode::GlobalRank,
            rrf_k: 60.0,
            vector_weight: 1.0,
            bm25_weight: 1.0,
            normalization: ScoreNormalization::MinMax,
            combination: ScoreCombination::Arithmetic,
        };
        let fused = 1.0 / 61.0 + 1.0 / 63.0;
        let hit = HybridHit {
            doc_id: 3,
            fused_score: fused as f32,
            shard: 0,
            vector_rank: Some(1),
            vector_score: 0.9,
            bm25_rank: Some(3),
            bm25_score: 2.0,
            boost_score: 0.0,
            vector_normalized: None,
            bm25_normalized: None,
        };
        let tree = composite(&hit, &fusion, "d", "l", None).unwrap();
        let sum: f64 = tree.details.iter().map(|n| n.value).sum();
        assert!((sum - tree.value).abs() < 1e-6, "{sum} vs {}", tree.value);
    }
}
