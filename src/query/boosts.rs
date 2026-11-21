//! Boost queries: candidate-scoped signals that score a fixed pool and
//! never admit a document.

use tonic::Status;

use super::refuse;
use super::response::ms;
use super::selection::Shape;
use crate::coordinator::CoordinatorServiceImpl;
use crate::pb::{
    search_query, BoostQuery, BoostRescore, DenseQuery, LexicalQuery, QueryHit, QuerySignal,
    SearchQuery,
};

/// One validated boost query, ready to score adapter-side through the
/// candidate-scoped rescore seams.
pub(super) struct ParsedBoost<'a> {
    id: &'a str,
    kind: BoostKind<'a>,
    window: u32,
    base_weight: f32,
    boost_weight: f32,
}

impl<'a> ParsedBoost<'a> {
    pub(super) fn new(id: &'a str, kind: BoostKind<'a>, boost: &BoostQuery) -> Self {
        ParsedBoost {
            id,
            kind,
            window: boost.window,
            base_weight: boost.base_weight,
            boost_weight: boost.boost_weight,
        }
    }
}

pub(super) enum BoostKind<'a> {
    /// A lexical boost with the analysis it resolves to: the
    /// selection's lexical leaf's spec when one exists (term identity
    /// must match the index the leaf searched), the boost's own on a
    /// dense-only selection (there is nothing to inherit).
    Lexical {
        text: &'a str,
        analysis: Option<crate::pb::AnalysisSpec>,
    },
    Dense {
        field: &'a str,
        vector: &'a [f32],
    },
}

/// How the request's boosts execute.
pub(super) enum BoostPlan<'a> {
    None,
    /// The composite + single lexical boost without a scorer keeps its
    /// original engine path (`BoostRescore` on the hybrid route),
    /// bitwise.
    LegacyHybrid(String, BoostRescore),
    /// Everything else scores adapter-side through the rescore seams:
    /// signal-only under a scorer, the weighted window reorder alone.
    Adapter(Vec<ParsedBoost<'a>>),
}

/// A reorder weight as the boost surface reads it: unset (0) is 1.0,
/// the `BoostRescore` convention.
pub(super) fn weight_or_one(weight: f32) -> f64 {
    if weight == 0.0 {
        1.0
    } else {
        f64::from(weight)
    }
}

/// A dense boost scores a fixed pool with the provider's native scores: it
/// selects nothing, so it names no execution mode, precision or routing.
pub(super) fn native_boost_scoring(dense: &DenseQuery) -> Result<(), Status> {
    if dense.execution_mode != 0 || dense.precision != 0 || dense.filter_strategy != 0 {
        return Err(refuse(
            "a dense boost scores its pool with provider-native candidate scoring; leave \
             execution_mode, precision and filter_strategy unset",
        ));
    }
    Ok(())
}

/// The boost's search query, which every boost must carry.
pub(super) fn boost_query(boost: &BoostQuery) -> Result<&SearchQuery, Status> {
    boost
        .query
        .as_ref()
        .ok_or_else(|| refuse("boost has no query"))
}

/// The lexical or dense query inside a boost's search query.
pub(super) fn boost_leaf(query: &SearchQuery) -> Result<&search_query::Query, Status> {
    query
        .query
        .as_ref()
        .ok_or_else(|| refuse(format!("boost {:?} has no query", query.id)))
}

/// Whether the boost names any knob of its own window reorder.
pub(super) fn names_reorder_knobs(boost: &BoostQuery) -> bool {
    boost.window != 0 || boost.base_weight != 0.0 || boost.boost_weight != 0.0
}

/// Boost validation. A boost never admits a document; it scores the
/// fixed candidate pool. Without a scorer exactly one boost is served
/// (its base_weight/boost_weight reorder is the combination); multiple
/// boosts need the composite scorer, which owns combination. With a
/// scorer present every boost is signal-only: the reorder knobs
/// (window, base_weight, boost_weight) are refused — each would be a
/// silent no-op — and the whole candidate set is scored.
pub(super) fn parse_boosts<'a>(
    boosts: &'a [BoostQuery],
    shape: &Shape<'a>,
    scorer_present: bool,
) -> Result<BoostPlan<'a>, Status> {
    if boosts.is_empty() {
        return Ok(BoostPlan::None);
    }
    if matches!(shape, Shape::Browse) {
        return Err(refuse(
            "a boost needs a SCORED selection: a browse has no base order to boost, and \
             reordering its deterministic id order would make it a scored query in \
             disguise",
        ));
    }
    if boosts.len() > 1 && !scorer_present {
        return Err(refuse(format!(
            "{} boost queries with no composite scorer; the scorer is what defines how \
             multiple boost signals combine — name one, or send a single boost with \
             base_weight/boost_weight",
            boosts.len()
        )));
    }
    let mut parsed = Vec::with_capacity(boosts.len());
    for boost in boosts {
        let query = boost_query(boost)?;
        if scorer_present && names_reorder_knobs(boost) {
            return Err(refuse(
                "window, base_weight, and boost_weight belong to the boost's own reorder; \
                 with a composite scorer present the boost is signal-only (the scorer owns \
                 combination and the whole candidate set is scored), so naming them would \
                 be a silent no-op",
            ));
        }
        let kind = match boost_leaf(query)? {
            search_query::Query::Lexical(lexical) => lexical_boost(&query.id, lexical, shape)?,
            search_query::Query::Dense(dense) => dense_boost(&query.id, dense)?,
        };
        parsed.push(ParsedBoost::new(&query.id, kind, boost));
    }
    if !scorer_present && parsed.len() == 1 && matches!(shape, Shape::Composite { .. }) {
        if let BoostKind::Lexical { text, .. } = &parsed[0].kind {
            return Ok(BoostPlan::LegacyHybrid(
                parsed[0].id.to_string(),
                BoostRescore {
                    text: (*text).to_string(),
                    window: parsed[0].window,
                    base_weight: parsed[0].base_weight,
                    boost_weight: parsed[0].boost_weight,
                },
            ));
        }
    }
    Ok(BoostPlan::Adapter(parsed))
}

fn lexical_boost<'a>(
    id: &str,
    lexical: &'a LexicalQuery,
    shape: &Shape<'_>,
) -> Result<BoostKind<'a>, Status> {
    if lexical.phrase.is_some() || !lexical.prefixes.is_empty() {
        return Err(refuse(
            "a phrase or prefix constraint on a boost query is not served; a boost scores \
             a fixed candidate set, and the phrase gate belongs to selection",
        ));
    }
    if !lexical.score_stages.is_empty() {
        return Err(refuse(
            "score_stages on a boost query are not served; stages ride the \
             selection's lexical leaf only",
        ));
    }
    let has_lexical_leaf = matches!(shape, Shape::Lexical { .. } | Shape::Composite { .. });
    if lexical.analysis.is_some() && has_lexical_leaf {
        return Err(refuse(
            "a boost query carries no analysis of its own when the selection \
             has a lexical leaf: the boost text is analyzed under that leaf's \
             analysis options (term identity must match the index it \
             searched), so a differing spec here would be silently ignored",
        ));
    }
    if lexical.text.is_empty() {
        return Err(refuse(format!("boost {id:?} has empty text")));
    }
    let analysis = match shape {
        Shape::Lexical { query: leaf, .. } => leaf.analysis.clone(),
        Shape::Composite { lexical: leaf, .. } => leaf.analysis.clone(),
        Shape::Dense { .. } => lexical.analysis.clone(),
        Shape::Browse => unreachable!("refused above"),
    };
    Ok(BoostKind::Lexical {
        text: &lexical.text,
        analysis,
    })
}

fn dense_boost<'a>(id: &str, dense: &'a DenseQuery) -> Result<BoostKind<'a>, Status> {
    native_boost_scoring(dense)?;
    if dense.vector.is_empty() {
        return Err(refuse(format!("dense boost {id:?} has an empty vector")));
    }
    Ok(BoostKind::Dense {
        field: &dense.field,
        vector: &dense.vector,
    })
}

/// Score the adapter-side boosts over the candidate pool: each boost
/// scores the top-`window` of the CURRENT order through its rescore
/// seam and lands its raw relevance as a named signal on the hits it
/// scored. Without a scorer (a single boost, by validation) the window
/// is then reordered by `base_weight * base + boost_weight * boost`
/// (0 = 1.0, the BoostRescore convention), ties by doc id; hits beyond
/// the window keep their order after it. The reported score stays the
/// BASE score — exactly the legacy boost surface: the boost's own
/// relevance rides provenance, never the score field.
pub(super) async fn apply_boosts(
    coordinator: &CoordinatorServiceImpl,
    plan: &BoostPlan<'_>,
    hits: &mut [QueryHit],
    scorer_present: bool,
    prof: &mut Option<crate::pb::QueryProfile>,
) -> Result<(), Status> {
    let BoostPlan::Adapter(list) = plan else {
        return Ok(());
    };
    let t0 = std::time::Instant::now();
    for b in list {
        let window = if b.window == 0 {
            hits.len()
        } else {
            (b.window as usize).min(hits.len())
        };
        let ids: Vec<u64> = hits[..window].iter().map(|h| h.doc_id).collect();
        let scores = match &b.kind {
            BoostKind::Lexical { text, analysis } => {
                coordinator
                    .lexical_signal(text, analysis.as_ref(), &ids)
                    .await?
            }
            BoostKind::Dense { field, vector } => {
                coordinator.dense_signal(vector, &ids, field).await?
            }
        };
        for hit in hits[..window].iter_mut() {
            if let Some(score) = scores.get(&hit.doc_id) {
                hit.signals.push(QuerySignal {
                    id: b.id.to_string(),
                    score: *score,
                });
                hit.matched.push(b.id.to_string());
            }
        }
        if !scorer_present {
            reorder_window(&mut hits[..window], b);
        }
    }
    if let Some(p) = prof.as_mut() {
        p.boost_ms = ms(t0);
    }
    Ok(())
}

/// Reorder a boost's window by its weighted base-plus-boost key, ties by
/// doc id.
fn reorder_window(window: &mut [QueryHit], b: &ParsedBoost<'_>) {
    let base_w = weight_or_one(b.base_weight);
    let boost_w = weight_or_one(b.boost_weight);
    let combined = |h: &QueryHit| {
        let boost = h
            .signals
            .iter()
            .find(|s| s.id == b.id)
            .map_or(0.0, |s| f64::from(s.score));
        base_w * f64::from(h.score) + boost_w * boost
    };
    window.sort_by(|a, c| {
        combined(c)
            .total_cmp(&combined(a))
            .then_with(|| a.doc_id.cmp(&c.doc_id))
    });
}

/// The window boosts the explain tree reports: the adapter-planned
/// boosts with their reorder weights defaulted the way `apply_boosts`
/// defaults them. A legacy hybrid boost is reported by the composite
/// builder instead; a scorer-owned boost is a plain signal and appears
/// as a dimension.
pub(super) fn window_boosts(plan: &BoostPlan<'_>) -> Vec<crate::explain::WindowBoost> {
    match plan {
        BoostPlan::Adapter(list) => list
            .iter()
            .map(|b| crate::explain::WindowBoost {
                id: b.id.to_string(),
                base_weight: weight_or_one(b.base_weight),
                boost_weight: weight_or_one(b.boost_weight),
            })
            .collect(),
        _ => Vec::new(),
    }
}
