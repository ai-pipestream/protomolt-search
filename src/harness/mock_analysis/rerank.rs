//! The mock reranker: query-token overlap per sentence span, a logistic
//! calibration of the best span, and the top spans as highlights.

use turbo_client::proto::result::{
    ModelRef, RerankAndHighlightRequest, RerankAndHighlightResponse, RerankCandidateInputProto,
    RerankedCandidateProto, SemanticHighlightSpanProto,
};

/// Lowercased alphanumeric words.
fn words(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .collect()
}

/// The candidate's sentence spans, or the whole text as one span.
fn candidate_spans(cand: &RerankCandidateInputProto) -> Vec<(u32, u32)> {
    if !cand.sentence_span_start.is_empty()
        && cand.sentence_span_start.len() == cand.sentence_span_end.len()
    {
        cand.sentence_span_start
            .iter()
            .copied()
            .zip(cand.sentence_span_end.iter().copied())
            .collect()
    } else if !cand.text.is_empty() {
        vec![(0, cand.text.len() as u32)]
    } else {
        Vec::new()
    }
}

/// The share of query tokens a span's words match (exactly or as a
/// prefix); an empty query scores every span 0.5.
fn overlap(q_tokens: &[String], slice: &str) -> f64 {
    if q_tokens.is_empty() {
        return 0.5;
    }
    let words = words(slice);
    let matched = q_tokens
        .iter()
        .filter(|qt| words.iter().any(|w| w == *qt || w.starts_with(qt.as_str())))
        .count();
    matched as f64 / q_tokens.len() as f64
}

fn rerank_candidate(
    idx: usize,
    cand: &RerankCandidateInputProto,
    q_tokens: &[String],
    max_snips: usize,
) -> RerankedCandidateProto {
    let mut scored_spans: Vec<(u32, u32, f64)> = Vec::new();
    let mut best = 0.0_f64;
    for (s, e) in candidate_spans(cand) {
        let Some(slice) = cand.text.get(s as usize..e as usize) else {
            continue;
        };
        let score = overlap(q_tokens, slice);
        best = best.max(score);
        scored_spans.push((s, e, score));
    }
    scored_spans.sort_by(|a, b| b.2.total_cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
    let raw_logit = (3.0 * best - 1.0) as f32;
    let prob = (1.0 / (1.0 + (-f64::from(raw_logit)).exp())).clamp(0.0, 1.0);
    let highlights = scored_spans
        .into_iter()
        .take(max_snips)
        .filter_map(|(s, e, score)| {
            cand.text
                .get(s as usize..e as usize)
                .map(|surface| SemanticHighlightSpanProto {
                    span_start: s,
                    span_end: e,
                    parent_span_start: cand.parent_offset + u64::from(s),
                    parent_span_end: cand.parent_offset + u64::from(e),
                    score,
                    surface: surface.to_string(),
                })
        })
        .collect();
    RerankedCandidateProto {
        input_index: idx as u32,
        doc_id: cand.doc_id.clone(),
        raw_logit,
        calibrated_probability: prob,
        conformal_p_value: None,
        highlights,
        qa_abstained: false,
        qa_null_probability: None,
    }
}

/// Rerank every candidate, most probable first, input order on ties.
pub(super) fn rerank_and_highlight(inner: RerankAndHighlightRequest) -> RerankAndHighlightResponse {
    let q_tokens = words(&inner.query);
    let max_snips = inner.max_snippets_per_candidate as usize;
    let mut results: Vec<RerankedCandidateProto> = inner
        .candidates
        .iter()
        .enumerate()
        .map(|(idx, cand)| rerank_candidate(idx, cand, &q_tokens, max_snips))
        .collect();
    results.sort_by(|a, b| {
        b.calibrated_probability
            .total_cmp(&a.calibrated_probability)
            .then_with(|| a.input_index.cmp(&b.input_index))
    });
    RerankAndHighlightResponse {
        query: inner.query,
        rerank_model: Some(ModelRef {
            id: inner.rerank_model,
            revision: "mock-v1".into(),
            sha256: String::new(),
        }),
        highlight_model: None,
        offsets: inner.offsets,
        results,
    }
}
