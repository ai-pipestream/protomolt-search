//! The deterministic calibrated lexical-semantic overlap backend.

use super::snippet::build_wire_snippet;
use super::{RerankCandidate, RerankSpec, RerankedHit};
use crate::pb::Snippet;

fn platt_sigmoid(logit: f64, slope: f64, intercept: f64) -> f64 {
    let z = (slope * logit + intercept).clamp(-60.0, 60.0);
    (1.0 / (1.0 + (-z).exp())).clamp(0.0, 1.0)
}

struct PrelimRerank {
    input_index: usize,
    raw_logit: f32,
    prob: f64,
    scored_sentences: Vec<(u32, u32, f64)>,
}

pub(super) fn rerank(
    spec: &RerankSpec,
    query_text: &str,
    shortlist: &[RerankCandidate],
    platt_slope: f64,
    platt_intercept: f64,
) -> Vec<RerankedHit> {
    let query_tokens: Vec<String> = query_text
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .collect();

    let mut prelim: Vec<PrelimRerank> = shortlist
        .iter()
        .enumerate()
        .map(|(idx, cand)| score_candidate(idx, cand, &query_tokens, platt_slope, platt_intercept))
        .collect();

    prelim.sort_by(|a, b| {
        b.prob
            .total_cmp(&a.prob)
            .then_with(|| b.raw_logit.total_cmp(&a.raw_logit))
            .then_with(|| {
                shortlist[a.input_index]
                    .doc_id
                    .cmp(&shortlist[b.input_index].doc_id)
            })
    });

    let mut out = Vec::with_capacity(prelim.len());
    for (rank, entry) in prelim.into_iter().enumerate() {
        let cand = &shortlist[entry.input_index];
        let snippets = if rank < spec.highlight_top_k {
            top_sentence_snippets(spec, cand, entry.scored_sentences)
        } else {
            Vec::new()
        };
        out.push(RerankedHit {
            input_index: entry.input_index,
            doc_id: cand.doc_id,
            raw_logit: entry.raw_logit,
            calibrated_probability: entry.prob,
            snippets,
        });
    }
    out
}

/// One candidate's raw logit and calibrated probability from its best
/// sentence overlap and its Stage-1 prior.
fn score_candidate(
    idx: usize,
    cand: &RerankCandidate,
    query_tokens: &[String],
    platt_slope: f64,
    platt_intercept: f64,
) -> PrelimRerank {
    let sentences = if cand.sentence_spans.is_empty() {
        fallback_sentence_spans(&cand.text)
    } else {
        cand.sentence_spans.clone()
    };

    let mut scored_sentences: Vec<(u32, u32, f64)> = Vec::with_capacity(sentences.len());
    let mut best_sent_score = 0.0_f64;
    for &(s, e) in &sentences {
        let Some(slice) = cand.text.get(s as usize..e as usize) else {
            continue;
        };
        if slice.trim().is_empty() {
            continue;
        };
        let sent_score = sentence_semantic_overlap(query_tokens, slice);
        best_sent_score = best_sent_score.max(sent_score);
        scored_sentences.push((s, e, sent_score));
    }

    // Combine normalized Stage-1 prior signal with best sentence semantic overlap
    // so pure dense hits (zero lexical token overlap) still receive valid calibrated
    // probabilities and lead-sentence semantic highlights.
    let prior_tanh = cand.prior_score.clamp(-10.0, 10.0).tanh();
    let raw_logit = (2.5 * best_sent_score + 0.75 * prior_tanh - 0.5) as f32;
    let prob = platt_sigmoid(f64::from(raw_logit), platt_slope, platt_intercept);
    PrelimRerank {
        input_index: idx,
        raw_logit,
        prob,
        scored_sentences,
    }
}

/// Snippets of the candidate's best-scoring sentences, at most
/// `max_snippets_per_candidate`.
fn top_sentence_snippets(
    spec: &RerankSpec,
    cand: &RerankCandidate,
    mut scored_sentences: Vec<(u32, u32, f64)>,
) -> Vec<Snippet> {
    let mut snippets = Vec::new();
    if spec.max_snippets_per_candidate == 0 || cand.text.is_empty() {
        return snippets;
    }
    scored_sentences.sort_by(|a, b| b.2.total_cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
    let lineage_shift = if spec.compose_parent_lineage_offsets {
        cand.lineage_span_start
    } else {
        0
    };
    for (s, e, _) in scored_sentences
        .into_iter()
        .take(spec.max_snippets_per_candidate)
    {
        if let Some(snip) = build_wire_snippet(
            &spec.field,
            &cand.text,
            &cand.sentence_spans,
            s,
            e,
            lineage_shift,
        ) {
            snippets.push(snip);
        }
    }
    snippets
}

fn sentence_semantic_overlap(query_tokens: &[String], sentence: &str) -> f64 {
    if sentence.is_empty() {
        return 0.0;
    }
    if query_tokens.is_empty() {
        return 0.25;
    }
    let sent_tokens: Vec<String> = sentence
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .collect();
    if sent_tokens.is_empty() {
        return 0.0;
    }
    let mut matched = 0usize;
    for qt in query_tokens {
        let prefix_len = qt.len().clamp(3, 5);
        let q_pref = &qt[..qt.floor_char_boundary(prefix_len.min(qt.len()))];
        if sent_tokens.iter().any(|st| {
            st == qt
                || (q_pref.len() >= 4 && st.starts_with(q_pref))
                || (st.len() >= 4 && qt.starts_with(st.as_str()))
        }) {
            matched += 1;
        }
    }
    matched as f64 / query_tokens.len() as f64
}

fn fallback_sentence_spans(text: &str) -> Vec<(u32, u32)> {
    let mut spans = Vec::new();
    let mut start = 0usize;
    for (idx, ch) in text.char_indices() {
        if matches!(ch, '.' | '!' | '?' | '\n') {
            let end = idx + ch.len_utf8();
            if text[start..end].trim().is_empty() {
                start = end;
                continue;
            }
            spans.push((start as u32, end as u32));
            start = end;
        }
    }
    if start < text.len() && !text[start..].trim().is_empty() {
        spans.push((start as u32, text.len() as u32));
    }
    if spans.is_empty() && !text.is_empty() {
        spans.push((0, text.len() as u32));
    }
    spans
}
