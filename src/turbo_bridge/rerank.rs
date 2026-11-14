//! Stage-2 calibrated `RerankAndHighlight` integration.
//!
//! Reranks a top-`C` Stage-1 shortlist into calibrated `[0.0, 1.0]` relevance
//! probabilities while attributing semantic highlight spans (`QueryHit.snippets`)
//! in original `ViewId::SOURCE` UTF-8 byte coordinates (with optional
//! `DocLineage.span_start` parent-document offset composition), populating
//! snippets on pure dense (`VectorQuery`) and hybrid hits even when zero
//! lexical postings matched.

#[cfg(feature = "local-core")]
use std::sync::Arc;

use tonic::Status;

use crate::pb::{OffsetSpan, Snippet, SnippetCut};
use crate::turbo_bridge::remaining_deadline_duration;

/// Execution backend for Stage-2 calibrated reranking and semantic highlighting.
#[derive(Clone)]
pub enum RerankBackend {
    /// Remote `turbo-kserve` `AnalysisService::RerankAndHighlight` execution over
    /// runtime-scoped pooled HTTP/2 with `RemotePriority::Interactive`, bearer
    /// auth / mTLS, and `finish_deadline_us` propagation.
    #[cfg(feature = "net")]
    Remote {
        /// Pooled remote `turbo-kserve` client.
        client: Box<turbo_client::RemoteClient>,
        /// `TURBO_TASK_RERANK` bundle name served on `turbo-kserve`.
        rerank_model: String,
        /// Optional `TURBO_TASK_EMBED` bundle name for pooled/MaxSim highlights.
        embed_model: String,
        /// Optional `TURBO_TASK_EXTRACTIVE_QA` bundle name for extractive QA spans.
        qa_model: String,
        /// Temperature calibration factor (`> 0.0`, default `1.0`).
        temperature: f64,
    },
    /// In-process `libturbo` `TURBO_TASK_RERANK` bundle (`feature = "local-core"`).
    #[cfg(feature = "local-core")]
    Local {
        /// Thread-safe local rerank + calibrator session.
        engine: Arc<turbo_client::LocalRerankEngine>,
    },
    /// Deterministic calibrated lexical-semantic overlap scorer (`Platt`
    /// logistic mapping into `[0.0, 1.0]`) with sentence/window span attribution
    /// for embedded / CPU-only deployments and deterministic integration tests.
    DeterministicCalibrated {
        /// Logistic Platt slope (`a` in `sigmoid(a * z + b)`).
        platt_slope: f64,
        /// Logistic Platt intercept (`b` in `sigmoid(a * z + b)`).
        platt_intercept: f64,
    },
}

impl std::fmt::Debug for RerankBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "net")]
            Self::Remote { rerank_model, .. } => f
                .debug_struct("Remote")
                .field("rerank_model", rerank_model)
                .finish(),
            #[cfg(feature = "local-core")]
            Self::Local { .. } => f.debug_struct("Local").finish(),
            Self::DeterministicCalibrated {
                platt_slope,
                platt_intercept,
            } => f
                .debug_struct("DeterministicCalibrated")
                .field("platt_slope", platt_slope)
                .field("platt_intercept", platt_intercept)
                .finish(),
        }
    }
}

/// Stage-2 reranking and semantic highlight policy attached to a coordinator
/// or query execution plan.
#[derive(Clone, Debug)]
pub struct RerankSpec {
    /// Maximum Stage-1 shortlist depth `C` sent to the cross-encoder (`>= 1`).
    pub shortlist_size: usize,
    /// Top-`K` reranked candidates for which semantic highlight spans are computed.
    pub highlight_top_k: usize,
    /// Maximum non-overlapping semantic snippets emitted per candidate.
    pub max_snippets_per_candidate: usize,
    /// Stored text field name reported on emitted [`Snippet`] messages.
    pub field: String,
    /// ISO 639-3 sentence segmentation language code (e.g. `"eng"`).
    pub language: String,
    /// When `true`, adds `DocLineage.span_start` onto emitted snippet and
    /// highlight UTF-8 byte offsets so chunk hits project directly onto their
    /// parent document's coordinate space.
    pub compose_parent_lineage_offsets: bool,
    /// Optional explicit query string used when reranking a pure `VectorQuery`
    /// selection that carries only float coordinates on the wire.
    pub query_text_override: Option<String>,
    /// Underlying remote, local-bundle, or deterministic calibrated backend.
    pub backend: RerankBackend,
}

impl RerankSpec {
    /// Creates a Stage-2 rerank specification with sensible defaults (`C = 32`,
    /// `highlight_top_k = 10`, `max_snippets = 2`, `field = "body"`).
    pub fn new(backend: RerankBackend) -> Self {
        Self {
            shortlist_size: 32,
            highlight_top_k: 10,
            max_snippets_per_candidate: 2,
            field: "body".to_string(),
            language: "eng".to_string(),
            compose_parent_lineage_offsets: false,
            query_text_override: None,
            backend,
        }
    }
}

/// Candidate passage supplied to [`rerank_and_highlight_shortlist`].
#[derive(Clone, Debug)]
pub struct RerankCandidate {
    /// Document identifier (`QueryHit.id`).
    pub doc_id: u64,
    /// Stage-1 retrieval or fusion score prior to Stage-2 reranking.
    pub prior_score: f64,
    /// Stored original UTF-8 text of `RerankSpec::field`.
    pub text: String,
    /// Optional stored sentence spans `(start_utf8, end_utf8)` over `text`.
    pub sentence_spans: Vec<(u32, u32)>,
    /// Optional `DocLineage.span_start` offset within a parent document.
    pub lineage_span_start: u32,
}

/// Calibrated Stage-2 rerank output and semantic snippets for one candidate.
#[derive(Clone, Debug)]
pub struct RerankedHit {
    /// Zero-based index into the input `candidates` slice.
    pub input_index: usize,
    /// Document identifier (`QueryHit.id`).
    pub doc_id: u64,
    /// Uncalibrated cross-encoder logit.
    pub raw_logit: f32,
    /// Calibrated relevance probability strictly within `[0.0, 1.0]`.
    pub calibrated_probability: f64,
    /// Source-projected semantic highlight snippets (`QueryHit.snippets`).
    pub snippets: Vec<Snippet>,
}

/// Reranks up to `spec.shortlist_size` candidates in descending order of
/// calibrated `[0.0, 1.0]` relevance probability and attributes semantic
/// highlight snippets.
pub async fn rerank_and_highlight_shortlist(
    spec: &RerankSpec,
    query_text: &str,
    candidates: &[RerankCandidate],
    finish_deadline_us: u64,
) -> Result<Vec<RerankedHit>, Status> {
    if candidates.is_empty() || spec.shortlist_size == 0 {
        return Ok(Vec::new());
    }
    let limit = candidates.len().min(spec.shortlist_size);
    let shortlist = &candidates[..limit];
    let _timeout = remaining_deadline_duration(finish_deadline_us)?;

    match &spec.backend {
        #[cfg(feature = "net")]
        RerankBackend::Remote {
            client,
            rerank_model,
            embed_model,
            qa_model,
            temperature,
        } => {
            use turbo_client::proto::result::{
                rerank_calibration_spec_proto::Kind as CalKind, OffsetMode,
                RerankAndHighlightRequest, RerankCalibrationSpecProto, RerankCandidateInputProto,
                RerankHighlightMode,
            };

            let wire_candidates: Vec<RerankCandidateInputProto> = shortlist
                .iter()
                .map(|c| RerankCandidateInputProto {
                    doc_id: c.doc_id.to_string(),
                    text: c.text.clone(),
                    parent_offset: u64::from(c.lineage_span_start),
                    sentence_span_start: c.sentence_spans.iter().map(|&(s, _)| s).collect(),
                    sentence_span_end: c.sentence_spans.iter().map(|&(_, e)| e).collect(),
                })
                .collect();

            let req = RerankAndHighlightRequest {
                query: query_text.to_string(),
                rerank_model: rerank_model.clone(),
                embed_model: embed_model.clone(),
                qa_model: qa_model.clone(),
                candidates: wire_candidates,
                offsets: OffsetMode::Utf8 as i32,
                calibration: Some(RerankCalibrationSpecProto {
                    kind: Some(CalKind::Temperature((*temperature).max(1e-6))),
                    domain: String::new(),
                    inference_hash: 0,
                }),
                highlight_mode: if spec.max_snippets_per_candidate == 0 {
                    RerankHighlightMode::None as i32
                } else {
                    RerankHighlightMode::Auto as i32
                },
                highlight_top_k: spec.highlight_top_k as u32,
                max_snippets_per_candidate: spec.max_snippets_per_candidate as u32,
                language: spec.language.clone(),
                query_tokens: 0,
                max_tokens: 0,
                stride: 0,
                max_answer_tokens: 0,
                qa_null_threshold: None,
            };

            let resp = client
                .rerank_and_highlight(req, _timeout)
                .await
                .map_err(|e| crate::analyzer::remote_client_err_to_status(client, e))?;

            let mut out = Vec::with_capacity(resp.results.len());
            for item in resp.results {
                let idx = item.input_index as usize;
                let Some(cand) = shortlist.get(idx) else {
                    return Err(Status::internal(format!(
                        "RerankAndHighlight returned out-of-range input_index {idx} for {} candidates",
                        shortlist.len()
                    )));
                };
                let prob = item.calibrated_probability.clamp(0.0, 1.0);
                let mut snippets = Vec::with_capacity(item.highlights.len());
                for h in item.highlights {
                    if let Some(snip) = build_wire_snippet(
                        &spec.field,
                        &cand.text,
                        &cand.sentence_spans,
                        h.span_start,
                        h.span_end,
                        if spec.compose_parent_lineage_offsets {
                            cand.lineage_span_start
                        } else {
                            0
                        },
                    ) {
                        snippets.push(snip);
                    }
                }
                out.push(RerankedHit {
                    input_index: idx,
                    doc_id: cand.doc_id,
                    raw_logit: item.raw_logit,
                    calibrated_probability: prob,
                    snippets,
                });
            }
            sort_reranked_hits(&mut out);
            Ok(out)
        }
        #[cfg(feature = "local-core")]
        RerankBackend::Local { engine } => {
            let engine = Arc::clone(engine);
            let query_owned = query_text.to_string();
            let field = spec.field.clone();
            let language = spec.language.clone();
            let max_snips = spec.max_snippets_per_candidate;
            let highlight_top_k = spec.highlight_top_k;
            let compose_lineage = spec.compose_parent_lineage_offsets;
            let cands_owned: Vec<RerankCandidate> = shortlist.to_vec();

            tokio::task::spawn_blocking(move || {
                let pairs: Vec<(&str, &[_])> = cands_owned
                    .iter()
                    .map(|c| (c.text.as_str(), &[][..]))
                    .collect();
                let hits = engine
                    .rerank_and_highlight(
                        &query_owned,
                        &pairs,
                        turbo_client::LocalRerankOptions::default(),
                        max_snips,
                        &language,
                        None,
                    )
                    .map_err(|e| Status::internal(format!("local rerank failed: {e}")))?;

                let mut out = Vec::with_capacity(hits.len());
                for (rank, h) in hits.into_iter().enumerate() {
                    let cand = &cands_owned[h.candidate_index];
                    let mut snippets = Vec::new();
                    if rank < highlight_top_k {
                        for s in h.snippets {
                            let (u8_start, u8_end) = s.spans.utf8;
                            if let Some(snip) = build_wire_snippet(
                                &field,
                                &cand.text,
                                &cand.sentence_spans,
                                u8_start,
                                u8_end,
                                if compose_lineage {
                                    cand.lineage_span_start
                                } else {
                                    0
                                },
                            ) {
                                snippets.push(snip);
                            }
                        }
                    }
                    out.push(RerankedHit {
                        input_index: h.candidate_index,
                        doc_id: cand.doc_id,
                        raw_logit: h.score.raw_logit,
                        calibrated_probability: h.score.calibrated_probability.clamp(0.0, 1.0),
                        snippets,
                    });
                }
                sort_reranked_hits(&mut out);
                Ok(out)
            })
            .await
            .map_err(|e| Status::internal(format!("local rerank join error: {e}")))?
        }
        RerankBackend::DeterministicCalibrated {
            platt_slope,
            platt_intercept,
        } => Ok(deterministic_rerank_and_highlight(
            spec,
            query_text,
            shortlist,
            *platt_slope,
            *platt_intercept,
        )),
    }
}

#[cfg(any(feature = "net", feature = "local-core"))]
fn sort_reranked_hits(hits: &mut [RerankedHit]) {
    hits.sort_by(|a, b| {
        b.calibrated_probability
            .total_cmp(&a.calibrated_probability)
            .then_with(|| b.raw_logit.total_cmp(&a.raw_logit))
            .then_with(|| a.doc_id.cmp(&b.doc_id))
    });
}

fn platt_sigmoid(logit: f64, slope: f64, intercept: f64) -> f64 {
    let z = (slope * logit + intercept).clamp(-60.0, 60.0);
    (1.0 / (1.0 + (-z).exp())).clamp(0.0, 1.0)
}

fn deterministic_rerank_and_highlight(
    spec: &RerankSpec,
    query_text: &str,
    shortlist: &[RerankCandidate],
    platt_slope: f64,
    platt_intercept: f64,
) -> Vec<RerankedHit> {
    struct PrelimRerank {
        input_index: usize,
        raw_logit: f32,
        prob: f64,
        scored_sentences: Vec<(u32, u32, f64)>,
    }

    let query_tokens: Vec<String> = query_text
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .collect();

    let mut prelim: Vec<PrelimRerank> = Vec::with_capacity(shortlist.len());

    for (idx, cand) in shortlist.iter().enumerate() {
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
            let sent_score = sentence_semantic_overlap(&query_tokens, slice);
            best_sent_score = best_sent_score.max(sent_score);
            scored_sentences.push((s, e, sent_score));
        }

        // Combine normalized Stage-1 prior signal with best sentence semantic overlap
        // so pure dense hits (zero lexical token overlap) still receive valid calibrated
        // probabilities and lead-sentence semantic highlights.
        let prior_tanh = cand.prior_score.clamp(-10.0, 10.0).tanh();
        let raw_logit = (2.5 * best_sent_score + 0.75 * prior_tanh - 0.5) as f32;
        let prob = platt_sigmoid(f64::from(raw_logit), platt_slope, platt_intercept);
        prelim.push(PrelimRerank {
            input_index: idx,
            raw_logit,
            prob,
            scored_sentences,
        });
    }

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
    for (rank, mut entry) in prelim.into_iter().enumerate() {
        let cand = &shortlist[entry.input_index];
        let mut snippets = Vec::new();
        if rank < spec.highlight_top_k
            && spec.max_snippets_per_candidate > 0
            && !cand.text.is_empty()
        {
            entry
                .scored_sentences
                .sort_by(|a, b| b.2.total_cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
            let lineage_shift = if spec.compose_parent_lineage_offsets {
                cand.lineage_span_start
            } else {
                0
            };
            for (s, e, _) in entry
                .scored_sentences
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
        }
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

fn snap_char_bounds(text: &str, start: u32, end: u32) -> Option<(usize, usize)> {
    if text.is_empty() {
        return None;
    }
    let s = text.floor_char_boundary((start as usize).min(text.len()));
    let e = text.ceil_char_boundary((end as usize).min(text.len()));
    (s < e).then_some((s, e))
}

fn build_wire_snippet(
    field: &str,
    text: &str,
    sentence_spans: &[(u32, u32)],
    raw_start: u32,
    raw_end: u32,
    lineage_shift: u32,
) -> Option<Snippet> {
    let (s, e) = snap_char_bounds(text, raw_start, raw_end)?;
    let slice = &text[s..e];
    let s_u32 = s as u32;
    let e_u32 = e as u32;
    let sentence_index = sentence_spans
        .iter()
        .position(|&(ss, se)| ss == s_u32 && se == e_u32)
        .map(|i| i as u32);
    let cut = if sentence_index.is_some() {
        SnippetCut::Sentence as i32
    } else {
        SnippetCut::Window as i32
    };
    let shifted_start = s_u32.saturating_add(lineage_shift);
    let shifted_end = e_u32.saturating_add(lineage_shift);
    Some(Snippet {
        field: field.to_string(),
        text: slice.to_string(),
        start: shifted_start,
        end: shifted_end,
        highlights: vec![OffsetSpan {
            start: shifted_start,
            end: shifted_end,
        }],
        cut,
        sentence_index,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn calibrated_rerank_populates_snippets_on_dense_hits_and_composes_lineage_offsets() {
        let mut spec = RerankSpec::new(RerankBackend::DeterministicCalibrated {
            platt_slope: 1.5,
            platt_intercept: 0.0,
        });
        spec.compose_parent_lineage_offsets = true;

        let candidates = vec![
            RerankCandidate {
                doc_id: 10,
                prior_score: 0.88,
                text: "General corporate boilerplate without the target concept.".into(),
                sentence_spans: vec![(0, 57)],
                lineage_span_start: 100,
            },
            RerankCandidate {
                doc_id: 20,
                prior_score: 0.72,
                text: "Background preamble. Sublinear HNSW vector quantization lowers latency."
                    .into(),
                sentence_spans: vec![(0, 20), (21, 71)],
                lineage_span_start: 500,
            },
        ];

        let hits =
            rerank_and_highlight_shortlist(&spec, "vector quantization latency", &candidates, 0)
                .await
                .unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].doc_id, 20);
        assert!((0.0..=1.0).contains(&hits[0].calibrated_probability));
        assert!(hits[0].calibrated_probability > hits[1].calibrated_probability);
        assert!(!hits[0].snippets.is_empty());
        let top_snip = &hits[0].snippets[0];
        assert_eq!(
            top_snip.text,
            "Sublinear HNSW vector quantization lowers latency."
        );
        // Composed onto parent coordinates via lineage_span_start = 500:
        assert_eq!(top_snip.start, 521);
        assert_eq!(top_snip.end, 571);
        assert_eq!(top_snip.sentence_index, Some(1));
    }
}
