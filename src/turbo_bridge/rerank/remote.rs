//! Remote `turbo-kserve` `RerankAndHighlight` execution.

use std::time::Duration;

use tonic::Status;
use turbo_client::proto::result::{
    rerank_calibration_spec_proto::Kind as CalKind, OffsetMode, RerankAndHighlightRequest,
    RerankAndHighlightResponse, RerankCalibrationSpecProto, RerankCandidateInputProto,
    RerankHighlightMode,
};

use super::snippet::{build_wire_snippet, sort_reranked_hits};
use super::{RerankCandidate, RerankSpec, RerankedHit};

/// The remote backend's client, bundle names, and calibration.
pub(super) struct RemoteRerank<'a> {
    pub(super) client: &'a turbo_client::RemoteClient,
    pub(super) rerank_model: &'a str,
    pub(super) embed_model: &'a str,
    pub(super) qa_model: &'a str,
    pub(super) temperature: f64,
}

pub(super) async fn rerank(
    spec: &RerankSpec,
    query_text: &str,
    shortlist: &[RerankCandidate],
    call: &RemoteRerank<'_>,
    timeout: Option<Duration>,
) -> Result<Vec<RerankedHit>, Status> {
    let req = build_request(spec, query_text, shortlist, call);
    let resp = call
        .client
        .rerank_and_highlight(req, timeout)
        .await
        .map_err(|e| crate::analyzer::remote_client_err_to_status(call.client, e))?;
    hits_from_response(spec, shortlist, resp)
}

fn build_request(
    spec: &RerankSpec,
    query_text: &str,
    shortlist: &[RerankCandidate],
    call: &RemoteRerank<'_>,
) -> RerankAndHighlightRequest {
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

    RerankAndHighlightRequest {
        query: query_text.to_string(),
        rerank_model: call.rerank_model.to_string(),
        embed_model: call.embed_model.to_string(),
        qa_model: call.qa_model.to_string(),
        candidates: wire_candidates,
        offsets: OffsetMode::Utf8 as i32,
        calibration: Some(RerankCalibrationSpecProto {
            kind: Some(CalKind::Temperature(call.temperature.max(1e-6))),
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
    }
}

fn hits_from_response(
    spec: &RerankSpec,
    shortlist: &[RerankCandidate],
    resp: RerankAndHighlightResponse,
) -> Result<Vec<RerankedHit>, Status> {
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
