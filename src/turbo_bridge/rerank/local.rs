//! In-process `libturbo` rerank execution.

use std::sync::Arc;

use tonic::Status;

use super::snippet::{build_wire_snippet, sort_reranked_hits};
use super::{RerankCandidate, RerankSpec, RerankedHit};

pub(super) async fn rerank(
    spec: &RerankSpec,
    query_text: &str,
    shortlist: &[RerankCandidate],
    engine: &Arc<turbo_client::LocalRerankEngine>,
) -> Result<Vec<RerankedHit>, Status> {
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
