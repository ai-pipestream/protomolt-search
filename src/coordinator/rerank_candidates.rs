use std::collections::HashMap;

use tonic::{Request, Status};

use super::CoordinatorServiceImpl;
use crate::pb::GetDocumentsRequest;
use crate::turbo_bridge::rerank::RerankCandidate;

impl CoordinatorServiceImpl {
    /// Fetch stored text, sentence spans, and lineage in shortlist order.
    pub async fn fetch_rerank_candidates(
        &self,
        hit_scores: &[(u64, f64)],
    ) -> Result<Vec<RerankCandidate>, Status> {
        self.fetch_rerank_candidates_with_deadline(hit_scores, 0)
            .await
    }

    /// Bound fallback document reads by the same absolute deadline as reranking.
    /// Dropping this future also drops the outstanding RPC; no work is spawned.
    pub async fn fetch_rerank_candidates_with_deadline(
        &self,
        hit_scores: &[(u64, f64)],
        finish_deadline_us: u64,
    ) -> Result<Vec<RerankCandidate>, Status> {
        let remaining = crate::turbo_bridge::remaining_deadline_duration(finish_deadline_us)?;
        let coordinator = self.request_snapshot().unwrap_or_else(|| self.clone());
        let fetch = coordinator.fetch_rerank_candidates_inner(hit_scores, finish_deadline_us);
        let result = match remaining {
            Some(remaining) => tokio::time::timeout(remaining, fetch).await.map_err(|_| {
                Status::deadline_exceeded("rerank candidate fetch deadline expired")
            })?,
            None => fetch.await,
        };
        // Tonic's transport timeout can finish just before the enclosing
        // timer and surface as Cancelled. Report an exhausted request budget
        // consistently, while preserving cancellation before the deadline.
        if matches!(&result, Err(status) if status.code() == tonic::Code::Cancelled)
            && crate::turbo_bridge::remaining_deadline_duration(finish_deadline_us).is_err()
        {
            return Err(Status::deadline_exceeded(
                "rerank candidate fetch deadline expired",
            ));
        }
        result
    }

    async fn fetch_rerank_candidates_inner(
        &self,
        hit_scores: &[(u64, f64)],
        finish_deadline_us: u64,
    ) -> Result<Vec<RerankCandidate>, Status> {
        if hit_scores.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<u64> = hit_scores.iter().map(|&(id, _)| id).collect();
        let mut docs_by_id = HashMap::with_capacity(ids.len());
        for addr in &self.node_addrs {
            let mut client = self.node_client(addr)?;
            let mut request = Request::new(GetDocumentsRequest {
                doc_ids: ids.clone(),
            });
            if let Some(remaining) =
                crate::turbo_bridge::remaining_deadline_duration(finish_deadline_us)?
            {
                request.set_timeout(remaining);
            }
            for doc in client.get_documents(request).await?.into_inner().documents {
                docs_by_id.insert(doc.doc_id, doc);
            }
        }
        Ok(hit_scores
            .iter()
            .map(|&(doc_id, prior_score)| {
                let doc = docs_by_id.get(&doc_id);
                RerankCandidate {
                    doc_id,
                    prior_score,
                    text: doc.map_or_else(String::new, |doc| doc.text.clone()),
                    sentence_spans: doc.map_or_else(Vec::new, |doc| {
                        doc.sentence_spans
                            .iter()
                            .map(|span| (span.start, span.end))
                            .collect()
                    }),
                    lineage_span_start: doc
                        .and_then(|doc| doc.lineage.as_ref())
                        .map_or(0, |l| l.span_start),
                }
            })
            .collect())
    }
}
