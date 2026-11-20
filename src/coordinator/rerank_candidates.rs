use std::collections::{BTreeMap, HashMap, HashSet};

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
    /// Dropping this future aborts every in-flight shard RPC via its `JoinSet`.
    pub async fn fetch_rerank_candidates_with_deadline(
        &self,
        hit_scores: &[(u64, f64)],
        finish_deadline_us: u64,
    ) -> Result<Vec<RerankCandidate>, Status> {
        let empty_owners = HashMap::new();
        self.fetch_rerank_candidates_routed(hit_scores, &empty_owners, finish_deadline_us)
            .await
    }

    /// Fetch shortlist candidates bounded by `finish_deadline_us`, routing each
    /// candidate to its known owning shard in `shard_owners` and broadcasting
    /// only candidates whose shard owner is unknown.
    pub async fn fetch_rerank_candidates_routed(
        &self,
        hit_scores: &[(u64, f64)],
        shard_owners: &HashMap<u64, usize>,
        finish_deadline_us: u64,
    ) -> Result<Vec<RerankCandidate>, Status> {
        let remaining = crate::turbo_bridge::remaining_deadline_duration(finish_deadline_us)?;
        let coordinator = self.request_snapshot().unwrap_or_else(|| self.clone());
        let fetch =
            coordinator.fetch_rerank_candidates_inner(hit_scores, shard_owners, finish_deadline_us);
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
        shard_owners: &HashMap<u64, usize>,
        finish_deadline_us: u64,
    ) -> Result<Vec<RerankCandidate>, Status> {
        if hit_scores.is_empty() {
            return Ok(Vec::new());
        }
        let per_shard = rerank_fetch_plan(hit_scores, shard_owners, self.node_addrs.len());

        let mut tasks = tokio::task::JoinSet::new();
        for (shard, doc_ids) in per_shard.into_iter().enumerate() {
            if doc_ids.is_empty() {
                continue;
            }
            let mut client = self.node_client(&self.node_addrs[shard])?;
            let mut request = Request::new(GetDocumentsRequest { doc_ids });
            if let Some(remaining) =
                crate::turbo_bridge::remaining_deadline_duration(finish_deadline_us)?
            {
                request.set_timeout(remaining);
            }
            tasks.spawn(async move {
                (
                    shard,
                    client
                        .get_documents(request)
                        .await
                        .map(|resp| resp.into_inner().documents),
                )
            });
        }

        let mut responses = BTreeMap::new();
        while let Some(joined) = tasks.join_next().await {
            let (shard, response) = joined.map_err(|err| {
                Status::internal(format!("rerank candidate fetch task failed: {err}"))
            })?;
            responses.insert(shard, response?);
        }

        Ok(assemble_rerank_candidates(hit_scores, responses))
    }
}

/// The ids each shard is asked for: an id goes to its known owner, or to
/// every shard when its owner is unknown; each shard sees an id once.
fn rerank_fetch_plan(
    hit_scores: &[(u64, f64)],
    shard_owners: &HashMap<u64, usize>,
    shard_count: usize,
) -> Vec<Vec<u64>> {
    let mut per_shard: Vec<Vec<u64>> = vec![Vec::new(); shard_count];
    let mut seen_per_shard: Vec<HashSet<u64>> = vec![HashSet::new(); shard_count];
    for &(doc_id, _) in hit_scores {
        if let Some(&owner) = shard_owners
            .get(&doc_id)
            .filter(|&&shard| shard < shard_count)
        {
            if seen_per_shard[owner].insert(doc_id) {
                per_shard[owner].push(doc_id);
            }
        } else {
            for shard in 0..shard_count {
                if seen_per_shard[shard].insert(doc_id) {
                    per_shard[shard].push(doc_id);
                }
            }
        }
    }
    per_shard
}

fn assemble_rerank_candidates(
    hit_scores: &[(u64, f64)],
    responses: BTreeMap<usize, Vec<crate::pb::StoredDocument>>,
) -> Vec<RerankCandidate> {
    let mut docs_by_id = HashMap::with_capacity(hit_scores.len());
    for (_shard, documents) in responses {
        for doc in documents {
            docs_by_id.insert(doc.doc_id, doc);
        }
    }
    hit_scores
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
        .collect()
}
