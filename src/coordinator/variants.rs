//! Query variant execution and ranking comparisons.

use super::*;

/// Diff one arm against the reference arm.
///
/// Split out from the handler so the measure wiring is testable without a
/// cluster: everything below the fan-out is a pure function of two
/// rankings.
pub(super) fn diff_against(
    reference: &VariantResult,
    variant: &VariantResult,
    k: usize,
    rbo_p: f64,
) -> RankingDiff {
    let ref_ids: Vec<u64> = reference.hits.iter().map(|h| h.doc_id).collect();
    let var_ids: Vec<u64> = variant.hits.iter().map(|h| h.doc_id).collect();
    let depth = k.min(ref_ids.len()).min(var_ids.len());
    let overlap_fraction = rankdiff::overlap_at_k(&ref_ids, &var_ids, k);
    // The reference's own scores are the yardstick for both sides, so
    // regret never compares a BM25 score with a fused one.
    let scored: Vec<(u64, f32)> = reference.hits.iter().map(|h| (h.doc_id, h.score)).collect();
    let regret = rankdiff::score_regret(&scored, &var_ids, k);
    RankingDiff {
        reference: reference.label.clone(),
        variant: variant.label.clone(),
        depth: depth as u32,
        // Recovered from the fraction rather than recounted, so the two
        // can never disagree.
        overlap: (overlap_fraction * depth as f64).round() as u32,
        overlap_fraction: overlap_fraction as f32,
        kendall_tau: rankdiff::kendall_tau(&ref_ids, &var_ids) as f32,
        rbo: rankdiff::rbo(&ref_ids, &var_ids, rbo_p) as f32,
        score_regret: regret.mean as f32,
        regret_counted: regret.counted as u32,
        regret_unscored: regret.unscored as u32,
        top1_flipped: rankdiff::top1_flipped(&ref_ids, &var_ids),
    }
}

/// The query text of an arm, whatever kind it is — the stable thing to
/// seed an interleaving from.
pub(super) fn variant_text(variant: &crate::pb::SearchVariant) -> &str {
    match &variant.query {
        Some(search_variant::Query::Bm25(r)) => &r.text,
        Some(search_variant::Query::Hybrid(r)) => &r.text,
        None => "",
    }
}

impl CoordinatorServiceImpl {
    pub async fn fanout_vector_backend(
        &self,
        req: &BroadcastVectorBackendRequest,
    ) -> Vec<VectorBackendApplyResult> {
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for node in &self.node_addrs {
            let node = node.clone();
            let request = ConfigureVectorBackendRequest {
                dim: req.dim,
                config: req.config.clone(),
            };
            let client = self.node_client(&node);
            tasks.push(tokio::spawn(async move {
                let result = match client {
                    Ok(mut client) => client.configure_vector_backend(request).await,
                    Err(e) => Err(e),
                };
                match result {
                    Ok(resp) => VectorBackendApplyResult {
                        node: node.clone(),
                        ok: true,
                        already_configured: resp.into_inner().already_configured,
                        error: String::new(),
                    },
                    Err(e) => VectorBackendApplyResult {
                        node: node.clone(),
                        ok: false,
                        already_configured: false,
                        error: e.message().to_string(),
                    },
                }
            }));
        }
        let mut results = Vec::with_capacity(tasks.len());
        for task in tasks {
            match task.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(VectorBackendApplyResult {
                    node: String::new(),
                    ok: false,
                    already_configured: false,
                    error: format!("task failed: {e}"),
                }),
            }
        }
        results
    }

    /// Run one A/B arm through the ordinary handler for its kind.
    ///
    /// This dispatches to `bm25_search`/`hybrid_search` rather than
    /// reimplementing them: the comparison is only trustworthy if each
    /// arm executes the path it would execute in production, and a
    /// parallel scoring path written for the A/B would drift from the
    /// served one until the diff was measuring the drift.
    pub(super) async fn run_variant(
        &self,
        query: &search_variant::Query,
        k: u32,
    ) -> Result<Vec<RankedHit>, Status> {
        match query {
            search_variant::Query::Bm25(req) => {
                // `k` is the request's shared depth; an arm's own k is
                // ignored so the rankings stay comparable.
                let mut req = req.clone();
                req.k = k;
                let resp =
                    SearchService::bm25_search(self, crate::metrics::nested(Request::new(req)))
                        .await?
                        .into_inner();
                Ok(resp
                    .hits
                    .into_iter()
                    .map(|h| RankedHit {
                        doc_id: h.doc_id,
                        score: h.score,
                    })
                    .collect())
            }
            search_variant::Query::Hybrid(req) => {
                let mut req = req.clone();
                req.k = k;
                // The profile block is per-arm noise here and the caller
                // asked for a comparison, not a trace.
                req.debug = false;
                let resp =
                    SearchService::hybrid_search(self, crate::metrics::nested(Request::new(req)))
                        .await?
                        .into_inner();
                // CASCADE reports in `cascade_hits` and leaves `hits`
                // empty; the other modes do the reverse. Both are a
                // ranking, which is all a diff needs.
                if resp.hits.is_empty() {
                    Ok(resp
                        .cascade_hits
                        .into_iter()
                        .map(|h| RankedHit {
                            doc_id: h.doc_id,
                            score: h.bm25_score,
                        })
                        .collect())
                } else {
                    Ok(resp
                        .hits
                        .into_iter()
                        .map(|h| RankedHit {
                            doc_id: h.doc_id,
                            score: h.fused_score,
                        })
                        .collect())
                }
            }
        }
    }
}
