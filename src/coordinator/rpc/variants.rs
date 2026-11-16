//! Variant comparison handler: per-arm results, ranking diffs, interleaving.

use super::*;

/// Labels carry the whole result: a blank or duplicated one makes
/// the diffs unreadable, so reject rather than disambiguate.
fn check_variant_labels(variants: &[crate::pb::SearchVariant]) -> Result<(), Status> {
    let mut seen: Vec<&str> = Vec::with_capacity(variants.len());
    for (i, v) in variants.iter().enumerate() {
        if v.label.is_empty() {
            return Err(Status::invalid_argument(format!(
                "variant {i} has an empty label; every arm must be named"
            )));
        }
        if seen.contains(&v.label.as_str()) {
            return Err(Status::invalid_argument(format!(
                "duplicate variant label {:?}: labels identify arms in the diffs",
                v.label
            )));
        }
        seen.push(&v.label);
        if v.query.is_none() {
            return Err(Status::invalid_argument(format!(
                "variant {:?} has no query set (expected bm25 or hybrid)",
                v.label
            )));
        }
    }
    Ok(())
}

/// RBO's persistence is a probability; 1.0 would never terminate
/// its weighting and is as much an error as 2.0.
fn resolve_rbo_p(requested: f32) -> Result<f64, Status> {
    let rbo_p = if requested == 0.0 {
        0.9
    } else {
        f64::from(requested)
    };
    if !(rbo_p.is_finite() && rbo_p > 0.0 && rbo_p < 1.0) {
        return Err(Status::invalid_argument(format!(
            "rbo_p must be in (0, 1); got {requested}"
        )));
    }
    Ok(rbo_p)
}

/// Team-draft interleave the first two arms.
fn interleave_arms(req: &VariantSearchRequest, results: &[VariantResult], k: u32) -> Interleaving {
    let a: Vec<u64> = results[0].hits.iter().map(|h| h.doc_id).collect();
    let b: Vec<u64> = results[1].hits.iter().map(|h| h.doc_id).collect();
    // A seed derived from the query text keeps a re-run of the
    // same query byte-identical while still varying across
    // queries, so determinism does not become a first-position
    // bias for one arm.
    let seed = if req.interleave_seed == 0 {
        crate::interleave::seed_for(variant_text(&req.variants[0]))
    } else {
        req.interleave_seed
    };
    let merged = crate::interleave::team_draft(&a, &b, k as usize, seed);
    Interleaving {
        doc_ids: merged.ids,
        teams: merged
            .team
            .into_iter()
            .map(|t| match t {
                crate::interleave::Team::A => InterleaveTeam::A as i32,
                crate::interleave::Team::B => InterleaveTeam::B as i32,
            })
            .collect(),
        seed,
    }
}

impl CoordinatorServiceImpl {
    pub(in crate::coordinator) async fn rpc_variant_search(
        &self,
        request: Request<VariantSearchRequest>,
    ) -> Result<Response<VariantSearchResponse>, Status> {
        crate::metrics::timed(Route::VariantSearch, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::variant_search(
                    &snapshot,
                    crate::metrics::nested(request),
                ))
                .await;
            }
            self.compare_variants(request.into_inner())
                .await
                .map(Response::new)
        })
        .await
    }

    async fn compare_variants(
        &self,
        req: VariantSearchRequest,
    ) -> Result<VariantSearchResponse, Status> {
        if req.variants.len() < 2 {
            return Err(Status::invalid_argument(format!(
                "variant search compares configurations: at least 2 variants required, got {}",
                req.variants.len()
            )));
        }
        // 0 = unset selects max_k, like every other client-facing k.
        let k = self.resolve_k(req.k)?;
        check_variant_labels(&req.variants)?;
        let rbo_p = resolve_rbo_p(req.rbo_p)?;
        if req.interleave && req.variants.len() != 2 {
            return Err(Status::invalid_argument(format!(
                "interleaving is a two-way method (team draft); got {} variants. \
                 Compare more arms with diffs, or interleave them pairwise.",
                req.variants.len()
            )));
        }
        let request_id = if req.request_id.is_empty() {
            format!(
                "req-{}-{}",
                std::process::id(),
                REQUEST_COUNTER.fetch_add(1, AtomicOrdering::Relaxed)
            )
        } else {
            req.request_id.clone()
        };
        let results = self.run_variant_arms(&req.variants, k).await?;
        let diffs: Vec<RankingDiff> = results[1..]
            .iter()
            .map(|v| diff_against(&results[0], v, k as usize, rbo_p))
            .collect();
        let interleaving = req.interleave.then(|| interleave_arms(&req, &results, k));
        Ok(VariantSearchResponse {
            request_id,
            results,
            diffs,
            interleaving,
        })
    }

    /// Run the arms sequentially, not concurrently. Running them together
    /// would make each one's `elapsed_ms` a measure of how hard the other
    /// arms were hitting the same shards, and per-arm cost is part of
    /// what an A/B is asked to report.
    async fn run_variant_arms(
        &self,
        variants: &[crate::pb::SearchVariant],
        k: u32,
    ) -> Result<Vec<VariantResult>, Status> {
        let mut results = Vec::with_capacity(variants.len());
        for variant in variants {
            let query = variant
                .query
                .as_ref()
                .expect("query presence checked by check_variant_labels");
            let started = std::time::Instant::now();
            let hits = self.run_variant(query, k).await.map_err(|e| {
                // Name the arm: with several in flight, an unadorned
                // status leaves the caller guessing which one failed.
                Status::new(
                    e.code(),
                    format!("variant {:?}: {}", variant.label, e.message()),
                )
            })?;
            results.push(VariantResult {
                label: variant.label.clone(),
                hits,
                elapsed_ms: started.elapsed().as_secs_f32() * 1000.0,
            });
        }
        Ok(results)
    }
}
