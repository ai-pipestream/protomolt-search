//! Boost.

use super::*;

impl CoordinatorServiceImpl {
    /// Second-pass lexical boost (see the proto's `BoostRescore`): score
    /// the top-`window` hits against the boost query's terms through the
    /// candidate-scoped `Bm25Rescore` seam and reorder the window by
    /// `base_weight * base + boost_weight * boost`; hits beyond the
    /// window keep their relative order after it. Exactly one of
    /// `hits` / `cascade_hits` is non-empty; `base` is the fused score
    /// for the former, the phase-2 BM25 score for the latter (whose
    /// ranks are reassigned to the final order).
    pub async fn apply_boost(
        &self,
        boost: &crate::pb::BoostRescore,
        spec: Option<&crate::pb::AnalysisSpec>,
        hits: &mut [HybridHit],
        cascade_hits: &mut [CascadeHit],
        debug: &mut Option<HybridDebug>,
    ) -> Result<(), Status> {
        let analysis_fingerprint = crate::analyzer::analysis_fingerprint(spec);
        if boost.text.is_empty() {
            return Err(Status::invalid_argument(
                "boost.text must be non-empty when boost is present",
            ));
        }
        let t0 = std::time::Instant::now();
        let base_w = if boost.base_weight == 0.0 {
            1.0
        } else {
            f64::from(boost.base_weight)
        };
        let boost_w = if boost.boost_weight == 0.0 {
            1.0
        } else {
            f64::from(boost.boost_weight)
        };
        let len = hits.len().max(cascade_hits.len());
        let window = if boost.window == 0 {
            len
        } else {
            (boost.window as usize).min(len)
        };

        // Analyze the boost text with the SAME options as the main query
        // (term identity must match the index, as everywhere).
        let addr = self.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis backend configured on the coordinator (analysis_addr)")
        })?;
        let analyzed = crate::analyzer::analyze_document(&addr, &boost.text, spec).await?;
        let mut terms: Vec<String> = Vec::new();
        for (term, _, _) in analyzed.into_body().terms {
            if !terms.contains(&term) {
                terms.push(term);
            }
        }

        // Candidate-scoped scoring of the window, routed by owning
        // shard. Stats + rescore run as a round (a stale-stats refusal
        // reruns them once with fresh stats and a new fenced claim).
        let mut scores: HashMap<u64, f64> = HashMap::new();
        if window > 0 && !terms.is_empty() {
            let mut by_shard: HashMap<u32, Vec<u64>> = HashMap::new();
            for (doc_id, shard) in hits[..window.min(hits.len())]
                .iter()
                .map(|h| (h.doc_id, h.shard))
                .chain(
                    cascade_hits[..window.min(cascade_hits.len())]
                        .iter()
                        .map(|h| (h.doc_id, h.shard)),
                )
            {
                by_shard.entry(shard).or_default().push(doc_id);
            }
            let mut fresh = false;
            let rescored = loop {
                let (global, epochs) = self.body_stats(&terms, fresh).await?;
                let claims = epochs;
                match self
                    .fanout_bm25_rescore_scores(
                        &terms,
                        analysis_fingerprint,
                        &global,
                        &claims,
                        by_shard.clone(),
                        &[],
                    )
                    .await
                {
                    Err(e) if !fresh && is_stale_stats(&e) => {
                        self.stats_cache.invalidate_all();
                        fresh = true;
                    }
                    Err(e) => return Err(e),
                    Ok(rescored) => break rescored,
                }
            };
            for (doc_id, score) in rescored {
                scores.insert(doc_id, f64::from(score));
            }
        }

        if !hits.is_empty() {
            let window = window.min(hits.len());
            for h in &mut hits[..window] {
                h.boost_score = scores.get(&h.doc_id).copied().unwrap_or(0.0) as f32;
            }
            hits[..window].sort_by(|a, b| {
                let fa = base_w * f64::from(a.fused_score) + boost_w * f64::from(a.boost_score);
                let fb = base_w * f64::from(b.fused_score) + boost_w * f64::from(b.boost_score);
                fb.total_cmp(&fa)
                    .then_with(|| a.shard.cmp(&b.shard))
                    .then_with(|| a.doc_id.cmp(&b.doc_id))
            });
        } else if !cascade_hits.is_empty() {
            let window = window.min(cascade_hits.len());
            for h in &mut cascade_hits[..window] {
                h.boost_score = scores.get(&h.doc_id).copied().unwrap_or(0.0) as f32;
            }
            cascade_hits[..window].sort_by(|a, b| {
                let fa = base_w * f64::from(a.bm25_score) + boost_w * f64::from(a.boost_score);
                let fb = base_w * f64::from(b.bm25_score) + boost_w * f64::from(b.boost_score);
                fb.total_cmp(&fa)
                    .then_with(|| b.vector_score.total_cmp(&a.vector_score))
                    .then_with(|| a.doc_id.cmp(&b.doc_id))
            });
            for (i, hit) in cascade_hits.iter_mut().enumerate() {
                hit.rank = i as u32 + 1;
            }
        }

        if let Some(d) = debug.as_mut() {
            d.boost_terms = terms;
            d.boost_ms = t0.elapsed().as_secs_f32() * 1e3;
            d.total_ms += d.boost_ms;
        }
        Ok(())
    }
}
