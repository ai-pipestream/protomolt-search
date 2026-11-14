//! Signals.

use super::*;

impl CoordinatorServiceImpl {
    /// Candidate-scoped LEXICAL signal for the public route's boost
    /// phase: analyze `text` under `spec`, then score `ids` against
    /// the analyzed terms with GLOBAL stats through the `Bm25Rescore`
    /// seam, broadcast to every shard — the rescore contract ignores
    /// ids a shard does not own, so no id-to-shard map is needed.
    /// Returns doc -> score for exactly the candidates matching at
    /// least one term; absence means the document matched nothing.
    pub async fn lexical_signal(
        &self,
        text: &str,
        spec: Option<&crate::pb::AnalysisSpec>,
        ids: &[u64],
    ) -> Result<HashMap<u64, f32>, Status> {
        let analysis_fingerprint = crate::analyzer::analysis_fingerprint(spec);
        if text.is_empty() {
            return Err(Status::invalid_argument(
                "boost.text must be non-empty when boost is present",
            ));
        }
        let addr = self.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis backend configured on the coordinator (analysis_addr)")
        })?;
        let analyzed = crate::analyzer::analyze_document(&addr, text, spec).await?;
        let mut terms: Vec<String> = Vec::new();
        for (term, _, _) in analyzed.into_body().terms {
            if !terms.contains(&term) {
                terms.push(term);
            }
        }
        self.lexical_signal_terms(&terms, analysis_fingerprint, ids, None)
            .await
    }

    /// Candidate-scoped lexical scoring when the planner already analyzed the
    /// clause during membership resolution. `expected_epochs` closes the gap
    /// between that bitmap and this rescore: any lexical mutation aborts the
    /// plan so the caller can rebuild it once from a fresh snapshot.
    pub async fn lexical_signal_terms(
        &self,
        terms: &[String],
        analysis_fingerprint: u64,
        ids: &[u64],
        expected_epochs: Option<&[StatsClaim]>,
    ) -> Result<HashMap<u64, f32>, Status> {
        self.lexical_signal_terms_with_stages(
            terms,
            analysis_fingerprint,
            ids,
            expected_epochs,
            &[],
        )
        .await
    }

    /// [`Self::lexical_signal_terms`] with the ordinary lexical score-stage
    /// chain applied on each owning shard before the final f32 conversion.
    pub async fn lexical_signal_terms_with_stages(
        &self,
        terms: &[String],
        analysis_fingerprint: u64,
        ids: &[u64],
        expected_epochs: Option<&[StatsClaim]>,
        score_stages: &[crate::pb::ScoreStage],
    ) -> Result<HashMap<u64, f32>, Status> {
        if terms.is_empty() || ids.is_empty() {
            return Ok(HashMap::new());
        }
        let by_shard: HashMap<u32, Vec<u64>> = (0..self.node_addrs.len())
            .map(|s| (s as u32, ids.to_vec()))
            .collect();
        // Stats + rescore run as a round (a stale-stats refusal reruns
        // them once with fresh stats and a new fenced claim) — the same protocol
        // as every other stats consumer.
        let mut fresh = false;
        loop {
            let (global, epochs) = self.body_stats(terms, fresh).await?;
            if let Some(expected) = expected_epochs {
                if expected != epochs {
                    self.stats_cache.invalidate_all();
                    return Err(Status::aborted(
                        "boolean membership epoch changed before lexical scoring",
                    ));
                }
            }
            let claims = epochs;
            match self
                .fanout_bm25_rescore_scores(
                    terms,
                    analysis_fingerprint,
                    &global,
                    &claims,
                    by_shard.clone(),
                    score_stages,
                )
                .await
            {
                Err(e) if !fresh && is_stale_stats(&e) => {
                    if expected_epochs.is_some() {
                        self.stats_cache.invalidate_all();
                        return Err(Status::aborted(
                            "boolean membership epoch changed during lexical scoring",
                        ));
                    }
                    self.stats_cache.invalidate_all();
                    fresh = true;
                }
                Err(e) => return Err(e),
                Ok(scores) => return Ok(scores),
            }
        }
    }

    /// Candidate-scoped DENSE signal for the public route's boost
    /// phase (the `VectorRescore` seam), broadcast to every shard.
    /// Present exactly for the candidates that carry a vector; scores
    /// are bitwise the same calibrated products a full search emits.
    pub async fn dense_signal(
        &self,
        vector: &[f32],
        ids: &[u64],
        field: &str,
    ) -> Result<HashMap<u64, f32>, Status> {
        if let Some(fields) = &self.field_permissions {
            fields.vector(field)?;
        }
        if self.has_clustered_vectors()
            && (!field.is_empty()
                || self.document_visibility.is_some()
                || self.field_permissions.is_some())
        {
            return Err(Status::failed_precondition(
                "scoped vector scoring requires a product-node field binding",
            ));
        }
        if vector.is_empty() {
            return Err(Status::invalid_argument(
                "a dense boost needs a non-empty vector",
            ));
        }
        if ids.is_empty() && field.is_empty() && self.document_visibility.is_none() {
            return Ok(HashMap::new());
        }
        #[cfg(feature = "net")]
        if let Some(clustered) = &self.clustered_vectors {
            let k = u32::try_from(ids.len()).map_err(|_| {
                Status::resource_exhausted("dense boost candidate set does not fit u32")
            })?;
            let response = clustered
                .search(
                    vector.to_vec(),
                    k,
                    Some(ClusteredLabelFilter::Labels(ids.to_vec())),
                    None,
                    false,
                )
                .await?;
            if response.results.len() != 1 {
                return Err(Status::internal(format!(
                    "clustered TurboVec returned {} query results for one dense boost",
                    response.results.len()
                )));
            }
            return response.results[0]
                .neighbours
                .iter()
                .map(|neighbour| {
                    neighbour
                        .label
                        .map(|label| (label, neighbour.score))
                        .ok_or_else(|| {
                            Status::failed_precondition(
                                "clustered TurboVec dense boosts require stable labels",
                            )
                        })
                })
                .collect();
        }
        let by_shard: HashMap<u32, Vec<u64>> = (0..self.node_addrs.len())
            .map(|s| (s as u32, ids.to_vec()))
            .collect();
        self.fanout_vector_rescore(vector, by_shard, field).await
    }
}
