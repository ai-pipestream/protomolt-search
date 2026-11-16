//! Certified close-out: exact rescoring, ranking and candidate resolution.

use super::*;

impl DecomposedRun<'_> {
    pub(super) fn exact_and_unknown(&self) -> (Vec<ExactCandidate>, Vec<VectorCandidate>) {
        let mut exact = Vec::new();
        let mut unknown = Vec::new();
        for (&doc, &(vector, shard)) in &self.vectors {
            if self.bounds.minimum_vector.is_some_and(|m| vector < m) {
                continue;
            }
            match self.bm25.get(&doc) {
                Some(&(bm25, _)) => exact.push((doc, shard, vector, bm25)),
                None if !self.filled => exact.push((doc, shard, vector, 0.0)),
                None => unknown.push((doc, shard, vector)),
            }
        }
        (exact, unknown)
    }

    pub(super) fn rescore_candidates(
        &self,
        exact: &[ExactCandidate],
        unknown: Vec<VectorCandidate>,
    ) -> (HashMap<u32, Vec<u64>>, Vec<VectorCandidate>) {
        let mut fused: Vec<f64> = exact
            .iter()
            .map(|&(_, _, vector, bm25)| self.bounds.fused(vector, bm25))
            .collect();
        let known = if fused.len() >= self.bounds.k {
            *fused
                .select_nth_unstable_by(self.bounds.k - 1, |a, b| b.total_cmp(a))
                .1
        } else {
            f64::NEG_INFINITY
        };
        let mut ids: HashMap<u32, Vec<u64>> = HashMap::new();
        let mut docs = Vec::new();
        for (doc, shard, vector) in unknown {
            let upper = self.bounds.vector_weight * f64::from(vector)
                + self.bounds.bm25_weight * self.outside_bm25;
            let magnitude = upper.abs()
                + (self.bounds.vector_weight * f64::from(vector)).abs()
                + self.bounds.bm25_weight * self.outside_bm25;
            let safe_upper = upper + magnitude * SLACK_REL + dot_underflow_slack(1);
            if safe_upper >= known {
                ids.entry(shard).or_default().push(doc);
                docs.push((doc, shard, vector));
            }
        }
        (ids, docs)
    }

    pub(super) async fn close_out(
        &mut self,
        coordinator: &CoordinatorServiceImpl,
    ) -> Result<Vec<HybridHit>, Status> {
        let (mut exact, unknown) = self.exact_and_unknown();
        let (ids, docs) = self.rescore_candidates(&exact, unknown);
        let rescored = coordinator
            .fanout_bm25_rescore_scores(
                self.round.terms(),
                self.round.analysis_fingerprint(),
                self.round.global(),
                self.round.claims(),
                ids,
                &[],
            )
            .await?;
        for (doc, shard, vector) in docs {
            exact.push((
                doc,
                shard,
                vector,
                rescored.get(&doc).copied().unwrap_or(0.0),
            ));
        }
        let mut ranked: Vec<RankedCandidate> = exact
            .into_iter()
            .map(|(doc, shard, vector, bm25)| {
                (doc, shard, vector, bm25, self.bounds.fused(vector, bm25))
            })
            .collect();
        ranked.sort_by(|a, b| {
            b.4.total_cmp(&a.4)
                .then_with(|| a.1.cmp(&b.1))
                .then_with(|| a.0.cmp(&b.0))
        });
        ranked.truncate(self.bounds.k);
        self.resolve_candidates(&ranked).await?;
        Ok(ranked
            .into_iter()
            .map(|(doc, shard, vector, bm25, fused)| HybridHit {
                doc_id: doc,
                fused_score: fused as f32,
                shard,
                vector_rank: None,
                vector_score: vector,
                bm25_rank: self.bm25_ranks.get(&doc).copied(),
                bm25_score: bm25,
                boost_score: 0.0,
                vector_normalized: None,
                bm25_normalized: None,
                rerank_candidate: self.candidates.remove(&doc),
            })
            .collect())
    }

    pub(super) async fn resolve_candidates(
        &mut self,
        ranked: &[RankedCandidate],
    ) -> Result<(), Status> {
        if let Some(mut session) = self.identity.take() {
            let hits: Vec<MergedHit> = ranked
                .iter()
                .filter(|(doc, _, _, _, _)| !self.candidates.contains_key(doc))
                .map(|&(doc, shard, vector, _, _)| MergedHit {
                    vector_id: doc,
                    shard,
                    score: vector,
                })
                .collect();
            let (_, candidates) = session
                .fanout
                .resolve_identities_and_candidates(
                    &hits,
                    &self.summaries,
                    &mut session.terminal,
                    &session.limits,
                )
                .await?;
            for ((_, doc), candidate) in candidates {
                if let Some(candidate) = candidate {
                    self.candidates.entry(doc).or_insert(candidate);
                }
            }
        }
        Ok(())
    }

    pub(super) fn debug(
        &self,
        legs_ms: f32,
        fusion_started: std::time::Instant,
    ) -> Option<HybridDebug> {
        self.round.query().debug().then(|| {
            let shards = self
                .summaries
                .iter()
                .enumerate()
                .map(|(shard, summary)| HybridShardDebug {
                    shard: shard as u32,
                    rpc_ms: 0.0,
                    vector_hits: summary.as_ref().map_or(self.clustered_counts[shard], |s| {
                        u32::try_from(s.emitted).unwrap_or(u32::MAX)
                    }),
                    bm25_hits: self
                        .lexical_counts
                        .get(&(shard as u32))
                        .copied()
                        .unwrap_or(0),
                    scan: None,
                })
                .collect();
            HybridDebug {
                fusion_mode: FusionMode::Decomposed as i32,
                leg_k: self.round.query().legs().leg_k,
                terms: Vec::new(),
                analysis_ms: 0.0,
                stats_ms: 0.0,
                legs_ms,
                fusion_ms: fusion_started.elapsed().as_secs_f32() * 1e3,
                total_ms: 0.0,
                shards,
                boost_ms: 0.0,
                boost_terms: Vec::new(),
            }
        })
    }
}
