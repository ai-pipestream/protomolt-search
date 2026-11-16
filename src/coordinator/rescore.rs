//! Vector and BM25 rescoring over selected documents.

use std::collections::HashSet;

use super::*;

impl CoordinatorServiceImpl {
    /// Candidate-scoped vector scoring fan-out: `VectorRescore` on
    /// every shard that owns candidates, merged into doc -> score.
    pub(super) async fn fanout_vector_rescore(
        &self,
        vector: &[f32],
        by_shard: HashMap<u32, Vec<u64>>,
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
        #[cfg(feature = "net")]
        if let Some(clustered) = &self.clustered_vectors {
            return clustered_vector_rescore(clustered, vector, by_shard).await;
        }
        if self.query_read_versions.is_none() {
            let (pinned, reads) = self.pin_read_versions().await?;
            let result = Box::pin(pinned.fanout_vector_rescore(vector, by_shard, field)).await;
            pinned.validate_read_versions(&reads).await?;
            return result;
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let mut by_shard = by_shard;
        if !field.is_empty() || self.document_visibility.is_some() {
            for shard in 0..self.node_addrs.len() {
                by_shard.entry(shard as u32).or_default();
            }
        }
        let tasks = self.spawn_vector_rescores(vector, by_shard, field)?;
        let mut scores = HashMap::new();
        let mut held_binding = None;
        for (shard, requested, task) in tasks {
            let response = task
                .await
                .map_err(|e| Status::internal(format!("vector rescore task failed: {e}")))??;
            Self::check_vector_binding(field, response.vector_binding.as_ref(), &mut held_binding)?;
            self.check_read_view(shard, &scope, &response, &mut visibility_known)?;
            for hit in response.hits {
                if !requested.contains(&hit.doc_id)
                    || !hit.score.is_finite()
                    || scores.insert(hit.doc_id, hit.score).is_some()
                {
                    return Err(Status::failed_precondition(
                        "vector rescore returned an unrequested, duplicate or invalid score",
                    ));
                }
            }
        }
        self.check_visibility_columns(&visibility_known)?;
        Ok(scores)
    }

    /// Candidate-scoped BM25 fan-out (the cascade phase-2 seam),
    /// reduced to doc -> score. Docs absent from the response match no
    /// query term and score exactly 0.
    pub(super) async fn fanout_bm25_rescore_scores(
        &self,
        terms: &[String],
        analysis_fingerprint: u64,
        global: &CorpusStats,
        claims: &[StatsClaim],
        by_shard: HashMap<u32, Vec<u64>>,
        score_stages: &[crate::pb::ScoreStage],
    ) -> Result<HashMap<u64, f32>, Status> {
        let (scores, _) = self
            .bm25_rescore_round(
                terms,
                analysis_fingerprint,
                global,
                claims,
                &by_shard,
                score_stages,
            )
            .await?;
        Ok(scores
            .into_iter()
            .map(|(id, score)| (id, score as f32))
            .collect())
    }

    /// Candidate-scoped lexical scoring shared by decomposed, cascade and
    /// legacy boosts. Admission precedes scoring; every result carries the
    /// authority view and the same physical claim as its global statistics.
    pub(super) async fn bm25_rescore_round(
        &self,
        terms: &[String],
        analysis_fingerprint: u64,
        global: &CorpusStats,
        claims: &[StatsClaim],
        by_shard: &HashMap<u32, Vec<u64>>,
        score_stages: &[crate::pb::ScoreStage],
    ) -> Result<(HashMap<u64, f64>, HashMap<u32, (f32, u32)>), Status> {
        if let Some(fields) = &self.field_permissions {
            fields.lexical_scores(score_stages)?;
        }
        if claims.len() != self.node_addrs.len() {
            return Err(Status::failed_precondition(
                "BM25 rescore needs the complete statistics read set",
            ));
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let mut by_shard = by_shard.clone();
        if self.document_visibility.is_some() || !score_stages.is_empty() {
            // Empty owners still acknowledge authority and stage columns.
            for shard in 0..self.node_addrs.len() {
                by_shard.entry(shard as u32).or_default();
            }
        }
        let tasks = self.spawn_bm25_rescores(
            terms,
            analysis_fingerprint,
            global,
            claims,
            by_shard,
            score_stages,
        )?;
        let mut scores = HashMap::new();
        let mut debug = HashMap::new();
        let mut stage_known = vec![false; score_stages.len()];
        for (shard, requested, task) in tasks {
            let (rpc_ms, response) = task.await.map_err(|error| {
                Status::internal(format!("BM25 rescore task failed: {error}"))
            })??;
            let claim =
                self.check_read_view(shard as usize, &scope, &response, &mut visibility_known)?;
            if claim != claims[shard as usize] {
                return Err(Status::failed_precondition(
                    "BM25 rescore changed its statistics read version",
                ));
            }
            if response.stage_columns_known.len() != stage_known.len() {
                return Err(Status::failed_precondition(
                    "BM25 rescore stage-column handshake has the wrong length",
                ));
            }
            for (held, present) in stage_known.iter_mut().zip(&response.stage_columns_known) {
                *held |= present;
            }
            debug.insert(shard, (rpc_ms, response.hits.len() as u32));
            merge_bm25_rescore_hits(&mut scores, &requested, response.hits)?;
        }
        self.check_visibility_columns(&visibility_known)?;
        refuse_unknown_stage_columns(score_stages, stage_known)?;
        Ok((scores, debug))
    }

    fn spawn_vector_rescores(
        &self,
        vector: &[f32],
        by_shard: HashMap<u32, Vec<u64>>,
        field: &str,
    ) -> Result<Vec<(usize, HashSet<u64>, VectorRescoreTask)>, Status> {
        let mut tasks = Vec::with_capacity(by_shard.len());
        for (shard, ids) in by_shard {
            let claim = self.query_read_versions.as_ref().expect("pinned reads")[shard as usize];
            let requested: HashSet<u64> = ids.iter().copied().collect();
            let request = VectorRescoreRequest {
                field: field.into(),
                visibility: self.document_visibility.clone(),
                expected_stats_epoch: claim.epoch,
                expected_stats_incarnation: claim.incarnation(),
                vector: vector.to_vec(),
                candidate_ids: ids,
            };
            let mut client = self.node_client(&self.node_addrs[shard as usize])?;
            tasks.push((
                shard as usize,
                requested,
                tokio::spawn(async move {
                    client.vector_rescore(request).await.map(|r| r.into_inner())
                }),
            ));
        }
        Ok(tasks)
    }

    fn spawn_bm25_rescores(
        &self,
        terms: &[String],
        analysis_fingerprint: u64,
        global: &CorpusStats,
        claims: &[StatsClaim],
        by_shard: HashMap<u32, Vec<u64>>,
        score_stages: &[crate::pb::ScoreStage],
    ) -> Result<Vec<(u32, HashSet<u64>, Bm25RescoreTask)>, Status> {
        let mut tasks = Vec::with_capacity(by_shard.len());
        for (shard, ids) in by_shard {
            let node = self.node_addrs.get(shard as usize).ok_or_else(|| {
                Status::failed_precondition("BM25 rescore candidate owner is outside the read set")
            })?;
            let requested: HashSet<u64> = ids.iter().copied().collect();
            let request = Bm25RescoreRequest {
                analysis_fingerprint,
                terms: terms.to_vec(),
                global_doc_count: global.doc_count,
                global_total_doc_length: global.total_doc_length,
                global_doc_frequencies: global.dfs.clone(),
                candidate_ids: ids,
                k1: self.bm25_params.k1 as f32,
                b: self.bm25_params.b as f32,
                expected_stats_epoch: claims[shard as usize].epoch,
                expected_stats_incarnation: claims[shard as usize].incarnation(),
                score_stages: score_stages.to_vec(),
                visibility: self.document_visibility.clone(),
            };
            let mut client = self.node_client(node)?;
            tasks.push((
                shard,
                requested,
                tokio::spawn(async move {
                    let started = std::time::Instant::now();
                    client.bm25_rescore(request).await.map(|response| {
                        (started.elapsed().as_secs_f32() * 1e3, response.into_inner())
                    })
                }),
            ));
        }
        Ok(tasks)
    }
}

type VectorRescoreTask = tokio::task::JoinHandle<Result<crate::pb::VectorRescoreResponse, Status>>;
type Bm25RescoreTask =
    tokio::task::JoinHandle<Result<(f32, crate::pb::Bm25RescoreResponse), Status>>;

/// Score the candidates on the clustered provider, which owns every vector.
#[cfg(feature = "net")]
async fn clustered_vector_rescore(
    clustered: &ClusteredTurboVecBackend,
    vector: &[f32],
    by_shard: HashMap<u32, Vec<u64>>,
) -> Result<HashMap<u64, f32>, Status> {
    let mut ids: Vec<u64> = by_shard.into_values().flatten().collect();
    ids.sort_unstable();
    ids.dedup();
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let response = clustered
        .search(
            vector.to_vec(),
            u32::try_from(ids.len()).map_err(|_| {
                Status::resource_exhausted("clustered vector rescore candidate count exceeds u32")
            })?,
            Some(ClusteredLabelFilter::Labels(ids)),
            None,
            false,
        )
        .await?;
    if response.results.len() != 1 {
        return Err(Status::internal(format!(
            "clustered TurboVec returned {} rescore results for one query",
            response.results.len()
        )));
    }
    response.results[0]
        .neighbours
        .iter()
        .map(|neighbour| {
            neighbour
                .label
                .map(|label| (label, neighbour.score))
                .ok_or_else(|| {
                    Status::failed_precondition(
                        "clustered TurboVec rescore returned an unlabelled row",
                    )
                })
        })
        .collect()
}

fn merge_bm25_rescore_hits(
    scores: &mut HashMap<u64, f64>,
    requested: &HashSet<u64>,
    hits: Vec<crate::pb::Bm25Hit>,
) -> Result<(), Status> {
    for hit in hits {
        if !requested.contains(&hit.doc_id)
            || !hit.score.is_finite()
            || scores.insert(hit.doc_id, f64::from(hit.score)).is_some()
        {
            return Err(Status::failed_precondition(
                "BM25 rescore returned an unrequested, duplicate or nonfinite score",
            ));
        }
    }
    Ok(())
}

fn refuse_unknown_stage_columns(
    score_stages: &[crate::pb::ScoreStage],
    stage_known: Vec<bool>,
) -> Result<(), Status> {
    for (stage, known) in score_stages.iter().zip(stage_known) {
        if !known {
            return Err(Status::invalid_argument(format!(
                "no shard has numeric column {}: the score stage would be a silent no-op",
                stage.column,
            )));
        }
    }
    Ok(())
}
