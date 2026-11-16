//! Parent-level collapse over one clustered provider candidate stream.

use std::collections::HashSet;

use super::candidates::require_finite;
use super::*;
use crate::clustered_turbovec::ClusteredCandidate;

struct ParentAgg {
    best_score: f32,
    best_id: u64,
    chunks: Vec<(u64, f32)>,
}

/// Parent-level aggregation of one provider stream.
struct ParentCollapse {
    k: usize,
    parents: HashMap<u64, ParentAgg>,
    labels: HashSet<u64>,
    kth: f32,
    last_floor: f32,
    floors_sent: u64,
}

impl ParentCollapse {
    /// The batch's labels, each admitted exactly once across the stream.
    fn admit_labels(&mut self, batch: &[ClusteredCandidate]) -> Result<Vec<u64>, Status> {
        let batch_labels: Vec<u64> = batch.iter().map(|item| item.label).collect();
        for &label in &batch_labels {
            if !self.labels.insert(label) {
                return Err(Status::failed_precondition(format!(
                    "clustered TurboVec emitted duplicate stable label {label}"
                )));
            }
        }
        Ok(batch_labels)
    }

    /// Fold one batch under its resolved parents; returns the raised
    /// inclusive floor, if it rose.
    fn offer(
        &mut self,
        batch: Vec<ClusteredCandidate>,
        parent_ids: &HashMap<u64, u64>,
    ) -> Result<Option<f32>, Status> {
        let kth = self.kth;
        let mut dirty = false;
        for candidate in batch {
            require_finite(&candidate)?;
            let parent = parent_ids[&candidate.label];
            let agg = self.parents.entry(parent).or_insert_with(|| {
                dirty = true;
                ParentAgg {
                    best_score: f32::NEG_INFINITY,
                    best_id: u64::MAX,
                    chunks: Vec::new(),
                }
            });
            agg.chunks.push((candidate.label, candidate.score));
            if candidate.score > agg.best_score
                || (candidate.score == agg.best_score && candidate.label < agg.best_id)
            {
                if candidate.score > agg.best_score && candidate.score > kth {
                    dirty = true;
                }
                agg.best_score = candidate.score;
                agg.best_id = candidate.label;
            }
        }
        if dirty && self.parents.len() >= self.k {
            let mut bests: Vec<f32> = self
                .parents
                .values()
                .map(|parent| parent.best_score)
                .collect();
            let new_kth = *bests
                .select_nth_unstable_by(self.k - 1, |a, b| b.total_cmp(a))
                .1;
            if new_kth > self.kth {
                self.kth = new_kth;
                let floor = self.kth.next_down();
                if floor > self.last_floor {
                    self.last_floor = floor;
                    self.floors_sent += 1;
                    return Ok(Some(floor));
                }
            }
        }
        Ok(None)
    }

    fn finish(self) -> CollapseStreamResult {
        let k = self.k;
        let mut ranked: Vec<(u64, ParentAgg)> = self.parents.into_iter().collect();
        ranked.sort_by(|a, b| {
            b.1.best_score
                .total_cmp(&a.1.best_score)
                .then_with(|| a.1.best_id.cmp(&b.1.best_id))
        });
        let chunk_floor = if ranked.len() >= k {
            ranked[k - 1].1.best_score.next_down()
        } else {
            f32::NEG_INFINITY
        };
        ranked.truncate(k);
        let mut hits = Vec::with_capacity(ranked.len());
        let mut groups = Vec::with_capacity(ranked.len());
        for (parent, agg) in ranked {
            hits.push(ScoredHit {
                identity: None,
                vector_id: agg.best_id,
                score: agg.best_score,
                parent_id: parent,
                rerank_candidate: None,
            });
            let mut chunks: Vec<(u64, f32)> = agg
                .chunks
                .into_iter()
                .filter(|&(_, score)| score >= chunk_floor)
                .collect();
            chunks.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            groups.push(ParentGroup {
                parent_id: parent,
                chunks: chunks
                    .into_iter()
                    .map(|(vector_id, score)| ScoredHit {
                        identity: None,
                        vector_id,
                        score,
                        parent_id: parent,
                        rerank_candidate: None,
                    })
                    .collect(),
            });
        }
        CollapseStreamResult {
            hits,
            groups,
            chunk_floor,
            summaries: Vec::new(),
            floors_sent: self.floors_sent,
            shards_finished_early: 0,
            shards_floor_pruned: 0,
        }
    }
}

impl CoordinatorServiceImpl {
    pub(in crate::coordinator) async fn clustered_parent_collapse(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        filters: &RequestFilters,
    ) -> Result<CollapseStreamResult, Status> {
        if k == 0 {
            return Ok(CollapseStreamResult {
                hits: Vec::new(),
                groups: Vec::new(),
                chunk_floor: f32::NEG_INFINITY,
                summaries: Vec::new(),
                floors_sent: 0,
                shards_finished_early: 0,
                shards_floor_pruned: 0,
            });
        }
        let backend = self
            .clustered_vectors
            .as_ref()
            .expect("clustered parent route is selected only with a backend");
        let ranges = self.product_label_ranges().await?;
        let allowed = self.clustered_allowed_labels(filters).await?;
        let mut stream = backend
            .candidate_stream(request_id, vector.to_vec(), allowed, None)
            .await?;
        let mut collapse = ParentCollapse {
            k: k as usize,
            parents: HashMap::new(),
            labels: HashSet::new(),
            kth: f32::NEG_INFINITY,
            last_floor: f32::NEG_INFINITY,
            floors_sent: 0,
        };
        let completion = loop {
            match stream.next_event().await? {
                ClusteredCandidateEvent::Batch(batch) => {
                    let batch_labels = collapse.admit_labels(&batch)?;
                    let parent_ids = self.product_parent_ids(&ranges, &batch_labels).await?;
                    if let Some(floor) = collapse.offer(batch, &parent_ids)? {
                        stream.raise_floor(floor)?;
                    }
                }
                ClusteredCandidateEvent::Completion(completion) => break completion,
            }
        };
        if completion.emitted != collapse.labels.len() as u64 {
            return Err(Status::internal(format!(
                "clustered TurboVec completion counted {} candidates but collapse received {}",
                completion.emitted,
                collapse.labels.len()
            )));
        }
        Ok(collapse.finish())
    }
}
