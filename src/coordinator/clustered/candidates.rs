//! Product-owned collection over one clustered provider candidate stream.

use std::collections::{BinaryHeap, HashSet};

use super::*;
use crate::clustered_turbovec::ClusteredCandidate;

pub(super) fn require_finite(candidate: &ClusteredCandidate) -> Result<(), Status> {
    if !candidate.score.is_finite() {
        return Err(Status::internal(format!(
            "clustered TurboVec label {} has non-finite score {}",
            candidate.label, candidate.score
        )));
    }
    Ok(())
}

/// The product's only heap for one exact provider top-k. Tie-complete mode
/// retains the entire final boundary group.
struct TopKCollector {
    k: usize,
    tie_complete: bool,
    heap: BinaryHeap<StreamHeapEntry>,
    tie_candidates: Vec<MergedHit>,
    labels: HashSet<u64>,
    last_floor: f32,
}

impl TopKCollector {
    /// Admit one batch; returns the raised inclusive floor, if it rose.
    fn offer(&mut self, batch: Vec<ClusteredCandidate>) -> Result<Option<f32>, Status> {
        for candidate in batch {
            require_finite(&candidate)?;
            if !self.labels.insert(candidate.label) {
                return Err(Status::failed_precondition(format!(
                    "clustered TurboVec emitted duplicate stable label {}",
                    candidate.label
                )));
            }
            let entry = StreamHeapEntry(MergedHit {
                vector_id: candidate.label,
                shard: 0,
                score: candidate.score,
            });
            if self.tie_complete {
                self.tie_candidates.push(entry.0);
            }
            if self.heap.len() < self.k {
                self.heap.push(entry);
            } else if cmp_hits(&entry.0, &self.heap.peek().expect("heap is full").0)
                == std::cmp::Ordering::Less
            {
                self.heap.pop();
                self.heap.push(entry);
            }
        }
        if self.heap.len() == self.k {
            let floor = self.heap.peek().expect("heap is full").0.score.next_down();
            if floor > self.last_floor {
                self.last_floor = floor;
                if self.tie_complete {
                    self.tie_candidates
                        .retain(|candidate| candidate.score >= floor);
                }
                return Ok(Some(floor));
            }
        }
        Ok(None)
    }

    fn finish(self) -> ClusteredVectorResult {
        let Self {
            k,
            tie_complete,
            heap,
            mut tie_candidates,
            ..
        } = self;
        let mut hits = if tie_complete {
            let boundary = heap.peek().map_or(f32::NEG_INFINITY, |entry| entry.0.score);
            tie_candidates.retain(|candidate| candidate.score >= boundary);
            tie_candidates
        } else {
            heap.into_iter().map(|entry| entry.0).collect()
        };
        hits.sort_by(cmp_hits);
        if !tie_complete {
            hits.truncate(k);
        }
        ClusteredVectorResult {
            hits: hits
                .into_iter()
                .map(|hit| (hit.vector_id, hit.score))
                .collect(),
        }
    }
}

/// One local top-k heap per product shard over a single provider stream.
struct LocalLegs {
    k: usize,
    heaps: Vec<BinaryHeap<StreamHeapEntry>>,
    active: HashSet<u32>,
    labels: HashSet<u64>,
    last_floor: f32,
}

impl LocalLegs {
    /// Admit one batch; returns the raised inclusive floor, if it rose.
    fn offer(
        &mut self,
        ranges: &[ProductLabelRange],
        batch: Vec<ClusteredCandidate>,
    ) -> Result<Option<f32>, Status> {
        let Self {
            k, heaps, active, ..
        } = self;
        let k = *k;
        for candidate in batch {
            require_finite(&candidate)?;
            if !self.labels.insert(candidate.label) {
                return Err(Status::failed_precondition(format!(
                    "clustered TurboVec emitted duplicate stable label {}",
                    candidate.label
                )));
            }
            let shard = CoordinatorServiceImpl::product_owner(ranges, candidate.label)? as usize;
            let entry = StreamHeapEntry(MergedHit {
                vector_id: candidate.label,
                shard: shard as u32,
                score: candidate.score,
            });
            if heaps[shard].len() < k {
                heaps[shard].push(entry);
            } else if cmp_hits(
                &entry.0,
                &heaps[shard].peek().expect("local heap is full").0,
            ) == std::cmp::Ordering::Less
            {
                heaps[shard].pop();
                heaps[shard].push(entry);
            }
        }
        if !active.is_empty() && active.iter().all(|&shard| heaps[shard as usize].len() == k) {
            let floor = active
                .iter()
                .map(|&shard| {
                    heaps[shard as usize]
                        .peek()
                        .expect("active local heap is full")
                        .0
                        .score
                })
                .min_by(f32::total_cmp)
                .expect("active set is non-empty")
                .next_down();
            if floor > self.last_floor {
                self.last_floor = floor;
                return Ok(Some(floor));
            }
        }
        Ok(None)
    }

    fn finish(self) -> Vec<Vec<(u64, f64)>> {
        let mut result = vec![Vec::new(); self.heaps.len()];
        for (shard, heap) in self.heaps.into_iter().enumerate() {
            let mut hits: Vec<MergedHit> = heap.into_iter().map(|entry| entry.0).collect();
            hits.sort_by(cmp_hits);
            result[shard] = hits
                .into_iter()
                .map(|hit| (hit.vector_id, f64::from(hit.score)))
                .collect();
        }
        result
    }
}

impl CoordinatorServiceImpl {
    /// Collect one exact provider top-k. The product owns the only heap and
    /// drives its inclusive live floor. Tie-complete mode retains the entire
    /// final boundary group for cascade.
    pub(in crate::coordinator) async fn clustered_vector_candidates(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        initial_floor: Option<f32>,
        tie_complete: bool,
        filters: &RequestFilters,
    ) -> Result<ClusteredVectorResult, Status> {
        if k == 0 {
            return Ok(ClusteredVectorResult { hits: Vec::new() });
        }
        let backend = self.clustered_vectors.as_ref().ok_or_else(|| {
            Status::failed_precondition("clustered TurboVec backend is not configured")
        })?;
        let allowed = self.clustered_allowed_labels(filters).await?;
        let mut stream = backend
            .candidate_stream(request_id, vector.to_vec(), allowed, initial_floor)
            .await?;
        let mut collector = TopKCollector {
            k: k as usize,
            tie_complete,
            heap: BinaryHeap::with_capacity(k as usize + 1),
            tie_candidates: Vec::new(),
            labels: HashSet::new(),
            last_floor: initial_floor.unwrap_or(f32::NEG_INFINITY),
        };
        let completion = loop {
            match stream.next_event().await? {
                ClusteredCandidateEvent::Batch(batch) => {
                    if let Some(floor) = collector.offer(batch)? {
                        stream.raise_floor(floor)?;
                    }
                }
                ClusteredCandidateEvent::Completion(completion) => break completion,
            }
        };
        if completion.emitted != collector.labels.len() as u64 {
            return Err(Status::internal(format!(
                "clustered TurboVec completion counted {} candidates but the product received {}",
                completion.emitted,
                collector.labels.len()
            )));
        }
        self.publish_progress(
            crate::pb::QueryStreamPhase::Dense,
            collector
                .heap
                .iter()
                .map(|entry| (entry.0.vector_id, entry.0.score))
                .collect(),
            completion.scoring_fingerprint.clone(),
        );
        Ok(collector.finish())
    }

    /// Exact product-shard-local vector legs over one provider stream. The
    /// safe collection floor is the minimum filled local k-th score because a
    /// lower candidate cannot enter any product shard's local list.
    pub(in crate::coordinator) async fn clustered_local_vector_legs(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        filters: &RequestFilters,
        ranges: &[ProductLabelRange],
    ) -> Result<Vec<Vec<(u64, f64)>>, Status> {
        if k == 0 {
            return Ok(vec![Vec::new(); self.node_addrs.len()]);
        }
        let backend = self
            .clustered_vectors
            .as_ref()
            .expect("clustered local legs require a configured backend");
        let allowed = self.clustered_allowed_labels(filters).await?;
        let mut stream = backend
            .candidate_stream(request_id, vector.to_vec(), allowed, None)
            .await?;
        let mut legs = LocalLegs {
            k: k as usize,
            heaps: (0..self.node_addrs.len())
                .map(|_| BinaryHeap::with_capacity(k as usize + 1))
                .collect(),
            active: ranges.iter().map(|range| range.shard).collect(),
            labels: HashSet::new(),
            last_floor: f32::NEG_INFINITY,
        };
        let completion = loop {
            match stream.next_event().await? {
                ClusteredCandidateEvent::Batch(batch) => {
                    if let Some(floor) = legs.offer(ranges, batch)? {
                        stream.raise_floor(floor)?;
                    }
                }
                ClusteredCandidateEvent::Completion(completion) => break completion,
            }
        };
        if completion.emitted != legs.labels.len() as u64 {
            return Err(Status::internal(format!(
                "clustered TurboVec completion counted {} candidates but local legs received {}",
                completion.emitted,
                legs.labels.len()
            )));
        }
        Ok(legs.finish())
    }
}
