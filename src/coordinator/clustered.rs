//! Clustered.

use super::*;

#[cfg(feature = "net")]
#[derive(Clone, Copy, Debug)]
pub(super) struct ProductLabelRange {
    pub(super) start: u64,
    pub(super) end: u64,
    pub(super) shard: u32,
}

#[cfg(feature = "net")]
impl ProductLabelRange {
    pub(super) fn contains(self, label: u64) -> bool {
        label >= self.start && label < self.end
    }
}

#[cfg(feature = "net")]
pub(super) struct ClusteredVectorResult {
    pub(super) hits: Vec<(u64, f32)>,
}

impl CoordinatorServiceImpl {
    /// Resolve a product filter into one packed stable-id bitmap per product
    /// shard for a vector provider that does not own document columns. No
    /// filter remains `None` and therefore costs no shard pass. An explicitly
    /// present empty bitmap set is an intentional match-none set and stays
    /// distinguishable at the provider boundary.
    #[cfg(feature = "net")]
    pub(super) async fn clustered_allowed_labels(
        &self,
        filters: &RequestFilters,
    ) -> Result<Option<ClusteredLabelFilter>, Status> {
        if filters.geo.is_empty() && filters.tree.is_none() && self.document_visibility.is_none() {
            return Ok(None);
        }

        if let Some(fields) = &self.field_permissions {
            fields.filter(&filters.geo, filters.tree.as_ref())?;
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let mask = self
            .document_visibility
            .is_none()
            .then(|| self.shard_mask(filters.tree.as_ref()))
            .flatten();
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let request = crate::pb::FilterBitmapRequest {
                visibility: self.document_visibility.clone(),
                geo_filters: filters.geo.clone(),
                filter: Self::shard_filter_tree(filters, mask.as_ref(), shard),
            };
            let client = self.node_client(node);
            tasks.push((
                shard,
                tokio::spawn(async move {
                    client?
                        .resolve_filter_bitmap(request)
                        .await
                        .map(|response| response.into_inner())
                }),
            ));
        }

        let mut known = Self::filter_known(filters, mask.as_ref());
        let mut bitmaps = Vec::with_capacity(tasks.len());
        for (shard, task) in tasks {
            let response = task.await.map_err(|error| {
                Status::internal(format!("filter bitmap task failed: {error}"))
            })??;
            known.merge_shard(
                shard,
                &response.geo_columns_known,
                &response.filter_columns_known,
            )?;
            let response = Self::membership_from_filter(response);
            self.check_read_view(shard, &scope, &response, &mut visibility_known)?;
            if response.label_count == 0 {
                if !response.bits.is_empty() {
                    return Err(Status::internal(
                        "shard answered an empty filter bitmap with payload bytes",
                    ));
                }
                continue;
            }
            let expected_bytes =
                usize::try_from(response.label_count.div_ceil(8)).map_err(|_| {
                    Status::internal("shard filter bitmap length does not fit this process")
                })?;
            if response.bits.len() != expected_bytes {
                return Err(Status::internal(format!(
                    "shard answered {} filter bitmap bytes for {} labels; expected {expected_bytes}",
                    response.bits.len(),
                    response.label_count
                )));
            }
            response
                .base_label
                .checked_add(response.label_count)
                .ok_or_else(|| Status::internal("shard filter bitmap label range overflows u64"))?;
            bitmaps.push(turbovec_grpc::proto::LabelBitmap {
                base_label: response.base_label,
                label_count: response.label_count,
                bits: response.bits,
            });
        }
        self.check_visibility_columns(&visibility_known)?;
        known.refuse_unknown(filters)?;
        bitmaps.sort_unstable_by_key(|bitmap| bitmap.base_label);
        for pair in bitmaps.windows(2) {
            let previous_end = pair[0]
                .base_label
                .checked_add(pair[0].label_count)
                .expect("bitmap range was validated above");
            if pair[1].base_label < previous_end {
                return Err(Status::internal(
                    "product shard filter bitmap label ranges overlap",
                ));
            }
        }
        Ok(Some(ClusteredLabelFilter::Bitmaps(bitmaps)))
    }

    #[cfg(feature = "net")]
    /// Product ownership ranges, independent of vector shard cuts. Hybrid
    /// provenance, BM25 rescoring, and lineage lookup all route by this map;
    /// the clustered provider remains unaware of product shard meaning.
    pub(super) async fn product_label_ranges(&self) -> Result<Vec<ProductLabelRange>, Status> {
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            let client = self.node_client(node);
            tasks.push(tokio::spawn(async move {
                client?
                    .health(HealthRequest {})
                    .await
                    .map(|response| (shard as u32, response.into_inner()))
            }));
        }
        let mut ranges = Vec::with_capacity(tasks.len());
        for task in tasks {
            let (shard, health) = task
                .await
                .map_err(|error| Status::internal(format!("health task failed: {error}")))??;
            let count = health.num_vectors.max(health.bm25_docs);
            if count == 0 {
                continue;
            }
            let end = health.slot_offset.checked_add(count).ok_or_else(|| {
                Status::failed_precondition("product shard label range overflows u64")
            })?;
            ranges.push(ProductLabelRange {
                start: health.slot_offset,
                end,
                shard,
            });
        }
        ranges.sort_unstable_by_key(|range| range.start);
        for pair in ranges.windows(2) {
            if pair[1].start < pair[0].end {
                return Err(Status::failed_precondition(format!(
                    "product shard label ranges overlap: [{}, {}) and [{}, {})",
                    pair[0].start, pair[0].end, pair[1].start, pair[1].end
                )));
            }
        }
        Ok(ranges)
    }

    #[cfg(feature = "net")]
    pub(super) fn product_owner(ranges: &[ProductLabelRange], label: u64) -> Result<u32, Status> {
        ranges
            .iter()
            .copied()
            .find(|range| range.contains(label))
            .map(|range| range.shard)
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "clustered TurboVec label {label} has no product shard owner"
                ))
            })
    }

    #[cfg(feature = "net")]
    /// Resolve lineage in compact batches on the owning product shards. A
    /// document without stored lineage parents itself under the same tagged
    /// domain as the embedded path; raw document text never crosses this seam.
    pub(super) async fn product_parent_ids(
        &self,
        ranges: &[ProductLabelRange],
        labels: &[u64],
    ) -> Result<HashMap<u64, u64>, Status> {
        let mut by_shard: HashMap<usize, Vec<u64>> = HashMap::new();
        for &label in labels {
            by_shard
                .entry(Self::product_owner(ranges, label)? as usize)
                .or_default()
                .push(label);
        }
        let selection =
            crate::lineage::LineageSelection::new(&[crate::pb::LineageField::ParentId as i32])?;
        let parents: HashMap<u64, u64> = self
            .read_lineage(labels, &selection, Some(&by_shard))
            .await?
            .into_iter()
            .map(|(id, (parent, _))| (id, parent))
            .collect();
        for &label in labels {
            if !parents.contains_key(&label) {
                return Err(Status::failed_precondition(format!(
                    "product shard did not resolve parent identity for clustered label {label}"
                )));
            }
        }
        Ok(parents)
    }

    /// Collect one exact provider top-k. The product owns the only heap and
    /// drives its inclusive live floor. Tie-complete mode retains the entire
    /// final boundary group for cascade.
    #[cfg(feature = "net")]
    pub(super) async fn clustered_vector_candidates(
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
        let mut heap: std::collections::BinaryHeap<StreamHeapEntry> =
            std::collections::BinaryHeap::with_capacity(k as usize + 1);
        let mut tie_candidates = Vec::new();
        let mut labels = std::collections::HashSet::new();
        let mut last_floor = initial_floor.unwrap_or(f32::NEG_INFINITY);
        let completion = loop {
            match stream.next_event().await? {
                ClusteredCandidateEvent::Batch(batch) => {
                    for candidate in batch {
                        if !candidate.score.is_finite() {
                            return Err(Status::internal(format!(
                                "clustered TurboVec label {} has non-finite score {}",
                                candidate.label, candidate.score
                            )));
                        }
                        if !labels.insert(candidate.label) {
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
                        if tie_complete {
                            tie_candidates.push(entry.0);
                        }
                        if heap.len() < k as usize {
                            heap.push(entry);
                        } else if cmp_hits(&entry.0, &heap.peek().expect("heap is full").0)
                            == std::cmp::Ordering::Less
                        {
                            heap.pop();
                            heap.push(entry);
                        }
                    }
                    if heap.len() == k as usize {
                        let floor = heap.peek().expect("heap is full").0.score.next_down();
                        if floor > last_floor {
                            stream.raise_floor(floor)?;
                            last_floor = floor;
                            if tie_complete {
                                tie_candidates.retain(|candidate| candidate.score >= floor);
                            }
                        }
                    }
                }
                ClusteredCandidateEvent::Completion(completion) => break completion,
            }
        };
        if completion.emitted != labels.len() as u64 {
            return Err(Status::internal(format!(
                "clustered TurboVec completion counted {} candidates but the product received {}",
                completion.emitted,
                labels.len()
            )));
        }
        self.publish_progress(
            crate::pb::QueryStreamPhase::Dense,
            heap.iter()
                .map(|entry| (entry.0.vector_id, entry.0.score))
                .collect(),
            completion.scoring_fingerprint.clone(),
        );

        let mut hits = if tie_complete {
            let boundary = heap.peek().map_or(f32::NEG_INFINITY, |entry| entry.0.score);
            tie_candidates.retain(|candidate| candidate.score >= boundary);
            tie_candidates
        } else {
            heap.into_iter().map(|entry| entry.0).collect()
        };
        hits.sort_by(cmp_hits);
        if !tie_complete {
            hits.truncate(k as usize);
        }
        Ok(ClusteredVectorResult {
            hits: hits
                .into_iter()
                .map(|hit| (hit.vector_id, hit.score))
                .collect(),
        })
    }

    #[cfg(feature = "net")]
    pub(super) async fn clustered_parent_collapse(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        filters: &RequestFilters,
    ) -> Result<CollapseStreamResult, Status> {
        struct ParentAgg {
            best_score: f32,
            best_id: u64,
            chunks: Vec<(u64, f32)>,
        }
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
        let mut parents: HashMap<u64, ParentAgg> = HashMap::new();
        let mut labels = std::collections::HashSet::new();
        let mut kth = f32::NEG_INFINITY;
        let mut last_floor = f32::NEG_INFINITY;
        let mut floors_sent = 0;
        let completion = loop {
            match stream.next_event().await? {
                ClusteredCandidateEvent::Batch(batch) => {
                    let batch_labels: Vec<u64> = batch.iter().map(|item| item.label).collect();
                    for &label in &batch_labels {
                        if !labels.insert(label) {
                            return Err(Status::failed_precondition(format!(
                                "clustered TurboVec emitted duplicate stable label {label}"
                            )));
                        }
                    }
                    let parent_ids = self.product_parent_ids(&ranges, &batch_labels).await?;
                    let mut dirty = false;
                    for candidate in batch {
                        if !candidate.score.is_finite() {
                            return Err(Status::internal(format!(
                                "clustered TurboVec label {} has non-finite score {}",
                                candidate.label, candidate.score
                            )));
                        }
                        let parent = parent_ids[&candidate.label];
                        let agg = parents.entry(parent).or_insert_with(|| {
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
                    if dirty && parents.len() >= k as usize {
                        let mut bests: Vec<f32> =
                            parents.values().map(|parent| parent.best_score).collect();
                        let new_kth = *bests
                            .select_nth_unstable_by(k as usize - 1, |a, b| b.total_cmp(a))
                            .1;
                        if new_kth > kth {
                            kth = new_kth;
                            let floor = kth.next_down();
                            if floor > last_floor {
                                stream.raise_floor(floor)?;
                                last_floor = floor;
                                floors_sent += 1;
                            }
                        }
                    }
                }
                ClusteredCandidateEvent::Completion(completion) => break completion,
            }
        };
        if completion.emitted != labels.len() as u64 {
            return Err(Status::internal(format!(
                "clustered TurboVec completion counted {} candidates but collapse received {}",
                completion.emitted,
                labels.len()
            )));
        }

        let mut ranked: Vec<(u64, ParentAgg)> = parents.into_iter().collect();
        ranked.sort_by(|a, b| {
            b.1.best_score
                .total_cmp(&a.1.best_score)
                .then_with(|| a.1.best_id.cmp(&b.1.best_id))
        });
        let chunk_floor = if ranked.len() >= k as usize {
            ranked[k as usize - 1].1.best_score.next_down()
        } else {
            f32::NEG_INFINITY
        };
        ranked.truncate(k as usize);
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
        Ok(CollapseStreamResult {
            hits,
            groups,
            chunk_floor,
            summaries: Vec::new(),
            floors_sent,
            shards_finished_early: 0,
            shards_floor_pruned: 0,
        })
    }

    /// Exact product-shard-local vector legs over one provider stream. The
    /// safe collection floor is the minimum filled local k-th score because a
    /// lower candidate cannot enter any product shard's local list.
    #[cfg(feature = "net")]
    pub(super) async fn clustered_local_vector_legs(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        filters: &RequestFilters,
        ranges: &[ProductLabelRange],
    ) -> Result<Vec<Vec<(u64, f64)>>, Status> {
        let mut result = vec![Vec::new(); self.node_addrs.len()];
        if k == 0 {
            return Ok(result);
        }
        let backend = self
            .clustered_vectors
            .as_ref()
            .expect("clustered local legs require a configured backend");
        let allowed = self.clustered_allowed_labels(filters).await?;
        let mut stream = backend
            .candidate_stream(request_id, vector.to_vec(), allowed, None)
            .await?;
        let mut heaps: Vec<std::collections::BinaryHeap<StreamHeapEntry>> =
            (0..self.node_addrs.len())
                .map(|_| std::collections::BinaryHeap::with_capacity(k as usize + 1))
                .collect();
        let active: std::collections::HashSet<u32> =
            ranges.iter().map(|range| range.shard).collect();
        let mut labels = std::collections::HashSet::new();
        let mut last_floor = f32::NEG_INFINITY;
        let completion = loop {
            match stream.next_event().await? {
                ClusteredCandidateEvent::Batch(batch) => {
                    for candidate in batch {
                        if !candidate.score.is_finite() {
                            return Err(Status::internal(format!(
                                "clustered TurboVec label {} has non-finite score {}",
                                candidate.label, candidate.score
                            )));
                        }
                        if !labels.insert(candidate.label) {
                            return Err(Status::failed_precondition(format!(
                                "clustered TurboVec emitted duplicate stable label {}",
                                candidate.label
                            )));
                        }
                        let shard = Self::product_owner(ranges, candidate.label)? as usize;
                        let entry = StreamHeapEntry(MergedHit {
                            vector_id: candidate.label,
                            shard: shard as u32,
                            score: candidate.score,
                        });
                        if heaps[shard].len() < k as usize {
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
                    if !active.is_empty()
                        && active
                            .iter()
                            .all(|&shard| heaps[shard as usize].len() == k as usize)
                    {
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
                        if floor > last_floor {
                            stream.raise_floor(floor)?;
                            last_floor = floor;
                        }
                    }
                }
                ClusteredCandidateEvent::Completion(completion) => break completion,
            }
        };
        if completion.emitted != labels.len() as u64 {
            return Err(Status::internal(format!(
                "clustered TurboVec completion counted {} candidates but local legs received {}",
                completion.emitted,
                labels.len()
            )));
        }
        for (shard, heap) in heaps.into_iter().enumerate() {
            let mut hits: Vec<MergedHit> = heap.into_iter().map(|entry| entry.0).collect();
            hits.sort_by(cmp_hits);
            result[shard] = hits
                .into_iter()
                .map(|hit| (hit.vector_id, f64::from(hit.score)))
                .collect();
        }
        Ok(result)
    }
}
