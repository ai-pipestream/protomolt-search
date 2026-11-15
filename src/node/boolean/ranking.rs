//! Ranked boolean scoring across lexical and dense vector leaves.

use crate::node::*;

/// One ranked member: better is a higher score, then a lower slot, the
/// order the coordinator sorts the merged answer in.
#[derive(Clone, Copy, PartialEq)]
struct RankedSlot {
    pub(in crate::node) score: f32,
    pub(in crate::node) slot: u32,
}

impl Eq for RankedSlot {}

impl PartialOrd for RankedSlot {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RankedSlot {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| other.slot.cmp(&self.slot))
    }
}

/// The best `depth` of a stream of ranked slots.
struct TopSlots {
    pub(in crate::node) depth: usize,
    pub(in crate::node) heap: std::collections::BinaryHeap<std::cmp::Reverse<RankedSlot>>,
}

impl TopSlots {
    pub(in crate::node) fn new(depth: usize) -> Self {
        Self {
            depth,
            heap: std::collections::BinaryHeap::with_capacity(depth + 1),
        }
    }

    /// Offer one slot; returns the current cutoff once the heap is full.
    pub(in crate::node) fn offer(&mut self, entry: RankedSlot) -> Option<f32> {
        if self.heap.len() < self.depth {
            self.heap.push(std::cmp::Reverse(entry));
        } else if self.heap.peek().is_some_and(|worst| entry > worst.0) {
            self.heap.pop();
            self.heap.push(std::cmp::Reverse(entry));
        }
        (self.heap.len() == self.depth)
            .then(|| self.heap.peek().map_or(f32::NEG_INFINITY, |w| w.0.score))
    }

    pub(in crate::node) fn into_sorted(self) -> Vec<RankedSlot> {
        let mut out: Vec<RankedSlot> = self.heap.into_iter().map(|r| r.0).collect();
        out.sort_by(|a, b| b.cmp(a));
        out
    }
}

/// An enumerate threshold names a filter regime, so only a provider that
/// has one may be handed it: anything else would serve the request under a
/// rule its policy point was not measured with.
pub(in crate::node) fn check_enumerate_threshold(
    index: &VectorIndex,
    rows: Option<u32>,
) -> Result<(), Status> {
    let Some(rows) = rows else {
        return Ok(());
    };
    let descriptor = index.descriptor();
    if descriptor
        .capabilities
        .contains(&crate::vector::VectorCapability::MaskEnumeration)
    {
        return Ok(());
    }
    Err(Status::failed_precondition(format!(
        "vector backend {:?} has no enumerate regime for the request's threshold of {rows} \
         rows (it does not advertise mask_enumeration)",
        descriptor.backend_kind
    )))
}

pub(in crate::node) fn check_patience_options(
    index: &VectorIndex,
    saturation_ppm: Option<u32>,
    hops: Option<u32>,
) -> Result<Option<crate::vector::PatienceSpec>, Status> {
    let (ppm, hops) = match (saturation_ppm, hops) {
        (None, None) => return Ok(None),
        (Some(ppm), Some(hops)) => (ppm, hops),
        _ => {
            return Err(Status::invalid_argument(
                "patience_saturation_ppm and patience_hops must be set together",
            ));
        }
    };
    let descriptor = index.descriptor();
    if !descriptor
        .capabilities
        .contains(&crate::vector::VectorCapability::PatienceEarlyStop)
    {
        return Err(Status::failed_precondition(format!(
            "vector backend {:?} does not support patience early stopping (it does not advertise \
             patience_early_stop)",
            descriptor.backend_kind
        )));
    }
    if !(1..=1_000_000).contains(&ppm) {
        return Err(Status::invalid_argument(format!(
            "patience_saturation_ppm {ppm} must be in 1..=1000000"
        )));
    }
    if hops == 0 {
        return Err(Status::invalid_argument(
            "patience_hops must be positive when patience_saturation_ppm is set",
        ));
    }
    Ok(Some(crate::vector::PatienceSpec::from_ppm(ppm, hops)))
}

pub(in crate::node) fn check_floor_prune_options(
    index: &VectorIndex,
    floor_prune: Option<bool>,
    floor_k: Option<u32>,
    default_k: u32,
    floor_share_ppm: Option<u32>,
    floor_greediness_ppm: Option<u32>,
    floor_clamp_min: Option<u32>,
) -> Result<Option<crate::vector::FloorPruneSpec>, Status> {
    if floor_prune == Some(false) && (floor_greediness_ppm.is_some() || floor_clamp_min.is_some()) {
        return Err(Status::invalid_argument(
            "floor_greediness_ppm and floor_clamp_min require floor_prune = true",
        ));
    }
    let requested = floor_prune == Some(true)
        || (floor_prune.is_none() && (floor_greediness_ppm.is_some() || floor_clamp_min.is_some()));
    if let Some(share) = floor_share_ppm {
        if share > 1_000_000 {
            return Err(Status::invalid_argument(format!(
                "floor_share_ppm {share} must be in 0..=1000000"
            )));
        }
    }
    if !requested {
        // When `floor_k` and `floor_share_ppm` are supplied without enabling
        // floor pruning, carry them with `enabled = false` on graph providers
        // so `#95` patience still enforces `#72`'s ascent gate when a shared
        // floor is active.
        if let Some(k) = floor_k.filter(|&k| k > 0) {
            let descriptor = index.descriptor();
            if descriptor
                .capabilities
                .contains(&crate::vector::VectorCapability::PatienceEarlyStop)
            {
                return Ok(Some(crate::vector::FloorPruneSpec {
                    enabled: false,
                    floor_k: k as usize,
                    share_ppm: floor_share_ppm.unwrap_or(1_000_000),
                    greediness_ppm: 0,
                    clamp_min: crate::vector::DEFAULT_FLOOR_CLAMP_MIN as usize,
                }));
            }
        }
        return Ok(None);
    }
    let descriptor = index.descriptor();
    if !descriptor
        .capabilities
        .contains(&crate::vector::VectorCapability::FloorPruning)
    {
        return Err(Status::failed_precondition(format!(
            "vector backend {:?} does not support graph floor pruning (it does not advertise \
             floor_pruning)",
            descriptor.backend_kind
        )));
    }
    let greediness_ppm = floor_greediness_ppm.unwrap_or(0);
    if greediness_ppm > 1_000_000 {
        return Err(Status::invalid_argument(format!(
            "floor_greediness_ppm {greediness_ppm} must be in 0..=1000000"
        )));
    }
    let clamp_min = floor_clamp_min.unwrap_or(crate::vector::DEFAULT_FLOOR_CLAMP_MIN) as usize;
    let share_ppm = floor_share_ppm.unwrap_or(1_000_000);
    let resolved_k = floor_k.unwrap_or(default_k) as usize;
    Ok(Some(crate::vector::FloorPruneSpec {
        enabled: true,
        floor_k: resolved_k,
        share_ppm,
        greediness_ppm,
        clamp_min,
    }))
}

/// Products of one dense pass over `mask` (an index-sized allowlist):
/// every masked slot's calibrated score, or with `top` the best `depth`
/// under a self-raised floor, ties at the floor included.
fn dense_pass(
    index: &VectorIndex,
    vector: &[f32],
    mask: &[bool],
    mut top: Option<&mut TopSlots>,
    mut each: impl FnMut(usize, f32),
) -> Result<(), Status> {
    let dim = index
        .dim_opt()
        .ok_or_else(|| Status::failed_precondition("vector index has no dimension"))?;
    if vector.len() != dim {
        return Err(Status::invalid_argument(format!(
            "query vector has dim {}, index expects {dim}",
            vector.len()
        )));
    }
    if let Some((_, coord, value)) = first_invalid_coordinate(vector, dim) {
        return Err(Status::invalid_argument(format!(
            "query coordinate {coord} is invalid: {value}"
        )));
    }
    // A pass must score every masked slot. A provider that certifies
    // exhaustive completion does so on its own; a graph walk reaches only
    // part of the mask and still completes, so a provider without the
    // certificate is pinned to the enumerate regime, which scores every
    // allowed row exactly by construction (docs/hnsw.md, "Filters"). A
    // provider with neither would silently drop members or their scores.
    let descriptor = index.descriptor();
    let has = |capability| descriptor.capabilities.contains(&capability);
    let options = VectorSearchOptions::new().with_mask(mask);
    let options = if has(crate::vector::VectorCapability::ExhaustiveCompletion) {
        options
    } else if has(crate::vector::VectorCapability::MaskEnumeration) {
        // The depth sizes a walk; enumerate has none to size.
        options
            .with_filter_regime(crate::vector::FilterRegime::Enumerate)
            .with_candidate_depth(1)
    } else {
        return Err(Status::failed_precondition(format!(
            "vector backend {:?} neither completes exhaustively nor enumerates a mask; \
             Boolean evaluation needs every member's dense score",
            descriptor.backend_kind
        )));
    };
    let summary = index
        .try_search_streaming_controlled(
            vector,
            options,
            |batch| {
                let mut raise = None;
                for (&slot, &score) in batch.slots.iter().zip(batch.scores) {
                    if slot < 0 {
                        continue;
                    }
                    match top.as_deref_mut() {
                        Some(top) => {
                            if let Some(floor) = top.offer(RankedSlot {
                                score,
                                slot: slot as u32,
                            }) {
                                raise = Some(floor);
                            }
                        }
                        None => each(slot as usize, score),
                    }
                }
                match raise {
                    Some(floor) => VectorStreamControl::RaiseFloor(floor),
                    None => VectorStreamControl::Continue,
                }
            },
            || VectorStreamControl::Continue,
        )
        .map_err(|e| Status::invalid_argument(e.to_string()))?;
    if !summary.completed {
        return Err(Status::internal(
            "the dense pass of a Boolean evaluation ended without its completion certificate",
        ));
    }
    Ok(())
}

impl NodeServiceImpl {
    /// Score `slots` (sorted, distinct, all members) for one lexical
    /// leaf: the plain candidate walk under the leaf's global stats,
    /// then the score stages, as `Bm25Rescore` does per call.
    pub(in crate::node) fn boolean_lexical_scores(
        guard: &ShardState,
        leaf: &crate::pb::BooleanPlanLexical,
        slots: &[u32],
    ) -> Result<(Vec<Option<f32>>, Vec<bool>), Status> {
        let stage_specs = parse_score_stages(&leaf.score_stages)?;
        let Some(store) = guard.bm25.as_ref() else {
            return Ok((vec![None; slots.len()], vec![false; stage_specs.len()]));
        };
        let index = store.as_index().ok_or_else(|| {
            Status::failed_precondition("bm25 bulk build in progress; Flush first")
        })?;
        let chain = store.resolve_chain(&stage_specs);
        let stage_columns_known = chain
            .stages
            .iter()
            .map(|stage| stage.column.is_some())
            .collect();
        if leaf.terms.len() != leaf.global_doc_frequencies.len() {
            return Err(Status::invalid_argument(
                "terms and global_doc_frequencies must have the same length",
            ));
        }
        let params = Bm25Params {
            k1: if leaf.k1 == 0.0 {
                bm25::DEFAULT_K1
            } else {
                f64::from(leaf.k1)
            },
            b: if leaf.b == 0.0 {
                bm25::DEFAULT_B
            } else {
                f64::from(leaf.b)
            },
        };
        let stats = bm25::CorpusStats {
            doc_count: leaf.global_doc_count,
            total_doc_length: leaf.global_total_doc_length,
            dfs: leaf.global_doc_frequencies.clone(),
        };
        let numeric_read = ShardNumericRead(store);
        let raw = bm25::score_candidates_plain(index, &leaf.terms, &stats, params, slots);
        let scores = raw
            .into_iter()
            .zip(slots)
            .map(|(score, &slot)| score.map(|score| chain.eval(score, slot, &numeric_read) as f32))
            .collect();
        Ok((scores, stage_columns_known))
    }

    /// Score `slots` (sorted, distinct) for one dense leaf, `each` per
    /// product: one masked pass of the provider index, or the FP32 rows
    /// in `exact_batch` pieces under the byte ceiling, as the rescore
    /// routes do. Returns the number of products.
    pub(in crate::node) fn boolean_dense_each(
        &self,
        guard: &ShardState,
        leaf: &crate::pb::BooleanPlanDense,
        slots: &[u32],
        req: &crate::pb::BooleanShardRequest,
        mut each: impl FnMut(u32, f32),
    ) -> Result<usize, Status> {
        if slots.is_empty() {
            return Ok(0);
        }
        let mut products = 0usize;
        if leaf.exact_fp32 {
            let (exact, dim) = Self::prepare_exact_sidecar(guard, self.config.slot_offset, None)?;
            let parallel = resolved_rerank_parallel(self.config.rerank_parallel);
            let batch = if req.exact_batch == 0 {
                10_000
            } else {
                req.exact_batch as usize
            };
            for chunk in slots.chunks(batch) {
                let local: Vec<usize> = chunk.iter().map(|&slot| slot as usize).collect();
                let scored = score_exact_slots_checked(
                    exact,
                    dim,
                    &leaf.vector,
                    &local,
                    req.max_logical_bytes,
                    parallel,
                )?;
                for (slot, score) in scored.rows {
                    products += 1;
                    each(slot as u32, score);
                }
            }
            return Ok(products);
        }
        let Some(index) = guard.index.as_ref() else {
            return Ok(0);
        };
        let rows = index.len();
        let mut mask = vec![false; rows];
        for &slot in slots {
            if (slot as usize) < rows {
                mask[slot as usize] = true;
            }
        }
        dense_pass(index, &leaf.vector, &mask, None, |slot, score| {
            products += 1;
            each(slot as u32, score);
        })?;
        Ok(products)
    }

    /// One `EvaluateBoolean` request under one read guard.
    pub(in crate::node) fn run_evaluate_boolean(
        &self,
        req: crate::pb::BooleanShardRequest,
    ) -> Result<crate::pb::BooleanShardResponse, Status> {
        use crate::pb::boolean_plan_leaf::Leaf as L;
        if req.depth == 0 {
            return Err(Status::invalid_argument(
                "EvaluateBoolean: depth 0 asks for no candidates",
            ));
        }
        let depth = req.depth as usize;
        let guard = read_shard(&self.state);
        guard.check_stats_epoch(req.expected_stats_epoch, &req.expected_stats_incarnation)?;
        let receipt = guard.vector_read_receipt(Some(&crate::pb::VectorReadContext {
            field: String::new(),
            visibility: req.visibility.clone(),
            expected_stats_epoch: req.expected_stats_epoch,
            expected_stats_incarnation: req.expected_stats_incarnation.clone(),
        }))?;
        let evaluated: EvaluatedBoolean =
            evaluate_boolean_membership(&guard, &req, self.knobs.segment_pruning())?;
        let members = &evaluated.members;
        // The positive scoring leaves in leaf order, which is the
        // coordinator's traversal order and the order the signals sum in.
        let scoring: Vec<usize> = req
            .leaves
            .iter()
            .enumerate()
            .filter(|(index, leaf)| {
                evaluated.leaves[*index].positive
                    && matches!(leaf.leaf.as_ref(), Some(L::Lexical(_)) | Some(L::Dense(_)))
            })
            .map(|(index, _)| index)
            .collect();
        let mut stages_known: Vec<crate::pb::BooleanStagesKnown> = req
            .leaves
            .iter()
            .map(|_| crate::pb::BooleanStagesKnown::default())
            .collect();
        let offset = self.config.slot_offset;
        // Selection: the best `depth` members by summed score.
        let ranked: Vec<RankedSlot> = match scoring.as_slice() {
            [] => members
                .iter()
                .take(depth)
                .map(|slot| RankedSlot {
                    score: 0.0,
                    slot: slot as u32,
                })
                .collect(),
            [single]
                if matches!(
                    req.leaves[*single].leaf.as_ref(),
                    Some(L::Dense(dense)) if !dense.exact_fp32 && members.iter().all(|slot| evaluated.leaves[*single].membership.test(slot))
                ) =>
            {
                // One native dense leaf: one masked pass under a
                // self-raised floor answers the top directly.
                let Some(L::Dense(dense)) = req.leaves[*single].leaf.as_ref() else {
                    unreachable!("matched a dense leaf")
                };
                match guard.index.as_ref() {
                    None => Vec::new(),
                    // No members: an all-false mask would still walk the
                    // index.
                    Some(_) if members.count() == 0 => Vec::new(),
                    Some(index) => {
                        let rows = index.len();
                        let mut mask = vec![false; rows];
                        for slot in members.iter() {
                            if slot < rows {
                                mask[slot] = true;
                            }
                        }
                        let mut top = TopSlots::new(depth);
                        dense_pass(index, &dense.vector, &mask, Some(&mut top), |_, _| {})?;
                        top.into_sorted()
                    }
                }
            }
            _ => {
                // One accumulator per member, in member order (slot
                // ascending), not one per row of the shard: a narrow
                // group over a wide shard costs the members, not the
                // universe.
                let member_slots: Vec<u32> = members.iter().map(|slot| slot as u32).collect();
                let mut acc = vec![0.0f32; member_slots.len()];
                let mut scored = vec![false; member_slots.len()];
                // The members a leaf's membership holds: their slots
                // (a subsequence of the member list) and their indexes
                // into it.
                let in_leaf = |leaf_index: usize| -> (Vec<u32>, Vec<usize>) {
                    let bits = &evaluated.leaves[leaf_index].membership;
                    member_slots
                        .iter()
                        .enumerate()
                        .filter(|(_, &slot)| bits.test(slot as usize))
                        .map(|(at, &slot)| (slot, at))
                        .unzip()
                };
                for &leaf_index in &scoring {
                    match req.leaves[leaf_index].leaf.as_ref() {
                        Some(L::Lexical(lexical)) => {
                            let (candidates, at) = in_leaf(leaf_index);
                            let (scores, known) =
                                Self::boolean_lexical_scores(&guard, lexical, &candidates)?;
                            stages_known[leaf_index].stage_columns_known = known;
                            for ((slot, member), score) in candidates.iter().zip(&at).zip(scores) {
                                let Some(score) = score else {
                                    return Err(Status::failed_precondition(format!(
                                        "boolean membership selected doc {} for scoring clause \
                                         {leaf_index}, but the candidate scorer did not return it",
                                        offset + u64::from(*slot)
                                    )));
                                };
                                acc[*member] += score;
                                scored[*member] = true;
                            }
                        }
                        Some(L::Dense(dense)) => {
                            // The members inside the clause hold a vector
                            // (the leaf's membership says so); a member
                            // outside it, admitted by another clause,
                            // takes no score from this one.
                            let (candidates, at) = in_leaf(leaf_index);
                            let mut stray = None;
                            let products = self.boolean_dense_each(
                                &guard,
                                dense,
                                &candidates,
                                &req,
                                |slot, score| match candidates.binary_search(&slot) {
                                    Ok(position) => {
                                        acc[at[position]] += score;
                                        scored[at[position]] = true;
                                    }
                                    Err(_) => stray = Some(slot),
                                },
                            )?;
                            if let Some(slot) = stray {
                                return Err(Status::internal(format!(
                                    "dense leaf {leaf_index} scored slot {slot}, which is not \
                                     one of its {} members holding a vector",
                                    candidates.len()
                                )));
                            }
                            if products != candidates.len() {
                                return Err(Status::internal(format!(
                                    "dense leaf {leaf_index} returned {products} products for \
                                     {} members holding a vector",
                                    candidates.len()
                                )));
                            }
                        }
                        _ => unreachable!("scoring leaves are lexical or dense"),
                    }
                }
                let mut top = TopSlots::new(depth);
                for (member, &slot) in member_slots.iter().enumerate() {
                    let score = if scored[member] { acc[member] } else { 0.0 };
                    top.offer(RankedSlot { score, slot });
                }
                top.into_sorted()
            }
        };
        // Provenance: each candidate's signals in leaf order and the
        // leaves whose membership holds it.
        let candidate_slots: Vec<u32> = {
            let mut slots: Vec<u32> = ranked.iter().map(|r| r.slot).collect();
            slots.sort_unstable();
            slots
        };
        // The candidates a leaf's membership holds, in candidate order.
        let held = |slots: &[u32], leaf_index: usize| -> Vec<u32> {
            let bits = &evaluated.leaves[leaf_index].membership;
            slots
                .iter()
                .copied()
                .filter(|&slot| bits.test(slot as usize))
                .collect()
        };
        let mut per_leaf: Vec<(usize, Vec<Option<f32>>)> = Vec::with_capacity(scoring.len());
        for &leaf_index in &scoring {
            let scores = match req.leaves[leaf_index].leaf.as_ref() {
                Some(L::Lexical(lexical)) => {
                    let in_leaf = held(&candidate_slots, leaf_index);
                    let (scores, known) = Self::boolean_lexical_scores(&guard, lexical, &in_leaf)?;
                    if scoring.len() == 1 {
                        stages_known[leaf_index].stage_columns_known = known;
                    }
                    // Spread back over the candidate list.
                    let mut spread = vec![None; candidate_slots.len()];
                    for (slot, score) in in_leaf.iter().zip(scores) {
                        let position = candidate_slots
                            .binary_search(slot)
                            .expect("in_leaf is a subsequence of the candidates");
                        spread[position] = score;
                    }
                    spread
                }
                Some(L::Dense(dense)) => {
                    let in_leaf = held(&candidate_slots, leaf_index);
                    let mut spread = vec![None; candidate_slots.len()];
                    self.boolean_dense_each(&guard, dense, &in_leaf, &req, |slot, score| {
                        if let Ok(position) = candidate_slots.binary_search(&slot) {
                            spread[position] = Some(score);
                        }
                    })?;
                    spread
                }
                _ => unreachable!("scoring leaves are lexical or dense"),
            };
            per_leaf.push((leaf_index, scores));
        }
        let mut candidates = Vec::with_capacity(ranked.len());
        for entry in &ranked {
            let position = candidate_slots
                .binary_search(&entry.slot)
                .expect("ranked slots are the candidate slots");
            let mut signals = Vec::new();
            let mut score = 0.0f32;
            for (leaf_index, scores) in &per_leaf {
                if let Some(leaf_score) = scores[position] {
                    score += leaf_score;
                    signals.push(crate::pb::BooleanSignal {
                        leaf: *leaf_index as u32,
                        score: leaf_score,
                    });
                }
            }
            debug_assert_eq!(
                score.to_bits(),
                entry.score.to_bits(),
                "the provenance sum equals the selection score"
            );
            let matched = evaluated
                .leaves
                .iter()
                .enumerate()
                .filter(|(_, leaf)| {
                    leaf.membership.test(entry.slot as usize)
                        || leaf.filter_recheck.as_ref().is_some_and(|filter| {
                            guard.bm25.as_ref().is_some_and(|store| {
                                entry.slot < store.next_doc_id()
                                    && filter.passes(entry.slot, &ShardNumericRead(store))
                            })
                        })
                })
                .map(|(index, _)| index as u32)
                .collect();
            candidates.push(crate::pb::BooleanCandidate {
                doc_id: offset + u64::from(entry.slot),
                score,
                signals,
                matched,
            });
        }
        let aggregate = match req.aggregate.as_ref() {
            Some(spec) => Some(self.run_aggregate_shard(
                &guard,
                crate::pb::AggregateShardRequest {
                    visibility: req.visibility.clone(),
                    expected_stats_epoch: req.expected_stats_epoch,
                    expected_stats_incarnation: req.expected_stats_incarnation.clone(),
                    filter: None,
                    geo_filters: Vec::new(),
                    aggregations: spec.aggregations.clone(),
                    group_by: spec.group_by.clone(),
                    max_groups: spec.max_groups,
                    histograms: spec.histograms.clone(),
                    percentiles: spec.percentiles.clone(),
                    doc_ids: Vec::new(),
                    restrict_doc_ids: false,
                },
                Some(members),
            )?),
            None => None,
        };
        Ok(crate::pb::BooleanShardResponse {
            read_receipt: receipt,
            candidates,
            matched: members.count(),
            segments_total: evaluated.prune.segments_total,
            segments_skipped: evaluated.prune.segments_skipped,
            stats_epoch: guard.stats_epoch,
            filters_known: evaluated
                .leaves
                .iter()
                .map(|leaf| leaf.filter_known.clone().unwrap_or_default())
                .collect(),
            stages_known,
            aggregate,
        })
    }
}
