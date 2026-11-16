//! Ranked boolean scoring across lexical and dense vector leaves.

use crate::node::*;

mod evaluate;
mod options;

pub(in crate::node) use options::{
    check_enumerate_threshold, check_floor_prune_options, check_patience_options,
};

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
    // Score every masked slot: providers without `ExhaustiveCompletion` fall
    // back to `MaskEnumeration` so a partial graph walk cannot drop members.
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
        let index = store.as_index();
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
}
