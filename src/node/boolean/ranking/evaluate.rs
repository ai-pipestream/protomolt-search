//! One `EvaluateBoolean` request: selection, provenance, and the response.

use super::*;
use crate::pb::boolean_plan_leaf::Leaf as L;

/// One request's resolved membership and the leaves that score it.
struct Evaluation<'a> {
    node: &'a NodeServiceImpl,
    guard: &'a ShardState,
    req: &'a crate::pb::BooleanShardRequest,
    evaluated: &'a EvaluatedBoolean,
    /// The positive scoring leaves in leaf order, which is the
    /// coordinator's traversal order and the order the signals sum in.
    scoring: Vec<usize>,
    depth: usize,
}

/// One accumulator per member, in member order (slot ascending), not one
/// per row of the shard: a narrow group over a wide shard costs the
/// members, not the universe.
struct MemberSums {
    slots: Vec<u32>,
    acc: Vec<f32>,
    scored: Vec<bool>,
}

/// One scoring leaf's index and its score for each candidate it holds.
type LeafSignals = (usize, Vec<Option<f32>>);

impl MemberSums {
    /// The members a leaf's membership holds: their slots (a subsequence
    /// of the member list) and their indexes into it.
    fn in_leaf(&self, bits: &crate::boolean_bits::Bits) -> (Vec<u32>, Vec<usize>) {
        self.slots
            .iter()
            .enumerate()
            .filter(|(_, &slot)| bits.test(slot as usize))
            .map(|(at, &slot)| (slot, at))
            .unzip()
    }
}

fn scoring_leaves(
    req: &crate::pb::BooleanShardRequest,
    evaluated: &EvaluatedBoolean,
) -> Vec<usize> {
    req.leaves
        .iter()
        .enumerate()
        .filter(|(index, leaf)| {
            evaluated.leaves[*index].positive
                && matches!(leaf.leaf.as_ref(), Some(L::Lexical(_)) | Some(L::Dense(_)))
        })
        .map(|(index, _)| index)
        .collect()
}

impl<'a> Evaluation<'a> {
    /// Selection: the best `depth` members by summed score.
    fn select(
        &self,
        stages_known: &mut [crate::pb::BooleanStagesKnown],
    ) -> Result<Vec<RankedSlot>, Status> {
        if self.scoring.is_empty() {
            return Ok(self
                .evaluated
                .members
                .iter()
                .take(self.depth)
                .map(|slot| RankedSlot {
                    score: 0.0,
                    slot: slot as u32,
                })
                .collect());
        }
        if let Some(dense) = self.sole_native_dense() {
            return self.select_native_dense(dense);
        }
        self.select_summed(stages_known)
    }

    /// The only scoring leaf, when it is a native dense leaf whose
    /// membership holds every member.
    fn sole_native_dense(&self) -> Option<&'a crate::pb::BooleanPlanDense> {
        let [single] = self.scoring.as_slice() else {
            return None;
        };
        match self.req.leaves[*single].leaf.as_ref() {
            Some(L::Dense(dense))
                if !dense.exact_fp32
                    && self
                        .evaluated
                        .members
                        .iter()
                        .all(|slot| self.evaluated.leaves[*single].membership.test(slot)) =>
            {
                Some(dense)
            }
            _ => None,
        }
    }

    /// One native dense leaf: one masked pass under a self-raised floor
    /// answers the top directly.
    fn select_native_dense(
        &self,
        dense: &crate::pb::BooleanPlanDense,
    ) -> Result<Vec<RankedSlot>, Status> {
        let members = &self.evaluated.members;
        match self.guard.index.as_ref() {
            None => Ok(Vec::new()),
            // No members: an all-false mask would still walk the index.
            Some(_) if members.count() == 0 => Ok(Vec::new()),
            Some(index) => {
                let rows = index.len();
                let mut mask = vec![false; rows];
                for slot in members.iter() {
                    if slot < rows {
                        mask[slot] = true;
                    }
                }
                let mut top = TopSlots::new(self.depth);
                dense_pass(index, &dense.vector, &mask, Some(&mut top), |_, _| {})?;
                Ok(top.into_sorted())
            }
        }
    }

    fn select_summed(
        &self,
        stages_known: &mut [crate::pb::BooleanStagesKnown],
    ) -> Result<Vec<RankedSlot>, Status> {
        let slots: Vec<u32> = self
            .evaluated
            .members
            .iter()
            .map(|slot| slot as u32)
            .collect();
        let mut sums = MemberSums {
            acc: vec![0.0f32; slots.len()],
            scored: vec![false; slots.len()],
            slots,
        };
        for &leaf_index in &self.scoring {
            match self.req.leaves[leaf_index].leaf.as_ref() {
                Some(L::Lexical(lexical)) => {
                    stages_known[leaf_index].stage_columns_known =
                        self.sum_lexical(leaf_index, lexical, &mut sums)?;
                }
                Some(L::Dense(dense)) => self.sum_dense(leaf_index, dense, &mut sums)?,
                _ => unreachable!("scoring leaves are lexical or dense"),
            }
        }
        let mut top = TopSlots::new(self.depth);
        for (member, &slot) in sums.slots.iter().enumerate() {
            let score = if sums.scored[member] {
                sums.acc[member]
            } else {
                0.0
            };
            top.offer(RankedSlot { score, slot });
        }
        Ok(top.into_sorted())
    }

    /// Add one lexical leaf's scores to its members. Returns which of the
    /// leaf's score stages name a known column.
    fn sum_lexical(
        &self,
        leaf_index: usize,
        lexical: &crate::pb::BooleanPlanLexical,
        sums: &mut MemberSums,
    ) -> Result<Vec<bool>, Status> {
        let (candidates, at) = sums.in_leaf(&self.evaluated.leaves[leaf_index].membership);
        let (scores, known) =
            NodeServiceImpl::boolean_lexical_scores(self.guard, lexical, &candidates)?;
        for ((slot, member), score) in candidates.iter().zip(&at).zip(scores) {
            let Some(score) = score else {
                return Err(Status::failed_precondition(format!(
                    "boolean membership selected doc {} for scoring clause \
                     {leaf_index}, but the candidate scorer did not return it",
                    self.node.config.slot_offset + u64::from(*slot)
                )));
            };
            sums.acc[*member] += score;
            sums.scored[*member] = true;
        }
        Ok(known)
    }

    /// Add one dense leaf's scores to its members. The members inside the
    /// clause hold a vector (the leaf's membership says so); a member
    /// outside it, admitted by another clause, takes no score from this one.
    fn sum_dense(
        &self,
        leaf_index: usize,
        dense: &crate::pb::BooleanPlanDense,
        sums: &mut MemberSums,
    ) -> Result<(), Status> {
        let (candidates, at) = sums.in_leaf(&self.evaluated.leaves[leaf_index].membership);
        let (acc, scored) = (&mut sums.acc, &mut sums.scored);
        let mut stray = None;
        let products = self.node.boolean_dense_each(
            self.guard,
            dense,
            &candidates,
            self.req,
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
        Ok(())
    }

    /// The candidates a leaf's membership holds, in candidate order.
    fn held(&self, slots: &[u32], leaf_index: usize) -> Vec<u32> {
        let bits = &self.evaluated.leaves[leaf_index].membership;
        slots
            .iter()
            .copied()
            .filter(|&slot| bits.test(slot as usize))
            .collect()
    }

    /// Provenance: each scoring leaf's score for every candidate it holds,
    /// spread over the candidate list.
    fn leaf_signals(
        &self,
        candidate_slots: &[u32],
        stages_known: &mut [crate::pb::BooleanStagesKnown],
    ) -> Result<Vec<LeafSignals>, Status> {
        let mut per_leaf = Vec::with_capacity(self.scoring.len());
        for &leaf_index in &self.scoring {
            let in_leaf = self.held(candidate_slots, leaf_index);
            let mut spread = vec![None; candidate_slots.len()];
            match self.req.leaves[leaf_index].leaf.as_ref() {
                Some(L::Lexical(lexical)) => {
                    let (scores, known) =
                        NodeServiceImpl::boolean_lexical_scores(self.guard, lexical, &in_leaf)?;
                    if self.scoring.len() == 1 {
                        stages_known[leaf_index].stage_columns_known = known;
                    }
                    for (slot, score) in in_leaf.iter().zip(scores) {
                        let position = candidate_slots
                            .binary_search(slot)
                            .expect("in_leaf is a subsequence of the candidates");
                        spread[position] = score;
                    }
                }
                Some(L::Dense(dense)) => {
                    self.node.boolean_dense_each(
                        self.guard,
                        dense,
                        &in_leaf,
                        self.req,
                        |slot, score| {
                            if let Ok(position) = candidate_slots.binary_search(&slot) {
                                spread[position] = Some(score);
                            }
                        },
                    )?;
                }
                _ => unreachable!("scoring leaves are lexical or dense"),
            }
            per_leaf.push((leaf_index, spread));
        }
        Ok(per_leaf)
    }

    /// The leaves whose membership, or narrowed filter, holds `slot`.
    fn matched(&self, slot: u32) -> Vec<u32> {
        let guard = self.guard;
        self.evaluated
            .leaves
            .iter()
            .enumerate()
            .filter(|(_, leaf)| {
                leaf.membership.test(slot as usize)
                    || leaf.filter_recheck.as_ref().is_some_and(|filter| {
                        guard.bm25.as_ref().is_some_and(|store| {
                            slot < store.next_doc_id()
                                && filter.passes(slot, &ShardNumericRead(store))
                        })
                    })
            })
            .map(|(index, _)| index as u32)
            .collect()
    }

    /// Each ranked candidate with its signals in leaf order.
    fn candidates(
        &self,
        ranked: &[RankedSlot],
        candidate_slots: &[u32],
        per_leaf: &[LeafSignals],
    ) -> Vec<crate::pb::BooleanCandidate> {
        let mut candidates = Vec::with_capacity(ranked.len());
        for entry in ranked {
            let position = candidate_slots
                .binary_search(&entry.slot)
                .expect("ranked slots are the candidate slots");
            let mut signals = Vec::new();
            let mut score = 0.0f32;
            for (leaf_index, scores) in per_leaf {
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
            candidates.push(crate::pb::BooleanCandidate {
                doc_id: self.node.config.slot_offset + u64::from(entry.slot),
                score,
                signals,
                matched: self.matched(entry.slot),
            });
        }
        candidates
    }

    fn aggregate(&self) -> Result<Option<crate::pb::AggregateShardResponse>, Status> {
        let req = self.req;
        match req.aggregate.as_ref() {
            Some(spec) => Ok(Some(self.node.run_aggregate_shard(
                self.guard,
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
                Some(&self.evaluated.members),
            )?)),
            None => Ok(None),
        }
    }
}

impl NodeServiceImpl {
    /// One `EvaluateBoolean` request under one read guard.
    pub(in crate::node) fn run_evaluate_boolean(
        &self,
        req: crate::pb::BooleanShardRequest,
    ) -> Result<crate::pb::BooleanShardResponse, Status> {
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
        let eval = Evaluation {
            node: self,
            guard: &guard,
            req: &req,
            evaluated: &evaluated,
            scoring: scoring_leaves(&req, &evaluated),
            depth,
        };
        let mut stages_known: Vec<crate::pb::BooleanStagesKnown> = req
            .leaves
            .iter()
            .map(|_| crate::pb::BooleanStagesKnown::default())
            .collect();
        let ranked = eval.select(&mut stages_known)?;
        let candidate_slots: Vec<u32> = {
            let mut slots: Vec<u32> = ranked.iter().map(|r| r.slot).collect();
            slots.sort_unstable();
            slots
        };
        let per_leaf = eval.leaf_signals(&candidate_slots, &mut stages_known)?;
        let candidates = eval.candidates(&ranked, &candidate_slots, &per_leaf);
        let aggregate = eval.aggregate()?;
        Ok(crate::pb::BooleanShardResponse {
            read_receipt: receipt,
            candidates,
            matched: evaluated.members.count(),
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
