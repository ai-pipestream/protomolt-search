//! On-demand leaf resolution and the group walk of one evaluation.

use super::*;

/// The working state of one [`evaluate_boolean_membership`] call: the
/// shard, the request, the shared bitmaps, the per-leaf domains, and
/// the slots the group walk fills on demand. Lexical leaves resolve in
/// the pre-pass (they are the filters' narrowing source); dense leaves
/// and filter leaves resolve when the group walk reaches them, so a
/// group whose MUST intersection is already empty never pays them.
pub(super) struct BooleanMembership<'a> {
    pub(in crate::node) guard: &'a ShardState,
    pub(in crate::node) req: &'a crate::pb::BooleanShardRequest,
    pub(in crate::node) n: usize,
    pub(in crate::node) prune_enabled: bool,
    pub(in crate::node) positive: &'a [bool],
    pub(in crate::node) domains: &'a [Option<crate::boolean_bits::Bits>],
    pub(in crate::node) deleted: &'a Option<Arc<Vec<u64>>>,
    pub(in crate::node) authority: &'a crate::boolean_bits::Bits,
    pub(in crate::node) live: &'a crate::boolean_bits::Bits,
    pub(in crate::node) prune: crate::segment_prune::PruneStats,
    pub(in crate::node) seeded: u32,
    pub(in crate::node) leaves: Vec<LeafSlot>,
}

impl BooleanMembership<'_> {
    /// The filter leaf's membership over its domain — over an empty
    /// domain when its group is already empty (`short`), so the
    /// validation, the known-column handshake, and the prune counts
    /// still run and no row is read.
    pub(in crate::node) fn resolve_filter(
        &mut self,
        index: usize,
        short: bool,
    ) -> Result<(), Status> {
        use crate::boolean_bits::Bits;
        if matches!(self.leaves[index], LeafSlot::Done(_)) {
            return Ok(());
        }
        let filter = match boolean_leaf(self.req, index as u32)? {
            crate::pb::boolean_plan_leaf::Leaf::Filter(filter) => filter,
            _ => {
                return Err(Status::internal(format!(
                    "boolean leaf {index} is not a filter"
                )))
            }
        };
        let geo_regions = validate_geo_filters(&filter.geo_filters)?;
        let bm25 = self.guard.bm25.as_ref();
        let (geo_columns_known, filter_columns_known) =
            filter_known_flags(bm25, &filter.geo_filters, filter.filter.as_ref());
        let empty;
        let domain = if short {
            BOOLEAN_MUST_SHORT_CIRCUIT_SKIPS.fetch_add(1, AtomicOrdering::Relaxed);
            empty = Bits::empty(self.n);
            Some(&empty)
        } else {
            self.domains[index].as_ref()
        };
        let (predicate, allow, stats) = resolve_shard_filters(
            bm25,
            self.deleted.clone(),
            self.n,
            ShardFilterInputs {
                geo: &filter.geo_filters,
                regions: &geo_regions,
                tree: filter.filter.as_ref(),
            },
            self.prune_enabled,
            domain,
        )?;
        self.prune.add(stats);
        let mut membership = match allow {
            None => Bits::full(self.n),
            Some(allow) => {
                if allow.len() != self.n {
                    return Err(Status::internal(format!(
                        "Boolean filter leaf {index} resolved {} slots over a universe \
                         of {}",
                        allow.len(),
                        self.n
                    )));
                }
                allow
            }
        };
        // The early shapes of resolve_shard_filters (no predicate, no
        // store) ignore the domain; narrow here so every shape is
        // bounded the same way.
        if let Some(domain) = domain {
            membership.and_with(domain);
        }
        membership.and_with(self.authority);
        membership.and_with(self.live);
        self.leaves[index] = LeafSlot::Done(EvaluatedLeaf {
            membership,
            positive: self.positive[index],
            filter_known: Some(crate::pb::BooleanFilterKnown {
                geo_columns_known,
                filter_columns_known,
            }),
            filter_recheck: (domain.is_some() && bm25.is_some())
                .then(|| predicate.unwrap_or_default()),
        });
        Ok(())
    }

    /// The dense leaf's membership: the live rows that hold a vector,
    /// resolved on first use. Scoring mode does not change clause
    /// membership: an absent FP32 sidecar must not turn a native vector
    /// into a non-member and silently skip its rescore.
    pub(in crate::node) fn resolve_dense(&mut self, index: usize) -> Result<(), Status> {
        use crate::boolean_bits::Bits;
        if matches!(self.leaves[index], LeafSlot::Done(_)) {
            return Ok(());
        }
        let dense = match boolean_leaf(self.req, index as u32)? {
            crate::pb::boolean_plan_leaf::Leaf::Dense(dense) => dense,
            _ => {
                return Err(Status::internal(format!(
                    "boolean leaf {index} is not dense"
                )))
            }
        };
        let guard = self.guard;
        let ranges = guard.index.as_ref().map_or_else(
            || {
                if dense.exact_fp32 {
                    vec![(
                        0,
                        guard
                            .exact_vectors
                            .as_ref()
                            .map_or(0, ExactVectorStore::len),
                    )]
                } else {
                    Vec::new()
                }
            },
            VectorIndex::vector_rows,
        );
        let mut membership = Bits::ranges(self.n, &ranges);
        membership.and_with(self.authority);
        self.leaves[index] = LeafSlot::Done(EvaluatedLeaf {
            membership,
            positive: self.positive[index],
            filter_known: None,
            filter_recheck: None,
        });
        Ok(())
    }

    /// A dense leaf under a short-circuited group: the empty set,
    /// unpaid. The bits are invisible — the emptiness reaches the root
    /// through the MUST chain, so no scoring, provenance, or aggregate
    /// row can consult them.
    pub(in crate::node) fn skip_dense(&mut self, index: usize) {
        if matches!(self.leaves[index], LeafSlot::Todo) {
            BOOLEAN_MUST_SHORT_CIRCUIT_SKIPS.fetch_add(1, AtomicOrdering::Relaxed);
            self.leaves[index] = LeafSlot::Done(EvaluatedLeaf {
                membership: crate::boolean_bits::Bits::empty(self.n),
                positive: self.positive[index],
                filter_known: None,
                filter_recheck: None,
            });
        }
    }

    pub(in crate::node) fn short_leaf(&mut self, index: u32) -> Result<(), Status> {
        match boolean_leaf(self.req, index)? {
            crate::pb::boolean_plan_leaf::Leaf::Lexical(_) => Ok(()),
            crate::pb::boolean_plan_leaf::Leaf::Dense(_) => {
                self.skip_dense(index as usize);
                Ok(())
            }
            crate::pb::boolean_plan_leaf::Leaf::Filter(_) => {
                self.resolve_filter(index as usize, true)
            }
        }
    }

    /// Resolve every leaf under an already-empty group without paying
    /// for it: filters over the empty domain, dense leaves as the
    /// empty set.
    pub(in crate::node) fn short_group(
        &mut self,
        group: &crate::pb::BooleanPlanGroup,
        depth: usize,
    ) -> Result<(), Status> {
        if depth > 64 {
            return Err(Status::invalid_argument(
                "Boolean plan exceeds the 64-level recursion limit",
            ));
        }
        for node in group
            .must
            .iter()
            .chain(&group.should)
            .chain(&group.must_not)
        {
            self.short_node(node, depth)?;
        }
        Ok(())
    }

    pub(in crate::node) fn short_node(
        &mut self,
        node: &crate::pb::BooleanPlanNode,
        depth: usize,
    ) -> Result<(), Status> {
        use crate::pb::boolean_plan_node::Node;
        match node.node.as_ref() {
            Some(Node::Group(child)) => self.short_group(child, depth + 1),
            Some(Node::Leaf(index)) => self.short_leaf(*index),
            None => Err(Status::invalid_argument(
                "Boolean plan node is neither a group nor a leaf",
            )),
        }
    }

    /// One resolved node's membership bits.
    pub(in crate::node) fn resolve_node(
        &mut self,
        node: &crate::pb::BooleanPlanNode,
        on_must_path: bool,
        depth: usize,
    ) -> Result<crate::boolean_bits::Bits, Status> {
        use crate::pb::boolean_plan_node::Node;
        match node.node.as_ref() {
            Some(Node::Group(child)) => self.resolve_group(child, on_must_path, depth + 1),
            Some(Node::Leaf(index)) => {
                let index = *index as usize;
                match boolean_leaf(self.req, index as u32)? {
                    crate::pb::boolean_plan_leaf::Leaf::Lexical(_) => {}
                    crate::pb::boolean_plan_leaf::Leaf::Filter(_) => {
                        self.resolve_filter(index, false)?
                    }
                    crate::pb::boolean_plan_leaf::Leaf::Dense(_) => self.resolve_dense(index)?,
                }
                match &self.leaves[index] {
                    LeafSlot::Done(leaf) => Ok(leaf.membership.clone()),
                    LeafSlot::Todo => Err(Status::internal(format!(
                        "boolean leaf {index} was never evaluated"
                    ))),
                }
            }
            None => Err(Status::invalid_argument(
                "Boolean plan node is neither a group nor a leaf",
            )),
        }
    }

    /// The group's members under the Boolean group rule. MUST children
    /// resolve cheapest first —
    /// lexical bits come from the pre-pass, then filters, then nested
    /// groups, then dense leaves — folding the running intersection.
    /// Once the intersection is empty on a MUST-only path from the
    /// root the group admits no row and neither does the root, so the
    /// remaining children resolve in short mode: their verdicts are
    /// unread by the group rule, the scorers, and provenance. SHOULD
    /// and MUST_NOT subtrees never short-circuit: off the MUST path
    /// their verdicts still count toward the minimum or the
    /// subtraction, and a returned candidate's matched list reads
    /// every leaf.
    pub(in crate::node) fn resolve_group(
        &mut self,
        group: &crate::pb::BooleanPlanGroup,
        on_must_path: bool,
        depth: usize,
    ) -> Result<crate::boolean_bits::Bits, Status> {
        use crate::boolean_bits::Bits;
        if depth > 64 {
            return Err(Status::invalid_argument(
                "Boolean plan exceeds the 64-level recursion limit",
            ));
        }
        let order = self.must_order(group)?;
        let mut must: Vec<Option<Bits>> = (0..group.must.len()).map(|_| None).collect();
        let mut acc: Option<Bits> = None;
        let mut short = false;
        for &c in &order {
            if short {
                continue;
            }
            let bits = self.resolve_node(&group.must[c], on_must_path, depth)?;
            match &mut acc {
                Some(acc) => acc.and_with(&bits),
                None => acc = Some(bits.clone()),
            }
            must[c] = Some(bits);
            // The restricted bits of a filter are exact wherever the
            // exact conjuncts hold, so the running intersection equals
            // the true intersection of the children resolved so far.
            if on_must_path && acc.as_ref().is_some_and(|acc| acc.count() == 0) {
                short = true;
            }
        }
        if short {
            for &c in &order {
                if must[c].is_none() {
                    self.short_node(&group.must[c], depth)?;
                }
            }
            for node in group.should.iter().chain(&group.must_not) {
                self.short_node(node, depth)?;
            }
            return Ok(Bits::empty(self.n));
        }
        let mut should = Vec::with_capacity(group.should.len());
        for node in &group.should {
            should.push(self.resolve_node(node, false, depth)?);
        }
        let mut must_not = Vec::with_capacity(group.must_not.len());
        for node in &group.must_not {
            must_not.push(self.resolve_node(node, false, depth)?);
        }
        let must: Vec<Bits> = must
            .into_iter()
            .map(|bits| bits.expect("every MUST child resolved above"))
            .collect();
        let (members, seeded_live) = crate::boolean_bits::group_members(
            &must,
            &should,
            &must_not,
            group.minimum_should_match as usize,
            self.live,
        );
        if seeded_live {
            self.seeded += 1;
        }
        Ok(members)
    }

    /// The group's MUST children cheapest first, stable inside a class:
    /// lexical (pre-resolved), filter, nested group, dense.
    fn must_order(&self, group: &crate::pb::BooleanPlanGroup) -> Result<Vec<usize>, Status> {
        use crate::pb::boolean_plan_node::Node;
        let mut order: Vec<usize> = (0..group.must.len()).collect();
        let mut classes = Vec::with_capacity(order.len());
        for &c in &order {
            classes.push(match group.must[c].node.as_ref() {
                Some(Node::Group(_)) => 2,
                Some(Node::Leaf(index)) => match boolean_leaf(self.req, *index)? {
                    crate::pb::boolean_plan_leaf::Leaf::Lexical(_) => 0,
                    crate::pb::boolean_plan_leaf::Leaf::Filter(_) => 1,
                    crate::pb::boolean_plan_leaf::Leaf::Dense(_) => 3,
                },
                None => {
                    return Err(Status::invalid_argument(
                        "Boolean plan node is neither a group nor a leaf",
                    ))
                }
            });
        }
        order.sort_by_key(|&c| classes[c]);
        Ok(order)
    }

    /// A leaf the tree never references still resolves: its handshakes
    /// ride the response.
    pub(super) fn resolve_unreferenced(&mut self) -> Result<(), Status> {
        for index in 0..self.req.leaves.len() {
            if matches!(self.leaves[index], LeafSlot::Done(_)) {
                continue;
            }
            match boolean_leaf(self.req, index as u32)? {
                crate::pb::boolean_plan_leaf::Leaf::Filter(_) => {
                    self.resolve_filter(index, false)?
                }
                crate::pb::boolean_plan_leaf::Leaf::Dense(_) => self.resolve_dense(index)?,
                crate::pb::boolean_plan_leaf::Leaf::Lexical(_) => {
                    return Err(Status::internal(format!(
                        "boolean lexical leaf {index} survived the pre-pass unresolved"
                    )))
                }
            }
        }
        Ok(())
    }

    /// The evaluation's result once every leaf is resolved.
    pub(super) fn finish(
        self,
        members: crate::boolean_bits::Bits,
    ) -> Result<EvaluatedBoolean, Status> {
        let bm25 = self.guard.bm25.as_ref();
        let BooleanMembership {
            mut prune,
            seeded,
            leaves,
            ..
        } = self;
        let leaves: Vec<EvaluatedLeaf> = leaves
            .into_iter()
            .enumerate()
            .map(|(index, leaf)| match leaf {
                LeafSlot::Done(leaf) => Ok(leaf),
                LeafSlot::Todo => Err(Status::internal(format!(
                    "boolean leaf {index} was never evaluated"
                ))),
            })
            .collect::<Result<_, _>>()?;
        for _ in 0..seeded {
            prune.add(crate::segment_prune::PruneStats {
                segments_total: sealed_total(bm25),
                segments_skipped: 0,
            });
        }
        Ok(EvaluatedBoolean {
            members,
            leaves,
            prune,
        })
    }
}
