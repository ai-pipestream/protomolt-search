//! The steps before the group walk: binding checks, the row universe, the
//! caller's authority, lexical memberships, and the filter domains.

use super::*;
use crate::boolean_bits::Bits;

pub(super) fn check_leaf_bindings(
    guard: &ShardState,
    req: &crate::pb::BooleanShardRequest,
) -> Result<(), Status> {
    for leaf in &req.leaves {
        match leaf.leaf.as_ref() {
            Some(crate::pb::boolean_plan_leaf::Leaf::Lexical(leaf)) => {
                guard.check_query_analysis("body", leaf.analysis_fingerprint)?
            }
            Some(crate::pb::boolean_plan_leaf::Leaf::Dense(leaf)) => {
                guard.vector_binding(&leaf.field)?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// The universe is the shard's rows: the lexical store's documents
/// (some without a vector) and the provider's vectors (some without a
/// document yet), whichever reaches further.
pub(super) fn universe(guard: &ShardState) -> usize {
    guard
        .bm25
        .as_ref()
        .map_or(0, |store| store.next_doc_id() as usize)
        .max(guard.index.as_ref().map_or(0, VectorIndex::len))
        .max(
            guard
                .exact_vectors
                .as_ref()
                .map_or(0, ExactVectorStore::len),
        )
}

/// The live rows the request's visibility view admits, over a universe of
/// `n` rows of which the first `docs` hold a document.
pub(super) fn shard_authority(
    guard: &ShardState,
    req: &crate::pb::BooleanShardRequest,
    deleted: &Option<Arc<Vec<u64>>>,
    (n, docs): (usize, usize),
    prune_enabled: bool,
) -> Result<Bits, Status> {
    let mut authority = Bits::live(deleted.as_deref().map(Vec::as_slice), n);
    if let Some(view) = &req.visibility {
        let (_, allowed, _) = resolve_shard_filters(
            guard.bm25.as_ref(),
            deleted.clone(),
            n,
            ShardFilterInputs {
                geo: &[],
                regions: &[],
                tree: view.filter.as_ref(),
            },
            prune_enabled,
            None,
        )?;
        if let Some(allowed) = allowed {
            authority.and_with(&allowed);
        }
        authority.and_with(&Bits::prefix(n, docs));
    }
    Ok(authority)
}

/// Lexical leaves resolve in the pre-pass: the cheapest membership
/// (a postings walk) and the filters' narrowing source. Dense and
/// filter leaves resolve when the group walk reaches them, so a
/// group whose MUST intersection is already empty never pays them.
pub(super) struct LexicalPrepass<'a> {
    pub(super) guard: &'a ShardState,
    pub(super) n: usize,
    pub(super) prune_enabled: bool,
    pub(super) authority: &'a Bits,
    pub(super) positive: &'a [bool],
}

impl LexicalPrepass<'_> {
    pub(super) fn resolve(
        &self,
        req: &crate::pb::BooleanShardRequest,
        prune: &mut crate::segment_prune::PruneStats,
    ) -> Result<Vec<LeafSlot>, Status> {
        use crate::pb::boolean_plan_leaf::Leaf as L;
        let mut leaves: Vec<LeafSlot> = Vec::with_capacity(req.leaves.len());
        for (index, leaf) in req.leaves.iter().enumerate() {
            match leaf.leaf.as_ref() {
                Some(L::Filter(_)) | Some(L::Dense(_)) => leaves.push(LeafSlot::Todo),
                Some(L::Lexical(lexical)) => {
                    let mut bits = self.term_members(&lexical.terms, prune)?;
                    bits.and_with(self.authority);
                    leaves.push(LeafSlot::Done(EvaluatedLeaf {
                        membership: bits,
                        positive: self.positive[index],
                        filter_known: None,
                        filter_recheck: None,
                    }));
                }
                None => {
                    return Err(Status::invalid_argument(format!(
                        "Boolean plan leaf {index} has no kind"
                    )))
                }
            }
        }
        Ok(leaves)
    }

    /// The undeleted rows holding any of `terms` in the body field.
    fn term_members(
        &self,
        terms: &[String],
        prune: &mut crate::segment_prune::PruneStats,
    ) -> Result<Bits, Status> {
        let (guard, n, prune_enabled) = (self.guard, self.n, self.prune_enabled);
        let mut bits = Bits::empty(n);
        if terms.is_empty() {
            return Ok(bits);
        }
        let Some(store) = guard.bm25.as_ref() else {
            return Ok(bits);
        };
        let index = store.as_index();
        // A sealed segment none of the terms occur in contributes no member.
        let (masked, stats) = match store {
            Bm25Shard::Segmented(segmented) => {
                let lacking = segmented.parts_lacking_terms(0, terms);
                let skipped = lacking.iter().filter(|&&lacks| lacks).count() as u32;
                let stats = crate::segment_prune::PruneStats {
                    segments_total: lacking.len() as u32,
                    segments_skipped: if prune_enabled { skipped } else { 0 },
                };
                let masked = (prune_enabled && skipped > 0).then(|| {
                    segmented.masked(lacking.iter().map(|&lacks| !lacks).collect::<Arc<[bool]>>())
                });
                (masked, stats)
            }
            _ => (None, crate::segment_prune::PruneStats::default()),
        };
        prune.add(stats);
        let index: &dyn Bm25Index = match masked.as_ref() {
            Some(masked) => masked,
            None => index,
        };
        for term in terms {
            index.for_each_doc_tf(term, &mut |doc_id, _tf| {
                let slot = doc_id as usize;
                if slot < n && !guard.live_docs.is_deleted(slot) {
                    bits.set(slot);
                }
            });
        }
        Ok(bits)
    }
}

/// Each filter leaf's narrowed domain; `None` keeps the leaf whole-shard.
pub(super) fn filter_domains(
    req: &crate::pb::BooleanShardRequest,
    root: &crate::pb::BooleanPlanGroup,
    leaves: &[LeafSlot],
) -> Result<Vec<Option<Bits>>, Status> {
    let mut domains: Vec<Option<Bits>> = vec![None; req.leaves.len()];
    if boolean_filter_domain_enabled() {
        let mut bounded: Vec<Option<Bits>> = vec![None; req.leaves.len()];
        let mut unbounded = vec![false; req.leaves.len()];
        filter_leaf_domains(req, root, None, leaves, &mut bounded, &mut unbounded, 1)?;
        for (index, domain) in bounded.into_iter().enumerate() {
            if !unbounded[index] {
                domains[index] = domain;
            }
        }
    }
    Ok(domains)
}
