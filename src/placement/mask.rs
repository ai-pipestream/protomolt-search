//! One request's verdict over a topology.

use std::collections::{BTreeSet, HashMap};

use super::implied::{implied_under, without_leaves};
use super::prune::impossible_under;
use super::tree::Placement;
use crate::pb;

/// One request's verdict over a topology: which shards the filter
/// cannot match, and which filter leaves proved it (they count as
/// resolved for the typo handshake, since the leaf predicate that
/// excluded the shard named the same column).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShardMask {
    pub skipped: Vec<bool>,
    /// Filter-leaf indices that took part in at least one exclusion, or
    /// that a consulted shard's leaf implies (they count as resolved:
    /// the leaf predicate that decided them named the same column, and
    /// the shard holds a value for it on every row).
    pub known: Vec<usize>,
    /// Per shard, the filter leaves its placement leaf implies
    /// ([`implied_under`]); empty for a skipped shard or one without a
    /// code. [`ShardMask::filter_for`] removes them from that shard's tree.
    pub implied: Vec<Vec<usize>>,
}

impl ShardMask {
    /// The mask for `filter` over `codes` (one code per shard, `None`
    /// for a shard without one). A shard whose code the tree does not
    /// know is never skipped. One shard is always consulted: when the
    /// filter would exclude every shard, the first stays, so the known
    /// handshake and the answer's shape are the ones a consulted fleet
    /// produces.
    pub fn compute(placement: &Placement, codes: &[Option<i64>], filter: &pb::FilterExpr) -> Self {
        let mut skipped = vec![false; codes.len()];
        let mut implied = vec![Vec::new(); codes.len()];
        let mut known = BTreeSet::new();
        let mut verdicts: HashMap<i64, Option<Vec<usize>>> = HashMap::new();
        let mut implications: HashMap<i64, Vec<usize>> = HashMap::new();
        for (shard, code) in codes.iter().enumerate() {
            let Some(code) = code else { continue };
            let verdict = verdicts.entry(*code).or_insert_with(|| {
                placement
                    .leaf_by_code(*code)
                    .and_then(|leaf| impossible_under(filter, &leaf.bounds))
            });
            if let Some(leaves) = verdict {
                skipped[shard] = true;
                known.extend(leaves.iter().copied());
                continue;
            }
            let leaves = implications.entry(*code).or_insert_with(|| {
                placement
                    .leaf_by_code(*code)
                    .map(|leaf| implied_under(filter, &leaf.bounds))
                    .unwrap_or_default()
            });
            known.extend(leaves.iter().copied());
            implied[shard] = leaves.clone();
        }
        if !skipped.is_empty() && skipped.iter().all(|s| *s) {
            skipped[0] = false;
        }
        ShardMask {
            skipped,
            known: known.into_iter().collect(),
            implied,
        }
    }

    /// The tree to send to `shard`: `tree` with the leaves the shard's
    /// placement leaf implies removed ([`without_leaves`]); `None` when
    /// the shard's leaf implies the entire tree, which is a request with
    /// no filter on that shard.
    pub fn filter_for(&self, shard: usize, tree: &pb::FilterExpr) -> Option<pb::FilterExpr> {
        match self.implied.get(shard) {
            Some(dropped) if !dropped.is_empty() => without_leaves(tree, dropped),
            _ => Some(tree.clone()),
        }
    }

    pub fn skipped_count(&self) -> u32 {
        self.skipped.iter().filter(|s| **s).count() as u32
    }
}
