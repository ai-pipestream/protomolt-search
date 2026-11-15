//! Small merge primitives every composed route shares: known flags, slot
//! ranges, id routing, and checked counters.

use tonic::Status;

use crate::coordinator::RequestFilters;
use crate::pb::HealthResponse;

use super::health::{health_span, merge_health};

/// OR one child's per-column flags into the accumulator, which every
/// child must size the same way.
pub(super) fn merge_known(
    what: &str,
    shard: usize,
    acc: &mut Option<Vec<bool>>,
    share: &[bool],
) -> Result<(), Status> {
    match acc {
        None => *acc = Some(share.to_vec()),
        Some(flags) => or_flags(flags, share, shard, what)?,
    }
    Ok(())
}

/// Join a share into an optional accumulator: the first share is taken
/// as is, later shares go through `join`.
pub(super) fn join_flags<T: Clone>(
    acc: &mut Option<T>,
    share: &T,
    join: impl FnOnce(&mut T, &T) -> Result<(), Status>,
) -> Result<(), Status> {
    match acc {
        None => {
            *acc = Some(share.clone());
            Ok(())
        }
        Some(acc) => join(acc, share),
    }
}

/// OR `share` into `acc` position by position; a length mismatch is a
/// protocol break named by child.
pub(super) fn or_flags(
    acc: &mut [bool],
    share: &[bool],
    shard: usize,
    what: &str,
) -> Result<(), Status> {
    if acc.len() != share.len() {
        return Err(Status::internal(format!(
            "relay: child {shard} answered {} {what} flags while an earlier child answered {}",
            share.len(),
            acc.len()
        )));
    }
    or_into(acc, share);
    Ok(())
}

/// OR `share` into the prefix of `acc` it covers, without a length check.
pub(super) fn or_into(acc: &mut [bool], share: &[bool]) {
    for (a, k) in acc.iter_mut().zip(share) {
        *a |= *k;
    }
}

/// Which child holds a global id: the one whose slot range (from its
/// health report) contains it. Contiguity was checked at startup; a gap
/// that appeared since refuses by name here.
pub(super) fn child_of_id(ranges: &[(u64, u64)], id: u64) -> Option<usize> {
    ranges
        .iter()
        .position(|&(offset, span)| id >= offset && id - offset < span)
}

/// The known-column accumulator of one fan-out at this level: geo flags
/// ORed, filter-leaf flags ORed over the request's tree with each
/// child's answer mapped through the leaves its placement code implied
/// away (`ShardMask::implied`: the child was sent a shorter tree and
/// answers in that order), and the leaves the mask itself resolved
/// marked from the start (`ShardMask::known`).
pub(super) struct KnownFlags {
    pub(super) geo: Vec<bool>,
    pub(super) tree: Vec<bool>,
}

impl KnownFlags {
    pub(super) fn new(
        filters: &RequestFilters,
        mask: Option<&crate::placement::ShardMask>,
    ) -> Self {
        let leaves = filters.tree.as_ref().map_or(0, crate::filter::leaf_count);
        let mut tree = vec![false; leaves];
        if let Some(mask) = mask {
            for &leaf in &mask.known {
                if let Some(flag) = tree.get_mut(leaf) {
                    *flag = true;
                }
            }
        }
        KnownFlags {
            geo: vec![false; filters.geo.len()],
            tree,
        }
    }

    pub(super) fn merge(
        &mut self,
        child: usize,
        mask: Option<&crate::placement::ShardMask>,
        geo: &[bool],
        tree: &[bool],
    ) -> Result<(), Status> {
        if geo.len() != self.geo.len() {
            return Err(Status::internal(format!(
                "relay: child {child} answered {} geo-column flags for {} geo filters",
                geo.len(),
                self.geo.len()
            )));
        }
        or_into(&mut self.geo, geo);
        let dropped: &[usize] = mask
            .and_then(|m| m.implied.get(child))
            .map_or(&[], Vec::as_slice);
        let kept: Vec<usize> = (0..self.tree.len())
            .filter(|index| !dropped.contains(index))
            .collect();
        if tree.len() != kept.len() {
            return Err(Status::internal(format!(
                "relay: child {child} answered {} filter-leaf flags for the {} leaves it was \
                 sent (of {})",
                tree.len(),
                kept.len(),
                self.tree.len()
            )));
        }
        for (&index, k) in kept.iter().zip(tree) {
            self.tree[index] |= *k;
        }
        Ok(())
    }
}

/// The slot range of each child, `(offset, span)`, from the children's
/// health reports, with the contiguity rule applied: a gap or an overlap
/// refuses by name, because an id routed by these ranges, or a bitmap
/// laid out over them, would otherwise be wrong without a word.
pub(super) fn child_ranges(
    collection: &str,
    children: &[String],
    reports: &[HealthResponse],
) -> Result<Vec<(u64, u64)>, Status> {
    merge_health(collection, children, reports)?;
    Ok(reports
        .iter()
        .map(|r| (r.slot_offset, health_span(r)))
        .collect())
}

/// Sort ids into the child whose slot range holds each. An id in no
/// child's range is another shard's and is dropped, which is what a
/// node does with an id outside its own range (the boolean planner and
/// the exact rerank send every shard every candidate; the cascade routes
/// by shard, and a relay is one shard to it).
pub(super) fn route_ids(ranges: &[(u64, u64)], ids: &[u64]) -> Result<Vec<Vec<u64>>, Status> {
    let mut by_child: Vec<Vec<u64>> = vec![Vec::new(); ranges.len()];
    for &id in ids {
        if let Some(child) = child_of_id(ranges, id) {
            by_child[child].push(id);
        }
    }
    Ok(by_child)
}

/// Overflow-checked `u64` sum for a counter the children report.
pub(super) fn add_count(acc: &mut u64, share: u64, child: usize, what: &str) -> Result<(), Status> {
    *acc = acc.checked_add(share).ok_or_else(|| {
        Status::failed_precondition(format!("relay: child {child} overflows the summed {what}"))
    })?;
    Ok(())
}

/// Overflow-checked `u32` sum for a counter the children report.
pub(super) fn add_count32(
    acc: &mut u32,
    share: u32,
    child: usize,
    what: &str,
) -> Result<(), Status> {
    *acc = acc.checked_add(share).ok_or_else(|| {
        Status::failed_precondition(format!("relay: child {child} overflows the summed {what}"))
    })?;
    Ok(())
}

/// The segment counters every scan answer carries, summed with a check.
#[derive(Default, Clone, Copy)]
pub(super) struct SegmentCounts {
    pub(super) total: u32,
    pub(super) skipped: u32,
}

impl SegmentCounts {
    pub(super) fn add(&mut self, child: usize, total: u32, skipped: u32) -> Result<(), Status> {
        add_count32(&mut self.total, total, child, "segment count")?;
        add_count32(&mut self.skipped, skipped, child, "skipped segment count")
    }
}

/// Score descending, then id ascending: the order a node answers in.
pub(super) fn by_score_then_id(a: (f32, u64), b: (f32, u64)) -> std::cmp::Ordering {
    b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1))
}
