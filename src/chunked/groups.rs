//! Grouping a batch's queries into shared kernel calls.

use super::*;

/// Queries that share one kernel call: the same allowlist, and their
/// vectors laid out back to back.
///
/// Queries sharing an allowlist share a kernel call; queries with
/// DIFFERENT allowlists must not. The floor is safe to share because
/// a lower floor only collects MORE, but the mask is not: the kernel
/// returns each query's top `chunk_k` over the masked set, so a query
/// batched under a union mask could have its whole chunk quota filled
/// by documents its own filter rejects while its real candidates sit
/// below the cut. Grouping keeps every kernel call's mask exactly the
/// allowlist of every query in it, which is the unfiltered exactness
/// argument unchanged. Unfiltered queries all share the `None` group.
pub(super) struct KernelGroup<'q> {
    pub(super) members: Vec<usize>,
    /// The group's allowlist as its queries hold it; the scan reads it a
    /// chunk at a time.
    pub(super) allow: Option<Mask<'q>>,
    pub(super) flat: Vec<f32>,
}

impl<'q> KernelGroup<'q> {
    pub(super) fn partition(queries: &[BatchQuery<'q>]) -> Vec<Self> {
        group_by_allow(queries)
            .into_iter()
            .map(|members| {
                let allow = queries[members[0]].allow;
                let mut flat: Vec<f32> =
                    Vec::with_capacity(members.iter().map(|&qi| queries[qi].vector.len()).sum());
                for &qi in &members {
                    flat.extend_from_slice(queries[qi].vector);
                }
                Self {
                    members,
                    allow,
                    flat,
                }
            })
            .collect()
    }
}

/// A mask's shape and identity (kind, address, length) beside the plan's
/// selection controls.
type GroupKey = (Option<(u8, usize, usize)>, crate::vector::SelectionControls);

/// Partition query positions into groups that may share a kernel call:
/// same selection controls and the same allowlist in the same shape, compared by identity (address and length) rather than
/// contents. Two callers holding equal-but-distinct allowlists land in
/// separate groups, which costs a kernel pass and never costs
/// correctness. Order is preserved so a single-group batch runs exactly
/// the sequence it ran before allowlists existed.
fn group_by_allow(queries: &[BatchQuery<'_>]) -> Vec<Vec<usize>> {
    let key = |a: Option<Mask<'_>>| {
        a.map(|mask| match mask {
            Mask::Bools(s) => (0u8, s.as_ptr() as usize, s.len()),
            Mask::Bits(bits) => (1u8, std::ptr::from_ref(bits) as usize, bits.len()),
            Mask::All(len) => (2u8, 0usize, len),
        })
    };
    let mut keys: Vec<GroupKey> = Vec::new();
    let mut groups: Vec<Vec<usize>> = Vec::new();
    for (qi, q) in queries.iter().enumerate() {
        // Two plans never share a kernel call merely because their masks match.
        let k = (key(q.allow), q.controls);
        match keys.iter().position(|existing| *existing == k) {
            Some(gi) => groups[gi].push(qi),
            None => {
                keys.push(k);
                groups.push(vec![qi]);
            }
        }
    }
    groups
}
