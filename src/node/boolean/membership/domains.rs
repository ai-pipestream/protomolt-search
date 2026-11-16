//! Filter-leaf domain narrowing over the planned tree.

use super::*;
use crate::boolean_bits::Bits;
use crate::pb::boolean_plan_leaf::Leaf;
use crate::pb::boolean_plan_node::Node;

/// `bound` intersected with every lexical leaf directly under the group's
/// MUST clauses.
fn narrow_by_lexical_must(
    req: &crate::pb::BooleanShardRequest,
    group: &crate::pb::BooleanPlanGroup,
    bound: Option<&Bits>,
    leaves: &[LeafSlot],
) -> Result<Option<Bits>, Status> {
    let mut narrowed = bound.cloned();
    for node in &group.must {
        if let Some(Node::Leaf(index)) = node.node.as_ref() {
            if !matches!(boolean_leaf(req, *index)?, Leaf::Lexical(_)) {
                continue;
            }
            let membership = &leaves[*index as usize]
                .done()
                .ok_or_else(|| {
                    Status::internal(format!(
                        "boolean lexical leaf {index} has no membership before the filter domains"
                    ))
                })?
                .membership;
            match &mut narrowed {
                Some(bits) => bits.and_with(membership),
                None => narrowed = Some(membership.clone()),
            }
        }
    }
    Ok(narrowed)
}

/// Record that filter leaf `index` is consulted on `domain`; a leaf shared
/// between positions covers the union, and an absent domain is whole-shard.
fn widen_filter_domain(
    domain: Option<&Bits>,
    index: usize,
    bounded: &mut [Option<Bits>],
    unbounded: &mut [bool],
) {
    match (domain, &mut bounded[index]) {
        (None, _) => unbounded[index] = true,
        (Some(domain), slot @ None) => *slot = Some(domain.clone()),
        (Some(domain), Some(bits)) => bits.or_with(domain),
    }
}

/// The slots a filter leaf's verdict can still decide, per leaf: a row
/// is out of a group the moment one MUST clause misses it, so a filter
/// leaf that appears under MUST beside a lexical sibling (the lexical
/// leaves resolve in the pre-pass, over the whole shard) is consulted
/// on that intersection and nowhere else. A dense sibling narrows no
/// filter: dense leaves resolve after the filters, so a filter that
/// empties its group never pays a dense membership. The bound narrows
/// down a MUST chain of groups; a SHOULD or MUST_NOT position keeps its
/// incoming bound — a minimum-should-match count reads every SHOULD
/// clause wherever the group can still admit, so no sibling narrows a
/// filter there. A leaf the tree reaches under MUST with no lexical
/// sibling and no ancestor bound stays whole-shard (`unbounded`), as
/// does one shared between a bounded and an unbounded position.
pub(in crate::node) fn filter_leaf_domains(
    req: &crate::pb::BooleanShardRequest,
    group: &crate::pb::BooleanPlanGroup,
    bound: Option<&Bits>,
    leaves: &[LeafSlot],
    bounded: &mut [Option<Bits>],
    unbounded: &mut [bool],
    depth: usize,
) -> Result<(), Status> {
    if depth > 64 {
        return Err(Status::invalid_argument(
            "Boolean plan exceeds the 64-level recursion limit",
        ));
    }
    let narrowed = narrow_by_lexical_must(req, group, bound, leaves)?;
    let must = group.must.iter().map(|node| (node, narrowed.as_ref()));
    // Sibling MUST clauses cannot narrow a SHOULD or MUST_NOT position, and
    // a shared leaf must cover its direct occurrence there too.
    let rest = group.should.iter().chain(&group.must_not);
    for (node, domain) in must.chain(rest.map(|node| (node, bound))) {
        match node.node.as_ref() {
            Some(Node::Group(child)) => {
                filter_leaf_domains(req, child, domain, leaves, bounded, unbounded, depth + 1)?;
            }
            Some(Node::Leaf(index)) => {
                if matches!(boolean_leaf(req, *index)?, Leaf::Filter(_)) {
                    widen_filter_domain(domain, *index as usize, bounded, unbounded);
                }
            }
            None => {
                return Err(Status::invalid_argument(
                    "Boolean plan node is neither a group nor a leaf",
                ))
            }
        }
    }
    Ok(())
}
