//! Boolean filter-leaf domain bounds.

use super::*;
use crate::boolean_bits::Bits;
use crate::pb::boolean_plan_leaf::Leaf;
use crate::pb::boolean_plan_node::Node;
use crate::pb::{
    BooleanPlanDense, BooleanPlanFilter, BooleanPlanGroup, BooleanPlanLeaf, BooleanPlanLexical,
    BooleanPlanNode, BooleanShardRequest,
};

fn leaf_node(index: u32) -> BooleanPlanNode {
    BooleanPlanNode {
        node: Some(Node::Leaf(index)),
    }
}

fn group_node(group: BooleanPlanGroup) -> BooleanPlanNode {
    BooleanPlanNode {
        node: Some(Node::Group(group)),
    }
}

fn group(
    must: Vec<BooleanPlanNode>,
    should: Vec<BooleanPlanNode>,
    must_not: Vec<BooleanPlanNode>,
    minimum_should_match: u32,
) -> BooleanPlanGroup {
    BooleanPlanGroup {
        must,
        should,
        must_not,
        minimum_should_match,
    }
}

fn lexical() -> BooleanPlanLeaf {
    BooleanPlanLeaf {
        leaf: Some(Leaf::Lexical(BooleanPlanLexical::default())),
    }
}

fn dense() -> BooleanPlanLeaf {
    BooleanPlanLeaf {
        leaf: Some(Leaf::Dense(BooleanPlanDense::default())),
    }
}

fn filter() -> BooleanPlanLeaf {
    BooleanPlanLeaf {
        leaf: Some(Leaf::Filter(BooleanPlanFilter::default())),
    }
}

fn of(len: usize, slots: &[usize]) -> LeafSlot {
    let mut membership = Bits::empty(len);
    for &slot in slots {
        membership.set(slot);
    }
    LeafSlot::Done(EvaluatedLeaf {
        membership,
        positive: true,
        filter_known: None,
        filter_recheck: None,
    })
}

/// The domains one plan walk assigns, in leaf order; `None` is the
/// whole shard.
fn domains(req: &BooleanShardRequest, leaves: &[LeafSlot]) -> Result<Vec<Option<Bits>>, Status> {
    let mut bounded: Vec<Option<Bits>> = vec![None; req.leaves.len()];
    let mut unbounded = vec![false; req.leaves.len()];
    let root = req.root.as_ref().expect("the test plans have a root");
    filter_leaf_domains(req, root, None, leaves, &mut bounded, &mut unbounded, 1)?;
    Ok(bounded
        .into_iter()
        .zip(unbounded)
        .map(|(domain, unbounded)| if unbounded { None } else { domain })
        .collect())
}

fn request(leaves: Vec<BooleanPlanLeaf>, root: BooleanPlanGroup) -> BooleanShardRequest {
    BooleanShardRequest {
        root: Some(root),
        leaves,
        ..Default::default()
    }
}

/// A MUST filter leaf beside an exact MUST sibling resolves over the
/// sibling's members; alone, or beside another filter only, it keeps
/// the whole shard.
#[test]
fn a_must_filter_is_bounded_by_exact_must_siblings_only() {
    let n = 100;
    let mut lex_bits = Bits::empty(n);
    for slot in [3, 4, 5] {
        lex_bits.set(slot);
    }
    let lex = of(n, &[3, 4, 5]);
    // MUST lexical + MUST filter: the filter takes the lexical set.
    let req = request(
        vec![lexical(), filter()],
        group(vec![leaf_node(0), leaf_node(1)], vec![], vec![], 0),
    );
    let leaves = vec![lex, LeafSlot::Todo];
    let assigned = domains(&req, &leaves).unwrap();
    assert_eq!(assigned, vec![None, Some(lex_bits)]);
    // A dense sibling narrows no filter: dense leaves resolve
    // after the filters, so a filter that empties its group never
    // pays a dense membership.
    let req = request(
        vec![dense(), filter()],
        group(vec![leaf_node(0), leaf_node(1)], vec![], vec![], 0),
    );
    let assigned = domains(&req, &leaves).unwrap();
    assert_eq!(assigned, vec![None, None]);
    // A lone MUST filter, or two filters without an exact sibling,
    // keep the whole shard.
    let req = request(vec![filter()], group(vec![leaf_node(0)], vec![], vec![], 0));
    assert_eq!(domains(&req, &[LeafSlot::Todo]).unwrap(), vec![None]);
    let req = request(
        vec![filter(), filter()],
        group(vec![leaf_node(0), leaf_node(1)], vec![], vec![], 0),
    );
    assert_eq!(
        domains(&req, &[LeafSlot::Todo, LeafSlot::Todo]).unwrap(),
        vec![None, None]
    );
}

/// The bound narrows down a MUST chain of groups; a SHOULD or
/// MUST_NOT position narrows no filter, and a leaf shared between a
/// bounded and an unbounded position is evaluated whole.
#[test]
fn the_bound_follows_must_edges_and_unions_over_occurrences() {
    let n = 100;
    let a = of(n, &[1, 2, 3]).done().unwrap().membership.clone();
    let b = of(n, &[2, 3, 4]).done().unwrap().membership.clone();
    // Nested: root MUST lexical + MUST group(MUST lexical + MUST
    // filter): the filter takes the intersection of both lexicals.
    let inner = group(vec![leaf_node(1), leaf_node(2)], vec![], vec![], 0);
    let req = request(
        vec![lexical(), lexical(), filter()],
        group(vec![leaf_node(0), group_node(inner)], vec![], vec![], 0),
    );
    let leaves = vec![of(n, &[1, 2, 3]), of(n, &[2, 3, 4]), LeafSlot::Todo];
    let assigned = domains(&req, &leaves).unwrap();
    let mut both = a.clone();
    both.and_with(&b);
    assert_eq!(assigned, vec![None, None, Some(both)]);
    // MUST_NOT and SHOULD filters keep the whole shard; their
    // subtrees keep only the incoming (here: no) bound.
    let inner = group(vec![leaf_node(2)], vec![], vec![], 0);
    let req = request(
        vec![lexical(), filter(), filter(), filter()],
        group(
            vec![leaf_node(0), leaf_node(1)],
            vec![group_node(inner)],
            vec![leaf_node(3)],
            1,
        ),
    );
    let leaves = vec![
        of(n, &[1, 2, 3]),
        LeafSlot::Todo,
        LeafSlot::Todo,
        LeafSlot::Todo,
    ];
    let assigned = domains(&req, &leaves).unwrap();
    assert_eq!(assigned, vec![None, Some(a.clone()), None, None]);
    // A leaf shared between two bounded positions takes the union.
    let left = group(vec![leaf_node(0), leaf_node(2)], vec![], vec![], 0);
    let right = group(vec![leaf_node(1), leaf_node(2)], vec![], vec![], 0);
    let req = request(
        vec![lexical(), lexical(), filter()],
        group(vec![group_node(left), group_node(right)], vec![], vec![], 0),
    );
    let leaves = vec![of(n, &[1, 2, 3]), of(n, &[2, 3, 4]), LeafSlot::Todo];
    let assigned = domains(&req, &leaves).unwrap();
    let mut union = a.clone();
    union.or_with(&b);
    assert_eq!(assigned, vec![None, None, Some(union)]);
    // A leaf shared between a bounded and an unbounded position is
    // whole-shard.
    let inner = group(vec![leaf_node(1)], vec![], vec![], 0);
    let req = request(
        vec![lexical(), filter()],
        group(
            vec![leaf_node(0), leaf_node(1)],
            vec![group_node(inner)],
            vec![],
            0,
        ),
    );
    let leaves = vec![of(n, &[1, 2, 3]), LeafSlot::Todo];
    let assigned = domains(&req, &leaves).unwrap();
    assert_eq!(assigned, vec![None, None]);
}

/// The recursion limit is the planner's own.
#[test]
fn the_domain_walk_keeps_the_recursion_limit() {
    let mut root = group(vec![leaf_node(0)], vec![], vec![], 0);
    for _ in 0..70 {
        root = group(vec![group_node(root)], vec![], vec![], 0);
    }
    let req = request(vec![lexical()], root);
    let err = domains(&req, &[of(8, &[1])]).unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}
