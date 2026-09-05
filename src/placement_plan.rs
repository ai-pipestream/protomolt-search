//! The placement dry run (`docs/placement.md`, "The dry run"): what a
//! proposed tree would do to the rows a cluster holds, computed from
//! filtered counts the shards already know how to answer.
//!
//! A row lands on a node when the node's predicate is TRUE and no
//! earlier sibling's predicate at any level of the path is TRUE (an
//! UNKNOWN sibling falls through, the same as FALSE). With Kleene
//! semantics that count is an exact difference of two filtered counts:
//!
//! ```text
//! first(n) = count(A) - count(A && B)
//! A = every non-default predicate on n's path, ANDed
//! B = every earlier sibling's predicate along the path, ORed
//! ```
//!
//! `A && B` is TRUE exactly when A is TRUE and some earlier sibling is
//! TRUE, so the difference counts the rows where A holds and no earlier
//! sibling does, UNKNOWN siblings included. A default node's count is
//! its parent's first-match count minus its non-default siblings'. The
//! rows that would stay are the same counts restricted to rows whose
//! placement column already carries the leaf's code.
//!
//! The counting itself (a fan-out of `AggregateShard` requests) lives in
//! the coordinator; this module builds the plan and the filter trees.

use crate::pb;
use crate::placement::Placement;

/// One node of the plan: the trees the counts need.
#[derive(Debug, Clone)]
pub struct PlanNode {
    /// Dotted path of names from the root.
    pub name: String,
    /// The leaf code, or the first code of the subtree for a node with
    /// children.
    pub code: i64,
    /// Every non-default predicate on the path, root first, this node's
    /// last.
    pub conjunction: Vec<pb::FilterExpr>,
    /// Every earlier sibling's predicate at every level of the path.
    pub earlier: Vec<pb::FilterExpr>,
    pub is_default: bool,
    pub children: Vec<PlanNode>,
}

impl PlanNode {
    pub fn is_leaf(&self) -> bool {
        self.children.is_empty()
    }
}

/// The plan for a validated tree: the root chain, in order.
pub fn plan(placement: &Placement) -> Result<Vec<PlanNode>, String> {
    walk(
        &placement.config().nodes,
        "",
        &[],
        &[],
        &[],
        placement.level_bits(),
    )
}

fn walk(
    chain: &[crate::placement::PlacementNodeConfig],
    prefix: &str,
    path: &[u32],
    conjunction: &[pb::FilterExpr],
    earlier: &[pb::FilterExpr],
    level_bits: u32,
) -> Result<Vec<PlanNode>, String> {
    let mut out = Vec::with_capacity(chain.len());
    let mut earlier_here: Vec<pb::FilterExpr> = Vec::new();
    for (index, node) in chain.iter().enumerate() {
        let name = node.name.trim();
        let full = if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{prefix}.{name}")
        };
        let cel = node.cel.as_deref().map(str::trim).unwrap_or("");
        let is_default = cel.is_empty();
        let mut conjunction_here = conjunction.to_vec();
        let mut own = None;
        if !is_default {
            let expr = crate::cel::compile_filter(cel)
                .map_err(|status| format!("placement: node {full:?}: {}", status.message()))?
                .ok_or_else(|| format!("placement: node {full:?} compiles to no filter"))?;
            conjunction_here.push(expr.clone());
            own = Some(expr);
        }
        let mut path_here = path.to_vec();
        path_here.push(index as u32);
        let mut all_earlier = earlier.to_vec();
        all_earlier.extend(earlier_here.iter().cloned());
        let children = if node.children.is_empty() {
            Vec::new()
        } else {
            walk(
                &node.children,
                &full,
                &path_here,
                &conjunction_here,
                &all_earlier,
                level_bits,
            )?
        };
        out.push(PlanNode {
            name: full,
            code: crate::placement::encode(&path_here, level_bits),
            conjunction: conjunction_here,
            earlier: all_earlier,
            is_default,
            children,
        });
        if let Some(expr) = own {
            earlier_here.push(expr);
        }
    }
    Ok(out)
}

/// `exprs` ANDed; `None` for an empty list.
pub fn and(exprs: Vec<pb::FilterExpr>) -> Option<pb::FilterExpr> {
    match exprs.len() {
        0 => None,
        1 => exprs.into_iter().next(),
        _ => Some(pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::And(pb::FilterList { exprs })),
        }),
    }
}

/// `exprs` ORed; `None` for an empty list.
pub fn or(exprs: Vec<pb::FilterExpr>) -> Option<pb::FilterExpr> {
    match exprs.len() {
        0 => None,
        1 => exprs.into_iter().next(),
        _ => Some(pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::Or(pb::FilterList { exprs })),
        }),
    }
}

/// `column == code` as a filter leaf.
pub fn code_equals(column: &str, code: i64) -> pb::FilterExpr {
    let bound = |value: i64| pb::FilterBound {
        value: Some(pb::filter_bound::Value::Int(value)),
        exclusive: false,
    };
    pb::FilterExpr {
        expr: Some(pb::filter_expr::Expr::Number(pb::NumberPredicate {
            column: column.to_string(),
            min: Some(bound(code)),
            max: Some(bound(code)),
        })),
    }
}

/// The two trees whose counts differ to `first(node)` under an extra
/// restriction: `(A && extra, A && B && extra)`. The second is `None`
/// when the node has no earlier sibling anywhere on its path, and the
/// first is `None` only for a root-level default with no restriction
/// (every row).
pub fn first_match_trees(
    node: &PlanNode,
    base: Option<&pb::FilterExpr>,
    extra: Option<&pb::FilterExpr>,
) -> (Option<pb::FilterExpr>, Option<pb::FilterExpr>) {
    let mut a: Vec<pb::FilterExpr> = Vec::new();
    a.extend(base.cloned());
    a.extend(extra.cloned());
    a.extend(node.conjunction.iter().cloned());
    let with_b = or(node.earlier.clone()).map(|b| {
        let mut ab = a.clone();
        ab.push(b);
        and(ab).expect("non-empty")
    });
    (and(a), with_b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::placement::{PlacementNodeConfig, PlacementTreeConfig};

    fn node(name: &str, cel: Option<&str>) -> PlacementNodeConfig {
        PlacementNodeConfig {
            name: name.into(),
            cel: cel.map(str::to_string),
            ..Default::default()
        }
    }

    #[test]
    fn the_plan_carries_ancestors_and_earlier_siblings() {
        let mut recent = node("recent", Some("year >= 2020"));
        recent.children = vec![
            node("scotus", Some("court == \"scotus\"")),
            node("rest", None),
        ];
        let config = PlacementTreeConfig {
            column: "placement".into(),
            level_bits: 0,
            nodes: vec![
                node("large", Some("pages >= 100")),
                recent,
                node("other", None),
            ],
        };
        let placement = Placement::validate(&config).unwrap();
        let plan = plan(&placement).unwrap();
        assert_eq!(plan.len(), 3);
        assert_eq!(plan[0].conjunction.len(), 1);
        assert!(plan[0].earlier.is_empty());
        let recent = &plan[1];
        assert_eq!(recent.conjunction.len(), 1);
        assert_eq!(recent.earlier.len(), 1, "large comes before it");
        let scotus = &recent.children[0];
        assert_eq!(scotus.conjunction.len(), 2, "recent and scotus");
        assert_eq!(scotus.earlier.len(), 1, "only large");
        let rest = &recent.children[1];
        assert!(rest.is_default);
        assert_eq!(rest.conjunction.len(), 1, "the parent's predicate only");
        assert_eq!(rest.earlier.len(), 2, "large, then scotus");
        assert!(plan[2].is_default && plan[2].conjunction.is_empty());
        assert_eq!(plan[2].earlier.len(), 2, "large and recent");
        assert_eq!(
            scotus.code,
            placement.leaf_by_name("recent.scotus").unwrap().code
        );

        let (a, ab) = first_match_trees(scotus, None, None);
        assert!(a.is_some() && ab.is_some());
        let (a, ab) = first_match_trees(&plan[0], None, None);
        assert!(
            a.is_some() && ab.is_none(),
            "nothing precedes the first node"
        );
        let (a, ab) = first_match_trees(&plan[2], None, None);
        assert!(
            a.is_none() && ab.is_some(),
            "the root default counts all rows minus the earlier"
        );
        let extra = code_equals("placement", 7);
        let (a, _) = first_match_trees(&plan[2], None, Some(&extra));
        assert_eq!(a, Some(extra));
    }
}
