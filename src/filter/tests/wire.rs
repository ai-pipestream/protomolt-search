//! The wire tree: shape validation, the caps, and the leaf walk order.

use super::*;

fn leaf(column: &str) -> pb::FilterExpr {
    pb::FilterExpr {
        expr: Some(pb::filter_expr::Expr::Facet(pb::FacetPredicate {
            column: column.into(),
            values: vec!["v".into()],
        })),
    }
}

/// Shape validation: refusals for the empty node, empty
/// connectives, bound-less numbers, valueless facets, non-finite
/// bounds, and the depth and leaf caps.
#[test]
fn validation_refuses_by_name() {
    let refused = |e: &pb::FilterExpr, needle: &str| {
        let err = validate_filter(e).expect_err(needle);
        assert!(
            err.message().contains(needle),
            "{needle:?} not in {:?}",
            err.message()
        );
    };
    refused(&pb::FilterExpr { expr: None }, "no expression set");
    refused(
        &pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::And(pb::FilterList { exprs: vec![] })),
        },
        "no children",
    );
    refused(
        &pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::Facet(pb::FacetPredicate {
                column: "c".into(),
                values: vec![],
            })),
        },
        "at least one value",
    );
    refused(
        &pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::Number(pb::NumberPredicate {
                column: "n".into(),
                min: None,
                max: None,
            })),
        },
        "at least one bound",
    );
    refused(
        &pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::Number(pb::NumberPredicate {
                column: "n".into(),
                min: Some(pb::FilterBound {
                    value: Some(pb::filter_bound::Value::Num(f64::NAN)),
                    exclusive: false,
                }),
                max: None,
            })),
        },
        "not finite",
    );
    refused(
        &pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::Number(pb::NumberPredicate {
                column: "n".into(),
                min: Some(pb::FilterBound {
                    value: None,
                    exclusive: false,
                }),
                max: None,
            })),
        },
        "no value set",
    );
    refused(
        &pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::Has(pb::HasPredicate {
                column: String::new(),
            })),
        },
        "names the column",
    );
}

fn refused(e: &pb::FilterExpr, needle: &str) {
    let err = validate_filter(e).expect_err(needle);
    assert!(
        err.message().contains(needle),
        "{needle:?} not in {:?}",
        err.message()
    );
}

/// The depth and leaf caps refuse one past the cap and admit the cap.
#[test]
fn validation_caps_depth_and_leaves() {
    // Depth: a chain of NOTs one past the cap.
    let mut deep = leaf("c");
    for _ in 0..MAX_DEPTH {
        deep = pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::Not(Box::new(deep))),
        };
    }
    refused(&deep, "deeper than");

    // Leaves: one past the cap under a single AND.
    let wide = pb::FilterExpr {
        expr: Some(pb::filter_expr::Expr::And(pb::FilterList {
            exprs: (0..=MAX_LEAVES).map(|_| leaf("c")).collect(),
        })),
    };
    refused(&wide, "leaves");

    // And the legal versions of each cap pass.
    let mut ok = leaf("c");
    for _ in 0..MAX_DEPTH - 1 {
        ok = pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::Not(Box::new(ok))),
        };
    }
    validate_filter(&ok).expect("depth at the cap is legal");
    let ok_wide = pb::FilterExpr {
        expr: Some(pb::filter_expr::Expr::And(pb::FilterList {
            exprs: (0..MAX_LEAVES).map(|_| leaf("c")).collect(),
        })),
    };
    validate_filter(&ok_wide).expect("leaf count at the cap is legal");
}

/// The positional contract: leaves surface in depth-first,
/// left-to-right order, the order both sides of the
/// `filter_columns_known` handshake derive independently.
#[test]
fn walk_order_is_depth_first() {
    let tree = pb::FilterExpr {
        expr: Some(pb::filter_expr::Expr::And(pb::FilterList {
            exprs: vec![
                leaf("a"),
                pb::FilterExpr {
                    expr: Some(pb::filter_expr::Expr::Or(pb::FilterList {
                        exprs: vec![
                            leaf("b"),
                            pb::FilterExpr {
                                expr: Some(pb::filter_expr::Expr::Not(Box::new(leaf("c")))),
                            },
                        ],
                    })),
                },
                leaf("d"),
            ],
        })),
    };
    let mut seen = Vec::new();
    walk_leaves(&tree, &mut |l| {
        if let LeafRef::Facet(p) = l {
            seen.push(p.column.clone());
        }
    });
    assert_eq!(seen, ["a", "b", "c", "d"]);
    assert_eq!(leaf_count(&tree), 4);
}
