use super::tree_tests::{leaf, tree, two_level};
use super::*;
use crate::pb;

fn compiled(cel: &str) -> pb::FilterExpr {
    crate::cel::compile_filter(cel).unwrap().unwrap()
}

fn leaf_bounds(cel: &str) -> ColumnBounds {
    ColumnBounds::of_conjunction(&[compiled(cel)])
}

#[test]
fn a_leaf_implies_the_clauses_its_interval_and_facet_contain() {
    let recent = leaf_bounds("year >= 2015 && court == \"scotus\"");
    // Contained ranges on the AND spine are implied; the live clause
    // and anything under OR or NOT is not.
    let filter = compiled(
        "year >= 2010 && court in [\"scotus\", \"ca9\"] && has(year) && pages > 3 \
         && (year >= 2000 || court == \"x\") && !(year >= 2000)",
    );
    assert_eq!(implied_under(&filter, &recent), vec![0, 1, 2]);
    let sent = without_leaves(&filter, &[0, 1, 2]).unwrap();
    let mut names = Vec::new();
    crate::filter::walk_leaves(&sent, &mut |leaf| match leaf {
        crate::filter::LeafRef::Number(p) => names.push(p.column.clone()),
        crate::filter::LeafRef::Facet(p) => names.push(p.column.clone()),
        _ => names.push("other".into()),
    });
    assert_eq!(names, vec!["pages", "year", "court", "year"]);

    // Edges: an equal bound is implied unless the clause is strict
    // where the leaf is not; a bound the leaf lacks is never implied.
    assert_eq!(implied_under(&compiled("year >= 2015"), &recent), vec![0]);
    assert_eq!(
        implied_under(&compiled("year > 2015"), &recent),
        Vec::<usize>::new()
    );
    assert_eq!(implied_under(&compiled("year > 2014"), &recent), vec![0]);
    assert_eq!(
        implied_under(&compiled("year <= 2030"), &recent),
        Vec::<usize>::new()
    );
    // Two clauses on one column: the contained one goes, the other stays.
    assert_eq!(
        implied_under(&compiled("year >= 2010 && year < 2030"), &recent),
        vec![0]
    );
    let strict = leaf_bounds("year > 2015");
    assert_eq!(implied_under(&compiled("year >= 2015"), &strict), vec![0]);
    assert_eq!(implied_under(&compiled("year > 2015"), &strict), vec![0]);
    // A float clause against an integer interval compares exactly.
    assert_eq!(implied_under(&compiled("year >= 2014.5"), &recent), vec![0]);
    assert_eq!(
        implied_under(&compiled("year >= 2015.5"), &recent),
        Vec::<usize>::new()
    );
    // Facets: the admitted set must sit inside the clause's values.
    assert_eq!(
        implied_under(&compiled("court == \"ca9\""), &recent),
        Vec::<usize>::new()
    );
    assert_eq!(implied_under(&compiled("has(court)"), &recent), vec![0]);
    assert_eq!(
        implied_under(&compiled("has(pages)"), &recent),
        Vec::<usize>::new()
    );
    // A default leaf constrains nothing.
    let default = ColumnBounds::default();
    assert_eq!(
        implied_under(&compiled("year >= 1"), &default),
        Vec::<usize>::new()
    );
}

#[test]
fn stripping_leaves_keeps_the_shape_it_must() {
    let filter = compiled("year >= 2010 && court == \"scotus\"");
    // Both implied: no filter at all.
    assert_eq!(without_leaves(&filter, &[0, 1]), None);
    // One left: the AND unwraps to it.
    let one = without_leaves(&filter, &[0]).unwrap();
    assert!(matches!(one.expr, Some(pb::filter_expr::Expr::Facet(_))));
    // A single implied root is no filter.
    assert_eq!(without_leaves(&compiled("year >= 2010"), &[0]), None);
    // Nothing dropped: the tree as it was.
    assert_eq!(without_leaves(&filter, &[]), Some(filter.clone()));
    // Indices under OR are counted so later spine leaves keep their
    // numbers, and the OR itself is untouched.
    let mixed = compiled("(year >= 1 || court == \"a\") && year >= 2010");
    assert_eq!(implied_under(&mixed, &leaf_bounds("year >= 2015")), vec![2]);
    let sent = without_leaves(&mixed, &[2]).unwrap();
    assert!(matches!(sent.expr, Some(pb::filter_expr::Expr::Or(_))));
}

#[test]
fn the_mask_lists_implied_leaves_per_consulted_shard() {
    let mut recent = leaf("recent", Some("year >= 2015"));
    recent.children = vec![
        leaf("scotus", Some("court == \"scotus\"")),
        leaf("rest", None),
    ];
    let config = tree(vec![recent, leaf("other", None)]);
    let placement = Placement::validate(&config).unwrap();
    let code = |name: &str| Some(placement.leaf_by_name(name).unwrap().code);
    let codes = [
        code("recent.scotus"),
        code("recent.rest"),
        code("other"),
        None,
    ];
    let filter = compiled("year >= 2010 && has(court)");
    let mask = ShardMask::compute(&placement, &codes, &filter);
    assert_eq!(mask.skipped, vec![false; 4]);
    assert_eq!(mask.implied[0], vec![0, 1], "scotus pins both");
    assert_eq!(mask.implied[1], vec![0], "rest pins the year");
    assert!(mask.implied[2].is_empty(), "the default pins nothing");
    assert!(mask.implied[3].is_empty(), "no code, no claim");
    assert_eq!(mask.known, vec![0, 1]);
    assert_eq!(mask.filter_for(0, &filter), None);
    let rest = mask.filter_for(1, &filter).unwrap();
    assert!(matches!(rest.expr, Some(pb::filter_expr::Expr::Has(_))));
    assert_eq!(mask.filter_for(2, &filter), Some(filter.clone()));
    assert_eq!(mask.filter_for(3, &filter), Some(filter.clone()));
    // A skipped shard implies nothing; the exclusion is what counts.
    let excluded = ShardMask::compute(&placement, &codes, &compiled("year < 2000"));
    assert!(excluded.skipped[0] && excluded.skipped[1] && !excluded.skipped[2]);
    assert!(excluded.implied.iter().all(Vec::is_empty));
}

fn compile(cel: &str) -> pb::FilterExpr {
    crate::cel::compile_filter(cel).unwrap().unwrap()
}

#[test]
fn a_filter_the_path_cannot_hold_with_excludes_the_leaf() {
    let placement = two_level();
    let scotus = placement.leaf_by_name("recent.scotus").unwrap();
    let rest = placement.leaf_by_name("recent.rest").unwrap();
    let old = placement.leaf_by_name("old").unwrap();
    let other = placement.leaf_by_name("other").unwrap();
    let verdict = |leaf: &Leaf, cel: &str| impossible_under(&compile(cel), &leaf.bounds);
    // Ranges on the path's column.
    assert_eq!(verdict(scotus, "year < 2000"), Some(vec![0]));
    assert_eq!(verdict(scotus, "year < 2020"), Some(vec![0]));
    assert_eq!(verdict(scotus, "year <= 2020"), None);
    assert_eq!(verdict(scotus, "year < 2020.5"), None);
    assert_eq!(verdict(old, "year >= 2000"), Some(vec![0]));
    assert_eq!(verdict(old, "year > 1999.5"), None);
    assert_eq!(
        verdict(other, "year < 1900"),
        None,
        "the default carries no bound"
    );
    // Facet equality on the path.
    assert_eq!(verdict(scotus, "court == \"ca9\""), Some(vec![0]));
    assert_eq!(verdict(scotus, "court in [\"ca9\", \"scotus\"]"), None);
    assert_eq!(verdict(rest, "court == \"ca9\""), None);
    assert_eq!(verdict(scotus, "court.startsWith(\"ca\")"), Some(vec![0]));
    assert_eq!(verdict(scotus, "court < \"s\""), Some(vec![0]));
    // Connectives: AND takes one impossible child, OR needs all,
    // NOT never prunes, and indices follow the walk order.
    assert_eq!(verdict(scotus, "pages >= 1 && year < 2000"), Some(vec![1]));
    assert_eq!(
        verdict(scotus, "year < 2000 || court == \"ca9\""),
        Some(vec![0, 1])
    );
    assert_eq!(verdict(scotus, "year < 2000 || has(pages)"), None);
    assert_eq!(verdict(scotus, "!(year >= 2020)"), None);
    assert_eq!(verdict(scotus, "has(year)"), None);
    // The placement column itself is pinned per leaf.
    assert_eq!(
        verdict(scotus, &format!("placement > {}", scotus.code)),
        Some(vec![0])
    );
    let (lo, hi) = placement.subtree_range(&[1]);
    assert_eq!(
        verdict(scotus, &format!("placement >= {lo} && placement <= {hi}")),
        None
    );
    assert_eq!(verdict(old, &format!("placement >= {lo}")), Some(vec![0]));
    // An empty filter range excludes on its own.
    assert_eq!(verdict(other, "year > 5 && year < 5"), None);
    assert_eq!(verdict(other, "year == 5 && year == 6"), None);
}

#[test]
fn the_mask_keeps_one_shard_and_names_the_leaves_that_excluded() {
    let placement = two_level();
    let code = |name: &str| Some(placement.leaf_by_name(name).unwrap().code);
    let codes = vec![
        code("old"),
        code("recent.scotus"),
        code("recent.rest"),
        code("other"),
        None,
    ];
    let mask = ShardMask::compute(&placement, &codes, &compile("year >= 2020"));
    assert_eq!(mask.skipped, vec![true, false, false, false, false]);
    assert_eq!(mask.known, vec![0]);
    assert_eq!(mask.skipped_count(), 1);
    let mask = ShardMask::compute(
        &placement,
        &codes,
        &compile("year >= 2020 && court == \"ca9\""),
    );
    assert_eq!(mask.skipped, vec![true, true, false, false, false]);
    assert_eq!(mask.known, vec![0, 1]);
    // The unkeyed shard is never skipped, and one shard always stays.
    let two = vec![code("old"), code("recent.scotus")];
    let mask = ShardMask::compute(&placement, &two, &compile("year >= 2005 && year < 2010"));
    assert_eq!(mask.skipped, vec![false, true]);
}
