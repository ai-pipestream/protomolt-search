use super::*;
use crate::filter::Tri;
use crate::pb;

pub(super) fn leaf(name: &str, cel: Option<&str>) -> PlacementNodeConfig {
    PlacementNodeConfig {
        name: name.into(),
        cel: cel.map(str::to_string),
        ..Default::default()
    }
}

pub(super) fn tree(nodes: Vec<PlacementNodeConfig>) -> PlacementTreeConfig {
    PlacementTreeConfig {
        column: "placement".into(),
        level_bits: 0,
        nodes,
    }
}

#[test]
fn codes_are_prefix_ranges() {
    let bits = 9;
    assert_eq!(encode(&[], bits), 0);
    assert_eq!(encode(&[0], bits), 0);
    assert_eq!(encode(&[1], bits), 1 << 54);
    assert_eq!(encode(&[1, 2], bits), (1 << 54) | (2 << 45));
    let (lo, hi) = subtree_range(&[1], bits);
    assert_eq!(lo, 1 << 54);
    assert_eq!(hi, (2 << 54) - 1);
    let (lo2, hi2) = subtree_range(&[1, 2], bits);
    assert!(lo <= lo2 && hi2 <= hi);
    assert_eq!(hi2 - lo2 + 1, 1 << 45);
    assert_eq!(index_at(encode(&[3, 7, 1], bits), 1, bits), 7);
    assert_eq!(index_at(encode(&[3, 7, 1], bits), 2, bits), 1);
    assert!(encode(&[511, 511, 511, 511, 511, 511, 511], bits) >= 0);
    let (_, whole_hi) = subtree_range(&[], bits);
    assert_eq!(whole_hi, i64::MAX);
}

#[test]
fn a_valid_tree_lists_its_leaves_in_code_order() {
    let mut recent = leaf("recent", Some("year >= 2020"));
    recent.children = vec![
        leaf("scotus", Some("court == \"scotus\"")),
        leaf("rest", None),
    ];
    let config = tree(vec![
        leaf("large", Some("body_bytes >= 65536")),
        recent,
        leaf("other", None),
    ]);
    let placement = Placement::validate(&config).unwrap();
    let names: Vec<&str> = placement.leaves().iter().map(|l| l.name.as_str()).collect();
    assert_eq!(names, ["large", "recent.scotus", "recent.rest", "other"]);
    let codes: Vec<i64> = placement.leaves().iter().map(|l| l.code).collect();
    assert!(codes.windows(2).all(|w| w[0] < w[1]));
    assert_eq!(placement.depth(), 2);
    let scotus = placement.leaf_by_name("recent.scotus").unwrap();
    assert_eq!(scotus.own.len(), 2, "both predicates on the path");
    assert!(!scotus.is_default);
    let rest = placement.leaf_by_name("recent.rest").unwrap();
    assert_eq!(rest.own.len(), 1, "only the parent's predicate");
    assert!(rest.is_default);
    let (lo, hi) = placement.subtree_range(&[1]);
    assert!(lo <= scotus.code && rest.code <= hi);
    assert!(placement.leaf_by_name("large").unwrap().code < lo);
    assert_eq!(
        placement.leaf_by_code(rest.code).unwrap().name,
        "recent.rest"
    );
    let round = PlacementTreeConfig::from_proto(&config.to_proto());
    assert_eq!(round, config);
}

#[test]
fn refusals_name_the_node() {
    let no_default = tree(vec![leaf("a", Some("year >= 1"))]);
    let err = Placement::validate(&no_default).unwrap_err();
    assert!(err.contains("no default"), "{err}");

    let default_first = tree(vec![leaf("d", None), leaf("a", Some("year >= 1"))]);
    let err = Placement::validate(&default_first).unwrap_err();
    assert!(err.contains("\"d\"") && err.contains("last"), "{err}");

    let value_dialect = tree(vec![leaf("a", Some("year + 1 >= 2")), leaf("d", None)]);
    let err = Placement::validate(&value_dialect).unwrap_err();
    assert!(err.contains("\"a\""), "{err}");

    let repeated = tree(vec![
        leaf("a", Some("year >= 1")),
        leaf("a", Some("year >= 2")),
        leaf("d", None),
    ]);
    let err = Placement::validate(&repeated).unwrap_err();
    assert!(err.contains("repeats"), "{err}");

    let mut sized_parent = leaf("p", Some("year >= 1"));
    sized_parent.shards = 2;
    sized_parent.children = vec![leaf("d", None)];
    let err = Placement::validate(&tree(vec![sized_parent, leaf("d", None)])).unwrap_err();
    assert!(err.contains("children and also shards"), "{err}");

    let mut deep = tree(vec![leaf("a", Some("year >= 1")), leaf("d", None)]);
    deep.level_bits = 32;
    let mut inner = leaf("b", Some("year >= 2"));
    inner.children = vec![leaf("d", None)];
    deep.nodes[0].children = vec![inner, leaf("d", None)];
    let err = Placement::validate(&deep).unwrap_err();
    assert!(err.contains("exceeds"), "{err}");

    let mut wide = tree(
        (0..4)
            .map(|i| leaf(&format!("n{i}"), Some("year >= 1")))
            .collect(),
    );
    wide.nodes.push(leaf("d", None));
    wide.level_bits = 2;
    let err = Placement::validate(&wide).unwrap_err();
    assert!(err.contains("exceed"), "{err}");

    let empty = tree(vec![]);
    assert!(Placement::validate(&empty)
        .unwrap_err()
        .contains("no nodes"));
}

fn doc(court: Option<&str>, year: Option<i64>, score: Option<f64>) -> pb::AddDocumentsRequest {
    pb::AddDocumentsRequest {
        text: "t".into(),
        facets: court
            .map(|c| {
                vec![pb::FacetValue {
                    field: "court".into(),
                    value: c.into(),
                }]
            })
            .unwrap_or_default(),
        integers: year
            .map(|y| {
                vec![pb::IntegerValue {
                    field: "year".into(),
                    value: y,
                }]
            })
            .unwrap_or_default(),
        numerics: score
            .map(|v| {
                vec![pb::NumericValue {
                    field: "score".into(),
                    value: v,
                }]
            })
            .unwrap_or_default(),
        ..Default::default()
    }
}

pub(super) fn two_level() -> Placement {
    let mut recent = leaf("recent", Some("year >= 2020"));
    recent.children = vec![
        leaf("scotus", Some("court == \"scotus\"")),
        leaf("rest", None),
    ];
    Placement::validate(&tree(vec![
        leaf("old", Some("year < 2000")),
        recent,
        leaf("other", None),
    ]))
    .unwrap()
}

#[test]
fn a_document_takes_the_first_match_and_unknown_falls_through() {
    let placement = two_level();
    let name = |d: &pb::AddDocumentsRequest| placement.evaluate(d).unwrap().name.clone();
    assert_eq!(name(&doc(Some("ca9"), Some(1990), None)), "old");
    assert_eq!(
        name(&doc(Some("scotus"), Some(2021), None)),
        "recent.scotus"
    );
    assert_eq!(name(&doc(Some("ca9"), Some(2021), None)), "recent.rest");
    assert_eq!(
        name(&doc(None, Some(2021), None)),
        "recent.rest",
        "unknown court"
    );
    assert_eq!(name(&doc(Some("scotus"), Some(2010), None)), "other");
    assert_eq!(
        name(&doc(Some("scotus"), None, None)),
        "other",
        "unknown year"
    );
    // A timestamp is an integer column at evaluation time.
    let mut ts = doc(None, None, None);
    ts.timestamps.push(pb::TimestampValue {
        field: "year".into(),
        value: Some(prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        }),
    });
    assert_eq!(name(&ts), "recent.rest");
}

#[test]
fn document_evaluation_matches_the_shard_rules() {
    let columns_doc = doc(Some("ca9"), Some(2001), Some(0.5));
    let columns = DocColumns::of(&columns_doc).unwrap();
    let tri = |cel: &str| {
        let expr = crate::cel::compile_filter(cel).unwrap().unwrap();
        eval_document(&expr, &columns)
    };
    assert_eq!(tri("year >= 2001"), Tri::True);
    assert_eq!(tri("year > 2001"), Tri::False);
    assert_eq!(tri("year > 2000.5"), Tri::True);
    assert_eq!(tri("score < 0.5"), Tri::False);
    assert_eq!(tri("score <= 0.5"), Tri::True);
    assert_eq!(tri("court == \"ca9\""), Tri::True);
    assert_eq!(tri("court in [\"ca5\", \"scotus\"]"), Tri::False);
    assert_eq!(tri("!(court == \"ca9\")"), Tri::False);
    assert_eq!(tri("pages >= 1"), Tri::Unknown);
    assert_eq!(tri("!(pages >= 1)"), Tri::Unknown);
    assert_eq!(tri("has(pages)"), Tri::False);
    assert_eq!(tri("has(court)"), Tri::True);
    assert_eq!(tri("\"k\" in tags"), Tri::False);
    assert_eq!(tri("court.startsWith(\"ca\")"), Tri::True);
    assert_eq!(tri("court < \"cb\" && court >= \"ca9\""), Tri::True);
    assert_eq!(tri("court > \"ca9\""), Tri::False);
    assert_eq!(tri("pages >= 1 || year == 2001"), Tri::True);
    assert_eq!(tri("pages >= 1 && year == 2001"), Tri::Unknown);
}
