use super::compile_filter;
use super::timestamp::parse_rfc3339_micros;
use crate::pb;

fn compiled(text: &str) -> pb::FilterExpr {
    compile_filter(text)
        .unwrap_or_else(|e| panic!("{text:?} refused: {}", e.message()))
        .expect("non-empty filter")
}

fn refused(text: &str, needle: &str) {
    let err = compile_filter(text).expect_err(needle);
    assert!(
        err.message().contains(needle),
        "{text:?}: {needle:?} not in {:?}",
        err.message()
    );
}

fn facet(column: &str, values: &[&str]) -> pb::FilterExpr {
    pb::FilterExpr {
        expr: Some(pb::filter_expr::Expr::Facet(pb::FacetPredicate {
            column: column.into(),
            values: values.iter().map(|v| v.to_string()).collect(),
        })),
    }
}

/// The string constructs, checked structurally against the hand-built
/// tree.
#[test]
fn compiles_string_equality_and_membership() {
    assert_eq!(compile_filter("").expect("empty is no filter"), None);
    assert_eq!(compile_filter("  \n ").expect("blank is no filter"), None);

    assert_eq!(compiled("court == \"scotus\""), facet("court", &["scotus"]));
    assert_eq!(compiled("'scotus' == court"), facet("court", &["scotus"]));
    assert_eq!(
        compiled("court != \"scotus\""),
        pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::Not(Box::new(facet(
                "court",
                &["scotus"]
            )))),
        }
    );
    assert_eq!(
        compiled("court in [\"scotus\", \"ca9\"]"),
        facet("court", &["scotus", "ca9"])
    );
    // Dotted identifiers are flat column names here.
    assert_eq!(
        compiled("meta.court == \"scotus\""),
        facet("meta.court", &["scotus"])
    );
}

/// Numeric bounds, checked structurally against the hand-built tree.
#[test]
fn compiles_numeric_bounds() {
    let year_ge = pb::FilterExpr {
        expr: Some(pb::filter_expr::Expr::Number(pb::NumberPredicate {
            column: "year".into(),
            min: Some(pb::FilterBound {
                value: Some(pb::filter_bound::Value::Int(1990)),
                exclusive: false,
            }),
            max: None,
        })),
    };
    assert_eq!(compiled("year >= 1990"), year_ge);
    assert_eq!(compiled("1990 <= year"), year_ge, "mirrored literal-first");
    assert_eq!(
        compiled("score < 2.5"),
        pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::Number(pb::NumberPredicate {
                column: "score".into(),
                min: None,
                max: Some(pb::FilterBound {
                    value: Some(pb::filter_bound::Value::Num(2.5)),
                    exclusive: true,
                }),
            })),
        }
    );
    // Equality is the two-sided inclusive point range.
    assert_eq!(
        compiled("year == 1990"),
        pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::Number(pb::NumberPredicate {
                column: "year".into(),
                min: Some(pb::FilterBound {
                    value: Some(pb::filter_bound::Value::Int(1990)),
                    exclusive: false,
                }),
                max: Some(pb::FilterBound {
                    value: Some(pb::filter_bound::Value::Int(1990)),
                    exclusive: false,
                }),
            })),
        }
    );
    // i64::MIN folds through the unary minus.
    assert!(matches!(
        compiled("year == -9223372036854775808").expr,
        Some(pb::filter_expr::Expr::Number(p))
            if p.min == Some(pb::FilterBound {
                value: Some(pb::filter_bound::Value::Int(i64::MIN)),
                exclusive: false,
            })
    ));
}

/// Map entries and presence, checked structurally against the hand-built
/// tree.
#[test]
fn compiles_map_and_presence_predicates() {
    assert_eq!(
        compiled("tags[\"color\"] == \"red\""),
        pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::MapFacet(pb::MapFacetPredicate {
                column: "tags".into(),
                key: "color".into(),
                values: vec!["red".into()],
            })),
        }
    );
    assert_eq!(
        compiled("cites[\"410 U.S. 113\"] >= 3"),
        pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::MapNumber(pb::MapNumberPredicate {
                column: "cites".into(),
                key: "410 U.S. 113".into(),
                min: Some(pb::FilterBound {
                    value: Some(pb::filter_bound::Value::Int(3)),
                    exclusive: false,
                }),
                max: None,
            })),
        }
    );
    assert_eq!(
        compiled("\"color\" in tags"),
        pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::MapHasKey(pb::MapKeyPredicate {
                column: "tags".into(),
                key: "color".into(),
            })),
        }
    );
    assert_eq!(
        compiled("has(court)"),
        pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::Has(pb::HasPredicate {
                column: "court".into(),
            })),
        }
    );
}

/// Numeric membership, geo sugar, and timestamps.
#[test]
fn compiles_numeric_membership_geo_and_timestamps() {
    // Numeric membership is a disjunction of point ranges.
    let year_in = compiled("year in [1990, 1995]");
    match &year_in.expr {
        Some(pb::filter_expr::Expr::Or(list)) => assert_eq!(list.exprs.len(), 2),
        other => panic!("expected Or of point ranges, got {other:?}"),
    }

    // Geo sugar compiles to the existing GeoFilter leaf.
    assert_eq!(
        compiled("within_radius(courthouse, 38.9, -77.0, 25000)"),
        pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::Geo(pb::GeoFilter {
                column: "courthouse".into(),
                region: Some(pb::geo_filter::Region::Radius(pb::GeoRadius {
                    lat: 38.9,
                    lon: -77.0,
                    meters: 25000.0,
                    metric: pb::GeoMetric::Haversine.into(),
                })),
            })),
        }
    );
    assert!(matches!(
        compiled("within_bbox(courthouse, 30.0, 40.0, -80.0, -70.0)").expr,
        Some(pb::filter_expr::Expr::Geo(_))
    ));

    // timestamp() becomes an integer bound in epoch micros.
    assert!(matches!(
        compiled("decided >= timestamp(\"2015-01-01T00:00:00Z\")").expr,
        Some(pb::filter_expr::Expr::Number(p))
            if p.min == Some(pb::FilterBound {
                value: Some(pb::filter_bound::Value::Int(1_420_070_400_000_000)),
                exclusive: false,
            })
    ));
}

/// CEL precedence: `||` binds loosest, then `&&`, then relations,
/// then `!`; parentheses override.
#[test]
fn precedence_matches_cel() {
    let e = compiled("a == \"x\" || b == \"y\" && c == \"z\"");
    match &e.expr {
        Some(pb::filter_expr::Expr::Or(or)) => {
            assert_eq!(or.exprs.len(), 2);
            assert_eq!(or.exprs[0], facet("a", &["x"]));
            match &or.exprs[1].expr {
                Some(pb::filter_expr::Expr::And(and)) => {
                    assert_eq!(and.exprs[0], facet("b", &["y"]));
                    assert_eq!(and.exprs[1], facet("c", &["z"]));
                }
                other => panic!("expected And on the right, got {other:?}"),
            }
        }
        other => panic!("expected Or at the top, got {other:?}"),
    }
    let e = compiled("(a == \"x\" || b == \"y\") && c == \"z\"");
    assert!(
        matches!(&e.expr, Some(pb::filter_expr::Expr::And(_))),
        "parentheses hoist the Or under the And"
    );
    let e = compiled("!(a == \"x\") && b == \"y\"");
    match &e.expr {
        Some(pb::filter_expr::Expr::And(and)) => {
            assert!(matches!(
                &and.exprs[0].expr,
                Some(pb::filter_expr::Expr::Not(_))
            ));
        }
        other => panic!("expected And, got {other:?}"),
    }
    // A chain of three folds into one connective, not a skewed
    // tree — depth budget is spent on the user's nesting only.
    let e = compiled("a == \"x\" && b == \"y\" && c == \"z\"");
    match &e.expr {
        Some(pb::filter_expr::Expr::And(and)) => assert_eq!(and.exprs.len(), 3),
        other => panic!("expected flat And, got {other:?}"),
    }
}

fn string_range(
    column: &str,
    key: Option<&str>,
    min: Option<(&str, bool)>,
    max: Option<(&str, bool)>,
) -> pb::FilterExpr {
    let bound = |b: Option<(&str, bool)>| {
        b.map(|(value, exclusive)| pb::StringBound {
            value: value.into(),
            exclusive,
        })
    };
    pb::FilterExpr {
        expr: Some(pb::filter_expr::Expr::StringRange(
            pb::StringRangePredicate {
                column: column.into(),
                map_key: key.map(str::to_owned),
                min: bound(min),
                max: bound(max),
            },
        )),
    }
}

/// String ordering and prefixes compile to dictionary ranges; the mirrored forms orient the column.
#[test]
fn string_ordering_and_prefixes_compile_to_dictionary_ranges() {
    assert_eq!(
        compiled("court < \"b\""),
        string_range("court", None, None, Some(("b", true)))
    );
    assert_eq!(
        compiled("court <= \"b\""),
        string_range("court", None, None, Some(("b", false)))
    );
    assert_eq!(
        compiled("court > \"b\""),
        string_range("court", None, Some(("b", true)), None)
    );
    assert_eq!(
        compiled("\"b\" <= court"),
        string_range("court", None, Some(("b", false)), None)
    );
    assert_eq!(
        compiled("tags[\"color\"] >= \"m\""),
        string_range("tags", Some("color"), Some(("m", false)), None)
    );
    assert_eq!(
        compiled("court.startsWith(\"ca\")"),
        pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::StringPrefix(
                pb::StringPrefixPredicate {
                    column: "court".into(),
                    map_key: None,
                    prefix: "ca".into(),
                }
            )),
        }
    );
    assert_eq!(
        compiled("tags[\"color\"].startsWith(\"re\")"),
        pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::StringPrefix(
                pb::StringPrefixPredicate {
                    column: "tags".into(),
                    map_key: Some("color".into()),
                    prefix: "re".into(),
                }
            )),
        }
    );
}

/// Everything outside the vocabulary refuses BY NAME.
#[test]
fn refusals_name_the_construct() {
    refused("year + 1 > 1990", "arithmetic (`+`)");
    refused("year - 1 > 1990", "arithmetic (`-`)");
    refused("year * 2 > 1990", "arithmetic (`*`)");
    refused("court == \"a\" ? has(x) : has(y)", "ternary");
    refused("court.matches(\"sco.*\")", "matches() (regex)");
    refused("court.endsWith(\"sco\")", "endsWith()");
    refused("court.contains(\"sco\")", "contains()");
    refused("court.startsWith(\"\")", "matches every value");
    refused("court.startsWith(year)", "string literal prefix");
    refused("tags.all(t, t == \"x\")", "all() macro");
    refused("size(court) > 2", "size()");
    refused("int(year) == 1990", "int() conversion");
    refused("court == name", "cross-column");
    refused("1 == 1", "constant comparison");
    refused("court", "bare column");
    refused("true", "boolean literal");
    refused("court == true", "boolean literal");
    refused("court == null", "null");
    refused("year in []", "empty list");
    refused("year in [1990, \"x\"]", "mixes strings and numbers");
    refused("has(tags[\"color\"])", "'color' in tags");
    refused("-court == \"x\"", "unary minus on a non-literal");
    refused("court == 'a' 'b'", "trailing input");
    refused("r\"raw\" == court", "raw string literal");
    refused("b'bytes' == court", "bytes string literal");
    refused("5.0u == year", "uint suffix");
    refused("court = \"x\"", "single `=`");
    refused("a && (b == \"x\"", "expected `)`");
    refused("m[court] == \"x\"", "string literal key");
    refused("duration(\"1h\") == x", "duration()");
    refused("frobnicate(court)", "not in the compiled vocabulary");
    refused("year == 9223372036854775808", "out of i64 range");
    refused("court == \"\\q\"", "unsupported string escape");
    refused("year >= timestamp(\"2015-13-01T00:00:00Z\")", "month");
    refused(
        "year >= timestamp(\"2015-02-29T00:00:00Z\")",
        "day is outside",
    );
    refused("year >= timestamp(\"2015-06-30T23:59:60Z\")", "leap second");
    refused("year >= timestamp(\"2015-06-30\")", "expected 'T'");
    // A refusal inside a nested expression still surfaces.
    refused("a == \"x\" && (year + 1 > 2)", "arithmetic");
}

/// The exact instants the integer math must produce: the epoch,
/// a leap day, a negative-offset instant, fractional flooring, and
/// a pre-epoch date.
#[test]
fn rfc3339_micros_are_exact() {
    let us = |s: &str| parse_rfc3339_micros(s).expect(s);
    assert_eq!(us("1970-01-01T00:00:00Z"), 0);
    assert_eq!(us("2015-01-01T00:00:00Z"), 1_420_070_400_000_000);
    assert_eq!(us("2016-02-29T12:00:00Z"), 1_456_747_200_000_000);
    // -05:00 means five hours LATER in UTC.
    assert_eq!(
        us("2015-01-01T00:00:00-05:00"),
        1_420_070_400_000_000 + 5 * 3600 * 1_000_000
    );
    assert_eq!(
        us("2015-01-01T00:00:00+05:30"),
        1_420_070_400_000_000 - (5 * 3600 + 30 * 60) * 1_000_000
    );
    // Micros survive; the seventh digit floors away.
    assert_eq!(us("1970-01-01T00:00:00.123456Z"), 123_456);
    assert_eq!(us("1970-01-01T00:00:00.1234569Z"), 123_456);
    assert_eq!(us("1970-01-01T00:00:00.5Z"), 500_000);
    // Pre-epoch: 1969-12-31T23:59:59Z is minus one second.
    assert_eq!(us("1969-12-31T23:59:59Z"), -1_000_000);
    // Case-insensitive T and Z, per RFC 3339.
    assert_eq!(us("1970-01-01t00:00:00z"), 0);
}
