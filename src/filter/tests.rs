mod wire;

use super::*;
use crate::pb;
use crate::scorefn::NumericRead;

/// Kleene tables, pinned value by value: SQL semantics, not stock
/// CEL's error propagation and not two-valued shortcuts.
#[test]
fn kleene_tables_are_sql() {
    use Tri::*;
    let all = [True, False, Unknown];
    for a in all {
        assert_eq!(a.and(True), a);
        assert_eq!(a.and(False), False);
        assert_eq!(a.or(False), a);
        assert_eq!(a.or(True), True);
        assert_eq!(a.and(a), a);
        assert_eq!(a.or(a), a);
        for b in all {
            assert_eq!(a.and(b), b.and(a), "and commutes");
            assert_eq!(a.or(b), b.or(a), "or commutes");
            // De Morgan holds in Kleene logic.
            assert_eq!(!a.and(b), (!a).or(!b));
        }
    }
    assert_eq!(Unknown.and(Unknown), Unknown);
    assert_eq!(Unknown.or(Unknown), Unknown);
    assert_eq!(!Unknown, Unknown, "negation cannot launder absence");
    assert_eq!(!True, False);
    assert_eq!(!False, True);
}

/// The exact f64-vs-i64 comparison at the values where a rounded
/// comparison would lie: above 2^53, at the 2^63 edges, and on
/// fractions.
#[test]
fn cross_domain_comparison_is_exact() {
    use Ordering::*;
    assert_eq!(cmp_f64_i64(5.5, 5), Greater);
    assert_eq!(cmp_f64_i64(5.5, 6), Less);
    assert_eq!(cmp_f64_i64(5.0, 5), Equal);
    assert_eq!(cmp_f64_i64(-5.5, -5), Less);
    assert_eq!(cmp_f64_i64(-5.5, -6), Greater);
    assert_eq!(cmp_f64_i64(-0.0, 0), Equal);
    // 2^53 + 1 is not representable: the f64 next to it is 2^53.
    // A comparison through `as f64` would call them equal.
    let two53 = 9_007_199_254_740_992i64; // 2^53
    assert_eq!(cmp_f64_i64(two53 as f64, two53 + 1), Less);
    assert_eq!(cmp_f64_i64(two53 as f64, two53), Equal);
    // 2^63 is above every i64; i64::MAX as f64 ROUNDS to 2^63,
    // which is exactly why the comparison must not go through it.
    assert_eq!(cmp_f64_i64(9_223_372_036_854_775_808.0, i64::MAX), Greater);
    assert_eq!(cmp_f64_i64(-9_223_372_036_854_775_808.0, i64::MIN), Equal);
    assert_eq!(cmp_f64_i64(f64::INFINITY, i64::MAX), Greater);
    assert_eq!(cmp_f64_i64(f64::NEG_INFINITY, i64::MIN), Less);
    // The value just below 2^63 (exact f64) vs i64::MAX.
    let below = 9_223_372_036_854_774_784.0f64; // 2^63 - 1024
    assert_eq!(cmp_f64_i64(below, i64::MAX), Less);
}

/// Float bounds onto i64 columns become exact integer edges; the
/// +1 of an exclusive bound happens in integer math where a float
/// +1 would round away.
#[test]
fn int_range_normalization_is_exact() {
    let edge = |value, exclusive| Some(Edge { value, exclusive });
    // year > 1989.5 -> lo 1990; year >= 1990.0 -> lo 1990.
    assert_eq!(int_range(&edge(NumBound::F(1989.5), true), &None).0, 1990);
    assert_eq!(int_range(&edge(NumBound::F(1990.0), false), &None).0, 1990);
    // year > 1990.0 -> lo 1991; year < 1990.0 -> hi 1989.
    assert_eq!(int_range(&edge(NumBound::F(1990.0), true), &None).0, 1991);
    assert_eq!(int_range(&None, &edge(NumBound::F(1990.0), true)).1, 1989);
    // Exclusive integer bounds at the ends: > i64::MAX admits
    // nothing; >= stays inclusive.
    assert_eq!(int_range(&edge(NumBound::I(i64::MAX), true), &None), (1, 0));
    assert_eq!(
        int_range(&edge(NumBound::I(i64::MAX), false), &None),
        (i64::MAX, i64::MAX)
    );
    assert_eq!(int_range(&None, &edge(NumBound::I(i64::MIN), true)), (1, 0));
    // A float bound at 2^62: floor()+1.0 would round back to 2^62
    // (the ULP there is 1024); the i128 arithmetic must not.
    let x = 4_611_686_018_427_387_904.0f64; // 2^62 exactly
    assert_eq!(
        int_range(&edge(NumBound::F(x), true), &None).0,
        4_611_686_018_427_387_905i64
    );
    // Bounds far outside i64 clamp to the full range (below) or
    // empty (above).
    assert_eq!(
        int_range(&edge(NumBound::F(-1e300), false), &None).0,
        i64::MIN
    );
    assert_eq!(int_range(&edge(NumBound::F(1e300), false), &None), (1, 0));
    assert_eq!(
        int_range(&None, &edge(NumBound::F(1e300), false)).1,
        i64::MAX
    );
}

/// A read surface with one column of each kind, two documents:
/// doc 0 holds every value, doc 1 holds none.
struct TwoDocs;
impl NumericRead for TwoDocs {
    fn uint_value(&self, _ii: usize, _doc_id: u32) -> Option<u64> {
        None
    }
    fn value(&self, _ni: usize, doc_id: u32) -> Option<f64> {
        (doc_id == 0).then_some(2.5)
    }
    fn map_value(&self, _ci: usize, key_ord: u32, doc_id: u32) -> Option<f64> {
        (doc_id == 0 && key_ord == 7).then_some(9.0)
    }
    fn map_int_value(&self, _: usize, _: u32, _: u32) -> Option<i64> {
        None
    }
    fn map_uint_value(&self, _: usize, _: u32, _: u32) -> Option<u64> {
        None
    }
    fn int_value(&self, _ii: usize, doc_id: u32) -> Option<i64> {
        (doc_id == 0).then_some(1990)
    }
    fn geo_value(&self, _gi: usize, doc_id: u32) -> Option<(f64, f64)> {
        (doc_id == 0).then_some((38.0, -77.0))
    }
    fn facet_ord(&self, _fi: usize, doc_id: u32) -> Option<u32> {
        (doc_id == 0).then_some(3)
    }
    fn map_facet_value_ord(&self, _ci: usize, key_ord: u32, doc_id: u32) -> Option<u32> {
        (doc_id == 0 && key_ord == 4).then_some(11)
    }
}

/// Absence semantics, leaf by leaf: comparisons answer Unknown on
/// the value-less document, presence tests answer False — and only
/// True survives [`DocFilter::passes`].
#[test]
fn absence_is_unknown_and_presence_is_total() {
    let cols = TwoDocs;
    let facet = ResolvedLeaf::Facet {
        column: Some(0),
        ords: vec![3],
    };
    assert_eq!(facet.eval(0, &cols), Tri::True);
    assert_eq!(facet.eval(1, &cols), Tri::Unknown);
    let missing_value = ResolvedLeaf::Facet {
        column: Some(0),
        ords: vec![9],
    };
    assert_eq!(
        missing_value.eval(0, &cols),
        Tri::False,
        "held a DIFFERENT value"
    );
    assert_eq!(missing_value.eval(1, &cols), Tri::Unknown);
    let unknown_col = ResolvedLeaf::Facet {
        column: None,
        ords: vec![],
    };
    assert_eq!(unknown_col.eval(0, &cols), Tri::Unknown);

    let int = ResolvedLeaf::IntRange {
        column: 0,
        lo: 1990,
        hi: i64::MAX,
    };
    assert_eq!(int.eval(0, &cols), Tri::True);
    assert_eq!(int.eval(1, &cols), Tri::Unknown);
    let empty = ResolvedLeaf::IntRange {
        column: 0,
        lo: 1,
        hi: 0,
    };
    assert_eq!(
        empty.eval(0, &cols),
        Tri::False,
        "empty range fails the present"
    );
    assert_eq!(
        empty.eval(1, &cols),
        Tri::Unknown,
        "and stays unknown for the absent"
    );

    let f64r = ResolvedLeaf::F64Range {
        column: 0,
        lo: Some(Edge {
            value: NumBound::I(2),
            exclusive: false,
        }),
        hi: Some(Edge {
            value: NumBound::F(2.5),
            exclusive: false,
        }),
    };
    assert_eq!(f64r.eval(0, &cols), Tri::True, "2.5 in [2, 2.5]");
    assert_eq!(f64r.eval(1, &cols), Tri::Unknown);
    let f64x = ResolvedLeaf::F64Range {
        column: 0,
        lo: None,
        hi: Some(Edge {
            value: NumBound::F(2.5),
            exclusive: true,
        }),
    };
    assert_eq!(f64x.eval(0, &cols), Tri::False, "2.5 < 2.5 is false");
}

/// Map comparisons answer Unknown on absence; the presence tests,
/// map keys and `has()`, are total and answer False.
#[test]
fn map_absence_is_unknown_and_presence_is_total() {
    let cols = TwoDocs;
    let map_facet = ResolvedLeaf::MapFacet {
        target: Some((0, 4)),
        ords: vec![11],
    };
    assert_eq!(map_facet.eval(0, &cols), Tri::True);
    assert_eq!(map_facet.eval(1, &cols), Tri::Unknown);

    let map_num = ResolvedLeaf::MapNumber {
        target: Some((0, 7)),
        lo: Some(Edge {
            value: NumBound::I(9),
            exclusive: false,
        }),
        hi: None,
    };
    assert_eq!(map_num.eval(0, &cols), Tri::True);
    assert_eq!(map_num.eval(1, &cols), Tri::Unknown);

    // Presence tests: total, absence is False, never Unknown.
    let has_key = ResolvedLeaf::MapHasKey(MapKeyRef::Facet {
        column: 0,
        key_ord: Some(4),
    });
    assert_eq!(has_key.eval(0, &cols), Tri::True);
    assert_eq!(has_key.eval(1, &cols), Tri::False);
    let has_key_never = ResolvedLeaf::MapHasKey(MapKeyRef::Unknown);
    assert_eq!(has_key_never.eval(0, &cols), Tri::False);
    let has = ResolvedLeaf::Has {
        facet: Some(0),
        numeric: None,
        integer: None,
        unsigned_integer: None,
        geo: None,
    };
    assert_eq!(has.eval(0, &cols), Tri::True);
    assert_eq!(has.eval(1, &cols), Tri::False);

    let geo = ResolvedLeaf::Geo {
        column: Some(0),
        region: crate::geo::GeoRegion::Bbox {
            min_lat: 30.0,
            max_lat: 40.0,
            min_lon: -80.0,
            max_lon: -70.0,
        },
    };
    assert_eq!(geo.eval(0, &cols), Tri::True);
    assert_eq!(geo.eval(1, &cols), Tri::Unknown);
}

/// NOT cannot launder absence into a match, `has()` is the escape
/// hatch that can see it, and only True passes the gate.
#[test]
fn only_true_survives_the_document_gate() {
    let cols = TwoDocs;
    let facet = ResolvedLeaf::Facet {
        column: Some(0),
        ords: vec![3],
    };
    let has = ResolvedLeaf::Has {
        facet: Some(0),
        numeric: None,
        integer: None,
        unsigned_integer: None,
        geo: None,
    };
    // NOT cannot launder absence into a match, and has() is the
    // escape hatch that can see it.
    let not_facet = ResolvedFilter::Not(Box::new(ResolvedFilter::Leaf(facet)));
    assert_eq!(not_facet.eval(1, &cols), Tri::Unknown);
    let not_has = ResolvedFilter::Not(Box::new(ResolvedFilter::Leaf(has)));
    assert_eq!(not_has.eval(1, &cols), Tri::True);

    // Only True passes the gate.
    let gate = |pred: ResolvedFilter, doc: u32| {
        DocFilter {
            deleted: None,
            geo: crate::geo::GeoFilters::default(),
            pred: Some(pred),
            phrase: Vec::new(),
        }
        .passes(doc, &cols)
    };
    assert!(gate(not_has.clone(), 1));
    assert!(!gate(not_facet.clone(), 1), "Unknown is filtered out");
    // Kleene rescue: Unknown OR True is True.
    let rescued = ResolvedFilter::Or(vec![not_facet, not_has]);
    assert!(gate(rescued, 1));
}
