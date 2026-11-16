//! Unit tests of the digest, the civil year, and the check specs.

use super::digest::digest;
use super::*;
use crate::reshard::{reconstruct_document, segment_tables};

#[test]
fn the_civil_year_counts_days_from_the_epoch() {
    let day = 86_400_000_000i64;
    assert_eq!(civil_year(0), 1970);
    assert_eq!(civil_year(-1), 1969);
    assert_eq!(civil_year(365 * day - 1), 1970);
    assert_eq!(civil_year(365 * day), 1971);
    // 1972 is a leap year: 1970 and 1971 are 730 days, 1972 366.
    assert_eq!(civil_year((730 + 365) * day), 1972);
    assert_eq!(civil_year((730 + 366) * day), 1973);
    // 2000-01-01T00:00:00Z is 946684800 s after the epoch.
    assert_eq!(civil_year(946_684_800_000_000), 2000);
    assert_eq!(civil_year(946_684_800_000_000 - 1), 1999);
    // 1900-01-01T00:00:00Z is -2208988800 s; 1900 is not a leap year.
    assert_eq!(civil_year(-2_208_988_800_000_000), 1900);
    assert_eq!(civil_year(-2_208_988_800_000_000 - 1), 1899);
    for (micros, year) in [
        (crate::calendar::days_from_civil(1600, 2, 29) * day, 1600),
        (crate::calendar::days_from_civil(202, 1, 1) * day, 202),
        (crate::calendar::days_from_civil(202, 12, 31) * day, 202),
        (crate::calendar::days_from_civil(2100, 3, 1) * day, 2100),
    ] {
        assert_eq!(civil_year(micros), year, "{micros}");
    }
}

#[test]
fn a_digest_ignores_the_order_of_a_value_list() {
    let mut a = AddDocumentsRequest {
        text: "t".into(),
        ..Default::default()
    };
    a.integers.push(pb::IntegerValue {
        field: "year".into(),
        value: 1,
    });
    a.integers.push(pb::IntegerValue {
        field: "decided".into(),
        value: 2,
    });
    let mut b = a.clone();
    b.integers.reverse();
    assert_eq!(digest(&a, None), digest(&b, None));
    b.integers[0].value += 1;
    assert_ne!(digest(&a, None), digest(&b, None));
    assert_ne!(digest(&a, None), digest(&a, Some(&[0.0])));
}

#[test]
fn exact_integer_maps_change_the_digest_without_float_conversion() {
    for value in [i64::MIN, 0, (1i64 << 53) + 1, i64::MAX - 1] {
        let mut original = AddDocumentsRequest::default();
        original.map_integers.push(pb::MapIntegerEntry {
            field: "m".into(),
            key: "".into(),
            value,
        });
        let mut changed = original.clone();
        changed.map_integers[0].value += 1;
        assert_ne!(
            digest(&original, None),
            digest(&changed, None),
            "signed {value}"
        );
        assert_ne!(
            digest(&original, None),
            digest(&AddDocumentsRequest::default(), None)
        );
    }
    for value in [0, (1u64 << 53) + 1, u64::MAX - 1] {
        let mut original = AddDocumentsRequest::default();
        original
            .map_unsigned_integers
            .push(pb::MapUnsignedIntegerEntry {
                field: "m".into(),
                key: "".into(),
                value,
            });
        let mut changed = original.clone();
        changed.map_unsigned_integers[0].value += 1;
        assert_ne!(
            digest(&original, None),
            digest(&changed, None),
            "unsigned {value}"
        );
        changed = original.clone();
        changed.map_unsigned_integers[0].key = "0".into();
        assert_ne!(digest(&original, None), digest(&changed, None));
    }
}

#[test]
fn reconstructed_rows_and_tables_retain_both_integer_map_families() {
    use crate::postings::{AnalyzedDoc, Bm25Reader, Bm25Store};
    let path = std::env::temp_dir().join(format!(
        "reconcile-integer-maps-{}.bm25",
        std::process::id()
    ));
    let mut store = Bm25Store::new()
        .with_columns(
            crate::postings::ColumnKind::MapInteger,
            &["signed", "empty"],
        )
        .with_columns(
            crate::postings::ColumnKind::MapUnsignedInteger,
            &["unsigned"],
        );
    store.add_document(
        0,
        "map".into(),
        AnalyzedDoc::body(vec![("map".into(), 1, vec![(0, 3)])], 1),
    );
    store.set_map_integer(0, 0, "", i64::MIN).unwrap();
    store.set_map_unsigned_integer(0, 0, "", u64::MAX).unwrap();
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let tables = segment_tables(&reader);
    assert_eq!(tables.columns.map_integers, ["signed", "empty"]);
    assert_eq!(tables.columns.map_unsigned_integers, ["unsigned"]);
    let doc = reconstruct_document(&reader, 0, &tables, true).unwrap();
    assert_eq!(doc.map_integers[0].value, i64::MIN);
    assert_eq!(doc.map_unsigned_integers[0].value, u64::MAX);
    let mut missing = doc.clone();
    missing.map_integers.clear();
    assert_ne!(digest(&doc, None), digest(&missing, None));
    let mut changed = doc.clone();
    changed.map_unsigned_integers[0].value -= 1;
    assert_ne!(digest(&doc, None), digest(&changed, None));
    std::fs::remove_file(path).unwrap();
}

#[test]
fn a_check_spec_names_two_columns() {
    assert_eq!(
        ColumnCheck::parse("equal", "a:b").unwrap(),
        ColumnCheck::Equal {
            column: "a".into(),
            other: "b".into()
        }
    );
    assert!(ColumnCheck::parse("equal", "a").is_err());
    assert!(ColumnCheck::parse("civil-year", ":b").is_err());
    assert!(ColumnCheck::parse("other", "a:b").is_err());
}
