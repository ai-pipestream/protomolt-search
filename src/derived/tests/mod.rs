use super::config::DeclarationFile;
use super::*;

fn column(name: &str, kind: pb::MaterializeKind, expression: &str) -> pb::DerivedColumn {
    pb::DerivedColumn {
        name: name.into(),
        expression: expression.into(),
        kind: kind as i32,
        disclosure: pb::DerivedDisclosure::Inputs as i32,
    }
}

fn tables<'a>(integers: &'a [String], facets: &'a [String]) -> values::NumericTypes<'a> {
    values::NumericTypes {
        numerics: &[],
        integers,
        unsigned_integers: &[],
        map_numerics: &[],
        map_integers: &[],
        map_unsigned_integers: &[],
        facets,
    }
}

#[test]
fn the_fingerprint_follows_the_canonical_encoding_not_the_spelling() {
    let a = pb::DerivedColumns {
        columns: vec![column(
            "y",
            pb::MaterializeKind::I64,
            "calendar.year(decided)",
        )],
    };
    let mut b = a.clone();
    b.columns[0].expression = "calendar.year( decided )".into();
    assert_ne!(fingerprint_of(&a), fingerprint_of(&b));
    let toml = declaration_toml(&a).unwrap();
    let file: DeclarationFile = toml::from_str(&toml).unwrap();
    assert_eq!(spec_from_config(&file.derived).unwrap(), a);
    assert_eq!(
        fingerprint_of(&spec_from_config(&file.derived).unwrap()),
        fingerprint_of(&a)
    );
}

#[test]
fn compile_refuses_by_name() {
    let refused = |spec: pb::DerivedColumns, needle: &str| {
        let err = Declaration::compile(&spec).unwrap_err();
        assert!(err.contains(needle), "{err}");
    };
    refused(
        pb::DerivedColumns {
            columns: vec![column("", pb::MaterializeKind::I64, "year")],
        },
        "non-empty",
    );
    refused(
        pb::DerivedColumns {
            columns: vec![
                column("y", pb::MaterializeKind::I64, "year"),
                column("y", pb::MaterializeKind::I64, "year"),
            ],
        },
        "declared twice",
    );
    refused(
        pb::DerivedColumns {
            columns: vec![column("y", pb::MaterializeKind::Unspecified, "year")],
        },
        "no kind",
    );
    let mut no_disclosure = column("y", pb::MaterializeKind::I64, "year");
    no_disclosure.disclosure = 0;
    refused(
        pb::DerivedColumns {
            columns: vec![no_disclosure],
        },
        "no disclosure rule",
    );
    refused(
        pb::DerivedColumns {
            columns: vec![
                column("a", pb::MaterializeKind::I64, "year / 10"),
                column("b", pb::MaterializeKind::I64, "a * 10"),
            ],
        },
        "outputs are not inputs",
    );
    refused(
        pb::DerivedColumns {
            columns: vec![column("y", pb::MaterializeKind::I64, "stable_key()")],
        },
        "hash.fnv64(stable_key())",
    );
}

#[test]
fn check_tables_refuses_unknown_inputs_collisions_and_type_mismatches() {
    let integers = vec!["year".to_string(), "decided".to_string()];
    let facets = vec!["court".to_string()];
    let t = tables(&integers, &facets);
    let ok = Declaration::compile(&pb::DerivedColumns {
        columns: vec![
            column("y", pb::MaterializeKind::I64, "calendar.year(decided)"),
            column("court_hash", pb::MaterializeKind::U64, "hash.fnv64(court)"),
            column(
                "scotus",
                pb::MaterializeKind::I64,
                "court == \"scotus\" ? 1 : 0",
            ),
            column(
                "key_bucket",
                pb::MaterializeKind::U64,
                "hash.fnv64(stable_key()) % 64u",
            ),
        ],
    })
    .unwrap();
    ok.check_tables(&t).unwrap();
    assert!(ok.reads_stable_key());
    assert_eq!(
        ok.column("scotus").unwrap().inputs,
        vec!["court".to_string()]
    );
    let refused = |spec: pb::DerivedColumns, needle: &str| {
        let err = Declaration::compile(&spec)
            .unwrap()
            .check_tables(&t)
            .unwrap_err();
        assert!(err.contains(needle), "{err}");
    };
    refused(
        pb::DerivedColumns {
            columns: vec![column("year", pb::MaterializeKind::I64, "decided")],
        },
        "collides with a source column",
    );
    refused(
        pb::DerivedColumns {
            columns: vec![column("y", pb::MaterializeKind::I64, "yeer")],
        },
        "does not declare",
    );
    refused(
        pb::DerivedColumns {
            columns: vec![column("y", pb::MaterializeKind::F64, "year")],
        },
        "declares f64 but its expression resolves to int",
    );
    refused(
        pb::DerivedColumns {
            columns: vec![column("y", pb::MaterializeKind::I64, "year > 1990")],
        },
        "wrap the boolean",
    );
    refused(
        pb::DerivedColumns {
            columns: vec![column("y", pb::MaterializeKind::I64, "court")],
        },
        "bare facet read",
    );
}

#[test]
fn apply_computes_stamps_and_refuses_forgeries() {
    let decl = Declaration::compile(&pb::DerivedColumns {
        columns: vec![
            column("y", pb::MaterializeKind::I64, "calendar.year(decided)"),
            column("court_hash", pb::MaterializeKind::U64, "hash.fnv64(court)"),
            column(
                "key_bucket",
                pb::MaterializeKind::U64,
                "hash.fnv64(stable_key()) % 64u",
            ),
        ],
    })
    .unwrap();
    let doc = AddDocumentsRequest {
        timestamps: vec![pb::TimestampValue {
            field: "decided".into(),
            value: Some(prost_types::Timestamp {
                seconds: 1_420_070_400, // 2015-01-01T00:00:00Z
                nanos: 0,
            }),
        }],
        facets: vec![pb::FacetValue {
            field: "court".into(),
            value: "scotus".into(),
        }],
        ..Default::default()
    };
    let out = decl.apply(doc.clone(), Some(b"case-1")).unwrap();
    assert_eq!(out.derived_fingerprint, decl.fingerprint());
    assert_eq!(
        out.integers,
        vec![pb::IntegerValue {
            field: "y".into(),
            value: 2015
        }]
    );
    assert_eq!(
        out.unsigned_integers,
        vec![
            pb::UnsignedIntegerValue {
                field: "court_hash".into(),
                value: values::fnv1a64_bytes(b"scotus"),
            },
            pb::UnsignedIntegerValue {
                field: "key_bucket".into(),
                value: values::fnv1a64_bytes(b"case-1") % 64,
            },
        ]
    );
    let err = decl.apply(doc.clone(), None).unwrap_err();
    assert!(
        err.message().contains("stable routing key"),
        "{}",
        err.message()
    );
    let mut forged = doc.clone();
    forged.integers.push(pb::IntegerValue {
        field: "y".into(),
        value: 1999,
    });
    let err = decl.apply(forged, Some(b"k")).unwrap_err();
    assert!(
        err.message().contains("refused as forged"),
        "{}",
        err.message()
    );
    let mut stamped = doc;
    stamped.derived_fingerprint = "abc".into();
    let err = decl.apply(stamped, Some(b"k")).unwrap_err();
    assert!(
        err.message().contains("stamps derived fingerprint"),
        "{}",
        err.message()
    );
}

#[test]
fn derived_inputs_keep_exact_maps_calendar_facets_sources_and_dependencies() {
    let decl = Declaration::compile(&pb::DerivedColumns {
        columns: vec![
            column("signed_copy", pb::MaterializeKind::I64, "signed['']"),
            column("unsigned_copy", pb::MaterializeKind::U64, "unsigned['']"),
            column("year", pb::MaterializeKind::I64, "calendar.year(decided)"),
            column("hash", pb::MaterializeKind::U64, "hash.fnv64(court)"),
        ],
    })
    .unwrap();
    let signed = ["signed".to_string()];
    let unsigned = ["unsigned".to_string()];
    let integers = ["decided".to_string()];
    let facets = ["court".to_string()];
    let types = values::NumericTypes {
        numerics: &[],
        integers: &integers,
        unsigned_integers: &[],
        map_numerics: &[],
        map_integers: &signed,
        map_unsigned_integers: &unsigned,
        facets: &facets,
    };
    decl.check_tables(&types).unwrap();
    assert_eq!(decl.column("signed_copy").unwrap().inputs, ["signed"]);
    assert_eq!(decl.column("unsigned_copy").unwrap().inputs, ["unsigned"]);
    let source = pb::ProtobufSource {
        descriptor_set: vec![0, 255],
        message_type: "opaque".into(),
        payload: vec![8, 0x81, 0],
    };
    let doc = AddDocumentsRequest {
        map_integers: vec![pb::MapIntegerEntry {
            field: "signed".into(),
            key: "".into(),
            value: i64::MIN,
        }],
        map_unsigned_integers: vec![pb::MapUnsignedIntegerEntry {
            field: "unsigned".into(),
            key: "".into(),
            value: u64::MAX,
        }],
        integers: vec![pb::IntegerValue {
            field: "decided".into(),
            value: 1_420_070_400_000_000,
        }],
        facets: vec![pb::FacetValue {
            field: "court".into(),
            value: "court".into(),
        }],
        original_source: Some(source.clone()),
        ..Default::default()
    };
    let out = decl.apply(doc.clone(), None).unwrap();
    assert_eq!(out.original_source, Some(source));
    assert_eq!(out.map_integers, doc.map_integers);
    assert_eq!(out.map_unsigned_integers, doc.map_unsigned_integers);
    assert_eq!(out.integers[1].value, i64::MIN);
    assert_eq!(out.integers[2].value, 2015);
    assert_eq!(out.unsigned_integers[0].value, u64::MAX);
    assert_eq!(
        out.unsigned_integers[1].value,
        values::fnv1a64_bytes(b"court")
    );
    let mut duplicate = doc;
    duplicate
        .map_integers
        .push(duplicate.map_integers[0].clone());
    assert!(decl
        .apply(duplicate, None)
        .unwrap_err()
        .message()
        .contains("repeats"));
}

mod exact;
