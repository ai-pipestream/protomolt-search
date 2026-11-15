use super::*;

#[test]
fn rederive_follows_the_row_fingerprint() {
    let decl = Declaration::compile(&pb::DerivedColumns {
        columns: vec![
            column("y", pb::MaterializeKind::I64, "calendar.year(decided)"),
            column("m", pb::MaterializeKind::I64, "calendar.month(decided)"),
        ],
    })
    .unwrap();
    let decided = pb::IntegerValue {
        field: "decided".into(),
        value: 1_420_070_400_000_000,
    };
    let y = |value: i64| pb::IntegerValue {
        field: "y".into(),
        value,
    };
    let m = |value: i64| pb::IntegerValue {
        field: "m".into(),
        value,
    };
    // Never derived: every column must be named.
    let fresh = AddDocumentsRequest {
        integers: vec![decided.clone()],
        ..Default::default()
    };
    let err = decl
        .rederive(fresh.clone(), &["y".to_string()], None)
        .unwrap_err();
    assert!(err.message().contains("never derived"), "{}", err.message());
    let out = decl
        .rederive(fresh, &["y".to_string(), "m".to_string()], None)
        .unwrap();
    assert_eq!(out.derived_fingerprint, decl.fingerprint());
    assert_eq!(out.integers, vec![decided.clone(), y(2015), m(1)]);
    // Derived under another declaration: the named column is
    // recomputed in place, the other carried through.
    let older = AddDocumentsRequest {
        integers: vec![decided.clone(), y(1999), m(7)],
        derived_fingerprint: "older".into(),
        ..Default::default()
    };
    let out = decl.rederive(older, &["y".to_string()], None).unwrap();
    assert_eq!(out.derived_fingerprint, decl.fingerprint());
    assert_eq!(out.integers, vec![decided.clone(), m(7), y(2015)]);
    // Already this declaration: nothing to recompute.
    let current = AddDocumentsRequest {
        integers: vec![decided.clone(), y(2015), m(1)],
        derived_fingerprint: decl.fingerprint().to_string(),
        ..Default::default()
    };
    assert_eq!(decl.rederive(current.clone(), &[], None).unwrap(), current);
    let err = decl
        .rederive(current, &["y".to_string()], None)
        .unwrap_err();
    assert!(
        err.message().contains("already derived"),
        "{}",
        err.message()
    );
    let err = decl
        .rederive(AddDocumentsRequest::default(), &["z".to_string()], None)
        .unwrap_err();
    assert!(err.message().contains("not a column"), "{}", err.message());
}

/// The civil date the declaration computes for one epoch-micro
/// input, through the full evaluator.
fn civil_at(decl: &Declaration, micros: i64) -> (i64, i64, i64) {
    let doc = AddDocumentsRequest {
        integers: vec![pb::IntegerValue {
            field: "decided".into(),
            value: micros,
        }],
        ..Default::default()
    };
    let out = decl.apply(doc, None).unwrap();
    let get = |name: &str| {
        out.integers
            .iter()
            .find(|v| v.field == name)
            .unwrap_or_else(|| panic!("no {name} computed for {micros}"))
            .value
    };
    (get("y"), get("m"), get("d"))
}

#[test]
fn calendar_functions_are_exact_at_the_boundaries() {
    let decl = Declaration::compile(&pb::DerivedColumns {
        columns: vec![
            column("y", pb::MaterializeKind::I64, "calendar.year(decided)"),
            column("m", pb::MaterializeKind::I64, "calendar.month(decided)"),
            column("d", pb::MaterializeKind::I64, "calendar.day(decided)"),
        ],
    })
    .unwrap();
    const DAY: i64 = 86_400_000_000;
    let at = |y: i64, m: u32, d: u32| crate::calendar::days_from_civil(y, m, d) * DAY;
    // The proleptic Gregorian edge: the first day of year 1.
    assert_eq!(civil_at(&decl, at(1, 1, 1)), (1, 1, 1));
    assert_eq!(civil_at(&decl, at(1, 6, 15) + DAY / 2), (1, 6, 15));
    // The epoch exactly, and the microsecond before it.
    assert_eq!(civil_at(&decl, 0), (1970, 1, 1));
    assert_eq!(civil_at(&decl, -1), (1969, 12, 31));
    assert_eq!(civil_at(&decl, at(1969, 12, 31) + DAY - 1), (1969, 12, 31));
    // 2000 is a leap year by the 400-rule; 1900 is not by the
    // 100-rule, so the microsecond after 1900-02-28 is 1900-03-01.
    assert_eq!(civil_at(&decl, at(2000, 2, 29)), (2000, 2, 29));
    assert_eq!(civil_at(&decl, at(2000, 2, 29) + DAY - 1), (2000, 2, 29));
    assert_eq!(civil_at(&decl, at(1900, 2, 28) + DAY - 1), (1900, 2, 28));
    assert_eq!(civil_at(&decl, at(1900, 2, 28) + DAY), (1900, 3, 1));
    // The last microsecond of a UTC day keeps the day.
    assert_eq!(civil_at(&decl, at(2024, 12, 31) + DAY - 1), (2024, 12, 31));
    // The ends of the i64 range are exact proleptic dates, not a
    // wrap into a wrong sign and not a panic: i64::MIN micros is
    // 290309 BCE (year -290308), i64::MAX is 294247 CE.
    assert_eq!(civil_at(&decl, i64::MIN), (-290308, 12, 21));
    assert_eq!(civil_at(&decl, i64::MAX), (294247, 1, 10));
}

fn edge_decl(kind: pb::MaterializeKind, expression: &str) -> Declaration {
    Declaration::compile(&pb::DerivedColumns {
        columns: vec![column("edge", kind, expression)],
    })
    .unwrap()
}

fn int_decl(expression: &str) -> Declaration {
    edge_decl(pb::MaterializeKind::I64, expression)
}

fn uint_decl(expression: &str) -> Declaration {
    edge_decl(pb::MaterializeKind::U64, expression)
}

fn with_int(n: i64) -> AddDocumentsRequest {
    AddDocumentsRequest {
        integers: vec![pb::IntegerValue {
            field: "n".into(),
            value: n,
        }],
        ..Default::default()
    }
}

fn with_uint(u: u64) -> AddDocumentsRequest {
    AddDocumentsRequest {
        unsigned_integers: vec![pb::UnsignedIntegerValue {
            field: "u".into(),
            value: u,
        }],
        ..Default::default()
    }
}

// The input column sits in the same list, so read the derived one by
// name.
fn edge_int(out: &AddDocumentsRequest) -> Option<i64> {
    out.integers
        .iter()
        .find(|v| v.field == "edge")
        .map(|v| v.value)
}

fn edge_uint(out: &AddDocumentsRequest) -> Option<u64> {
    out.unsigned_integers
        .iter()
        .find(|v| v.field == "edge")
        .map(|v| v.value)
}

/// Correct values at the edges still compute.
#[test]
fn integer_boundaries_stay_exact() {
    let out = int_decl("n + 1")
        .apply(with_int(i64::MAX - 1), None)
        .unwrap();
    assert_eq!(edge_int(&out), Some(i64::MAX));
    let out = int_decl("n + 1").apply(with_int(i64::MIN), None).unwrap();
    assert_eq!(edge_int(&out), Some(i64::MIN + 1));
    let out = int_decl("n - 1")
        .apply(with_int(i64::MIN + 1), None)
        .unwrap();
    assert_eq!(edge_int(&out), Some(i64::MIN));
    let out = int_decl("-n").apply(with_int(i64::MIN + 1), None).unwrap();
    assert_eq!(edge_int(&out), Some(i64::MAX));
    let out = uint_decl("u % 64u")
        .apply(with_uint(u64::MAX), None)
        .unwrap();
    assert_eq!(edge_uint(&out), Some(63));
    let out = uint_decl("u % 64u").apply(with_uint(0), None).unwrap();
    assert_eq!(edge_uint(&out), Some(0));
}

/// The impossible states refuse the document, naming the column and
/// the cause; they are not the absence a missing input stores.
#[test]
fn integer_overflow_refuses_the_document() {
    for (expression, n, needle) in [
        ("n + 1", i64::MAX, "overflow"),
        ("n - 1", i64::MIN, "overflow"),
        ("-n", i64::MIN, "overflow"),
        ("math.abs(n)", i64::MIN, "overflow"),
        ("n / -1", i64::MIN, "overflow"),
        ("n / 0", 42, "division by zero"),
        ("n % 0", 42, "division by zero"),
    ] {
        let err = int_decl(expression).apply(with_int(n), None).unwrap_err();
        assert!(
            err.message().contains("\"edge\"") && err.message().contains(needle),
            "{expression} at {n}: {}",
            err.message()
        );
    }
    let err = uint_decl("u + 1u")
        .apply(with_uint(u64::MAX), None)
        .unwrap_err();
    assert!(
        err.message().contains("\"edge\"") && err.message().contains("overflow"),
        "{}",
        err.message()
    );
}

/// A genuinely missing input still stores absence, and the reshard
/// path's rederive refuses an impossible row the same way (the tool's
/// own error adds the row's source id).
#[test]
fn missing_inputs_store_absence_and_rederive_refuses_overflow() {
    let decl = Declaration::compile(&pb::DerivedColumns {
        columns: vec![
            column("plus_one", pb::MaterializeKind::I64, "n + 1"),
            column(
                "bucket",
                pb::MaterializeKind::U64,
                "hash.fnv64(court) % 64u",
            ),
        ],
    })
    .unwrap();
    let out = decl.apply(AddDocumentsRequest::default(), None).unwrap();
    assert!(out.integers.is_empty() && out.unsigned_integers.is_empty());
    let err = decl
        .rederive(
            with_int(i64::MAX),
            &["plus_one".to_string(), "bucket".to_string()],
            None,
        )
        .unwrap_err();
    assert!(
        err.message().contains("\"plus_one\"") && err.message().contains("overflow"),
        "{}",
        err.message()
    );
}

#[test]
fn strict_function_overflow_preserves_request_absence_and_untaken_branches() {
    let declaration = |expression: &str| {
        Declaration::compile(&pb::DerivedColumns {
            columns: vec![column("magnitude", pb::MaterializeKind::I64, expression)],
        })
        .unwrap()
    };
    let doc = AddDocumentsRequest {
        integers: vec![pb::IntegerValue {
            field: "n".into(),
            value: i64::MIN,
        }],
        ..Default::default()
    };
    let decl = declaration("math.abs(n)");
    let env = ingest_env(&doc, None).unwrap();
    let request = materialize_into(doc.clone(), &decl.triples(), &env, false).unwrap();
    assert_eq!(request.integers, doc.integers);
    let error = decl
        .rederive(doc.clone(), &["magnitude".into()], None)
        .unwrap_err();
    assert!(error.message().contains("magnitude"), "{error}");
    assert!(error.message().contains("math.abs"), "{error}");
    assert!(error.message().contains("overflow"), "{error}");
    let absent = decl.apply(AddDocumentsRequest::default(), None).unwrap();
    assert!(absent.integers.is_empty());
    let guarded = declaration("n < 0 ? 0 : math.abs(n)")
        .apply(doc, None)
        .unwrap();
    assert_eq!(guarded.integers.last().unwrap().value, 0);
}

#[test]
fn fnv64_is_one_function_for_ingest_and_routing() {
    // The published FNV-1a 64 vectors pin the function itself.
    assert_eq!(values::fnv1a64_bytes(b""), 0xcbf2_9ce4_8422_2325);
    assert_eq!(values::fnv1a64_bytes(b"a"), 0xaf63_dc4c_8601_ec8c);
    assert_eq!(values::fnv1a64_bytes(b"foobar"), 0x8594_4171_f739_67e8);
    // The coordinator's routing hash and the derived-column hash
    // are the same function over the same bytes, binary input
    // included: an ingest-time key bucket and the coordinator's
    // routing cannot drift apart.
    let long = [0xabu8; 1024];
    let inputs: [&[u8]; 4] = [b"", b"case-1", &[0x00, 0xff, 0x7f, 0x80], &long];
    for input in inputs {
        assert_eq!(
            crate::coordinator::stable_routing_hash(input),
            values::fnv1a64_bytes(input),
            "{input:?}"
        );
    }
    // The declaration path stores exactly that function's output.
    let decl = Declaration::compile(&pb::DerivedColumns {
        columns: vec![
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
        facets: vec![pb::FacetValue {
            field: "court".into(),
            value: "scotus".into(),
        }],
        ..Default::default()
    };
    let out = decl.apply(doc, Some(b"case-1")).unwrap();
    assert_eq!(
        out.unsigned_integers,
        vec![
            pb::UnsignedIntegerValue {
                field: "court_hash".into(),
                value: crate::coordinator::stable_routing_hash(b"scotus"),
            },
            pb::UnsignedIntegerValue {
                field: "key_bucket".into(),
                value: crate::coordinator::stable_routing_hash(b"case-1") % 64,
            },
        ]
    );
}
