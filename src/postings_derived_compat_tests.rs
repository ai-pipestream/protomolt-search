use super::*;

fn declaration() -> StoredDerived {
    StoredDerived::of(
        &crate::derived::Declaration::compile(&crate::pb::DerivedColumns {
            columns: vec![crate::pb::DerivedColumn {
                name: "year".into(),
                expression: "calendar.year(decided)".into(),
                kind: crate::pb::MaterializeKind::I64 as i32,
                disclosure: crate::pb::DerivedDisclosure::Inputs as i32,
            }],
        })
        .unwrap(),
    )
}

fn path(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("derived-compat-{tag}-{}.bm25", std::process::id()))
}

#[test]
fn legacy_kind15_declaration_opens_in_both_readers_and_rewrites_as_kind17() {
    let stored = declaration();
    let mut store = Bm25Store::new();
    store.set_derived(Some(stored.clone()));
    let mut old = Vec::new();
    store.write_v6_to(&mut old).unwrap();
    let kind = old
        .windows(15)
        .position(|b| b == b"derived-columns")
        .unwrap()
        + 15;
    assert_eq!(old[kind], 17);
    // Main cea2c63's exact inline layout: the only changed byte is its kind.
    old[kind] = 15;
    let old_path = path("legacy");
    let new_path = path("rewritten");
    std::fs::write(&old_path, &old).unwrap();
    for integrity in [false, true] {
        if integrity {
            finalize_v8(&old_path).unwrap();
        }
        let heap = Bm25Store::load(&old_path).unwrap();
        assert_eq!(heap.derived(), Some(&stored));
        let reader = Bm25Reader::open(&old_path).unwrap();
        assert_eq!(reader.derived(), Some(&stored));
        if integrity {
            reader.verify_integrity().unwrap();
        }
        heap.save(&new_path).unwrap();
        let bytes = std::fs::read(&new_path).unwrap();
        let kind = bytes
            .windows(15)
            .position(|b| b == b"derived-columns")
            .unwrap()
            + 15;
        assert_eq!(bytes[kind], 17);
        assert_eq!(
            Bm25Reader::open(&new_path).unwrap().derived(),
            Some(&stored)
        );
    }
    old[kind + 3] = if old[kind + 3] == b'0' { b'1' } else { b'0' };
    std::fs::write(&old_path, old).unwrap();
    assert!(Bm25Store::load(&old_path).is_err());
    assert!(Bm25Reader::open(&old_path).is_err());
    std::fs::remove_file(old_path).unwrap();
    std::fs::remove_file(new_path).unwrap();
}

#[test]
fn signed_map_named_derived_columns_remains_a_map() {
    let mut store = Bm25Store::new().with_map_integers(&["derived-columns"]);
    store.add_document(
        0,
        "word".into(),
        AnalyzedDoc::body(vec![("word".into(), 1, vec![(0, 4)])], 1),
    );
    store.set_map_integer(0, 0, "key", i64::MIN).unwrap();
    let p = path("map-name");
    store.save(&p).unwrap();
    let heap = Bm25Store::load(&p).unwrap();
    assert_eq!(heap.map_integer_value(0, 0, 0), Some(i64::MIN));
    assert!(heap.derived().is_none());
    let reader = Bm25Reader::open(&p).unwrap();
    assert_eq!(reader.map_integer_value(0, 0, 0), Some(i64::MIN));
    assert!(reader.derived().is_none());
    reader.verify_integrity().unwrap();
    std::fs::remove_file(p).unwrap();
}

#[test]
fn declaration_and_both_integer_map_families_share_one_image() {
    let mut store = Bm25Store::new()
        .with_map_integers(&["signed"])
        .with_map_unsigned_integers(&["unsigned", "entirely_absent"]);
    let stored = declaration();
    store.set_derived(Some(stored.clone()));
    store.add_document(
        0,
        "word".into(),
        AnalyzedDoc::body(vec![("word".into(), 1, vec![(0, 4)])], 1),
    );
    store.set_map_integer(0, 0, "key", i64::MIN).unwrap();
    store
        .set_map_unsigned_integer(0, 0, "key", u64::MAX)
        .unwrap();
    let p = path("combined");
    store.save(&p).unwrap();
    let heap = Bm25Store::load(&p).unwrap();
    let reader = Bm25Reader::open(&p).unwrap();
    assert_eq!(heap.derived(), Some(&stored));
    assert_eq!(reader.derived(), Some(&stored));
    assert_eq!(heap.map_integer_value(0, 0, 0), Some(i64::MIN));
    assert_eq!(reader.map_integer_value(0, 0, 0), Some(i64::MIN));
    assert_eq!(heap.map_unsigned_integer_value(0, 0, 0), Some(u64::MAX));
    assert_eq!(reader.map_unsigned_integer_value(0, 0, 0), Some(u64::MAX));
    assert_eq!(reader.map_unsigned_integer_name(1), "entirely_absent");
    reader.verify_integrity().unwrap();
    std::fs::remove_file(p).unwrap();
}
