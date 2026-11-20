//! Dual-writer integer column round-trips and partial-kind section tiling.

use super::*;

/// The v7 round-trip for i64 columns: the kinded table entry, min/max
/// metadata, both readers, the heap loader, dual-writer byte identity,
/// and the exactness that motivates the kind — a value above 2^53 comes
/// back as the integer it went in as, which an f64 column cannot do.
#[test]
fn integer_columns_roundtrip_and_dual_writers_agree() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("integer_roundtrip_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let analyzed = |terms: &[(&str, u32)]| {
        AnalyzedDoc::body(
            terms
                .iter()
                .map(|(t, tf)| (t.to_string(), *tf, vec![(0u32, 4u32)]))
                .collect(),
            terms.iter().map(|(_, tf)| tf).sum(),
        )
    };
    // 2^53 + 1 is the first integer an f64 cannot hold: it rounds to
    // 2^53. Doc 1 has no value; doc 2 is negative, so min/max must
    // cover both signs.
    const BEYOND_F64: i64 = (1i64 << 53) + 1;
    const INT_SETS: [(u32, i64); 2] = [(0, BEYOND_F64), (2, -7)];

    // A store carrying all five column kinds at once, to pin the
    // kinded table's ordering with kind 4 appended last.
    let mut store = Bm25Store::with_text_fields(&["body"])
        .with_columns(Facet, &["court"])
        .with_columns(Numeric, &["date"])
        .with_columns(MapFacet, &["meta"])
        .with_columns(MapNumeric, &["attrs"])
        .with_columns(Integer, &["citations"]);
    for (i, terms) in [
        &[("rust", 1u32), ("search", 1)][..],
        &[("vector", 1)][..],
        &[("plain", 1)][..],
    ]
    .iter()
    .enumerate()
    {
        store.add_document(i as u32, format!("doc {i}"), analyzed(terms));
    }
    store.set_facet(0, 0, "scotus");
    store.set_numeric(0, 0, 150.5);
    store.set_map_facet(0, 0, "color", "red");
    store.set_map_numeric(0, 1, "boost", 2.0);
    for (d, v) in INT_SETS {
        store.set_integer(0, d, v);
    }
    let heap_path = dir.join("heap.bm25");
    store.save(&heap_path).unwrap();
    let bytes = std::fs::read(&heap_path).unwrap();
    assert_eq!(
        &bytes[..8],
        b"TVBM2508",
        "integer columns opt into the v7-shaped v8 payload"
    );

    let mut builder = SpillBuilder::create_with_text_fields(&dir.join("spill.build"), &["body"])
        .unwrap()
        .with_columns(Facet, &["court"])
        .with_columns(Numeric, &["date"])
        .with_columns(MapFacet, &["meta"])
        .with_columns(MapNumeric, &["attrs"])
        .with_columns(Integer, &["citations"])
        .with_buffer_bytes(32);
    for (i, terms) in [
        &[("rust", 1u32), ("search", 1)][..],
        &[("vector", 1)][..],
        &[("plain", 1)][..],
    ]
    .iter()
    .enumerate()
    {
        builder
            .add_document_with_lineage(i as u32, format!("doc {i}"), analyzed(terms), None)
            .unwrap();
    }
    builder.set_facet(0, 0, "scotus");
    builder.set_numeric(0, 0, 150.5);
    builder.set_map_facet(0, 0, "color", "red");
    builder.set_map_numeric(0, 1, "boost", 2.0);
    for (d, v) in INT_SETS {
        builder.set_integer(0, d, v);
    }
    let spill_path = dir.join("spill.bm25");
    builder.finish(&spill_path).unwrap();
    assert_eq!(
        bytes,
        std::fs::read(&spill_path).unwrap(),
        "dual writers must stay byte-identical on integer-bearing stores"
    );

    let reader = Bm25Reader::open(&heap_path).unwrap();
    assert_eq!(reader.integer_count(), 1);
    assert_eq!(reader.integer_name(0), "citations");
    assert_eq!(reader.integer_index("citations"), Some(0));
    assert_eq!(reader.integer_index("citation"), None);
    assert_eq!(reader.integer_value(0, 0), Some(BEYOND_F64));
    assert_eq!(reader.integer_value(0, 1), None, "doc 1 has no value");
    assert_eq!(reader.integer_value(0, 2), Some(-7));
    assert_eq!(reader.integer_min_max(0), (-7, BEYOND_F64));
    // The whole argument for the kind, stated as an assertion: the f64
    // column could not have told 2^53 + 1 from 2^53.
    assert_ne!(BEYOND_F64 as f64 as i64, BEYOND_F64);
    assert_eq!(reader.integer_value(0, 0).unwrap(), 9_007_199_254_740_993);
    // The other kinds still read back beside it.
    assert_eq!(reader.facet_count(), 1);
    assert_eq!(reader.numeric_value(0, 0), Some(150.5));
    let meta = reader.map_facet_index("meta").unwrap();
    let color = reader.map_facet_key_ord(meta, "color").unwrap();
    assert_eq!(
        reader
            .map_facet_value_ord(meta, color, 0)
            .map(|o| reader.map_facet_value(meta, o)),
        Some("red")
    );
    let attrs = reader.map_numeric_index("attrs").unwrap();
    let boost = reader.map_numeric_key_ord(attrs, "boost").unwrap();
    assert_eq!(reader.map_numeric_value(attrs, boost, 1), Some(2.0));

    // The heap loader (the resident-append reload path) recovers the
    // same values and re-derives the same metadata.
    let loaded = Bm25Store::load(&heap_path).unwrap();
    assert_eq!(loaded.integer_index("citations"), Some(0));
    assert_eq!(loaded.integer_value(0, 0), Some(BEYOND_F64));
    assert_eq!(loaded.integer_value(0, 1), None);
    assert_eq!(loaded.integer_value(0, 2), Some(-7));
    assert_eq!(loaded.integer_min_max(0), (-7, BEYOND_F64));

    // Integers alone still opt the shard into v7, and a column no
    // document valued folds to the empty range rather than to a value.
    let mut only_integer = Bm25Store::new().with_columns(Integer, &["a", "b"]);
    only_integer.add_document(0, "rust".to_string(), analyzed(&[("rust", 1)]));
    only_integer.set_integer(0, 0, 42);
    let integer_path = dir.join("integer.bm25");
    only_integer.save(&integer_path).unwrap();
    assert_eq!(&std::fs::read(&integer_path).unwrap()[..8], b"TVBM2508");
    let r = Bm25Reader::open(&integer_path).unwrap();
    assert_eq!(
        (r.facet_count(), r.numeric_count(), r.integer_count()),
        (0, 0, 2)
    );
    assert_eq!(r.integer_min_max(0), (42, 42));
    assert_eq!(
        r.integer_min_max(1),
        (i64::MAX, i64::MIN),
        "a column with no values folds to the empty range, not to a value"
    );
    assert_eq!(r.integer_value(1, 0), None);

    let _ = std::fs::remove_dir_all(&dir);
}

/// Stores that declare SOME column kinds and skip the middle ones. The
/// validator locates each kind group's end by falling through the
/// ABSENT kinds to the next declared group's start, and each skip is a
/// distinct fallback branch a full-kind store never takes: with every
/// kind present the facet group ends at the numeric group, never at
/// the integers. One case per skip boundary into the integer section,
/// with dual-writer identity pinning both writers' offset arithmetic
/// on the same partial layouts. Open runs full validation, so opening
/// IS the tiling assertion.
#[test]
fn partial_kind_stores_tile_and_validate() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("partial_kinds_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let analyzed = || AnalyzedDoc::body(vec![("rust".to_string(), 1u32, vec![(0u32, 4u32)])], 1);
    // (case, facets, numerics, map facets): integers always present,
    // so the cases pin facets->integers, numerics->integers, and
    // map-facets->integers respectively. map-numerics->integers is
    // pinned by the all-kinds store above.
    for (case, facets, numerics, map_facets) in [
        ("facets_int", true, false, false),
        ("numerics_int", false, true, false),
        ("mapfacets_int", false, false, true),
    ] {
        let mut store = Bm25Store::with_text_fields(&["body"]);
        if facets {
            store = store.with_columns(Facet, &["court"]);
        }
        if numerics {
            store = store.with_columns(Numeric, &["date"]);
        }
        if map_facets {
            store = store.with_columns(MapFacet, &["meta"]);
        }
        let mut store = store.with_columns(Integer, &["citations"]);
        store.add_document(0, "doc".to_string(), analyzed());
        if facets {
            store.set_facet(0, 0, "scotus");
        }
        if numerics {
            store.set_numeric(0, 0, 1.5);
        }
        if map_facets {
            store.set_map_facet(0, 0, "color", "red");
        }
        store.set_integer(0, 0, 7);
        let path = dir.join(format!("{case}.bm25"));
        store.save(&path).unwrap();

        let mut builder =
            SpillBuilder::create_with_text_fields(&dir.join(format!("{case}.build")), &["body"])
                .unwrap();
        if facets {
            builder = builder.with_columns(Facet, &["court"]);
        }
        if numerics {
            builder = builder.with_columns(Numeric, &["date"]);
        }
        if map_facets {
            builder = builder.with_columns(MapFacet, &["meta"]);
        }
        let mut builder = builder.with_columns(Integer, &["citations"]);
        builder
            .add_document_with_lineage(0, "doc".to_string(), analyzed(), None)
            .unwrap();
        if facets {
            builder.set_facet(0, 0, "scotus");
        }
        if numerics {
            builder.set_numeric(0, 0, 1.5);
        }
        if map_facets {
            builder.set_map_facet(0, 0, "color", "red");
        }
        builder.set_integer(0, 0, 7);
        let spill_path = dir.join(format!("{case}_spill.bm25"));
        builder.finish(&spill_path).unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap(),
            std::fs::read(&spill_path).unwrap(),
            "{case}: dual writers must agree on partial-kind layouts too"
        );

        let r = Bm25Reader::open(&path).unwrap_or_else(|e| panic!("{case}: {e}"));
        assert_eq!(r.integer_value(0, 0), Some(7), "{case}");
        assert_eq!(r.integer_min_max(0), (7, 7), "{case}");
        if facets {
            assert_eq!(
                r.facet_ord(0, 0).map(|o| r.facet_value(0, o)),
                Some("scotus"),
                "{case}"
            );
        }
        if numerics {
            assert_eq!(r.numeric_value(0, 0), Some(1.5), "{case}");
        }
        if map_facets {
            let ci = r.map_facet_index("meta").unwrap();
            let key = r.map_facet_key_ord(ci, "color").unwrap();
            assert_eq!(
                r.map_facet_value_ord(ci, key, 0)
                    .map(|o| r.map_facet_value(ci, o)),
                Some("red"),
                "{case}"
            );
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}
