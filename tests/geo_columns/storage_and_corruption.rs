//! Geo-point dual-writer round-trips, corruption refusal, and partial-kind tiling.

use super::*;

/// The v7 round-trip for geo columns: the kinded table entry, the
/// bounding-box metadata, both readers, the heap loader, dual-writer
/// byte identity with all SIX kinds in one store, and the absence
/// convention.
#[test]
fn geo_columns_roundtrip_and_dual_writers_agree() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("geo_roundtrip_{}", std::process::id()));
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
    // Doc 1 has no point at all; doc 0 and doc 2 straddle the equator
    // and the prime meridian, so the box has to grow on all four sides.
    const POINTS: [(u32, f64, f64); 2] = [(0, 38.8977, -77.0365), (2, -33.8688, 151.2093)];

    // A store carrying all six column kinds at once, to pin the kinded
    // table's ordering with kind 5 appended last.
    let mut store = Bm25Store::with_text_fields(&["body"])
        .with_columns(Facet, &["court"])
        .with_columns(Numeric, &["date"])
        .with_columns(MapFacet, &["meta"])
        .with_columns(MapNumeric, &["attrs"])
        .with_columns(Integer, &["citations"])
        .with_columns(Geo, &["courthouse"]);
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
    store.set_integer(0, 0, 4103);
    for (d, lat, lon) in POINTS {
        store.set_geo(0, d, lat, lon);
    }
    let heap_path = dir.join("heap.bm25");
    store.save(&heap_path).unwrap();
    let bytes = std::fs::read(&heap_path).unwrap();
    assert_eq!(
        &bytes[..8],
        b"TVBM2508",
        "geo columns opt into the v7-shaped v8 payload"
    );

    let mut builder = SpillBuilder::create_with_text_fields(&dir.join("spill.build"), &["body"])
        .unwrap()
        .with_columns(Facet, &["court"])
        .with_columns(Numeric, &["date"])
        .with_columns(MapFacet, &["meta"])
        .with_columns(MapNumeric, &["attrs"])
        .with_columns(Integer, &["citations"])
        .with_columns(Geo, &["courthouse"]);
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
    builder.set_integer(0, 0, 4103);
    for (d, lat, lon) in POINTS {
        builder.set_geo(0, d, lat, lon);
    }
    let spill_path = dir.join("spill.bm25");
    builder.finish(&spill_path).unwrap();
    assert_eq!(
        std::fs::read(&heap_path).unwrap(),
        std::fs::read(&spill_path).unwrap(),
        "the two writers must agree byte for byte with all six kinds present"
    );

    // The bounding box the writer computed, re-derived by hand: min/max
    // over each axis independently, which is why it is a box and not a
    // hull.
    let want_bbox = (-33.8688, 38.8977, -77.0365, 151.2093);
    let reader = Bm25Reader::open(&heap_path).unwrap();
    assert_eq!(reader.geo_count(), 1);
    assert_eq!(reader.geo_name(0), "courthouse");
    assert_eq!(reader.geo_index("courthouse"), Some(0));
    assert_eq!(reader.geo_index("nope"), None);
    assert_eq!(reader.geo_bbox(0), want_bbox);
    assert_eq!(reader.geo_value(0, 0), Some((38.8977, -77.0365)));
    assert_eq!(reader.geo_value(0, 1), None, "absence is (NaN, NaN)");
    assert_eq!(reader.geo_value(0, 2), Some((-33.8688, 151.2093)));
    // The other kinds are untouched by the new one.
    assert_eq!(reader.integer_value(0, 0), Some(4103));
    assert_eq!(reader.numeric_value(0, 0), Some(150.5));

    // The heap loader round-trips the same values and metadata.
    let loaded = Bm25Store::load(&heap_path).unwrap();
    assert_eq!(loaded.geo_count(), 1);
    assert_eq!(loaded.geo_value(0, 0), Some((38.8977, -77.0365)));
    assert_eq!(loaded.geo_value(0, 1), None);
    assert_eq!(loaded.geo_bbox(0), want_bbox);
    // Re-saving from the loaded store reproduces the file exactly, so
    // the metadata really was recomputed and not smuggled through.
    let resaved = dir.join("resaved.bm25");
    loaded.save(&resaved).unwrap();
    assert_eq!(
        std::fs::read(&heap_path).unwrap(),
        std::fs::read(&resaved).unwrap()
    );

    // A column no document valued folds to four NaNs, the kind-1 empty
    // convention.
    let mut empty = Bm25Store::with_text_fields(&["body"]).with_columns(Geo, &["courthouse"]);
    empty.add_document(0, "doc".to_string(), analyzed(&[("rust", 1)]));
    let empty_path = dir.join("empty.bm25");
    empty.save(&empty_path).unwrap();
    let r = Bm25Reader::open(&empty_path).unwrap();
    let (a, b, c, d) = r.geo_bbox(0);
    assert!(
        a.is_nan() && b.is_nan() && c.is_nan() && d.is_nan(),
        "an unvalued geo column has an empty box, not a point at the origin"
    );
    assert_eq!(r.geo_value(0, 0), None);

    let _ = std::fs::remove_dir_all(&dir);
}

/// Corruption the reader must refuse rather than interpret: a half-NaN
/// coordinate pair (a point that lost one axis), and a bounding box that
/// disagrees with the values it claims to summarize. The second is the
/// dangerous one — stale box metadata is exactly what a wrong bound is
/// made of — so both are refused at OPEN, before any query can read
/// them.
#[test]
fn corrupt_geo_sections_refuse_at_open() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("geo_corrupt_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let mut store = Bm25Store::with_text_fields(&["body"]).with_columns(Geo, &["courthouse"]);
    for i in 0..2u32 {
        store.add_document(
            i,
            format!("doc {i}"),
            AnalyzedDoc::body(vec![("rust".to_string(), 1, vec![(0, 4)])], 1),
        );
    }
    store.set_geo(0, 0, 10.0, 20.0);
    // The geo validators are pinned on a raw v7 payload (a pre-v8
    // build): under v8 the integrity CRC refuses stomped bytes BEFORE
    // any semantic walk runs, which is its own pin below.
    let mut good = Vec::new();
    store.write_v6_to(&mut good).unwrap();
    let path = dir.join("good.bm25");
    std::fs::write(&path, &good).unwrap();
    Bm25Reader::open(&path).expect("the untouched file opens");

    // The vals section is the last thing in the file: doc 0's pair is
    // the first 16 bytes of it, doc 1's the next 16 (both NaN).
    let vals_off = good.len() - 32;
    let mut half_nan = good.clone();
    half_nan[vals_off + 8..vals_off + 16].copy_from_slice(&f64::NAN.to_bits().to_le_bytes());
    let p = dir.join("half_nan.bm25");
    std::fs::write(&p, &half_nan).unwrap();
    let err = Bm25Reader::open(&p)
        .err()
        .expect("half-NaN pairs must refuse");
    assert!(
        err.to_string().contains("half-NaN"),
        "a point that lost one axis must refuse by name: {err}"
    );

    // Move the value without moving the box: the scan disagrees.
    let mut stale_box = good.clone();
    stale_box[vals_off..vals_off + 8].copy_from_slice(&11.0f64.to_bits().to_le_bytes());
    let p = dir.join("stale_box.bm25");
    std::fs::write(&p, &stale_box).unwrap();
    let err = Bm25Reader::open(&p)
        .err()
        .expect("stale box metadata must refuse");
    assert!(
        err.to_string().contains("bounding-box metadata disagrees"),
        "stale box metadata is what a wrong bound is made of: {err}"
    );

    // A coordinate off the globe never survived ingest, so finding one
    // means the bytes are not what the writer wrote.
    let mut off_globe = good.clone();
    off_globe[vals_off..vals_off + 8].copy_from_slice(&91.0f64.to_bits().to_le_bytes());
    let p = dir.join("off_globe.bm25");
    std::fs::write(&p, &off_globe).unwrap();
    assert!(Bm25Reader::open(&p).is_err());

    // On a v8 SAVE of the same store, the identical stomp is refused
    // one layer earlier: the eager integrity check names the column
    // section before any geo semantics run.
    let v8_path = dir.join("v8.bm25");
    store.save(&v8_path).unwrap();
    let mut v8_bytes = std::fs::read(&v8_path).unwrap();
    let payload_len = good.len();
    v8_bytes[payload_len - 32 + 8..payload_len - 16]
        .copy_from_slice(&f64::NAN.to_bits().to_le_bytes());
    std::fs::write(&v8_path, &v8_bytes).unwrap();
    let err = Bm25Reader::open(&v8_path)
        .err()
        .expect("v8 must refuse stomped column bytes");
    assert!(
        err.to_string().contains("column:courthouse:vals") && err.to_string().contains("CRC"),
        "v8 refusal names the rotted section, not a symptom: {err}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Stores that declare SOME column kinds and skip the ones between them
/// and the geo group. Each skip is a distinct fallback branch a
/// full-kind store never takes: with every kind present the facet group
/// ends at the numeric group, never at the geos. One case per skip
/// boundary into the geo section, with dual-writer identity pinning both
/// writers' offset arithmetic on the same partial layouts. Open runs
/// full validation, so opening IS the tiling assertion.
#[test]
fn partial_kind_stores_tile_into_the_geo_section() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("geo_partial_kinds_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let analyzed = || AnalyzedDoc::body(vec![("rust".to_string(), 1u32, vec![(0u32, 4u32)])], 1);
    // (case, facets, numerics, map facets, map numerics, integers): geos
    // always present, so each case pins one kind's fallthrough to the
    // geo group.
    for (case, facets, numerics, map_facets, map_numerics, integers) in [
        ("geo_only", false, false, false, false, false),
        ("facets_geo", true, false, false, false, false),
        ("numerics_geo", false, true, false, false, false),
        ("mapfacets_geo", false, false, true, false, false),
        ("mapnumerics_geo", false, false, false, true, false),
        ("integers_geo", false, false, false, false, true),
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
        if map_numerics {
            store = store.with_columns(MapNumeric, &["attrs"]);
        }
        if integers {
            store = store.with_columns(Integer, &["citations"]);
        }
        let mut store = store.with_columns(Geo, &["courthouse"]);
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
        if map_numerics {
            store.set_map_numeric(0, 0, "boost", 2.0);
        }
        if integers {
            store.set_integer(0, 0, 7);
        }
        store.set_geo(0, 0, 12.5, -34.25);
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
        if map_numerics {
            builder = builder.with_columns(MapNumeric, &["attrs"]);
        }
        if integers {
            builder = builder.with_columns(Integer, &["citations"]);
        }
        let mut builder = builder.with_columns(Geo, &["courthouse"]);
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
        if map_numerics {
            builder.set_map_numeric(0, 0, "boost", 2.0);
        }
        if integers {
            builder.set_integer(0, 0, 7);
        }
        builder.set_geo(0, 0, 12.5, -34.25);
        let spill_path = dir.join(format!("{case}_spill.bm25"));
        builder.finish(&spill_path).unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap(),
            std::fs::read(&spill_path).unwrap(),
            "{case}: dual writers must agree on partial-kind layouts too"
        );

        let r = Bm25Reader::open(&path).unwrap_or_else(|e| panic!("{case}: {e}"));
        assert_eq!(r.geo_value(0, 0), Some((12.5, -34.25)), "{case}");
        assert_eq!(r.geo_bbox(0), (12.5, 12.5, -34.25, -34.25), "{case}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}
