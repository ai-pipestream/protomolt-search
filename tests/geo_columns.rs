//! Geo-point column and geo-filter acceptance tests
//! (`docs/geo-columns.md`): kind 5 through the v7 column table with a
//! bounding box validated against a full scan, bbox and radius filters
//! that remove documents exactly (edges inside, absence out) without
//! disturbing any block-max bound, and distance-decay score stages that
//! prune bitwise identically to the exhaustive oracle.

mod common;

use pipestream_search::bm25::{self, Bm25Params, CorpusStats};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::geo::{self, GeoFilters, GeoMetric, GeoRegion};
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    AddDocumentsRequest, Bm25Hit, Bm25SearchRequest, GeoBbox, GeoFilter, GeoPointValue, GeoRadius,
    QueryField, ScoreOp, ScoreStage,
};
use pipestream_search::postings::{AnalyzedDoc, Bm25Reader, Bm25Store, SpillBuilder};
use pipestream_search::scorefn::{ColumnRef, NumericRead, ScoreChain, Stage, StageOp};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

use common::{mock::start_mock_analysis, start_empty_node};

/// A document's geo points at ingest: (field, lat, lon) triples.
type Points = &'static [(&'static str, f64, f64)];

/// The controlled corpus: nine documents over three shards with a
/// "courthouse" geo column. Shard 2 declares NO geo field — the
/// heterogeneous-fleet case, whose documents hold no location and are
/// therefore inside no region, exactly and not by degradation.
///
/// The coordinates are chosen so every boundary rule can be hand
/// computed on the sphere `geo::EARTH_RADIUS_M` pins:
///
/// - d0/d1/d2 sit on three CORNERS of the unit box `[0,1] x [0,1]`, so
///   one filter pins all four inclusive edges at once,
/// - d1 and d2 are each exactly one degree of arc from the origin
///   `(0, 0)` (at the equator a degree of longitude is a degree of
///   latitude), so "distance exactly == meters is inside" has two
///   witnesses,
/// - d4 sits ON the north pole, where `cos(lat)` is zero,
/// - d6 sits at longitude 179.9, just west of the antimeridian, which
///   is reachable by an ordinary `[179, 180]` box and NOT by a
///   wraparound box (this increment refuses those),
/// - d5 has no point at all, and d7/d8 live on the column-less shard.
///
/// df("rust") = 7 (d0, d1, d2, d3, d5, d6, d7).
const SHARD_DOCS: [&[(&str, Points)]; 3] = [
    &[
        ("rust search rust fast", &[("courthouse", 0.0, 0.0)]),
        ("vector search rust", &[("courthouse", 0.0, 1.0)]),
        ("rust rust", &[("courthouse", 1.0, 0.0)]),
    ],
    &[
        ("search engines love rust", &[("courthouse", 10.0, 10.0)]),
        ("vector vector vector", &[("courthouse", 90.0, 45.0)]),
        ("rust fast", &[]),
        ("rust near the dateline", &[("courthouse", -45.0, 179.9)]),
    ],
    &[("rust", &[]), ("nothing relevant here", &[])],
];

/// Global slot offsets, matching `SHARD_DOCS`' shard sizes.
const OFFSETS: [u64; 3] = [0, 3, 7];

async fn add_documents_geo(
    addr: &str,
    docs: &[(&str, Points)],
) -> Result<pipestream_search::pb::AddDocumentsResponse, tonic::Status> {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    // Capacity above the largest batch this file sends: the whole
    // stream is queued BEFORE `add_documents` starts draining it, so a
    // channel smaller than the batch deadlocks on the send.
    let (tx, rx) = mpsc::channel(64);
    for (text, points) in docs {
        tx.send(AddDocumentsRequest {
            unsigned_integers: Vec::new(),
            original_source: None,
            source_chunk_ordinal: None,
            identity: None,
            collection: String::new(),
            cased_field: String::new(),
            sentence_fields: Vec::new(),
            materialize: None,
            text: text.to_string(),
            analysis: None,
            lineage: None,
            fields: Vec::new(),
            facets: Vec::new(),
            numerics: Vec::new(),
            map_facets: Vec::new(),
            map_numerics: Vec::new(),
            integers: Vec::new(),
            timestamps: Vec::new(),
            geo_points: points
                .iter()
                .map(|(field, lat, lon)| GeoPointValue {
                    field: field.to_string(),
                    lat: *lat,
                    lon: *lon,
                })
                .collect(),
            quality: None,
            geography: None,
            phrases: Vec::new(),
            phrase_fingerprint: 0,
            phrase_field: String::new(),
            position_fields: Vec::new(),
            bigram_fields: Vec::new(),
        })
        .await
        .unwrap();
    }
    drop(tx);
    client
        .add_documents(ReceiverStream::new(rx))
        .await
        .map(|r| r.into_inner())
}

/// Three shards, the geo table declared on shards 0 and 1 only.
async fn start_geo_shards(
    analysis: &str,
) -> (
    Vec<String>,
    Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
) {
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for (i, &offset) in OFFSETS.iter().enumerate() {
        let geo_fields = if i < 2 {
            vec!["courthouse".to_string()]
        } else {
            Vec::new()
        };
        let (addr, handle) = start_empty_node(NodeConfig {
            slot_offset: offset,
            analysis_addr: Some(analysis.to_string()),
            geo_fields,
            ..Default::default()
        })
        .await;
        addrs.push(addr);
        handles.push(handle);
    }
    for (i, docs) in SHARD_DOCS.iter().enumerate() {
        add_documents_geo(&addrs[i], docs).await.unwrap();
    }
    (addrs, handles)
}

fn bbox_filter(column: &str, min_lat: f64, max_lat: f64, min_lon: f64, max_lon: f64) -> GeoFilter {
    GeoFilter {
        column: column.to_string(),
        region: Some(pipestream_search::pb::geo_filter::Region::Bbox(GeoBbox {
            min_lat,
            max_lat,
            min_lon,
            max_lon,
        })),
    }
}

fn radius_filter(column: &str, lat: f64, lon: f64, meters: f64, metric: GeoMetric) -> GeoFilter {
    GeoFilter {
        column: column.to_string(),
        region: Some(pipestream_search::pb::geo_filter::Region::Radius(
            GeoRadius {
                lat,
                lon,
                meters,
                metric: match metric {
                    GeoMetric::Haversine => pipestream_search::pb::GeoMetric::Haversine as i32,
                    GeoMetric::Manhattan => pipestream_search::pb::GeoMetric::Manhattan as i32,
                },
            },
        )),
    }
}

fn geo_stage(column: &str, op: ScoreOp, lat: f64, lon: f64, scale: f64) -> ScoreStage {
    ScoreStage {
        op: op as i32,
        column: column.to_string(),
        key: String::new(),
        weight: 0.0,
        origin: 0.0,
        scale,
        origin_lat: lat,
        origin_lon: lon,
    }
}

fn ids(hits: &[Bm25Hit]) -> Vec<u64> {
    let mut v: Vec<u64> = hits.iter().map(|h| h.doc_id).collect();
    v.sort_unstable();
    v
}

fn hit_signature(hits: &[Bm25Hit]) -> Vec<(u64, u32)> {
    hits.iter().map(|h| (h.doc_id, h.score.to_bits())).collect()
}

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
    let mut store = Bm25Store::with_fields(&["body"])
        .with_facets(&["court"])
        .with_numerics(&["date"])
        .with_map_facets(&["meta"])
        .with_map_numerics(&["attrs"])
        .with_integers(&["citations"])
        .with_geos(&["courthouse"]);
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

    let mut builder = SpillBuilder::create_with_fields(&dir.join("spill.build"), &["body"])
        .unwrap()
        .with_facet_fields(&["court"])
        .with_numeric_fields(&["date"])
        .with_map_facet_fields(&["meta"])
        .with_map_numeric_fields(&["attrs"])
        .with_integer_fields(&["citations"])
        .with_geo_fields(&["courthouse"]);
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
    let mut empty = Bm25Store::with_fields(&["body"]).with_geos(&["courthouse"]);
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

    let mut store = Bm25Store::with_fields(&["body"]).with_geos(&["courthouse"]);
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
        let mut store = Bm25Store::with_fields(&["body"]);
        if facets {
            store = store.with_facets(&["court"]);
        }
        if numerics {
            store = store.with_numerics(&["date"]);
        }
        if map_facets {
            store = store.with_map_facets(&["meta"]);
        }
        if map_numerics {
            store = store.with_map_numerics(&["attrs"]);
        }
        if integers {
            store = store.with_integers(&["citations"]);
        }
        let mut store = store.with_geos(&["courthouse"]);
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
            SpillBuilder::create_with_fields(&dir.join(format!("{case}.build")), &["body"])
                .unwrap();
        if facets {
            builder = builder.with_facet_fields(&["court"]);
        }
        if numerics {
            builder = builder.with_numeric_fields(&["date"]);
        }
        if map_facets {
            builder = builder.with_map_facet_fields(&["meta"]);
        }
        if map_numerics {
            builder = builder.with_map_numeric_fields(&["attrs"]);
        }
        if integers {
            builder = builder.with_integer_fields(&["citations"]);
        }
        let mut builder = builder.with_geo_fields(&["courthouse"]);
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

/// Distributed geo filters: exact over a heterogeneous fleet where one
/// shard has no geo column at all, with every boundary rule the wire
/// promises pinned, and bitwise equal to the same corpus in one shard.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_geo_filters_are_exact_and_boundary_correct() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addrs, handles) = start_geo_shards(&analysis).await;
    let coordinator = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());

    // Unfiltered, "rust" matches seven documents across all three
    // shards — including the two on the geo-less shard 2.
    let unfiltered = coordinator.fanout_bm25("rust", 10, None).await.unwrap();
    assert_eq!(ids(&unfiltered), vec![0, 1, 2, 3, 5, 6, 7]);

    // The unit box: d0 (0,0), d1 (0,1) and d2 (1,0) sit on three of its
    // corners, so this one assertion pins ALL FOUR edges as inclusive.
    // d3 and d6 are outside, d5 has no point, and d7 lives on a shard
    // with no geo column — the last two fail for the same reason, and
    // it is exact: no location is inside no region.
    let (hits, _, _) = coordinator
        .fanout_bm25_faceted(
            "rust",
            10,
            None,
            0.0,
            &[],
            &[],
            &[],
            &[],
            &[bbox_filter("courthouse", 0.0, 1.0, 0.0, 1.0)],
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        ids(&hits),
        vec![0, 1, 2],
        "bbox edges are inclusive; absence and a missing column both fail"
    );
    // Shrinking each edge by a hair drops every corner it moved past,
    // which is what "inclusive" has to mean. d0 and d1 share latitude 0
    // and d0 and d2 share longitude 0, so the south and west edges take
    // two documents each and the north and east edges take one.
    for (case, want, f) in [
        (
            "south",
            vec![2u64],
            bbox_filter("courthouse", 1e-9, 1.0, 0.0, 1.0),
        ),
        (
            "north",
            vec![0, 1],
            bbox_filter("courthouse", 0.0, 1.0 - 1e-9, 0.0, 1.0),
        ),
        (
            "west",
            vec![1],
            bbox_filter("courthouse", 0.0, 1.0, 1e-9, 1.0),
        ),
        (
            "east",
            vec![0, 2],
            bbox_filter("courthouse", 0.0, 1.0, 0.0, 1.0 - 1e-9),
        ),
    ] {
        let (hits, _, _) = coordinator
            .fanout_bm25_faceted("rust", 10, None, 0.0, &[], &[], &[], &[], &[f], None)
            .await
            .unwrap();
        assert_eq!(ids(&hits), want, "{case} edge moved past its corner");
    }

    // Radius, both metrics: `distance <= meters` is inside. At the
    // equator one degree of longitude is one degree of latitude, so d1
    // and d2 are BOTH exactly at the boundary.
    let one_degree = geo::haversine_meters(0.0, 0.0, 0.0, 1.0);
    let (hits, _, _) = coordinator
        .fanout_bm25_faceted(
            "rust",
            10,
            None,
            0.0,
            &[],
            &[],
            &[],
            &[],
            &[radius_filter(
                "courthouse",
                0.0,
                0.0,
                one_degree,
                GeoMetric::Haversine,
            )],
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        ids(&hits),
        vec![0, 1, 2],
        "distance exactly == meters is in"
    );
    let (hits, _, _) = coordinator
        .fanout_bm25_faceted(
            "rust",
            10,
            None,
            0.0,
            &[],
            &[],
            &[],
            &[],
            &[radius_filter(
                "courthouse",
                0.0,
                0.0,
                one_degree.next_down(),
                GeoMetric::Haversine,
            )],
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        ids(&hits),
        vec![0],
        "one ULP tighter and the boundary drops"
    );
    let (hits, _, _) = coordinator
        .fanout_bm25_faceted(
            "rust",
            10,
            None,
            0.0,
            &[],
            &[],
            &[],
            &[],
            &[radius_filter(
                "courthouse",
                0.0,
                0.0,
                geo::M_PER_DEG_LAT,
                GeoMetric::Manhattan,
            )],
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        ids(&hits),
        vec![0, 1, 2],
        "Manhattan pins the same boundary"
    );

    // The pole, where cos(lat) is zero: a box whose northern edge IS 90
    // contains it. ("vector" matches d1 and d4.)
    let (hits, _, _) = coordinator
        .fanout_bm25_faceted(
            "vector",
            10,
            None,
            0.0,
            &[],
            &[],
            &[],
            &[],
            &[bbox_filter("courthouse", 89.0, 90.0, -180.0, 180.0)],
            None,
        )
        .await
        .unwrap();
    assert_eq!(ids(&hits), vec![4], "lat exactly 90 is inside the box");

    // Just west of the antimeridian, reached by an ORDINARY box whose
    // eastern edge is 180. The wraparound box that would also reach it
    // is refused (see the refusal matrix).
    let (hits, _, _) = coordinator
        .fanout_bm25_faceted(
            "rust",
            10,
            None,
            0.0,
            &[],
            &[],
            &[],
            &[],
            &[bbox_filter("courthouse", -46.0, -44.0, 179.0, 180.0)],
            None,
        )
        .await
        .unwrap();
    assert_eq!(ids(&hits), vec![6], "lon 179.9 needs no wraparound");

    // ALL filters must pass: the intersection, not the union.
    let (hits, _, _) = coordinator
        .fanout_bm25_faceted(
            "rust",
            10,
            None,
            0.0,
            &[],
            &[],
            &[],
            &[],
            &[
                bbox_filter("courthouse", 0.0, 1.0, 0.0, 1.0),
                bbox_filter("courthouse", 0.5, 90.0, -180.0, 180.0),
            ],
            None,
        )
        .await
        .unwrap();
    assert_eq!(ids(&hits), vec![2], "AND semantics, not OR");

    // Distributed == monolith, bitwise, with a filter applied.
    let (mono_addr, mono) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        geo_fields: vec!["courthouse".to_string()],
        ..Default::default()
    })
    .await;
    // The monolith declares the column, so the two documents that live
    // on the geo-less shard 2 simply carry no point there either --
    // "absent" and "column absent" have to agree, and this is the test
    // that they do.
    let all: Vec<(&str, Points)> = SHARD_DOCS.concat();
    add_documents_geo(&mono_addr, &all).await.unwrap();
    let monolithic = CoordinatorServiceImpl::new(vec![mono_addr])
        .with_bm25(Some(analysis.clone()), Default::default());
    for text in ["rust", "search rust", "vector"] {
        for filters in [
            vec![bbox_filter("courthouse", -90.0, 90.0, -180.0, 180.0)],
            vec![bbox_filter("courthouse", 0.0, 1.0, 0.0, 1.0)],
            vec![radius_filter(
                "courthouse",
                0.0,
                0.0,
                2_000_000.0,
                GeoMetric::Haversine,
            )],
        ] {
            let (got, _, _) = coordinator
                .fanout_bm25_faceted(text, 10, None, 0.0, &[], &[], &[], &[], &filters, None)
                .await
                .unwrap();
            let (want, _, _) = monolithic
                .fanout_bm25_faceted(text, 10, None, 0.0, &[], &[], &[], &[], &filters, None)
                .await
                .unwrap();
            assert_eq!(
                hit_signature(&got),
                hit_signature(&want),
                "text {text:?}: distributed filtered hits must be bitwise equal"
            );
        }
    }

    // Facet counting is NOT narrowed by geo filters in this increment:
    // counts are over the match set as already defined, and the doc
    // says so loudly. Pinned here so a future CEL layer changing it is
    // a deliberate, visible change.
    let (hits, facets, _) = coordinator
        .fanout_bm25_faceted(
            "rust",
            10,
            None,
            0.0,
            &[],
            &[],
            &[],
            &[],
            &[bbox_filter("courthouse", 0.0, 1.0, 0.0, 1.0)],
            None,
        )
        .await
        .unwrap();
    assert_eq!(ids(&hits), vec![0, 1, 2]);
    assert!(facets.is_empty(), "none were requested");

    // The fused route carries filters too, forwarded like range facets.
    let fields = vec![QueryField {
        field: "body".to_string(),
        analysis: None,
        weight: 1.0,
        k1: 0.0,
        b: 0.0,
        phrase: None,
        prefixes: Vec::new(),
        synonyms: Vec::new(),
        synonyms_off: false,
    }];
    let (hits, _, _) = coordinator
        .fanout_bm25_fused_faceted(
            "rust",
            10,
            &fields,
            0.0,
            &[],
            &[],
            &[],
            &[bbox_filter("courthouse", 0.0, 1.0, 0.0, 1.0)],
            None,
        )
        .await
        .unwrap();
    assert_eq!(ids(&hits), vec![0, 1, 2], "the fused route filters too");

    // The public RPC carries filters end to end.
    let resp = SearchService::bm25_search(
        &coordinator,
        Request::new(Bm25SearchRequest {
            explain: false,
            collection: String::new(),
            highlight: None,
            projections: Vec::new(),
            filter: String::new(),
            text: "rust".to_string(),
            k: 10,
            analysis: None,
            min_score: 0.0,
            fields: Vec::new(),
            facet_fields: Vec::new(),
            score_stages: Vec::new(),
            map_facet_fields: Vec::new(),
            range_facet_fields: Vec::new(),
            geo_filters: vec![bbox_filter("courthouse", 0.0, 1.0, 0.0, 1.0)],
            stats_fields: Vec::new(),
            cardinality_fields: Vec::new(),
            phrase: None,
            prefixes: Vec::new(),
            synonyms: Vec::new(),
            synonyms_off: false,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(ids(&resp.hits), vec![0, 1, 2]);

    for h in handles {
        h.abort();
    }
    mono.abort();
    mock.abort();
}

/// Every geo-filter refusal, each naming the column and the knob. The
/// coordinator runs the shard-free half BEFORE its zero-term/k=0 early
/// return on both fan-out routes, so a malformed filter cannot hide
/// behind an empty Ok.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn geo_filter_refusals_are_loud() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addrs, handles) = start_geo_shards(&analysis).await;
    let coordinator = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());

    // A column NO shard knows is a typo, not an empty result set.
    let err = coordinator
        .fanout_bm25_faceted(
            "rust",
            10,
            None,
            0.0,
            &[],
            &[],
            &[],
            &[],
            &[bbox_filter("courthosue", 0.0, 1.0, 0.0, 1.0)],
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("courthosue") && err.message().contains("--geo-fields"),
        "the refusal names the column and the knob: {}",
        err.message()
    );

    // A column only SOME shards know is the heterogeneous fleet, and is
    // fine — shard 2 has no geo column and contributes nothing.
    coordinator
        .fanout_bm25_faceted(
            "rust",
            10,
            None,
            0.0,
            &[],
            &[],
            &[],
            &[],
            &[bbox_filter("courthouse", 0.0, 1.0, 0.0, 1.0)],
            None,
        )
        .await
        .expect("a partially known column is exact, not an error");

    let antimeridian = GeoFilter {
        column: "courthouse".to_string(),
        region: Some(pipestream_search::pb::geo_filter::Region::Bbox(GeoBbox {
            min_lat: -46.0,
            max_lat: -44.0,
            min_lon: 179.0,
            max_lon: -179.0,
        })),
    };
    let no_region = GeoFilter {
        column: "courthouse".to_string(),
        region: None,
    };
    let unspecified_metric = GeoFilter {
        column: "courthouse".to_string(),
        region: Some(pipestream_search::pb::geo_filter::Region::Radius(
            GeoRadius {
                lat: 0.0,
                lon: 0.0,
                meters: 1000.0,
                metric: 0,
            },
        )),
    };
    for (bad, needle) in [
        (antimeridian, "antimeridian"),
        (
            bbox_filter("courthouse", 1.0, 0.0, 0.0, 1.0),
            "above max_lat",
        ),
        (bbox_filter("courthouse", 0.0, 91.0, 0.0, 1.0), "[-90, 90]"),
        (
            bbox_filter("courthouse", 0.0, 1.0, 0.0, 181.0),
            "[-180, 180]",
        ),
        (
            bbox_filter("courthouse", f64::NAN, 1.0, 0.0, 1.0),
            "not a finite degree",
        ),
        (
            radius_filter("courthouse", 0.0, 0.0, 0.0, GeoMetric::Haversine),
            "above zero",
        ),
        (
            radius_filter("courthouse", 0.0, 0.0, -5.0, GeoMetric::Haversine),
            "above zero",
        ),
        (
            radius_filter("courthouse", 0.0, 0.0, f64::INFINITY, GeoMetric::Haversine),
            "finite",
        ),
        (no_region, "no region set"),
        (unspecified_metric, "unknown geo metric"),
        (bbox_filter("", 0.0, 1.0, 0.0, 1.0), "names the geo column"),
    ] {
        let err = coordinator
            .fanout_bm25_faceted("rust", 10, None, 0.0, &[], &[], &[], &[], &[bad], None)
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{needle}");
        assert!(
            err.message().contains(needle),
            "expected {needle:?} in: {}",
            err.message()
        );
    }

    // Filter validation needs no shard, so the coordinator's zero-term
    // and k = 0 early returns must not swallow it into an empty Ok --
    // on BOTH fan-out routes.
    let bad = bbox_filter("courthouse", 0.0, 1.0, 179.0, -179.0);
    let bad_one = std::slice::from_ref(&bad);
    for (text, k) in [("", 10u32), ("rust", 0)] {
        let err = coordinator
            .fanout_bm25_faceted(text, k, None, 0.0, &[], &[], &[], &[], bad_one, None)
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            tonic::Code::InvalidArgument,
            "text {text:?}, k {k}: the early return must not hide the refusal"
        );
    }
    let fields = vec![QueryField {
        field: "body".to_string(),
        analysis: None,
        weight: 1.0,
        k1: 0.0,
        b: 0.0,
        phrase: None,
        prefixes: Vec::new(),
        synonyms: Vec::new(),
        synonyms_off: false,
    }];
    let err = coordinator
        .fanout_bm25_fused_faceted("", 10, &fields, 0.0, &[], &[], &[], bad_one, None)
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::InvalidArgument,
        "the fused route honors the same rule before its own early return"
    );

    for h in handles {
        h.abort();
    }
    mock.abort();
}

/// The geo ingest refusal matrix: coordinates off the globe, non-finite
/// coordinates, an unknown field, and a repeat within one document.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn geo_ingest_refusals_are_loud() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addr, handle) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        geo_fields: vec!["courthouse".to_string()],
        ..Default::default()
    })
    .await;

    for (points, needle) in [
        (
            &[("courthouse", 90.000_001, 0.0)][..],
            "latitude 90.000001 is not a finite degree in [-90, 90]",
        ),
        (&[("courthouse", -90.5, 0.0)][..], "[-90, 90]"),
        (&[("courthouse", 0.0, 180.5)][..], "[-180, 180]"),
        (&[("courthouse", f64::NAN, 0.0)][..], "[-90, 90]"),
        (&[("courthouse", 0.0, f64::INFINITY)][..], "[-180, 180]"),
        (
            &[("courthosue", 0.0, 0.0)][..],
            "unknown geo field \"courthosue\"",
        ),
        (
            &[("courthouse", 0.0, 0.0), ("courthouse", 1.0, 1.0)][..],
            "repeats in one document",
        ),
    ] {
        let err = add_documents_geo(&addr, &[("rust", points)])
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{needle}");
        assert!(
            err.message().contains(needle),
            "expected {needle:?} in: {}",
            err.message()
        );
    }
    // The pole and the antimeridian ARE valid coordinates, and ingest
    // must not confuse "extreme" with "invalid".
    add_documents_geo(
        &addr,
        &[
            ("rust north", &[("courthouse", 90.0, 180.0)]),
            ("rust south", &[("courthouse", -90.0, -180.0)]),
            ("rust nowhere", &[]),
        ],
    )
    .await
    .expect("the corners of the coordinate domain are points, not errors");

    // The geo-field knob is named in the unknown-field refusal even on a
    // shard with no geo table at all.
    let (bare_addr, bare) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        ..Default::default()
    })
    .await;
    let err = add_documents_geo(&bare_addr, &[("rust", &[("courthouse", 0.0, 0.0)])])
        .await
        .unwrap_err();
    assert!(err.message().contains("--geo-fields"), "{}", err.message());

    handle.abort();
    bare.abort();
    mock.abort();
}

/// [`NumericRead`] over an open reader, as the node's shard wrapper
/// provides it in production.
struct ReaderNumerics<'a>(&'a Bm25Reader);
impl NumericRead for ReaderNumerics<'_> {
    fn value(&self, ni: usize, doc_id: u32) -> Option<f64> {
        self.0.numeric_value(ni, doc_id)
    }
    fn map_value(&self, column: usize, key_ord: u32, doc_id: u32) -> Option<f64> {
        self.0.map_numeric_value(column, key_ord, doc_id)
    }
    fn int_value(&self, ii: usize, doc_id: u32) -> Option<i64> {
        self.0.integer_value(ii, doc_id)
    }
    fn geo_value(&self, gi: usize, doc_id: u32) -> Option<(f64, f64)> {
        self.0.geo_value(gi, doc_id)
    }
    fn facet_ord(&self, fi: usize, doc_id: u32) -> Option<u32> {
        self.0.facet_ord(fi, doc_id)
    }
    fn map_facet_value_ord(&self, ci: usize, key_ord: u32, doc_id: u32) -> Option<u32> {
        self.0.map_facet_value_ord(ci, key_ord, doc_id)
    }
}

/// The exactness gate: on a file-backed shard (impacts present, so the
/// block-max path really prunes) the filtered, geo-decayed pruned
/// scorer is bitwise identical to the filtered, geo-decayed exhaustive
/// oracle — at several k, under seeded floors, and with the filter both
/// wide open and narrow enough to remove most of the corpus.
#[test]
fn geo_filtered_decayed_pruned_matches_exhaustive_bitwise() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("geo_pruned_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let n = 3000u32;
    let tf_a = |d: u32| 1 + (u64::from(d) * 2654435761 % 7) as u32;
    // A deterministic spread over a real chunk of the globe.
    let lat = |d: u32| -60.0 + f64::from(d % 121);
    let lon = |d: u32| -179.0 + f64::from(d % 359);
    let mut store = Bm25Store::with_fields(&["body"]).with_geos(&["courthouse"]);
    for doc in 0..n {
        let mut terms = vec![("a".to_string(), tf_a(doc), Vec::new())];
        if doc % 3 == 0 {
            terms.push(("b".to_string(), 1 + doc % 3, Vec::new()));
        }
        if doc % 61 == 0 {
            terms.push(("c".to_string(), 1, Vec::new()));
        }
        let len: u32 = terms.iter().map(|(_, tf, _)| tf).sum();
        store.add_document(doc, ".".to_string(), AnalyzedDoc::body(terms, len));
        // Every 7th document has no point at all, which is the case
        // that forces the decay's bound lift to 1 and makes every such
        // document fail every filter.
        if doc % 7 != 0 {
            store.set_geo(0, doc, lat(doc), lon(doc));
        }
    }
    let path = dir.join("geo.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let body = reader.field(0);
    let cols = ReaderNumerics(&reader);
    let gi = reader.geo_index("courthouse").unwrap();

    let stats = CorpusStats {
        doc_count: u64::from(n),
        total_doc_length: (0..n).map(|d| u64::from(tf_a(d))).sum::<u64>()
            + (0..n)
                .filter(|d| d % 3 == 0)
                .map(|d| u64::from(1 + d % 3))
                .sum::<u64>()
            + (0..n).filter(|d| d % 61 == 0).count() as u64,
        dfs: vec![n, n.div_ceil(3), n.div_ceil(61)],
    };
    let terms: Vec<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
    let params = Bm25Params::default();
    let signature = |docs: &[bm25::ScoredDoc]| -> Vec<(u32, u64)> {
        docs.iter().map(|d| (d.doc_id, d.score.to_bits())).collect()
    };

    let stage = |metric| Stage {
        op: StageOp::MultGeoDecay {
            metric,
            origin_lat: 38.8977,
            origin_lon: -77.0365,
            scale: 1_500_000.0,
        },
        column: Some(ColumnRef::Geo(gi)),
        min_max: (f64::NAN, f64::NAN),
    };
    for metric in [GeoMetric::Haversine, GeoMetric::Manhattan] {
        let chain = ScoreChain {
            stages: vec![stage(metric)],
        };
        let chain_ctx = Some((&chain, &cols as &dyn NumericRead));
        for region in [
            // Everything: only the pointless documents are removed.
            GeoRegion::Bbox {
                min_lat: -90.0,
                max_lat: 90.0,
                min_lon: -180.0,
                max_lon: 180.0,
            },
            // A band that keeps a minority of the corpus.
            GeoRegion::Bbox {
                min_lat: 0.0,
                max_lat: 20.0,
                min_lon: -180.0,
                max_lon: 0.0,
            },
            GeoRegion::Radius {
                lat: 38.8977,
                lon: -77.0365,
                meters: 3_000_000.0,
                metric: GeoMetric::Haversine,
            },
            GeoRegion::Radius {
                lat: 0.0,
                lon: 0.0,
                meters: 900_000.0,
                metric: GeoMetric::Manhattan,
            },
        ] {
            let filters = pipestream_search::filter::DocFilter {
                deleted: None,
                geo: GeoFilters {
                    filters: vec![pipestream_search::geo::GeoFilter {
                        column: Some(gi),
                        region,
                    }],
                },
                pred: None,
                phrase: Vec::new(),
            };
            let filter_ctx = Some((&filters, &cols as &dyn NumericRead));
            for k in [1usize, 5, 50] {
                let exhaustive = bm25::top_k_exhaustive_chained_filtered(
                    &body, &terms, &stats, params, k, chain_ctx, filter_ctx,
                );
                let mut prune = bm25::PruneStats::default();
                let pruned = bm25::top_k_pruned_chained_filtered_stats(
                    &body,
                    &terms,
                    &stats,
                    params,
                    k,
                    f64::NEG_INFINITY,
                    chain_ctx,
                    filter_ctx,
                    &mut prune,
                );
                assert_eq!(
                    signature(&exhaustive),
                    signature(&pruned),
                    "{metric:?} k={k} {region:?}: filtered pruned != filtered exhaustive"
                );
                // A seeded floor must not change the answer either: the
                // floor is a lower bound on the k-th best SURVIVOR, and
                // the filter never raised it.
                if let Some(kth) = exhaustive.last() {
                    let mut prune = bm25::PruneStats::default();
                    let seeded = bm25::top_k_pruned_chained_filtered_stats(
                        &body, &terms, &stats, params, k, kth.score, chain_ctx, filter_ctx,
                        &mut prune,
                    );
                    assert_eq!(
                        signature(&exhaustive),
                        signature(&seeded),
                        "{metric:?} k={k}: seeded floor changed the filtered answer"
                    );
                }
                // Every survivor really is inside the region, and every
                // document that is inside and outranks the k-th
                // survivor really is there. The first half is the
                // filter working; the second is it not over-removing.
                for d in &pruned {
                    let (dlat, dlon) = cols.geo_value(gi, d.doc_id).expect("survivor has a point");
                    assert!(region.contains(dlat, dlon), "survivor outside the region");
                }
                // ...and the filter is not vacuous: the unfiltered
                // top-k differs, so the assertions above have something
                // to be about.
                let unfiltered = bm25::top_k_pruned_chained_filtered_stats(
                    &body,
                    &terms,
                    &stats,
                    params,
                    k,
                    f64::NEG_INFINITY,
                    chain_ctx,
                    None,
                    &mut bm25::PruneStats::default(),
                );
                assert_ne!(
                    signature(&unfiltered),
                    signature(&pruned),
                    "{metric:?} k={k} {region:?}: this filter removed nothing"
                );
            }
        }
    }

    // With no filter and no chain the filtered entry points are
    // bit-identical to the plain ones: the additions are gated, not
    // forked.
    for k in [1usize, 10] {
        let plain = bm25::top_k_pruned(&body, &terms, &stats, params, k, f64::NEG_INFINITY);
        let mut prune = bm25::PruneStats::default();
        let gated = bm25::top_k_pruned_chained_filtered_stats(
            &body,
            &terms,
            &stats,
            params,
            k,
            f64::NEG_INFINITY,
            None,
            None,
            &mut prune,
        );
        assert_eq!(signature(&plain), signature(&gated), "k={k}: ungated drift");
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Distance-decay stages, distributed: bitwise equal to the monolith,
/// with absence as exact identity on the shard that has no geo column,
/// and a chain that actually reorders (otherwise the test proves
/// nothing).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_geo_decay_matches_monolith() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addrs, handles) = start_geo_shards(&analysis).await;
    let distributed = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());

    let (mono_addr, mono) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        geo_fields: vec!["courthouse".to_string()],
        ..Default::default()
    })
    .await;
    let all: Vec<(&str, Points)> = SHARD_DOCS.concat();
    add_documents_geo(&mono_addr, &all).await.unwrap();
    let monolithic = CoordinatorServiceImpl::new(vec![mono_addr])
        .with_bm25(Some(analysis.clone()), Default::default());

    for op in [
        ScoreOp::MultGeoDecayHaversine,
        ScoreOp::MultGeoDecayManhattan,
    ] {
        // Origin at (1, 0): d2 sits ON it (factor 1), d0 and d1 are one
        // degree away, d3 and d6 are far.
        let stages = vec![geo_stage("courthouse", op, 1.0, 0.0, 100_000.0)];
        for text in ["rust", "search rust", "vector"] {
            let (got, _, _) = distributed
                .fanout_bm25_faceted(text, 10, None, 0.0, &[], &[], &[], &stages, &[], None)
                .await
                .unwrap();
            let (want, _, _) = monolithic
                .fanout_bm25_faceted(text, 10, None, 0.0, &[], &[], &[], &stages, &[], None)
                .await
                .unwrap();
            assert_eq!(
                hit_signature(&got),
                hit_signature(&want),
                "{op:?} text {text:?}: distributed != monolith"
            );
        }

        // The chain must actually do something: the same match set, and
        // d2 (distance 0, factor 1) climbing over documents that beat it
        // unchained.
        let unchained = distributed.fanout_bm25("rust", 10, None).await.unwrap();
        let (chained, _, _) = distributed
            .fanout_bm25_faceted("rust", 10, None, 0.0, &[], &[], &[], &stages, &[], None)
            .await
            .unwrap();
        assert_eq!(ids(&unchained), ids(&chained), "a decay removes nothing");
        assert_eq!(chained[0].doc_id, 2, "{op:?}: the doc at the origin wins");
        // Documents with no point (d5) and documents on the shard with
        // no column (d7) pass through the stage unchanged — exact
        // identity, not degradation.
        for id in [5u64, 7] {
            let before = unchained.iter().find(|h| h.doc_id == id).unwrap().score;
            let after = chained.iter().find(|h| h.doc_id == id).unwrap().score;
            assert_eq!(
                before.to_bits(),
                after.to_bits(),
                "{op:?}: doc {id} has no point, so the stage is identity"
            );
        }
        // And a document WITH a point away from the origin really is
        // scaled down.
        let before = unchained.iter().find(|h| h.doc_id == 3).unwrap().score;
        let after = chained.iter().find(|h| h.doc_id == 3).unwrap().score;
        assert!(after < before, "{op:?}: doc 3 is far from the origin");
    }

    // Stage refusals, each naming the stage.
    for (bad, needle) in [
        (
            geo_stage("courthouse", ScoreOp::MultGeoDecayHaversine, 0.0, 0.0, 0.0),
            "scale > 0",
        ),
        (
            geo_stage(
                "courthouse",
                ScoreOp::MultGeoDecayHaversine,
                91.0,
                0.0,
                10.0,
            ),
            "[-90, 90]",
        ),
        (
            geo_stage(
                "courthouse",
                ScoreOp::MultGeoDecayManhattan,
                0.0,
                f64::NAN,
                10.0,
            ),
            "[-180, 180]",
        ),
    ] {
        let err = distributed
            .fanout_bm25_faceted("rust", 10, None, 0.0, &[], &[], &[], &[bad], &[], None)
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{needle}");
        assert!(
            err.message().contains(needle),
            "expected {needle:?} in: {}",
            err.message()
        );
    }
    // A geo column NO shard knows is refused by the chain's typo rule.
    let err = distributed
        .fanout_bm25_faceted(
            "rust",
            10,
            None,
            0.0,
            &[],
            &[],
            &[],
            &[geo_stage(
                "courthosue",
                ScoreOp::MultGeoDecayHaversine,
                0.0,
                0.0,
                10.0,
            )],
            &[],
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("courthosue"), "{}", err.message());

    for h in handles {
        h.abort();
    }
    mono.abort();
    mock.abort();
}

/// The FUSED twin of the exactness gate above: on a file-backed shard
/// (impacts present, so the fused block-max path really prunes), the
/// filtered fused pruned scorer is bitwise identical to the filtered
/// fused exhaustive oracle. The fused scorer has its own insertion
/// gate, and the flat-path oracle cannot vouch for it — while "fused
/// route plus filters over file shards" is exactly the production
/// shape, since the distributed tests above run on heap shards whose
/// missing impact surfaces fall the node back to the exhaustive path.
#[test]
fn geo_filtered_fused_pruned_matches_exhaustive_bitwise() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("geo_fused_pruned_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    // The same corpus shape as the flat gate: term "a" everywhere with
    // a spread of tfs, "b" on every 3rd document, "c" rare, points over
    // a real chunk of the globe, and every 7th document pointless.
    let n = 3000u32;
    let tf_a = |d: u32| 1 + (u64::from(d) * 2654435761 % 7) as u32;
    let lat = |d: u32| -60.0 + f64::from(d % 121);
    let lon = |d: u32| -179.0 + f64::from(d % 359);
    let mut store = Bm25Store::with_fields(&["body"]).with_geos(&["courthouse"]);
    for doc in 0..n {
        let mut terms = vec![("a".to_string(), tf_a(doc), Vec::new())];
        if doc % 3 == 0 {
            terms.push(("b".to_string(), 1 + doc % 3, Vec::new()));
        }
        if doc % 61 == 0 {
            terms.push(("c".to_string(), 1, Vec::new()));
        }
        let len: u32 = terms.iter().map(|(_, tf, _)| tf).sum();
        store.add_document(doc, ".".to_string(), AnalyzedDoc::body(terms, len));
        if doc % 7 != 0 {
            store.set_geo(0, doc, lat(doc), lon(doc));
        }
    }
    let path = dir.join("geo_fused.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let body = reader.field(0);
    let cols = ReaderNumerics(&reader);
    let gi = reader.geo_index("courthouse").unwrap();

    let total_doc_length: u64 = (0..n).map(|d| u64::from(tf_a(d))).sum::<u64>()
        + (0..n)
            .filter(|d| d % 3 == 0)
            .map(|d| u64::from(1 + d % 3))
            .sum::<u64>()
        + (0..n).filter(|d| d % 61 == 0).count() as u64;
    // Two legs over the body field with distinct term sets and non-unit
    // weights, so the fused accumulation and its pair cursors are
    // genuinely exercised rather than collapsing to the flat scorer.
    let leg1_terms: Vec<String> = ["a", "b"].iter().map(|s| s.to_string()).collect();
    let leg2_terms: Vec<String> = vec!["c".to_string()];
    let fields = vec![
        bm25::FieldQuery {
            index: &body,
            terms: &leg1_terms,
            stats: CorpusStats {
                doc_count: u64::from(n),
                total_doc_length,
                dfs: vec![n, n.div_ceil(3)],
            },
            params: Bm25Params::default(),
            weight: 0.75,
        },
        bm25::FieldQuery {
            index: &body,
            terms: &leg2_terms,
            stats: CorpusStats {
                doc_count: u64::from(n),
                total_doc_length,
                dfs: vec![n.div_ceil(61)],
            },
            params: Bm25Params::default(),
            weight: 1.25,
        },
    ];
    let signature = |docs: &[bm25::FusedDoc]| -> Vec<(u32, u64)> {
        docs.iter().map(|d| (d.doc_id, d.score.to_bits())).collect()
    };

    for region in [
        // Everything: only the pointless documents are removed.
        GeoRegion::Bbox {
            min_lat: -90.0,
            max_lat: 90.0,
            min_lon: -180.0,
            max_lon: 180.0,
        },
        // A band that keeps a minority of the corpus.
        GeoRegion::Bbox {
            min_lat: 0.0,
            max_lat: 20.0,
            min_lon: -180.0,
            max_lon: 0.0,
        },
        GeoRegion::Radius {
            lat: 38.8977,
            lon: -77.0365,
            meters: 3_000_000.0,
            metric: GeoMetric::Haversine,
        },
        GeoRegion::Radius {
            lat: 0.0,
            lon: 0.0,
            meters: 900_000.0,
            metric: GeoMetric::Manhattan,
        },
    ] {
        let filters = pipestream_search::filter::DocFilter {
            deleted: None,
            geo: GeoFilters {
                filters: vec![pipestream_search::geo::GeoFilter {
                    column: Some(gi),
                    region,
                }],
            },
            pred: None,
            phrase: Vec::new(),
        };
        let filter_ctx = Some((&filters, &cols as &dyn NumericRead));
        for k in [1usize, 5, 50] {
            let exhaustive = bm25::top_k_fused_exhaustive_filtered(&fields, k, filter_ctx);
            let mut prune = bm25::PruneStats::default();
            let pruned = bm25::top_k_fused_pruned_filtered_stats(
                &fields,
                k,
                f64::NEG_INFINITY,
                filter_ctx,
                &mut prune,
            );
            assert_eq!(
                signature(&exhaustive),
                signature(&pruned),
                "k={k} {region:?}: filtered fused pruned != filtered fused exhaustive"
            );
            // A seeded floor must not change the answer: every returned
            // score is >= the k-th survivor's, and the filter never
            // raised the floor.
            if let Some(kth) = exhaustive.last() {
                let seeded = bm25::top_k_fused_pruned_filtered_stats(
                    &fields,
                    k,
                    kth.score,
                    filter_ctx,
                    &mut bm25::PruneStats::default(),
                );
                assert_eq!(
                    signature(&exhaustive),
                    signature(&seeded),
                    "k={k} {region:?}: seeded floor changed the filtered fused answer"
                );
            }
            // Every survivor really is inside the region...
            for d in &pruned {
                let (dlat, dlon) = cols.geo_value(gi, d.doc_id).expect("survivor has a point");
                assert!(region.contains(dlat, dlon), "survivor outside the region");
            }
            // ...and the filter is not vacuous.
            let unfiltered = bm25::top_k_fused_pruned_filtered_stats(
                &fields,
                k,
                f64::NEG_INFINITY,
                None,
                &mut bm25::PruneStats::default(),
            );
            assert_ne!(
                signature(&unfiltered),
                signature(&pruned),
                "k={k} {region:?}: this filter removed nothing"
            );
        }
    }

    // With no filter the filtered entry point is bit-identical to the
    // plain fused pruned scorer: the addition is gated, not forked.
    for k in [1usize, 10] {
        let plain = bm25::top_k_fused_pruned(&fields, k, f64::NEG_INFINITY);
        let gated = bm25::top_k_fused_pruned_filtered_stats(
            &fields,
            k,
            f64::NEG_INFINITY,
            None,
            &mut bm25::PruneStats::default(),
        );
        assert_eq!(signature(&plain), signature(&gated), "k={k}: ungated drift");
    }

    let _ = std::fs::remove_dir_all(&dir);
}
