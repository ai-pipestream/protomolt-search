//! CEL filter acceptance tests (`docs/cel-filters.md`): the public
//! `Bm25SearchRequest.filter` string, compiled once at the coordinator
//! into the FilterExpr IR, resolved per shard, evaluated three-valued
//! at the heap gate, narrowing the facet match set, and refusing loudly
//! at every seam — plus the differential oracle that holds the
//! compiled ordinal path to agreement with a reference CEL interpreter
//! wherever stock CEL is defined.

mod common;

use std::collections::{BTreeSet, HashMap};

use pipestream_search::bm25::{self, Bm25Params, CorpusStats};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::filter::{DocFilter, Edge, NumBound, ResolvedFilter, ResolvedLeaf, Tri};
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    AddDocumentsRequest, Bm25SearchRequest, Bm25SearchResponse, FacetValue, GeoBbox, GeoFilter,
    GeoPointValue, IntegerValue, MapFacetEntry, MapNumericEntry, NumericValue, TimestampValue,
};
use pipestream_search::postings::{AnalyzedDoc, Bm25Reader, Bm25Store};
use pipestream_search::scorefn::NumericRead;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

use common::{mock::start_mock_analysis, start_empty_node};

/// One controlled document: text plus every column value it holds.
/// The same table drives ingest AND the oracle's variable bindings, so
/// the two sides cannot drift.
#[derive(Clone, Copy)]
struct Doc {
    text: &'static str,
    court: Option<&'static str>,
    year: Option<i64>,
    score: Option<f64>,
    /// RFC 3339; lands in the `decided` i64 column via timestamp
    /// ingest sugar (epoch micros).
    decided: Option<&'static str>,
    tags: &'static [(&'static str, &'static str)],
    cites: &'static [(&'static str, f64)],
    point: Option<(f64, f64)>,
}

const NONE_DOC: Doc = Doc {
    text: "",
    court: None,
    year: None,
    score: None,
    decided: None,
    tags: &[],
    cites: &[],
    point: None,
};

/// The distributed corpus: shards 0 and 1 declare every column, shard
/// 2 declares NONE (the heterogeneous fleet, whose documents hold no
/// values at all). Global ids 0..=7; df("rust") = 7 (d7 has none).
const SHARD_DOCS: [&[Doc]; 3] = [
    &[
        Doc {
            text: "rust search rust fast",
            court: Some("scotus"),
            year: Some(1990),
            score: Some(1.5),
            decided: Some("2015-06-01T00:00:00Z"),
            tags: &[("color", "red")],
            cites: &[("a", 3.0)],
            point: Some((0.0, 0.0)),
        },
        Doc {
            text: "vector search rust",
            court: Some("ca5"),
            year: Some(2000),
            score: Some(2.5),
            tags: &[("color", "blue")],
            point: Some((0.0, 1.0)),
            ..NONE_DOC
        },
        Doc {
            text: "rust rust",
            court: Some("scotus"),
            year: Some(2010),
            ..NONE_DOC
        },
    ],
    &[
        Doc {
            text: "search engines love rust",
            court: Some("ca9"),
            year: Some(1995),
            score: Some(0.5),
            decided: Some("1990-01-15T12:00:00Z"),
            tags: &[("color", "red"), ("status", "live")],
            cites: &[("a", 1.0)],
            point: Some((10.0, 10.0)),
        },
        Doc {
            text: "vector vector vector rust",
            year: Some(2020),
            score: Some(3.5),
            ..NONE_DOC
        },
        Doc {
            text: "rust fast",
            court: Some("scotus"),
            ..NONE_DOC
        },
    ],
    &[
        Doc {
            text: "rust",
            ..NONE_DOC
        },
        Doc {
            text: "nothing relevant here",
            ..NONE_DOC
        },
    ],
];

const OFFSETS: [u64; 3] = [0, 3, 6];

fn timestamp_of(rfc3339: &str) -> prost_types::Timestamp {
    // The tests' instants are whole UTC seconds; converting through
    // the same civil-date math the compiler uses would test it against
    // itself, so this uses the independent (slow, obviously-correct)
    // route: count days by iteration.
    let b = rfc3339.as_bytes();
    let num =
        |r: std::ops::Range<usize>| -> i64 { std::str::from_utf8(&b[r]).unwrap().parse().unwrap() };
    let (y, m, d) = (num(0..4), num(5..7), num(8..10));
    let (hh, mm, ss) = (num(11..13), num(14..16), num(17..19));
    assert_eq!(b[19], b'Z', "test instants are UTC whole seconds");
    let leap = |y: i64| (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let mdays = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut days: i64 = 0;
    for year in 1970..y {
        days += if leap(year) { 366 } else { 365 };
    }
    for month in 1..m {
        days += mdays[month as usize - 1] + i64::from(month == 2 && leap(y));
    }
    days += d - 1;
    prost_types::Timestamp {
        seconds: days * 86_400 + hh * 3_600 + mm * 60 + ss,
        nanos: 0,
    }
}

fn ingest_request(doc: &Doc) -> AddDocumentsRequest {
    AddDocumentsRequest {
        original_source: None,
        source_chunk_ordinal: None,
        collection: String::new(),
        cased_field: String::new(),
        sentence_fields: Vec::new(),
        materialize: None,
        text: doc.text.to_string(),
        analysis: None,
        lineage: None,
        fields: Vec::new(),
        facets: doc
            .court
            .iter()
            .map(|c| FacetValue {
                field: "court".into(),
                value: c.to_string(),
            })
            .collect(),
        numerics: doc
            .score
            .iter()
            .map(|s| NumericValue {
                field: "score".into(),
                value: *s,
            })
            .collect(),
        integers: doc
            .year
            .iter()
            .map(|y| IntegerValue {
                field: "year".into(),
                value: *y,
            })
            .collect(),
        timestamps: doc
            .decided
            .iter()
            .map(|t| TimestampValue {
                field: "decided".into(),
                value: Some(timestamp_of(t)),
            })
            .collect(),
        map_facets: doc
            .tags
            .iter()
            .map(|(k, v)| MapFacetEntry {
                field: "tags".into(),
                key: k.to_string(),
                value: v.to_string(),
            })
            .collect(),
        map_numerics: doc
            .cites
            .iter()
            .map(|(k, v)| MapNumericEntry {
                field: "cites".into(),
                key: k.to_string(),
                value: *v,
            })
            .collect(),
        geo_points: doc
            .point
            .iter()
            .map(|(lat, lon)| GeoPointValue {
                field: "courthouse".into(),
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
    }
}

async fn add_docs(addr: &str, docs: &[Doc]) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(64);
    for doc in docs {
        tx.send(ingest_request(doc)).await.unwrap();
    }
    drop(tx);
    client.add_documents(ReceiverStream::new(rx)).await.unwrap();
}

/// Shards 0 and 1 declare every column family; shard 2 declares none.
async fn start_cel_shards(
    analysis: &str,
) -> (
    Vec<String>,
    Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
) {
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for (i, &offset) in OFFSETS.iter().enumerate() {
        let all = i < 2;
        let cols = |name: &str| {
            if all {
                vec![name.to_string()]
            } else {
                Vec::new()
            }
        };
        let (addr, handle) = start_empty_node(NodeConfig {
            slot_offset: offset,
            analysis_addr: Some(analysis.to_string()),
            facet_fields: cols("court"),
            numeric_fields: cols("score"),
            integer_fields: if all {
                vec!["year".to_string(), "decided".to_string()]
            } else {
                Vec::new()
            },
            map_facet_fields: cols("tags"),
            map_numeric_fields: cols("cites"),
            geo_fields: cols("courthouse"),
            ..Default::default()
        })
        .await;
        addrs.push(addr);
        handles.push(handle);
    }
    for (i, docs) in SHARD_DOCS.iter().enumerate() {
        add_docs(&addrs[i], docs).await;
    }
    (addrs, handles)
}

/// One public Bm25Search with a CEL filter string.
async fn search(
    coordinator: &CoordinatorServiceImpl,
    filter: &str,
) -> Result<Bm25SearchResponse, tonic::Status> {
    search_request(
        coordinator,
        Bm25SearchRequest {
            text: "rust".into(),
            k: 20,
            filter: filter.into(),
            ..Default::default()
        },
    )
    .await
}

async fn search_request(
    coordinator: &CoordinatorServiceImpl,
    req: Bm25SearchRequest,
) -> Result<Bm25SearchResponse, tonic::Status> {
    coordinator
        .bm25_search(Request::new(req))
        .await
        .map(|r| r.into_inner())
}

fn ids(resp: &Bm25SearchResponse) -> Vec<u64> {
    let mut v: Vec<u64> = resp.hits.iter().map(|h| h.doc_id).collect();
    v.sort_unstable();
    v
}

/// The selection matrix: every leaf kind, the Kleene absence rules,
/// per-shard dictionary divergence, value-level honesty, and the AND
/// with the standalone geo family — all through the public route.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_cel_filters_select_exactly() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addrs, _handles) = start_cel_shards(&analysis).await;
    let coordinator = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());

    let unfiltered = search(&coordinator, "").await.unwrap();
    assert_eq!(ids(&unfiltered), vec![0, 1, 2, 3, 4, 5, 6], "df(rust) = 7");

    for (filter, want, why) in [
        (
            r#"court == "scotus""#,
            vec![0, 2, 5],
            "facet equality selects across shards",
        ),
        (
            r#"court == "scotus" && year >= 2000"#,
            vec![2],
            "d5 has no year: a comparison on absence is Unknown, and Kleene AND \
             with Unknown cannot reach True",
        ),
        (
            r#"court != "scotus""#,
            vec![1, 3],
            "negation cannot launder absence: d4 (no court) and shard 2's \
             column-less documents stay out",
        ),
        (
            r#"!(court == "scotus")"#,
            vec![1, 3],
            "`!=` and negated `==` are the same predicate",
        ),
        (
            "has(court)",
            vec![0, 1, 2, 3, 5],
            "presence is total: exactly the documents holding a value",
        ),
        (
            "!has(court)",
            vec![4, 6],
            "and its negation is the escape hatch that SEES absence — \
             including on the shard with no column at all",
        ),
        (
            r#"court == "ca9""#,
            vec![3],
            "the value exists only in shard 1's dictionary; per-shard \
             resolution keeps the union exact",
        ),
        (
            r#"court == "scotsu""#,
            vec![],
            "a value the corpus never held is an honest empty set, not a \
             refusal: the typo rule guards structure, not data",
        ),
        (
            r#"court in ["scotus", "ca9"]"#,
            vec![0, 2, 3, 5],
            "membership is the union of equalities",
        ),
        ("year in [1990, 2020]", vec![0, 4], "numeric membership"),
        (
            "score < 2.5",
            vec![0, 3],
            "exclusive bound: d1 sits exactly on 2.5 and is out",
        ),
        (
            "score <= 2.5",
            vec![0, 1, 3],
            "inclusive bound: d1 sits exactly on 2.5 and is in",
        ),
        (
            "score >= 1.5 && score <= 2.5",
            vec![0, 1],
            "two-sided f64 range",
        ),
        (
            "year > 1990 && year < 2020",
            vec![1, 2, 3],
            "two-sided i64 range, both edges exclusive",
        ),
        (
            r#"tags["color"] == "red""#,
            vec![0, 3],
            "map-facet equality under one key",
        ),
        (
            r#"tags["color"] != "red""#,
            vec![1],
            "documents without the key are Unknown, not not-red",
        ),
        (r#""status" in tags"#, vec![3], "key presence is total"),
        (
            r#"!("status" in tags)"#,
            vec![0, 1, 2, 4, 5, 6],
            "so its negation admits every document without the key, \
             column-less shard included",
        ),
        (
            r#"cites["a"] >= 2"#,
            vec![0],
            "map-numeric range under one key",
        ),
        (
            r#"decided >= timestamp("2000-01-01T00:00:00Z")"#,
            vec![0],
            "timestamp() compiles to the epoch-micros bound the ingest \
             sugar stored",
        ),
        (
            r#"within_bbox(courthouse, 0.0, 1.0, 0.0, 1.0) && court == "scotus""#,
            vec![0],
            "a geo leaf mixes with scalar predicates in one expression",
        ),
        (
            r#"court == "ca9" || year >= 2015"#,
            vec![3, 4],
            "Kleene OR: d4's missing court is rescued by its year",
        ),
        (
            "year >= 5 && year <= 4",
            vec![],
            "a legal empty range answers empty, honestly",
        ),
    ] {
        let resp = search(&coordinator, filter)
            .await
            .unwrap_or_else(|e| panic!("{filter:?} refused unexpectedly: {}", e.message()));
        assert_eq!(ids(&resp), want, "{filter:?}: {why}");
    }

    // The standalone geo family and the compiled tree AND together:
    // the unit box keeps d0 and d1; the year bound keeps d1.
    let resp = search_request(
        &coordinator,
        Bm25SearchRequest {
            text: "rust".into(),
            k: 20,
            filter: "year >= 2000".into(),
            geo_filters: vec![GeoFilter {
                column: "courthouse".into(),
                region: Some(pipestream_search::pb::geo_filter::Region::Bbox(GeoBbox {
                    min_lat: 0.0,
                    max_lat: 1.0,
                    min_lon: 0.0,
                    max_lon: 1.0,
                })),
            }],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        ids(&resp),
        vec![1],
        "geo_filters and filter are conjoined, not either-or"
    );

    // The floor contract survives filtering: kth_best seeds a re-query
    // to the identical answer (ties at the floor survive).
    let first = search_request(
        &coordinator,
        Bm25SearchRequest {
            text: "rust".into(),
            k: 2,
            filter: r#"court == "scotus""#.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(first.hits.len(), 2);
    assert!(first.kth_best > 0.0, "a filled heap emits a seedable floor");
    let seeded = search_request(
        &coordinator,
        Bm25SearchRequest {
            text: "rust".into(),
            k: 2,
            min_score: first.kth_best,
            filter: r#"court == "scotus""#.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        seeded.hits.iter().map(|h| h.doc_id).collect::<Vec<_>>(),
        first.hits.iter().map(|h| h.doc_id).collect::<Vec<_>>(),
        "seeding with kth_best under a filter returns the same top-k"
    );
}

/// Facet counting counts the FILTERED match set — all three facet
/// kinds, narrowed by the compiled tree and by the standalone geo
/// family alike (the increment where "facet counts are not narrowed by
/// filters" stopped being true, as the plan deferred it to the CEL
/// story).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filters_narrow_facet_counts() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addrs, _handles) = start_cel_shards(&analysis).await;
    let coordinator = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());

    let counts_of = |resp: &Bm25SearchResponse, field: &str| -> Vec<(String, u64)> {
        let f = resp
            .facets
            .iter()
            .find(|f| f.field == field)
            .unwrap_or_else(|| panic!("facet {field} missing"));
        f.counts
            .iter()
            .map(|c| (c.value.clone(), c.count))
            .collect()
    };

    // Unfiltered baseline over the rust match set (d0..d6).
    let base = search_request(
        &coordinator,
        Bm25SearchRequest {
            text: "rust".into(),
            k: 1,
            facet_fields: vec!["court".into()],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        counts_of(&base, "court"),
        vec![
            ("scotus".to_string(), 3),
            ("ca5".to_string(), 1),
            ("ca9".to_string(), 1)
        ]
    );

    // Narrowed by the compiled tree: year >= 1995 keeps d1 d2 d3 d4.
    let filtered = search_request(
        &coordinator,
        Bm25SearchRequest {
            text: "rust".into(),
            k: 1,
            facet_fields: vec!["court".into()],
            filter: "year >= 1995".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        counts_of(&filtered, "court"),
        vec![
            ("ca5".to_string(), 1),
            ("ca9".to_string(), 1),
            ("scotus".to_string(), 1)
        ],
        "court counts are over the FILTERED match set"
    );

    // Narrowed by the standalone geo family: the unit box keeps d0 d1.
    let geo_narrowed = search_request(
        &coordinator,
        Bm25SearchRequest {
            text: "rust".into(),
            k: 1,
            facet_fields: vec!["court".into()],
            geo_filters: vec![GeoFilter {
                column: "courthouse".into(),
                region: Some(pipestream_search::pb::geo_filter::Region::Bbox(GeoBbox {
                    min_lat: 0.0,
                    max_lat: 1.0,
                    min_lon: 0.0,
                    max_lon: 1.0,
                })),
            }],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        counts_of(&geo_narrowed, "court"),
        vec![("ca5".to_string(), 1), ("scotus".to_string(), 1)],
        "geo filters narrow the facet match set too"
    );

    // Range and map facets share the same narrowed bitmap.
    let combo = search_request(
        &coordinator,
        Bm25SearchRequest {
            text: "rust".into(),
            k: 1,
            map_facet_fields: vec![pipestream_search::pb::MapFacetField {
                column: "tags".into(),
                key: "color".into(),
            }],
            range_facet_fields: vec![pipestream_search::pb::RangeFacetField {
                column: "year".into(),
                key: String::new(),
                edges: vec![1990.0, 2000.0, 2030.0],
            }],
            filter: r#"court == "scotus""#.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let tags = combo
        .facets
        .iter()
        .find(|f| f.field == "tags")
        .expect("map facet answered");
    assert_eq!(
        tags.counts
            .iter()
            .map(|c| (c.value.clone(), c.count))
            .collect::<Vec<_>>(),
        vec![("red".to_string(), 1)],
        "map-facet counts narrowed: only d0 is scotus with a color"
    );
    let years = &combo.range_facets[0];
    assert_eq!(
        years.buckets.iter().map(|b| b.count).collect::<Vec<_>>(),
        vec![1, 1],
        "range buckets narrowed: d0 in [1990,2000), d2 in [2000,2030), d5 has no year"
    );
}

/// Refusals through the public route: structural typos refuse naming
/// the leaf's table, non-compilable constructs refuse naming the
/// construct, and the messages say which knob to check.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cel_refusals_are_loud() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addrs, _handles) = start_cel_shards(&analysis).await;
    let coordinator = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());

    for (filter, needle, why) in [
        (
            r#"nosuch == "x""#,
            r#"facet column "nosuch""#,
            "unknown facet column",
        ),
        (
            r#"year == "1990""#,
            r#"facet column "year""#,
            "a string literal selects the facet table, and no shard has a \
             FACET named year — the kind-mismatch refusal",
        ),
        (
            "court >= 5",
            r#"numeric column "court""#,
            "a number selects the numeric tables, and court is a facet",
        ),
        (
            r#"tags["colr"] == "red""#,
            r#"map-facet column "tags" key "colr""#,
            "a map key no shard ingested is drill-down structure spelled wrong",
        ),
        (
            r#""k" in nomap"#,
            r#"map column "nomap""#,
            "unknown map column",
        ),
        (
            "has(nothing_anywhere)",
            r#"column "nothing_anywhere""#,
            "has() refuses a name no family anywhere knows",
        ),
        (
            "within_bbox(nogeo, 0.0, 1.0, 0.0, 1.0)",
            r#"geo column "nogeo""#,
            "unknown geo column through the CEL sugar",
        ),
        ("year + 1 > 1990", "arithmetic", "construct refusal"),
        (
            r#"court.matches("sco.*")"#,
            "matches() (regex)",
            "regex refusal by name",
        ),
        (
            r#"court == "a" ? has(x) : has(y)"#,
            "ternary",
            "ternary refusal by name",
        ),
    ] {
        let err = search(&coordinator, filter)
            .await
            .expect_err(&format!("{filter:?} should refuse ({why})"));
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{filter:?}");
        assert!(
            err.message().contains(needle),
            "{filter:?}: {needle:?} not in {:?} ({why})",
            err.message()
        );
    }

    // A partially-known column does NOT refuse: shard 2 lacks every
    // column, and that is the heterogeneous fleet, not a typo.
    search(&coordinator, r#"court == "scotus""#)
        .await
        .expect("partially-known columns are the heterogeneous fleet");
}

/// [`crate::scorefn::NumericRead`] over a reader, for the local
/// scorer-seam test.
struct ReaderCols<'a>(&'a Bm25Reader);
impl NumericRead for ReaderCols<'_> {
    fn value(&self, ni: usize, doc_id: u32) -> Option<f64> {
        self.0.numeric_value(ni, doc_id)
    }
    fn map_value(&self, ci: usize, key_ord: u32, doc_id: u32) -> Option<f64> {
        self.0.map_numeric_value(ci, key_ord, doc_id)
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

/// The scorer seam, locally and at scale: a compound resolved tree
/// (AND over OR, NOT, i64 and f64 ranges, facet membership, map-key
/// presence) gates the pruned scorer and the exhaustive oracle to
/// bitwise-identical answers on a v5-impacted reader, seeded floors
/// included, with survivors verified against a direct per-document
/// evaluation.
#[test]
fn cel_filtered_pruned_matches_exhaustive_bitwise() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("cel_pruned_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let n = 3000u32;
    let tf_a = |d: u32| 1 + (u64::from(d) * 2654435761 % 7) as u32;
    let mut store = Bm25Store::with_fields(&["body"])
        .with_facets(&["court"])
        .with_integers(&["year"])
        .with_numerics(&["score"])
        .with_map_facets(&["tags"]);
    let courts = ["scotus", "ca5", "ca9", "dcc"];
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
        // Deterministic value spread with holes: every 5th document
        // has no court, every 7th no year, every 4th no score, every
        // 3rd no tag — absence at every leaf.
        if doc % 5 != 0 {
            store.set_facet(0, doc, courts[doc as usize % 4]);
        }
        if doc % 7 != 0 {
            store.set_integer(0, doc, 1900 + i64::from(doc % 130));
        }
        if doc % 4 != 0 {
            store.set_numeric(0, doc, f64::from(doc % 50) / 4.0);
        }
        if doc % 3 != 0 {
            store.set_map_facet(0, doc, "status", if doc % 2 == 0 { "live" } else { "dead" });
        }
    }
    let path = dir.join("cel.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let body = reader.field(0);
    let cols = ReaderCols(&reader);

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

    // court in {scotus, ca9} OR year in [1950, 2000) — NOT (score < 3)
    // — AND "status" in tags. Ordinals resolved by hand against the
    // store this test built: first-seen order makes scotus 0 and ca9 2.
    let scotus_ord = (0..n)
        .filter_map(|d| reader.facet_ord(0, d).map(|o| (d, o)))
        .find(|(d, _)| courts[*d as usize % 4] == "scotus")
        .map(|(_, o)| o)
        .unwrap();
    let ca9_ord = (0..n)
        .filter_map(|d| reader.facet_ord(0, d).map(|o| (d, o)))
        .find(|(d, _)| courts[*d as usize % 4] == "ca9")
        .map(|(_, o)| o)
        .unwrap();
    let status_key = reader.map_facet_key_ord(0, "status").unwrap();
    let mut ords = vec![scotus_ord, ca9_ord];
    ords.sort_unstable();
    let pred = ResolvedFilter::And(vec![
        ResolvedFilter::Or(vec![
            ResolvedFilter::Leaf(ResolvedLeaf::Facet {
                column: Some(0),
                ords,
            }),
            ResolvedFilter::Leaf(ResolvedLeaf::IntRange {
                column: 0,
                lo: 1950,
                hi: 1999,
            }),
        ]),
        ResolvedFilter::Not(Box::new(ResolvedFilter::Leaf(ResolvedLeaf::F64Range {
            column: 0,
            lo: None,
            hi: Some(Edge {
                value: NumBound::I(3),
                exclusive: true,
            }),
        }))),
        ResolvedFilter::Leaf(ResolvedLeaf::MapHasKey(
            pipestream_search::filter::MapKeyRef::Facet {
                column: 0,
                key_ord: Some(status_key),
            },
        )),
    ]);
    let doc_filter = DocFilter {
        deleted: None,
        geo: Default::default(),
        pred: Some(pred.clone()),
        phrase: Vec::new(),
    };
    let filter_ctx: bm25::FilterCtx = Some((&doc_filter, &cols as &dyn NumericRead));

    for k in [1usize, 5, 50] {
        let exhaustive = bm25::top_k_exhaustive_chained_filtered(
            &body, &terms, &stats, params, k, None, filter_ctx,
        );
        let mut prune = bm25::PruneStats::default();
        let pruned = bm25::top_k_pruned_chained_filtered_stats(
            &body,
            &terms,
            &stats,
            params,
            k,
            f64::NEG_INFINITY,
            None,
            filter_ctx,
            &mut prune,
        );
        assert_eq!(
            signature(&exhaustive),
            signature(&pruned),
            "k={k}: filtered pruned != filtered exhaustive"
        );
        // Seeding with the k-th best must not change the answer.
        if let Some(kth) = exhaustive.last() {
            let seeded = bm25::top_k_pruned_chained_filtered_stats(
                &body,
                &terms,
                &stats,
                params,
                k,
                kth.score,
                None,
                filter_ctx,
                &mut bm25::PruneStats::default(),
            );
            assert_eq!(
                signature(&exhaustive),
                signature(&seeded),
                "k={k}: seeded floor changed the filtered answer"
            );
        }
        // Survivors match a direct evaluation of the tree, and the
        // filter is not vacuous.
        for d in &pruned {
            assert_eq!(
                pred.eval(d.doc_id, &cols),
                Tri::True,
                "doc {} survived but does not pass the tree",
                d.doc_id
            );
        }
        let unfiltered = bm25::top_k_pruned_chained_filtered_stats(
            &body,
            &terms,
            &stats,
            params,
            k,
            f64::NEG_INFINITY,
            None,
            None,
            &mut bm25::PruneStats::default(),
        );
        assert_ne!(
            signature(&unfiltered),
            signature(&pruned),
            "k={k}: the filter should change the top-k, or this test pins nothing"
        );
    }
}

/// The differential oracle: on documents where every referenced value
/// is PRESENT — the domain where stock CEL is defined — the compiled
/// ordinal path must agree with the reference interpreter on every
/// (expression, document) pair, through the full wire stack. Absence
/// semantics are OUR documented deviation and are pinned by the other
/// tests, never by this one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compiled_filters_agree_with_reference_cel_interpreter() {
    // Eight fully-populated documents spanning the value space.
    let oracle_docs: Vec<Doc> = (0..8)
        .map(|i| Doc {
            text: "rust oracle document",
            court: Some(["scotus", "ca5", "ca9", "dcc"][i % 4]),
            year: Some(1980 + 10 * i as i64),
            score: Some(f64::from(i as u32) * 0.75),
            decided: None,
            tags: if i % 2 == 0 {
                &[("color", "red"), ("status", "live")]
            } else {
                &[("color", "blue"), ("status", "dead")]
            },
            cites: if i % 3 == 0 {
                &[("a", 1.0), ("b", 4.0)]
            } else {
                &[("a", 6.0), ("b", 2.0)]
            },
            point: None,
        })
        .collect();

    let (analysis, _mock) = start_mock_analysis().await;
    let (addr, _handle) = start_empty_node(NodeConfig {
        slot_offset: 0,
        analysis_addr: Some(analysis.to_string()),
        facet_fields: vec!["court".to_string()],
        numeric_fields: vec!["score".to_string()],
        integer_fields: vec!["year".to_string()],
        map_facet_fields: vec!["tags".to_string()],
        map_numeric_fields: vec!["cites".to_string()],
        ..Default::default()
    })
    .await;
    add_docs(&addr, &oracle_docs).await;
    let coordinator = CoordinatorServiceImpl::new(vec![addr])
        .with_bm25(Some(analysis.clone()), Default::default());

    // Only constructs stock CEL defines on present values: no has()
    // (its bare-identifier form is our extension of the macro), no
    // timestamp() (our version folds to the storage integer).
    let expressions = [
        r#"court == "scotus""#,
        r#"court != "scotus""#,
        r#"court in ["scotus", "ca9"]"#,
        r#""ca5" == court"#,
        "year >= 2010",
        "year < 2010",
        "2010 <= year",
        "year == 2000",
        "year != 2000",
        "year in [1980, 2010, 2040]",
        "score < 2.5",
        "score <= 1.5",
        "score > 0.0",
        "score >= 5.25",
        "score == 0.75",
        r#"tags["color"] == "red""#,
        r#"tags["color"] != "red""#,
        r#"tags["status"] == "live""#,
        r#"tags["color"] in ["red", "green"]"#,
        r#""status" in tags"#,
        r#"!("nope" in tags)"#,
        r#"cites["a"] >= 4.0"#,
        r#"cites["b"] < 3.0"#,
        r#"court == "scotus" && year >= 2000"#,
        r#"court == "scotus" || court == "ca5""#,
        r#"!(court == "dcc")"#,
        r#"(court == "scotus" || year > 2030) && score >= 0.0"#,
        r#"court != "ca5" && !(tags["status"] == "dead") && year in [1980, 2000, 2020, 2040]"#,
        // String ordering and prefixes over the byte-sorted dictionaries
        // (docs/prefix-terms.md): stock CEL compares strings by code
        // point, which is byte order over UTF-8.
        r#"court < "dcc""#,
        r#"court <= "ca5""#,
        r#"court > "ca5""#,
        r#"court >= "ca9""#,
        r#""ca9" > court"#,
        r#""dcc" <= court"#,
        r#"court >= "ca5" && court < "dcc""#,
        r#"court.startsWith("ca")"#,
        r#"court.startsWith("ca9")"#,
        r#"court.startsWith("s")"#,
        r#"court.startsWith("zz")"#,
        r#"!court.startsWith("ca")"#,
        r#"court < "a""#,
        r#"court >= "zzz""#,
        r#"tags["color"] < "green""#,
        r#"tags["color"] >= "green""#,
        r#"tags["color"].startsWith("r")"#,
        r#"tags["status"].startsWith("li")"#,
        r#"tags["color"] > "blue" && court.startsWith("ca")"#,
        r#"court.startsWith("ca") || year > 2030"#,
    ];

    for expr in expressions {
        // Reference truth, per document, from the interpreter.
        let mut want = BTreeSet::new();
        for (id, doc) in oracle_docs.iter().enumerate() {
            let program = cel_interpreter::Program::compile(expr)
                .unwrap_or_else(|e| panic!("reference CEL refused {expr:?}: {e}"));
            let mut ctx = cel_interpreter::Context::default();
            ctx.add_variable("court", doc.court.unwrap())
                .expect("bind court");
            ctx.add_variable("year", doc.year.unwrap())
                .expect("bind year");
            ctx.add_variable("score", doc.score.unwrap())
                .expect("bind score");
            let tags: HashMap<String, String> = doc
                .tags
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            ctx.add_variable("tags", tags).expect("bind tags");
            let cites: HashMap<String, f64> =
                doc.cites.iter().map(|(k, v)| (k.to_string(), *v)).collect();
            ctx.add_variable("cites", cites).expect("bind cites");
            match program.execute(&ctx) {
                Ok(cel_interpreter::Value::Bool(true)) => {
                    want.insert(id as u64);
                }
                Ok(cel_interpreter::Value::Bool(false)) => {}
                Ok(other) => panic!("reference CEL {expr:?} returned non-bool {other:?}"),
                Err(e) => {
                    panic!("reference CEL {expr:?} errored on a fully-populated document: {e}")
                }
            }
        }
        // Compiled truth, through the wire.
        let resp = search(&coordinator, expr)
            .await
            .unwrap_or_else(|e| panic!("compiled path refused {expr:?}: {}", e.message()));
        let got: BTreeSet<u64> = resp.hits.iter().map(|h| h.doc_id).collect();
        assert_eq!(
            got, want,
            "{expr:?}: compiled ordinal path disagrees with the reference interpreter"
        );
    }
}
