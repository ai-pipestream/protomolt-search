//! The placement dry run and the placement-keyed split
//! (`docs/placement.md`): `PlanPlacement` answers exact per-shard,
//! per-leaf counts that a plain evaluation of the same rules over the
//! test's own rows reproduces; a placement split puts every logged row
//! in the child whose code range holds it and the children reconstruct
//! the parent's top-k.

mod common;

use std::path::{Path, PathBuf};

use common::mock::start_mock_analysis;
use common::{fit_calibration, start_empty_node, unit_vectors, BIT_WIDTH, DIM};
use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::{Layout, NodeConfig};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    AddDocumentsRequest, AddVectorsRequest, AnalysisSpec, FacetValue, FlushRequest, IntegerValue,
    PlanPlacementRequest, PlanPlacementResponse, SetCalibrationRequest,
};
use pipestream_search::placement::{
    encode, subtree_range, PlacementNodeConfig, PlacementTreeConfig,
};
use pipestream_search::postings::AnalyzedDoc;
use pipestream_search::vector::{VectorIndex, EMBEDDED_TURBOVEC};
use pipestream_search::{analyzer, reshard};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

const BITS: u32 = 9;

fn tempdir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("placement-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// --- the dry run ----------------------------------------------------------

/// One row as the test knows it: the columns the tree reads, plus the
/// code the row carries today (`None`: no value).
#[derive(Clone, Copy)]
struct Row {
    year: Option<i64>,
    pages: i64,
    court: &'static str,
    current: Option<i64>,
}

fn node(name: &str, cel: Option<&str>) -> PlacementNodeConfig {
    PlacementNodeConfig {
        name: name.into(),
        cel: cel.map(str::to_string),
        ..Default::default()
    }
}

/// large: pages >= 100; recent: year >= 2020 with scotus / rest; other.
fn tree() -> PlacementTreeConfig {
    let mut recent = node("recent", Some("year >= 2020"));
    recent.children = vec![
        node("scotus", Some("court == \"scotus\"")),
        node("rest", None),
    ];
    PlacementTreeConfig {
        column: "placement".into(),
        level_bits: BITS,
        nodes: vec![
            node("large", Some("pages >= 100")),
            recent,
            node("other", None),
        ],
    }
}

fn code(name: &str) -> i64 {
    match name {
        "large" => encode(&[0], BITS),
        "recent.scotus" => encode(&[1, 0], BITS),
        "recent.rest" => encode(&[1, 1], BITS),
        "other" => encode(&[2], BITS),
        other => panic!("unknown leaf {other}"),
    }
}

/// The rules of `docs/placement.md` applied by hand: first match per
/// level, an absent value falls through, the default is last.
fn leaf_of(row: &Row) -> &'static str {
    if row.pages >= 100 {
        "large"
    } else if row.year.is_some_and(|y| y >= 2020) {
        if row.court == "scotus" {
            "recent.scotus"
        } else {
            "recent.rest"
        }
    } else {
        "other"
    }
}

/// Shard 0 declares the placement column and its rows carry codes;
/// shard 1 does not declare it, so all of its rows would move.
fn rows(shard: usize) -> Vec<Row> {
    let n = if shard == 0 { 12 } else { 8 };
    (0..n)
        .map(|i| {
            let year = if i == 5 {
                None
            } else {
                Some(if i % 3 == 0 { 2021 } else { 2010 })
            };
            let pages = if i % 4 == 0 { 150 } else { 10 };
            let court = if i % 2 == 0 { "scotus" } else { "ca9" };
            let mut row = Row {
                year,
                pages,
                court,
                current: None,
            };
            if shard == 0 && i != 11 {
                // Rows below 6 carry the code they would land on; the
                // rest carry "large" and move unless they land there.
                row.current = Some(if i < 6 {
                    code(leaf_of(&row))
                } else {
                    code("large")
                });
            }
            row
        })
        .collect()
}

fn config(shard: usize) -> NodeConfig {
    let mut integers = vec!["year".to_string(), "pages".to_string()];
    if shard == 0 {
        integers.push("placement".to_string());
    }
    NodeConfig {
        index_path: None,
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        facet_fields: vec!["court".to_string()],
        integer_fields: integers,
        layout: Layout::SingleImage,
        wal: false,
        ..Default::default()
    }
}

async fn ingest_rows(addr: &str, rows: &[Row]) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    // Room for every row: the stream is read only once the call starts.
    let (tx, rx) = mpsc::channel(rows.len().max(1));
    for (i, row) in rows.iter().enumerate() {
        let mut integers = vec![IntegerValue {
            field: "pages".into(),
            value: row.pages,
        }];
        if let Some(year) = row.year {
            integers.push(IntegerValue {
                field: "year".into(),
                value: year,
            });
        }
        if let Some(current) = row.current {
            integers.push(IntegerValue {
                field: "placement".into(),
                value: current,
            });
        }
        tx.send(AddDocumentsRequest {
            text: format!("opinion {i} about search"),
            analysis: Some(body_spec()),
            facets: vec![FacetValue {
                field: "court".into(),
                value: row.court.to_string(),
            }],
            integers,
            ..Default::default()
        })
        .await
        .unwrap();
    }
    drop(tx);
    let added = client
        .add_documents(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner()
        .added;
    assert_eq!(added as usize, rows.len());
}

async fn cluster() -> (
    CoordinatorServiceImpl,
    Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
) {
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..2 {
        let (addr, handle) = start_empty_node(config(shard)).await;
        ingest_rows(&addr, &rows(shard)).await;
        addrs.push(addr);
        handles.push(handle);
    }
    let coordinator = CoordinatorServiceImpl::new(addrs).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    );
    (coordinator, handles)
}

async fn plan(
    c: &CoordinatorServiceImpl,
    proposed: &PlacementTreeConfig,
    filter: &str,
) -> Result<PlanPlacementResponse, tonic::Status> {
    SearchService::plan_placement(
        c,
        Request::new(PlanPlacementRequest {
            proposed: Some(proposed.to_proto()),
            collection: String::new(),
            filter: filter.to_string(),
        }),
    )
    .await
    .map(|r| r.into_inner())
}

/// `(shard, leaf) -> (rows, moving)` from the test's own evaluation.
fn expected(keep: impl Fn(&Row) -> bool) -> std::collections::BTreeMap<(u32, String), (u64, u64)> {
    let mut out = std::collections::BTreeMap::new();
    for shard in 0..2u32 {
        for row in rows(shard as usize).iter().filter(|r| keep(r)) {
            let leaf = leaf_of(row);
            let entry = out.entry((shard, leaf.to_string())).or_insert((0u64, 0u64));
            entry.0 += 1;
            if row.current != Some(code(leaf)) {
                entry.1 += 1;
            }
        }
    }
    out
}

fn check(
    response: &PlanPlacementResponse,
    want: &std::collections::BTreeMap<(u32, String), (u64, u64)>,
) {
    let mut got = std::collections::BTreeMap::new();
    for cell in &response.cells {
        assert_eq!(cell.code, code(&cell.leaf) as u64, "{cell:?}");
        assert!(cell.rows > 0, "empty cells are not reported: {cell:?}");
        assert!(got
            .insert(
                (cell.shard, cell.leaf.clone()),
                (cell.rows, cell.moving_rows)
            )
            .is_none());
    }
    assert_eq!(&got, want);
    assert_eq!(response.rows, want.values().map(|v| v.0).sum::<u64>());
    assert_eq!(
        response.moving_rows,
        want.values().map(|v| v.1).sum::<u64>()
    );
    let defaulted: u64 = want
        .iter()
        .filter(|((_, leaf), _)| leaf == "recent.rest" || leaf == "other")
        .map(|(_, v)| v.0)
        .sum();
    assert_eq!(response.defaulted_rows, defaulted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_dry_run_counts_what_a_hand_evaluation_counts() {
    let (coordinator, _handles) = cluster().await;
    let response = plan(&coordinator, &tree(), "").await.unwrap();
    let want = expected(|_| true);
    // Both shards, all four leaves populated, rows with and without a
    // current code: the fixture covers every branch of the rules.
    assert!(want.len() >= 6, "{want:?}");
    assert!(want.values().any(|v| v.1 < v.0), "some rows stay");
    assert!(want.values().any(|v| v.1 == v.0), "some rows all move");
    check(&response, &want);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_optional_filter_narrows_the_dry_run() {
    let (coordinator, _handles) = cluster().await;
    let response = plan(&coordinator, &tree(), "court == \"ca9\"")
        .await
        .unwrap();
    check(&response, &expected(|r| r.court == "ca9"));
    let response = plan(&coordinator, &tree(), "!has(year)").await.unwrap();
    check(&response, &expected(|r| r.year.is_none()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_dry_run_refuses_by_name() {
    let (coordinator, _handles) = cluster().await;
    let mut default_first = tree();
    default_first.nodes.rotate_right(1);
    let status = plan(&coordinator, &default_first, "").await.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(
        status.message().contains("must be last"),
        "{}",
        status.message()
    );

    let mut typo = tree();
    typo.nodes[1].cel = Some("yeer >= 2020".into());
    let status = plan(&coordinator, &typo, "").await.unwrap_err();
    assert_eq!(
        status.code(),
        tonic::Code::InvalidArgument,
        "{}",
        status.message()
    );
    assert!(status.message().contains("yeer"), "{}", status.message());
    assert!(status.message().contains("recent"), "{}", status.message());

    let status =
        SearchService::plan_placement(&coordinator, Request::new(PlanPlacementRequest::default()))
            .await
            .unwrap_err();
    assert!(status.message().contains("absent"), "{}", status.message());
}

// --- the placement-keyed split ---------------------------------------------

const N: usize = 240;

/// The code row `i` carries in the log: none for every seventh row, else
/// one of three leaves, the last two inside the subtree under root
/// index 1.
fn split_code(i: usize) -> Option<i64> {
    if i.is_multiple_of(7) {
        return None;
    }
    Some(match i % 3 {
        0 => encode(&[0], BITS),
        1 => encode(&[1, 0], BITS),
        _ => encode(&[1, 3], BITS),
    })
}

#[allow(clippy::type_complexity)]
fn replay_analyzer(
    analysis_addr: &str,
) -> impl FnMut(
    &[(
        &str,
        Option<&AnalysisSpec>,
        pipestream_search::analyzer::SessionLayers,
    )],
) -> Result<Vec<AnalyzedDoc>, String>
       + '_ {
    let handle = tokio::runtime::Handle::current();
    move |docs| {
        tokio::task::block_in_place(|| {
            handle.block_on(async {
                let mut out = Vec::with_capacity(docs.len());
                for (text, spec, _) in docs {
                    let analyzed = analyzer::analyze_document(analysis_addr, text, *spec).await;
                    out.push(analyzed.map_err(|e| e.to_string())?);
                }
                Ok(out)
            })
        })
    }
}

fn topk(
    index: &VectorIndex,
    query: &[f32],
    k: usize,
    id_of: impl Fn(u64) -> u64,
) -> Vec<(u64, u32)> {
    let results = index.search_unfiltered(query, k);
    let mut hits: Vec<(u64, u32)> = results
        .indices_for_query(0)
        .iter()
        .zip(results.scores_for_query(0))
        .map(|(&i, &s)| (id_of(i as u64), s.to_bits()))
        .collect();
    hits.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    hits
}

/// A WAL-backed single-image shard with the placement column, the
/// corpus ingested and flushed; returns the index path and the parent
/// image loaded for reference.
async fn logged_shard(dir: &Path, analysis_addr: &str, corpus: &[f32]) -> (PathBuf, VectorIndex) {
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &corpus[..DIM * 64]);
    let index_path = dir.join("shard.tv");
    let (addr, _handle) = start_empty_node(NodeConfig {
        index_path: Some(index_path.clone()),
        layout: Layout::SingleImage,
        analysis_addr: Some(analysis_addr.to_string()),
        wal: true,
        wal_buckets: 8,
        integer_fields: vec!["placement".to_string()],
        ..Default::default()
    })
    .await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    client
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: BIT_WIDTH as u32,
            shift,
            scale,
        })
        .await
        .unwrap();
    let (tx, rx) = mpsc::channel(8);
    tokio::spawn(async move {
        for i in 0..N {
            let integers = split_code(i)
                .map(|value| {
                    vec![IntegerValue {
                        field: "placement".into(),
                        value,
                    }]
                })
                .unwrap_or_default();
            tx.send(AddDocumentsRequest {
                text: format!("document {i} in the log"),
                integers,
                ..Default::default()
            })
            .await
            .unwrap();
        }
    });
    let added = client
        .add_documents(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner()
        .added;
    assert_eq!(added as usize, N);
    let (tx, rx) = mpsc::channel(8);
    let vectors = corpus.to_vec();
    tokio::spawn(async move {
        for chunk in vectors.chunks(50 * DIM) {
            tx.send(AddVectorsRequest {
                vectors: chunk.to_vec(),
                dim: 0,
            })
            .await
            .unwrap();
        }
    });
    let added = client
        .add_vectors(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner()
        .added;
    assert_eq!(added as usize, N);
    assert!(
        client
            .flush(FlushRequest {})
            .await
            .unwrap()
            .into_inner()
            .written
    );
    let mut parent = VectorIndex::load(EMBEDDED_TURBOVEC, &index_path).unwrap();
    parent.prepare().unwrap();
    (index_path, parent)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_placement_split_routes_by_code_and_reconstructs_the_parent() {
    let dir = tempdir("split");
    let corpus = unit_vectors(N, DIM, 0x9A1E_0001);
    let (analysis_addr, _mock) = start_mock_analysis().await;
    let (index_path, parent) = logged_shard(&dir, &analysis_addr, &corpus).await;
    let gen = reshard::resolve_gen(&pipestream_search::wal::wal_dir(&index_path)).unwrap();

    // Child 0 takes one code, child 1 the subtree under root index 1,
    // child 2 (the default) the rows with no code.
    let (lo, hi) = subtree_range(&[1], BITS);
    let children = [
        reshard::PlacementChild { lo: 0, hi: 0 },
        reshard::PlacementChild { lo, hi },
        reshard::PlacementChild::NONE,
    ];
    let out = reshard::split_placement_logs(
        std::slice::from_ref(&gen),
        "placement",
        &children,
        Some(2),
        &dir.join("split"),
        &[0, 1_000, 2_000],
        false,
        None,
        &mut replay_analyzer(&analysis_addr),
    )
    .unwrap();
    assert_eq!(out.images.children.len(), 3);
    assert_eq!(out.ranges.to_vec(), children.to_vec());
    assert_eq!(out.default_child, Some(2));
    assert_eq!(out.source_cutoffs.len(), 1);
    let total: u64 = out.images.children.iter().map(|c| c.num_documents).sum();
    assert_eq!(total as usize, N);
    for (i, child) in out.images.children.iter().enumerate() {
        assert_eq!(child.hash_lo, 0);
        assert_eq!(child.hash_hi, u64::MAX);
        assert_eq!(child.slot_offset, i as u64 * 1_000);
        assert!(child.num_documents > 0, "child {i} is empty");
        for &parent_id in &child.row_parent_ids {
            let expected = match split_code(parent_id as usize) {
                None => 2,
                Some(0) => 0,
                Some(code) if code >= lo && code <= hi => 1,
                Some(code) => panic!("row {parent_id} carries an unexpected code {code}"),
            };
            assert_eq!(expected, i, "row {parent_id} in child {i}");
        }
    }
    let map = reshard::placement_shard_map_toml(&out);
    assert!(map.contains("placement = 0\n"), "{map}");
    assert!(map.contains(&format!("codes {lo}..={hi}")), "{map}");
    assert!(map.contains("default child"), "{map}");

    // The union of the children's top-k equals the parent's, bitwise.
    let images: Vec<(&reshard::ChildImage, VectorIndex)> = out
        .images
        .children
        .iter()
        .map(|c| {
            let mut index = VectorIndex::load(EMBEDDED_TURBOVEC, &c.vector_path).unwrap();
            index.prepare().unwrap();
            (c, index)
        })
        .collect();
    for q in 0..6u64 {
        let query = unit_vectors(1, DIM, 0xC0DE_0000 + q);
        let expected = topk(&parent, &query, 10, |id| id);
        let mut merged: Vec<(u64, u32)> = images
            .iter()
            .flat_map(|(child, index)| {
                topk(index, &query, 10, |local| child.parent_ids[local as usize])
            })
            .collect();
        merged.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        merged.truncate(10);
        assert_eq!(merged, expected, "query {q}");
    }

    // Refusals name the cause: an uncovered row without a default child,
    // an overlap, a rangeless child that is not the default, a missing
    // column name, and the wrong number of slot offsets.
    let mut analyze = replay_analyzer(&analysis_addr);
    let err = reshard::split_placement_logs(
        std::slice::from_ref(&gen),
        "placement",
        &children[..2],
        None,
        &dir.join("no-default"),
        &[0, 1_000],
        false,
        None,
        &mut analyze,
    )
    .unwrap_err();
    assert!(
        err.contains("live source id") && err.contains("no default"),
        "{err}"
    );
    let err = reshard::validate_placement_children(
        &[
            reshard::PlacementChild { lo: 0, hi: 10 },
            reshard::PlacementChild { lo: 10, hi: 20 },
        ],
        None,
    )
    .unwrap_err();
    assert!(err.contains("overlap"), "{err}");
    let err = reshard::validate_placement_children(
        &[
            reshard::PlacementChild::NONE,
            reshard::PlacementChild { lo: 0, hi: 0 },
        ],
        Some(1),
    )
    .unwrap_err();
    assert!(err.contains("not the default child"), "{err}");
    let err =
        reshard::validate_placement_children(&[reshard::PlacementChild { lo: 0, hi: 0 }], Some(3))
            .unwrap_err();
    assert!(err.contains("outside"), "{err}");
    let err = reshard::split_placement_logs(
        std::slice::from_ref(&gen),
        " ",
        &children,
        Some(2),
        &dir.join("no-column"),
        &[0, 1, 2],
        false,
        None,
        &mut analyze,
    )
    .unwrap_err();
    assert!(err.contains("column"), "{err}");
    let err = reshard::split_placement_logs(
        &[gen],
        "placement",
        &children,
        Some(2),
        &dir.join("offsets"),
        &[0, 1],
        false,
        None,
        &mut analyze,
    )
    .unwrap_err();
    assert!(err.contains("slot offset"), "{err}");
    std::fs::remove_dir_all(&dir).ok();
}
