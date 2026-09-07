//! Derived columns declared on the index (`docs/derived-columns.md`):
//! computed once at ingest from each document's own values, stored as
//! ordinary typed columns, pinned to the store, the log and every
//! segment by fingerprint, refused by name when forged or when a
//! declaration changes, and rebuilt through the reshard tool's
//! backfill, which stores bit for bit what direct ingest stores.

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::start_empty_node;
use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::derived::Declaration;
use pipestream_search::node::{Layout, NodeConfig, NodeServiceImpl};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    projected_value, AddDocumentsRequest, Bm25SearchRequest, DerivedColumn, DerivedColumns,
    DerivedDisclosure, FacetValue, FlushRequest, IntegerValue, MaterializeKind, MaterializeSpec,
    MaterializedColumn, NamedProjection,
};
use pipestream_search::placement::{
    PinnedLeaf, Placement, PlacementNodeConfig, PlacementTreeConfig,
};
use pipestream_search::reshard;
use pipestream_search::values::fnv1a64_bytes;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

const N: usize = 60;
const DIM: usize = 16;
const SEAL: usize = 16;

fn tempdir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("derived-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn column(name: &str, kind: MaterializeKind, expression: &str) -> DerivedColumn {
    DerivedColumn {
        name: name.into(),
        expression: expression.into(),
        kind: kind as i32,
        disclosure: DerivedDisclosure::Inputs as i32,
    }
}

/// The declaration under test: a calendar year from a timestamp, a
/// facet hash, and a facet comparison bucketed through the ternary.
fn spec() -> DerivedColumns {
    DerivedColumns {
        columns: vec![
            column("year_d", MaterializeKind::I64, "calendar.year(decided)"),
            column("court_hash", MaterializeKind::U64, "hash.fnv64(court)"),
            column(
                "scotus",
                MaterializeKind::I64,
                "court == \"scotus\" ? 1 : 0",
            ),
        ],
    }
}

fn declaration() -> Arc<Declaration> {
    Arc::new(Declaration::compile(&spec()).unwrap())
}

fn year(i: usize) -> i64 {
    1990 + (i % 25) as i64
}

fn court(i: usize) -> &'static str {
    ["scotus", "ca9", "nysd"][i % 3]
}

/// Epoch microseconds of January 1st (plus `i` days) of the year.
fn decided(i: usize) -> i64 {
    (pipestream_search::calendar::days_from_civil(year(i), 1, 1) + (i % 28) as i64) * 86_400_000_000
}

fn expected(i: usize) -> (i64, u64, i64) {
    (
        year(i),
        fnv1a64_bytes(court(i).as_bytes()),
        i64::from(court(i) == "scotus"),
    )
}

fn document(i: usize) -> AddDocumentsRequest {
    AddDocumentsRequest {
        text: format!("opinion {i} about search and the year {}", year(i)),
        analysis: Some(body_spec()),
        integers: vec![IntegerValue {
            field: "decided".into(),
            value: decided(i),
        }],
        facets: vec![FacetValue {
            field: "court".into(),
            value: court(i).into(),
        }],
        ..Default::default()
    }
}

fn config(index_path: Option<PathBuf>, derived: Option<Arc<Declaration>>) -> NodeConfig {
    NodeConfig {
        index_path: index_path.clone(),
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        layout: Layout::Segments,
        seal_tail_docs: SEAL as u32,
        wal: index_path.is_some(),
        wal_buckets: 4,
        facet_fields: vec!["court".into()],
        integer_fields: vec!["decided".into(), "placement".into()],
        derived,
        ..Default::default()
    }
}

async fn ingest(addr: &str, docs: Vec<AddDocumentsRequest>) -> Result<(), tonic::Status> {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(docs.len().max(1));
    for doc in docs {
        tx.send(doc).await.unwrap();
    }
    drop(tx);
    client
        .add_documents(ReceiverStream::new(rx))
        .await
        .map(|_| ())
}

fn root(addr: &str) -> CoordinatorServiceImpl {
    CoordinatorServiceImpl::new(vec![addr.to_string()]).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    )
}

fn projection(name: &str, expression: &str) -> NamedProjection {
    NamedProjection {
        name: name.into(),
        expression: expression.into(),
    }
}

/// Every document's `(decided, year_d, court_hash, scotus)` as
/// projected through the root, keyed by `decided`.
async fn projected(
    root: &CoordinatorServiceImpl,
    filter: &str,
) -> Result<Vec<(i64, i64, u64, i64)>, tonic::Status> {
    let response = root
        .bm25_search(Request::new(Bm25SearchRequest {
            text: "opinion".into(),
            analysis: Some(body_spec()),
            k: N as u32,
            filter: filter.to_string(),
            projections: vec![
                projection("decided", "decided"),
                projection("year_d", "year_d"),
                projection("court_hash", "court_hash"),
                projection("scotus", "scotus"),
            ],
            ..Default::default()
        }))
        .await?
        .into_inner();
    let mut out = Vec::new();
    for hit in response.hits {
        let int = |i: usize| match hit.projected[i].value {
            Some(projected_value::Value::IntValue(v)) => v,
            ref other => panic!("projection {i} of {}: {other:?}", hit.doc_id),
        };
        let uint = |i: usize| match hit.projected[i].value {
            Some(projected_value::Value::UintValue(v)) => v,
            ref other => panic!("projection {i} of {}: {other:?}", hit.doc_id),
        };
        out.push((int(0), int(1), uint(2), int(3)));
    }
    out.sort_unstable();
    Ok(out)
}

fn expected_rows(ids: impl Iterator<Item = usize>) -> Vec<(i64, i64, u64, i64)> {
    let mut out: Vec<_> = ids
        .map(|i| {
            let (y, h, s) = expected(i);
            (decided(i), y, h, s)
        })
        .collect();
    out.sort_unstable();
    out
}

/// Every sealed segment's store file under the catalog
/// (`<root>/segments/<id>/documents.bm25`).
fn segment_stores(index_path: &Path) -> Vec<PathBuf> {
    let root = pipestream_search::node::segments_root(index_path).join("segments");
    let mut out: Vec<PathBuf> = std::fs::read_dir(&root)
        .unwrap()
        .map(|entry| entry.unwrap().path().join("documents.bm25"))
        .filter(|path| path.exists())
        .collect();
    out.sort();
    out
}

/// Ingest `docs` with one unit vector each, in blocks smaller than a
/// seal, so the sealed segments are aligned (one document per vector).
async fn ingest_aligned(addr: &str, docs: Vec<AddDocumentsRequest>) {
    let vectors = common::unit_vectors(docs.len(), DIM, 0x5EED);
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    for (block, chunk) in docs.chunks(SEAL / 2).enumerate() {
        let (tx, rx) = mpsc::channel(chunk.len());
        for doc in chunk {
            tx.send(doc.clone()).await.unwrap();
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
        let start = block * (SEAL / 2) * DIM;
        let (tx, rx) = mpsc::channel(1);
        tx.send(pipestream_search::pb::AddVectorsRequest {
            vectors: vectors[start..start + chunk.len() * DIM].to_vec(),
            dim: DIM as u32,
        })
        .await
        .unwrap();
        drop(tx);
        client.add_vectors(ReceiverStream::new(rx)).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn derived_columns_are_computed_stored_pinned_and_queryable() {
    let dir = tempdir("store");
    let index_path = dir.join("shard.tv");
    let declaration = declaration();
    let (addr, handle) =
        start_empty_node(config(Some(index_path.clone()), Some(declaration.clone()))).await;
    ingest(&addr, (0..N).map(document).collect()).await.unwrap();
    let root_live = root(&addr);
    assert_eq!(
        projected(&root_live, "").await.unwrap(),
        expected_rows(0..N),
        "the derived columns project what the declaration computes"
    );
    // Filters, and query-time calendar arithmetic, read the column like
    // any other; the ingest-only functions refuse by name at query time.
    assert_eq!(
        projected(&root_live, "year_d == 2001 && scotus == 1")
            .await
            .unwrap(),
        expected_rows((0..N).filter(|&i| year(i) == 2001 && court(i) == "scotus")),
    );
    let response = root_live
        .bm25_search(Request::new(Bm25SearchRequest {
            text: "opinion".into(),
            analysis: Some(body_spec()),
            k: N as u32,
            projections: vec![
                projection("y", "calendar.year(decided)"),
                projection("year_d", "year_d"),
            ],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.hits.len(), N);
    for hit in &response.hits {
        assert_eq!(
            hit.projected[0].value, hit.projected[1].value,
            "doc {}",
            hit.doc_id
        );
    }
    for (expression, needle) in [
        ("hash.fnv64(court)", "computes at ingest"),
        ("hash.fnv64(stable_key())", "computes at ingest"),
    ] {
        let err = root_live
            .bm25_search(Request::new(Bm25SearchRequest {
                text: "opinion".into(),
                analysis: Some(body_spec()),
                k: 1,
                projections: vec![projection("h", expression)],
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert!(
            err.message().contains(needle),
            "{expression}: {}",
            err.message()
        );
    }
    NodeServiceClient::connect(addr.clone())
        .await
        .unwrap()
        .flush(FlushRequest {})
        .await
        .unwrap();
    handle.abort();

    // Every sealed segment carries the kind-15 entry, and the log's
    // manifest the fingerprint and the complete column table, derived
    // names after their sources.
    let stores = segment_stores(&index_path);
    assert!(!stores.is_empty());
    for path in &stores {
        let reader = pipestream_search::postings::Bm25Reader::open(path).unwrap();
        let stored = reader
            .derived()
            .expect("the segment records the declaration");
        assert_eq!(
            stored.fingerprint,
            declaration.fingerprint(),
            "{}",
            path.display()
        );
        assert_eq!(stored.validate().unwrap(), spec());
    }
    let (_, gen_dir) =
        pipestream_search::wal::latest_gen(&pipestream_search::wal::wal_dir(&index_path))
            .unwrap()
            .expect("the shard has a log");
    let manifest = pipestream_search::wal::read_manifest(&gen_dir).unwrap();
    assert_eq!(manifest.derived_fingerprint, declaration.fingerprint());
    assert_eq!(manifest.derived, declaration.config());
    let columns = manifest
        .columns
        .expect("the manifest records the column table");
    assert_eq!(
        columns.integers,
        vec!["decided", "placement", "year_d", "scotus"]
    );
    assert_eq!(columns.unsigned_integers, vec!["court_hash"]);
    assert_eq!(columns.facets, vec!["court"]);

    // Reopened under the same declaration the columns are what they
    // were; under another, or none, the store is refused by name.
    let (addr, handle) = pipestream_search::harness::start_opened_node(config(
        Some(index_path.clone()),
        Some(declaration.clone()),
    ))
    .await;
    assert_eq!(
        projected(&root(&addr), "").await.unwrap(),
        expected_rows(0..N)
    );
    handle.abort();
    let mut changed = spec();
    changed.columns[0].expression = "calendar.year(decided) - 1900".into();
    let other = Arc::new(Declaration::compile(&changed).unwrap());
    let err = NodeServiceImpl::open(config(Some(index_path.clone()), Some(other)), None, false)
        .err()
        .unwrap();
    assert!(
        err.contains("written under derived-column declaration") && err.contains("rebuild"),
        "{err}"
    );
    let err = NodeServiceImpl::open(config(Some(index_path.clone()), None), None, false)
        .err()
        .unwrap();
    assert!(err.contains("does not declare"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forged_values_stamped_fingerprints_and_redefinitions_are_refused() {
    let (addr, handle) = start_empty_node(config(None, Some(declaration()))).await;
    let mut forged = document(0);
    forged.integers.push(IntegerValue {
        field: "year_d".into(),
        value: 1066,
    });
    let err = ingest(&addr, vec![forged]).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("refused as forged"),
        "{}",
        err.message()
    );
    let mut stamped = document(0);
    stamped.derived_fingerprint = declaration().fingerprint().to_string();
    let err = ingest(&addr, vec![stamped]).await.unwrap_err();
    assert!(
        err.message().contains("stamps derived fingerprint"),
        "{}",
        err.message()
    );
    let mut redefined = document(0);
    redefined.materialize = Some(MaterializeSpec {
        columns: vec![MaterializedColumn {
            name: "year_d".into(),
            expression: "decided / 2".into(),
            kind: MaterializeKind::I64 as i32,
        }],
    });
    let err = ingest(&addr, vec![redefined]).await.unwrap_err();
    assert!(
        err.message().contains("cannot redefine"),
        "{}",
        err.message()
    );
    // The good document still goes in after the refusals.
    ingest(&addr, vec![document(0)]).await.unwrap();
    assert_eq!(
        projected(&root(&addr), "").await.unwrap(),
        expected_rows(0..1)
    );
    handle.abort();
}

#[test]
fn a_declaration_the_tables_cannot_satisfy_is_refused_at_startup() {
    let bad = Arc::new(
        Declaration::compile(&DerivedColumns {
            columns: vec![column("y", MaterializeKind::I64, "calendar.year(decidedd)")],
        })
        .unwrap(),
    );
    let err = NodeServiceImpl::open(config(None, Some(bad)), None, false)
        .err()
        .unwrap();
    assert!(
        err.contains("reads column \"decidedd\"") && err.contains("does not declare"),
        "{err}"
    );
    let collides = Arc::new(
        Declaration::compile(&DerivedColumns {
            columns: vec![column("decided", MaterializeKind::I64, "decided + 1")],
        })
        .unwrap(),
    );
    let err = NodeServiceImpl::open(config(None, Some(collides)), None, false)
        .err()
        .unwrap();
    assert!(err.contains("collides with a source column"), "{err}");
    // A placement tree reading a column the shard declares nowhere.
    let tree = PlacementTreeConfig {
        column: "placement".into(),
        level_bits: 0,
        nodes: vec![
            PlacementNodeConfig {
                name: "recent".into(),
                cel: Some("yeer >= 2010".into()),
                shards: 1,
                ..Default::default()
            },
            PlacementNodeConfig {
                name: "rest".into(),
                shards: 1,
                ..Default::default()
            },
        ],
    };
    let code = Placement::validate(&tree).unwrap().leaves()[0].code;
    let err = NodeServiceImpl::open(
        NodeConfig {
            placement_column: Some("placement".into()),
            placement_leaf: Some(code),
            placement_tree: Some(Arc::new(PinnedLeaf::pin(&tree, "placement", code).unwrap())),
            ..config(None, Some(declaration()))
        },
        None,
        false,
    )
    .err()
    .unwrap();
    assert!(
        err.contains("reads column \"yeer\"") && err.contains("no table"),
        "{err}"
    );
}

/// The tree under which a pinned shard checks direct rows on a
/// DERIVED column: recent decisions by the computed year.
fn derived_tree() -> PlacementTreeConfig {
    PlacementTreeConfig {
        column: "placement".into(),
        level_bits: 0,
        nodes: vec![
            PlacementNodeConfig {
                name: "recent".into(),
                cel: Some("year_d >= 2010".into()),
                shards: 1,
                ..Default::default()
            },
            PlacementNodeConfig {
                name: "archive".into(),
                shards: 1,
                ..Default::default()
            },
        ],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pinned_shard_judges_direct_rows_on_the_derived_column() {
    let tree = derived_tree();
    let placement = Placement::validate(&tree).unwrap();
    let recent = placement.leaf_by_name("recent").unwrap().code;
    let archive = placement.leaf_by_name("archive").unwrap().code;
    let pinned = |code: i64| NodeConfig {
        placement_column: Some("placement".into()),
        placement_leaf: Some(code),
        placement_tree: Some(Arc::new(PinnedLeaf::pin(&tree, "placement", code).unwrap())),
        ..config(None, Some(declaration()))
    };
    let (recent_addr, recent_handle) = start_empty_node(pinned(recent)).await;
    let (archive_addr, archive_handle) = start_empty_node(pinned(archive)).await;
    // Document 20 decides in 2010 (year(20) = 2010), document 0 in 1990.
    assert_eq!(year(20), 2010);
    ingest(&recent_addr, vec![document(20)]).await.unwrap();
    ingest(&archive_addr, vec![document(0)]).await.unwrap();
    let err = ingest(&archive_addr, vec![document(20)]).await.unwrap_err();
    assert!(
        err.message().contains("year_d >= 2010") && err.message().contains("\"recent\""),
        "{}",
        err.message()
    );
    let err = ingest(&recent_addr, vec![document(0)]).await.unwrap_err();
    assert!(err.message().contains("\"archive\""), "{}", err.message());
    assert_eq!(
        projected(&root(&recent_addr), "").await.unwrap(),
        expected_rows(20..21)
    );
    assert_eq!(
        projected(&root(&archive_addr), "").await.unwrap(),
        expected_rows(0..1)
    );
    recent_handle.abort();
    archive_handle.abort();
}

/// A one-leaf tree for the backfill: every row to one child.
fn one_leaf() -> PlacementTreeConfig {
    PlacementTreeConfig {
        column: "placement".into(),
        level_bits: 0,
        nodes: vec![PlacementNodeConfig {
            name: "all".into(),
            shards: 1,
            ..Default::default()
        }],
    }
}

#[allow(clippy::type_complexity)]
fn no_analyzer() -> impl FnMut(
    &[(
        &str,
        Option<&pipestream_search::pb::AnalysisSpec>,
        pipestream_search::analyzer::SessionLayers,
    )],
) -> Result<Vec<pipestream_search::postings::AnalyzedDoc>, String> {
    |_| Err("a transplant from segments analyzes nothing".to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_backfill_stores_what_direct_ingest_stores() {
    let dir = tempdir("backfill");
    // The source: written with no declaration at all, sealed, flushed.
    let source_path = dir.join("source.tv");
    let (addr, handle) = start_empty_node(config(Some(source_path.clone()), None)).await;
    // The reshard tool needs the log's locked vector backend, vectors
    // or not.
    let (shift, scale) = common::fit_calibration(DIM, 4, &common::unit_vectors(8, DIM, 7));
    NodeServiceClient::connect(addr.clone())
        .await
        .unwrap()
        .set_calibration(pipestream_search::pb::SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: 4,
            shift,
            scale,
        })
        .await
        .unwrap();
    ingest_aligned(&addr, (0..N).map(document).collect()).await;
    NodeServiceClient::connect(addr.clone())
        .await
        .unwrap()
        .flush(FlushRequest {})
        .await
        .unwrap();
    handle.abort();
    let (_, gen_dir) =
        pipestream_search::wal::latest_gen(&pipestream_search::wal::wal_dir(&source_path))
            .unwrap()
            .unwrap();
    let declaration = declaration();
    let every: Vec<String> = spec().columns.iter().map(|c| c.name.clone()).collect();
    let split = |out: &Path, options: reshard::TreeSplitOptions| {
        reshard::split_placement_tree_logs(
            std::slice::from_ref(&gen_dir),
            &one_leaf(),
            out,
            &[0],
            None,
            options,
            &mut no_analyzer(),
        )
    };
    // The refusals, by name.
    let err = split(
        &dir.join("no-declaration"),
        reshard::TreeSplitOptions {
            source: reshard::TreeRowSource::Segments,
            derive: vec!["year_d".into()],
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(err.contains("needs a declaration"), "{err}");
    let err = split(
        &dir.join("unknown"),
        reshard::TreeSplitOptions {
            source: reshard::TreeRowSource::Segments,
            derived: Some(declaration.clone()),
            derive: vec!["decade".into()],
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(err.contains("--derive=decade names no column"), "{err}");
    let err = split(
        &dir.join("cut"),
        reshard::TreeSplitOptions {
            source: reshard::TreeRowSource::Segments,
            derived: Some(declaration.clone()),
            derive: every.clone(),
            cut: reshard::SpillCut::Column {
                column: "year_d".into(),
                rows_per_cut: 8,
            },
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(err.contains("being derived in this run"), "{err}");
    let err = split(
        &dir.join("partial"),
        reshard::TreeSplitOptions {
            source: reshard::TreeRowSource::Segments,
            derived: Some(declaration.clone()),
            derive: vec!["year_d".into()],
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(
        err.contains("never derived") && err.contains("\"court_hash\""),
        "{err}"
    );

    // The backfill proper: every declared column computed from the
    // sealed rows, the child written under the declaration.
    let out = split(
        &dir.join("child"),
        reshard::TreeSplitOptions {
            source: reshard::TreeRowSource::Segments,
            derived: Some(declaration.clone()),
            derive: every,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(out.derived, declaration.config());
    let map = reshard::tree_shard_map_toml(&out, &one_leaf()).unwrap();
    assert!(
        map.contains("[[derived]]") && map.contains("calendar.year(decided)"),
        "{map}"
    );
    let image = &out.images.children[0];
    for path in segment_stores(&image.vector_path) {
        let reader = pipestream_search::postings::Bm25Reader::open(&path).unwrap();
        assert_eq!(
            reader.derived().map(|d| d.fingerprint.as_str()),
            Some(declaration.fingerprint()),
            "{}",
            path.display()
        );
        let integers: Vec<&str> = (0..reader.integer_count())
            .map(|i| reader.integer_name(i))
            .collect();
        assert_eq!(integers, vec!["decided", "placement", "year_d", "scotus"]);
        let unsigned: Vec<&str> = (0..reader.unsigned_integer_count())
            .map(|i| reader.unsigned_integer_name(i))
            .collect();
        assert_eq!(unsigned, vec!["court_hash"]);
    }
    let code = Placement::validate(&one_leaf()).unwrap().leaves()[0].code;
    let child = |derived: Option<Arc<Declaration>>| NodeConfig {
        placement_column: Some("placement".into()),
        placement_leaf: Some(code),
        placement_tree: Some(Arc::new(
            PinnedLeaf::pin(&one_leaf(), "placement", code).unwrap(),
        )),
        ..config(Some(image.vector_path.clone()), derived)
    };
    let err = NodeServiceImpl::open(child(None), None, false)
        .err()
        .unwrap();
    assert!(
        err.contains("written under derived-column declaration"),
        "{err}"
    );
    let (addr, handle) =
        pipestream_search::harness::start_opened_node(child(Some(declaration.clone()))).await;
    let backfilled = projected(&root(&addr), "").await.unwrap();
    assert_eq!(backfilled.len(), N);
    handle.abort();

    // Direct ingest under the same declaration stores the same values,
    // row for row.
    let (addr, handle) = start_empty_node(config(None, Some(declaration.clone()))).await;
    ingest(&addr, (0..N).map(document).collect()).await.unwrap();
    let direct = projected(&root(&addr), "").await.unwrap();
    handle.abort();
    assert_eq!(backfilled, direct);
    assert_eq!(direct, expected_rows(0..N));
}
