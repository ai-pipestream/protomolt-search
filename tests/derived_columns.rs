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
    // Every value family refuses the same way, not only integers.
    let mut forged = document(0);
    forged
        .unsigned_integers
        .push(pipestream_search::pb::UnsignedIntegerValue {
            field: "court_hash".into(),
            value: 42,
        });
    let err = ingest(&addr, vec![forged]).await.unwrap_err();
    assert!(
        err.message().contains("refused as forged"),
        "{}",
        err.message()
    );
    let mut forged = document(0);
    forged.numerics.push(pipestream_search::pb::NumericValue {
        field: "court_hash".into(),
        value: 1.0,
    });
    let err = ingest(&addr, vec![forged]).await.unwrap_err();
    assert!(
        err.message().contains("refused as forged"),
        "{}",
        err.message()
    );
    let mut forged = document(0);
    forged
        .timestamps
        .push(pipestream_search::pb::TimestampValue {
            field: "year_d".into(),
            value: Some(prost_types::Timestamp {
                seconds: 1_420_070_400,
                nanos: 0,
            }),
        });
    let err = ingest(&addr, vec![forged]).await.unwrap_err();
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
            derive: every.clone(),
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
    // The reconciliation: every source row in the child once, carried
    // whole, the derived values agreeing with an independent count of
    // the civil year; a child written under another declaration is
    // named row for row.
    let reconcile = |declaration: Arc<Declaration>| {
        pipestream_search::reconcile::reconcile(&pipestream_search::reconcile::ReconcileOptions {
            sources: vec![gen_dir.clone()],
            child: image.vector_path.clone(),
            tree: one_leaf(),
            child_index: 0,
            declaration: Some(declaration),
            derive: every.clone(),
            checks: vec![pipestream_search::reconcile::ColumnCheck::CivilYear {
                column: "year_d".into(),
                timestamp: "decided".into(),
            }],
            threads: 2,
        })
        .unwrap()
    };
    let report = reconcile(declaration.clone());
    assert!(report.is_clean(), "{}", report.render());
    assert_eq!(
        (
            report.source_rows_to_child,
            report.child_rows_live,
            report.matched
        ),
        (N as u64, N as u64, N as u64),
        "{}",
        report.render()
    );
    assert_eq!(report.checks[0].1.both, N as u64, "{}", report.render());
    let mut other = spec();
    other.columns[0].expression = "calendar.year(decided) - 1".into();
    let report = reconcile(Arc::new(Declaration::compile(&other).unwrap()));
    assert!(!report.is_clean());
    assert_eq!(
        (
            report.matched,
            report.unmatched_source,
            report.unmatched_child
        ),
        (0, N as u64, N as u64),
        "{}",
        report.render()
    );
    assert!(
        report
            .problems
            .iter()
            .any(|p| p.contains("carries declaration") && p.contains(declaration.fingerprint())),
        "{}",
        report.render()
    );
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

/// A changed declaration: `calendar.year(decided) - 1900` reads the
/// same input and stores the same kind, so only the fingerprints tell
/// it apart from `spec()`.
fn changed_declaration() -> Arc<Declaration> {
    let mut changed = spec();
    changed.columns[0].expression = "calendar.year(decided) - 1900".into();
    Arc::new(Declaration::compile(&changed).unwrap())
}

#[test]
fn an_empty_store_keeps_its_declaration_through_save_and_attach() {
    let dir = tempdir("empty-store");
    let index_path = dir.join("shard.tv");
    let declaration = declaration();
    // A zero-row store written under the declaration: the kind-15
    // entry and the complete tables, no rows.
    let mut store = pipestream_search::postings::Bm25Store::with_fields(&["body"])
        .with_facets(&["court"])
        .with_integers(&["decided", "placement", "year_d", "scotus"])
        .with_unsigned_integers(&["court_hash"]);
    store.set_derived(Some(pipestream_search::postings::StoredDerived::of(
        &declaration,
    )));
    let path = pipestream_search::node::bm25_sidecar_path(&index_path);
    store.save(&path).unwrap();
    let reader = pipestream_search::postings::Bm25Reader::open(&path).unwrap();
    let stored = reader
        .derived()
        .expect("the empty store keeps the declaration");
    assert_eq!(stored.fingerprint, declaration.fingerprint());
    assert_eq!(stored.validate().unwrap(), spec());
    let integers: Vec<&str> = (0..reader.integer_count())
        .map(|i| reader.integer_name(i))
        .collect();
    assert_eq!(integers, vec!["decided", "placement", "year_d", "scotus"]);
    let unsigned: Vec<&str> = (0..reader.unsigned_integer_count())
        .map(|i| reader.unsigned_integer_name(i))
        .collect();
    assert_eq!(unsigned, vec!["court_hash"]);
    drop(reader);

    // It attaches under its own declaration; every other pairing
    // refuses by name, both fingerprints in the message.
    let single = |derived: Option<Arc<Declaration>>| NodeConfig {
        layout: Layout::SingleImage,
        ..config(Some(index_path.clone()), derived)
    };
    NodeServiceImpl::open(single(Some(declaration.clone())), None, false).unwrap();
    let other = changed_declaration();
    let err = NodeServiceImpl::open(single(Some(other.clone())), None, false)
        .err()
        .unwrap();
    assert!(
        err.contains("written under derived-column declaration")
            && err.contains(declaration.fingerprint())
            && err.contains(other.fingerprint()),
        "{err}"
    );
    let err = NodeServiceImpl::open(single(None), None, false)
        .err()
        .unwrap();
    assert!(err.contains("does not declare"), "{err}");

    // A zero-row SEGMENT, though, never enters a catalog: the row
    // alignment check refuses it by name rather than publishing an
    // empty part.
    let live_path = dir.join("live-docs.bin");
    pipestream_search::live_docs::LiveDocs::default()
        .write(&live_path, 0)
        .unwrap();
    let catalog =
        pipestream_search::segments::SegmentCatalog::open(dir.join("catalog.segments")).unwrap();
    let err = catalog
        .append(pipestream_search::segments::SegmentSource {
            segment_id: "seg-empty",
            generation: 1,
            base_label: 0,
            backend_kind: "",
            vector_path: None,
            exact_vector_path: None,
            bm25_path: &path,
            live_docs_path: &live_path,
            partition_column: None,
        })
        .unwrap_err();
    assert!(err.contains("not aligned"), "{err}");

    // A zero-row store written under NO declaration may still attach:
    // "written under none" is refused only once the store holds rows.
    // Each open gets its own store: the first open's log records the
    // declaration, so a later open of the same path under none would
    // refuse the log, not the store.
    let plain = |name: &str| {
        let index_path = dir.join(name);
        pipestream_search::postings::Bm25Store::with_fields(&["body"])
            .with_facets(&["court"])
            .with_integers(&["decided", "placement"])
            .save(&pipestream_search::node::bm25_sidecar_path(&index_path))
            .unwrap();
        index_path
    };
    NodeServiceImpl::open(
        NodeConfig {
            layout: Layout::SingleImage,
            ..config(Some(plain("plain-declared.tv")), Some(declaration.clone()))
        },
        None,
        false,
    )
    .expect("an empty store written under none attaches");
    NodeServiceImpl::open(
        NodeConfig {
            layout: Layout::SingleImage,
            ..config(Some(plain("plain-undeclared.tv")), None)
        },
        None,
        false,
    )
    .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_log_written_under_another_declaration_refuses_to_resume() {
    let dir = tempdir("wal-declaration");
    let index_path = dir.join("shard.tv");
    let node = NodeServiceImpl::new(None, config(Some(index_path.clone()), Some(declaration())));
    drop(node);
    let other = changed_declaration();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        NodeServiceImpl::new(None, config(Some(index_path.clone()), Some(other.clone())))
    }));
    let payload = match result {
        Ok(_) => panic!("the mismatched log must refuse the node"),
        Err(payload) => payload,
    };
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("the log was written under derived-column declaration")
            && message.contains(declaration().fingerprint())
            && message.contains(other.fingerprint()),
        "{message}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_installed_snapshot_keeps_the_declaration_and_tables() {
    let dir = tempdir("snapshot");
    // The source: documents ingested under the declaration and flushed
    // to a single image.
    let source_path = dir.join("source.tv");
    let source_config = NodeConfig {
        layout: Layout::SingleImage,
        ..config(Some(source_path.clone()), Some(declaration()))
    };
    let (addr, handle) = start_empty_node(source_config).await;
    let (shift, scale) = common::fit_calibration(DIM, 4, &common::unit_vectors(8, DIM, 7));
    async fn seed(addr: &str, shift: Vec<f32>, scale: Vec<f32>) {
        NodeServiceClient::connect(addr.to_string())
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
    }
    seed(&addr, shift.clone(), scale.clone()).await;
    ingest_aligned(&addr, (0..N).map(document).collect()).await;
    NodeServiceClient::connect(addr.clone())
        .await
        .unwrap()
        .flush(FlushRequest {})
        .await
        .unwrap();
    handle.abort();
    let source_bm25 = pipestream_search::node::bm25_sidecar_path(&source_path);
    assert!(source_path.exists() && source_bm25.exists());

    // The target installs it under the same declaration; the installed
    // generation's store keeps the fingerprint and the complete tables,
    // and the shard serves the derived values, before and after a
    // restart.
    let target_path = dir.join("target.tv");
    let target_config = |derived: Option<Arc<Declaration>>| NodeConfig {
        layout: Layout::SingleImage,
        ..config(Some(target_path.clone()), derived)
    };
    let declaration = declaration();
    let (taddr, thandle) = start_empty_node(target_config(Some(declaration.clone()))).await;
    seed(&taddr, shift.clone(), scale.clone()).await;
    let report =
        pipestream_search::snapshot::install_snapshot(&taddr, &source_path, Some(&source_bm25))
            .await
            .unwrap();
    assert_eq!(report.num_documents, N as u64);
    let installed = pipestream_search::node::generation_bm25(
        &pipestream_search::node::generation_dir(&target_path),
    );
    let reader = pipestream_search::postings::Bm25Reader::open(&installed).unwrap();
    let stored = reader
        .derived()
        .expect("the installed store keeps the declaration");
    assert_eq!(stored.fingerprint, declaration.fingerprint());
    assert_eq!(stored.validate().unwrap(), spec());
    let integers: Vec<&str> = (0..reader.integer_count())
        .map(|i| reader.integer_name(i))
        .collect();
    assert_eq!(integers, vec!["decided", "placement", "year_d", "scotus"]);
    let unsigned: Vec<&str> = (0..reader.unsigned_integer_count())
        .map(|i| reader.unsigned_integer_name(i))
        .collect();
    assert_eq!(unsigned, vec!["court_hash"]);
    drop(reader);
    assert_eq!(
        projected(&root(&taddr), "").await.unwrap(),
        expected_rows(0..N)
    );
    thandle.abort();
    let (taddr, thandle) =
        pipestream_search::harness::start_opened_node(target_config(Some(declaration.clone())))
            .await;
    assert_eq!(
        projected(&root(&taddr), "").await.unwrap(),
        expected_rows(0..N),
        "the installed generation survives a restart"
    );
    thandle.abort();

    // A shard under another declaration refuses the same install,
    // naming both fingerprints, and stays empty.
    let other_path = dir.join("other.tv");
    let other = changed_declaration();
    let (oaddr, ohandle) = start_empty_node(NodeConfig {
        layout: Layout::SingleImage,
        ..config(Some(other_path.clone()), Some(other.clone()))
    })
    .await;
    seed(&oaddr, shift, scale).await;
    let err =
        pipestream_search::snapshot::install_snapshot(&oaddr, &source_path, Some(&source_bm25))
            .await
            .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message()
            .contains("written under derived-column declaration")
            && err.message().contains(declaration.fingerprint())
            && err.message().contains(other.fingerprint()),
        "{}",
        err.message()
    );
    let empty = root(&oaddr)
        .bm25_search(Request::new(Bm25SearchRequest {
            text: "opinion".into(),
            analysis: Some(body_spec()),
            k: 1,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(
        empty.hits.is_empty(),
        "a refused install leaves the shard untouched"
    );
    ohandle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seal_interrupted_mid_publish_never_serves_a_partial_segment() {
    let dir = tempdir("crash-seal");
    let index_path = dir.join("shard.tv");
    let declaration = declaration();
    let (addr, handle) =
        start_empty_node(config(Some(index_path.clone()), Some(declaration.clone()))).await;
    ingest(&addr, (0..N).map(document).collect()).await.unwrap();
    NodeServiceClient::connect(addr.clone())
        .await
        .unwrap()
        .flush(FlushRequest {})
        .await
        .unwrap();
    // The crash: the server task dies with no further writes.
    handle.abort();
    let root_path = pipestream_search::node::segments_root(&index_path);
    let segments_dir = root_path.join("segments");
    let set = pipestream_search::segments::OpenedSegmentSet::open(&root_path).unwrap();
    assert!(set.len() >= 2, "the flush sealed more than one segment");
    let sealed = set.len();
    let first_id = set.metadata(0).segment_id.clone();
    let first_dir = segments_dir.join(&first_id);
    drop(set);

    // The leftovers a seal that died mid-publish leaves: the catalog's
    // staging dir with a torn store, the segment's final directory
    // never listed by the manifest, and the node's own stage dir.
    let torn = segments_dir.join(".tmp-seg-dead-4242-5");
    std::fs::create_dir_all(&torn).unwrap();
    let whole = std::fs::read(first_dir.join("documents.bm25")).unwrap();
    std::fs::write(torn.join("documents.bm25"), &whole[..whole.len() / 2]).unwrap();
    let copy_dir = |from: &Path, to: &Path| {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
        }
    };
    copy_dir(&first_dir, &segments_dir.join("seg-orphan"));
    let stage = root_path.join(".seal-dead-4242");
    std::fs::create_dir_all(&stage).unwrap();
    std::fs::write(stage.join("documents.bm25"), &whole[..whole.len() / 4]).unwrap();

    // Reopen: the catalog serves exactly the flushed rows under the
    // declaration; the leftovers contribute nothing.
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
    let set = pipestream_search::segments::OpenedSegmentSet::open(&root_path).unwrap();
    assert_eq!(set.len(), sealed, "the orphan segment is not adopted");
    drop(set);

    // A manifest-listed segment with torn bytes is named, never
    // served: the integrity check fires before any row is read.
    let mut torn_bytes = whole.clone();
    let middle = torn_bytes.len() / 2;
    torn_bytes[middle] ^= 0x5a;
    std::fs::write(first_dir.join("documents.bm25"), &torn_bytes).unwrap();
    let err = NodeServiceImpl::open(
        config(Some(index_path.clone()), Some(declaration.clone())),
        None,
        false,
    )
    .err()
    .unwrap();
    assert!(
        err.contains("integrity mismatch") && err.contains(&first_id),
        "{err}"
    );
    std::fs::write(first_dir.join("documents.bm25"), &whole).unwrap();

    // A torn manifest the atomic publish never completed: named at
    // parse, not served.
    let manifest_path = root_path.join("segments.json");
    let manifest = std::fs::read(&manifest_path).unwrap();
    std::fs::write(&manifest_path, &manifest[..manifest.len() / 2]).unwrap();
    let err = NodeServiceImpl::open(
        config(Some(index_path.clone()), Some(declaration)),
        None,
        false,
    )
    .err()
    .unwrap();
    assert!(err.contains("segment set"), "{err}");
    std::fs::write(&manifest_path, &manifest).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_serving_routes_enforce_the_inputs_rule() {
    use pipestream_search::pb::{
        search_query, selection_query, AccessAction, AccessPolicy, CollectionGrant,
        CollectionResource, FieldAction, FieldGrant, FieldPermissions, LexicalQuery, QueryRequest,
        QuerySort, SearchQuery, SelectionQuery,
    };
    use FieldAction::{Disclose, Use};
    let declaration = declaration();
    let node = Arc::new(NodeServiceImpl::new(
        None,
        config(None, Some(declaration.clone())),
    ));
    pipestream_search::link::NodeLink::local(node.clone())
        .add_documents(tokio_stream::iter((0..6).map(document)))
        .await
        .unwrap();
    let coordinator = CoordinatorServiceImpl::with_local_nodes(vec![node])
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_derived(Some(declaration));

    let reader = |fields: &[(&str, &[FieldAction])]| {
        let permissions = FieldPermissions {
            grants: fields
                .iter()
                .map(|(field, actions)| FieldGrant {
                    field: (*field).into(),
                    actions: actions.iter().map(|a| *a as i32).collect(),
                })
                .collect(),
            disclose_document_identity: false,
        };
        pipestream_search::collections::CollectionSet::single(coordinator.clone()).with_principals(
            Arc::new(
                pipestream_search::security::Principals::from_configs(&[
                    pipestream_search::security::PrincipalConfig {
                        name: "reader".into(),
                        token: "reader-token-0123456789012345".into(),
                        ..Default::default()
                    },
                ])
                .unwrap()
                .with_authorizer(Arc::new(
                    pipestream_search::authorization::PolicyAuthority::new(AccessPolicy {
                        format_version: 3,
                        revision: 1,
                        resources: vec![CollectionResource {
                            workspace: "work".into(),
                            collection: "".into(),
                        }],
                        grants: vec![CollectionGrant {
                            principal: "reader".into(),
                            workspace: "work".into(),
                            collection: "".into(),
                            actions: vec![AccessAction::Search as i32],
                            field_permissions: Some(permissions),
                            document_visibility: None,
                        }],
                    })
                    .unwrap(),
                )),
            ),
        )
    };
    let bearer = |mut request: tonic::Request<Bm25SearchRequest>| {
        request.metadata_mut().insert(
            "authorization",
            "Bearer reader-token-0123456789012345".parse().unwrap(),
        );
        request
    };
    let body: (&str, &[FieldAction]) = ("body", &[Use, Disclose]);
    let full = reader(&[
        body,
        ("court_hash", &[Use, Disclose]),
        ("court", &[Use, Disclose]),
    ]);
    let no_input = reader(&[body, ("court_hash", &[Use, Disclose])]);
    let no_own = reader(&[body, ("court", &[Use, Disclose])]);
    let use_only_input = reader(&[body, ("court_hash", &[Use, Disclose]), ("court", &[Use])]);

    let scotus_hash = fnv1a64_bytes(b"scotus");
    let filtered = || Bm25SearchRequest {
        text: "opinion".into(),
        analysis: Some(body_spec()),
        k: 6,
        filter: format!("court_hash == {scotus_hash}u"),
        projections: vec![projection("court_hash", "court_hash")],
        ..Default::default()
    };
    // The scope holding the grant on the column and on every input may
    // filter and project it.
    let response = SearchService::bm25_search(&full, bearer(Request::new(filtered())))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.hits.len(), 2, "documents 0 and 3 are scotus");
    for hit in &response.hits {
        assert_eq!(
            hit.projected[0].value,
            Some(projected_value::Value::UintValue(scotus_hash))
        );
    }
    // Missing the input grant, or the column's own grant, refuses.
    for (who, reader) in [("no input grant", &no_input), ("no own grant", &no_own)] {
        let err = SearchService::bm25_search(reader, bearer(Request::new(filtered())))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied, "{who}");
    }

    // Column statistics disclose: the input's Use grant alone does not
    // admit them.
    let faceted = || Bm25SearchRequest {
        text: "opinion".into(),
        analysis: Some(body_spec()),
        k: 6,
        stats_fields: vec!["court_hash".into()],
        ..Default::default()
    };
    let response = SearchService::bm25_search(&full, bearer(Request::new(faceted())))
        .await
        .unwrap()
        .into_inner();
    let court_stats = response
        .stats
        .iter()
        .find(|f| f.field == "court_hash")
        .expect("the column stats");
    assert_eq!(court_stats.count, 6);
    let err = SearchService::bm25_search(&use_only_input, bearer(Request::new(faceted())))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);

    // Explanations disclose the fields they name: an explained score
    // stage over the derived column needs Disclose on its input.
    let stage = || pipestream_search::pb::ScoreStage {
        column: "court_hash".into(),
        operation: Some(pipestream_search::pb::score_stage::Operation::Op(
            pipestream_search::pb::ScoreOp::AddLinear as i32,
        )),
        weight: 1.0,
        ..Default::default()
    };
    let explained = || Bm25SearchRequest {
        text: "opinion".into(),
        analysis: Some(body_spec()),
        k: 6,
        filter: format!("court_hash == {scotus_hash}u"),
        score_stages: vec![stage()],
        explain: true,
        ..Default::default()
    };
    let response = SearchService::bm25_search(&full, bearer(Request::new(explained())))
        .await
        .unwrap()
        .into_inner();
    assert!(response.hits.iter().all(|h| h.explain.is_some()));
    assert!(
        response
            .hits
            .iter()
            .all(|h| h.explain.as_ref().unwrap().stages[0].column == "court_hash"),
        "the explanation names the derived column"
    );
    let err = SearchService::bm25_search(&use_only_input, bearer(Request::new(explained())))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);

    // The Query route's sort on the derived column follows the same rule.
    let sorted = || QueryRequest {
        k: 6,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "lex".into(),
                query: Some(search_query::Query::Lexical(LexicalQuery {
                    text: "opinion".into(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                })),
            })),
        }),
        sort: vec![QuerySort {
            column: "court_hash".into(),
            descending: false,
        }],
        ..Default::default()
    };
    let mut bearer_q = Request::new(sorted());
    bearer_q.metadata_mut().insert(
        "authorization",
        "Bearer reader-token-0123456789012345".parse().unwrap(),
    );
    let response = SearchService::query(&full, bearer_q)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.hits.len(), 6);
    let hashes: Vec<u64> = (0..6).map(|i| fnv1a64_bytes(court(i).as_bytes())).collect();
    let mut sorted_hashes = hashes.clone();
    sorted_hashes.sort_unstable();
    assert_eq!(
        response
            .hits
            .iter()
            .map(|h| hashes[h.doc_id as usize])
            .collect::<Vec<_>>(),
        sorted_hashes,
        "the sort orders by the derived column"
    );
    let mut bearer_q = Request::new(sorted());
    bearer_q.metadata_mut().insert(
        "authorization",
        "Bearer reader-token-0123456789012345".parse().unwrap(),
    );
    let err = SearchService::query(&no_input, bearer_q).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}
