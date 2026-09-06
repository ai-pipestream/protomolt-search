//! Placement trees (`docs/placement.md`): a shard map whose leaves are
//! CEL predicates, routed ingest that evaluates the tree per document,
//! the node-side placement rule, and the coordinator skipping shards a
//! filter cannot match. The answer never moves: every pruning case is
//! compared bitwise against a coordinator with `shard_pruning` off.

mod common;

use std::path::PathBuf;

use common::{fit_calibration, start_empty_node, start_opened_node, unit_vectors};
use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::config::ShardMap;
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::mapping::derive_plan;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    routed_ingest_mapped_request, selection_query, AddDocumentsRequest, BooleanQuery,
    CompositeSearchStrategy, DenseQuery, FacetValue, FilterQuery, FlushRequest, HealthRequest,
    IntegerValue, LexicalQuery, MappedBind, QueryHit, QueryRequest, QueryResponse,
    RoutedIngestMappedRequest, RoutedMappedBind, RoutedMappedDocument, SearchQuery,
    SelectionOperator, SelectionQuery, SetCalibrationRequest,
};
use pipestream_search::placement::{Placement, PlacementNodeConfig, PlacementTreeConfig};
use prost::Message as _;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

const DIM: usize = 16;
const BIT_WIDTH: usize = 4;
const COLUMN: &str = "placement";

// ---------------------------------------------------------------------
// The tree: old (year < 2000), recent.scotus, recent.rest, other
// ---------------------------------------------------------------------

fn node(name: &str, cel: Option<&str>) -> PlacementNodeConfig {
    PlacementNodeConfig {
        name: name.into(),
        cel: cel.map(str::to_string),
        shards: 1,
        ..Default::default()
    }
}

fn tree() -> PlacementTreeConfig {
    let mut recent = node("recent", Some("year >= 2020"));
    recent.shards = 0;
    recent.children = vec![
        node("scotus", Some("court_code == \"scotus\"")),
        node("rest", None),
    ];
    PlacementTreeConfig {
        column: COLUMN.into(),
        level_bits: 0,
        nodes: vec![
            node("old", Some("year < 2000")),
            recent,
            node("other", None),
        ],
    }
}

const LEAVES: [&str; 4] = ["old", "recent.scotus", "recent.rest", "other"];

fn codes() -> Vec<i64> {
    let placement = Placement::validate(&tree()).unwrap();
    LEAVES
        .iter()
        .map(|name| placement.leaf_by_name(name).unwrap().code)
        .collect()
}

// ---------------------------------------------------------------------
// A law.v1.Case message: id, title (the body), embedding, year, court
// ---------------------------------------------------------------------

fn scalar(name: &str, number: i32, typ: Type, label: Label) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        label: Some(label as i32),
        r#type: Some(typ as i32),
        ..Default::default()
    }
}

fn case_set() -> Vec<u8> {
    let case = DescriptorProto {
        name: Some("Case".to_string()),
        field: vec![
            scalar("id", 1, Type::String, Label::Optional),
            scalar("title", 2, Type::String, Label::Optional),
            scalar("embedding", 3, Type::Float, Label::Repeated),
            scalar("year", 4, Type::Int64, Label::Optional),
            scalar("court_code", 5, Type::String, Label::Optional),
        ],
        ..Default::default()
    };
    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("case.proto".to_string()),
            package: Some("law.v1".to_string()),
            message_type: vec![case],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

fn vint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

fn w_str(out: &mut Vec<u8>, field: u64, value: &str) {
    vint(out, field << 3 | 2);
    vint(out, value.len() as u64);
    out.extend_from_slice(value.as_bytes());
}

fn w_varint(out: &mut Vec<u8>, field: u64, value: u64) {
    vint(out, field << 3);
    vint(out, value);
}

fn w_packed_floats(out: &mut Vec<u8>, field: u64, values: &[f32]) {
    vint(out, field << 3 | 2);
    vint(out, (values.len() * 4) as u64);
    for v in values {
        out.extend_from_slice(&v.to_le_bytes());
    }
}

/// The corpus: `(year, court, expected leaf)` per document. Document 4
/// has no year, so it is UNKNOWN on both year predicates and takes the
/// root default; document 5 has no court and takes the recent default.
fn corpus() -> Vec<(Option<i64>, Option<&'static str>, &'static str)> {
    vec![
        (Some(1990), Some("ca9"), "old"),
        (Some(2021), Some("scotus"), "recent.scotus"),
        (Some(2022), Some("ca9"), "recent.rest"),
        (Some(2010), Some("scotus"), "other"),
        (None, Some("scotus"), "other"),
        (Some(2023), None, "recent.rest"),
        (Some(1995), Some("scotus"), "old"),
        (Some(2020), Some("scotus"), "recent.scotus"),
    ]
}

fn vectors() -> Vec<f32> {
    unit_vectors(corpus().len(), DIM, 0x9A7E_0042)
}

fn encode(i: usize) -> Vec<u8> {
    let (year, court, _) = corpus()[i];
    let mut out = Vec::new();
    w_str(&mut out, 1, &format!("case-{i}"));
    w_str(&mut out, 2, &format!("opinion {i} about search"));
    let all = vectors();
    w_packed_floats(&mut out, 3, &all[i * DIM..(i + 1) * DIM]);
    if let Some(year) = year {
        w_varint(&mut out, 4, year as u64);
    }
    if let Some(court) = court {
        w_str(&mut out, 5, court);
    }
    out
}

fn bind() -> MappedBind {
    let plan = derive_plan(&case_set(), "law.v1.Case").expect("the Case plan derives");
    MappedBind {
        collection: String::new(),
        descriptor_set: case_set(),
        message_type: "law.v1.Case".into(),
        expected_fingerprint: plan.fingerprint,
        body_path: "title".into(),
        analysis: Some(body_spec()),
        materialize: None,
        field_analysis: Vec::new(),
    }
}

// ---------------------------------------------------------------------
// Nodes and the coordinator
// ---------------------------------------------------------------------

fn tempdir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("placement-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn config(leaf: Option<i64>, slot_offset: u64) -> NodeConfig {
    NodeConfig {
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        facet_fields: vec!["id".into(), "court_code".into()],
        integer_fields: vec!["year".into()],
        placement_column: Some(COLUMN.into()),
        placement_leaf: leaf,
        slot_offset,
        wal: false,
        ..Default::default()
    }
}

async fn calibrate(addr: &str) {
    let all = vectors();
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &all);
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    client
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: BIT_WIDTH as u32,
            shift,
            scale,
        })
        .await
        .unwrap();
}

type Served = (
    String,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
);

/// One node per leaf, pinned to the leaf's code, in leaf order.
async fn leaf_nodes() -> Vec<Served> {
    let mut out = Vec::new();
    for (i, code) in codes().into_iter().enumerate() {
        let served = start_empty_node(config(Some(code), 1_000 * i as u64)).await;
        calibrate(&served.0).await;
        out.push(served);
    }
    out
}

fn coordinator(addrs: &[String], shard_pruning: bool) -> CoordinatorServiceImpl {
    let ranges = vec![Some((0, u64::MAX)); addrs.len()];
    let placement = codes().into_iter().map(Some).collect();
    CoordinatorServiceImpl::new(addrs.to_vec())
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_topology_generation(1)
        .with_shard_pruning(shard_pruning)
        .with_hot_topology_placed(ranges, Some((tree(), placement)))
        .unwrap()
}

/// Route `docs` through the coordinator's placement evaluation.
async fn routed(
    c: &CoordinatorServiceImpl,
    docs: &[usize],
) -> Result<pipestream_search::pb::RoutedIngestMappedResponse, tonic::Status> {
    let stream: Vec<Result<RoutedIngestMappedRequest, tonic::Status>> = docs
        .iter()
        .map(|&i| RoutedIngestMappedRequest {
            payload: Some(routed_ingest_mapped_request::Payload::Document(
                RoutedMappedDocument {
                    stable_key: format!("law.v1.Case/case-{i}").into_bytes(),
                    document: encode(i),
                },
            )),
        })
        .map(Ok)
        .collect();
    c.routed_ingest_mapped_bound(
        RoutedMappedBind {
            collection: String::new(),
            required_topology_generation: 1,
            bind: Some(bind()),
        },
        tokio_stream::iter(stream),
    )
    .await
}

async fn health_docs(addr: &str) -> u64 {
    NodeServiceClient::connect(addr.to_string())
        .await
        .unwrap()
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner()
        .document_slots
}

// ---------------------------------------------------------------------
// Query shapes
// ---------------------------------------------------------------------

fn cel(id: &str, cel: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Filter(FilterQuery {
            id: id.to_string(),
            predicate: Some(pipestream_search::pb::filter_query::Predicate::Cel(
                cel.to_string(),
            )),
        })),
    }
}

fn lexical(id: &str, text: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(pipestream_search::pb::search_query::Query::Lexical(
                LexicalQuery {
                    text: text.to_string(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                },
            )),
        })),
    }
}

fn dense(id: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(pipestream_search::pb::search_query::Query::Dense(
                DenseQuery {
                    vector: vectors()[..DIM].to_vec(),
                    ..Default::default()
                },
            )),
        })),
    }
}

fn and(clauses: Vec<SelectionQuery>) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
            operator: SelectionOperator::And as i32,
            clauses,
            scoring: None,
        })),
    }
}

fn boolean(must: Vec<SelectionQuery>) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Boolean(BooleanQuery {
            must,
            should: Vec::new(),
            must_not: Vec::new(),
            minimum_should_match: 0,
            aggregate: None,
        })),
    }
}

fn request(selection: SelectionQuery) -> QueryRequest {
    QueryRequest {
        request_id: "place".into(),
        k: 20,
        selection: Some(selection),
        profile: true,
        ..Default::default()
    }
}

async fn query(c: &CoordinatorServiceImpl, selection: SelectionQuery) -> QueryResponse {
    SearchService::query(c, Request::new(request(selection)))
        .await
        .unwrap()
        .into_inner()
}

fn ids(hits: &[QueryHit]) -> Vec<(u64, f32)> {
    hits.iter().map(|h| (h.doc_id, h.score)).collect()
}

fn shard_counts(response: &QueryResponse) -> (u32, u32) {
    let p = response.profile.as_ref().expect("profile requested");
    (p.shards_total, p.shards_skipped)
}

/// The ids under one leaf, by its code, on a browse of the placement
/// column.
async fn leaf_ids(c: &CoordinatorServiceImpl, code: i64) -> Vec<u64> {
    let response = query(c, cel("f", &format!("{COLUMN} == {code}"))).await;
    let mut out: Vec<u64> = response.hits.iter().map(|h| h.doc_id).collect();
    out.sort_unstable();
    out
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

#[test]
fn a_shard_map_with_a_tree_loads_and_validates() {
    let text = r#"
generation = 3

[[shards]]
addr = "127.0.0.1:19300"
slot_offset = 0
hash_lo = 0
hash_hi = 18446744073709551615
placement = 0

[[shards]]
addr = "127.0.0.1:19301"
slot_offset = 1000
hash_lo = 0
hash_hi = 18446744073709551615
placement = 18014398509481984

[placement]
column = "placement"

[[placement.nodes]]
name = "old"
cel = "year < 2000"
shards = 1

[[placement.nodes]]
name = "rest"
shards = 1
"#;
    let map: ShardMap = toml::from_str(text).unwrap();
    let tree = map.placement.clone().expect("the map has a tree");
    let placement = Placement::validate(&tree).unwrap();
    let names: Vec<&str> = placement.leaves().iter().map(|l| l.name.as_str()).collect();
    assert_eq!(names, ["old", "rest"]);
    assert_eq!(placement.leaves()[1].code, 1i64 << 54);
    let codes: Vec<Option<u64>> = map.shards.iter().map(|s| s.placement).collect();
    assert_eq!(codes, vec![Some(0), Some(1u64 << 54)]);

    let addrs = vec!["a:1".to_string(), "b:1".to_string()];
    let ranges = vec![Some((0, u64::MAX)); 2];
    let build = |codes: Vec<Option<i64>>, tree: Option<PlacementTreeConfig>| {
        CoordinatorServiceImpl::new(addrs.clone())
            .with_hot_topology_placed(ranges.clone(), tree.map(|t| (t, codes)))
            .err()
    };
    assert!(build(vec![Some(0), Some(1 << 54)], Some(tree.clone())).is_none());
    let err = build(vec![Some(0), None], Some(tree.clone())).unwrap();
    assert!(err.contains("shard 1 has no placement code"), "{err}");
    let err = build(vec![Some(0), Some(7)], Some(tree.clone())).unwrap();
    assert!(err.contains("code 7") && err.contains("no leaf"), "{err}");
    let err = build(vec![Some(0), Some(0)], Some(tree.clone())).unwrap();
    assert!(
        err.contains("\"rest\"") && err.contains("no shard"),
        "{err}"
    );
    // A code without a tree is refused on the reload path, where codes
    // arrive with the map's entries.
    let placed = CoordinatorServiceImpl::new(addrs.clone())
        .with_hot_topology_placed(
            ranges.clone(),
            Some((tree.clone(), vec![Some(0), Some(1 << 54)])),
        )
        .unwrap();
    let mut routes = placed.current_topology_routes();
    routes[0].hash_range = Some((0, 10));
    routes[1].hash_range = Some((11, u64::MAX));
    let err = placed.reload_topology(2, routes, None).unwrap_err();
    assert!(err.contains("no placement tree"), "{err}");
    // Under a tree the hash ranges tile the space per leaf.
    let err = CoordinatorServiceImpl::new(vec!["a:1".into(), "b:1".into(), "c:1".into()])
        .with_hot_topology_placed(
            vec![Some((0, u64::MAX)), Some((0, 10)), Some((11, u64::MAX / 2))],
            Some((tree, vec![Some(0), Some(1 << 54), Some(1 << 54)])),
        )
        .err()
        .unwrap();
    assert!(
        err.contains("of placement leaf \"rest\"") && err.contains("ends at"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn routed_ingest_places_each_document_in_its_leaf() {
    let nodes = leaf_nodes().await;
    let addrs: Vec<String> = nodes.iter().map(|n| n.0.clone()).collect();
    let c = coordinator(&addrs, true);
    let all: Vec<usize> = (0..corpus().len()).collect();
    let response = routed(&c, &all).await.unwrap();
    assert_eq!(response.added as usize, corpus().len());

    // Each leaf's node holds the documents the tree assigns to it, and
    // every row carries its leaf's code in the placement column.
    for (leaf, (name, code)) in LEAVES.iter().zip(codes()).enumerate() {
        let expected = corpus()
            .iter()
            .filter(|(_, _, leaf)| *leaf == *name)
            .count() as u64;
        assert_eq!(health_docs(&addrs[leaf]).await, expected, "leaf {name}");
        let under = leaf_ids(&c, code).await;
        assert_eq!(under.len() as u64, expected, "leaf {name} by column");
        assert!(
            under.iter().all(|id| (id / 1_000) as usize == leaf),
            "leaf {name}: ids {under:?} outside slot range"
        );
    }

    // A stable key routes inside the leaf, never by the plain rule.
    let err = c.route_stable_key(b"law.v1.Case/case-0").unwrap_err();
    assert!(err.contains("routes inside a leaf"), "{err}");
    let (generation, shard) = c
        .route_stable_key_in(b"law.v1.Case/case-0", Some(codes()[2]))
        .unwrap();
    assert_eq!((generation, shard), (1, 2));
    let err = c
        .route_stable_key_in(b"law.v1.Case/case-0", Some(12345))
        .unwrap_err();
    assert!(err.contains("inside placement leaf 12345"), "{err}");

    for (_, handle) in nodes {
        handle.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_direct_ingest_takes_the_pinned_leaf_or_is_refused_by_name() {
    let dir = tempdir("direct");
    let code = codes()[3];
    let (pinned, pinned_handle) = start_empty_node(NodeConfig {
        index_path: Some(dir.join("pinned.tv")),
        wal: true,
        ..config(Some(code), 0)
    })
    .await;
    let (unpinned, unpinned_handle) = start_empty_node(config(None, 1_000)).await;

    let doc = |placement: Option<i64>| AddDocumentsRequest {
        text: "a direct opinion about search".into(),
        analysis: Some(body_spec()),
        facets: vec![FacetValue {
            field: "court_code".into(),
            value: "ca9".into(),
        }],
        integers: placement
            .map(|value| {
                vec![IntegerValue {
                    field: COLUMN.into(),
                    value,
                }]
            })
            .unwrap_or_default(),
        ..Default::default()
    };
    let add = |addr: String, req: AddDocumentsRequest| async move {
        let mut client = NodeServiceClient::connect(addr).await.unwrap();
        let (tx, rx) = mpsc::channel(2);
        tx.send(req).await.unwrap();
        drop(tx);
        client
            .add_documents(ReceiverStream::new(rx))
            .await
            .map(|r| r.into_inner())
    };

    // Pinned: no value takes the code; the same code passes; another
    // code is refused naming both.
    add(pinned.clone(), doc(None)).await.unwrap();
    add(pinned.clone(), doc(Some(code))).await.unwrap();
    let err = add(pinned.clone(), doc(Some(code + 1))).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message()
            .contains(&format!("carries code {}", code + 1))
            && err.message().contains(&format!("placement leaf {code}")),
        "{}",
        err.message()
    );
    // Unpinned with the column declared: the value is required.
    let err = add(unpinned.clone(), doc(None)).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("--placement-leaf") && err.message().contains(COLUMN),
        "{}",
        err.message()
    );
    add(unpinned.clone(), doc(Some(5))).await.unwrap();

    // The column is queryable like any integer column.
    let c = CoordinatorServiceImpl::new(vec![pinned.clone()]).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    );
    assert_eq!(leaf_ids(&c, code).await, vec![0, 1]);

    // Replay yields the same code: flush, reopen from disk, ask again.
    NodeServiceClient::connect(pinned.clone())
        .await
        .unwrap()
        .flush(FlushRequest {})
        .await
        .unwrap();
    pinned_handle.abort();
    unpinned_handle.abort();
    let (reopened, reopened_handle) = start_opened_node(NodeConfig {
        index_path: Some(dir.join("pinned.tv")),
        wal: true,
        ..config(Some(code), 0)
    })
    .await;
    let c = CoordinatorServiceImpl::new(vec![reopened]).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    );
    assert_eq!(leaf_ids(&c, code).await, vec![0, 1]);
    reopened_handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Every pruning shape: the shards consulted and the shards the plan's
/// filter rules out, on the four leaves (old, recent.scotus,
/// recent.rest, other). The root default carries no bound and is never
/// skipped; a boolean root reports no plan-level skip.
fn cases() -> Vec<(&'static str, SelectionQuery, u32)> {
    vec![
        (
            "dense >= 2020",
            and(vec![cel("f", "year >= 2020"), dense("v")]),
            1,
        ),
        (
            "dense < 1990",
            and(vec![cel("f", "year < 1990"), dense("v")]),
            2,
        ),
        (
            "dense 2000..2020",
            and(vec![cel("f", "year >= 2000 && year < 2020"), dense("v")]),
            3,
        ),
        (
            "dense ca9 recent",
            and(vec![
                cel("f", "court_code == \"ca9\" && year >= 2020"),
                dense("v"),
            ]),
            2,
        ),
        (
            "lexical scotus",
            and(vec![
                cel("f", "court_code == \"scotus\""),
                lexical("l", "search"),
            ]),
            0,
        ),
        (
            "lexical >= 2020",
            and(vec![cel("f", "year >= 2020"), lexical("l", "search")]),
            1,
        ),
        ("browse >= 2020", cel("f", "year >= 2020"), 1),
        ("browse < 1990", cel("f", "year < 1990"), 2),
        (
            "browse by code",
            cel("f", &format!("{COLUMN} == {}", codes()[1])),
            3,
        ),
        // An OR is ruled out only where every branch is: recent.scotus
        // pins both the year and the court, so both branches fail
        // there and nowhere else.
        (
            "browse or",
            cel("f", "year < 1990 || court_code == \"ca9\""),
            1,
        ),
        ("browse not", cel("f", "!(year >= 2020)"), 0),
        ("browse has", cel("f", "has(year)"), 0),
        (
            "boolean",
            boolean(vec![cel("f", "year >= 2020"), lexical("l", "search")]),
            0,
        ),
        // Clauses a leaf implies are dropped from that shard's tree
        // (docs/placement.md, "Implied clauses"): the recent leaves
        // receive no `year` clause here, old is ruled out, other keeps it.
        (
            "dense >= 2015 implied",
            and(vec![cel("f", "year >= 2015"), dense("v")]),
            1,
        ),
        (
            "dense scotus implied",
            and(vec![cel("f", "court_code == \"scotus\""), dense("v")]),
            0,
        ),
        (
            "dense has year implied",
            and(vec![cel("f", "has(year)"), dense("v")]),
            0,
        ),
        (
            "dense implied under or",
            and(vec![
                cel("f", "year >= 2015 || court_code == \"ca9\""),
                dense("v"),
            ]),
            0,
        ),
        (
            "lexical whole tree implied",
            and(vec![
                cel("f", "year >= 2010 && has(court_code)"),
                lexical("l", "search"),
            ]),
            1,
        ),
        (
            "boolean implied",
            boolean(vec![cel("f", "year >= 2010"), lexical("l", "search")]),
            0,
        ),
    ]
}

/// The mask the pruning coordinator computes for the shapes above: which
/// clauses each consulted shard is spared. The wire cannot show a
/// dropped clause, so this reads the verdict the fan-out reads.
#[test]
fn the_recent_leaves_are_spared_the_clauses_they_imply() {
    use pipestream_search::cel::compile_filter;
    use pipestream_search::placement::ShardMask;
    let placement = Placement::validate(&tree()).unwrap();
    let codes: Vec<Option<i64>> = codes().into_iter().map(Some).collect();
    let filter = compile_filter("year >= 2010 && has(court_code)")
        .unwrap()
        .unwrap();
    let mask = ShardMask::compute(&placement, &codes, &filter);
    // LEAVES order: old, recent.scotus, recent.rest, other.
    assert_eq!(mask.skipped, vec![true, false, false, false]);
    assert_eq!(
        mask.filter_for(1, &filter),
        None,
        "scotus pins year and court"
    );
    let rest = mask.filter_for(2, &filter).unwrap();
    assert!(matches!(
        rest.expr,
        Some(pipestream_search::pb::filter_expr::Expr::Has(_))
    ));
    assert_eq!(mask.filter_for(3, &filter), Some(filter.clone()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn skipping_shards_changes_no_answer() {
    let nodes = leaf_nodes().await;
    let addrs: Vec<String> = nodes.iter().map(|n| n.0.clone()).collect();
    let pruning = coordinator(&addrs, true);
    let consulting = coordinator(&addrs, false);
    let all: Vec<usize> = (0..corpus().len()).collect();
    routed(&pruning, &all).await.unwrap();

    for (name, selection, skipped) in cases() {
        let a = query(&pruning, selection.clone()).await;
        let b = query(&consulting, selection).await;
        assert_eq!(ids(&a.hits), ids(&b.hits), "{name}: hits differ");
        assert_eq!(a.next_cursor, b.next_cursor, "{name}: cursor differs");
        assert_eq!(shard_counts(&a), (4, skipped), "{name}");
        assert_eq!(shard_counts(&b), (4, 0), "{name}: pruning off");
    }

    // The typo rule survives pruning: a column no shard has is refused
    // on both, and a real column only the skipped leaves carry is not.
    for c in [&pruning, &consulting] {
        let err = SearchService::query(
            c,
            Request::new(request(and(vec![cel("f", "yeer >= 2020"), dense("v")]))),
        )
        .await
        .unwrap_err();
        assert_eq!(
            err.code(),
            tonic::Code::InvalidArgument,
            "{}",
            err.message()
        );
    }

    // The knob flips live.
    pruning.knobs().set("shard_pruning", "false").unwrap();
    let off = query(&pruning, cel("f", "year >= 2020")).await;
    assert_eq!(shard_counts(&off), (4, 0));
    pruning.knobs().set("shard_pruning", "true").unwrap();
    let on = query(&pruning, cel("f", "year >= 2020")).await;
    assert_eq!(shard_counts(&on), (4, 1));
    assert_eq!(ids(&on.hits), ids(&off.hits));

    for (_, handle) in nodes {
        handle.abort();
    }
}

/// The document-level evaluator the coordinator routes with and the
/// shard's evaluation over stored rows agree predicate by predicate:
/// the set a browse returns is the set of documents whose own request
/// values evaluate TRUE.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_evaluation_agrees_with_the_shards() {
    use pipestream_search::placement::{eval_document, DocColumns};

    let nodes = leaf_nodes().await;
    let addrs: Vec<String> = nodes.iter().map(|n| n.0.clone()).collect();
    let c = coordinator(&addrs, true);
    let all: Vec<usize> = (0..corpus().len()).collect();
    routed(&c, &all).await.unwrap();

    // The global id of document i: its leaf's slot base plus its rank
    // among the corpus documents routed to that leaf, in stream order.
    let placement = Placement::validate(&tree()).unwrap();
    let mut seen = vec![0u64; LEAVES.len()];
    let mut rows: Vec<(u64, AddDocumentsRequest)> = Vec::new();
    for (i, (year, court, leaf)) in corpus().into_iter().enumerate() {
        let code = placement.leaf_by_name(leaf).unwrap().code;
        let slot = LEAVES.iter().position(|l| *l == leaf).unwrap();
        let id = 1_000 * slot as u64 + seen[slot];
        seen[slot] += 1;
        let mut facets = vec![FacetValue {
            field: "id".into(),
            value: format!("case-{i}"),
        }];
        if let Some(court) = court {
            facets.push(FacetValue {
                field: "court_code".into(),
                value: court.into(),
            });
        }
        let mut integers = vec![IntegerValue {
            field: COLUMN.into(),
            value: code,
        }];
        if let Some(year) = year {
            integers.push(IntegerValue {
                field: "year".into(),
                value: year,
            });
        }
        rows.push((
            id,
            AddDocumentsRequest {
                text: format!("opinion {i} about search"),
                facets,
                integers,
                ..Default::default()
            },
        ));
    }

    let predicates = [
        "year >= 2020",
        "year < 2000",
        "year >= 2000 && year < 2020",
        "year == 2021",
        "year > 2020.5",
        "!(year >= 2020)",
        "has(year)",
        "!has(year)",
        "court_code == \"scotus\"",
        "court_code in [\"ca9\", \"scotus\"]",
        "court_code != \"scotus\"",
        "court_code.startsWith(\"sc\")",
        "court_code < \"d\"",
        "year < 1990 || court_code == \"ca9\"",
        "id == \"case-3\"",
        &format!("{COLUMN} >= {}", codes()[1]),
        &format!("{COLUMN} == {}", codes()[3]),
    ];
    for predicate in predicates {
        let expr = pipestream_search::cel::compile_filter(predicate)
            .unwrap()
            .unwrap();
        let mut expected: Vec<u64> = rows
            .iter()
            .filter(|(_, doc)| {
                eval_document(&expr, &DocColumns::of(doc).unwrap())
                    == pipestream_search::filter::Tri::True
            })
            .map(|(id, _)| *id)
            .collect();
        expected.sort_unstable();
        let response = query(&c, cel("f", predicate)).await;
        let mut got: Vec<u64> = response.hits.iter().map(|h| h.doc_id).collect();
        got.sort_unstable();
        assert_eq!(got, expected, "{predicate}");
    }

    for (_, handle) in nodes {
        handle.abort();
    }
}

// ---------------------------------------------------------------------
// The tree on the node: --placement-tree
// ---------------------------------------------------------------------

fn pinned(code: i64) -> std::sync::Arc<pipestream_search::placement::PinnedLeaf> {
    std::sync::Arc::new(
        pipestream_search::placement::PinnedLeaf::pin(&tree(), COLUMN, code).unwrap(),
    )
}

#[test]
fn a_pinned_leaf_is_validated_by_name() {
    use pipestream_search::placement::PinnedLeaf;
    let old = codes()[0];
    let leaf = PinnedLeaf::pin(&tree(), COLUMN, old).unwrap();
    assert_eq!(leaf.leaf().name, "old");
    assert_eq!(leaf.placement().leaves().len(), 4);
    let err = PinnedLeaf::pin(&tree(), "elsewhere", old).unwrap_err();
    assert!(
        err.contains("column \"placement\"") && err.contains("--placement-column \"elsewhere\""),
        "{err}"
    );
    let err = PinnedLeaf::pin(&tree(), COLUMN, old + 7).unwrap_err();
    assert!(
        err.contains(&format!("--placement-leaf {} is not a leaf", old + 7))
            && err.contains("recent.scotus =")
            && err.contains("other ="),
        "{err}"
    );
    // A tree that fails validation refuses before the code is looked at.
    let mut broken = tree();
    broken.nodes[0].cel = Some("year <".into());
    let err = PinnedLeaf::pin(&broken, COLUMN, old).unwrap_err();
    assert!(err.contains("node \"old\""), "{err}");
}

#[test]
fn a_placement_tree_file_is_the_map_or_the_table_alone() {
    use pipestream_search::config::load_placement_tree;
    let dir = tempdir("tree-file");
    let map = dir.join("map.toml");
    std::fs::write(
        &map,
        r#"
generation = 3

[[shards]]
addr = "127.0.0.1:19300"
slot_offset = 0
placement = 0

[placement]
column = "placement"

[[placement.nodes]]
name = "old"
cel = "year < 2000"
shards = 1

[[placement.nodes]]
name = "rest"
shards = 1
"#,
    )
    .unwrap();
    let from_map = load_placement_tree(&map).unwrap();
    assert_eq!(from_map.nodes.len(), 2);
    let table = dir.join("tree.toml");
    std::fs::write(
        &table,
        r#"
column = "placement"

[[nodes]]
name = "old"
cel = "year < 2000"
shards = 1

[[nodes]]
name = "rest"
shards = 1
"#,
    )
    .unwrap();
    assert_eq!(load_placement_tree(&table).unwrap(), from_map);
    let bare = dir.join("bare.toml");
    std::fs::write(&bare, "generation = 3\n\n[[shards]]\naddr = \"a:1\"\n").unwrap();
    let err = load_placement_tree(&bare).unwrap_err();
    assert!(err.contains("no [placement] table"), "{err}");
    let junk = dir.join("junk.toml");
    std::fs::write(&junk, "nodes = 4\n").unwrap();
    let err = load_placement_tree(&junk).unwrap_err();
    assert!(err.contains("neither a shard map"), "{err}");
    let err = load_placement_tree(&dir.join("absent.toml")).unwrap_err();
    assert!(err.contains("read placement tree"), "{err}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pinned_shard_with_the_tree_refuses_a_row_the_tree_routes_elsewhere() {
    let dir = tempdir("tree-direct");
    let old = codes()[0];
    let rest = codes()[2];
    let (old_node, old_handle) = start_empty_node(NodeConfig {
        index_path: Some(dir.join("old.tv")),
        wal: true,
        placement_tree: Some(pinned(old)),
        ..config(Some(old), 0)
    })
    .await;
    let (rest_node, rest_handle) = start_empty_node(NodeConfig {
        placement_tree: Some(pinned(rest)),
        ..config(Some(rest), 1_000)
    })
    .await;

    let doc = |year: Option<i64>, court: &str, code: Option<i64>| {
        let mut integers = Vec::new();
        if let Some(year) = year {
            integers.push(IntegerValue {
                field: "year".into(),
                value: year,
            });
        }
        if let Some(code) = code {
            integers.push(IntegerValue {
                field: COLUMN.into(),
                value: code,
            });
        }
        AddDocumentsRequest {
            text: "a direct opinion about search".into(),
            analysis: Some(body_spec()),
            facets: vec![FacetValue {
                field: "court_code".into(),
                value: court.into(),
            }],
            integers,
            ..Default::default()
        }
    };
    let add = |addr: String, req: AddDocumentsRequest| async move {
        let mut client = NodeServiceClient::connect(addr).await.unwrap();
        let (tx, rx) = mpsc::channel(2);
        tx.send(req).await.unwrap();
        drop(tx);
        client
            .add_documents(ReceiverStream::new(rx))
            .await
            .map(|r| r.into_inner())
    };
    let refused = |err: tonic::Status, needles: &[&str]| {
        assert_eq!(
            err.code(),
            tonic::Code::InvalidArgument,
            "{}",
            err.message()
        );
        for needle in needles {
            assert!(
                err.message().contains(needle),
                "missing {needle:?} in {}",
                err.message()
            );
        }
    };

    // The leaf "old" (year < 2000): a row the predicate holds on passes
    // with or without the code; a row it is false on is refused naming
    // the node, the outcome, and the leaf the row belongs to, whether or
    // not it carries the pinned code; a row with no year is unknown and
    // belongs to the root default.
    add(old_node.clone(), doc(Some(1990), "ca9", None))
        .await
        .unwrap();
    add(old_node.clone(), doc(Some(1985), "ca9", Some(old)))
        .await
        .unwrap();
    let err = add(old_node.clone(), doc(Some(2021), "ca9", None))
        .await
        .unwrap_err();
    refused(
        err,
        &[
            "node \"old\" (year < 2000) is false",
            "leaf \"recent.rest\"",
            &format!("pinned leaf \"old\" (code {old})"),
            "--placement-tree",
        ],
    );
    let err = add(old_node.clone(), doc(Some(2021), "ca9", Some(old)))
        .await
        .unwrap_err();
    refused(err, &["node \"old\" (year < 2000) is false"]);
    let err = add(old_node.clone(), doc(None, "ca9", None))
        .await
        .unwrap_err();
    refused(
        err,
        &["node \"old\" (year < 2000) is unknown", "leaf \"other\""],
    );
    // A code from another leaf is still refused by the code rule.
    let err = add(old_node.clone(), doc(Some(1990), "ca9", Some(rest)))
        .await
        .unwrap_err();
    refused(err, &[&format!("carries code {rest}")]);
    assert_eq!(health_docs(&old_node).await, 2);

    // The leaf "recent.rest": an earlier node at either level that is
    // true on the row names itself.
    add(rest_node.clone(), doc(Some(2021), "ca9", None))
        .await
        .unwrap();
    let err = add(rest_node.clone(), doc(Some(2021), "scotus", None))
        .await
        .unwrap_err();
    refused(
        err,
        &[
            "node \"recent.scotus\" (court_code == \"scotus\") is true",
            "leaf \"recent.scotus\"",
        ],
    );
    let err = add(rest_node.clone(), doc(Some(1990), "ca9", None))
        .await
        .unwrap_err();
    refused(err, &["node \"old\" (year < 2000) is true", "leaf \"old\""]);
    assert_eq!(health_docs(&rest_node).await, 1);

    // Replay is not re-checked: flush, reopen with the tree, same rows.
    NodeServiceClient::connect(old_node.clone())
        .await
        .unwrap()
        .flush(FlushRequest {})
        .await
        .unwrap();
    old_handle.abort();
    rest_handle.abort();
    let (reopened, reopened_handle) = start_opened_node(NodeConfig {
        index_path: Some(dir.join("old.tv")),
        wal: true,
        placement_tree: Some(pinned(old)),
        ..config(Some(old), 0)
    })
    .await;
    assert_eq!(health_docs(&reopened).await, 2);
    let c = CoordinatorServiceImpl::new(vec![reopened.clone()]).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    );
    assert_eq!(leaf_ids(&c, old).await, vec![0, 1]);
    reopened_handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn routed_ingest_passes_the_tree_check_on_every_leaf() {
    // One node per leaf, each pinned with the tree; the coordinator's
    // routing and the node's check agree on every document, so the
    // counts are the ones routing alone produced.
    let mut nodes = Vec::new();
    for (i, code) in codes().into_iter().enumerate() {
        let served = start_empty_node(NodeConfig {
            placement_tree: Some(pinned(code)),
            ..config(Some(code), 1_000 * i as u64)
        })
        .await;
        calibrate(&served.0).await;
        nodes.push(served);
    }
    let addrs: Vec<String> = nodes.iter().map(|n| n.0.clone()).collect();
    let c = coordinator(&addrs, true);
    let all: Vec<usize> = (0..corpus().len()).collect();
    let response = routed(&c, &all).await.unwrap();
    assert_eq!(response.added as usize, corpus().len());
    for (leaf, name) in LEAVES.iter().enumerate() {
        let expected = corpus().iter().filter(|(_, _, l)| *l == *name).count() as u64;
        assert_eq!(health_docs(&addrs[leaf]).await, expected, "leaf {name}");
    }
    for (_, handle) in nodes {
        handle.abort();
    }
}
