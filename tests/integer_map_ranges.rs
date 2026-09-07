mod common;
use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    coordinator::CoordinatorServiceImpl,
    node::{Bm25Shard, NodeConfig, NodeServiceImpl},
    pb::{self, node_service_server::NodeService, search_service_server::SearchService},
    postings::{AnalyzedDoc, Bm25Reader, Bm25Store},
    relay::merge_bm25_responses,
};
use prost::Message;
use tonic::Request;
const I: [Option<i64>; 10] = [
    None,
    Some(i64::MIN),
    Some(i64::MIN + 1),
    Some(-1),
    Some(0),
    Some(1),
    Some(1 << 53),
    Some((1 << 53) + 1),
    Some(i64::MAX),
    Some(0),
];
const U: [Option<u64>; 10] = [
    None,
    Some(u64::MAX),
    Some(u64::MAX - 1),
    Some(1 << 63),
    Some(0),
    Some(1),
    Some(1 << 53),
    Some((1 << 53) + 1),
    Some(i64::MAX as u64),
    Some(0),
];
const KEY: &str = "zz'\\\"雪";
#[derive(Clone, PartialEq, Message)]
struct WireRange {
    #[prost(string, tag = "1")]
    column: String,
    #[prost(message, optional, tag = "6")]
    typed_map: Option<pb::MapRangeFacet>,
}
fn edges() -> Vec<pb::FilterBound> {
    use pb::filter_bound::Value::*;
    [
        Num(-9_223_372_036_854_775_808.0),
        Int(i64::MIN + 1),
        Num(-1.5),
        Int(-1),
        Uint(0),
        Num(0.5),
        Int(1),
        Uint(1 << 53),
        Uint((1 << 53) + 1),
        Num(9_223_372_036_854_775_808.0),
        Uint(u64::MAX),
        Num(18_446_744_073_709_551_616.0),
    ]
    .into_iter()
    .map(|value| pb::FilterBound {
        value: Some(value),
        exclusive: false,
    })
    .collect()
}
fn range(column: &str, key: &str) -> pb::RangeFacetField {
    pb::RangeFacetField::decode(
        WireRange {
            column: column.into(),
            typed_map: Some(pb::MapRangeFacet {
                key: key.into(),
                typed_edges: edges(),
                ..Default::default()
            }),
        }
        .encode_to_vec()
        .as_slice(),
    )
    .unwrap()
}
fn expected(column: &str, rows: std::ops::Range<usize>) -> Vec<u64> {
    // All fixture edges are integers or half-integers. Doubling them permits
    // an independent i128 oracle; document integers never convert to double.
    let bounds: Vec<i128> = edges()
        .into_iter()
        .map(|e| match e.value.unwrap() {
            pb::filter_bound::Value::Int(x) => i128::from(x) * 2,
            pb::filter_bound::Value::Uint(x) => i128::from(x) * 2,
            pb::filter_bound::Value::Num(x) => (x * 2.0) as i128,
        })
        .collect();
    bounds
        .windows(2)
        .map(|b| {
            rows.clone()
                .filter(|row| {
                    let value = if column == "signed" {
                        I[*row].map(i128::from)
                    } else {
                        U[*row].map(i128::from)
                    };
                    value.is_some_and(|v| v * 2 >= b[0] && v * 2 < b[1])
                })
                .count() as u64
        })
        .collect()
}
fn check(counts: &pb::RangeFacetCounts, column: &str, key: &str, rows: std::ops::Range<usize>) {
    assert!(counts.known);
    assert_eq!(counts.column, column);
    assert_eq!(counts.map_key.as_deref(), Some(key));
    assert_eq!(
        counts.buckets.iter().map(|b| b.count).collect::<Vec<_>>(),
        expected(column, rows)
    );
    let edges = edges();
    for (i, b) in counts.buckets.iter().enumerate() {
        assert_eq!(b.typed_from.as_ref(), Some(&edges[i]));
        assert_eq!(b.typed_to.as_ref(), Some(&edges[i + 1]));
    }
}
fn raw_request(column: &str, key: &str) -> pb::Bm25QueryRequest {
    pb::Bm25QueryRequest {
        terms: vec!["word".into()],
        global_doc_count: 10,
        global_doc_frequencies: vec![10],
        global_total_doc_length: 10,
        k: 0,
        range_facet_fields: vec![range(column, key)],
        ..Default::default()
    }
}
#[tokio::test]
async fn integer_map_ranges_keep_adjacent_large_values_in_distinct_buckets() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("map-ranges-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut store = Bm25Store::new()
        .with_map_integers(&["signed"])
        .with_map_unsigned_integers(&["unsigned"]);
    for row in 0..10 {
        store.add_document(
            row as u32,
            "word".into(),
            AnalyzedDoc::body(vec![("word".into(), 1, vec![(0, 4)])], 1),
        );
        for key in ["", KEY] {
            if let Some(v) = I[row] {
                store.set_map_integer(0, row as u32, key, v).unwrap();
            }
            if let Some(v) = U[row] {
                store
                    .set_map_unsigned_integer(0, row as u32, key, v)
                    .unwrap();
            }
        }
    }
    let path = dir.join("index.bm25");
    store.save(&path).unwrap();
    for shard in [
        Bm25Shard::Building(store),
        Bm25Shard::Resident(Bm25Reader::open(&path).unwrap()),
    ] {
        let node = NodeServiceImpl::new(
            None,
            NodeConfig {
                map_integer_fields: vec!["signed".into()],
                map_unsigned_integer_fields: vec!["unsigned".into()],
                ..Default::default()
            },
        )
        .with_bm25(Some(shard));
        for column in ["signed", "unsigned"] {
            for key in ["", KEY] {
                let req = raw_request(column, key);
                let response = node
                    .bm25_query(Request::new(req.clone()))
                    .await
                    .unwrap()
                    .into_inner();
                check(&response.range_facets[0], column, key, 0..10);
                let merged = merge_bm25_responses(&req, vec![response.clone()]).unwrap();
                check(&merged.range_facets[0], column, key, 0..10);
                let mut legacy = req.clone();
                legacy.range_facet_fields[0].map = legacy.range_facet_fields[0].typed_map.take();
                let legacy = node
                    .bm25_query(Request::new(legacy))
                    .await
                    .unwrap()
                    .into_inner();
                assert!(
                    !legacy.range_facets[0].known,
                    "legacy map ranges remain f64-only"
                );
            }
        }
    }
    std::fs::remove_dir_all(dir).unwrap();
}

async fn verify(addresses: Vec<String>, fused_supported: bool) {
    for stream in [false, true] {
        let coordinator = CoordinatorServiceImpl::new(addresses.clone())
            .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default())
            .with_bm25_stream(stream);
        for column in ["signed", "unsigned"] {
            for key in ["", KEY] {
                for filtered in [false, true] {
                    for (k, fused, text) in [
                        (1, false, "word"),
                        (1, true, "word"),
                        (0, false, "word"),
                        (0, true, "word"),
                        (0, false, " "),
                        (0, true, " "),
                    ] {
                        let response = coordinator
                            .bm25_search(Request::new(pb::Bm25SearchRequest {
                                text: text.into(),
                                k,
                                analysis: if fused { None } else { Some(body_spec()) },
                                fields: if fused {
                                    vec![pb::QueryField {
                                        field: "body".into(),
                                        analysis: Some(body_spec()),
                                        ..Default::default()
                                    }]
                                } else {
                                    vec![]
                                },
                                filter: if filtered {
                                    "row < 5".into()
                                } else {
                                    String::new()
                                },
                                range_facet_fields: vec![range(column, key)],
                                ..Default::default()
                            }))
                            .await;
                        if fused && !fused_supported {
                            let error = response.unwrap_err();
                            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
                            assert!(error.message().contains("homogeneous field capabilities"));
                            continue;
                        }
                        let response = response.unwrap().into_inner();
                        assert_eq!(
                            response.range_facets.len(),
                            1,
                            "requested ranges disappeared: k={k}, fused={fused}, text={text:?}"
                        );
                        if text.trim().is_empty() {
                            assert!(response.hits.is_empty());
                        } else if k == 0 {
                            assert_eq!(
                                response.hits.len(),
                                if filtered { 5 } else { 10 },
                                "public k=0 retains the configured maximum"
                            );
                        }
                        check(
                            &response.range_facets[0],
                            column,
                            key,
                            if text.trim().is_empty() {
                                0..0
                            } else if filtered {
                                0..5
                            } else {
                                0..10
                            },
                        );
                    }
                }
            }
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integer_map_ranges_compose_through_relays_and_compaction() {
    use pipestream_search::{
        harness::start_relay, node::Layout, pb::node_service_client::NodeServiceClient,
    };
    let (shift, scale) = common::fit_calibration(8, 4, &common::unit_vectors(64, 8, 92372));
    for layout in [Layout::SingleImage, Layout::Segments] {
        let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("map-range-relay-{layout:?}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut configs = vec![];
        let mut addresses = vec![];
        let mut handles = vec![];
        for rows in [0..5, 5..10, 10..10] {
            let config = NodeConfig {
                index_path: Some(dir.join(format!("{}.tv", rows.start))),
                layout,
                wal: true,
                wal_buckets: 2,
                slot_offset: rows.start as u64,
                seal_tail_docs: 2,
                integer_fields: vec!["row".into()],
                map_integer_fields: vec!["signed".into()],
                map_unsigned_integer_fields: vec!["unsigned".into()],
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
                ..Default::default()
            };
            let (addr, handle) = common::start_empty_node(config.clone()).await;
            let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
            client
                .set_calibration(pb::SetCalibrationRequest {
                    dim: 8,
                    bit_width: 4,
                    shift: shift.clone(),
                    scale: scale.clone(),
                })
                .await
                .unwrap();
            for row in rows.clone() {
                let keys = if row % 5 < 2 {
                    vec!["", KEY]
                } else {
                    vec!["", "a", KEY]
                };
                client
                    .add_documents(tokio_stream::iter([pb::AddDocumentsRequest {
                        text: "word".into(),
                        analysis: Some(body_spec()),
                        integers: vec![pb::IntegerValue {
                            field: "row".into(),
                            value: row as i64,
                        }],
                        map_integers: keys
                            .iter()
                            .flat_map(|key| {
                                I[row].map(|value| pb::MapIntegerEntry {
                                    field: "signed".into(),
                                    key: (*key).into(),
                                    value,
                                })
                            })
                            .collect(),
                        map_unsigned_integers: keys
                            .iter()
                            .flat_map(|key| {
                                U[row].map(|value| pb::MapUnsignedIntegerEntry {
                                    field: "unsigned".into(),
                                    key: (*key).into(),
                                    value,
                                })
                            })
                            .collect(),
                        ..Default::default()
                    }]))
                    .await
                    .unwrap();
                client
                    .add_vectors(tokio_stream::iter([pb::AddVectorsRequest {
                        dim: 8,
                        vectors: common::unit_vectors(1, 8, 712 + row as u64),
                    }]))
                    .await
                    .unwrap();
                if row % 5 == 1 {
                    client.flush(pb::FlushRequest {}).await.unwrap();
                }
            }
            configs.push(config);
            addresses.push(addr);
            handles.push(handle);
        }
        for phase in 0..3 {
            if phase > 0 {
                addresses.clear();
                for (part, config) in configs.iter().enumerate() {
                    let (addr, handle) = common::start_opened_node(config.clone()).await;
                    if phase == 1 && part < 2 {
                        NodeServiceClient::connect(addr.clone())
                            .await
                            .unwrap()
                            .compact_shard(pb::CompactShardRequest {
                                work_dir: dir.join(format!("compact-{part}")).display().to_string(),
                                ..Default::default()
                            })
                            .await
                            .unwrap();
                    }
                    addresses.push(addr);
                    handles.push(handle);
                }
            }
            verify(addresses.clone(), true).await;
            let (left, _, lh) = start_relay(addresses[..1].to_vec()).await;
            let (right, _, rh) = start_relay(addresses[1..].to_vec()).await;
            let (root, _, root_h) = start_relay(vec![left, right]).await;
            verify(vec![root], false).await;
            root_h.abort();
            lh.abort();
            rh.abort();
            if phase == 0 {
                for addr in &addresses {
                    NodeServiceClient::connect(addr.clone())
                        .await
                        .unwrap()
                        .flush(pb::FlushRequest {})
                        .await
                        .unwrap();
                }
            }
            for h in handles.drain(..) {
                h.abort();
                let _ = h.await;
            }
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[derive(Clone, PartialEq, Message)]
struct OldRange {
    #[prost(string, tag = "1")]
    column: String,
    #[prost(string, tag = "2")]
    key: String,
    #[prost(double, repeated, tag = "3")]
    edges: Vec<f64>,
    #[prost(message, repeated, tag = "4")]
    typed_edges: Vec<pb::FilterBound>,
    #[prost(message, optional, tag = "5")]
    map: Option<pb::MapRangeFacet>,
}
#[tokio::test]
async fn typed_map_ranges_refuse_unsupported_or_ambiguous_requests_before_empty_reads() {
    let node = NodeServiceImpl::new(None, NodeConfig::default());
    let request = range("signed", "");
    let old = OldRange::decode(request.encode_to_vec().as_slice()).unwrap();
    assert!(old.map.is_none() && old.edges.is_empty() && old.typed_edges.is_empty());
    let lost = pb::RangeFacetField::decode(old.encode_to_vec().as_slice()).unwrap();
    let mut both = request.clone();
    both.map = both.typed_map.clone();
    let mut legacy = request.clone();
    legacy.edges = vec![0., 1.];
    let mut wrong_key = request.clone();
    wrong_key.key = "other".into();
    let mut empty = request.clone();
    empty.typed_map.as_mut().unwrap().typed_edges.clear();
    let mut reversed = request.clone();
    reversed.typed_map.as_mut().unwrap().typed_edges.reverse();
    for field in [lost, both, legacy, wrong_key, empty, reversed] {
        assert_eq!(
            node.bm25_query(Request::new(pb::Bm25QueryRequest {
                range_facet_fields: vec![field],
                ..Default::default()
            }))
            .await
            .unwrap_err()
            .code(),
            tonic::Code::InvalidArgument
        );
    }
    let raw = raw_request("signed", "");
    let response = node
        .bm25_query(Request::new(raw.clone()))
        .await
        .unwrap()
        .into_inner();
    assert!(!response.range_facets[0].known);
    assert_eq!(response.range_facets[0].map_key.as_deref(), Some(""));
    assert!(merge_bm25_responses(&raw, vec![response.clone()]).is_ok());
    let mut missing = response;
    missing.range_facets[0].map_key = None;
    assert_eq!(
        merge_bm25_responses(&raw, vec![missing])
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
}
