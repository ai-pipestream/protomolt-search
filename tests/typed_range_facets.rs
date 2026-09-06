mod common;
use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    coordinator::CoordinatorServiceImpl,
    harness::start_relay,
    node::{Layout, NodeConfig},
    pb::{
        self, filter_bound::Value as V, node_service_client::NodeServiceClient,
        search_service_server::SearchService,
    },
    relay::merge_bm25_responses,
};
use tonic::{Code, Request};

// Values and edges use doubled i128 units as an independent exact oracle.
const ROWS: [[Option<i128>; 6]; 4] = [
    [
        Some((i64::MIN as i128) * 2),
        Some(-2),
        Some(0),
        Some(((1i128 << 53) + 1) * 2),
        Some((i64::MAX as i128) * 2),
        None,
    ],
    [
        Some(0),
        Some(((1i128 << 53) + 1) * 2),
        Some((1i128 << 63) * 2),
        Some(((u64::MAX as i128) - 1) * 2),
        Some((u64::MAX as i128) * 2),
        None,
    ],
    [
        Some(-1),
        Some(0),
        Some(1),
        Some((1i128 << 53) * 2),
        Some((1i128 << 63) * 2),
        Some((1i128 << 64) * 2),
    ],
    [None; 6],
];
fn fields() -> (Vec<pb::RangeFacetField>, Vec<Vec<i128>>) {
    let typed = vec![
        V::Int(i64::MIN),
        V::Int(-1),
        V::Uint(0),
        V::Num(0.5),
        V::Uint(1),
        V::Uint(1 << 53),
        V::Uint((1 << 53) + 1),
        V::Uint((1 << 53) + 2),
        V::Int(i64::MAX),
        V::Uint(1 << 63),
        V::Uint(u64::MAX - 1),
        V::Uint(u64::MAX),
        V::Num(18_446_744_073_709_551_616.0),
    ];
    let scaled = vec![
        (i64::MIN as i128) * 2,
        -2,
        0,
        1,
        2,
        (1i128 << 53) * 2,
        ((1i128 << 53) + 1) * 2,
        ((1i128 << 53) + 2) * 2,
        (i64::MAX as i128) * 2,
        (1i128 << 63) * 2,
        ((u64::MAX as i128) - 1) * 2,
        (u64::MAX as i128) * 2,
        (1i128 << 64) * 2,
    ];
    (
        vec![
            pb::RangeFacetField {
                column: "value".into(),
                typed_edges: typed
                    .into_iter()
                    .map(|value| pb::FilterBound {
                        value: Some(value),
                        exclusive: false,
                    })
                    .collect(),
                ..Default::default()
            },
            pb::RangeFacetField {
                column: "value".into(),
                edges: vec![
                    -9_223_372_036_854_775_808.0,
                    0.0,
                    9_007_199_254_740_992.0,
                    9_223_372_036_854_775_808.0,
                    18_446_744_073_709_551_616.0,
                ],
                ..Default::default()
            },
        ],
        vec![
            scaled,
            vec![
                (i64::MIN as i128) * 2,
                0,
                (1i128 << 53) * 2,
                (1i128 << 63) * 2,
                (1i128 << 64) * 2,
            ],
        ],
    )
}
async fn verify(addresses: Vec<String>) {
    let (a, _, ah) = start_relay(addresses[..2].to_vec()).await;
    let (b, _, bh) = start_relay(addresses[2..].to_vec()).await;
    let (root, _, rh) = start_relay(vec![a.clone(), b.clone()]).await;
    let (fields, edges) = fields();
    for (topology, roots) in [addresses, vec![a, b], vec![root]].into_iter().enumerate() {
        for stream in [false, true] {
            let coord = CoordinatorServiceImpl::new(roots.clone())
                .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default())
                .with_bm25_stream(stream);
            for (text, filter) in [("word", ""), ("word", "keep == 'yes'"), ("missing", "")] {
                let result = coord
                    .bm25_search(Request::new(pb::Bm25SearchRequest {
                        text: text.into(),
                        k: 1,
                        analysis: Some(body_spec()),
                        filter: filter.into(),
                        range_facet_fields: fields.clone(),
                        ..Default::default()
                    }))
                    .await
                    .unwrap()
                    .into_inner();
                if topology == 0 && !stream {
                    let filter_expr = if filter.is_empty() {
                        None
                    } else {
                        pipestream_search::cel::compile_filter(filter).unwrap()
                    };
                    let (_, _, fused) = coord
                        .fanout_bm25_fused_faceted(
                            text,
                            1,
                            &[pb::QueryField {
                                field: "body".into(),
                                analysis: Some(body_spec()),
                                weight: 1.0,
                                ..Default::default()
                            }],
                            0.0,
                            &[],
                            &[],
                            &fields,
                            &[],
                            filter_expr.as_ref(),
                        )
                        .await
                        .unwrap();
                    assert_eq!(fused, result.range_facets);
                }
                assert!(result.hits.len() <= 1);
                assert_eq!(result.range_facets.len(), fields.len());
                for (index, counts) in result.range_facets.iter().enumerate() {
                    let expected: Vec<u64> = edges[index]
                        .windows(2)
                        .map(|w| {
                            ROWS.iter()
                                .flatten()
                                .enumerate()
                                .filter(|(row, value)| {
                                    text == "word"
                                        && (filter.is_empty() || row % 2 == 0)
                                        && value.is_some_and(|v| w[0] <= v && v < w[1])
                                })
                                .count() as u64
                        })
                        .collect();
                    assert!(counts.known);
                    assert_eq!(
                        counts.buckets.iter().map(|b| b.count).collect::<Vec<_>>(),
                        expected,
                        "stream={stream} text={text} filter={filter} field={index}"
                    );
                    for (i, bucket) in counts.buckets.iter().enumerate() {
                        assert_eq!(bucket.typed_from, fields[index].typed_edges.get(i).cloned());
                        assert_eq!(
                            bucket.typed_to,
                            fields[index].typed_edges.get(i + 1).cloned()
                        );
                    }
                }
            }
            let mut unknown = fields[0].clone();
            unknown.column = "typo".into();
            let error = coord
                .bm25_search(Request::new(pb::Bm25SearchRequest {
                    text: "word".into(),
                    k: 1,
                    analysis: Some(body_spec()),
                    range_facet_fields: vec![unknown],
                    ..Default::default()
                }))
                .await
                .unwrap_err();
            assert_eq!(error.code(), Code::InvalidArgument);
        }
    }
    for h in [ah, bh, rh] {
        h.abort();
        let _ = h.await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_ranges_match_numeric_oracle_through_nested_relays_and_storage_lifecycle() {
    let (shift, scale) = common::fit_calibration(8, 4, &common::unit_vectors(64, 8, 53881));
    for layout in [Layout::SingleImage, Layout::Segments] {
        let temp = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("typed_ranges_{layout:?}_{}", std::process::id()));
        std::fs::create_dir_all(&temp).unwrap();
        let mut configs = Vec::new();
        let mut servers = Vec::new();
        let mut addresses = Vec::new();
        for (shard, rows) in ROWS.iter().enumerate() {
            let config = NodeConfig {
                index_path: Some(temp.join(format!("{shard}.tv"))),
                layout,
                wal: true,
                wal_buckets: 2,
                seal_tail_docs: 2,
                slot_offset: shard as u64 * 6,
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
                facet_fields: vec!["keep".into()],
                integer_fields: if shard == 0 {
                    vec!["value".into()]
                } else {
                    vec![]
                },
                unsigned_integer_fields: if shard == 1 {
                    vec!["value".into()]
                } else {
                    vec![]
                },
                numeric_fields: if shard == 2 {
                    vec!["value".into()]
                } else {
                    vec![]
                },
                ..Default::default()
            };
            let (addr, server) = common::start_empty_node(config.clone()).await;
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
            // Direct empty shards must reject malformed edges before reading a store.
            let mut bad = fields().0[0].clone();
            bad.typed_edges[0].exclusive = true;
            assert_eq!(
                client
                    .bm25_query(pb::Bm25QueryRequest {
                        range_facet_fields: vec![bad],
                        k: 0,
                        ..Default::default()
                    })
                    .await
                    .unwrap_err()
                    .code(),
                Code::InvalidArgument
            );
            for (row, value) in rows.iter().enumerate() {
                client
                    .add_documents(tokio_stream::iter([pb::AddDocumentsRequest {
                        text: "word".into(),
                        analysis: Some(body_spec()),
                        facets: vec![pb::FacetValue {
                            field: "keep".into(),
                            value: if row % 2 == 0 { "yes" } else { "no" }.into(),
                        }],
                        integers: value
                            .filter(|_| shard == 0)
                            .map(|v| pb::IntegerValue {
                                field: "value".into(),
                                value: (v / 2) as i64,
                            })
                            .into_iter()
                            .collect(),
                        unsigned_integers: value
                            .filter(|_| shard == 1)
                            .map(|v| pb::UnsignedIntegerValue {
                                field: "value".into(),
                                value: (v / 2) as u64,
                            })
                            .into_iter()
                            .collect(),
                        numerics: value
                            .filter(|_| shard == 2)
                            .map(|v| pb::NumericValue {
                                field: "value".into(),
                                value: v as f64 / 2.0,
                            })
                            .into_iter()
                            .collect(),
                        ..Default::default()
                    }]))
                    .await
                    .unwrap();
                client
                    .add_vectors(tokio_stream::iter([pb::AddVectorsRequest {
                        dim: 8,
                        vectors: common::unit_vectors(1, 8, 615 + row as u64),
                    }]))
                    .await
                    .unwrap();
            }
            client.flush(pb::FlushRequest {}).await.unwrap();
            configs.push(config);
            addresses.push(addr);
            servers.push(server);
        }
        verify(addresses).await;
        for h in servers.drain(..) {
            h.abort();
            let _ = h.await;
        }
        for round in 0..2 {
            let mut addresses = Vec::new();
            for (shard, config) in configs.iter().enumerate() {
                let (addr, server) = common::start_opened_node(config.clone()).await;
                if round == 0 {
                    NodeServiceClient::connect(addr.clone())
                        .await
                        .unwrap()
                        .compact_shard(pb::CompactShardRequest {
                            work_dir: temp.join(format!("compact-{shard}")).display().to_string(),
                            ..Default::default()
                        })
                        .await
                        .unwrap();
                }
                addresses.push(addr);
                servers.push(server);
            }
            verify(addresses).await;
            for h in servers.drain(..) {
                h.abort();
                let _ = h.await;
            }
        }
        std::fs::remove_dir_all(temp).unwrap();
    }
}
#[test]
fn relay_refuses_forged_bucket_edges_even_with_one_child() {
    let field = fields().0.remove(0);
    let req = pb::Bm25QueryRequest {
        range_facet_fields: vec![field.clone()],
        ..Default::default()
    };
    let malformed = pb::Bm25QueryResponse {
        range_facets: vec![pb::RangeFacetCounts {
            column: field.column,
            known: true,
            buckets: vec![pb::RangeBucket {
                from: 0.0,
                to: 1.0,
                count: 9,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    assert_eq!(
        merge_bm25_responses(&req, vec![malformed])
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
}
