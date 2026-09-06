mod common;
use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    coordinator::CoordinatorServiceImpl,
    node::{Layout, NodeConfig},
    pb::{
        self, column_stats::ExactInteger, node_service_client::NodeServiceClient,
        search_service_server::SearchService, ScalarValueType as Type,
    },
};
use tonic::{Code, Request};
const U: [Option<u64>; 12] = [
    None,
    Some(0),
    Some(1),
    Some((1 << 53) + 1),
    Some(1 << 63),
    Some(u64::MAX),
    Some(u64::MAX),
    Some(u64::MAX - 1),
    Some(0),
    Some((1 << 53) + 2),
    Some(12),
    None,
];
const I: [Option<i64>; 12] = [
    Some(i64::MIN),
    Some(i64::MIN),
    None,
    Some(-1),
    Some(0),
    Some(i64::MAX),
    Some(i64::MIN),
    Some(i64::MIN),
    Some(i64::MAX),
    Some((1 << 53) + 1),
    Some(-((1 << 53) + 1)),
    None,
];
fn verify_stats(actual: &[pb::ColumnStats], selected: impl Fn(usize) -> bool) {
    assert_eq!(actual.len(), 2);
    let u: Vec<_> = U
        .iter()
        .enumerate()
        .filter(|(row, _)| selected(*row))
        .filter_map(|(_, v)| *v)
        .collect();
    let i: Vec<_> = I
        .iter()
        .enumerate()
        .filter(|(row, _)| selected(*row))
        .filter_map(|(_, v)| *v)
        .collect();
    assert_eq!(actual[0].field, "unsigned");
    assert_eq!(actual[0].value_type, Type::UnsignedInteger as i32);
    assert_eq!(actual[1].field, "signed");
    assert_eq!(actual[1].value_type, Type::Integer as i32);
    assert!(actual.iter().all(|s| s.known));
    assert_eq!(actual[0].count, u.len() as u64);
    assert_eq!(actual[1].count, i.len() as u64);
    let Some(ExactInteger::Unsigned(s)) = &actual[0].exact_integer else {
        panic!("missing uint summary")
    };
    assert_eq!(s.min, u.iter().min().copied().unwrap_or(0));
    assert_eq!(s.max, u.iter().max().copied().unwrap_or(0));
    assert_eq!(
        (u128::from(s.sum_hi) << 64) | u128::from(s.sum_lo),
        u.iter().map(|v| u128::from(*v)).sum::<u128>()
    );
    let Some(ExactInteger::Signed(s)) = &actual[1].exact_integer else {
        panic!("missing int summary")
    };
    assert_eq!(s.min, i.iter().min().copied().unwrap_or(0));
    assert_eq!(s.max, i.iter().max().copied().unwrap_or(0));
    assert_eq!(
        (i128::from(s.sum_hi) << 64) | i128::from(s.sum_lo),
        i.iter().map(|v| i128::from(*v)).sum::<i128>()
    );
    for s in actual {
        assert!(s.sum.is_finite());
        assert_eq!(
            s.mean,
            if s.count == 0 {
                0.0
            } else {
                s.sum / s.count as f64
            }
        );
    }
}
async fn verify(addresses: Vec<String>) {
    for (shard, addr) in addresses.iter().enumerate() {
        let response = NodeServiceClient::connect(addr.clone())
            .await
            .unwrap()
            .bm25_query(pb::Bm25QueryRequest {
                stats_fields: vec!["unsigned".into(), "signed".into()],
                k: 0,
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.stats.len(), 2);
        for (index, s) in response.stats.iter().enumerate() {
            assert_eq!(s.known, shard < 3);
            assert_eq!(s.count, 0);
            assert_eq!(
                s.value_type,
                if shard < 3 {
                    if index == 0 {
                        Type::UnsignedInteger
                    } else {
                        Type::Integer
                    }
                } else {
                    Type::Unspecified
                } as i32
            );
            assert_eq!(s.exact_integer.is_some(), shard < 3);
        }
    }
    let mut reverse = addresses.clone();
    reverse.reverse();
    for roots in [addresses, reverse] {
        for stream in [false, true] {
            let coord = CoordinatorServiceImpl::new(roots.clone())
                .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default())
                .with_bm25_stream(stream);
            for (text, filter) in [
                ("word", ""),
                ("word", "row < 6"),
                ("missing", ""),
                ("word", "row >= 12"),
            ] {
                let response = coord
                    .bm25_search(Request::new(pb::Bm25SearchRequest {
                        text: text.into(),
                        analysis: Some(body_spec()),
                        filter: filter.into(),
                        k: 1,
                        stats_fields: vec!["unsigned".into(), "signed".into()],
                        ..Default::default()
                    }))
                    .await
                    .unwrap()
                    .into_inner();
                assert!(response.hits.len() <= 1);
                verify_stats(&response.stats, |row| {
                    text == "word" && (filter.is_empty() || (filter == "row < 6" && row < 6))
                });
            }
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exact_column_stats_survive_partition_order_reopen_and_compaction() {
    let (shift, scale) = common::fit_calibration(8, 4, &common::unit_vectors(64, 8, 68241));
    for layout in [Layout::SingleImage, Layout::Segments] {
        let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("typed_stats_{layout:?}_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut configs = vec![];
        let mut addresses = vec![];
        let mut handles = vec![];
        for shard in 0..4 {
            let config = NodeConfig {
                layout,
                index_path: Some(dir.join(format!("{shard}.tv"))),
                wal: true,
                wal_buckets: 2,
                seal_tail_docs: 2,
                slot_offset: shard as u64 * 6,
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
                unsigned_integer_fields: if shard < 3 {
                    vec!["unsigned".into()]
                } else {
                    vec![]
                },
                integer_fields: if shard < 3 {
                    vec!["row".into(), "signed".into()]
                } else {
                    vec!["row".into()]
                },
                ..Default::default()
            };
            let (addr, h) = common::start_empty_node(config.clone()).await;
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
            for row in shard * 6..shard * 6 + 6 {
                let mut integers = vec![pb::IntegerValue {
                    field: "row".into(),
                    value: row as i64,
                }];
                if let Some(v) = I.get(row).copied().flatten() {
                    integers.push(pb::IntegerValue {
                        field: "signed".into(),
                        value: v,
                    });
                }
                client
                    .add_documents(tokio_stream::iter([pb::AddDocumentsRequest {
                        text: "word".into(),
                        analysis: Some(body_spec()),
                        integers,
                        unsigned_integers: U
                            .get(row)
                            .copied()
                            .flatten()
                            .map(|v| pb::UnsignedIntegerValue {
                                field: "unsigned".into(),
                                value: v,
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
                        vectors: common::unit_vectors(1, 8, 746 + row as u64),
                    }]))
                    .await
                    .unwrap();
            }
            client.flush(pb::FlushRequest {}).await.unwrap();
            configs.push(config);
            addresses.push(addr);
            handles.push(h);
        }
        verify(addresses).await;
        for h in handles.drain(..) {
            h.abort();
            let _ = h.await;
        }
        for round in 0..2 {
            let mut addresses = vec![];
            for (shard, config) in configs.iter().enumerate() {
                let (addr, h) = common::start_opened_node(config.clone()).await;
                if round == 0 {
                    NodeServiceClient::connect(addr.clone())
                        .await
                        .unwrap()
                        .compact_shard(pb::CompactShardRequest {
                            work_dir: dir.join(format!("compact-{shard}")).display().to_string(),
                            ..Default::default()
                        })
                        .await
                        .unwrap();
                }
                addresses.push(addr);
                handles.push(h);
            }
            verify(addresses).await;
            for h in handles.drain(..) {
                h.abort();
                let _ = h.await;
            }
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stats_refuse_cross_shard_type_conflicts_even_when_no_values_match() {
    for pair in [
        [Type::Integer, Type::UnsignedInteger],
        [Type::Number, Type::UnsignedInteger],
        [Type::Integer, Type::Number],
    ] {
        let mut addresses = vec![];
        let mut handles = vec![];
        for ty in pair {
            let (addr, h) = common::start_empty_node(NodeConfig {
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
                integer_fields: if ty == Type::Integer {
                    vec!["value".into()]
                } else {
                    vec![]
                },
                unsigned_integer_fields: if ty == Type::UnsignedInteger {
                    vec!["value".into()]
                } else {
                    vec![]
                },
                numeric_fields: if ty == Type::Number {
                    vec!["value".into()]
                } else {
                    vec![]
                },
                ..Default::default()
            })
            .await;
            NodeServiceClient::connect(addr.clone())
                .await
                .unwrap()
                .add_documents(tokio_stream::iter([pb::AddDocumentsRequest {
                    text: "word".into(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                }]))
                .await
                .unwrap();
            addresses.push(addr);
            handles.push(h);
        }
        let coord = CoordinatorServiceImpl::new(addresses)
            .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default());
        for text in ["word", "missing"] {
            let err = coord
                .bm25_search(Request::new(pb::Bm25SearchRequest {
                    text: text.into(),
                    k: 1,
                    analysis: Some(body_spec()),
                    stats_fields: vec!["value".into()],
                    ..Default::default()
                }))
                .await
                .unwrap_err();
            assert_eq!(err.code(), Code::FailedPrecondition);
            assert!(err.message().contains("incompatible numeric types"));
        }
        for h in handles {
            h.abort();
            let _ = h.await;
        }
    }
}
