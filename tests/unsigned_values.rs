mod common;

use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    cel,
    coordinator::CoordinatorServiceImpl,
    node::{Layout, NodeConfig},
    pb,
    pb::{node_service_client::NodeServiceClient, search_service_server::SearchService},
    scorefn::NumericRead,
    values::{self, IngestEnv, IngestVal, NumericTypes, Val},
};
use prost::Message;

struct Pair(u64, u64);
impl NumericRead for Pair {
    fn uint_value(&self, column: usize, _: u32) -> Option<u64> {
        Some(if column == 0 { self.0 } else { self.1 })
    }
    fn value(&self, _: usize, _: u32) -> Option<f64> {
        None
    }
    fn int_value(&self, _: usize, _: u32) -> Option<i64> {
        None
    }
    fn map_value(&self, _: usize, _: u32, _: u32) -> Option<f64> {
        None
    }
    fn geo_value(&self, _: usize, _: u32) -> Option<(f64, f64)> {
        None
    }
    fn facet_ord(&self, _: usize, _: u32) -> Option<u32> {
        None
    }
    fn map_facet_value_ord(&self, _: usize, _: u32, _: u32) -> Option<u32> {
        None
    }
}

#[test]
fn unsigned_arithmetic_matches_wide_integer_and_cel_oracles() {
    let names = ["a".into(), "b".into()];
    let types = NumericTypes {
        numerics: &[],
        integers: &[],
        unsigned_integers: &names,
        map_numerics: &[],
    };
    let edges = [
        0,
        1,
        2,
        (1u64 << 53) + 1,
        i64::MAX as u64,
        1u64 << 63,
        u64::MAX - 1,
        u64::MAX,
    ];
    let mut pairs: Vec<_> = edges
        .iter()
        .flat_map(|a| edges.iter().map(move |b| (*a, *b)))
        .collect();
    let mut random = 8721513329u64;
    for _ in 0..512 {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        pairs.push((random, random.rotate_left(23)));
    }
    for op in ["+", "-", "*", "/", "%"] {
        let expression = format!("a {op} b");
        let compiled = cel::compile_value(&expression).unwrap();
        let (resolved, ty) = values::resolve(&compiled, &types).unwrap();
        assert_eq!(ty, values::ValueType::Uint);
        let oracle = cel_interpreter::Program::compile(&expression).unwrap();
        for &(a, b) in &pairs {
            // Compute in a wider integer domain; only then enforce the u64 range.
            let (wide_a, wide_b) = (u128::from(a), u128::from(b));
            let wide = match op {
                "+" => Some(wide_a + wide_b),
                "-" => wide_a.checked_sub(wide_b),
                "*" => Some(wide_a * wide_b),
                "/" => wide_a.checked_div(wide_b),
                "%" => wide_a.checked_rem(wide_b),
                _ => unreachable!(),
            };
            let expected = wide.and_then(|v| u64::try_from(v).ok());
            assert_eq!(
                values::eval(&resolved, 0, &Pair(a, b)),
                expected.map(Val::Uint),
                "{a} {op} {b}"
            );
            let env = IngestEnv {
                unsigned_integers: [("a".into(), a), ("b".into(), b)].into(),
                ..Default::default()
            };
            assert_eq!(
                values::eval_ingest(&compiled, &env).unwrap(),
                expected.map(IngestVal::Uint)
            );
            // Stock CEL errors are deliberately not the oracle for our absence rule.
            if let Some(v) = expected {
                let mut context = cel_interpreter::Context::default();
                context.add_variable("a", a).unwrap();
                context.add_variable("b", b).unwrap();
                assert_eq!(
                    oracle.execute(&context).unwrap(),
                    cel_interpreter::Value::UInt(v)
                );
            }
        }
    }
}

#[test]
fn unsigned_literals_types_conditionals_and_wire_presence() {
    let env = IngestEnv {
        unsigned_integers: [("counter".into(), u64::MAX)].into(),
        integers: [("signed".into(), 1)].into(),
        ..Default::default()
    };
    for (expression, expected) in [
        ("18446744073709551615u", Some(IngestVal::Uint(u64::MAX))),
        ("0xffffffffffffffffU", Some(IngestVal::Uint(u64::MAX))),
        ("math.abs(counter)", Some(IngestVal::Uint(u64::MAX))),
        ("math.sign(counter)", Some(IngestVal::Uint(1))),
        ("math.sign(0u)", Some(IngestVal::Uint(0))),
        (
            "math.greatest(counter, 0u)",
            Some(IngestVal::Uint(u64::MAX)),
        ),
        ("math.least(counter, 0u)", Some(IngestVal::Uint(0))),
        (
            "counter > 9223372036854775807u",
            Some(IngestVal::Bool(true)),
        ),
        (
            "counter == 18446744073709551614u",
            Some(IngestVal::Bool(false)),
        ),
        (
            "counter != 18446744073709551614u",
            Some(IngestVal::Bool(true)),
        ),
        (
            "counter >= 18446744073709551615u",
            Some(IngestVal::Bool(true)),
        ),
        (
            "counter < 18446744073709551615u",
            Some(IngestVal::Bool(false)),
        ),
        (
            "counter <= 18446744073709551615u",
            Some(IngestVal::Bool(true)),
        ),
        (
            "false ? counter + 1u : counter",
            Some(IngestVal::Uint(u64::MAX)),
        ),
        ("true ? counter : 1u / 0u", Some(IngestVal::Uint(u64::MAX))),
        ("missing ? 1u : counter", None),
        ("double(counter)", Some(IngestVal::Double(u64::MAX as f64))),
        ("missing + counter", None),
        ("counter * 2u", None),
        ("0u - counter", None),
        ("counter % 0u", None),
    ] {
        let compiled = cel::compile_value(expression).unwrap();
        let decoded = pb::ValueExpr::decode(compiled.encode_to_vec().as_slice()).unwrap();
        assert_eq!(
            values::eval_ingest(&decoded, &env).unwrap(),
            expected,
            "{expression}"
        );
    }
    for expression in [
        "1u + 1",
        "1u > 1.0",
        "-1u",
        "math.sqrt(1u)",
        "math.greatest(1u, 1)",
        "true ? 1u : 1",
        "18446744073709551616u",
    ] {
        assert!(cel::compile_value(expression).is_err(), "{expression}");
    }
    for expression in [
        "counter + signed",
        "counter == signed",
        "-counter",
        "math.sqrt(counter)",
        "true ? counter : signed",
        "math.greatest(counter, signed)",
    ] {
        let compiled = cel::compile_value(expression).unwrap();
        assert!(
            values::eval_ingest(&compiled, &env).is_err(),
            "{expression}"
        );
    }
    for value in [None, Some(0), Some(u64::MAX)] {
        let message = pb::ProjectedValue {
            value: value.map(pb::projected_value::Value::UintValue),
        };
        assert_eq!(
            pb::ProjectedValue::decode(message.encode_to_vec().as_slice()).unwrap(),
            message
        );
    }
}

fn projection(expression: &str) -> pb::NamedProjection {
    pb::NamedProjection {
        name: expression.into(),
        expression: expression.into(),
    }
}

async fn verify_queries(addresses: Vec<String>, expected: &[Option<u64>]) {
    let coordinator = CoordinatorServiceImpl::new(addresses)
        .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default());
    // Until typed uint aggregates land, these routes must refuse instead of
    // folding narrowed values or treating the known column as absent.
    for op in [
        pb::AggregateOp::Count,
        pb::AggregateOp::Sum,
        pb::AggregateOp::Cardinality,
    ] {
        let error = coordinator
            .aggregate(tonic::Request::new(pb::AggregateRequest {
                aggregations: vec![pb::Aggregation {
                    name: "n".into(),
                    expression: "counter".into(),
                    op: op as i32,
                    ..Default::default()
                }],
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("uint accumulators"), "{error}");
    }
    let error = coordinator
        .aggregate(tonic::Request::new(pb::AggregateRequest {
            percentiles: vec![pb::PercentileSpec {
                name: "p".into(),
                expression: "counter".into(),
                percentiles: vec![50.0],
            }],
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("uint rank expressions"), "{error}");
    let expressions = [
        "key",
        "counter",
        "derived",
        "overflow",
        "absent",
        "counter > 9223372036854775807u ? counter : 0u",
        "double(counter)",
    ];
    let response = coordinator
        .bm25_search(tonic::Request::new(pb::Bm25SearchRequest {
            text: "word".into(),
            analysis: Some(body_spec()),
            k: 100,
            projections: expressions.iter().map(|e| projection(e)).collect(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.hits.len(), expected.len());
    // The public browse adapter reaches FetchValues after selection. It must
    // carry the same uint wire values as the lexical projection route.
    let browsed = coordinator
        .query(tonic::Request::new(pb::QueryRequest {
            k: 100,
            selection: Some(pb::SelectionQuery {
                node: Some(pb::selection_query::Node::Filter(pb::FilterQuery {
                    id: "all".into(),
                    predicate: Some(pb::filter_query::Predicate::Cel(
                        "has(counter) || !has(counter)".into(),
                    )),
                })),
            }),
            projections: expressions.iter().map(|e| projection(e)).collect(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(browsed.hits.len(), response.hits.len());
    for hit in browsed.hits {
        let reference = response
            .hits
            .iter()
            .find(|h| h.doc_id == hit.doc_id)
            .unwrap();
        assert_eq!(hit.projected, reference.projected);
    }
    let mut seen = std::collections::BTreeSet::new();
    for hit in response.hits {
        use pb::projected_value::Value::{DoubleValue, StringValue, UintValue};
        let Some(StringValue(key)) = &hit.projected[0].value else {
            panic!("missing key")
        };
        let row: usize = key.parse().unwrap();
        assert!(seen.insert(row));
        let value = expected[row];
        let projected: Vec<_> = hit.projected.into_iter().skip(1).map(|v| v.value).collect();
        assert_eq!(
            projected,
            vec![
                value.map(UintValue),
                value.map(UintValue),
                value.and_then(|v| v.checked_add(1)).map(UintValue),
                None,
                value.map(|v| UintValue(if v > i64::MAX as u64 { v } else { 0 })),
                value.map(|v| DoubleValue(v as f64))
            ]
        );
    }
}

#[tokio::test]
async fn unsigned_materialized_projections_survive_distributed_reopen_and_compaction() {
    let values = [
        None,
        Some(0),
        Some(1),
        Some((1u64 << 53) + 1),
        Some(1u64 << 63),
        Some(u64::MAX - 1),
        Some(u64::MAX),
    ];
    let (shift, scale) = common::fit_calibration(8, 4, &common::unit_vectors(64, 8, 189479));
    for layout in [Layout::SingleImage, Layout::Segments] {
        let temp = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("uint_values_{layout:?}_{}", std::process::id()));
        std::fs::create_dir_all(&temp).unwrap();
        let mut addresses = Vec::new();
        let mut servers = Vec::new();
        let mut configs = Vec::new();
        for shard in 0..2 {
            let config = NodeConfig {
                index_path: Some(temp.join(format!("{shard}.tv"))),
                layout,
                wal: true,
                wal_buckets: 2,
                seal_tail_docs: 2,
                slot_offset: shard * 100,
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
                facet_fields: vec!["key".into()],
                unsigned_integer_fields: ["counter", "derived", "overflow", "missing", "absent"]
                    .map(String::from)
                    .to_vec(),
                ..Default::default()
            };
            let (address, server) = common::start_empty_node(config.clone()).await;
            let mut client = NodeServiceClient::connect(address.clone()).await.unwrap();
            client
                .set_calibration(pb::SetCalibrationRequest {
                    dim: 8,
                    bit_width: 4,
                    shift: shift.clone(),
                    scale: scale.clone(),
                })
                .await
                .unwrap();
            for (row, value) in values
                .iter()
                .enumerate()
                .filter(|(row, _)| row % 2 == shard as usize)
            {
                client
                    .add_documents(tokio_stream::iter([pb::AddDocumentsRequest {
                        text: "word".into(),
                        analysis: Some(body_spec()),
                        facets: vec![pb::FacetValue {
                            field: "key".into(),
                            value: row.to_string(),
                        }],
                        unsigned_integers: value
                            .map(|v| pb::UnsignedIntegerValue {
                                field: "counter".into(),
                                value: v,
                            })
                            .into_iter()
                            .collect(),
                        materialize: Some(pb::MaterializeSpec {
                            columns: [
                                ("derived", "counter + 0u"),
                                ("overflow", "counter + 1u"),
                                ("absent", "missing"),
                            ]
                            .map(|(name, expression)| pb::MaterializedColumn {
                                name: name.into(),
                                expression: expression.into(),
                                kind: pb::MaterializeKind::U64 as i32,
                            })
                            .to_vec(),
                        }),
                        ..Default::default()
                    }]))
                    .await
                    .unwrap();
                client
                    .add_vectors(tokio_stream::iter([pb::AddVectorsRequest {
                        dim: 8,
                        vectors: common::unit_vectors(1, 8, row as u64 + 891),
                    }]))
                    .await
                    .unwrap();
            }
            client.flush(pb::FlushRequest {}).await.unwrap();
            addresses.push(address);
            servers.push(server);
            configs.push(config);
        }
        verify_queries(addresses, &values).await;
        for server in servers.drain(..) {
            server.abort();
            let _ = server.await;
        }
        for round in 0..2 {
            let mut addresses = Vec::new();
            for (shard, config) in configs.iter().enumerate() {
                let (address, server) = common::start_opened_node(config.clone()).await;
                let mut client = NodeServiceClient::connect(address.clone()).await.unwrap();
                if round == 0 {
                    client
                        .compact_shard(pb::CompactShardRequest {
                            work_dir: temp
                                .as_path()
                                .join(format!("compact-{shard}"))
                                .display()
                                .to_string(),
                            ..Default::default()
                        })
                        .await
                        .unwrap();
                }
                addresses.push(address);
                servers.push(server);
            }
            verify_queries(addresses, &values).await;
            for server in servers.drain(..) {
                server.abort();
                let _ = server.await;
            }
        }
        std::fs::remove_dir_all(temp).unwrap();
    }
}
