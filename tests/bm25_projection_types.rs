mod common;

use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    coordinator::CoordinatorServiceImpl,
    harness::start_relay,
    node::NodeConfig,
    pb::{self, node_service_client::NodeServiceClient, search_service_server::SearchService},
    relay::merge_bm25_responses,
};
use tonic::{Code, Request};

fn compiled() -> Vec<pb::CompiledProjection> {
    vec![pb::CompiledProjection {
        name: "value".into(),
        expr: Some(pipestream_search::cel::compile_value("value").unwrap()),
    }]
}

fn shard_response(ty: i32, value: Option<pb::projected_value::Value>) -> pb::Bm25QueryResponse {
    pb::Bm25QueryResponse {
        projection_types: vec![ty],
        projection_leaves_known: vec![ty != 0],
        hits: vec![pb::Bm25Hit {
            projected: vec![pb::ProjectedValue { value }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[test]
fn relay_projection_metadata_is_compositional_and_refuses_malformed_children() {
    use pb::{projected_value::Value as V, ScalarValueType as T};
    let request = pb::Bm25QueryRequest {
        projections: compiled(),
        k: 10,
        ..Default::default()
    };
    let full = shard_response(T::UnsignedInteger as i32, Some(V::UintValue(u64::MAX)));
    let absent = shard_response(T::Unspecified as i32, None);
    let inner = merge_bm25_responses(&request, vec![absent.clone(), full.clone()]).unwrap();
    let nested = merge_bm25_responses(&request, vec![inner, absent.clone()]).unwrap();
    assert_eq!(nested.projection_types, [T::UnsignedInteger as i32]);
    assert_eq!(
        nested
            .hits
            .iter()
            .filter(|h| h.projected[0].value == Some(V::UintValue(u64::MAX)))
            .count(),
        1
    );
    let mut bad = vec![];
    let mut missing = full.clone();
    missing.projection_types.clear();
    bad.push(missing);
    bad.push(shard_response(999, None));
    bad.push(shard_response(
        T::UnsignedInteger as i32,
        Some(V::IntValue(1)),
    ));
    bad.push(shard_response(T::Unspecified as i32, Some(V::UintValue(1))));
    let mut width = full.clone();
    width.hits[0].projected.clear();
    bad.push(width);
    // A zero-hit child still contributes its declared type.
    for ty in [T::Integer, T::Number, T::Text, T::Boolean] {
        let mut conflicting = shard_response(ty as i32, None);
        conflicting.hits.clear();
        bad.push(conflicting);
    }
    for child in bad {
        let error = merge_bm25_responses(&request, vec![full.clone(), child]).unwrap_err();
        assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    }
}

/// Each leaf owns one aligned vector/document slot, so relays exercise their
/// real health/epoch and streaming paths. Leaf 0 has full-width unsigned data;
/// leaf 1 either lacks the column, agrees, or declares a conflicting family.
async fn check_topologies(second: pb::ScalarValueType) {
    use pb::{projected_value::Value as V, ScalarValueType as T};
    let mut servers = Vec::new();
    let mut addresses = Vec::new();
    let (shift, scale) = common::fit_calibration(8, 4, &common::unit_vectors(64, 8, 18452));
    for (slot, ty) in [T::UnsignedInteger, second].into_iter().enumerate() {
        let (addr, handle) = common::start_empty_node(NodeConfig {
            slot_offset: slot as u64,
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
            unsigned_integer_fields: if ty == T::UnsignedInteger {
                vec!["value".into()]
            } else {
                vec![]
            },
            integer_fields: if ty == T::Integer {
                vec!["value".into()]
            } else {
                vec![]
            },
            numeric_fields: if ty == T::Number {
                vec!["value".into()]
            } else {
                vec![]
            },
            ..Default::default()
        })
        .await;
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
        client
            .add_documents(tokio_stream::iter([pb::AddDocumentsRequest {
                text: if slot == 0 { "word" } else { "other" }.into(),
                analysis: Some(body_spec()),
                unsigned_integers: if ty == T::UnsignedInteger {
                    vec![pb::UnsignedIntegerValue {
                        field: "value".into(),
                        value: u64::MAX - slot as u64,
                    }]
                } else {
                    vec![]
                },
                integers: if ty == T::Integer {
                    vec![pb::IntegerValue {
                        field: "value".into(),
                        value: 1,
                    }]
                } else {
                    vec![]
                },
                numerics: if ty == T::Number {
                    vec![pb::NumericValue {
                        field: "value".into(),
                        value: 1.0,
                    }]
                } else {
                    vec![]
                },
                ..Default::default()
            }]))
            .await
            .unwrap();
        client
            .add_vectors(tokio_stream::iter([pb::AddVectorsRequest {
                dim: 8,
                vectors: common::unit_vectors(1, 8, 891 + slot as u64),
            }]))
            .await
            .unwrap();
        // Type metadata must not depend on selection or top-k size.
        let direct = client
            .bm25_query(pb::Bm25QueryRequest {
                projections: compiled(),
                k: 0,
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(direct.projection_types, [ty as i32]);
        assert!(direct.hits.is_empty());
        addresses.push(addr);
        servers.push(handle);
    }
    let (a, _, ah) = start_relay(vec![addresses[0].clone()]).await;
    let (b, _, bh) = start_relay(vec![addresses[1].clone()]).await;
    let (top, _, th) = start_relay(vec![a.clone(), b.clone()]).await;
    servers.extend([ah, bh, th]);
    assert!(
        pipestream_search::analyzer::analyze_document_native("   ", Some(&body_spec()))
            .unwrap()
            .into_body()
            .terms
            .is_empty()
    );
    let incompatible = matches!(second, T::Integer | T::Number);
    for roots in [addresses.clone(), vec![a, b], vec![top]] {
        for stream in [false, true] {
            let coordinator = CoordinatorServiceImpl::new(roots.clone())
                .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default())
                .with_bm25_stream(stream);
            // Both match, only one matches, and neither matches. A type
            // conflict cannot disappear just because a leaf has zero hits.
            for text in ["word other", "word", "missing", "   "] {
                let result = coordinator
                    .bm25_search(Request::new(pb::Bm25SearchRequest {
                        text: text.into(),
                        analysis: Some(body_spec()),
                        k: 10,
                        projections: vec![pb::NamedProjection {
                            name: "value".into(),
                            expression: "value".into(),
                        }],
                        ..Default::default()
                    }))
                    .await;
                if incompatible {
                    let error = result.unwrap_err();
                    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
                    assert!(
                        error.message().contains("incompatible types across shards"),
                        "{error}"
                    );
                } else {
                    let response = result.unwrap().into_inner();
                    assert_eq!(
                        response.hits.len(),
                        match text {
                            "word other" => 2,
                            "word" => 1,
                            _ => 0,
                        }
                    );
                    for hit in response.hits {
                        assert_eq!(hit.projected.len(), 1);
                        let expected = if hit.doc_id == 0 {
                            Some(V::UintValue(u64::MAX))
                        } else if second == T::UnsignedInteger {
                            Some(V::UintValue(u64::MAX - 1))
                        } else {
                            None
                        };
                        assert_eq!(hit.projected[0].value, expected);
                    }
                }
            }
        }
    }
    for server in servers {
        server.abort();
        let _ = server.await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lexical_projection_types_and_full_width_values_survive_nested_relays() {
    for ty in [
        pb::ScalarValueType::Unspecified,
        pb::ScalarValueType::UnsignedInteger,
    ] {
        check_topologies(ty).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lexical_projection_type_conflicts_refuse_including_empty_match_sets() {
    for ty in [pb::ScalarValueType::Integer, pb::ScalarValueType::Number] {
        check_topologies(ty).await;
    }
}

#[tokio::test]
async fn empty_lexical_store_validates_expressions_and_reports_literal_types() {
    let (addr, server) = common::start_empty_node(NodeConfig::default()).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let projections = ["18446744073709551615u", "true", "1", "1.0", "missing"]
        .into_iter()
        .map(|expression| pb::CompiledProjection {
            name: expression.into(),
            expr: Some(pipestream_search::cel::compile_value(expression).unwrap()),
        })
        .collect();
    let response = client
        .bm25_query(pb::Bm25QueryRequest {
            projections,
            k: 0,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    use pb::ScalarValueType as T;
    assert_eq!(
        response.projection_types,
        [
            T::UnsignedInteger,
            T::Boolean,
            T::Integer,
            T::Number,
            T::Unspecified
        ]
        .map(|ty| ty as i32)
    );
    assert!(response.hits.is_empty());
    for expr in [None, Some(pb::ValueExpr::default())] {
        let error = client
            .bm25_query(pb::Bm25QueryRequest {
                projections: vec![pb::CompiledProjection {
                    name: "invalid".into(),
                    expr,
                }],
                k: 0,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument, "{error}");
    }
    server.abort();
    let _ = server.await;
}
