//! Embedded runtime contract: the ordinary coordinator and shard services run
//! over private in-memory transports, retain multi-shard ranking semantics,
//! expose streaming completion, support mutations/persistence/mapped ingest,
//! and refuse configuration that could egress document text.

mod common;

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use common::{fit_calibration, start_empty_node, unit_vectors};
use pipestream_search::analyzer::body_spec;
use pipestream_search::bm25::Bm25Params;
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::embedded::{
    EmbeddedError, EmbeddedSearch, EmbeddedSearchConfig, EmbeddedShardConfig,
};
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    ingest_mapped_request, query_stream_response, search_query, selection_query,
    AddDocumentsRequest, AddVectorsRequest, BroadcastCalibrationRequest, CommitReplacementsRequest,
    DeleteDocumentsRequest, DenseQuery, IngestMappedRequest, IntegerValue, LexicalQuery,
    MappedBind, PlanIndexRequest, QueryRequest, QueryStreamRequest, Replacement, SearchQuery,
    SelectionQuery, SetCalibrationRequest,
};
use prost::Message;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet};
use tokio_stream::StreamExt;
use tonic::Request;

const DIM: usize = 32;
const SHARD_ROWS: usize = 3;

fn lexical_query(text: &str) -> QueryRequest {
    QueryRequest {
        request_id: "embedded-parity".into(),
        k: 10,
        selection_k: 10,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "lexical".into(),
                query: Some(search_query::Query::Lexical(LexicalQuery {
                    text: text.into(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    }
}

fn dense_query(vector: Vec<f32>) -> QueryRequest {
    QueryRequest {
        request_id: "embedded-dense".into(),
        k: 4,
        selection_k: 4,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "dense".into(),
                query: Some(search_query::Query::Dense(DenseQuery {
                    vector,
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    }
}

fn local_shards() -> Vec<EmbeddedShardConfig> {
    [0, 100]
        .into_iter()
        .map(|slot_offset| {
            let mut shard = EmbeddedShardConfig::in_memory(slot_offset);
            shard.node.integer_fields = vec!["year".into()];
            shard
        })
        .collect()
}

async fn populate_embedded(runtime: &EmbeddedSearch, corpus: &[f32]) {
    let (shift, scale) = fit_calibration(DIM, 4, corpus);
    let applied = runtime
        .broadcast_calibration(BroadcastCalibrationRequest {
            dim: DIM as u32,
            bit_width: 4,
            shift,
            scale,
        })
        .await
        .unwrap();
    assert!(applied.results.iter().all(|result| result.ok));

    for shard in 0..2 {
        let global_base = if shard == 0 { 0 } else { 100 };
        runtime
            .add_documents(
                shard,
                (0..SHARD_ROWS)
                    .map(|row| {
                        let global = global_base + row as u64;
                        AddDocumentsRequest {
                            text: if row.is_multiple_of(2) {
                                format!("private zebra row {global}")
                            } else {
                                format!("private plain row {global}")
                            },
                            integers: vec![IntegerValue {
                                field: "year".into(),
                                value: 2000 + global as i64,
                            }],
                            analysis: Some(body_spec()),
                            ..Default::default()
                        }
                    })
                    .collect(),
            )
            .await
            .unwrap();
        let start = shard * SHARD_ROWS * DIM;
        let end = start + SHARD_ROWS * DIM;
        runtime
            .add_vectors(
                shard,
                vec![AddVectorsRequest {
                    vectors: corpus[start..end].to_vec(),
                    dim: DIM as u32,
                }],
            )
            .await
            .unwrap();
    }
}

async fn populate_network(addrs: &[String], corpus: &[f32], shift: &[f32], scale: &[f32]) {
    for (shard, addr) in addrs.iter().enumerate() {
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        client
            .set_calibration(SetCalibrationRequest {
                dim: DIM as u32,
                bit_width: 4,
                shift: shift.to_vec(),
                scale: scale.to_vec(),
            })
            .await
            .unwrap();
        client
            .add_documents(tokio_stream::iter((0..SHARD_ROWS).map(move |row| {
                let global_base = if shard == 0 { 0 } else { 100 };
                let global = global_base + row as u64;
                AddDocumentsRequest {
                    text: if row.is_multiple_of(2) {
                        format!("private zebra row {global}")
                    } else {
                        format!("private plain row {global}")
                    },
                    integers: vec![IntegerValue {
                        field: "year".into(),
                        value: 2000 + global as i64,
                    }],
                    analysis: Some(body_spec()),
                    ..Default::default()
                }
            })))
            .await
            .unwrap();
        let start = shard * SHARD_ROWS * DIM;
        let end = start + SHARD_ROWS * DIM;
        client
            .add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
                vectors: corpus[start..end].to_vec(),
                dim: DIM as u32,
            }]))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn local_cluster_matches_network_service_and_streams_completion() {
    let corpus = unit_vectors(2 * SHARD_ROWS, DIM, 0xE8BE_DDED);
    let embedded = EmbeddedSearch::open(EmbeddedSearchConfig::new(local_shards()))
        .await
        .unwrap();
    assert_eq!(embedded.shard_count(), 2);
    assert!(!embedded.allows_network());
    populate_embedded(&embedded, &corpus).await;

    let mut network_handles = Vec::new();
    let mut addrs = Vec::new();
    for slot_offset in [0, 100] {
        let (addr, handle) = start_empty_node(NodeConfig {
            slot_offset,
            analysis_addr: Some("native".into()),
            integer_fields: vec!["year".into()],
            ..Default::default()
        })
        .await;
        addrs.push(addr);
        network_handles.push(handle);
    }
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    populate_network(&addrs, &corpus, &shift, &scale).await;
    let network = CoordinatorServiceImpl::new(addrs)
        .with_bm25(Some("native".into()), Bm25Params::default())
        .with_stream_search(true)
        .with_bm25_stream(true);

    let request = lexical_query("zebra");
    let local_response = embedded.query(request.clone()).await.unwrap();
    let network_response = SearchService::query(&network, Request::new(request.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(local_response, network_response);
    assert_eq!(
        local_response
            .hits
            .iter()
            .map(|hit| hit.doc_id)
            .collect::<Vec<_>>(),
        vec![0, 2, 100, 102]
    );

    let dense_local = embedded
        .query(dense_query(corpus[..DIM].to_vec()))
        .await
        .unwrap();
    let dense_network =
        SearchService::query(&network, Request::new(dense_query(corpus[..DIM].to_vec())))
            .await
            .unwrap()
            .into_inner();
    assert_eq!(dense_local, dense_network);

    let mut stream = embedded
        .query_stream(QueryStreamRequest {
            query: Some(request),
            timeout_ms: 0,
        })
        .await
        .unwrap();
    let mut completions = 0;
    let mut final_response = None;
    while let Some(message) = stream.next().await {
        match message.unwrap().payload.unwrap() {
            query_stream_response::Payload::Revision(_) => {}
            query_stream_response::Payload::Completion(completion) => {
                completions += 1;
                assert!(completion.completed);
                final_response = completion.response;
            }
        }
    }
    assert_eq!(completions, 1);
    assert_eq!(final_response.unwrap(), local_response);

    let health = embedded.cluster_health().await.unwrap();
    assert_eq!(health.targets.len(), 2);
    assert!(health.targets.iter().all(|shard| shard.reachable));
    for handle in network_handles {
        handle.abort();
    }
}

#[tokio::test]
async fn local_mutations_and_persistence_survive_reopen() {
    let root = unique_temp_dir("persistence");
    std::fs::create_dir(&root).unwrap();
    let index = root.join("private-shard.index");
    let corpus = unit_vectors(4, DIM, 0xF105_0001);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);

    let runtime = EmbeddedSearch::create(EmbeddedSearchConfig::single(
        EmbeddedShardConfig::persistent(&index, 0),
    ))
    .await
    .unwrap();
    runtime
        .set_calibration(
            0,
            SetCalibrationRequest {
                dim: DIM as u32,
                bit_width: 4,
                shift,
                scale,
            },
        )
        .await
        .unwrap();
    runtime
        .add_documents(
            0,
            [
                "keep private",
                "delete private",
                "replace private",
                "replacement private",
            ]
            .into_iter()
            .map(|text| AddDocumentsRequest {
                text: text.into(),
                analysis: Some(body_spec()),
                ..Default::default()
            })
            .collect(),
        )
        .await
        .unwrap();
    runtime
        .add_vectors(
            0,
            vec![AddVectorsRequest {
                vectors: corpus,
                dim: DIM as u32,
            }],
        )
        .await
        .unwrap();
    runtime
        .delete_documents(0, DeleteDocumentsRequest { doc_ids: vec![1] })
        .await
        .unwrap();
    runtime
        .commit_replacements(
            0,
            CommitReplacementsRequest {
                replacements: vec![Replacement {
                    old_doc_id: 2,
                    new_doc_id: 3,
                }],
            },
        )
        .await
        .unwrap();
    let flushed = runtime.flush_all().await.unwrap();
    assert_eq!(flushed.len(), 1);
    assert!(flushed[0].written);
    drop(runtime);

    let reopened = EmbeddedSearch::open(EmbeddedSearchConfig::single(
        EmbeddedShardConfig::persistent(&index, 0),
    ))
    .await
    .unwrap();
    let health = reopened.shard_health(0).await.unwrap();
    assert_eq!((health.num_vectors, health.bm25_docs), (4, 4));
    assert_eq!((health.live_docs, health.deleted_docs), (2, 2));
    let hits = reopened.query(lexical_query("private")).await.unwrap();
    assert_eq!(
        hits.hits.iter().map(|hit| hit.doc_id).collect::<Vec<_>>(),
        vec![0, 3]
    );
    drop(reopened);

    let refusal = EmbeddedSearch::create(EmbeddedSearchConfig::single(
        EmbeddedShardConfig::persistent(&index, 0),
    ))
    .await
    .err()
    .expect("create must refuse existing private data");
    assert!(matches!(refusal, EmbeddedError::ExistingData(_)));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn descriptor_plan_and_mapped_ingest_use_the_embedded_schema() {
    let mut shard = EmbeddedShardConfig::in_memory(0);
    shard.node.facet_fields = vec!["id".into()];
    let runtime = EmbeddedSearch::open(EmbeddedSearchConfig::single(shard))
        .await
        .unwrap();
    let descriptor_set = record_descriptor();
    let planned = runtime
        .plan_index(PlanIndexRequest {
            descriptor_set: descriptor_set.clone(),
            message_type: "private.v1.Record".into(),
        })
        .await
        .unwrap();
    let plan = planned.plan.unwrap();
    assert_eq!(plan.vector_path, "embedding");
    assert_eq!(plan.doc_id_path, "id");

    let corpus = unit_vectors(1, DIM, 0xB10D_0001);
    let (shift, scale) = fit_calibration(DIM, 4, &unit_vectors(64, DIM, 0xB10D_0002));
    runtime
        .set_calibration(
            0,
            SetCalibrationRequest {
                dim: DIM as u32,
                bit_width: 4,
                shift,
                scale,
            },
        )
        .await
        .unwrap();
    let ingested = runtime
        .ingest_mapped(
            0,
            vec![
                IngestMappedRequest {
                    payload: Some(ingest_mapped_request::Payload::Bind(MappedBind {
                        descriptor_set,
                        message_type: "private.v1.Record".into(),
                        expected_fingerprint: plan.fingerprint.clone(),
                        body_path: "body".into(),
                        analysis: Some(body_spec()),
                        materialize: None,
                    })),
                },
                IngestMappedRequest {
                    payload: Some(ingest_mapped_request::Payload::Document(record_document(
                        "private-1",
                        "local mapped zebra",
                        &corpus,
                    ))),
                },
            ],
        )
        .await
        .unwrap();
    assert_eq!((ingested.added, ingested.total), (1, 1));
    assert_eq!(ingested.fingerprint, plan.fingerprint);
    let result = runtime.query(lexical_query("zebra")).await.unwrap();
    assert_eq!(result.hits.len(), 1);
    assert_eq!(result.hits[0].doc_id, 0);
}

#[tokio::test]
async fn remote_analysis_configuration_is_refused_before_startup() {
    let mut shard = EmbeddedShardConfig::in_memory(0);
    shard.node.analysis_addr = Some("http://private-text-collector.invalid:9000".into());
    let error = EmbeddedSearch::open(EmbeddedSearchConfig::single(shard))
        .await
        .err()
        .expect("remote analyzer must be rejected");
    assert!(matches!(error, EmbeddedError::InvalidConfig(_)));
    assert!(error.to_string().contains("only native is allowed"));
}

fn scalar(name: &str, number: i32, typ: Type, label: Label) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        label: Some(label as i32),
        r#type: Some(typ as i32),
        ..Default::default()
    }
}

fn record_descriptor() -> Vec<u8> {
    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("private.proto".into()),
            package: Some("private.v1".into()),
            message_type: vec![DescriptorProto {
                name: Some("Record".into()),
                field: vec![
                    scalar("id", 1, Type::String, Label::Optional),
                    scalar("body", 2, Type::String, Label::Optional),
                    scalar("embedding", 3, Type::Float, Label::Repeated),
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

fn varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn string_field(out: &mut Vec<u8>, field: u64, value: &str) {
    varint(out, field << 3 | 2);
    varint(out, value.len() as u64);
    out.extend_from_slice(value.as_bytes());
}

fn record_document(id: &str, body: &str, embedding: &[f32]) -> Vec<u8> {
    let mut out = Vec::new();
    string_field(&mut out, 1, id);
    string_field(&mut out, 2, body);
    varint(&mut out, 3 << 3 | 2);
    varint(&mut out, (embedding.len() * 4) as u64);
    for value in embedding {
        out.extend_from_slice(&value.to_le_bytes());
    }
    out
}

fn unique_temp_dir(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "protomolt-search-embedded-{label}-{}-{nonce}",
        std::process::id()
    ))
}
