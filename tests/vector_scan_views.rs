mod common;

use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    node::{Layout, NodeConfig},
    pb::{node_service_client::NodeServiceClient, *},
    visibility::VisibilityScope,
};
use prost::Message;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Channel, Code, Status};

struct Fixture {
    root: std::path::PathBuf,
    client: NodeServiceClient<Channel>,
    task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    context: VectorReadContext,
    binding: MappedVectorBinding,
    vector: Vec<f32>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

async fn fixture(layout: Layout, coalesce: bool) -> Fixture {
    let binding = pipestream_search::mapping::derive_plan(
        include_bytes!("fixtures/vector-binding/descriptor.bin"),
        "vector_binding.Named",
    )
    .unwrap()
    .vector_binding
    .unwrap();
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "vector-scan-view-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir_all(root.parent().unwrap()).unwrap();
    std::fs::create_dir(&root).unwrap();
    let (addr, task) = common::start_empty_node(NodeConfig {
        index_path: Some(root.join("shard.tv")),
        seal_tail_docs: 2,
        layout,
        coalesce,
        slot_offset: 100,
        chunk_blocks: 1,
        facet_fields: vec!["audience".into()],
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
        ..Default::default()
    })
    .await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let vectors = common::unit_vectors(4, 16, 64022);
    let vector = vectors[16..32].to_vec(); // the best match is private
    let (shift, scale) = common::fit_calibration(16, 4, &vectors);
    client
        .set_calibration(SetCalibrationRequest {
            dim: 16,
            bit_width: 4,
            shift,
            scale,
        })
        .await
        .unwrap();
    client
        .apply_wal_binding(ApplyWalBindingRequest {
            plan_fingerprint: binding.plan_fingerprint.clone(),
            body_path: "body".into(),
            vector_binding: binding.encode_to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    for (row, audience) in ["public", "private", "public"].into_iter().enumerate() {
        client
            .add_documents(tokio_stream::iter([AddDocumentsRequest {
                text: "word".into(),
                analysis: Some(body_spec()),
                lineage: Some(DocLineage {
                    parent_id: 42,
                    ..Default::default()
                }),
                facets: vec![FacetValue {
                    field: "audience".into(),
                    value: audience.into(),
                }],
                ..Default::default()
            }]))
            .await
            .unwrap();
        if row == 1 {
            client
                .add_vectors(tokio_stream::iter([AddVectorsRequest {
                    dim: 16,
                    vectors: vectors[..32].to_vec(),
                }]))
                .await
                .unwrap();
        }
    }
    client
        .add_vectors(tokio_stream::iter([AddVectorsRequest {
            dim: 16,
            vectors: vectors[32..].to_vec(),
        }]))
        .await
        .unwrap();
    client
        .delete_documents(DeleteDocumentsRequest {
            doc_ids: vec![102],
            ..Default::default()
        })
        .await
        .unwrap();
    if matches!(layout, Layout::SingleImage) {
        client.flush(FlushRequest {}).await.unwrap();
    }
    let visibility = DocumentVisibility {
        filter: pipestream_search::cel::compile_filter("audience == 'public'").unwrap(),
    };
    let membership = client
        .resolve_vector_bitmap(VectorBitmapRequest {
            field: "semantic".into(),
            visibility: Some(visibility.clone()),
        })
        .await
        .unwrap()
        .into_inner();
    Fixture {
        root,
        client,
        task,
        binding,
        vector,
        context: VectorReadContext {
            field: "semantic".into(),
            visibility: Some(visibility),
            expected_stats_epoch: membership.stats_epoch,
            expected_stats_incarnation: membership.stats_incarnation,
        },
    }
}

fn check_receipt(receipt: &VectorReadReceipt, fixture: &Fixture) {
    assert_eq!(receipt.vector_binding.as_ref(), Some(&fixture.binding));
    assert_eq!(receipt.stats_epoch, fixture.context.expected_stats_epoch);
    assert_eq!(
        receipt.stats_incarnation,
        fixture.context.expected_stats_incarnation
    );
    VisibilityScope::new(fixture.context.visibility.as_ref())
        .unwrap()
        .validate_echo(
            &receipt.visibility_fingerprint,
            &receipt.visibility_columns_known,
        )
        .unwrap();
    assert_eq!(receipt.visibility_columns_known, vec![true]);
}

async fn shard_scan(
    fixture: &Fixture,
    context: Option<VectorReadContext>,
    collapse: bool,
    filter: &str,
) -> Result<(Option<VectorReadReceipt>, Vec<ScoredHit>), Status> {
    let mut client = fixture.client.clone();
    let mut stream = client
        .search_shard(tokio_stream::iter([SearchShardRequest {
            payload: Some(search_shard_request::Payload::Start(StartShardSearch {
                vector: fixture.vector.clone(),
                k: 10,
                collapse_parents: collapse,
                read_context: context.clone(),
                filter: pipestream_search::cel::compile_filter(filter).unwrap(),
                ..Default::default()
            })),
        }]))
        .await?
        .into_inner();
    let mut receipt = None;
    while let Some(frame) = match stream.message().await {
        Ok(frame) => frame,
        Err(error) => {
            assert!(receipt.is_none(), "refusal after scan output");
            return Err(error);
        }
    } {
        match frame.payload.unwrap() {
            search_shard_response::Payload::ReadReady(ready) => {
                assert!(context.is_some() && receipt.is_none());
                receipt = Some(ready);
            }
            search_shard_response::Payload::FloorUpdate(_) => {
                assert_eq!(receipt.is_some(), context.is_some(), "floor before receipt");
            }
            search_shard_response::Payload::Done(done) => {
                assert_eq!(
                    receipt.is_some(),
                    context.is_some(),
                    "completion before receipt"
                );
                assert!(stream.message().await?.is_none());
                return Ok((receipt, done.hits));
            }
        }
    }
    panic!("scan ended without completion")
}

async fn streaming_scan(
    fixture: &Fixture,
    context: Option<VectorReadContext>,
    collapse: bool,
    filter: &str,
) -> Result<(Option<VectorReadReceipt>, Vec<(u64, u32)>), Status> {
    let mut client = fixture.client.clone();
    let mut stream = client
        .stream_search(tokio_stream::iter([StreamSearchRequest {
            payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
                vector: fixture.vector.clone(),
                collapse_parents: collapse,
                read_context: context.clone(),
                filter: pipestream_search::cel::compile_filter(filter).unwrap(),
                ..Default::default()
            })),
        }]))
        .await?
        .into_inner();
    let mut receipt = None;
    let mut hits = Vec::new();
    while let Some(frame) = match stream.message().await {
        Ok(frame) => frame,
        Err(error) => {
            assert!(receipt.is_none(), "refusal after scan output");
            return Err(error);
        }
    } {
        match frame.payload.unwrap() {
            stream_search_response::Payload::ReadReady(ready) => {
                assert!(context.is_some() && receipt.is_none());
                receipt = Some(ready);
            }
            stream_search_response::Payload::Batch(batch) => {
                assert_eq!(
                    receipt.is_some(),
                    context.is_some(),
                    "candidate before receipt"
                );
                let stride = if collapse { 20 } else { 12 };
                assert_eq!(batch.hits.len() % stride, 0);
                for hit in batch.hits.chunks_exact(stride) {
                    hits.push((
                        u64::from_le_bytes(hit[..8].try_into().unwrap()),
                        u32::from_le_bytes(hit[8..12].try_into().unwrap()),
                    ));
                    if collapse {
                        assert_eq!(u64::from_le_bytes(hit[12..].try_into().unwrap()), 42);
                    }
                }
            }
            stream_search_response::Payload::Summary(summary) => {
                assert_eq!(
                    receipt.is_some(),
                    context.is_some(),
                    "completion before receipt"
                );
                assert!(summary.completed);
                assert!(stream.message().await?.is_none());
                return Ok((receipt, hits));
            }
            other => panic!("unexpected frame {other:?}"),
        }
    }
    panic!("scan ended without completion")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_scan_path_certifies_its_view_before_emitting_and_filters_before_scoring() {
    for layout in [Layout::SingleImage, Layout::Segments] {
        for coalesce in [false, true] {
            let fixture = fixture(layout, coalesce).await;
            let (_, reference) = shard_scan(&fixture, None, false, "").await.unwrap();
            assert_eq!(reference.len(), 3); // includes private and vector-only rows
            let score = reference
                .iter()
                .find(|hit| hit.vector_id == 100)
                .unwrap()
                .score
                .to_bits();
            for collapse in [false, true] {
                for filter in [
                    "",
                    "audience == 'public' || audience == 'private'",
                    "audience == 'private'",
                ] {
                    let (receipt, hits) =
                        shard_scan(&fixture, Some(fixture.context.clone()), collapse, filter)
                            .await
                            .unwrap();
                    check_receipt(&receipt.unwrap(), &fixture);
                    let actual: Vec<_> = hits
                        .iter()
                        .map(|hit| (hit.vector_id, hit.score.to_bits()))
                        .collect();
                    let expected = if filter == "audience == 'private'" {
                        vec![]
                    } else {
                        vec![(100, score)]
                    };
                    assert_eq!(actual, expected);
                    let (receipt, hits) =
                        streaming_scan(&fixture, Some(fixture.context.clone()), collapse, filter)
                            .await
                            .unwrap();
                    check_receipt(&receipt.unwrap(), &fixture);
                    assert_eq!(hits, expected);
                }
            }
            // A missing-value authority predicate must not admit vector-only row 103.
            let mut missing = fixture.context.clone();
            missing.visibility.as_mut().unwrap().filter =
                pipestream_search::cel::compile_filter("!has(audience)").unwrap();
            let (_, hits) = shard_scan(&fixture, Some(missing.clone()), false, "")
                .await
                .unwrap();
            assert!(hits.is_empty());
            let (_, hits) = streaming_scan(&fixture, Some(missing), false, "")
                .await
                .unwrap();
            assert!(hits.is_empty());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_or_stale_scan_contexts_refuse_before_any_receipt_or_hit() {
    for coalesce in [false, true] {
        let mut fixture = fixture(Layout::SingleImage, coalesce).await;
        for case in 0..4 {
            let mut context = fixture.context.clone();
            match case {
                0 => context.field = "signal".into(),
                1 => context.expected_stats_epoch += 1,
                2 => context.expected_stats_incarnation = vec![9; 32],
                _ => context.visibility = Some(DocumentVisibility::default()),
            }
            let expected = if case == 3 {
                Code::InvalidArgument
            } else {
                Code::FailedPrecondition
            };
            for collapse in [false, true] {
                assert_eq!(
                    shard_scan(&fixture, Some(context.clone()), collapse, "")
                        .await
                        .unwrap_err()
                        .code(),
                    expected
                );
                assert_eq!(
                    streaming_scan(&fixture, Some(context.clone()), collapse, "")
                        .await
                        .unwrap_err()
                        .code(),
                    expected
                );
            }
        }
        fixture
            .client
            .delete_documents(DeleteDocumentsRequest {
                doc_ids: vec![100],
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            shard_scan(&fixture, Some(fixture.context.clone()), false, "")
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
        assert_eq!(
            streaming_scan(&fixture, Some(fixture.context.clone()), false, "")
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streamed_identity_resolution_cannot_escape_the_scans_authority_view() {
    let fixture = fixture(Layout::SingleImage, false).await;
    let mut client = fixture.client.clone();
    let (tx, rx) = mpsc::channel(4);
    tx.send(StreamSearchRequest {
        payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
            vector: fixture.vector.clone(),
            read_context: Some(fixture.context.clone()),
            identity_limits: Some(StreamIdentityLimits {
                max_rows: 10,
                max_response_bytes: 4096,
                timeout_ms: 5000,
            }),
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let mut stream = client
        .stream_search(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    let stream_search_response::Payload::ReadReady(receipt) =
        stream.message().await.unwrap().unwrap().payload.unwrap()
    else {
        panic!("receipt must be first")
    };
    check_receipt(&receipt, &fixture);
    loop {
        match stream.message().await.unwrap().unwrap().payload.unwrap() {
            stream_search_response::Payload::Batch(_) => {}
            stream_search_response::Payload::IdentityReady(_) => break,
            other => panic!("unexpected {other:?}"),
        }
    }
    tx.send(StreamSearchRequest {
        payload: Some(stream_search_request::Payload::ResolveIdentities(
            ResolveStreamIdentities {
                vector_ids: vec![101],
            },
        )),
    })
    .await
    .unwrap();
    assert_eq!(
        stream.message().await.unwrap_err().code(),
        Code::PermissionDenied
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn relay_refuses_a_scoped_scan_before_dialing_any_children() {
    let (address, _, task) = pipestream_search::harness::start_relay(vec![
        "http://must-not-resolve.invalid:50051".into(),
    ])
    .await;
    let mut client = NodeServiceClient::connect(address).await.unwrap();
    let response = client
        .stream_search(tokio_stream::iter([StreamSearchRequest {
            payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
                read_context: Some(VectorReadContext {
                    field: "semantic".into(),
                    ..Default::default()
                }),
                ..Default::default()
            })),
        }]))
        .await;
    assert_eq!(response.unwrap_err().code(), Code::FailedPrecondition);
    task.abort();
    let _ = task.await;
}
