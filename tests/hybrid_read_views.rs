//! Both raw hybrid legs must share the admitted field and document view,
//! including when a relay composes multiple levels of physical claims.
use pipestream_search::{
    harness::{serve_node, start_relay},
    mapping::derive_plan,
    node::{Bm25Shard, NodeConfig, NodeServiceImpl},
    pb::{node_service_client::NodeServiceClient, node_service_server::NodeService, *},
    postings::{AnalyzedDoc, Bm25Store, StoredBinding},
    vector::{embedded_turbovec_config, VectorIndex},
    visibility::VisibilityScope,
};
use prost::Message;
use tonic::{Code, Request};

async fn leaf(offset: u64, different_binding: bool, empty: bool) -> NodeServiceImpl {
    let mut binding = derive_plan(
        include_bytes!("fixtures/vector-binding/descriptor.bin"),
        "vector_binding.Named",
    )
    .unwrap()
    .vector_binding
    .unwrap();
    if different_binding {
        binding.plan_fingerprint = "b".repeat(64);
    }
    let mut store = Bm25Store::new().with_facets(&["audience"]);
    store.set_binding(Some(StoredBinding {
        plan_fingerprint: binding.plan_fingerprint.clone(),
        body_path: "body".into(),
        vector_binding: binding.encode_to_vec(),
        ..Default::default()
    }));
    if !empty {
        for row in 0..3 {
            store.add_document(
                row,
                "word".into(),
                AnalyzedDoc::body(vec![("word".into(), 1, vec![(0, 4)])], 1),
            );
            store.set_facet(0, row, if row == 1 { "private" } else { "public" });
        }
    }
    let config = embedded_turbovec_config(4, &[0.0; 16], &[1.0; 16]).unwrap();
    let mut index = VectorIndex::from_backend_config(16, &config).unwrap();
    if !empty {
        // The fourth vector has no document metadata.
        index.add(&[0.25; 64], 16).unwrap();
    }
    let node = NodeServiceImpl::new(
        Some(index),
        NodeConfig {
            slot_offset: offset,
            ..Default::default()
        },
    )
    .with_bm25(Some(Bm25Shard::Building(store)));
    if !empty {
        node.delete_documents(Request::new(DeleteDocumentsRequest {
            doc_ids: vec![offset + 2],
            ..Default::default()
        }))
        .await
        .unwrap();
    }
    node
}

async fn request_for(
    client: &mut NodeServiceClient<tonic::transport::Channel>,
) -> ShardLegsRequest {
    let visibility = Some(DocumentVisibility {
        filter: pipestream_search::cel::compile_filter("audience == 'public'").unwrap(),
    });
    let stats = client
        .term_stats(TermStatsRequest {
            terms: vec!["word".into()],
            visibility: visibility.clone(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    ShardLegsRequest {
        k: 8,
        vector: vec![0.25; 16],
        terms: vec!["word".into()],
        global_doc_count: stats.doc_count,
        global_total_doc_length: stats.total_doc_length,
        global_doc_frequencies: stats.doc_frequencies,
        expected_stats_epoch: stats.stats_epoch,
        expected_stats_incarnation: stats.stats_incarnation.clone(),
        read_context: Some(VectorReadContext {
            field: "semantic".into(),
            visibility,
            expected_stats_epoch: stats.stats_epoch,
            expected_stats_incarnation: stats.stats_incarnation,
        }),
        ..Default::default()
    }
}

fn bits(hits: &[RawLegHit]) -> Vec<(u64, u32)> {
    let mut values: Vec<_> = hits
        .iter()
        .map(|hit| (hit.doc_id, hit.score.to_bits()))
        .collect();
    values.sort_unstable();
    values
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fused_legs_preserve_view_binding_and_versions_through_nested_relays() {
    let mut addresses = Vec::new();
    let mut handles = Vec::new();
    for offset in [0, 100, 200] {
        let (address, handle) = serve_node(leaf(offset, false, false).await).await;
        addresses.push(address);
        handles.push(handle);
    }
    let (left, _, handle) = start_relay(addresses[..2].to_vec()).await;
    handles.push(handle);
    let (root, _, handle) = start_relay(vec![left, addresses[2].clone()]).await;
    handles.push(handle);
    let mut client = NodeServiceClient::connect(root).await.unwrap();
    let request = request_for(&mut client).await;
    let response = client
        .shard_legs(request.clone())
        .await
        .unwrap()
        .into_inner();
    let receipt = response.read_receipt.as_ref().unwrap();
    let context = request.read_context.as_ref().unwrap();
    assert_eq!(receipt.vector_binding.as_ref().unwrap().field, "semantic");
    assert_eq!(receipt.stats_epoch, request.expected_stats_epoch);
    assert_eq!(
        receipt.stats_incarnation,
        request.expected_stats_incarnation
    );
    VisibilityScope::new(context.visibility.as_ref())
        .unwrap()
        .validate_echo(
            &receipt.visibility_fingerprint,
            &receipt.visibility_columns_known,
        )
        .unwrap();
    assert_eq!(receipt.visibility_columns_known, vec![true]);
    assert_eq!(
        response
            .vector_hits
            .iter()
            .map(|hit| hit.doc_id)
            .collect::<Vec<_>>(),
        vec![0, 100, 200]
    );
    assert_eq!(
        response
            .bm25_hits
            .iter()
            .map(|hit| hit.doc_id)
            .collect::<Vec<_>>(),
        vec![0, 100, 200]
    );
    let mut flat_vector = Vec::new();
    let mut flat_lexical = Vec::new();
    for address in &addresses {
        let mut child = NodeServiceClient::connect(address.clone()).await.unwrap();
        let mut direct = request_for(&mut child).await;
        // Same global statistics, physical claims local to each child.
        direct.global_doc_count = request.global_doc_count;
        direct.global_total_doc_length = request.global_total_doc_length;
        direct.global_doc_frequencies = request.global_doc_frequencies.clone();
        let share = child.shard_legs(direct).await.unwrap().into_inner();
        flat_vector.extend(share.vector_hits);
        flat_lexical.extend(share.bm25_hits);
    }
    assert_eq!(bits(&response.vector_hits), bits(&flat_vector));
    assert_eq!(bits(&response.bm25_hits), bits(&flat_lexical));
    // A caller predicate cannot widen the authority view on either leg.
    let mut conflict = request.clone();
    conflict.filter = pipestream_search::cel::compile_filter("audience == 'private'").unwrap();
    let empty = client.shard_legs(conflict).await.unwrap().into_inner();
    assert!(empty.vector_hits.is_empty() && empty.bm25_hits.is_empty());
    assert!(empty.read_receipt.is_some());
    for field in ["signal", "body", "missing"] {
        let mut empty = request.clone();
        empty.k = 0;
        empty.vector.clear();
        empty.terms.clear();
        empty.global_doc_frequencies.clear();
        empty.read_context.as_mut().unwrap().field = field.into();
        assert_eq!(
            client.shard_legs(empty).await.unwrap_err().code(),
            Code::FailedPrecondition
        );
    }
    // The nested read-context claim is independently fenced. Clearing the
    // legacy outer claim must not turn a stale admitted view into a new read.
    let mut child = NodeServiceClient::connect(addresses[0].clone())
        .await
        .unwrap();
    child
        .delete_documents(DeleteDocumentsRequest {
            doc_ids: vec![0],
            ..Default::default()
        })
        .await
        .unwrap();
    let mut stale = request;
    stale.expected_stats_epoch = 0;
    stale.expected_stats_incarnation.clear();
    assert_eq!(
        client.shard_legs(stale).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    for handle in handles {
        handle.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_empty_relay_child_cannot_hide_an_incompatible_binding() {
    let (first, a) = serve_node(leaf(0, false, false).await).await;
    let (second, b) = serve_node(leaf(100, true, true).await).await;
    let (relay, _, c) = start_relay(vec![first, second]).await;
    let mut client = NodeServiceClient::connect(relay).await.unwrap();
    let request = request_for(&mut client).await;
    let error = client.shard_legs(request).await.unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("binding"), "{error}");
    for handle in [a, b, c] {
        handle.abort();
    }
}
