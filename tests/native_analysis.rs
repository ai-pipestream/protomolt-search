//! End-to-end native lexical analysis through streamed ingest and coordinator
//! query analysis. No analysis service is started in this test.

mod common;

use pipestream_search::analyzer::{
    body_spec, uax29_body_spec, NATIVE_ANALYSIS_BACKEND, SOURCE_TOKENS, STEMMER_NONE,
};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::{AddDocumentsRequest, TermStatsRequest};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use common::start_empty_node;

#[tokio::test]
async fn document_fetch_retains_identity_and_does_not_alias_large_physical_ids() {
    use pipestream_search::pb::{DocumentIdentity, GetDocumentsRequest};
    let offset = 100;
    let (addr, node) = start_empty_node(NodeConfig {
        slot_offset: offset,
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        ..Default::default()
    })
    .await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let identity = DocumentIdentity {
        document_key: b"key\0\xff".to_vec(),
        version: 7,
        chunk_ordinal: None,
    };
    client
        .add_documents(tokio_stream::iter([AddDocumentsRequest {
            text: "private source".into(),
            analysis: Some(body_spec()),
            original_source: Some(common::protobuf_source("private source", "key")),
            identity: Some(identity.clone()),
            ..Default::default()
        }]))
        .await
        .unwrap();
    let found = client
        .get_documents(GetDocumentsRequest {
            doc_ids: vec![offset + (1u64 << 32), offset, u64::MAX],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(found.documents.len(), 1);
    assert_eq!(found.documents[0].doc_id, offset);
    assert_eq!(found.documents[0].identity, Some(identity));
    node.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_streamed_ingest_and_query_share_term_identity() {
    let (addr, node) = start_empty_node(NodeConfig {
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        ..Default::default()
    })
    .await;
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    let (send, receive) = mpsc::channel(4);
    for text in ["Running Rodríguez", "running Rodriguez", "unrelated"] {
        send.send(AddDocumentsRequest {
            text: text.to_string(),
            analysis: Some(body_spec()),
            ..Default::default()
        })
        .await
        .unwrap();
    }
    drop(send);
    let added = client
        .add_documents(ReceiverStream::new(receive))
        .await
        .unwrap()
        .into_inner();
    assert_eq!((added.added, added.total), (3, 3));

    let stats = client
        .term_stats(TermStatsRequest {
            version_only: false,
            visibility: None,
            terms: vec!["run".into(), "rodriguez".into()],
            fields: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(stats.doc_frequencies, vec![2, 2]);

    let coordinator = CoordinatorServiceImpl::new(vec![addr]).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    );
    let spec = body_spec();
    let hits = coordinator
        .fanout_bm25("RUNNING RODRÍGUEZ", 10, Some(&spec))
        .await
        .unwrap();
    assert_eq!(
        hits.iter().map(|hit| hit.doc_id).collect::<Vec<_>>(),
        [0, 1]
    );

    node.abort();
}

#[tokio::test]
async fn native_uax29_spec_preserves_opennlp_word_boundaries() {
    let mut spec = uax29_body_spec();
    spec.stemmer = STEMMER_NONE;
    spec.term_vector_source = SOURCE_TOKENS;
    let analyzed = pipestream_search::analyzer::analyze_document(
        NATIVE_ANALYSIS_BACKEND,
        "😀 U.S. 東京 hot-dog",
        Some(&spec),
    )
    .await
    .unwrap();
    let terms: Vec<String> = analyzed
        .into_body()
        .terms
        .into_iter()
        .map(|(term, _, _)| term)
        .collect();
    assert_eq!(terms, ["😀", "u.s", "東", "京", "hot", "dog"]);
}
