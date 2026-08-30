//! End-to-end native lexical analysis through streamed ingest and coordinator
//! query analysis. No analysis service is started in this test.

mod common;

use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::{AddDocumentsRequest, TermStatsRequest};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use common::start_empty_node;

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
