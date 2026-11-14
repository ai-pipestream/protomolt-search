#![cfg(feature = "net")]

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pipestream_search::analyzer::{body_spec, TURBO_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::link::NodeLink;
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::pb::{AddDocumentsRequest, DeleteDocumentsRequest, DocLineage};
use tonic::Code;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_fallback_preserves_local_sentences_lineage_order_and_tombstones() {
    let node = Arc::new(NodeServiceImpl::new(
        None,
        NodeConfig {
            slot_offset: 100,
            analysis_addr: Some(TURBO_ANALYSIS_BACKEND.into()),
            sentence_fields: vec!["body".into()],
            ..Default::default()
        },
    ));
    let mut link = NodeLink::local(Arc::clone(&node));
    let texts = [
        "Café is open. The café closes tonight.",
        "An introduction. The appellate court affirmed jurisdiction.",
    ];
    link.add_documents(tokio_stream::iter(texts.into_iter().map(|text| {
        AddDocumentsRequest {
            text: text.into(),
            analysis: Some(body_spec()),
            lineage: Some(DocLineage {
                parent_id: 7,
                group_id: 1,
                span_start: 1000,
                span_end: 1000 + text.len() as u32,
            }),
            ..Default::default()
        }
    })))
    .await
    .unwrap();
    let local = CoordinatorServiceImpl::with_local_nodes(vec![Arc::clone(&node)]);
    let (address, server) = pipestream_search::harness::serve_node((*node).clone()).await;
    let remote = CoordinatorServiceImpl::new(vec![address]);
    let shortlist = [(101, 3.0), (100, 2.0), (101, 1.0), (999, 0.0)];
    let expected = local.fetch_rerank_candidates(&shortlist).await.unwrap();
    let actual = remote.fetch_rerank_candidates(&shortlist).await.unwrap();
    for (i, (left, right)) in expected.iter().zip(&actual).enumerate() {
        assert_eq!((left.doc_id, left.prior_score), shortlist[i]);
        assert_eq!((right.doc_id, right.prior_score), shortlist[i]);
        assert_eq!(left.text, right.text);
        assert_eq!(left.sentence_spans, right.sentence_spans);
        assert_eq!(left.lineage_span_start, right.lineage_span_start);
        if i < 3 {
            assert_eq!(right.lineage_span_start, 1000);
            assert_eq!(right.sentence_spans.len(), 2);
            for &(start, end) in &right.sentence_spans {
                assert!(!right
                    .text
                    .get(start as usize..end as usize)
                    .unwrap()
                    .is_empty());
            }
        } else {
            assert!(right.text.is_empty());
            assert!(right.sentence_spans.is_empty());
        }
    }
    assert_eq!(actual[0].sentence_spans, actual[2].sentence_spans);
    link.delete_documents(DeleteDocumentsRequest {
        doc_ids: vec![100],
        ..Default::default()
    })
    .await
    .unwrap();
    for coord in [&local, &remote] {
        let rows = coord.fetch_rerank_candidates(&[(100, 1.0)]).await.unwrap();
        assert!(rows[0].text.is_empty());
        assert!(rows[0].sentence_spans.is_empty());
        assert_eq!(rows[0].lineage_span_start, 0);
    }
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn stalled_remote_document_fetch_obeys_the_rerank_deadline() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coordinator =
        CoordinatorServiceImpl::new(vec![format!("http://{}", listener.local_addr().unwrap())]);
    let deadline = (SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
        + Duration::from_millis(500))
    .as_micros() as u64;
    let request = tokio::spawn(async move {
        coordinator
            .fetch_rerank_candidates_with_deadline(&[(0, 1.0)], deadline)
            .await
    });
    // Accept the TCP connection but never answer HTTP/2 or the document RPC.
    let (_connection, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(3), request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.unwrap_err().code(), Code::DeadlineExceeded);
}

#[tokio::test]
async fn expired_fallback_refuses_before_contacting_any_node() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coordinator =
        CoordinatorServiceImpl::new(vec![format!("http://{}", listener.local_addr().unwrap())]);
    let error = coordinator
        .fetch_rerank_candidates_with_deadline(&[(0, 1.0)], 1)
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::DeadlineExceeded);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}
