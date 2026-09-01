//! Generation-overlay mutation acceptance: one tombstone decision is shared
//! by lexical, vector, exact-rerank, browse, fetch, and replacement paths.

mod common;

use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    search_query, selection_query, AddDocumentsRequest, AddVectorsRequest, Bm25SearchRequest,
    BrowseShardRequest, CommitReplacementsRequest, DeleteDocumentsRequest, DenseQuery,
    DenseScoreMode, ExactVectorRescoreRequest, FetchValuesRequest, FlushRequest,
    GetDocumentsRequest, HealthRequest, QueryRequest, Replacement, SearchQuery, SelectionQuery,
    SetCalibrationRequest, TermStatsRequest,
};
use tonic::Request;

const DIM: usize = 64;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_and_append_then_replace_are_consistent_across_read_paths() {
    let (analysis, analysis_handle) = common::mock::start_mock_analysis().await;
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("protomolt_mutations_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let index_path = dir.join("shard.vector");
    let corpus = common::unit_vectors(5, DIM, 0xD311_7001);
    let (shift, scale) = common::fit_calibration(DIM, 4, &corpus);
    let (addr, node_handle) = common::start_empty_node(NodeConfig {
        index_path: Some(index_path.clone()),
        analysis_addr: Some(analysis.clone()),
        ..Default::default()
    })
    .await;
    let mut node = NodeServiceClient::connect(addr.clone()).await.unwrap();
    node.set_calibration(SetCalibrationRequest {
        dim: DIM as u32,
        bit_width: 4,
        shift,
        scale,
    })
    .await
    .unwrap();
    node.add_documents(tokio_stream::iter((0..4).map(|id| AddDocumentsRequest {
        text: format!("common row {id}"),
        ..Default::default()
    })))
    .await
    .unwrap();
    node.add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
        vectors: corpus[..4 * DIM].to_vec(),
        dim: DIM as u32,
    }]))
    .await
    .unwrap();
    node.flush(FlushRequest {}).await.unwrap();

    let deleted = node
        .delete_documents(DeleteDocumentsRequest { doc_ids: vec![0] })
        .await
        .unwrap()
        .into_inner();
    assert_eq!((deleted.deleted, deleted.already_deleted), (1, 0));
    let idempotent = node
        .delete_documents(DeleteDocumentsRequest { doc_ids: vec![0] })
        .await
        .unwrap()
        .into_inner();
    assert_eq!((idempotent.deleted, idempotent.already_deleted), (0, 1));
    assert_eq!(idempotent.live_revision, deleted.live_revision);

    let health = node.health(HealthRequest {}).await.unwrap().into_inner();
    assert_eq!((health.live_docs, health.deleted_docs), (3, 1));
    let stats = node
        .term_stats(TermStatsRequest {
            terms: vec!["common".into()],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(stats.doc_count, 3);
    assert_eq!(stats.doc_frequencies, vec![3]);

    let browse = node
        .browse_shard(BrowseShardRequest {
            k: 10,
            first_page: true,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(browse.doc_ids, vec![1, 2, 3]);
    assert!(node
        .get_documents(GetDocumentsRequest { doc_ids: vec![0] })
        .await
        .unwrap()
        .into_inner()
        .documents
        .is_empty());
    assert!(node
        .fetch_values(FetchValuesRequest {
            candidate_ids: vec![0],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .rows
        .is_empty());
    assert!(node
        .exact_vector_rescore(ExactVectorRescoreRequest {
            vector: corpus[..DIM].to_vec(),
            candidate_ids: vec![0],
            max_logical_bytes: 0,
        })
        .await
        .unwrap()
        .into_inner()
        .hits
        .is_empty());

    let coordinator = CoordinatorServiceImpl::new(vec![addr.clone()])
        .with_bm25(Some(analysis), Default::default());
    let lexical = SearchService::bm25_search(
        &coordinator,
        Request::new(Bm25SearchRequest {
            text: "common".into(),
            k: 10,
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        lexical
            .hits
            .iter()
            .map(|hit| hit.doc_id)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );

    let exact = SearchService::query(
        &coordinator,
        Request::new(QueryRequest {
            k: 4,
            selection_k: 4,
            selection: Some(SelectionQuery {
                node: Some(selection_query::Node::Search(SearchQuery {
                    id: "dense".into(),
                    query: Some(search_query::Query::Dense(DenseQuery {
                        vector: corpus[..DIM].to_vec(),
                        score_mode: DenseScoreMode::Fp32Rerank as i32,
                        ..Default::default()
                    })),
                })),
            }),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(exact.hits.len(), 3);
    assert!(exact.hits.iter().all(|hit| hit.doc_id != 0));

    node.add_documents(tokio_stream::iter(vec![AddDocumentsRequest {
        text: "common replacement".into(),
        ..Default::default()
    }]))
    .await
    .unwrap();
    let incomplete = node
        .commit_replacements(CommitReplacementsRequest {
            replacements: vec![Replacement {
                old_doc_id: 1,
                new_doc_id: 4,
            }],
        })
        .await
        .unwrap_err();
    assert_eq!(incomplete.code(), tonic::Code::FailedPrecondition);
    assert!(incomplete.message().contains("every active"));
    node.add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
        vectors: corpus[4 * DIM..].to_vec(),
        dim: DIM as u32,
    }]))
    .await
    .unwrap();
    let replacement = node
        .commit_replacements(CommitReplacementsRequest {
            replacements: vec![Replacement {
                old_doc_id: 1,
                new_doc_id: 4,
            }],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (replacement.committed, replacement.already_committed),
        (1, 0)
    );
    let replacement_retry = node
        .commit_replacements(CommitReplacementsRequest {
            replacements: vec![Replacement {
                old_doc_id: 1,
                new_doc_id: 4,
            }],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (
            replacement_retry.committed,
            replacement_retry.already_committed
        ),
        (0, 1)
    );

    let browse = node
        .browse_shard(BrowseShardRequest {
            k: 10,
            first_page: true,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(browse.doc_ids, vec![2, 3, 4]);
    node.flush(FlushRequest {}).await.unwrap();
    let persisted = pipestream_search::live_docs::LiveDocs::open(
        &pipestream_search::node::live_docs_sidecar_path(&index_path),
    )
    .unwrap();
    assert!(persisted.is_deleted(0));
    assert!(persisted.is_deleted(1));
    assert_eq!(persisted.deleted_count(), 2);

    node_handle.abort();
    analysis_handle.abort();
    let _ = std::fs::remove_dir_all(dir);
}
