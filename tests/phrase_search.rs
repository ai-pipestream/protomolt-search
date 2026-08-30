//! End-to-end phrase vocabulary coverage through native ingest, durable
//! derived postings, exact distributed scoring, and entity map filtering.

use std::sync::Arc;

use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::harness::{start_empty_node, start_empty_phrase_node};
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    AddDocumentsRequest, Bm25SearchRequest, MapFacetField, PhraseSearchOptions, PhraseSearchRequest,
};
use pipestream_search::phrases::{entity_key, PhraseIndex};
use protomolt_analyzer::GlossaryEntry;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

fn phrase_index() -> Arc<PhraseIndex> {
    Arc::new(
        PhraseIndex::new(
            vec![
                GlossaryEntry {
                    id: "nyc".into(),
                    term: "New York City".into(),
                },
                GlossaryEntry {
                    id: "new-york".into(),
                    term: "New York".into(),
                },
                GlossaryEntry {
                    id: "hot-dog".into(),
                    term: "Hot Dog".into(),
                },
            ],
            "phrases".into(),
            Some("entities".into()),
            true,
            false,
        )
        .unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phrase_search_boosts_registered_matches_and_exposes_entity_map() {
    let phrases = phrase_index();
    let (addr, node) = start_empty_phrase_node(
        NodeConfig {
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            bm25_fields: vec!["body".into(), "phrases".into()],
            map_facet_fields: vec!["entities".into()],
            ..Default::default()
        },
        phrases.clone(),
    )
    .await;
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    let (send, receive) = mpsc::channel(4);
    for text in [
        "New York City food",
        "New x York x City food",
        "Hot Dog food",
    ] {
        send.send(AddDocumentsRequest {
            text: text.to_string(),
            analysis: Some(body_spec()),
            ..Default::default()
        })
        .await
        .unwrap();
    }
    drop(send);
    client
        .add_documents(ReceiverStream::new(receive))
        .await
        .unwrap();

    let coordinator = CoordinatorServiceImpl::new(vec![addr.clone()])
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_phrase_index(Some(phrases));
    let base = Bm25SearchRequest {
        text: "New York City".into(),
        k: 10,
        analysis: Some(body_spec()),
        ..Default::default()
    };
    let ordinary = SearchService::bm25_search(&coordinator, Request::new(base.clone()))
        .await
        .unwrap()
        .into_inner();
    let phrase = SearchService::phrase_search(
        &coordinator,
        Request::new(PhraseSearchRequest {
            base: Some(base),
            options: Some(PhraseSearchOptions {
                weight_per_token: 1.0,
                max_weight: 3.0,
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(phrase.hits[0].doc_id, 0);
    assert!(phrase.hits[0].score > ordinary.hits[0].score);
    let phrase_terms: Vec<_> = phrase.hits[0]
        .terms
        .iter()
        .filter(|term| term.field == "phrases")
        .collect();
    assert_eq!(
        phrase_terms.len(),
        2,
        "nested registered concepts highlight"
    );
    assert!(phrase_terms.iter().any(|term| term
        .offsets
        .iter()
        .any(|span| span.start == 0 && span.end == 13)));

    let concept_key = entity_key("glossary", "nyc");
    let filtered = SearchService::phrase_search(
        &coordinator,
        Request::new(PhraseSearchRequest {
            base: Some(Bm25SearchRequest {
                text: "food".into(),
                k: 10,
                analysis: Some(body_spec()),
                filter: format!(r#"entities["{concept_key}"] == "matched""#),
                map_facet_fields: vec![MapFacetField {
                    column: "entities".into(),
                    key: concept_key,
                }],
                ..Default::default()
            }),
            options: None,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        filtered
            .hits
            .iter()
            .map(|hit| hit.doc_id)
            .collect::<Vec<_>>(),
        [0]
    );
    assert_eq!(filtered.facets[0].counts[0].count, 1);

    // A mixed generation must refuse rather than quietly score phrase
    // evidence on only the rebuilt shard.
    let (old_addr, old_node) = start_empty_node(NodeConfig {
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        ..Default::default()
    })
    .await;
    let mut old_client = NodeServiceClient::connect(old_addr.clone()).await.unwrap();
    let (old_send, old_receive) = mpsc::channel(1);
    old_send
        .send(AddDocumentsRequest {
            text: "old generation food".into(),
            analysis: Some(body_spec()),
            ..Default::default()
        })
        .await
        .unwrap();
    drop(old_send);
    old_client
        .add_documents(ReceiverStream::new(old_receive))
        .await
        .unwrap();
    let mixed = CoordinatorServiceImpl::new(vec![addr, old_addr])
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_phrase_index(Some(phrase_index()));
    let error = SearchService::phrase_search(
        &mixed,
        Request::new(PhraseSearchRequest {
            base: Some(Bm25SearchRequest {
                text: "New York City food".into(),
                k: 10,
                analysis: Some(body_spec()),
                ..Default::default()
            }),
            options: None,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("complete rebuilt generation"));

    node.abort();
    old_node.abort();
}
