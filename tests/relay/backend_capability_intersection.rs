//! Relay `GetVectorBackend` capability intersection across heterogeneous
//! child row counts (`>= 32,768` staged-eligible vs `< 32,768` non-staged).

use super::*;
use pipestream_search::harness::seeded_index;
use pipestream_search::pb::{DenseCoverage, DenseQuery, GetVectorBackendRequest};

fn dense_mode_query(qvec: &[f32], mode: DenseExecutionMode) -> tonic::Request<QueryRequest> {
    tonic::Request::new(QueryRequest {
        k: 10,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "vec".into(),
                query: Some(search_query::Query::Dense(DenseQuery {
                    vector: qvec.to_vec(),
                    execution_mode: mode as i32,
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relay_intersects_staged_capability_across_40k_and_2k_children() {
    const BIG_ROWS: usize = 40_000;
    const SMALL_ROWS: usize = 2_048;
    const TOTAL_ROWS: usize = BIG_ROWS + SMALL_ROWS;

    let corpus = unit_vectors(TOTAL_ROWS, DIM, 0x57A6_2E1A);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &corpus[..1024 * DIM]);
    let mut big_index = seeded_index(DIM, BIT_WIDTH, &shift, &scale);
    big_index.add(&corpus[..BIG_ROWS * DIM], DIM).unwrap();
    let mut small_index = seeded_index(DIM, BIT_WIDTH, &shift, &scale);
    small_index.add(&corpus[BIG_ROWS * DIM..], DIM).unwrap();

    let (big_addr, h0) = start_node(big_index, NodeConfig::default()).await;
    let (small_addr, h1) = start_node(
        small_index,
        NodeConfig {
            slot_offset: BIG_ROWS as u64,
            ..Default::default()
        },
    )
    .await;
    let (relay_addr, _relay, relay_h) =
        start_relay(vec![big_addr.clone(), small_addr.clone()]).await;

    let merged = NodeServiceClient::connect(relay_addr.clone())
        .await
        .unwrap()
        .get_vector_backend(GetVectorBackendRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(merged.num_vectors, TOTAL_ROWS as u64);
    let caps = &merged.descriptor.as_ref().unwrap().capabilities;
    assert!(!caps.iter().any(|c| c == "staged_top_k"), "{caps:?}");

    let flat = CoordinatorServiceImpl::new(vec![big_addr, small_addr]);
    let relayed = CoordinatorServiceImpl::new(vec![relay_addr]);
    let qvec = unit_vectors(1, DIM, 0x57A6_2E1B);
    let hit_ids = |hits: &[QueryHit]| -> Vec<(u64, u32)> {
        hits.iter().map(|h| (h.doc_id, h.score.to_bits())).collect()
    };
    for mode in [
        DenseExecutionMode::Exhaustive,
        DenseExecutionMode::Balanced,
        DenseExecutionMode::Auto,
    ] {
        let expected = flat
            .query(dense_mode_query(&qvec, mode))
            .await
            .unwrap()
            .into_inner();
        let actual = relayed
            .query(dense_mode_query(&qvec, mode))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            hit_ids(&actual.hits),
            hit_ids(&expected.hits),
            "mode={mode:?}"
        );
        let outcome = actual.dense_execution.unwrap();
        assert_eq!(outcome.resolved_mode, DenseExecutionMode::Exhaustive as i32);
        assert_eq!(outcome.coverage, DenseCoverage::ExhaustiveCertified as i32);
    }
    relay_h.abort();
    h0.abort();
    h1.abort();
}
