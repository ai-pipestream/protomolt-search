//! Collapse-by-parent integration: k means k distinct parent documents,
//! each represented by its best chunk, deduped across shards.
//!
//! The fixture plants lineage so every interesting shape exists: parents
//! with several chunks on one shard, single-chunk parents, and one
//! STRADDLER opinion whose chunks live on both shards (the layout the
//! coordinator's parent dedupe exists for).

mod common;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use turbovec_search::coordinator::CoordinatorServiceImpl;
use turbovec_search::node::NodeConfig;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::search_service_server::SearchService as _;
use turbovec_search::pb::{
    AddDocumentsRequest, AddVectorsRequest, DocLineage, SearchRequest, SetCalibrationRequest,
};

use common::{fit_calibration, mock::start_mock_analysis, start_empty_node, unit_vectors};

const DIM: usize = 32;
const SHARD_DOCS: usize = 6;

async fn add_documents_with_lineage(addr: &str, opinions: &[u64]) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    let opinions = opinions.to_vec();
    let feeder = tokio::spawn(async move {
        for (i, &opinion) in opinions.iter().enumerate() {
            tx.send(AddDocumentsRequest {
                map_numerics: Vec::new(),
                map_facets: Vec::new(),
                numerics: Vec::new(),
                facets: Vec::new(),
                text: format!("chunk {i} of opinion {opinion} with some plain words"),
                analysis: None,
                lineage: Some(DocLineage {
                    opinion_id: opinion,
                    cluster_id: opinion,
                    span_start: 0,
                    span_end: 10,
                }),
                fields: Vec::new(),
                integers: Vec::new(),
                timestamps: Vec::new(),
                geo_points: Vec::new(),
            })
            .await
            .unwrap();
        }
    });
    client.add_documents(ReceiverStream::new(rx)).await.unwrap();
    feeder.await.unwrap();
}

async fn start_shard(
    analysis: &str,
    slot_offset: u64,
    opinions: &[u64],
    vectors: Vec<f32>,
    shift: &[f32],
    scale: &[f32],
) -> (
    String,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let (addr, handle) = start_empty_node(NodeConfig {
        slot_offset,
        analysis_addr: Some(analysis.to_string()),
        ..Default::default()
    })
    .await;
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
    add_documents_with_lineage(&addr, opinions).await;
    let (tx, rx) = mpsc::channel(4);
    tx.send(AddVectorsRequest {
        vectors,
        dim: DIM as u32,
    })
    .await
    .unwrap();
    drop(tx);
    client.add_vectors(ReceiverStream::new(rx)).await.unwrap();
    (addr, handle)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collapse_returns_distinct_parents_and_matches_reference() {
    let (analysis, mock) = start_mock_analysis().await;
    let corpus = unit_vectors(2 * SHARD_DOCS, DIM, 0xC011_0001);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);

    // Parent layout. Opinion 102 STRADDLES the shard cut.
    let shard0_opinions = [100u64, 100, 100, 101, 101, 102];
    let shard1_opinions = [102u64, 103, 103, 104, 105, 105];
    let opinion_of = |gid: u64| -> u64 {
        if gid < SHARD_DOCS as u64 {
            shard0_opinions[gid as usize]
        } else {
            shard1_opinions[(gid - 100) as usize]
        }
    };

    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for (shard, opinions) in [(0usize, &shard0_opinions), (1, &shard1_opinions)] {
        let start = shard * SHARD_DOCS;
        let vecs = corpus[start * DIM..(start + SHARD_DOCS) * DIM].to_vec();
        // Slot offsets 0 and 100: disjoint global id ranges.
        let (addr, handle) = start_shard(
            &analysis,
            shard as u64 * 100,
            opinions.as_slice(),
            vecs,
            &shift,
            &scale,
        )
        .await;
        addrs.push(addr);
        handles.push(handle);
    }
    let coordinator = CoordinatorServiceImpl::new(addrs);
    let query = corpus[..DIM].to_vec();
    let k = 4u32;

    // Reference: deep plain search over everything, grouped by parent
    // client-side (max aggregation, ties to the lower global id).
    let deep = coordinator
        .fanout_search("ref", &query, 2 * SHARD_DOCS as u32, false)
        .await
        .unwrap();
    let mut best: std::collections::HashMap<u64, (u64, f32)> = std::collections::HashMap::new();
    for h in &deep.hits {
        let parent = opinion_of(h.vector_id);
        let entry = best.entry(parent).or_insert((h.vector_id, h.score));
        if h.score > entry.1 || (h.score == entry.1 && h.vector_id < entry.0) {
            *entry = (h.vector_id, h.score);
        }
    }
    let mut want: Vec<(u64, u64, u32)> = best
        .into_iter()
        .map(|(parent, (gid, score))| (parent, gid, score.to_bits()))
        .collect();
    want.sort_by(|a, b| {
        f32::from_bits(b.2)
            .total_cmp(&f32::from_bits(a.2))
            .then_with(|| a.1.cmp(&b.1))
    });
    want.truncate(k as usize);

    // Collapse through the public Search handler, twice for determinism.
    let request = || {
        tonic::Request::new(SearchRequest {
            request_id: String::new(),
            k,
            vector: query.clone(),
            collapse_parents: true,
        })
    };
    let first = coordinator.search(request()).await.unwrap().into_inner();
    let second = coordinator.search(request()).await.unwrap().into_inner();

    let got: Vec<(u64, u64, u32)> = first
        .hits
        .iter()
        .map(|h| (h.parent_id, h.vector_id, h.score.to_bits()))
        .collect();
    assert_eq!(got, want, "collapse must equal the grouped deep reference");
    assert_eq!(
        got,
        second
            .hits
            .iter()
            .map(|h| (h.parent_id, h.vector_id, h.score.to_bits()))
            .collect::<Vec<_>>(),
        "collapse must be deterministic"
    );

    // Distinctness: every parent exactly once; parents are real opinion
    // ids from the lineage, and each hit's chunk really belongs to its
    // parent.
    let mut parents: Vec<u64> = first.hits.iter().map(|h| h.parent_id).collect();
    parents.sort_unstable();
    parents.dedup();
    assert_eq!(parents.len(), first.hits.len(), "no duplicate parents");
    for h in &first.hits {
        assert_eq!(
            h.parent_id,
            opinion_of(h.vector_id),
            "chunk belongs to parent"
        );
    }

    // The straddler: search deep enough that opinion 102 must be present,
    // and it appears exactly once even though its chunks live on both
    // shards.
    let all = coordinator
        .search(tonic::Request::new(SearchRequest {
            request_id: String::new(),
            k: 6,
            vector: query.clone(),
            collapse_parents: true,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(all.hits.len(), 6, "six distinct opinions exist");
    assert_eq!(
        all.hits.iter().filter(|h| h.parent_id == 102).count(),
        1,
        "straddler opinion collapses across shards"
    );

    // Plain search is untouched: no parent ids, chunk-level results.
    let plain = coordinator
        .search(tonic::Request::new(SearchRequest {
            request_id: String::new(),
            k,
            vector: query.clone(),
            collapse_parents: false,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(plain.hits.iter().all(|h| h.parent_id == 0));

    for h in handles {
        h.abort();
    }
    mock.abort();
}
