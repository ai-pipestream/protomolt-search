//! Document-mode streaming gates: `collapse_parents` on a streaming
//! coordinator returns the top-k PARENT documents with cross-shard
//! chunk retrieval — the coordinator aggregates tagged chunk emissions
//! by lineage, so an opinion whose chunks straddle a shard cut needs no
//! colocation. Representatives must equal the bidi collapse path's
//! (same semantic, both exact); each parent's group must hold EXACTLY
//! the chunks scoring at or above the returned floor, wherever they
//! lived; and everything is deterministic despite racy floor timing.

mod common;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use turbovec_search::coordinator::CoordinatorServiceImpl;
use turbovec_search::node::NodeConfig;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::search_service_server::SearchService as _;
use turbovec_search::pb::{
    AddDocumentsRequest, AddVectorsRequest, DocLineage, SearchRequest, SearchResponse,
    SetCalibrationRequest,
};

use common::{fit_calibration, mock::start_mock_analysis, start_empty_node, unit_vectors};

const DIM: usize = 32;
const SHARD_DOCS: usize = 6;

// Parent layout, straight from the bidi collapse gate: opinion 102
// STRADDLES the shard cut (chunk 5 on shard 0, chunk with global id
// 100 on shard 1). Shard 1's slot offset is 100.
const SHARD0_OPINIONS: [u64; SHARD_DOCS] = [100, 100, 100, 101, 101, 102];
const SHARD1_OPINIONS: [u64; SHARD_DOCS] = [102, 103, 103, 104, 105, 105];

fn opinion_of(gid: u64) -> u64 {
    if gid < SHARD_DOCS as u64 {
        SHARD0_OPINIONS[gid as usize]
    } else {
        SHARD1_OPINIONS[(gid - 100) as usize]
    }
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
    let (tx, rx) = mpsc::channel(8);
    let opinions = opinions.to_vec();
    let feeder = tokio::spawn(async move {
        for (i, &opinion) in opinions.iter().enumerate() {
            tx.send(AddDocumentsRequest {
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
            })
            .await
            .unwrap();
        }
    });
    client.add_documents(ReceiverStream::new(rx)).await.unwrap();
    feeder.await.unwrap();
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

struct Fixture {
    stream: CoordinatorServiceImpl,
    bidi: CoordinatorServiceImpl,
    corpus: Vec<f32>,
    handles: Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
    mock: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

async fn start_fixture() -> Fixture {
    let (analysis, mock) = start_mock_analysis().await;
    let mut corpus = unit_vectors(2 * SHARD_DOCS, DIM, 0xC011_0002);
    // The straddler's two chunks share one vector, so both score
    // identically and any floor that admits opinion 102 admits both —
    // the cross-shard retrieval is then a deterministic assertion, not
    // a hope about score layout.
    let straddler = corpus[5 * DIM..6 * DIM].to_vec();
    corpus[6 * DIM..7 * DIM].copy_from_slice(&straddler);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);

    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for (shard, opinions) in [(0usize, &SHARD0_OPINIONS), (1, &SHARD1_OPINIONS)] {
        let start = shard * SHARD_DOCS;
        let vecs = corpus[start * DIM..(start + SHARD_DOCS) * DIM].to_vec();
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
    Fixture {
        stream: CoordinatorServiceImpl::new(addrs.clone()).with_stream_search(true),
        bidi: CoordinatorServiceImpl::new(addrs),
        corpus,
        handles,
        mock,
    }
}

impl Fixture {
    async fn shutdown(self) {
        for h in self.handles {
            h.abort();
        }
        self.mock.abort();
    }
}

async fn document_search(
    coordinator: &CoordinatorServiceImpl,
    query: &[f32],
    k: u32,
) -> SearchResponse {
    coordinator
        .search(tonic::Request::new(SearchRequest {
            request_id: String::new(),
            k,
            vector: query.to_vec(),
            collapse_parents: true,
        }))
        .await
        .unwrap()
        .into_inner()
}

fn rep_signature(hits: &[turbovec_search::pb::ScoredHit]) -> Vec<(u64, u64, u32)> {
    hits.iter()
        .map(|h| (h.parent_id, h.vector_id, h.score.to_bits()))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_mode_matches_bidi_collapse_and_retrieves_chunk_groups() {
    let fx = start_fixture().await;
    let query = fx.corpus[..DIM].to_vec();
    let k = 4u32;

    let streamed = document_search(&fx.stream, &query, k).await;
    let bidi = fx
        .bidi
        .search(tonic::Request::new(SearchRequest {
            request_id: String::new(),
            k,
            vector: query.clone(),
            collapse_parents: true,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        rep_signature(&streamed.hits),
        rep_signature(&bidi.hits),
        "document-mode representatives must equal the bidi collapse path"
    );
    assert!(bidi.groups.is_empty(), "the bidi path carries no groups");

    // Chunk-group oracle from a deep plain search on the same cluster:
    // for each returned parent, exactly its chunks scoring at or above
    // the returned floor, score descending then id.
    let deep = fx
        .bidi
        .fanout_search("deep", &query, 2 * SHARD_DOCS as u32, false)
        .await
        .unwrap();
    assert_eq!(streamed.groups.len(), streamed.hits.len());
    for (hit, group) in streamed.hits.iter().zip(&streamed.groups) {
        assert_eq!(hit.parent_id, group.parent_id);
        let mut want: Vec<(u64, u32)> = deep
            .hits
            .iter()
            .filter(|h| {
                opinion_of(h.vector_id) == group.parent_id && h.score >= streamed.chunk_floor
            })
            .map(|h| (h.vector_id, h.score.to_bits()))
            .collect();
        want.sort_by(|a, b| {
            f32::from_bits(b.1)
                .total_cmp(&f32::from_bits(a.1))
                .then_with(|| a.0.cmp(&b.0))
        });
        let got: Vec<(u64, u32)> = group
            .chunks
            .iter()
            .map(|c| (c.vector_id, c.score.to_bits()))
            .collect();
        assert_eq!(
            got, want,
            "parent {}: group must be exactly the chunks at or above the floor",
            group.parent_id
        );
        // The representative is the group's best chunk.
        let best = &group.chunks[0];
        assert_eq!(
            (best.vector_id, best.score.to_bits()),
            (hit.vector_id, hit.score.to_bits())
        );
        for c in &group.chunks {
            assert_eq!(c.parent_id, group.parent_id);
        }
    }

    // The floor is one ULP below the k-th best parent score.
    let kth = streamed.hits.last().unwrap().score;
    assert_eq!(streamed.chunk_floor.to_bits(), kth.next_down().to_bits());

    // Determinism across racy floor timing.
    let again = document_search(&fx.stream, &query, k).await;
    assert_eq!(rep_signature(&streamed.hits), rep_signature(&again.hits));
    assert_eq!(streamed.chunk_floor.to_bits(), again.chunk_floor.to_bits());
    for (a, b) in streamed.groups.iter().zip(&again.groups) {
        let sig = |g: &turbovec_search::pb::ParentGroup| {
            g.chunks
                .iter()
                .map(|c| (c.vector_id, c.score.to_bits()))
                .collect::<Vec<_>>()
        };
        assert_eq!(sig(a), sig(b));
    }
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn straddler_group_retrieves_chunks_from_both_shards() {
    let fx = start_fixture().await;
    // Query at the straddler's own (shared) vector: opinion 102 tops
    // the ranking and both of its chunks — global ids 5 (shard 0) and
    // 100 (shard 1) — score identically, so both clear any floor that
    // admits the parent at all.
    let query = fx.corpus[5 * DIM..6 * DIM].to_vec();
    let streamed = document_search(&fx.stream, &query, 3).await;
    let group = streamed
        .groups
        .iter()
        .find(|g| g.parent_id == 102)
        .expect("the straddler ranks under its own vector");
    let ids: Vec<u64> = group.chunks.iter().map(|c| c.vector_id).collect();
    assert!(
        ids.contains(&5) && ids.contains(&100),
        "opinion 102 must retrieve chunks from BOTH shards, got {ids:?}"
    );
    // And exactly once each.
    assert_eq!(
        streamed
            .groups
            .iter()
            .filter(|g| g.parent_id == 102)
            .count(),
        1
    );
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fewer_parents_than_k_returns_every_chunk_unfloored() {
    let fx = start_fixture().await;
    let query = fx.corpus[..DIM].to_vec();
    // Six distinct opinions exist; ask for ten.
    let streamed = document_search(&fx.stream, &query, 10).await;
    assert_eq!(streamed.hits.len(), 6);
    assert_eq!(streamed.chunk_floor, f32::NEG_INFINITY);
    let total_chunks: usize = streamed.groups.iter().map(|g| g.chunks.len()).sum();
    assert_eq!(
        total_chunks,
        2 * SHARD_DOCS,
        "with no floor every chunk of every parent is retrieved"
    );
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plain_streaming_search_is_untouched() {
    let fx = start_fixture().await;
    let query = fx.corpus[..DIM].to_vec();
    let plain = fx
        .stream
        .search(tonic::Request::new(SearchRequest {
            request_id: String::new(),
            k: 5,
            vector: query.clone(),
            collapse_parents: false,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(plain.groups.is_empty());
    assert_eq!(plain.chunk_floor, 0.0);
    assert!(plain.hits.iter().all(|h| h.parent_id == 0));
    fx.shutdown().await;
}

/// A vector-only shard (no doc store, no lineage) degrades to tagged
/// self-parents: every chunk is its own parent, groups are singletons,
/// and the mode stays exact rather than failing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lineage_free_shards_self_parent() {
    let corpus = unit_vectors(8, DIM, 0x5E1F_0001);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let (addr, handle) = start_empty_node(NodeConfig {
        slot_offset: 0,
        ..Default::default()
    })
    .await;
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    client
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: 4,
            shift,
            scale,
        })
        .await
        .unwrap();
    let (tx, rx) = mpsc::channel(4);
    tx.send(AddVectorsRequest {
        vectors: corpus.clone(),
        dim: DIM as u32,
    })
    .await
    .unwrap();
    drop(tx);
    client.add_vectors(ReceiverStream::new(rx)).await.unwrap();

    let coordinator = CoordinatorServiceImpl::new(vec![addr]).with_stream_search(true);
    let streamed = document_search(&coordinator, &corpus[..DIM], 3).await;
    assert_eq!(streamed.hits.len(), 3);
    const SELF_PARENT_TAG: u64 = 1 << 63;
    for (hit, group) in streamed.hits.iter().zip(&streamed.groups) {
        assert_eq!(hit.parent_id, SELF_PARENT_TAG | hit.vector_id);
        assert_eq!(group.chunks.len(), 1);
        assert_eq!(group.chunks[0].vector_id, hit.vector_id);
    }
    handle.abort();
}
