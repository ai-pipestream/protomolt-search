//! Two equivalences. A chunked scan under a word-packed bitmap allowlist
//! is the scan under the same allowlist as one flag per row. A replica
//! caught up through coalesced append streams holds the log a record-at-a-time
//! catch-up writes, while keyed rows keep one stream and their key each.

mod common;

use pipestream_search::boolean_bits::Bits;
use pipestream_search::chunked::{
    chunked_topk, chunked_topk_batch_observed, chunked_topk_collapsed, BatchQuery, ChunkedScan,
    ScanStats,
};
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::pb::wal::{wal_record::Op, WalRecord};
use pipestream_search::pb::{self, node_service_client::NodeServiceClient};
use pipestream_search::replication::{sync_once, ReplicaCursor};
use pipestream_search::vector::{Mask, VectorIndex, EMBEDDED_TURBOVEC};
use prost::Message;
use std::sync::{Arc, Mutex};

const DIM: usize = 64;
const ROWS: usize = 5_003;

fn index(rows: usize) -> VectorIndex {
    let mut index = VectorIndex::create(EMBEDDED_TURBOVEC, DIM, 4).unwrap();
    index
        .add(&common::unit_vectors(rows, DIM, 0xB17_0001), DIM)
        .unwrap();
    index
}

/// Allowlists that leave whole chunks, whole blocks and whole words empty,
/// plus the dense, full and empty ones.
fn allowlists(rows: usize) -> Vec<Vec<bool>> {
    let mut state = 0x5EED_B175_u64;
    let mut random = |keep: u64| -> Vec<bool> {
        (0..rows)
            .map(|_| {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (state >> 33).is_multiple_of(keep)
            })
            .collect()
    };
    let mut clustered = vec![false; rows];
    for slot in [0, 31, 32, 63, 64, 65, 2_047, 2_048, 4_999, rows - 1] {
        clustered[slot] = true;
    }
    vec![
        random(2),
        random(7),
        random(97),
        clustered,
        vec![true; rows],
        vec![false; rows],
    ]
}

fn timeless(mut stats: ScanStats) -> ScanStats {
    stats.scan_nanos = 0;
    stats
}

#[test]
fn a_bitmap_span_reads_like_the_flags_it_packs() {
    for rows in [1, 63, 64, 65, 1_000, 4_097] {
        for flags in allowlists(rows.max(5_000)) {
            let flags = &flags[..rows];
            let bits = Bits::from_bools(flags);
            let (packed, plain) = (Mask::Bits(&bits), Mask::Bools(flags));
            for (start, end) in [
                (0, rows),
                (0, rows / 2),
                (rows / 3, rows),
                (rows / 2, rows / 2),
                (rows / 2, rows + 65),
                (rows, rows / 2),
            ] {
                assert_eq!(packed.any_in(start, end), plain.any_in(start, end));
                let (mut from_bits, mut from_flags) = (Vec::new(), Vec::new());
                packed.for_each_in(start, end, |row| from_bits.push(row));
                plain.for_each_in(start, end, |row| from_flags.push(row));
                assert_eq!(from_bits, from_flags, "rows {rows} span {start}..{end}");
                let clamped_end = end.min(rows);
                let expected: Vec<usize> = (start.min(clamped_end)..clamped_end)
                    .filter(|&row| flags[row])
                    .collect();
                assert_eq!(from_bits, expected);
            }
        }
        let mut every = Vec::new();
        Mask::All(rows).for_each_in(0, rows, |row| every.push(row));
        assert_eq!(every, (0..rows).collect::<Vec<_>>());
        assert!(Mask::All(rows).any_in(0, rows) && !Mask::All(rows).any_in(rows, rows));
    }
}

#[test]
fn a_bitmap_allowlist_scans_exactly_like_its_flags() {
    let index = index(ROWS);
    let query = common::unit_vectors(1, DIM, 0xB17_0002);
    for flags in allowlists(ROWS) {
        let bits = Bits::from_bools(&flags);
        for (chunk_blocks, k, keep_ties) in [(1, 10, false), (3, 200, true), (64, 1, false)] {
            let run = |allow: Mask<'_>| {
                let mut floors = Vec::new();
                let scan = ChunkedScan {
                    index: &index,
                    query: &query,
                    k,
                    chunk_blocks,
                    allow: Some(allow),
                };
                let (hits, stats) = chunked_topk(scan, keep_ties, &mut || None, &mut |floor| {
                    floors.push(floor);
                    true
                });
                (hits, timeless(stats), floors)
            };
            let packed = run(Mask::Bits(&bits));
            assert_eq!(
                packed,
                run(Mask::Bools(&flags)),
                "chunk_blocks {chunk_blocks}"
            );
        }
    }
}

#[test]
fn a_bitmap_batch_shares_passes_exactly_like_its_flags() {
    let index = index(ROWS);
    let lists = allowlists(ROWS);
    let bits: Vec<Bits> = lists.iter().map(|flags| Bits::from_bools(flags)).collect();
    let vectors = common::unit_vectors(4, DIM, 0xB17_0003);
    let run = |packed: bool| {
        let allow = |list: usize| {
            Some(if packed {
                Mask::Bits(&bits[list])
            } else {
                Mask::Bools(&lists[list])
            })
        };
        let allows = [allow(1), None, allow(1), allow(2)];
        let queries: Vec<BatchQuery> = (0..4)
            .map(|qi| BatchQuery {
                vector: &vectors[qi * DIM..(qi + 1) * DIM],
                k: 5 + 40 * qi,
                keep_ties: qi == 3,
                allow: allows[qi],
                enumerate_max_rows: None,
                patience: None,
                floor_prune: None,
            })
            .collect();
        let mut passes = Vec::new();
        let results = chunked_topk_batch_observed(
            &index,
            &queries,
            2,
            &mut |_| None,
            &mut |_, _| true,
            &mut |pass| passes.push(pass.bytes),
        );
        let results: Vec<_> = results
            .into_iter()
            .map(|(hits, stats)| (hits, timeless(stats)))
            .collect();
        (results, passes)
    };
    assert_eq!(run(true), run(false));
}

#[test]
fn a_collapsed_scan_under_a_bitmap_matches_its_flags() {
    let index = index(ROWS);
    let query = common::unit_vectors(1, DIM, 0xB17_0004);
    let parents: Vec<u64> = (0..ROWS as u64).map(|slot| slot / 9).collect();
    for flags in allowlists(ROWS) {
        let bits = Bits::from_bools(&flags);
        let scan = |allow| ChunkedScan {
            index: &index,
            query: &query,
            k: 25,
            chunk_blocks: 4,
            allow: Some(allow),
        };
        let run =
            |allow| chunked_topk_collapsed(scan(allow), &parents, &mut || None, &mut |_| true);
        let (packed, packed_stats) = run(Mask::Bits(&bits));
        let (plain, plain_stats) = run(Mask::Bools(&flags));
        assert_eq!(packed, plain);
        assert_eq!(timeless(packed_stats), timeless(plain_stats));
    }
}

#[test]
fn competitive_chunks_merge_through_two_buffers_without_changing_the_heap() {
    let index = index(ROWS);
    let query = common::unit_vectors(1, DIM, 0xB17_0005);
    for (k, keep_ties) in [(1_000, false), (1_000, true), (ROWS, false)] {
        let scan = |chunk_blocks| ChunkedScan {
            index: &index,
            query: &query,
            k,
            chunk_blocks,
            allow: None,
        };
        let whole = chunked_topk(scan(10_000), keep_ties, &mut || None, &mut |_| false).0;
        let chunked = chunked_topk(scan(1), keep_ties, &mut || None, &mut |_| false).0;
        assert_eq!(chunked, whole, "k {k} keep_ties {keep_ties}");
    }
}

/// A node that records which append RPCs reach it, in order.
#[derive(Clone)]
struct CountingReceiver {
    node: NodeServiceImpl,
    appends: Arc<Mutex<Vec<&'static str>>>,
}
impl tonic::server::NamedService for CountingReceiver {
    const NAME: &'static str = "ai.protomolt.search.v1.NodeService";
}
impl<B> tonic::codegen::Service<tonic::codegen::http::Request<B>> for CountingReceiver
where
    B: http_body::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = tonic::codegen::http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: tonic::codegen::http::Request<B>) -> Self::Future {
        for name in ["AddDocuments", "AddVectors"] {
            if request.uri().path().ends_with(&format!("/{name}")) {
                self.appends.lock().unwrap().push(name);
            }
        }
        let node = self.node.clone();
        Box::pin(pb::node_service_server::NodeServiceServer::new(node).call(request))
    }
}

/// A primary and a counting replica over fresh WAL-backed shards.
struct Pair {
    cursor: ReplicaCursor,
    source: NodeServiceClient<tonic::transport::Channel>,
    appends: Arc<Mutex<Vec<&'static str>>>,
}

impl Pair {
    async fn start(tag: &str, vectors: bool) -> Self {
        let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("replication-batch-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let config = |name: &str| NodeConfig {
            index_path: Some(root.join(format!("{name}.tv"))),
            wal: true,
            analysis_addr: Some(pipestream_search::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
            ..Default::default()
        };
        let (primary, _) = common::start_empty_node(config("source")).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let replica = format!("http://{}", listener.local_addr().unwrap());
        let appends = Arc::new(Mutex::new(Vec::new()));
        let receiver = CountingReceiver {
            node: NodeServiceImpl::new(None, config("receiver")),
            appends: appends.clone(),
        };
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(receiver)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        let sample = common::unit_vectors(64, DIM, 0xB17_0006);
        let (shift, scale) = common::fit_calibration(DIM, 4, &sample);
        for addr in [&primary, &replica].into_iter().filter(|_| vectors) {
            NodeServiceClient::connect(addr.clone())
                .await
                .unwrap()
                .configure_vector_backend(common::embedded_backend_request(DIM, 4, &shift, &scale))
                .await
                .unwrap();
        }
        Self {
            source: NodeServiceClient::connect(primary.clone()).await.unwrap(),
            cursor: ReplicaCursor {
                primary,
                replica,
                ..Default::default()
            },
            appends,
        }
    }

    /// Append `texts` to the primary, one logged record per document; a
    /// `key` sends each document in its own stream under that stable key.
    async fn documents(&mut self, texts: std::ops::Range<usize>, key: Option<&[u8]>) {
        let documents = texts.map(|i| pb::AddDocumentsRequest {
            text: format!("document {i} of the replicated log"),
            analysis: Some(pipestream_search::analyzer::body_spec()),
            ..Default::default()
        });
        let Some(key) = key else {
            let stream = tokio_stream::iter(documents.collect::<Vec<_>>());
            self.source.add_documents(stream).await.unwrap();
            return;
        };
        for document in documents {
            let mut request = tonic::Request::new(tokio_stream::iter([document]));
            request.metadata_mut().insert_bin(
                "x-protomolt-stable-key-bin",
                tonic::metadata::MetadataValue::from_bytes(key),
            );
            self.source.add_documents(request).await.unwrap();
        }
    }

    /// Append one logged record of `rows` vectors to the primary.
    async fn vectors(&mut self, rows: usize, seed: u64) {
        let batch = pb::AddVectorsRequest {
            materialized_rows: Vec::new(),
            vectors: common::unit_vectors(rows, DIM, seed),
            dim: DIM as u32,
        };
        let stream = tokio_stream::iter([batch]);
        self.source.add_vectors(stream).await.unwrap();
    }

    /// Flush the primary, catch the replica up from `from`, and hold the
    /// replica's append log to the primary's, record for record.
    async fn sync(&mut self, from: &ReplicaCursor) -> ReplicaCursor {
        self.source.flush(pb::FlushRequest {}).await.unwrap();
        let advanced = sync_once(from).await.unwrap();
        let logged = appended(&self.cursor.primary).await;
        assert!(!logged.is_empty());
        assert_eq!(appended(&self.cursor.replica).await, logged);
        advanced
    }

    fn appends(&self) -> Vec<&'static str> {
        self.appends.lock().unwrap().clone()
    }
}

/// The append records of a node's flushed WAL, in log order.
async fn appended(addr: &str) -> Vec<Op> {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let health = client.health(pb::HealthRequest {}).await.unwrap();
    let request = pb::ReadWalRequest {
        generation: health.into_inner().wal_generation,
        after_clock: 0,
    };
    let mut stream = client.read_wal(request).await.unwrap().into_inner();
    let mut ops = Vec::new();
    while let Some(frame) = stream.message().await.unwrap() {
        if frame.completed {
            continue;
        }
        let record = WalRecord::decode(frame.record.as_slice()).unwrap();
        if let Some(op @ (Op::AddDocuments(_) | Op::AddVectors(_))) = record.op {
            ops.push(op);
        }
    }
    ops
}

#[tokio::test]
async fn unkeyed_document_records_share_streams_and_a_retry_sends_nothing() {
    let mut pair = Pair::start("unkeyed", false).await;
    pair.documents(0..530, None).await;
    let start = pair.cursor.clone();
    let advanced = pair.sync(&start).await;
    assert_eq!(pair.appends(), ["AddDocuments", "AddDocuments"]);
    // A cursor lost after the replica flushed: every row is already there.
    assert!(pair.sync(&start).await.clock >= advanced.clock);
    assert_eq!(pair.appends().len(), 2);
    pair.documents(530..540, None).await;
    assert!(pair.sync(&start).await.clock > advanced.clock);
    assert_eq!(pair.appends().len(), 3);
}

#[tokio::test]
async fn keyed_rows_keep_one_stream_and_their_key_between_unkeyed_runs() {
    let mut pair = Pair::start("keyed", false).await;
    pair.documents(0..3, None).await;
    pair.documents(3..5, Some(b"stable-key")).await;
    pair.documents(5..8, None).await;
    let start = pair.cursor.clone();
    pair.sync(&start).await;
    assert_eq!(pair.appends().len(), 4, "one run, two keyed rows, one run");
    let keys: Vec<Vec<Vec<u8>>> = appended(&pair.cursor.replica)
        .await
        .into_iter()
        .map(|op| match op {
            Op::AddDocuments(add) => add.stable_routing_keys,
            other => panic!("unexpected record {other:?}"),
        })
        .collect();
    let keyed = vec![b"stable-key".to_vec()];
    let expected = [
        vec![],
        vec![],
        vec![],
        keyed.clone(),
        keyed,
        vec![],
        vec![],
        vec![],
    ];
    assert_eq!(keys, expected);
}

#[tokio::test]
async fn unkeyed_vector_records_share_one_stream_and_keep_their_records() {
    let mut pair = Pair::start("vectors", true).await;
    for seed in 0..3 {
        pair.vectors(4, 0xB17_0100 + seed).await;
    }
    let start = pair.cursor.clone();
    let advanced = pair.sync(&start).await;
    assert_eq!(pair.appends(), ["AddVectors"]);
    pair.vectors(2, 0xB17_0200).await;
    assert!(pair.sync(&start).await.clock > advanced.clock);
    assert_eq!(pair.appends(), ["AddVectors", "AddVectors"]);
}
