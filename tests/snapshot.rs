//! Acceptance tests for `NodeService.InstallSnapshot`: push a centrally
//! built shard image over one gRPC stream, atomically swap its generation
//! directory into place, and serve from it — with the
//! calibration-comparability and byte-accounting guards intact, and the
//! crash-recovery rules for interrupted swaps.

mod common;

use pipestream_search::node::{
    generation_bm25, generation_dir, generation_exact_vectors, generation_vector,
    recover_generation, Bm25Shard, NodeConfig, NodeServiceImpl,
};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::{
    search_shard_request, search_shard_response, snapshot_chunk, ExactVectorRescoreRequest,
    FlushRequest, GetCalibrationRequest, GetDocumentsRequest, HealthRequest, ScoredHit,
    SearchShardRequest, SetCalibrationRequest, SnapshotChunk, SnapshotManifest, StartShardSearch,
};
use pipestream_search::postings::{AnalyzedDoc, Bm25Store, DocTerms};
use pipestream_search::vector::{legacy_calibration_config, VectorIndex, EMBEDDED_TURBOVEC};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use common::{fit_calibration, monolithic_topk, unit_vectors, BIT_WIDTH, DIM};

const N: usize = 512;

fn tempdir(tag: &str) -> std::path::PathBuf {
    // CARGO_TARGET_TMPDIR lives under target/ (a real disk), not the
    // tmpfs /tmp — index files in tests must not consume RAM.
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("turbovec_snap_{tag}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Build an index over the first `n` vectors of `corpus`, seeded with the
/// given calibration, and persist it as a snapshot source image.
fn build_image(
    corpus: &[f32],
    n: usize,
    shift: &[f32],
    scale: &[f32],
    path: &std::path::Path,
) -> VectorIndex {
    let mut index = pipestream_search::harness::seeded_index(DIM, BIT_WIDTH, shift, scale);
    index.add(&corpus[..n * DIM], DIM).unwrap();
    index.prepare().unwrap();
    index.write(path).unwrap();
    index
}

/// Build the reference corpus index (seeded calibration) and persist it
/// as the snapshot source image.
fn build_source(dir: &std::path::Path) -> (VectorIndex, Vec<f32>, std::path::PathBuf) {
    let corpus = unit_vectors(N, DIM, 0x5EED_CA11);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &corpus[..DIM * 2_000.min(N)]);
    let tv = dir.join("source.tv");
    let index = build_image(&corpus, N, &shift, &scale, &tv);
    (index, corpus, tv)
}

fn build_exact_source(dir: &std::path::Path, corpus: &[f32], n: usize) -> std::path::PathBuf {
    let path = dir.join("source.exact");
    pipestream_search::exact_vectors::ExactVectorStore::from_values(
        DIM,
        corpus[..n * DIM].to_vec(),
    )
    .unwrap()
    .write(&path)
    .unwrap();
    path
}

fn legacy_calibration(index: &VectorIndex) -> (Vec<f32>, Vec<f32>) {
    let config = index.backend_config().unwrap();
    let legacy = legacy_calibration_config(&config).unwrap().unwrap();
    (legacy.shift, legacy.scale)
}

/// A one-document BM25 image containing the given text.
fn build_bm25(dir: &std::path::Path, text: &str) -> std::path::PathBuf {
    let mut store = Bm25Store::new();
    let mut terms: DocTerms = Vec::new();
    let mut offset = 0u32;
    for token in text.split_whitespace() {
        let start = text[offset as usize..].find(token).unwrap() as u32 + offset;
        terms.push((
            token.to_string(),
            1,
            vec![(start, start + token.len() as u32)],
        ));
        offset = start + token.len() as u32;
    }
    let length = terms.iter().map(|(_, f, _)| f).sum();
    store.add_document_with_lineage(0, text.to_string(), AnalyzedDoc::body(terms, length), None);
    let path = dir.join("source.tv.bm25");
    store.save(&path).unwrap();
    path
}

async fn seed(
    client: &mut NodeServiceClient<tonic::transport::Channel>,
    shift: &[f32],
    scale: &[f32],
) {
    client
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: BIT_WIDTH as u32,
            shift: shift.to_vec(),
            scale: scale.to_vec(),
        })
        .await
        .unwrap();
}

async fn search_topk(
    client: &mut NodeServiceClient<tonic::transport::Channel>,
    vector: Vec<f32>,
    k: u32,
) -> Vec<ScoredHit> {
    let (tx, rx) = mpsc::channel(8);
    tx.send(SearchShardRequest {
        payload: Some(search_shard_request::Payload::Start(StartShardSearch {
            request_id: "snap-test".into(),
            k,
            vector,
            tie_complete: false,
            collapse_parents: false,
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    drop(tx);
    let mut stream = client
        .search_shard(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    let mut done = None;
    while let Some(msg) = stream.message().await.unwrap() {
        if let Some(search_shard_response::Payload::Done(d)) = msg.payload {
            done = Some(d);
        }
    }
    done.expect("stream must end with SearchShardDone").hits
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seeded_install_serves_and_persists() {
    let dir = tempdir("seeded");
    let (reference, corpus, src_tv) = build_source(&dir);
    let src_exact = build_exact_source(&dir, &corpus, N);
    let (shift, scale) = legacy_calibration(&reference);
    let src_bm25 = build_bm25(&dir, "rust search engines");

    let node_tv = dir.join("shard.tv");
    let (addr, handle) = common::start_empty_node(NodeConfig {
        slot_offset: 100,
        index_path: Some(node_tv.clone()),
        ..Default::default()
    })
    .await;
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();

    // Seed the shard with the same calibration, then install the image.
    seed(&mut client, &shift, &scale).await;
    let report = pipestream_search::snapshot::install_snapshot_with_exact(
        &addr,
        &src_tv,
        Some(&src_exact),
        Some(&src_bm25),
    )
    .await
    .unwrap();
    assert_eq!(report.num_vectors, N as u64);
    assert_eq!(report.num_documents, 1);
    let gen = generation_dir(&node_tv);
    assert_eq!(report.path, generation_vector(&gen).display().to_string());

    // The installed shard serves exactly the monolithic reference.
    let query: Vec<f32> = corpus[..DIM].to_vec();
    let hits = search_topk(&mut client, query.clone(), 10).await;
    let expected = monolithic_topk(&reference, &query, 10);
    assert_eq!(hits.len(), expected.len());
    for (hit, (id, score_bits)) in hits.iter().zip(expected) {
        // Global id = slot_offset + local slot.
        assert_eq!(hit.vector_id, 100 + id);
        assert_eq!(hit.score.to_bits(), score_bits);
    }

    // The BM25 sidecar serves too: raw text comes back by global id.
    let docs = client
        .get_documents(GetDocumentsRequest { doc_ids: vec![100] })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(docs.documents.len(), 1);
    assert_eq!(docs.documents[0].text, "rust search engines");

    // Persistence without Flush: the generation holds both files, and the
    // legacy layout was never written.
    assert!(generation_vector(&gen).exists());
    assert!(generation_exact_vectors(&gen).exists());
    assert!(generation_bm25(&gen).exists());
    assert!(!node_tv.exists());
    assert_eq!(recover_generation(&node_tv), Some(gen));

    let health = client.health(HealthRequest {}).await.unwrap().into_inner();
    assert!(health.exact_vectors_available);
    assert!(health.exact_vectors_mmap);
    assert_eq!(health.exact_vector_rows, N as u64);
    let exact = client
        .exact_vector_rescore(ExactVectorRescoreRequest {
            vector: query.clone(),
            candidate_ids: vec![100, 101],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(exact.hits.len(), 2);
    let expected_dot: f32 = query.iter().map(|value| value * value).sum();
    assert_eq!(exact.hits[0].doc_id, 100);
    assert_eq!(exact.hits[0].score.to_bits(), expected_dot.to_bits());

    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_mismatched_calibration() {
    let dir = tempdir("mismatch");
    let (_reference, _corpus, src_tv) = build_source(&dir);
    // A DIFFERENT calibration than the image was built with.
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &unit_vectors(2_000, DIM, 0xBAD5_EED0));

    let node_tv = dir.join("shard.tv");
    let (addr, handle) = common::start_empty_node(NodeConfig {
        index_path: Some(node_tv.clone()),
        ..Default::default()
    })
    .await;
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    seed(&mut client, &shift, &scale).await;

    let err = pipestream_search::snapshot::install_snapshot(&addr, &src_tv, None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);

    // Nothing installed: shard still empty, no generation, no staging dir.
    let cal = client
        .get_calibration(GetCalibrationRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(cal.num_vectors, 0);
    assert!(!generation_dir(&node_tv).exists());
    assert!(!dir.join("shard.tv.snap-tmp").exists());

    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unseeded_node_adopts_snapshot_calibration() {
    let dir = tempdir("adopt");
    let (reference, corpus, src_tv) = build_source(&dir);
    let (shift, scale) = legacy_calibration(&reference);

    let node_tv = dir.join("shard.tv");
    let (addr, handle) = common::start_empty_node(NodeConfig {
        index_path: Some(node_tv),
        ..Default::default()
    })
    .await;
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();

    // No SetCalibration: the shard adopts whatever the image carries.
    let report = pipestream_search::snapshot::install_snapshot(&addr, &src_tv, None)
        .await
        .unwrap();
    assert_eq!(report.num_vectors, N as u64);
    assert_eq!(report.num_documents, 0);

    let cal = client
        .get_calibration(GetCalibrationRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(cal.dim, DIM as u32);
    assert_eq!(cal.bit_width, BIT_WIDTH as u32);
    assert_eq!(cal.shift, shift);
    assert_eq!(cal.scale, scale);

    let query: Vec<f32> = corpus[..DIM].to_vec();
    let hits = search_topk(&mut client, query.clone(), 5).await;
    let expected = monolithic_topk(&reference, &query, 5);
    assert_eq!(hits.len(), expected.len());
    let health = client.health(HealthRequest {}).await.unwrap().into_inner();
    assert!(!health.exact_vectors_available);
    let err = client
        .exact_vector_rescore(ExactVectorRescoreRequest {
            vector: query,
            candidate_ids: vec![0],
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("no exact-vector sidecar"));

    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn truncated_stream_rejected() {
    let dir = tempdir("truncated");
    let (_reference, _corpus, src_tv) = build_source(&dir);
    let bytes = std::fs::read(&src_tv).unwrap();

    let node_tv = dir.join("shard.tv");
    let (addr, handle) = common::start_empty_node(NodeConfig {
        index_path: Some(node_tv.clone()),
        ..Default::default()
    })
    .await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();

    // Declare 64 more bytes than the stream carries.
    let (tx, rx) = mpsc::channel(4);
    tx.send(SnapshotChunk {
        payload: Some(snapshot_chunk::Payload::Manifest(SnapshotManifest {
            vector_bytes: bytes.len() as u64 + 64,
            bm25_bytes: 0,
            exact_vector_bytes: 0,
        })),
    })
    .await
    .unwrap();
    tx.send(SnapshotChunk {
        payload: Some(snapshot_chunk::Payload::Data(bytes)),
    })
    .await
    .unwrap();
    drop(tx);

    let err = client
        .install_snapshot(ReceiverStream::new(rx))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(!generation_dir(&node_tv).exists());
    assert!(!dir.join("shard.tv.snap-tmp").exists());

    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replace_generation_swaps_and_survives_restart() {
    let dir = tempdir("replace");
    // Two images over one corpus prefix, same calibration: A has 512
    // vectors, B has 640 (a superset — a rebuild with more data).
    let corpus = unit_vectors(640, DIM, 0x5EED_CA12);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &corpus[..DIM * 512]);
    let tv_a = dir.join("a.tv");
    build_image(&corpus, 512, &shift, &scale, &tv_a);
    let reference_b = build_image(&corpus, 640, &shift, &scale, &dir.join("b.tv"));
    let tv_b = dir.join("b.tv");
    let bm25_b = build_bm25(&dir, "the law of vectors");

    let node_tv = dir.join("shard.tv");
    let (addr, handle) = common::start_empty_node(NodeConfig {
        slot_offset: 0,
        index_path: Some(node_tv.clone()),
        ..Default::default()
    })
    .await;
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    seed(&mut client, &shift, &scale).await;

    // Install A, then wholesale-replace with B.
    let report_a = pipestream_search::snapshot::install_snapshot(&addr, &tv_a, None)
        .await
        .unwrap();
    assert_eq!(report_a.num_vectors, 512);
    let report_b = pipestream_search::snapshot::install_snapshot(&addr, &tv_b, Some(&bm25_b))
        .await
        .unwrap();
    assert_eq!(report_b.num_vectors, 640);
    assert_eq!(report_b.num_documents, 1);

    // Exactly one generation, no swap-out dir left behind.
    let gen = generation_dir(&node_tv);
    assert!(generation_vector(&gen).exists());
    assert!(generation_bm25(&gen).exists());
    assert!(!dir.join("shard.tv.snap-old").exists());
    assert!(!dir.join("shard.tv.snap-tmp").exists());

    // The shard serves B.
    let query: Vec<f32> = corpus[..DIM].to_vec();
    let hits = search_topk(&mut client, query.clone(), 10).await;
    let expected = monolithic_topk(&reference_b, &query, 10);
    assert_eq!(hits.len(), expected.len());
    handle.abort();

    // Simulated restart: recover the generation and serve from disk, no
    // in-memory state carried over.
    let recovered = recover_generation(&node_tv).expect("generation survives");
    let mut index = VectorIndex::load(EMBEDDED_TURBOVEC, &generation_vector(&recovered)).unwrap();
    index.prepare().unwrap();
    assert_eq!(index.len(), 640);
    let bm25 = Bm25Shard::open(&generation_bm25(&recovered)).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = format!("http://{}", listener.local_addr().unwrap());
    let service = NodeServiceImpl::new(
        Some(index),
        NodeConfig {
            slot_offset: 0,
            index_path: Some(node_tv.clone()),
            ..Default::default()
        },
    )
    .with_bm25(Some(bm25))
    .with_generation(Some(recovered));
    let handle2 = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service.into_server(pipestream_search::MAX_MESSAGE_BYTES))
            .serve_with_incoming(pipestream_search::harness::nodelay_incoming(listener)),
    );
    let mut client2 = NodeServiceClient::connect(addr2).await.unwrap();
    let hits2 = search_topk(&mut client2, query.clone(), 10).await;
    assert_eq!(hits2.len(), expected.len());
    for (hit, (id, score_bits)) in hits2.iter().zip(expected) {
        assert_eq!(hit.vector_id, id);
        assert_eq!(hit.score.to_bits(), score_bits);
    }
    let docs = client2
        .get_documents(GetDocumentsRequest { doc_ids: vec![0] })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(docs.documents[0].text, "the law of vectors");

    handle2.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_after_install_writes_into_generation() {
    let dir = tempdir("flush");
    let (reference, _corpus, src_tv) = build_source(&dir);
    let (shift, scale) = legacy_calibration(&reference);

    let node_tv = dir.join("shard.tv");
    let (addr, handle) = common::start_empty_node(NodeConfig {
        index_path: Some(node_tv.clone()),
        ..Default::default()
    })
    .await;
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    seed(&mut client, &shift, &scale).await;
    pipestream_search::snapshot::install_snapshot(&addr, &src_tv, None)
        .await
        .unwrap();

    // Flush after a snapshot install writes INSIDE the generation — the
    // legacy layout never appears.
    let flushed = client.flush(FlushRequest {}).await.unwrap().into_inner();
    let gen = generation_dir(&node_tv);
    assert_eq!(flushed.path, generation_vector(&gen).display().to_string());
    assert!(flushed.written);
    assert!(generation_vector(&gen).exists());
    assert!(!node_tv.exists());
    let reloaded = VectorIndex::load(EMBEDDED_TURBOVEC, &generation_vector(&gen)).unwrap();
    assert_eq!(reloaded.len(), N);

    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn recover_generation_repairs_interrupted_swaps() {
    let dir = tempdir("recovery");
    let index_path = dir.join("shard.tv");
    let snap = generation_dir(&index_path);
    let old = dir.join("shard.tv.snap-old");
    let tmp = dir.join("shard.tv.snap-tmp");

    // Nothing on disk: no generation.
    assert_eq!(recover_generation(&index_path), None);

    // A stray staging dir is always removed (only complete staging dirs
    // are ever renamed into place).
    std::fs::create_dir_all(&tmp).unwrap();
    assert_eq!(recover_generation(&index_path), None);
    assert!(!tmp.exists());

    // Crashed between the swap renames: snap-old without snap is the
    // whole previous generation — rename it back.
    std::fs::create_dir_all(&old).unwrap();
    std::fs::write(old.join("index.tv"), b"x").unwrap();
    assert_eq!(recover_generation(&index_path), Some(snap.clone()));
    assert!(generation_vector(&snap).exists());
    assert!(!old.exists());

    // Crashed after the second rename: both present — the new generation
    // is live, the old one goes away.
    std::fs::create_dir_all(&old).unwrap();
    assert_eq!(recover_generation(&index_path), Some(snap.clone()));
    assert!(!old.exists());

    let _ = std::fs::remove_dir_all(&dir);
}
