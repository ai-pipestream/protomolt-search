//! Integration test for the write-ahead log and offline resharding:
//! ingest a deterministic corpus into a live loopback node over the real
//! AddVectors/AddDocuments RPCs (WAL on), flush, then replay the log.
//!
//! Asserted invariants:
//!
//! - SPLIT: the union of the children's per-child top-k equals the
//!   parent's top-k EXACTLY — same vectors plus byte-identical
//!   calibration give bitwise-identical scores, and a child's local
//!   top-k always contains every global-top-k member it owns (the same
//!   argument as the distributed lossless invariant), so merging the
//!   child lists by (score, id) reconstructs the parent's list. Ids are
//!   compared after mapping child local slots back to parent global ids
//!   via `ChildImage::parent_ids`.
//! - MERGE: replaying two shards' logs in id order reproduces the
//!   monolithic index over the full corpus (same exact-top-k argument),
//!   and a BM25 query against the merged image returns the same doc set
//!   (same ids, bitwise same scores) as against a reference store built
//!   from the same documents — documents are re-analyzed with the same
//!   options, so term identity and doc lengths are identical by
//!   construction.

mod common;

use std::path::{Path, PathBuf};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Error as TransportError;
use turbovec::TurboQuantIndex;
use turbovec_search::bm25::{self, CorpusStats};
use turbovec_search::harness::{build_monolithic, mock_analysis};
use turbovec_search::node::NodeConfig;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::{
    AddDocumentsRequest, AddVectorsRequest, AnalysisSpec, FlushRequest, SetCalibrationRequest,
};
use turbovec_search::postings::{AnalyzedDoc, Bm25Index, Bm25Reader, Bm25Store};
use turbovec_search::{analyzer, reshard};

use common::{fit_calibration, unit_vectors, BIT_WIDTH, DIM};

const N: usize = 3_000;
const DOCS: usize = 1_500;

fn tempdir(tag: &str) -> PathBuf {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "turbovec_reshard_{tag}_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Deterministic document text with shared terms (so BM25 has something
/// to rank) and enough variety to keep doc lengths unequal.
fn doc_text(i: usize) -> String {
    format!(
        "alpha {} {}",
        ["gamma", "delta", "epsilon", "zeta"][i % 4],
        ["one", "two", "three"][i % 3]
    )
}

/// Start an empty loopback node with persistence + WAL + the mock
/// analysis sidecar, seed it with the calibration, and return its client,
/// index path, and server handle (dropping the handle aborts the server).
async fn start_wal_node(
    dir: &Path,
    name: &str,
    slot_offset: u64,
    wal_buckets: u32,
    analysis_addr: &str,
    shift: &[f32],
    scale: &[f32],
) -> (
    NodeServiceClient<tonic::transport::Channel>,
    PathBuf,
    JoinHandle<Result<(), TransportError>>,
) {
    let index_path = dir.join(name);
    let (addr, handle) = common::start_empty_node(NodeConfig {
        slot_offset,
        index_path: Some(index_path.clone()),
        analysis_addr: Some(analysis_addr.to_string()),
        wal: true,
        wal_buckets,
        ..Default::default()
    })
    .await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    client
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: BIT_WIDTH as u32,
            shift: shift.to_vec(),
            scale: scale.to_vec(),
        })
        .await
        .unwrap();
    (client, index_path, handle)
}

/// Ingest documents first (ids 0..docs), then vectors (ids 0..vecs), so
/// the two sides share the aligned positional id space the court ingest
/// path uses; then flush.
async fn ingest(
    client: &mut NodeServiceClient<tonic::transport::Channel>,
    corpus: &[f32],
    vectors: usize,
    docs: usize,
) {
    let (tx, rx) = mpsc::channel(8);
    tokio::spawn(async move {
        for i in 0..docs {
            tx.send(AddDocumentsRequest {
                text: doc_text(i),
                analysis: None,
                lineage: None,
            })
            .await
            .unwrap();
        }
    });
    let resp = client.add_documents(ReceiverStream::new(rx)).await.unwrap();
    assert_eq!(resp.into_inner().added as usize, docs);

    let (tx, rx) = mpsc::channel(8);
    let corpus = corpus.to_vec();
    tokio::spawn(async move {
        for chunk in corpus[..vectors * DIM].chunks(500 * DIM) {
            tx.send(AddVectorsRequest {
                vectors: chunk.to_vec(),
                dim: 0,
            })
            .await
            .unwrap();
        }
    });
    let resp = client.add_vectors(ReceiverStream::new(rx)).await.unwrap();
    assert_eq!(resp.into_inner().added as usize, vectors);

    let flushed = client.flush(FlushRequest {}).await.unwrap().into_inner();
    assert!(flushed.written);
}

/// The batch analyzer closure for reshard replay: the same mock sidecar
/// the node ingested through, bridged into the sync core (sequential
/// within a batch — test corpora are small).
fn replay_analyzer(
    analysis_addr: &str,
) -> impl FnMut(&[(&str, Option<&AnalysisSpec>)]) -> Result<Vec<AnalyzedDoc>, String> + '_ {
    let handle = tokio::runtime::Handle::current();
    move |docs| {
        tokio::task::block_in_place(|| {
            handle.block_on(async {
                let mut out = Vec::with_capacity(docs.len());
                for (text, spec) in docs {
                    out.push(
                        analyzer::analyze_document(analysis_addr, text, *spec)
                            .await
                            .map_err(|e| e.to_string())?,
                    );
                }
                Ok(out)
            })
        })
    }
}

/// Analyze one document through a batch analyzer (test convenience).
fn analyze_one(
    analyze: &mut impl FnMut(&[(&str, Option<&AnalysisSpec>)]) -> Result<Vec<AnalyzedDoc>, String>,
    text: &str,
) -> AnalyzedDoc {
    analyze(&[(text, None)]).unwrap().remove(0)
}

/// Top-k of one query as `(global_id, score_bits)`, coordinator order
/// (score desc, id asc).
fn topk(index: &TurboQuantIndex, query: &[f32], k: usize, id_of: impl Fn(u64) -> u64) -> Vec<(u64, u32)> {
    let results = index.search(query, k);
    let mut hits: Vec<(u64, u32)> = results
        .indices_for_query(0)
        .iter()
        .zip(results.scores_for_query(0))
        .map(|(&i, &s)| (id_of(i as u64), s.to_bits()))
        .collect();
    hits.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    hits
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn split_reconstructs_parent_topk() {
    let dir = tempdir("split");
    let corpus = unit_vectors(N, DIM, 0x5EED_CA11);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &corpus[..DIM * 2_000.min(N)]);
    let (analysis_addr, analysis) = mock_analysis::start_mock_analysis().await;
    std::mem::forget(analysis);

    let (mut client, index_path, _node) =
        start_wal_node(&dir, "shard.tv", 0, 64, &analysis_addr, &shift, &scale).await;
    ingest(&mut client, &corpus, N, DOCS).await;

    // Parent reference: the flushed image.
    let parent = TurboQuantIndex::load(&index_path).unwrap();
    parent.prepare();

    // Split the node's WAL 1 -> 2 (cheap path: 2 <= 64 buckets, each
    // child owns a contiguous bucket range).
    let out_dir = dir.join("split");
    let output = reshard::split(
        &reshard::resolve_gen(&turbovec_search::wal::wal_dir(&index_path)).unwrap(),
        2,
        &out_dir,
        0,
        25_000_000,
        &mut replay_analyzer(&analysis_addr),
    )
    .unwrap();

    assert_eq!(output.generation, 1);
    assert_eq!(output.children.len(), 2);
    let total_vectors: u64 = output.children.iter().map(|c| c.num_vectors).sum();
    let total_docs: u64 = output.children.iter().map(|c| c.num_documents).sum();
    assert_eq!(total_vectors, N as u64);
    assert_eq!(total_docs, DOCS as u64);
    // The hash ranges tile the full u64 space.
    assert_eq!(output.children[0].hash_lo, 0);
    assert_eq!(output.children[1].hash_hi, u64::MAX);
    assert_eq!(output.children[0].hash_hi + 1, output.children[1].hash_lo);
    // Slot offsets follow base + i * stride.
    assert_eq!(output.children[1].slot_offset, 25_000_000);
    // Cheap split: child i owns exactly bucket range [i*32, (i+1)*32).
    for (i, child) in output.children.iter().enumerate() {
        assert!(
            child
                .parent_ids
                .iter()
                .all(|&id| reshard::bucket_of(id, 64) / 32 == i),
            "child {i} holds ids outside its bucket range"
        );
    }

    let children: Vec<(&reshard::ChildImage, TurboQuantIndex)> = output
        .children
        .iter()
        .map(|c| {
            let index = TurboQuantIndex::load(&c.tv_path).unwrap();
            index.prepare();
            (c, index)
        })
        .collect();

    // Union of child top-k == parent top-k, bitwise (see the module docs
    // for the invariant).
    for q in 0..8u64 {
        let query = unit_vectors(1, DIM, 0xB0B0_0000 + q);
        let k = 10;
        let expected = topk(&parent, &query, k, |id| id);
        let mut merged: Vec<(u64, u32)> = children
            .iter()
            .flat_map(|(child, index)| {
                topk(index, &query, k, |local| child.parent_ids[local as usize])
            })
            .collect();
        merged.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        merged.truncate(k);
        assert_eq!(merged, expected, "query {q}: split changed the top-k");
    }
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merge_reproduces_monolithic() {
    let dir = tempdir("merge");
    let corpus = unit_vectors(N, DIM, 0x5EED_CA11);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &corpus[..DIM * 2_000.min(N)]);
    let (analysis_addr, analysis) = mock_analysis::start_mock_analysis().await;
    std::mem::forget(analysis);

    // Two shards, contiguous global id ranges, same calibration.
    let half = N / 2;
    let (mut a, a_path, _node_a) =
        start_wal_node(&dir, "a.tv", 0, 64, &analysis_addr, &shift, &scale).await;
    ingest(&mut a, &corpus[..half * DIM], half, DOCS / 2).await;
    let (mut b, b_path, _node_b) =
        start_wal_node(&dir, "b.tv", half as u64, 64, &analysis_addr, &shift, &scale).await;
    ingest(&mut b, &corpus[half * DIM..], half, DOCS / 2).await;

    let generations = [a_path, b_path]
        .iter()
        .map(|p| reshard::resolve_gen(&turbovec_search::wal::wal_dir(p)).unwrap())
        .collect::<Vec<_>>();
    let out_dir = dir.join("merged");
    let output = reshard::merge(&generations, &out_dir, None, &mut replay_analyzer(&analysis_addr))
        .unwrap();
    assert_eq!(output.children.len(), 1);
    let child = &output.children[0];
    assert_eq!(child.num_vectors, N as u64);
    assert_eq!(child.num_documents, DOCS as u64);
    assert_eq!(child.slot_offset, 0);
    // Contiguous input ranges replay in id order: the remap is identity.
    assert!(child.parent_ids.iter().enumerate().all(|(i, &id)| id == i as u64));

    // Vector side: merged image == monolithic reference, bitwise.
    let reference = build_monolithic(&corpus, DIM, BIT_WIDTH, &shift, &scale);
    let merged = TurboQuantIndex::load(&child.tv_path).unwrap();
    merged.prepare();
    for q in 0..8u64 {
        let query = unit_vectors(1, DIM, 0xB0B0_0000 + q);
        assert_eq!(
            topk(&merged, &query, 10, |id| id),
            topk(&reference, &query, 10, |id| id),
            "query {q}: merge changed the top-k"
        );
    }

    // BM25 side: the merged image's store answers the same doc set as a
    // reference store built from the same documents (ids are the
    // identity here, scores bitwise under identical stats).
    let mut reference_store = Bm25Store::new();
    let mut analyze = replay_analyzer(&analysis_addr);
    for i in 0..DOCS {
        let text = doc_text(i);
        let analyzed = analyze_one(&mut analyze, &text);
        reference_store.add_document(i as u32, text, analyzed);
    }
    let bm25_path = child.bm25_path.as_ref().expect("merged image has docs");
    let merged_store = Bm25Reader::open(bm25_path).unwrap();
    let merged_index = &merged_store as &dyn Bm25Index;
    let terms = vec!["alpha".to_string(), "gamma".to_string()];
    let stats = CorpusStats {
        doc_count: reference_store.doc_count(),
        total_doc_length: reference_store.total_doc_length(),
        dfs: terms.iter().map(|t| reference_store.df(t)).collect(),
    };
    let params = turbovec_search::bm25::Bm25Params::default();
    let expected: Vec<(u32, u64)> = bm25::top_k(&reference_store, &terms, &stats, params, 10)
        .iter()
        .map(|d| (d.doc_id, d.score.to_bits()))
        .collect();
    let got: Vec<(u32, u64)> = bm25::top_k(merged_index, &terms, &stats, params, 10)
        .iter()
        .map(|d| (d.doc_id, d.score.to_bits()))
        .collect();
    assert_eq!(got, expected, "merge changed the BM25 doc set");

    // Merge across calibrations or bucket geometries is rejected. Craft
    // generations by hand: one with a perturbed calibration, one with a
    // different bucket count (cheap proxies for differently-built shards).
    let craft_gen = |name: &str, bucket_count: u32, perturb: bool| {
        let wal_root = dir.join(name);
        let manifest = turbovec_search::wal::WalManifest {
            dim: DIM as u32,
            bit_width: BIT_WIDTH as u32,
            calibration_shift: if perturb {
                shift.iter().map(|x| x + 1.0).collect()
            } else {
                shift.clone()
            },
            calibration_scale: scale.clone(),
            slot_offset: 0,
            generation: 0,
            bucket_bits: bucket_count.trailing_zeros(),
            bucket_count,
            preexisting_vectors: 0,
            preexisting_documents: 0,
            format_version: turbovec_search::wal::FORMAT_VERSION,
        };
        let mut writer = turbovec_search::wal::WalWriter::create(&wal_root, manifest).unwrap();
        writer
            .append(turbovec_search::pb::wal::wal_record::Op::Flush(
                turbovec_search::pb::wal::FlushMarker {},
            ))
            .unwrap();
        writer.flush().unwrap();
        turbovec_search::wal::gen_dir(&wal_root, 0)
    };
    let bad_calibration = craft_gen("other-cal.wal", 64, true);
    let bad = reshard::merge(
        &[generations[0].clone(), bad_calibration],
        &dir.join("bad-cal"),
        None,
        &mut replay_analyzer(&analysis_addr),
    );
    assert!(bad.is_err(), "mixed calibrations must be rejected");
    let bad_buckets = craft_gen("other-buckets.wal", 32, false);
    let bad = reshard::merge(
        &[generations[0].clone(), bad_buckets],
        &dir.join("bad-buckets"),
        None,
        &mut replay_analyzer(&analysis_addr),
    );
    let err = match bad {
        Ok(_) => panic!("mismatched bucket counts must be rejected"),
        Err(e) => e,
    };
    assert!(err.contains("bucket"), "{err}");
    std::fs::remove_dir_all(&dir).ok();
}


/// Split with N == bucket_count: every child owns exactly one bucket
/// file, and the union of children still reconstructs the parent top-k.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn split_consumes_each_bucket_once() {
    const BUCKETS: usize = 4;
    let dir = tempdir("onebucket");
    let n = 1_200;
    let corpus = unit_vectors(n, DIM, 0x5EED_CA11);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &corpus[..DIM * n.min(2_000)]);
    let (analysis_addr, analysis) = mock_analysis::start_mock_analysis().await;
    std::mem::forget(analysis);

    let (mut client, index_path, _node) = start_wal_node(
        &dir,
        "shard.tv",
        0,
        BUCKETS as u32,
        &analysis_addr,
        &shift,
        &scale,
    )
    .await;
    ingest(&mut client, &corpus, n, 0).await;
    let parent = TurboQuantIndex::load(&index_path).unwrap();
    parent.prepare();

    let output = reshard::split(
        &reshard::resolve_gen(&turbovec_search::wal::wal_dir(&index_path)).unwrap(),
        BUCKETS,
        &dir.join("split"),
        0,
        25_000_000,
        &mut replay_analyzer(&analysis_addr),
    )
    .unwrap();
    assert_eq!(output.children.len(), BUCKETS);
    assert_eq!(
        output.children.iter().map(|c| c.num_vectors).sum::<u64>(),
        n as u64
    );
    for (i, child) in output.children.iter().enumerate() {
        assert!(!child.parent_ids.is_empty(), "bucket {i} was never consumed");
        // Child i's ids are exactly bucket i's — each bucket consumed once.
        assert!(
            child
                .parent_ids
                .iter()
                .all(|&id| reshard::bucket_of(id, BUCKETS) == i),
            "child {i} holds ids outside bucket {i}"
        );
        assert_eq!(child.slot_offset, i as u64 * 25_000_000);
        // Hash ranges tile the u64 space in bucket order.
        assert_eq!(child.hash_lo, (i as u64) << 62);
        assert_eq!(
            child.hash_hi,
            if i + 1 == BUCKETS {
                u64::MAX
            } else {
                ((i as u64 + 1) << 62) - 1
            }
        );
    }
    // The top-k invariant holds per child pair too (union == parent).
    let children: Vec<(&reshard::ChildImage, TurboQuantIndex)> = output
        .children
        .iter()
        .map(|c| {
            let index = TurboQuantIndex::load(&c.tv_path).unwrap();
            index.prepare();
            (c, index)
        })
        .collect();
    for q in 0..4u64 {
        let query = unit_vectors(1, DIM, 0xB0B0_0000 + q);
        let k = 10;
        let expected = topk(&parent, &query, k, |id| id);
        let mut merged: Vec<(u64, u32)> = children
            .iter()
            .flat_map(|(child, index)| {
                topk(index, &query, k, |local| child.parent_ids[local as usize])
            })
            .collect();
        merged.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        merged.truncate(k);
        assert_eq!(merged, expected, "query {q}: split changed the top-k");
    }
    std::fs::remove_dir_all(&dir).ok();
}

/// Split finer than the WAL bucket count: the fallback re-partitions
/// every record and still reconstructs the parent top-k.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn split_finer_than_buckets_repartitions() {
    let dir = tempdir("fallback");
    let n = 800;
    let corpus = unit_vectors(n, DIM, 0x5EED_CA11);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &corpus[..DIM * n.min(2_000)]);
    let (analysis_addr, analysis) = mock_analysis::start_mock_analysis().await;
    std::mem::forget(analysis);

    let (mut client, index_path, _node) =
        start_wal_node(&dir, "shard.tv", 0, 2, &analysis_addr, &shift, &scale).await;
    ingest(&mut client, &corpus, n, 0).await;
    let parent = TurboQuantIndex::load(&index_path).unwrap();
    parent.prepare();

    // 2 bucket files, split=4: the WAL bucket count caps cheap splits at
    // 2, so this exercises the re-partitioning fallback.
    let output = reshard::split(
        &reshard::resolve_gen(&turbovec_search::wal::wal_dir(&index_path)).unwrap(),
        4,
        &dir.join("split"),
        0,
        25_000_000,
        &mut replay_analyzer(&analysis_addr),
    )
    .unwrap();
    assert_eq!(output.children.len(), 4);
    assert_eq!(
        output.children.iter().map(|c| c.num_vectors).sum::<u64>(),
        n as u64
    );
    for (i, child) in output.children.iter().enumerate() {
        assert!(
            child
                .parent_ids
                .iter()
                .all(|&id| reshard::bucket_of(id, 4) == i),
            "child {i} holds ids outside its repartitioned range"
        );
    }
    let children: Vec<(&reshard::ChildImage, TurboQuantIndex)> = output
        .children
        .iter()
        .map(|c| {
            let index = TurboQuantIndex::load(&c.tv_path).unwrap();
            index.prepare();
            (c, index)
        })
        .collect();
    for q in 0..4u64 {
        let query = unit_vectors(1, DIM, 0xB0B0_0000 + q);
        let k = 10;
        let expected = topk(&parent, &query, k, |id| id);
        let mut merged: Vec<(u64, u32)> = children
            .iter()
            .flat_map(|(child, index)| {
                topk(index, &query, k, |local| child.parent_ids[local as usize])
            })
            .collect();
        merged.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        merged.truncate(k);
        assert_eq!(merged, expected, "query {q}: fallback split changed the top-k");
    }
    std::fs::remove_dir_all(&dir).ok();
}

/// A generation whose manifest records preexisting state (an installed
/// snapshot image, or logging enabled on an already-populated shard) is
/// not full history: both reshard entry points must refuse it before
/// doing any work.
#[test]
fn reshard_refuses_a_log_with_preexisting_state() {
    let dir = tempdir("preexisting");
    let manifest = turbovec_search::wal::WalManifest {
        dim: DIM as u32,
        bit_width: BIT_WIDTH as u32,
        calibration_shift: vec![0.0; DIM],
        calibration_scale: vec![1.0; DIM],
        slot_offset: 0,
        generation: 1,
        bucket_bits: 2,
        bucket_count: 4,
        preexisting_vectors: 123,
        preexisting_documents: 0,
        format_version: turbovec_search::wal::FORMAT_VERSION,
    };
    let writer = turbovec_search::wal::WalWriter::create(&dir, manifest).unwrap();
    let gen = writer.dir().to_path_buf();
    drop(writer);

    let mut analyze = |_docs: &[(&str, Option<&AnalysisSpec>)]| -> Result<Vec<AnalyzedDoc>, String> {
        unreachable!("reshard must refuse before analyzing anything")
    };
    let err = reshard::split(&gen, 2, &dir.join("out"), 0, 25_000_000, &mut analyze)
        .expect_err("split must refuse preexisting state");
    assert!(err.contains("preexisting"), "{err}");
    let err = reshard::merge(&[gen], &dir.join("out"), None, &mut analyze)
        .expect_err("merge must refuse preexisting state");
    assert!(err.contains("preexisting"), "{err}");
}

/// The general N -> M reshard: two block-routed shards' logs
/// redistributed across four children with no intermediate merge. The
/// union of the children IS the corpus (conservation), every id lands in
/// its bucket-range child (partition — this is what makes the result
/// hash-uniform), and the union of child top-k reconstructs the
/// monolithic reference bitwise (the same lossless argument as split).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn split_logs_redistributes_two_shards_into_four() {
    let dir = tempdir("split_logs");
    let corpus = unit_vectors(N, DIM, 0x5EED_CA11);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &corpus[..DIM * 2_000.min(N)]);
    let (analysis_addr, analysis) = mock_analysis::start_mock_analysis().await;
    std::mem::forget(analysis);

    let half = N / 2;
    let (mut a, a_path, _node_a) =
        start_wal_node(&dir, "a.tv", 0, 64, &analysis_addr, &shift, &scale).await;
    ingest(&mut a, &corpus[..half * DIM], half, DOCS / 2).await;
    let (mut b, b_path, _node_b) =
        start_wal_node(&dir, "b.tv", half as u64, 64, &analysis_addr, &shift, &scale).await;
    ingest(&mut b, &corpus[half * DIM..], half, DOCS / 2).await;

    let generations = [a_path, b_path]
        .iter()
        .map(|p| reshard::resolve_gen(&turbovec_search::wal::wal_dir(p)).unwrap())
        .collect::<Vec<_>>();
    let out_dir = dir.join("redistributed");
    let output = reshard::split_logs(
        &generations,
        4,
        &out_dir,
        0,
        25_000_000,
        &mut replay_analyzer(&analysis_addr),
    )
    .unwrap();
    assert_eq!(output.children.len(), 4);

    // Conservation across the union of both inputs.
    let total_vectors: u64 = output.children.iter().map(|c| c.num_vectors).sum();
    let total_docs: u64 = output.children.iter().map(|c| c.num_documents).sum();
    assert_eq!(total_vectors, N as u64);
    assert_eq!(total_docs, DOCS as u64);

    // Partition: child i holds exactly bucket range [i*16, (i+1)*16).
    for (i, child) in output.children.iter().enumerate() {
        assert!(
            child
                .parent_ids
                .iter()
                .all(|&id| reshard::bucket_of(id, 64) / 16 == i),
            "child {i} holds ids outside its bucket range"
        );
        assert_eq!(child.slot_offset, i as u64 * 25_000_000);
    }

    // Union of child top-k == monolithic reference, bitwise.
    let reference = build_monolithic(&corpus, DIM, BIT_WIDTH, &shift, &scale);
    let children: Vec<(&reshard::ChildImage, TurboQuantIndex)> = output
        .children
        .iter()
        .map(|c| {
            let index = TurboQuantIndex::load(&c.tv_path).unwrap();
            index.prepare();
            (c, index)
        })
        .collect();
    for q in 0..8u64 {
        let query = unit_vectors(1, DIM, 0xB0B0_0000 + q);
        let k = 10;
        let expected = topk(&reference, &query, k, |id| id);
        let mut merged: Vec<(u64, u32)> = children
            .iter()
            .flat_map(|(child, index)| {
                topk(index, &query, k, |local| child.parent_ids[local as usize])
            })
            .collect();
        merged.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        merged.truncate(k);
        assert_eq!(merged, expected, "query {q}: redistribution changed the top-k");
    }
    std::fs::remove_dir_all(&dir).ok();
}
