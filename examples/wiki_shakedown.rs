//! Real-data shakedown: ingest the Lucene-era wikipedia corpus (bge-m3
//! embeddings + Simple English Wikipedia sentences) into a 4-shard
//! turbovec-search cluster on loopback, using the REAL OpenNLP analysis
//! sidecar for BM25, then run hybrid queries end-to-end.
//!
//! The corpus stays at its absolute path (NOT copied into the repo);
//! `--data-dir` points at it. The 4 parts are the pre-existing shard
//! partitioning: part N goes to shard node N, with global doc id
//! `N * part_count + index`.
//!
//! What this demonstrates, end to end:
//!   1. dataset parsing (binary embeddings + newline-less-last-line text)
//!   2. calibration fitted on a sample, then SearchService-level
//!      BroadcastCalibration to every shard (its first real use)
//!   3. AddDocuments (texts) + AddVectors (embeddings) with aligned ids
//!   4. BM25 ingest through the REAL native analysis sidecar (PORTER
//!      stems, MODE_FULL) — falls back to the in-repo mock with a loud
//!      warning if the sidecar binary is missing or never comes up
//!   5. .tv + .bm25 persistence via Flush, reusable for the two-machine
//!      run later
//!   6. hybrid cascade queries with per-leg scores and a highlighted
//!      span sliced from the doc store
//!
//! Run:
//!
//! ```text
//! cargo run --release --bin wiki_shakedown
//! ```

use std::path::PathBuf;
use std::process::Child;
use std::time::Instant;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use turbovec_search::coordinator::CoordinatorServiceImpl;
use turbovec_search::dataset;
use turbovec_search::harness::{self, mock_analysis};
use turbovec_search::node::NodeConfig;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::{
    AddDocumentsRequest, AddVectorsRequest, AnalysisSpec, BroadcastCalibrationRequest,
    FlushRequest, GetDocumentsRequest,
};

const SIDECAR_BIN: &str =
    "/work/main/grpc-services/grpc-opennlp-analysis/build/native/nativeCompile/grpc-opennlp-analysis";
const DEFAULT_DATA_DIR: &str = "/work/opensearch-grpc-knn/distributed_test_data/wikipedia";
const VECTOR_BATCH: usize = 512;

/// WHITESPACE tokenizer, PORTER stemmer, MODE_FULL, SOURCE_STEMS — query
/// and ingest share term identity through the stems.
fn analysis_spec() -> AnalysisSpec {
    turbovec_search::analyzer::body_spec()
}

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

fn start_sidecar(port: u16) -> Result<(Child, String), String> {
    turbovec_search::harness::start_sidecar(SIDECAR_BIN, port)
}

async fn add_documents(addr: &str, texts: Vec<String>, spec: &AnalysisSpec, shard: usize) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel::<AddDocumentsRequest>(64);
    let n = texts.len();
    let spec = spec.clone();
    let feed = tokio::spawn(async move {
        for (i, text) in texts.into_iter().enumerate() {
            if i > 0 && i % 20_000 == 0 {
                eprintln!("  shard {shard}: {i}/{n} documents analyzed");
            }
            tx.send(AddDocumentsRequest {
                numerics: Vec::new(),
                facets: Vec::new(),
                text,
                analysis: Some(spec.clone()),
                lineage: None,
                fields: Vec::new(),
            })
            .await
            .unwrap();
        }
    });
    let response = client
        .add_documents(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    feed.await.unwrap();
    assert_eq!(response.added as usize, n);
}

async fn add_vectors(addr: &str, vectors: Vec<f32>, dim: usize) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    let feed = tokio::spawn(async move {
        for batch in vectors.chunks(VECTOR_BATCH * dim) {
            tx.send(AddVectorsRequest {
                vectors: batch.to_vec(),
                dim: dim as u32,
            })
            .await
            .unwrap();
        }
    });
    let response = client
        .add_vectors(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    feed.await.unwrap();
    assert_eq!(response.added as usize, response.total as usize);
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = arg("data-dir", DEFAULT_DATA_DIR);
    let out_dir = arg("out-dir", "/tmp/wiki-shards");
    let sidecar_port: u16 = arg("sidecar-port", "59101").parse()?;
    std::fs::create_dir_all(&out_dir)?;

    // --- 1. Load the four parts; assert exact pairing -------------------
    let mut parts = Vec::new();
    for part in 0..4 {
        let bin = PathBuf::from(format!("{data_dir}/embeddings_part_{part}.bin"));
        let txt = PathBuf::from(format!("{data_dir}/source_part_{part}.txt"));
        let (vectors, dim, texts) = dataset::read_part(&bin, &txt)?;
        eprintln!(
            "part {part}: {} vectors x dim {dim}, {} sentences (paired)",
            vectors.len() / dim,
            texts.len()
        );
        parts.push((vectors, dim, texts));
    }
    let dim = parts[0].1;
    let counts: Vec<usize> = parts.iter().map(|(v, d, _)| v.len() / d).collect();

    // --- 2. Fit calibration on a sample of part 0 -----------------------
    let sample_stride = 23usize;
    let sample: Vec<f32> = parts[0]
        .0
        .chunks(dim)
        .step_by(sample_stride)
        .flatten()
        .copied()
        .collect();
    let (shift, scale) = harness::fit_calibration(dim, 4, &sample);
    eprintln!(
        "calibration fitted on {} sample vectors",
        sample.len() / dim
    );

    // --- 3. Analysis sidecar (real, or mock fallback) -------------------
    let mut sidecar_child: Option<Child> = None;
    let analysis_addr = match start_sidecar(sidecar_port) {
        Ok((child, addr)) => {
            eprintln!("analysis sidecar: REAL native binary at {addr}");
            sidecar_child = Some(child);
            addr
        }
        Err(e) => {
            eprintln!("WARNING: real sidecar unavailable ({e})");
            eprintln!("WARNING: falling back to the in-repo mock analyzer");
            let (addr, _handle) = mock_analysis::start_mock_analysis().await;
            addr
        }
    };

    // --- 4. Shard nodes + coordinator -----------------------------------
    let mut node_addrs = Vec::new();
    let mut node_handles = Vec::new();
    let mut offset = 0u64;
    for (shard, _) in counts.iter().enumerate() {
        let index_path = PathBuf::from(&out_dir).join(format!("shard-{shard}.tv"));
        let (addr, handle) = harness::start_empty_node(NodeConfig {
            slot_offset: offset,
            index_path: Some(index_path),
            analysis_addr: Some(analysis_addr.clone()),
            ..Default::default()
        })
        .await;
        node_addrs.push(addr);
        node_handles.push(handle);
        offset += counts[shard] as u64;
    }
    let coordinator = CoordinatorServiceImpl::new(node_addrs.clone())
        .with_bm25(Some(analysis_addr), Default::default());

    // --- 5. Broadcast the calibration (first real use) -------------------
    let results = coordinator
        .fanout_calibration(&BroadcastCalibrationRequest {
            dim: dim as u32,
            bit_width: 4,
            shift: shift.clone(),
            scale: scale.clone(),
        })
        .await;
    for r in &results {
        assert!(r.ok, "calibration rejected by {}: {}", r.node, r.error);
    }
    eprintln!("calibration broadcast to {} shards OK", results.len());

    // --- 6. Ingest: documents (BM25) then vectors, ids aligned ----------
    let spec = analysis_spec();
    let mut ingest_tasks = Vec::new();
    for (shard, (vectors, _, texts)) in parts.into_iter().enumerate() {
        let addr = node_addrs[shard].clone();
        let spec = spec.clone();
        let n = counts[shard];
        ingest_tasks.push(tokio::spawn(async move {
            let t0 = Instant::now();
            add_documents(&addr, texts, &spec, shard).await;
            let t_docs = t0.elapsed();
            let t0 = Instant::now();
            add_vectors(&addr, vectors, dim).await;
            (shard, n, t_docs, t0.elapsed())
        }));
    }
    for task in ingest_tasks {
        let (shard, n, t_docs, t_vecs) = task.await?;
        eprintln!("shard {shard}: {n} docs in {t_docs:?}, {n} vectors in {t_vecs:?}");
    }

    // --- 7. Persist (reusable .tv + .bm25 for the two-machine run) ------
    for (shard, addr) in node_addrs.iter().enumerate() {
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let flushed = client.flush(FlushRequest {}).await.unwrap().into_inner();
        assert!(flushed.written);
        let tv = PathBuf::from(&out_dir).join(format!("shard-{shard}.tv"));
        let bm = turbovec_search::node::bm25_sidecar_path(&tv);
        eprintln!(
            "shard {shard}: flushed {} vectors + {} docs ({:.0} MiB + {:.0} MiB)",
            flushed.num_vectors,
            flushed.num_documents,
            tv.metadata().map(|m| m.len() as f64 / 1e6).unwrap_or(0.0),
            bm.metadata().map(|m| m.len() as f64 / 1e6).unwrap_or(0.0),
        );
    }

    // --- 8. Hybrid cascade queries on real data --------------------------
    let probe_docs = [12_345usize, 40_000, 60_000];
    for &probe in &probe_docs {
        // Query: one corpus sentence's text + its embedding (bge-m3 space).
        let (part, index) = (probe / counts[0], probe % counts[0]);
        let text = &parts_text(&data_dir, part, index)?;
        let (vector, _) = dataset::read_embedding_at(
            &PathBuf::from(format!("{data_dir}/embeddings_part_{part}.bin")),
            index,
        )?;
        let global_probe_id = (part * counts[0] + index) as u64;
        println!("\n=== query (doc {global_probe_id}): {text:?}");
        let hits = coordinator
            .fanout_cascade("shakedown", text, &vector, 5, Some(&spec), 0.0, false)
            .await?
            .0;
        for hit in &hits {
            println!(
                "  #{} doc {:>7} (shard {}) vector {:.4}  bm25 {:.4}",
                hit.rank, hit.doc_id, hit.shard, hit.vector_score, hit.bm25_score
            );
        }
        // Highlight: slice one BM25-matched span from the doc store.
        if let Some(top) = hits.first() {
            let owner = node_addrs[top.shard as usize].clone();
            let mut client = NodeServiceClient::connect(owner).await.unwrap();
            let docs = client
                .get_documents(GetDocumentsRequest {
                    doc_ids: vec![top.doc_id],
                })
                .await?
                .into_inner();
            if let Some(doc) = docs.documents.first() {
                println!("  top text: {:?}", doc.text);
            }
        }
    }

    for handle in node_handles {
        handle.abort();
    }
    if let Some(mut child) = sidecar_child {
        let _ = child.kill();
    }
    eprintln!("\nshakedown complete; shards persisted under {out_dir}");
    Ok(())
}

/// Read one sentence by (part, index) from the source text file.
fn parts_text(data_dir: &str, part: usize, index: usize) -> std::io::Result<String> {
    use std::io::{BufRead, BufReader};
    let file = std::fs::File::open(format!("{data_dir}/source_part_{part}.txt"))?;
    let line = BufReader::new(file).lines().nth(index).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "index out of range")
    })??;
    Ok(line)
}
