//! Court pipeline stage 3: ingest the chunked, embedded corpus into an
//! N-shard turbovec-search cluster on loopback — calibration fit +
//! BroadcastCalibration, AddDocuments (chunk texts WITH lineage, real
//! analysis sidecar for BM25) + AddVectors with aligned ids, Flush for
//! persistence — then run sample hybrid cascade queries.
//!
//! Shard assignment: contiguous blocks of the chunks-file order (chunk_id
//! ranges), preserving per-opinion locality within a shard where the
//! file order allows.
//!
//! ```text
//! court_ingest --shards=4 --out-dir=/work/court-corpus/shards
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use turbovec_search::coordinator::CoordinatorServiceImpl;
use turbovec_search::court::{self, Chunk};
use turbovec_search::harness::{self, mock_analysis, start_sidecar};
use turbovec_search::node::NodeConfig;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::{
    AddDocumentsRequest, AddVectorsRequest, AnalysisSpec, BroadcastCalibrationRequest, DocLineage,
    FlushRequest, GetDocumentsRequest,
};

const SIDECAR_BIN: &str =
    "/work/worktrees/turbovec-workspace/grpc-opennlp-analysis/build/native/nativeCompile/grpc-opennlp-analysis";

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

fn analysis_spec() -> AnalysisSpec {
    // WHITESPACE tokenizer, PORTER stemmer, MODE_FULL, SOURCE_STEMS.
    AnalysisSpec {
        tokenizer: 1,
        stemmer: 2,
        term_vector_mode: 1,
        term_vector_source: 2,
        normalizer_rungs: vec![],
    }
}

/// Sequential reader over an embeddings-file block starting at record
/// `start`, reached by a direct seek (records are fixed stride: 8-byte
/// opinion_id, 4-byte ordinal, dim little-endian f32s after the 12-byte
/// header).
struct EmbBlock {
    reader: std::io::BufReader<std::fs::File>,
    dim: usize,
}

impl EmbBlock {
    fn open(path: &str, start: u64, dim: usize) -> std::io::Result<Self> {
        use std::io::{Seek, SeekFrom};
        let mut file = std::fs::File::open(path)?;
        file.seek(SeekFrom::Start(12 + start * (12 + dim as u64 * 4)))?;
        Ok(Self {
            reader: std::io::BufReader::with_capacity(1 << 20, file),
            dim,
        })
    }

    /// The next record's (opinion_id, ordinal), skipping its vector.
    fn next_key_skip_vector(&mut self) -> std::io::Result<(u64, u32)> {
        use std::io::Read;
        let mut fixed = [0u8; 12];
        self.reader.read_exact(&mut fixed)?;
        self.reader.seek_relative(self.dim as i64 * 4)?;
        Ok((
            u64::from_le_bytes(fixed[..8].try_into().unwrap()),
            u32::from_le_bytes(fixed[8..12].try_into().unwrap()),
        ))
    }

    /// The next record's vector.
    fn next_vector(&mut self) -> std::io::Result<Vec<f32>> {
        use std::io::Read;
        let mut fixed = [0u8; 12];
        self.reader.read_exact(&mut fixed)?;
        let mut buf = vec![0u8; self.dim * 4];
        self.reader.read_exact(&mut buf)?;
        Ok(buf
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
            .collect())
    }
}

/// Remote-shard mode: stream the chunks and embeddings files shard by
/// shard into already-running nodes (`--nodes`), instead of building the
/// join in memory. Nothing is buffered beyond the channel window, so the
/// driver stays in the tens of MB at any corpus size.
#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nodes_arg = arg("nodes", "");
    if !nodes_arg.is_empty() {
        return run_remote(nodes_arg).await;
    }

    let chunks_path = arg("chunks", "/work/court-corpus/chunks.ndjson");
    let embeddings_path = arg("embeddings", "/work/court-corpus/embeddings.bin");
    let out_dir = arg("out-dir", "/work/court-corpus/shards");
    let n_shards: usize = arg("shards", "4").parse()?;
    let limit: usize = arg("limit", "0").parse()?;
    let sidecar_port: u16 = arg("sidecar-port", "59101").parse()?;
    std::fs::create_dir_all(&out_dir)?;

    // --- Load and join chunks x embeddings -------------------------------
    let t0 = Instant::now();
    let mut chunks: Vec<Chunk> = Vec::new();
    for chunk in court::read_chunks(std::path::Path::new(&chunks_path))? {
        chunks.push(chunk?);
        if limit > 0 && chunks.len() >= limit {
            break;
        }
    }
    let mut embeddings: HashMap<(u64, u32), Vec<f32>> = HashMap::new();
    let (dim, reader) = court::EmbeddingReader::open(std::path::Path::new(&embeddings_path))?;
    let dim = dim as usize;
    for record in reader {
        let record = record?;
        embeddings.insert((record.opinion_id, record.ordinal), record.vector);
    }
    let mut joined: Vec<(Chunk, Vec<f32>)> = Vec::with_capacity(chunks.len());
    let mut missing = 0usize;
    for chunk in chunks {
        match embeddings.remove(&(chunk.opinion_id, chunk.ordinal)) {
            Some(vector) => joined.push((chunk, vector)),
            None => missing += 1,
        }
    }
    eprintln!(
        "loaded {} chunks joined with dim-{dim} embeddings ({missing} without embeddings) in {:?}",
        joined.len(),
        t0.elapsed()
    );
    let m = joined.len();

    // --- Calibration on a stride sample ----------------------------------
    let sample: Vec<f32> = joined
        .iter()
        .step_by(37)
        .flat_map(|(_, v)| v.iter().copied())
        .collect();
    let (shift, scale) = harness::fit_calibration(dim, 4, &sample);
    eprintln!(
        "calibration fitted on {} sample vectors",
        sample.len() / dim
    );

    // --- Sidecar (real; mock only with --allow-mock) ----------------------
    // A mock-analyzed BM25 index looks healthy and scores garbage, so
    // falling back silently is never acceptable for a real corpus.
    let mut sidecar_child = None;
    let analysis_addr = match start_sidecar(&arg("sidecar-bin", SIDECAR_BIN), sidecar_port) {
        Ok((child, addr)) => {
            eprintln!("analysis sidecar: REAL native binary at {addr}");
            sidecar_child = Some(child);
            addr
        }
        Err(e) if std::env::args().any(|a| a == "--allow-mock") => {
            eprintln!("WARNING: real sidecar unavailable ({e}); using the in-repo mock");
            mock_analysis::start_mock_analysis().await.0
        }
        Err(e) => return Err(format!("analysis sidecar unavailable: {e}").into()),
    };

    // --- Shard nodes + coordinator ----------------------------------------
    let per = m / n_shards;
    let mut node_addrs = Vec::new();
    let mut node_handles = Vec::new();
    for shard in 0..n_shards {
        let start = shard * per;
        let (addr, handle) = harness::start_empty_node(NodeConfig {
            slot_offset: start as u64,
            index_path: Some(PathBuf::from(&out_dir).join(format!("shard-{shard}.tv"))),
            analysis_addr: Some(analysis_addr.clone()),
            ..Default::default()
        })
        .await;
        node_addrs.push(addr);
        node_handles.push(handle);
    }
    let coordinator = CoordinatorServiceImpl::new(node_addrs.clone())
        .with_bm25(Some(analysis_addr), Default::default());
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

    // --- Ingest: documents (with lineage) then vectors --------------------
    let spec = analysis_spec();
    let mut ingest_tasks = Vec::new();
    for (shard, addr) in node_addrs.iter().enumerate() {
        let start = shard * per;
        let end = if shard == n_shards - 1 {
            m
        } else {
            start + per
        };
        let block: Vec<(Chunk, Vec<f32>)> = joined[start..end].to_vec();
        let addr = addr.clone();
        let spec = spec.clone();
        ingest_tasks.push(tokio::spawn(async move {
            let n = block.len();
            // Documents first so doc ids and vector slots align 1:1.
            let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
            let (tx, rx) = mpsc::channel::<AddDocumentsRequest>(64);
            let feeder = tokio::spawn(async move {
                for (i, (chunk, _)) in block.iter().enumerate() {
                    if i > 0 && i.is_multiple_of(20_000) {
                        eprintln!("  shard {shard}: {i}/{n} documents analyzed");
                    }
                    tx.send(AddDocumentsRequest {
                        text: chunk.text.clone(),
                        analysis: Some(spec.clone()),
                        lineage: Some(DocLineage {
                            opinion_id: chunk.opinion_id,
                            cluster_id: chunk.cluster_id,
                            span_start: chunk.span_start,
                            span_end: chunk.span_end,
                        }),
                    })
                    .await
                    .unwrap();
                }
                let vectors: Vec<f32> = block.into_iter().flat_map(|(_, v)| v).collect();
                (vectors, dim)
            });
            let docs = client
                .add_documents(ReceiverStream::new(rx))
                .await
                .unwrap()
                .into_inner();
            let (vectors, dim) = feeder.await.unwrap();
            assert_eq!(docs.added as usize, n);

            let t0 = Instant::now();
            let (tx, rx) = mpsc::channel(8);
            let vf = tokio::spawn(async move {
                for batch in vectors.chunks(512 * dim) {
                    tx.send(AddVectorsRequest {
                        vectors: batch.to_vec(),
                        dim: dim as u32,
                    })
                    .await
                    .unwrap();
                }
            });
            let vecs = client
                .add_vectors(ReceiverStream::new(rx))
                .await
                .unwrap()
                .into_inner();
            vf.await.unwrap();
            assert_eq!(vecs.added as usize, n);
            (shard, n, t0.elapsed())
        }));
    }
    for task in ingest_tasks {
        let (shard, n, t) = task.await?;
        eprintln!("shard {shard}: {n} chunks ingested (vectors in {t:?})");
    }

    // --- Flush -------------------------------------------------------------
    for (shard, addr) in node_addrs.iter().enumerate() {
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let flushed = client.flush(FlushRequest {}).await.unwrap().into_inner();
        assert!(flushed.written);
        eprintln!(
            "shard {shard}: flushed {} vectors + {} docs",
            flushed.num_vectors, flushed.num_documents
        );
    }

    // --- Sample hybrid cascade queries --------------------------------------
    let probes = [0usize, m / 2, m - 1];
    for probe in probes {
        let (chunk, vector) = &joined[probe];
        println!(
            "\n=== query (chunk {}, opinion {}, cluster {}): {:?}",
            chunk.chunk_id,
            chunk.opinion_id,
            chunk.cluster_id,
            &chunk.text[..chunk.text.len().min(120)]
        );
        let hits = coordinator
            .fanout_cascade("court", &chunk.text, vector, 5, Some(&spec))
            .await?;
        for hit in &hits {
            println!(
                "  #{} doc {:>7} (shard {}) vector {:.4}  bm25 {:.4}",
                hit.rank, hit.doc_id, hit.shard, hit.vector_score, hit.bm25_score
            );
        }
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
                let lineage = doc
                    .lineage
                    .as_ref()
                    .map(|l| {
                        format!(
                            "opinion {} cluster {} span {}..{}",
                            l.opinion_id, l.cluster_id, l.span_start, l.span_end
                        )
                    })
                    .unwrap_or_default();
                println!("  top doc {}: {}", top.doc_id, lineage);
                println!("  top text: {:?}", &doc.text[..doc.text.len().min(160)]);
            }
        }
    }

    for handle in node_handles {
        handle.abort();
    }
    if let Some(mut child) = sidecar_child {
        let _ = child.kill();
    }
    eprintln!("\ningest complete; shards persisted under {out_dir}");
    Ok(())
}

/// Ingest into already-running shard nodes (e.g. on a second host).
/// Streams both files per shard instead of holding the full join.
async fn run_remote(nodes_arg: String) -> Result<(), Box<dyn std::error::Error>> {
    let chunks_path = arg("chunks", "/work/court-corpus/chunks.ndjson");
    let embeddings_path = arg("embeddings", "/work/court-corpus/embeddings-static.bin");
    let node_addrs: Vec<String> = nodes_arg
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            if s.starts_with("http://") || s.starts_with("https://") {
                s.to_string()
            } else {
                format!("http://{s}")
            }
        })
        .collect();
    let n_shards = node_addrs.len();
    if n_shards == 0 {
        return Err("--nodes must list at least one node".into());
    }
    // Optional shard range: run several drivers in parallel, each owning a
    // disjoint slice of the SAME full node list. Block offsets stay global
    // (per = m / n_shards), so the union of ranges reproduces the single
    // sequential run exactly.
    let first_shard: usize = arg("first-shard", "0").parse()?;
    let end_shard: usize = match arg("end-shard", "").as_str() {
        "" => n_shards,
        s => s.parse()?,
    };
    if first_shard >= end_shard || end_shard > n_shards {
        return Err(format!(
            "--first-shard={first_shard} --end-shard={end_shard} out of range for {n_shards} nodes"
        )
        .into());
    }

    // Counts: positional ids == file order, established by the stage-1
    // integrity check (chunk count == embedding count).
    let mut m = 0usize;
    for chunk in court::read_chunks(std::path::Path::new(&chunks_path))? {
        chunk?;
        m += 1;
    }
    let per = m / n_shards;
    eprintln!(
        "remote ingest: {m} chunks over {n_shards} nodes ({per}/shard), \
         this driver handles shards {first_shard}..{end_shard}"
    );

    // Calibration: stride sample streamed from the embeddings file.
    let (dim, reader) = court::EmbeddingReader::open(std::path::Path::new(&embeddings_path))?;
    let dim = dim as usize;
    let mut sample: Vec<f32> = Vec::new();
    for (i, record) in reader.enumerate() {
        let record = record?;
        if i % 300 == 0 {
            sample.extend_from_slice(&record.vector);
        }
    }
    let (shift, scale) = harness::fit_calibration(dim, 4, &sample);
    eprintln!(
        "calibration fitted on {} sample vectors",
        sample.len() / dim
    );

    // Broadcast only to this driver's range: nodes outside it may already
    // hold vectors (another driver's shards), where SetCalibration is a
    // failed_precondition rather than an idempotent retry.
    let coordinator = CoordinatorServiceImpl::new(node_addrs[first_shard..end_shard].to_vec())
        .with_bm25(
            Some(arg("analysis-addr", "http://127.0.0.1:59111")),
            Default::default(),
        );
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

    let spec = analysis_spec();
    for (shard, addr) in node_addrs.iter().enumerate().take(end_shard).skip(first_shard) {
        let t0 = Instant::now();
        let start = shard * per;
        let end = if shard == n_shards - 1 {
            m
        } else {
            start + per
        };

        let n = end - start;
        let mut client = NodeServiceClient::connect(addr.clone()).await?;

        // Documents first (ids 0..), then vectors (slots align). The doc
        // feeder walks the chunks file and the embeddings block in lock
        // step, asserting key equality at every position — both files were
        // written in the same order, so that equality IS the join.
        let (tx, rx) = mpsc::channel::<AddDocumentsRequest>(256);
        let spec2 = spec.clone();
        let cp = chunks_path.clone();
        let ep = embeddings_path.clone();
        let feeder = tokio::task::spawn_blocking(move || -> Result<(), String> {
            use std::io::BufRead;
            let mut emb = EmbBlock::open(&ep, start as u64, dim).map_err(|e| e.to_string())?;
            let file = std::fs::File::open(&cp).map_err(|e| e.to_string())?;
            let mut sent = 0usize;
            for (i, line) in std::io::BufReader::new(file).lines().enumerate() {
                if i < start {
                    line.map_err(|e| e.to_string())?;
                    continue;
                }
                if i >= end {
                    break;
                }
                let line = line.map_err(|e| e.to_string())?;
                let chunk: Chunk = serde_json::from_str(&line).map_err(|e| e.to_string())?;
                let key = emb.next_key_skip_vector().map_err(|e| e.to_string())?;
                if key != (chunk.opinion_id, chunk.ordinal) {
                    return Err(format!(
                        "chunk/embedding order mismatch at shard {shard} position {i}: \
                         chunk ({}, {}), embedding ({}, {})",
                        chunk.opinion_id, chunk.ordinal, key.0, key.1
                    ));
                }
                tx.blocking_send(AddDocumentsRequest {
                    text: chunk.text,
                    analysis: Some(spec2.clone()),
                    lineage: Some(DocLineage {
                        opinion_id: chunk.opinion_id,
                        cluster_id: chunk.cluster_id,
                        span_start: chunk.span_start,
                        span_end: chunk.span_end,
                    }),
                })
                .map_err(|e| e.to_string())?;
                sent += 1;
            }
            if sent != n {
                return Err(format!("shard {shard}: sent {sent} of {n} docs"));
            }
            Ok(())
        });
        let docs = client
            .add_documents(ReceiverStream::new(rx))
            .await?
            .into_inner();
        feeder.await??;
        assert_eq!(docs.added as usize, n);

        // Vectors: a direct seek into the fixed-stride embeddings file;
        // keys were verified during the doc phase.
        let (tx, rx) = mpsc::channel::<AddVectorsRequest>(8);
        let ep = embeddings_path.clone();
        let vf = tokio::task::spawn_blocking(move || -> Result<(), String> {
            let mut emb = EmbBlock::open(&ep, start as u64, dim).map_err(|e| e.to_string())?;
            let mut batch: Vec<f32> = Vec::with_capacity(512 * dim);
            for _ in 0..n {
                batch.extend(emb.next_vector().map_err(|e| e.to_string())?);
                if batch.len() == 512 * dim {
                    tx.blocking_send(AddVectorsRequest {
                        vectors: std::mem::replace(&mut batch, Vec::with_capacity(512 * dim)),
                        dim: dim as u32,
                    })
                    .map_err(|e| e.to_string())?;
                }
            }
            if !batch.is_empty() {
                tx.blocking_send(AddVectorsRequest {
                    vectors: batch,
                    dim: dim as u32,
                })
                .map_err(|e| e.to_string())?;
            }
            Ok(())
        });
        let vecs = client
            .add_vectors(ReceiverStream::new(rx))
            .await?
            .into_inner();
        vf.await??;
        assert_eq!(vecs.added as usize, n);

        let flushed = client.flush(FlushRequest {}).await?.into_inner();
        assert!(flushed.written);
        eprintln!(
            "shard {shard}: {n} chunks ingested + flushed ({} vectors, {} docs) in {:?}",
            flushed.num_vectors,
            flushed.num_documents,
            t0.elapsed()
        );
    }
    eprintln!("remote ingest complete");
    Ok(())
}
