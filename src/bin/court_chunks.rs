//! Court pipeline stage 1 (v2, static-embedding path): stream the
//! opinions NDJSON through the OpenNLP analysis sidecar and produce
//! sentence-aware chunks AND their embeddings in one pass.
//!
//! Per opinion: ONE Analyze with sentence detection, whitespace tokens,
//! and EmbeddingOptions.SOURCE_SENTENCES, which returns per-block
//! ChunkEmbeddings (dim 256) with spans in original coordinates. Blocks
//! are packed into ~`--target-tokens` chunks (`plan_chunks`); a block
//! over `--hard-cap-tokens` is re-Analyzed solo and split at its own
//! token boundaries (no text is ever dropped).
//! Chunk vectors for packed blocks are the token-weighted pool of the
//! block vectors — EXACT for a mean-pooled static table, see
//! `court::pool_block_vectors`.
//!
//! Outputs (both resumable): `chunks.ndjson` (lineage) and
//! `embeddings.bin` (keyed by (opinion_id, ordinal)). The TEI/bge-m3
//! 1024d path (`court_embed`) remains the quality path.
//!
//! `--verify-pooling=N` re-Analyzes N sampled multi-block chunks with
//! SOURCE_TOKENS and cosine-compares the mean of token vectors against
//! the pooled vector (expected ~1.0; min/median reported).

use std::io::BufRead;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{mpsc, Mutex};
use turbovec_search::court::{self, ChunkSource, ChunkWriter, EmbeddingWriter};
use turbovec_search::harness::start_sidecar_with_env;
use turbovec_search::pb::analysis::analysis_service_client::AnalysisServiceClient;
use turbovec_search::pb::analysis::{AnalysisOptions, AnalyzeRequest, EmbeddingOptions};

const SIDECAR_BIN: &str =
    "/work/main/grpc-services/grpc-opennlp-analysis/build/native/nativeCompile/grpc-opennlp-analysis";
const MODEL_DIR: &str = "/work/court-corpus/models/minilm-l6-v2-static";
const DIM: usize = 256;

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

struct ChunkedOpinion {
    src_line: u64,
    opinion_id: u64,
    cluster_id: u64,
    text: String,
    /// (span, vector) per planned chunk, in ordinal order.
    chunks: Vec<(court::Span, Vec<f32>)>,
}

async fn analyze(
    addr: &str,
    text: &str,
    embeddings: Option<EmbeddingOptions>,
) -> Result<turbovec_search::pb::analysis::AnalyzeResponse, tonic::Status> {
    let mut client = AnalysisServiceClient::connect(addr.to_string())
        .await
        .map_err(|e| tonic::Status::unavailable(format!("analysis sidecar: {e}")))?
        .max_decoding_message_size(turbovec_search::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(turbovec_search::MAX_MESSAGE_BYTES);
    client
        .analyze(AnalyzeRequest {
            text: text.to_string(),
            options: Some(AnalysisOptions {
                tokenizer: 1,
                sentence_detection: true,
                embeddings,
                ..Default::default()
            }),
        })
        .await
        .map(|r| r.into_inner())
}

fn to_spans(spans: &[turbovec_search::pb::analysis::Span]) -> Vec<court::Span> {
    spans
        .iter()
        .map(|s| court::Span {
            start: s.start.max(0) as u32,
            end: s.end.max(0) as u32,
        })
        .collect()
}

/// Compute the vector for one planned chunk from the block embeddings.
async fn chunk_vector(
    addr: &str,
    text: &str,
    plan: &court::ChunkPlan,
    block_vectors: &[(court::Span, Vec<f32>)],
    tokens: &[court::Span],
) -> Result<Vec<f32>, String> {
    match plan.source {
        ChunkSource::Blocks { first, last } => {
            let weighted: Vec<(&[f32], u32)> = block_vectors[first..=last]
                .iter()
                .map(|(span, vector)| {
                    let weight = tokens
                        .iter()
                        .filter(|t| t.start >= span.start && t.start < span.end)
                        .count() as u32;
                    (vector.as_slice(), weight.max(1))
                })
                .collect();
            Ok(court::pool_block_vectors(&weighted, DIM))
        }
        ChunkSource::SoloPiece => {
            let piece_text = court::slice_chars(text, plan.span.start, plan.span.end);
            let response = analyze(addr, &piece_text, Some(EmbeddingOptions { source: 1 }))
                .await
                .map_err(|e| format!("solo embed: {e}"))?;
            let embedding = response
                .embeddings
                .into_iter()
                .next()
                .ok_or_else(|| "solo embed returned no vectors".to_string())?;
            Ok(embedding.vector)
        }
    }
}

async fn analyze_opinion(
    addr: &str,
    src_line: u64,
    line: &str,
    target_tokens: usize,
    hard_cap: usize,
) -> Result<ChunkedOpinion, String> {
    let (opinion_id, cluster_id, text) = court::parse_opinion(line)?;
    let response = analyze(addr, &text, Some(EmbeddingOptions { source: 1 }))
        .await
        .map_err(|e| format!("analyze: {e}"))?;
    let sentences = to_spans(&response.sentences);
    let tokens: Vec<court::Span> = response
        .tokens
        .iter()
        .filter_map(|t| t.span)
        .map(|s| court::Span {
            start: s.start.max(0) as u32,
            end: s.end.max(0) as u32,
        })
        .collect();
    // Pair each block with its embedding by exact span.
    let mut block_vectors: Vec<(court::Span, Vec<f32>)> = Vec::new();
    for embedding in response.embeddings {
        let span = embedding.span.ok_or("embedding without span")?;
        let span = court::Span {
            start: span.start.max(0) as u32,
            end: span.end.max(0) as u32,
        };
        if !sentences.contains(&span) {
            return Err(format!(
                "embedding span ({},{}) not among sentence spans",
                span.start, span.end
            ));
        }
        block_vectors.push((span, embedding.vector));
    }
    block_vectors.sort_by_key(|(s, _)| s.start);
    if block_vectors.len() != sentences.len() {
        return Err(format!(
            "{} sentence blocks but {} embeddings",
            sentences.len(),
            block_vectors.len()
        ));
    }

    let plans = court::plan_chunks(&text, &sentences, &tokens, target_tokens, hard_cap);
    let spans: Vec<court::Span> = plans.iter().map(|p| p.span).collect();
    if !court::is_contiguous(&text, &spans) {
        return Err("CONTIGUITY VIOLATION".to_string());
    }
    let mut chunks = Vec::with_capacity(plans.len());
    for plan in &plans {
        let vector = chunk_vector(addr, &text, plan, &block_vectors, &tokens).await?;
        chunks.push((plan.span, vector));
    }
    Ok(ChunkedOpinion {
        src_line,
        opinion_id,
        cluster_id,
        text,
        chunks,
    })
}

/// `--verify-pooling`: cosine agreement between the stored pooled vector
/// and a direct mean of token-level embeddings (the exactness claim made
/// concrete) for N sampled multi-block chunks.
async fn verify_pooling(
    chunks_path: &str,
    embeddings_path: &str,
    addr: &str,
    sample: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut vectors: std::collections::HashMap<(u64, u32), Vec<f32>> =
        std::collections::HashMap::new();
    let (_, reader) = court::EmbeddingReader::open(std::path::Path::new(embeddings_path))?;
    for record in reader {
        let record = record?;
        vectors.insert((record.opinion_id, record.ordinal), record.vector);
    }
    let mut cosines = Vec::new();
    for chunk in court::read_chunks(std::path::Path::new(chunks_path))? {
        let chunk = chunk?;
        if cosines.len() >= sample {
            break;
        }
        let Some(pooled) = vectors.get(&(chunk.opinion_id, chunk.ordinal)) else {
            continue;
        };
        let response = analyze(addr, &chunk.text, Some(EmbeddingOptions { source: 2 })).await?;
        if response.embeddings.len() < 2 {
            continue; // single-token chunks: agreement is trivial
        }
        let token_vectors: Vec<(&[f32], u32)> = response
            .embeddings
            .iter()
            .map(|e| (e.vector.as_slice(), 1u32))
            .collect();
        let direct = court::pool_block_vectors(&token_vectors, DIM);
        cosines.push(court::cosine(pooled, &direct));
    }
    cosines.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let min = cosines.first().copied().unwrap_or(f64::NAN);
    let median = cosines.get(cosines.len() / 2).copied().unwrap_or(f64::NAN);
    eprintln!(
        "pooling agreement over {} chunks: min cosine {:.6}, median {:.6}",
        cosines.len(),
        min,
        median
    );
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input = arg("input", "/work/court-corpus/opinions-sample.ndjson");
    let output = arg("output", "/work/court-corpus/chunks.ndjson");
    let embeddings_out = arg("embeddings-out", "/work/court-corpus/embeddings-static.bin");
    let sidecar_port: u16 = arg("sidecar-port", "59112").parse()?;
    let analysis_override = arg("analysis-addr", "");
    let target_tokens: usize = arg("target-tokens", "256").parse()?;
    let hard_cap: usize = arg("hard-cap-tokens", "1024").parse()?;
    let concurrency: usize = arg("concurrency", "16").parse()?;
    let limit: u64 = arg("limit", "0").parse()?;
    let verify: usize = arg("verify-pooling", "0").parse()?;

    let mut sidecar_child = None;
    let analysis_addr = if analysis_override.is_empty() {
        let (child, addr) = start_sidecar_with_env(
            SIDECAR_BIN,
            sidecar_port,
            &[("OPENNLP_EMBEDDINGS_DIR", MODEL_DIR)],
        )?;
        sidecar_child = Some(child);
        addr
    } else {
        analysis_override
    };
    eprintln!("analysis sidecar at {analysis_addr} (static embeddings {MODEL_DIR})");

    if verify > 0 {
        return verify_pooling(&output, &embeddings_out, &analysis_addr, verify).await;
    }

    let output_path = PathBuf::from(&output);
    let embeddings_path = PathBuf::from(&embeddings_out);
    let (mut next_chunk_id, skip_lines) = court::chunks_resume_state(&output_path)?;
    let embedded_keys = court::embedded_keys(&embeddings_path)?;
    if skip_lines > 0 {
        eprintln!(
            "resuming: {next_chunk_id} chunks / {} embeddings exist, skipping {skip_lines} input lines",
            embedded_keys.len()
        );
    }

    let (work_tx, work_rx) = mpsc::channel::<(u64, String)>(concurrency * 4);
    let work_rx = Arc::new(Mutex::new(work_rx));
    let (done_tx, mut done_rx) = mpsc::channel::<ChunkedOpinion>(concurrency * 4);
    let analyzed = Arc::new(AtomicU64::new(0));
    let chunked_count = Arc::new(AtomicU64::new(0));

    let mut workers = Vec::new();
    for _ in 0..concurrency {
        let work_rx = work_rx.clone();
        let done_tx = done_tx.clone();
        let addr = analysis_addr.clone();
        let analyzed = analyzed.clone();
        let chunked_count = chunked_count.clone();
        workers.push(tokio::spawn(async move {
            loop {
                let next = work_rx.lock().await.recv().await;
                let Some((src_line, line)) = next else { break };
                match analyze_opinion(&addr, src_line, &line, target_tokens, hard_cap).await {
                    Ok(opinion) => {
                        analyzed.fetch_add(1, Ordering::Relaxed);
                        chunked_count.fetch_add(opinion.chunks.len() as u64, Ordering::Relaxed);
                        if done_tx.send(opinion).await.is_err() {
                            return;
                        }
                    }
                    Err(e) => eprintln!("line {src_line}: {e}"),
                }
            }
        }));
    }
    drop(done_tx);

    // Ordered writer for chunks + embeddings together.
    let out_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&output_path)?;
    let mut chunk_writer = ChunkWriter::new(out_file, next_chunk_id);
    let emb_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&embeddings_path)?;
    let mut emb_writer = if embedded_keys.is_empty() {
        EmbeddingWriter::create(emb_file, DIM as u32)?
    } else {
        EmbeddingWriter::append(emb_file)
    };
    let mut pending: std::collections::BTreeMap<u64, ChunkedOpinion> =
        std::collections::BTreeMap::new();
    let mut watermark = skip_lines;
    let embedded_keys = Arc::new(embedded_keys);
    let embedded_keys_w = embedded_keys.clone();
    let write_opinion = move |chunk_writer: &mut ChunkWriter<std::fs::File>,
                              emb_writer: &mut EmbeddingWriter<std::fs::File>,
                              opinion: &ChunkedOpinion,
                              first_chunk_id: u64| {
        let embedded_keys = &embedded_keys_w;
        let spans: Vec<court::Span> = opinion.chunks.iter().map(|(s, _)| *s).collect();
        chunk_writer
            .write_opinion_chunks(
                opinion.opinion_id,
                opinion.cluster_id,
                opinion.src_line,
                &opinion.text,
                &spans,
            )
            .expect("write chunks");
        for (ordinal, (_, vector)) in opinion.chunks.iter().enumerate() {
            let key = (opinion.opinion_id, ordinal as u32);
            if embedded_keys.contains(&key) {
                continue;
            }
            emb_writer
                .write(key.0, key.1, vector)
                .expect("write embedding");
        }
        first_chunk_id + opinion.chunks.len() as u64
    };
    let writer_task = tokio::spawn(async move {
        while let Some(opinion) = done_rx.recv().await {
            pending.insert(opinion.src_line, opinion);
            while let Some(opinion) = pending.remove(&watermark) {
                next_chunk_id =
                    write_opinion(&mut chunk_writer, &mut emb_writer, &opinion, next_chunk_id);
                watermark += 1;
            }
        }
        for (_, opinion) in pending {
            next_chunk_id =
                write_opinion(&mut chunk_writer, &mut emb_writer, &opinion, next_chunk_id);
        }
        chunk_writer.finish().expect("flush chunks");
        emb_writer.finish().expect("flush embeddings");
        next_chunk_id
    });

    let file = std::fs::File::open(&input)?;
    let t0 = Instant::now();
    let mut fed = 0u64;
    for (i, line) in std::io::BufReader::new(file).lines().enumerate() {
        let line_no = i as u64;
        if line_no < skip_lines {
            continue;
        }
        if limit > 0 && fed >= limit {
            break;
        }
        work_tx.send((line_no, line?)).await?;
        fed += 1;
        if fed.is_multiple_of(2_000) {
            let rate = fed as f64 / t0.elapsed().as_secs_f64();
            let chunks = chunked_count.load(Ordering::Relaxed);
            let crate_ = chunks as f64 / t0.elapsed().as_secs_f64();
            eprintln!("{fed} opinions fed ({rate:.0} opinions/s, {crate_:.0} chunks/s)...");
        }
    }
    drop(work_tx);
    for w in workers {
        w.await?;
    }
    let total_chunks = writer_task.await?;
    let elapsed = t0.elapsed();
    let analyzed_n = analyzed.load(Ordering::Relaxed);
    eprintln!(
        "done: {analyzed_n} opinions analyzed, {total_chunks} chunks+vectors total in {:?} ({:.0} opinions/s, {:.0} chunks/s)",
        elapsed,
        analyzed_n as f64 / elapsed.as_secs_f64(),
        total_chunks as f64 / elapsed.as_secs_f64()
    );

    if let Some(mut child) = sidecar_child {
        let _ = child.kill();
    }
    Ok(())
}
