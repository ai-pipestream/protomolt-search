//! Court pipeline stage 1 (pure-Rust static-embedding path): stream the
//! opinions NDJSON through `turbo-client`'s in-process sentence + lexical
//! analyzer and `protomolt-embedder`'s memory-mapped Model2Vec table to
//! produce sentence-aware chunks AND their embeddings in one pass without
//! any external JVM sidecar.
//!
//! Outputs (both resumable): `chunks.ndjson` (lineage) and `embeddings.bin`
//! (keyed by `(opinion_id, ordinal)`).

use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use pipestream_search::demo::court::{self, ChunkSource, ChunkWriter, EmbeddingWriter};
use protomolt_embedder::StaticEmbedder;
use tokio::sync::{mpsc, Mutex};
use turbo_client::{
    FieldAnalysisConfig, LexicalAnalyzer, StemmerKind, TermSourceKind, TokenizerKind,
};

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
    /// `(span, vector)` per planned chunk, in ordinal order.
    chunks: Vec<(court::Span, Vec<f32>)>,
}

fn whitespace_analyzer() -> LexicalAnalyzer {
    LexicalAnalyzer::new(FieldAnalysisConfig {
        tokenizer: TokenizerKind::Whitespace,
        stemmer: StemmerKind::None,
        term_source: TermSourceKind::Tokens,
        normalizers: Vec::new(),
        sentence_language: "en".to_string(),
        emit_lattice: false,
    })
}

fn embed_or_unit(embedder: &StaticEmbedder, text: &str) -> Vec<f32> {
    embedder.embed(text).unwrap_or_else(|| {
        let mut fallback = vec![0.0f32; embedder.dim()];
        if let Some(first) = fallback.first_mut() {
            *first = 1.0;
        }
        fallback
    })
}

fn chunk_vector(
    embedder: &StaticEmbedder,
    text: &str,
    plan: &court::ChunkPlan,
    block_vectors: &[(court::Span, Vec<f32>)],
    tokens: &[court::Span],
) -> Vec<f32> {
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
            court::pool_block_vectors(&weighted, embedder.dim())
        }
        ChunkSource::SoloPiece => {
            let piece_text = court::slice_chars(text, plan.span.start, plan.span.end);
            embed_or_unit(embedder, &piece_text)
        }
    }
}

fn analyze_opinion(
    analyzer: &LexicalAnalyzer,
    embedder: &StaticEmbedder,
    src_line: u64,
    line: &str,
    target_tokens: usize,
    hard_cap: usize,
) -> Result<ChunkedOpinion, String> {
    let (opinion_id, cluster_id, text) = court::parse_opinion(line)?;
    let field = analyzer
        .analyze(&text)
        .map_err(|e| format!("analyze: {e}"))?;
    let sentences: Vec<court::Span> = field
        .sentences
        .iter()
        .map(|s| court::Span {
            start: s.spans.code_point.0,
            end: s.spans.code_point.1,
        })
        .collect();
    let tokens: Vec<court::Span> = field
        .arcs
        .iter()
        .map(|t| court::Span {
            start: t.spans.code_point.0,
            end: t.spans.code_point.1,
        })
        .collect();
    let mut block_vectors: Vec<(court::Span, Vec<f32>)> = Vec::with_capacity(sentences.len());
    for (sent, span) in field.sentences.iter().zip(sentences.iter().copied()) {
        let slice = &text[sent.spans.utf8.0 as usize..sent.spans.utf8.1 as usize];
        block_vectors.push((span, embed_or_unit(embedder, slice)));
    }

    let plans = court::plan_chunks(&text, &sentences, &tokens, target_tokens, hard_cap);
    let spans: Vec<court::Span> = plans.iter().map(|p| p.span).collect();
    if !court::is_contiguous(&text, &spans) {
        return Err("CONTIGUITY VIOLATION".to_string());
    }
    let chunks = plans
        .iter()
        .map(|plan| {
            let vector = chunk_vector(embedder, &text, plan, &block_vectors, &tokens);
            (plan.span, vector)
        })
        .collect();
    Ok(ChunkedOpinion {
        src_line,
        opinion_id,
        cluster_id,
        text,
        chunks,
    })
}

fn verify_pooling(
    chunks_path: &str,
    embeddings_path: &str,
    embedder: &StaticEmbedder,
    sample: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut vectors: std::collections::HashMap<(u64, u32), Vec<f32>> =
        std::collections::HashMap::new();
    let (_, reader) = court::EmbeddingReader::open(Path::new(embeddings_path))?;
    for record in reader {
        let record = record?;
        vectors.insert((record.opinion_id, record.ordinal), record.vector);
    }
    let mut cosines = Vec::new();
    for chunk in court::read_chunks(Path::new(chunks_path))? {
        let chunk = chunk?;
        if cosines.len() >= sample {
            break;
        }
        let Some(pooled) = vectors.get(&(chunk.opinion_id, chunk.ordinal)) else {
            continue;
        };
        let Some(direct) = embedder.embed(&chunk.text) else {
            continue;
        };
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
    let input = arg("input", "/work/court-corpus/opinions.ndjson");
    let output = arg("output", "/work/court-corpus/chunks.ndjson");
    let embeddings_out = arg("embeddings-out", "/work/court-corpus/embeddings-static.bin");
    let model_dir = arg("model-dir", MODEL_DIR);
    let target_tokens: usize = arg("target-tokens", "256").parse()?;
    let hard_cap: usize = arg("hard-cap-tokens", "1024").parse()?;
    let concurrency: usize = arg("concurrency", "16").parse()?;
    let limit: u64 = arg("limit", "0").parse()?;
    let verify: usize = arg("verify-pooling", "0").parse()?;

    let embedder = Arc::new(StaticEmbedder::load(Path::new(&model_dir))?);
    if embedder.dim() != DIM {
        eprintln!(
            "loaded static embeddings from {model_dir} (dim {}, rows {})",
            embedder.dim(),
            embedder.rows()
        );
    }

    if verify > 0 {
        return verify_pooling(&output, &embeddings_out, &embedder, verify);
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
    let failed = Arc::new(AtomicU64::new(0));
    let (done_tx, mut done_rx) = mpsc::channel::<Result<ChunkedOpinion, u64>>(concurrency * 4);
    let analyzed = Arc::new(AtomicU64::new(0));
    let chunked_count = Arc::new(AtomicU64::new(0));
    let analyzer = Arc::new(whitespace_analyzer());

    let mut workers = Vec::new();
    for _ in 0..concurrency {
        let work_rx = work_rx.clone();
        let done_tx = done_tx.clone();
        let analyzer = analyzer.clone();
        let embedder = embedder.clone();
        let analyzed = analyzed.clone();
        let chunked_count = chunked_count.clone();
        let failed = failed.clone();
        workers.push(tokio::spawn(async move {
            loop {
                let next = work_rx.lock().await.recv().await;
                let Some((src_line, line)) = next else { break };
                match analyze_opinion(
                    &analyzer,
                    &embedder,
                    src_line,
                    &line,
                    target_tokens,
                    hard_cap,
                ) {
                    Ok(opinion) => {
                        analyzed.fetch_add(1, Ordering::Relaxed);
                        chunked_count.fetch_add(opinion.chunks.len() as u64, Ordering::Relaxed);
                        if done_tx.send(Ok(opinion)).await.is_err() {
                            return;
                        }
                    }
                    Err(e) => {
                        failed.fetch_add(1, Ordering::Relaxed);
                        eprintln!("line {src_line}: {e}");
                        if done_tx.send(Err(src_line)).await.is_err() {
                            return;
                        }
                    }
                }
            }
        }));
    }
    drop(done_tx);

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
        EmbeddingWriter::create(emb_file, embedder.dim() as u32)?
    } else {
        EmbeddingWriter::append(emb_file)
    };
    let mut pending: std::collections::BTreeMap<u64, Option<ChunkedOpinion>> =
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
        let mut stalled = false;
        'recv: while let Some(done) = done_rx.recv().await {
            match done {
                Ok(opinion) => pending.insert(opinion.src_line, Some(opinion)),
                Err(src_line) => pending.insert(src_line, None),
            };
            while let Some(slot) = pending.remove(&watermark) {
                let Some(opinion) = slot else {
                    eprintln!(
                        "writer: line {watermark} failed analysis; output ends at the last \
                         contiguous line so a rerun resumes exactly here"
                    );
                    stalled = true;
                    break 'recv;
                };
                next_chunk_id =
                    write_opinion(&mut chunk_writer, &mut emb_writer, &opinion, next_chunk_id);
                watermark += 1;
            }
        }
        if stalled {
            pending.clear();
            while done_rx.recv().await.is_some() {}
        } else {
            for (src_line, slot) in std::mem::take(&mut pending) {
                let Some(opinion) = slot else { break };
                if src_line != watermark {
                    break;
                }
                next_chunk_id =
                    write_opinion(&mut chunk_writer, &mut emb_writer, &opinion, next_chunk_id);
                watermark += 1;
            }
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
        if failed.load(Ordering::Relaxed) > 0 {
            eprintln!("stopping feed at line {line_no}: an earlier line failed analysis");
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
    let failed_n = failed.load(Ordering::Relaxed);
    eprintln!(
        "done: {analyzed_n} opinions analyzed, {total_chunks} chunks+vectors total in {:?} ({:.0} opinions/s, {:.0} chunks/s)",
        elapsed,
        analyzed_n as f64 / elapsed.as_secs_f64(),
        total_chunks as f64 / elapsed.as_secs_f64()
    );

    if failed_n > 0 || analyzed_n + failed_n != fed {
        eprintln!(
            "LOST OPINIONS: fed {fed}, analyzed {analyzed_n}, failed {failed_n} - \
             the corpus is NOT fully chunked"
        );
        std::process::exit(1);
    }
    Ok(())
}
