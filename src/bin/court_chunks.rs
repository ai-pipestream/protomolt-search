//! Court pipeline stage 1: stream the opinions NDJSON through the REAL
//! OpenNLP analysis sidecar and write sentence-aware chunks (~256
//! whitespace tokens, configurable) with lineage and a contiguity
//! invariant (concatenating an opinion's chunk texts in ordinal order
//! reproduces the original text byte-for-byte — asserted per opinion).
//!
//! Output: NDJSON chunks file (one record per line, see `court::Chunk`),
//! streamable and resumable — a rerun scans the existing file, continues
//! chunk ids, and skips input lines already processed.
//!
//! ```text
//! court_chunks --input=/work/court-corpus/opinions-sample.ndjson \
//!     --output=/work/court-corpus/chunks.ndjson --limit=10000
//! ```

use std::io::BufRead;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{mpsc, Mutex};
use turbovec_search::court::{self, ChunkWriter, Span};
use turbovec_search::harness::start_sidecar;
use turbovec_search::pb::analysis::analysis_service_client::AnalysisServiceClient;
use turbovec_search::pb::analysis::{AnalysisOptions, AnalyzeRequest};

const SIDECAR_BIN: &str =
    "/work/main/grpc-services/grpc-opennlp-analysis/build/native/nativeCompile/grpc-opennlp-analysis";

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

/// One completed opinion's chunks, tagged with its input line for
/// ordered writing.
struct Chunked {
    src_line: u64,
    opinion_id: u64,
    cluster_id: u64,
    text: String,
    spans: Vec<Span>,
}

async fn analyze_sentences(
    addr: &str,
    text: &str,
) -> Result<(Vec<Span>, Vec<Span>), tonic::Status> {
    let mut client = AnalysisServiceClient::connect(addr.to_string())
        .await
        .map_err(|e| tonic::Status::unavailable(format!("analysis sidecar: {e}")))?;
    let response = client
        .analyze(AnalyzeRequest {
            text: text.to_string(),
            options: Some(AnalysisOptions {
                tokenizer: 1, // WHITESPACE
                sentence_detection: true,
                ..Default::default()
            }),
        })
        .await?
        .into_inner();
    let sentences = response
        .sentences
        .iter()
        .map(|s| Span {
            start: s.start.max(0) as u32,
            end: s.end.max(0) as u32,
        })
        .collect();
    let tokens = response
        .tokens
        .iter()
        .filter_map(|t| {
            t.span.map(|s| Span {
                start: s.start.max(0) as u32,
                end: s.end.max(0) as u32,
            })
        })
        .collect();
    Ok((sentences, tokens))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input = arg("input", "/work/court-corpus/opinions-sample.ndjson");
    let output = arg("output", "/work/court-corpus/chunks.ndjson");
    let sidecar_port: u16 = arg("sidecar-port", "59101").parse()?;
    let target_tokens: usize = arg("target-tokens", "256").parse()?;
    let concurrency: usize = arg("concurrency", "16").parse()?;
    let limit: u64 = arg("limit", "0").parse()?;

    let output_path = PathBuf::from(&output);
    let (next_chunk_id, skip_lines) = court::chunks_resume_state(&output_path)?;
    if skip_lines > 0 {
        eprintln!("resuming: {next_chunk_id} chunks exist, skipping {skip_lines} input lines");
    }

    let (mut sidecar, analysis_addr) = start_sidecar(SIDECAR_BIN, sidecar_port)?;
    eprintln!("analysis sidecar at {analysis_addr}");

    // Producer -> workers -> ordered writer.
    let (work_tx, work_rx) = mpsc::channel::<(u64, String)>(concurrency * 4);
    let work_rx = Arc::new(Mutex::new(work_rx));
    let (done_tx, mut done_rx) = mpsc::channel::<Chunked>(concurrency * 4);
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
                let (opinion_id, cluster_id, text) = match court::parse_opinion(&line) {
                    Ok(o) => o,
                    Err(e) => {
                        eprintln!("line {src_line}: {e}");
                        continue;
                    }
                };
                match analyze_sentences(&addr, &text).await {
                    Ok((sentences, tokens)) => {
                        let spans =
                            court::assemble_chunks(&text, &sentences, &tokens, target_tokens);
                        if !court::is_contiguous(&text, &spans) {
                            eprintln!("line {src_line}: CONTIGUITY VIOLATION, opinion skipped");
                            continue;
                        }
                        analyzed.fetch_add(1, Ordering::Relaxed);
                        chunked_count.fetch_add(spans.len() as u64, Ordering::Relaxed);
                        if done_tx
                            .send(Chunked {
                                src_line,
                                opinion_id,
                                cluster_id,
                                text,
                                spans,
                            })
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Err(e) => eprintln!("line {src_line}: analysis failed: {e}"),
                }
            }
        }));
    }
    drop(done_tx);

    // Writer: flush opinions in input-line order via a watermark buffer.
    let out_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&output_path)?;
    let mut writer = ChunkWriter::new(out_file, next_chunk_id);
    let mut pending: std::collections::BTreeMap<u64, Chunked> = std::collections::BTreeMap::new();
    let mut watermark = skip_lines;
    let writer_task = tokio::spawn(async move {
        while let Some(chunked) = done_rx.recv().await {
            pending.insert(chunked.src_line, chunked);
            while let Some(chunked) = pending.remove(&watermark) {
                writer
                    .write_opinion_chunks(
                        chunked.opinion_id,
                        chunked.cluster_id,
                        chunked.src_line,
                        &chunked.text,
                        &chunked.spans,
                    )
                    .expect("write chunk");
                watermark += 1;
            }
        }
        // Drain any stragglers (out-of-order tail).
        for (_, chunked) in pending {
            writer
                .write_opinion_chunks(
                    chunked.opinion_id,
                    chunked.cluster_id,
                    chunked.src_line,
                    &chunked.text,
                    &chunked.spans,
                )
                .expect("write chunk");
        }
        writer.finish().expect("flush chunks")
    });

    // Producer.
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
        "done: {analyzed_n} opinions analyzed, {total_chunks} chunks total in {:?} ({:.0} opinions/s, {:.0} chunks/s)",
        elapsed,
        analyzed_n as f64 / elapsed.as_secs_f64(),
        total_chunks as f64 / elapsed.as_secs_f64()
    );

    let _ = sidecar.kill();
    Ok(())
}
