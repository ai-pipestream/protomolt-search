//! Court pipeline stage 2: stream the chunks file through TEI (native
//! gRPC, bge-m3 1024d) and persist embeddings next to their chunk keys
//! ((opinion_id, ordinal) — the durable resume key, stable across reruns
//! of the chunking pass). Resumable: already-embedded keys are skipped.
//!
//! Guards: TEI's max input is 8192 tokens and the chunker targets ~256,
//! so inputs are always in range; `truncate` stays false so embeddings
//! always cover the full chunk text (no silent truncation against the
//! contiguity invariant).
//!
//! ```text
//! court_embed --chunks=/work/court-corpus/chunks.ndjson \
//!     --output=/work/court-corpus/embeddings.bin --limit=50000
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{mpsc, Mutex};
use turbovec_search::demo::court::{self, EmbeddingWriter};
use turbovec_search::pb::tei::embed_client::EmbedClient;
use turbovec_search::pb::tei::EmbedRequest;

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

struct Embedded {
    opinion_id: u64,
    ordinal: u32,
    vector: Vec<f32>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let chunks_path = arg("chunks", "/work/court-corpus/chunks.ndjson");
    let output = arg("output", "/work/court-corpus/embeddings.bin");
    let tei_addr = arg("tei-addr", "http://127.0.0.1:8095");
    // Models with short input caps (all-MiniLM-L6-v2: 256 tokens) need
    // server-side truncation or every long chunk errors; bge-m3 (8192)
    // keeps the historical default of no truncation.
    let truncate = arg("truncate", "false") == "true";
    let concurrency: usize = arg("concurrency", "32").parse()?;
    // Embedding dimension the output header declares. MUST match what
    // the TEI model actually produces (validated on the first record):
    // bge-m3 1024, all-MiniLM-L6-v2 384.
    let dim: usize = arg("dim", "1024").parse()?;
    let limit: u64 = arg("limit", "0").parse()?;

    let done = court::embedded_keys(std::path::Path::new(&output))?;
    if !done.is_empty() {
        eprintln!("resuming: {} embeddings already present", done.len());
    }
    let append = !done.is_empty();

    let (work_tx, work_rx) = mpsc::channel::<court::Chunk>(concurrency * 8);
    let work_rx = Arc::new(Mutex::new(work_rx));
    let (done_tx, mut done_rx) = mpsc::channel::<Embedded>(concurrency * 8);
    let embedded = Arc::new(AtomicU64::new(0));

    let mut workers = Vec::new();
    for _ in 0..concurrency {
        let work_rx = work_rx.clone();
        let done_tx = done_tx.clone();
        let addr = tei_addr.clone();
        let embedded = embedded.clone();
        workers.push(tokio::spawn(async move {
            let mut client = EmbedClient::connect(addr.clone())
                .await
                .expect("connect TEI");
            loop {
                let next = work_rx.lock().await.recv().await;
                let Some(chunk) = next else { break };
                // Short-cap models read ~256 tokens; capping the payload
                // client-side keeps one outlier chunk from tearing down
                // the connection for every in-flight request.
                let text: String = if truncate {
                    chunk.text.chars().take(4000).collect()
                } else {
                    chunk.text
                };
                let request = || EmbedRequest {
                    inputs: text.clone(),
                    truncate,
                    normalize: Some(true),
                    truncation_direction: 0,
                    prompt_name: None,
                    dimensions: None,
                };
                let mut attempt = 0;
                let response = loop {
                    match client.embed(request()).await {
                        Ok(r) => break Ok(r),
                        Err(_) if attempt < 5 => {
                            attempt += 1;
                            tokio::time::sleep(std::time::Duration::from_millis(500 * attempt))
                                .await;
                            if let Ok(fresh) = EmbedClient::connect(addr.clone()).await {
                                client = fresh;
                            }
                        }
                        Err(e) => break Err(e),
                    }
                }
                .expect("embed");
                let vector = response.into_inner().embeddings;
                embedded.fetch_add(1, Ordering::Relaxed);
                if done_tx
                    .send(Embedded {
                        opinion_id: chunk.opinion_id,
                        ordinal: chunk.ordinal,
                        vector,
                    })
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }));
    }
    drop(done_tx);

    let out_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&output)?;
    let mut writer = if append {
        EmbeddingWriter::append(out_file)
    } else {
        EmbeddingWriter::create(out_file, dim as u32)?
    };
    let writer_task = tokio::spawn(async move {
        while let Some(record) = done_rx.recv().await {
            assert_eq!(
                record.vector.len(),
                dim,
                "TEI returned a different dim than the output declares (fix --dim)"
            );
            writer
                .write(record.opinion_id, record.ordinal, &record.vector)
                .expect("write embedding");
        }
        writer.finish().expect("flush embeddings");
    });

    let t0 = Instant::now();
    let mut fed = 0u64;
    let mut skipped = 0u64;
    for chunk in court::read_chunks(std::path::Path::new(&chunks_path))? {
        let chunk = chunk?;
        if limit > 0 && fed >= limit {
            break;
        }
        if done.contains(&(chunk.opinion_id, chunk.ordinal)) {
            skipped += 1;
            continue;
        }
        work_tx.send(chunk).await?;
        fed += 1;
        if fed.is_multiple_of(10_000) {
            let rate = fed as f64 / t0.elapsed().as_secs_f64();
            eprintln!("{fed} chunks fed ({rate:.0} chunks/s, {skipped} skipped)...");
        }
    }
    drop(work_tx);
    for w in workers {
        w.await?;
    }
    writer_task.await?;
    let elapsed = t0.elapsed();
    let n = embedded.load(Ordering::Relaxed);
    eprintln!(
        "done: {n} chunks embedded in {:?} ({:.0} chunks/s, {skipped} skipped on resume)",
        elapsed,
        n as f64 / elapsed.as_secs_f64()
    );
    Ok(())
}
