//! Throughput of the analysis sidecar under concurrency, per layer set.
//!
//! `annotation_cost` measures LATENCY on one connection, which is the
//! wrong number for planning a corpus pass: the sidecar is thread safe
//! and the pass is embarrassingly parallel, so what matters is where
//! throughput stops scaling. This runs the same work at increasing
//! concurrency and reports where that is.
//!
//! Throughput is reported in MB/s as well as docs/s, because docs/s is
//! not portable between corpora: a 1.2 KB chunk and a 10 KB opinion are
//! both "a doc" and differ by an order of magnitude in work.
//!
//! ```text
//! annotation_throughput --addr=http://127.0.0.1:59202 --n=100 --levels=1,4,8,16,32
//! ```
use pipestream_search::pb::analysis::analysis_service_client::AnalysisServiceClient;
use pipestream_search::pb::analysis::{
    analyze_stream_request, AnalysisOptions, AnalyzeRequest, AnalyzeStreamDoc,
    AnalyzeStreamRequest, TermVectorOptions,
};
use std::sync::Arc;
use std::time::Instant;

fn arg(key: &str, default: &str) -> String {
    let p = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&p).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

fn base() -> AnalysisOptions {
    let spec = pipestream_search::analyzer::body_spec();
    AnalysisOptions {
        language: "en".into(),
        tokenizer: spec.tokenizer,
        stemmer: spec.stemmer,
        term_vectors: Some(TermVectorOptions {
            enabled: true,
            mode: spec.term_vector_mode,
            steps: spec.char_filters.clone(),
            source: spec.term_vector_source,
            dual_cased: false,
        }),
        ..Default::default()
    }
}

/// Baseline plus the NER capture set. NER consumes the sentence layer
/// and is refused without it, so the pair is the honest unit.
fn ner() -> AnalysisOptions {
    AnalysisOptions {
        sentence_detection: true,
        ner: true,
        ..base()
    }
}

async fn run_at(
    client: &AnalysisServiceClient<tonic::transport::Channel>,
    opts: &AnalysisOptions,
    texts: &Arc<Vec<String>>,
    concurrency: usize,
) -> (f64, usize) {
    let sem = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let start = Instant::now();
    let mut tasks = Vec::with_capacity(texts.len());
    for i in 0..texts.len() {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let mut c = client.clone();
        let o = opts.clone();
        let t = texts.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = permit;
            let r = c
                .analyze(AnalyzeRequest {
                    text: t[i].clone(),
                    options: Some(o),
                })
                .await;
            match r {
                Ok(resp) => resp.into_inner().entities.len(),
                Err(_) => usize::MAX,
            }
        }));
    }
    let mut entities = 0usize;
    let mut failed = 0usize;
    for task in tasks {
        match task.await.unwrap() {
            usize::MAX => failed += 1,
            n => entities += n,
        }
    }
    let secs = start.elapsed().as_secs_f64();
    if failed > 0 {
        eprintln!("  ({failed} calls failed at concurrency {concurrency})");
    }
    (secs, entities)
}

/// The same work over N concurrent AnalyzeStream sessions instead of N
/// concurrent unary calls. If the server's limit is transport or
/// per-call overhead, this beats unary; if it is a lock around the
/// annotator, both land in the same place.
async fn run_streams(
    client: &AnalysisServiceClient<tonic::transport::Channel>,
    opts: &AnalysisOptions,
    texts: &Arc<Vec<String>>,
    streams: usize,
) -> (f64, usize) {
    let start = Instant::now();
    let per = texts.len().div_ceil(streams);
    let mut tasks = Vec::new();
    for s in 0..streams {
        let lo = s * per;
        let hi = ((s + 1) * per).min(texts.len());
        if lo >= hi {
            break;
        }
        let mut c = client.clone();
        let o = opts.clone();
        let t = texts.clone();
        tasks.push(tokio::spawn(async move {
            let (tx, rx) = tokio::sync::mpsc::channel(32);
            tx.send(AnalyzeStreamRequest {
                msg: Some(analyze_stream_request::Msg::Options(o)),
            })
            .await
            .ok();
            let feeder = tokio::spawn(async move {
                for (i, idx) in (lo..hi).enumerate() {
                    if tx
                        .send(AnalyzeStreamRequest {
                            msg: Some(analyze_stream_request::Msg::Doc(AnalyzeStreamDoc {
                                sequence: i as u64,
                                text: t[idx].clone(),
                            })),
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });
            let mut entities = 0usize;
            match c
                .analyze_stream(tokio_stream::wrappers::ReceiverStream::new(rx))
                .await
            {
                Ok(resp) => {
                    let mut inbound = resp.into_inner();
                    while let Ok(Some(msg)) = inbound.message().await {
                        if let Some(
                            pipestream_search::pb::analysis::analyze_stream_response::Result::Ok(r),
                        ) = msg.result
                        {
                            entities += r.entities.len();
                        }
                    }
                }
                Err(e) => eprintln!("  stream failed: {}", e.message()),
            }
            feeder.abort();
            entities
        }));
    }
    let mut entities = 0usize;
    for t in tasks {
        entities += t.await.unwrap_or(0);
    }
    (start.elapsed().as_secs_f64(), entities)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = arg("addr", "http://127.0.0.1:59202");
    let path = arg("chunks", "/work/court-corpus/canary-chunks.ndjson");
    let n: usize = arg("n", "100").parse()?;
    let levels: Vec<usize> = arg("levels", "1,2,4,8,16,32,64")
        .split(',')
        .map(|s| s.trim().parse())
        .collect::<Result<_, _>>()?;

    let texts: Arc<Vec<String>> = {
        use std::io::BufRead;
        let f = std::io::BufReader::new(std::fs::File::open(&path)?);
        Arc::new(
            f.lines()
                .map_while(Result::ok)
                .filter_map(|l| {
                    let v: serde_json::Value = serde_json::from_str(&l).ok()?;
                    let t = v.get("text")?.as_str()?.to_string();
                    (t.len() > 200).then_some(t)
                })
                .take(n)
                .collect(),
        )
    };
    let bytes: usize = texts.iter().map(String::len).sum();
    println!(
        "{} docs, {} bytes total, mean {} bytes/doc",
        texts.len(),
        bytes,
        bytes / texts.len().max(1)
    );

    let client = AnalysisServiceClient::connect(addr.clone()).await?;
    // Warm before the first timed level, so level 1 is not paying for
    // everything the runtime does lazily.
    let _ = run_at(&client, &base(), &texts, 8).await;
    let _ = run_at(&client, &ner(), &texts, 8).await;

    for (label, opts) in [("term_vectors only", base()), ("+ sentences + NER", ner())] {
        println!("\n{label}");
        println!(
            "{:>6}{:>12}{:>12}{:>10}{:>12}{:>12}",
            "conc", "unary/s", "MB/s", "scaling", "entities", "stream/s"
        );
        let mut first = 0f64;
        for &c in &levels {
            let (secs, entities) = run_at(&client, &opts, &texts, c).await;
            let dps = texts.len() as f64 / secs;
            let mbps = bytes as f64 / secs / 1_048_576.0;
            if first == 0.0 {
                first = dps;
            }
            let (ssecs, sent) = run_streams(&client, &opts, &texts, c).await;
            let sdps = texts.len() as f64 / ssecs;
            println!(
                "{:>6}{:>12.1}{:>12.2}{:>9.1}x{:>12}{:>12.1}",
                c,
                dps,
                mbps,
                dps / first,
                entities,
                sdps
            );
            let _ = sent;
        }
    }
    println!(
        "\nProjection for 86,633,399 chunks at the best rate above:\n  divide 86633399 by docs/s, then by 3600 for hours."
    );
    Ok(())
}
