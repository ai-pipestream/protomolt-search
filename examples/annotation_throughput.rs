//! Throughput of an analysis backend under concurrency, per layer set.
//!
//! Compares N concurrent `analyze_document_with_options` calls against N
//! concurrent `AnalyzeStream` sessions across `"turbo"` (in-process) or a
//! remote `turbo-kserve` endpoint.
//!
//! ```text
//! annotation_throughput --addr=turbo --n=100 --levels=1,4,8,16,32
//! ```

use std::sync::Arc;
use std::time::Instant;

use pipestream_search::analyzer::{
    analyze_document_with_options, body_spec, AnalyzeStream, RemoteCallOptions, SessionLayers,
};

fn arg(key: &str, default: &str) -> String {
    let p = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&p).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

fn base() -> SessionLayers {
    SessionLayers::default()
}

fn ner() -> SessionLayers {
    SessionLayers {
        sentences: true,
        entities: true,
        ..Default::default()
    }
}

async fn run_at(
    addr: &str,
    layers: SessionLayers,
    texts: &Arc<Vec<String>>,
    concurrency: usize,
) -> (f64, usize) {
    let sem = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let spec = body_spec();
    let start = Instant::now();
    let mut tasks = Vec::with_capacity(texts.len());
    for i in 0..texts.len() {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let a = addr.to_string();
        let spec = spec.clone();
        let t = texts.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = permit;
            match analyze_document_with_options(
                &a,
                &t[i],
                Some(&spec),
                layers,
                &RemoteCallOptions::default(),
            )
            .await
            {
                Ok(doc) => doc.entities.len(),
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

async fn run_streams(
    addr: &str,
    layers: SessionLayers,
    texts: &Arc<Vec<String>>,
    streams: usize,
) -> (f64, usize) {
    let start = Instant::now();
    let per = texts.len().div_ceil(streams);
    let spec = body_spec();
    let mut tasks = Vec::new();
    for s in 0..streams {
        let lo = s * per;
        let hi = ((s + 1) * per).min(texts.len());
        if lo >= hi {
            break;
        }
        let a = addr.to_string();
        let spec = spec.clone();
        let t = texts.clone();
        tasks.push(tokio::spawn(async move {
            let mut stream =
                match AnalyzeStream::open_with_vocab(&a, Some(&spec), None, layers).await {
                    Ok(st) => st,
                    Err(e) => {
                        eprintln!("  stream open failed: {}", e.message());
                        return 0;
                    }
                };
            let submit = stream.submitter();
            let feeder = tokio::spawn(async move {
                for (i, idx) in (lo..hi).enumerate() {
                    if submit.submit(i as u64, &t[idx]).await.is_err() {
                        break;
                    }
                }
            });
            let mut entities = 0usize;
            let expected = hi - lo;
            let mut received = 0usize;
            while received < expected {
                match stream.next().await {
                    Ok(Some((_, Ok(doc)))) => {
                        entities += doc.entities.len();
                        received += 1;
                    }
                    Ok(Some((_, Err(_)))) => {
                        received += 1;
                    }
                    _ => break,
                }
            }
            let _ = feeder.await;
            stream.finish();
            let _ = stream.drain_to_end().await;
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
    let addr = arg("addr", "turbo");
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

    let _ = run_at(&addr, base(), &texts, 8).await;
    let _ = run_at(&addr, ner(), &texts, 8).await;

    for (label, opts) in [("term_vectors only", base()), ("+ sentences + NER", ner())] {
        println!("\n{label}");
        println!(
            "{:>6}{:>12}{:>12}{:>10}{:>12}{:>12}",
            "conc", "unary/s", "MB/s", "scaling", "entities", "stream/s"
        );
        let mut first = 0f64;
        for &c in &levels {
            let (secs, entities) = run_at(&addr, opts, &texts, c).await;
            let dps = texts.len() as f64 / secs.max(1e-9);
            let mbps = bytes as f64 / secs.max(1e-9) / 1_048_576.0;
            if first == 0.0 {
                first = dps;
            }
            let (ssecs, sent) = run_streams(&addr, opts, &texts, c).await;
            let sdps = texts.len() as f64 / ssecs.max(1e-9);
            println!(
                "{:>6}{:>12.1}{:>12.2}{:>9.1}x{:>12}{:>12.1}",
                c,
                dps,
                mbps,
                dps / first.max(1e-9),
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
