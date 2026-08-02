//! Does removing a lock change the answers?
//!
//! Compares a REFERENCE sidecar driven serially against a CANDIDATE
//! sidecar driven at high concurrency, asserting the annotations match
//! per document. A data race in a shared annotator shows up as answers
//! that differ from the serial ones, so serial-reference versus
//! concurrent-candidate is the comparison that can actually catch it.
//!
//! ```text
//! annotator_race_check --ref=http://127.0.0.1:59220 --cand=http://127.0.0.1:59221 \
//!     --n=400 --concurrency=32
//! ```
use std::sync::Arc;
use turbovec_search::pb::analysis::analysis_service_client::AnalysisServiceClient;
use turbovec_search::pb::analysis::{AnalysisOptions, AnalyzeRequest, TermVectorOptions};

fn arg(key: &str, default: &str) -> String {
    let p = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&p).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

fn opts() -> AnalysisOptions {
    let spec = turbovec_search::analyzer::body_spec();
    AnalysisOptions {
        language: "en".into(),
        tokenizer: spec.tokenizer,
        stemmer: spec.stemmer,
        sentence_detection: true,
        ner: true,
        pos_tags: true,
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

/// Everything the guarded annotators produce, flattened to a comparable
/// string: entity spans with their types, and the POS tag sequence.
fn fingerprint(r: &turbovec_search::pb::analysis::AnalyzeResponse) -> String {
    let mut ents: Vec<String> = r
        .entities
        .iter()
        .map(|e| {
            let (s, t) = e.span.map(|s| (s.start, s.end)).unwrap_or((0, 0));
            format!("{}:{}:{}:{}", e.r#type, s, t, e.text)
        })
        .collect();
    ents.sort();
    let pos: Vec<&str> = r.tokens.iter().map(|t| t.pos.as_str()).collect();
    format!("E[{}] P[{}]", ents.join(","), pos.join(" "))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reference = arg("ref", "http://127.0.0.1:59220");
    let candidate = arg("cand", "http://127.0.0.1:59221");
    let n: usize = arg("n", "400").parse()?;
    let concurrency: usize = arg("concurrency", "32").parse()?;
    let path = arg("chunks", "/work/court-corpus/canary-chunks.ndjson");

    let texts: Arc<Vec<String>> = {
        use std::io::BufRead;
        let f = std::io::BufReader::new(std::fs::File::open(&path)?);
        Arc::new(
            f.lines()
                .filter_map(|l| l.ok())
                .filter_map(|l| {
                    let v: serde_json::Value = serde_json::from_str(&l).ok()?;
                    let t = v.get("text")?.as_str()?.to_string();
                    (t.len() > 400).then_some(t)
                })
                .take(n)
                .collect(),
        )
    };
    println!("{} docs; reference serial on {reference}, candidate x{concurrency} on {candidate}", texts.len());

    // Reference: one at a time, so nothing can interfere with it.
    let mut rc = AnalysisServiceClient::connect(reference).await?;
    let mut expected = Vec::with_capacity(texts.len());
    for t in texts.iter() {
        let r = rc
            .analyze(AnalyzeRequest { text: t.clone(), options: Some(opts()) })
            .await?
            .into_inner();
        expected.push(fingerprint(&r));
    }
    let entities: usize = expected.iter().filter(|f| !f.starts_with("E[]")).count();
    println!("reference done: {} docs, {} with at least one entity", expected.len(), entities);

    // Candidate: all at once.
    let cc = AnalysisServiceClient::connect(candidate).await?;
    let sem = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut tasks = Vec::new();
    for i in 0..texts.len() {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let mut c = cc.clone();
        let t = texts.clone();
        tasks.push(tokio::spawn(async move {
            let _p = permit;
            c.analyze(AnalyzeRequest { text: t[i].clone(), options: Some(opts()) })
                .await
                .map(|r| fingerprint(&r.into_inner()))
                .map_err(|e| e.message().to_string())
        }));
    }
    let mut mismatches = 0usize;
    let mut errors = 0usize;
    for (i, task) in tasks.into_iter().enumerate() {
        match task.await.unwrap() {
            Err(e) => {
                errors += 1;
                if errors <= 3 {
                    println!("  doc {i}: ERROR {e}");
                }
            }
            Ok(got) if got != expected[i] => {
                mismatches += 1;
                if mismatches <= 3 {
                    println!("  doc {i} DIFFERS\n    serial:     {}\n    concurrent: {}",
                             &expected[i][..expected[i].len().min(160)],
                             &got[..got.len().min(160)]);
                }
            }
            Ok(_) => {}
        }
    }
    println!("\nmismatches: {mismatches}   errors: {errors}   of {} docs", texts.len());
    if mismatches == 0 && errors == 0 {
        println!("PASS: concurrent candidate agrees with the serial reference on every document.");
    } else {
        println!("FAIL: the candidate does not reproduce serial results.");
        std::process::exit(1);
    }
    Ok(())
}
