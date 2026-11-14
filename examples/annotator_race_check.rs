//! Does removing a lock change the answers?
//!
//! Compares a REFERENCE analysis backend driven serially against a CANDIDATE
//! backend driven at high concurrency, asserting the annotations match per
//! document. Works with `"turbo"` (in-process) and `turbo-kserve` endpoints.
//!
//! ```text
//! annotator_race_check --ref=http://127.0.0.1:59220 --cand=http://127.0.0.1:59221 \
//!     --n=400 --concurrency=32
//! ```

use std::sync::Arc;

use pipestream_search::analyzer::{
    analyze_document_with_options, body_spec, RemoteCallOptions, SessionLayers,
};
use pipestream_search::postings::AnalyzedDoc;

fn arg(key: &str, default: &str) -> String {
    let p = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&p).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

fn layers() -> SessionLayers {
    SessionLayers {
        sentences: true,
        entities: true,
        geography: true,
        quality: true,
        dual_cased: false,
        nlp_columns: false,
    }
}

/// Deterministic summary of lexical, entity, sentence, and geography output.
fn fingerprint(doc: &AnalyzedDoc) -> String {
    let mut ents: Vec<String> = doc
        .entities
        .iter()
        .map(|e| format!("{}:{}:{}:{}", e.kind, e.start, e.end, e.text))
        .collect();
    ents.sort();
    let terms: Vec<String> = doc
        .fields
        .first()
        .map(|f| f.terms.iter().map(|(t, c, _)| format!("{t}:{c}")).collect())
        .unwrap_or_default();
    let geo = match &doc.geography {
        Some(g) => match g.point {
            Some((lat, lon)) => format!("{}@{lat:.5},{lon:.5}", g.country),
            None => g.country.clone(),
        },
        None => String::new(),
    };
    format!(
        "E[{}] T[{}] S[{}] G[{}]",
        ents.join(","),
        terms.join(" "),
        doc.fields
            .first()
            .and_then(|f| f.sentences.as_ref())
            .map_or(0, Vec::len),
        geo
    )
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reference = arg("ref", "turbo");
    let candidate = arg("cand", "turbo");
    let n: usize = arg("n", "400").parse()?;
    let concurrency: usize = arg("concurrency", "32").parse()?;
    let path = arg("chunks", "/work/court-corpus/canary-chunks.ndjson");

    let texts: Arc<Vec<String>> = {
        use std::io::BufRead;
        let f = std::io::BufReader::new(std::fs::File::open(&path)?);
        Arc::new(
            f.lines()
                .map_while(Result::ok)
                .filter_map(|l| {
                    let v: serde_json::Value = serde_json::from_str(&l).ok()?;
                    let t = v.get("text")?.as_str()?.to_string();
                    (t.len() > 400).then_some(t)
                })
                .take(n)
                .collect(),
        )
    };
    println!(
        "{} docs; reference serial on {reference}, candidate x{concurrency} on {candidate}",
        texts.len()
    );

    let spec = body_spec();
    let opts = RemoteCallOptions::default();
    let mut expected = Vec::with_capacity(texts.len());
    for t in texts.iter() {
        let doc =
            analyze_document_with_options(&reference, t, Some(&spec), layers(), &opts).await?;
        expected.push(fingerprint(&doc));
    }
    let entities = expected.iter().filter(|f| !f.contains("E[]")).count();
    let terms = expected.iter().filter(|f| !f.contains("T[]")).count();
    let geo = expected.iter().filter(|f| !f.contains("G[]")).count();
    println!(
        "reference done: {} docs; with entities {entities}, with terms {terms}, with locations {geo}",
        expected.len()
    );
    for (layer, count) in [("entities", entities), ("terms", terms), ("locations", geo)] {
        if count == 0 {
            println!(
                "  WARNING: {layer} empty on every document; this run does not test that layer"
            );
        }
    }

    let sem = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut tasks = Vec::new();
    for i in 0..texts.len() {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let cand = candidate.clone();
        let t = texts.clone();
        let spec = spec.clone();
        tasks.push(tokio::spawn(async move {
            let _p = permit;
            analyze_document_with_options(
                &cand,
                &t[i],
                Some(&spec),
                layers(),
                &RemoteCallOptions::default(),
            )
            .await
            .map(|r| fingerprint(&r))
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
                    println!(
                        "  doc {i} DIFFERS\n    serial:     {}\n    concurrent: {}",
                        &expected[i][..expected[i].len().min(160)],
                        &got[..got.len().min(160)]
                    );
                }
            }
            Ok(_) => {}
        }
    }
    println!(
        "\nmismatches: {mismatches}   errors: {errors}   of {} docs",
        texts.len()
    );
    if mismatches == 0 && errors == 0 {
        println!("PASS: concurrent candidate agrees with the serial reference on every document.");
    } else {
        println!("FAIL: the candidate does not reproduce serial results.");
        std::process::exit(1);
    }
    Ok(())
}
