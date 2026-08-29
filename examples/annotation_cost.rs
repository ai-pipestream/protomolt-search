//! Price each sidecar annotation layer against real corpus text.
//!
//! Capturing NLP output for the repo service is cheap only if it rides
//! the SAME analysis pass the index build already pays for. That is an
//! empirical question per layer, not a design preference: some layers
//! are near-free once tokens exist, and some (dependency parsing,
//! coreference) cost multiples of the whole term-vector pass.
//!
//! ```text
//! annotation_cost --addr=http://127.0.0.1:59202 --chunks=/path/to.ndjson --n=200
//! ```
use pipestream_search::pb::analysis::analysis_service_client::AnalysisServiceClient;
use pipestream_search::pb::analysis::{AnalysisOptions, AnalyzeRequest, TermVectorOptions};
use std::time::Instant;

type LayerToggle = fn(&mut AnalysisOptions);

fn arg(key: &str, default: &str) -> String {
    let p = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&p).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

/// The corpus analyzer's own term-vector options, so the baseline here
/// is the cost the index build actually pays rather than a stand-in.
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

/// One timed pass over `texts`. Returns mean ms/chunk and the first
/// refusal message, if the sidecar refused the layer at all.
async fn measure(
    client: &mut AnalysisServiceClient<tonic::transport::Channel>,
    opts: AnalysisOptions,
    texts: &[String],
) -> (f64, Option<String>, u64) {
    let mut refused: Option<String> = None;
    // Total annotations returned across the pass. A layer that costs
    // nothing AND returns nothing is not free, it is absent, and that
    // distinction has to be visible or it gets adopted by accident.
    let mut produced: u64 = 0;
    let start = Instant::now();
    for t in texts {
        match client
            .analyze(AnalyzeRequest {
                text: t.clone(),
                options: Some(opts.clone()),
            })
            .await
        {
            Err(e) => {
                refused.get_or_insert_with(|| e.message().to_string());
            }
            Ok(r) => {
                let r = r.into_inner();
                produced += (r.sentences.len()
                    + r.entities.len()
                    + r.lemmas.len()
                    + r.noise.len()
                    + r.artifacts.len()
                    + r.pii.len()
                    + r.coref_mentions.len()
                    + r.dependencies.len()
                    + r.relations.len()
                    + r.locations.len()
                    + r.tokens.iter().filter(|t| !t.pos.is_empty()).count())
                    as u64;
            }
        }
    }
    (
        start.elapsed().as_secs_f64() * 1000.0 / texts.len() as f64,
        refused,
        produced,
    )
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = arg("addr", "http://127.0.0.1:59202");
    let path = arg("chunks", "/work/court-corpus/canary-chunks.ndjson");
    let n: usize = arg("n", "200").parse()?;

    let texts: Vec<String> = {
        use std::io::BufRead;
        let f = std::io::BufReader::new(std::fs::File::open(&path)?);
        f.lines()
            .map_while(Result::ok)
            .filter_map(|l| {
                let v: serde_json::Value = serde_json::from_str(&l).ok()?;
                let t = v.get("text")?.as_str()?.to_string();
                (t.len() > 200).then_some(t)
            })
            .take(n)
            .collect()
    };
    println!(
        "{} chunks, mean {} bytes",
        texts.len(),
        texts.iter().map(String::len).sum::<usize>() / texts.len().max(1)
    );

    let layers: Vec<(&str, LayerToggle)> = vec![
        ("term_vectors only (baseline)", |_| {}),
        ("+ sentence_detection", |o| o.sentence_detection = true),
        ("+ pos_tags", |o| o.pos_tags = true),
        // NER and geo consume the sentence layer, and the sidecar
        // refuses them without it rather than quietly degrading. They
        // are therefore priced as the PAIR, which is what enabling them
        // actually costs.
        ("+ sentences + ner", |o| {
            o.sentence_detection = true;
            o.ner = true
        }),
        ("+ lemmatize", |o| o.lemmatize = true),
        ("+ noise", |o| o.noise = true),
        ("+ artifacts", |o| o.artifacts = true),
        ("+ pii", |o| o.pii = true),
        ("+ coref", |o| o.coref = true),
        ("+ dependency_parse", |o| o.dependency_parse = true),
        ("+ sentences + geo", |o| {
            o.sentence_detection = true;
            o.geo = true
        }),
    ];

    let mut client = AnalysisServiceClient::connect(addr.clone()).await?;

    // Warm hard before the first timed pass. Whatever the runtime does
    // lazily (native-image page-ins, model loads, connection ramp) lands
    // on the FIRST layer measured otherwise, and the first layer is the
    // baseline every ratio divides by.
    for t in texts.iter().cycle().take(60) {
        let _ = client
            .analyze(AnalyzeRequest {
                text: t.clone(),
                options: Some(base()),
            })
            .await;
    }

    let (baseline_ms, _, base_out) = measure(&mut client, base(), &texts).await;
    println!(
        "\n{:<30}{:>10}{:>9}{:>11}{:>12}",
        "layer", "ms/chunk", "vs base", "per 86.6M", "annots/chunk"
    );
    println!(
        "{:<30}{:>10.2}{:>9}{:>10.1}h{:>12.1}",
        "term_vectors only (baseline)",
        baseline_ms,
        "1.00x",
        baseline_ms / 1000.0 * 86_633_399.0 / 3600.0,
        base_out as f64 / texts.len() as f64
    );

    let mut notes: Vec<String> = Vec::new();
    for (name, apply) in layers.iter().skip(1) {
        let mut opts = base();
        apply(&mut opts);
        let (per, refused, out) = measure(&mut client, opts, &texts).await;
        if let Some(why) = refused {
            println!("{:<30}{:>10}{:>9}{:>11}", name, "-", "-", "REFUSED");
            notes.push(format!("  {name}: {why}"));
            continue;
        }
        let gained = out.saturating_sub(base_out) as f64 / texts.len() as f64;
        let flag = if gained < 0.01 {
            "  <- PRODUCED NOTHING"
        } else {
            ""
        };
        println!(
            "{:<30}{:>10.2}{:>9.2}x{:>10.1}h{:>12.1}{}",
            name,
            per,
            per / baseline_ms,
            per / 1000.0 * 86_633_399.0 / 3600.0,
            gained,
            flag
        );
    }

    // Re-measure the baseline last. If it moved, the run drifted and no
    // ratio above is worth quoting.
    let (baseline_again, _, _) = measure(&mut client, base(), &texts).await;
    let drift = (baseline_again - baseline_ms).abs() / baseline_ms * 100.0;
    println!("\nbaseline re-measured: {baseline_again:.2} ms/chunk ({drift:.0}% drift)");
    if drift > 15.0 {
        println!("DRIFT ABOVE 15%: the host was not quiet; treat every ratio as unusable.");
    }
    if !notes.is_empty() {
        println!("\nrefused layers (model not configured on this sidecar):");
        for n in &notes {
            println!("{n}");
        }
    }
    println!("\nPer-86.6M hours are single-threaded and single-connection: divide by the\ningest fan-out actually used. The ratio column is the portable number.");
    Ok(())
}
