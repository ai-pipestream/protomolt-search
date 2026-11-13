//! Price each analysis layer against real corpus text.
//!
//! Measures single-connection latency across incremental [`SessionLayers`] on
//! `"turbo"` (in-process) or a remote `turbo-kserve` endpoint.
//!
//! ```text
//! annotation_cost --addr=turbo --chunks=/path/to.ndjson --n=200
//! ```

use std::time::Instant;

use pipestream_search::analyzer::{
    analyze_document_with_options, body_spec, RemoteCallOptions, SessionLayers,
};

type LayerToggle = fn(&mut SessionLayers);

fn arg(key: &str, default: &str) -> String {
    let p = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&p).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

async fn measure(
    addr: &str,
    layers: SessionLayers,
    texts: &[String],
) -> (f64, Option<String>, u64) {
    let spec = body_spec();
    let opts = RemoteCallOptions::default();
    let mut refused: Option<String> = None;
    let mut produced: u64 = 0;
    let start = Instant::now();
    for t in texts {
        match analyze_document_with_options(addr, t, Some(&spec), layers, &opts).await {
            Err(e) => {
                refused.get_or_insert_with(|| e.message().to_string());
            }
            Ok(doc) => {
                produced += (doc
                    .fields
                    .first()
                    .and_then(|f| f.sentences.as_ref())
                    .map_or(0, Vec::len)
                    + doc.entities.len()
                    + usize::from(doc.geography.is_some())
                    + usize::from(doc.quality.is_some())
                    + usize::from(doc.cased.is_some())
                    + doc.fields.first().map_or(0, |f| f.terms.len()))
                    as u64;
            }
        }
    }
    (
        start.elapsed().as_secs_f64() * 1000.0 / texts.len().max(1) as f64,
        refused,
        produced,
    )
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = arg("addr", "turbo");
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
        ("+ sentences", |o| o.sentences = true),
        ("+ dual_cased", |o| o.dual_cased = true),
        ("+ sentences + entities", |o| {
            o.sentences = true;
            o.entities = true;
        }),
        ("+ quality (noise + artifacts)", |o| o.quality = true),
        ("+ sentences + geography", |o| {
            o.sentences = true;
            o.geography = true;
        }),
    ];

    let spec = body_spec();
    let opts = RemoteCallOptions::default();
    for t in texts.iter().cycle().take(60) {
        let _ =
            analyze_document_with_options(&addr, t, Some(&spec), SessionLayers::default(), &opts)
                .await;
    }

    let (baseline_ms, _, base_out) = measure(&addr, SessionLayers::default(), &texts).await;
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
        base_out as f64 / texts.len().max(1) as f64
    );

    let mut notes: Vec<String> = Vec::new();
    for (name, apply) in layers.iter().skip(1) {
        let mut l = SessionLayers::default();
        apply(&mut l);
        let (per, refused, out) = measure(&addr, l, &texts).await;
        if let Some(why) = refused {
            println!("{:<30}{:>10}{:>9}{:>11}", name, "-", "-", "REFUSED");
            notes.push(format!("  {name}: {why}"));
            continue;
        }
        let gained = out.saturating_sub(base_out) as f64 / texts.len().max(1) as f64;
        let flag = if gained < 0.01 {
            "  <- PRODUCED NOTHING"
        } else {
            ""
        };
        println!(
            "{:<30}{:>10.2}{:>9.2}x{:>10.1}h{:>12.1}{}",
            name,
            per,
            per / baseline_ms.max(1e-9),
            per / 1000.0 * 86_633_399.0 / 3600.0,
            gained,
            flag
        );
    }

    let (baseline_again, _, _) = measure(&addr, SessionLayers::default(), &texts).await;
    let drift = (baseline_again - baseline_ms).abs() / baseline_ms.max(1e-9) * 100.0;
    println!("\nbaseline re-measured: {baseline_again:.2} ms/chunk ({drift:.0}% drift)");
    if drift > 15.0 {
        println!("DRIFT ABOVE 15%: the host was not quiet; treat every ratio as unusable.");
    }
    if !notes.is_empty() {
        println!("\nrefused layers:");
        for n in &notes {
            println!("{n}");
        }
    }
    println!("\nPer-86.6M hours are single-threaded and single-connection: divide by the\ningest fan-out actually used. The ratio column is the portable number.");
    Ok(())
}
