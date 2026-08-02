//! Print exactly what the sidecar makes of a piece of text under a given
//! analysis spec: the terms, their frequencies, and how many distinct
//! terms came back.
//!
//! Term identity is the whole BM25 contract, and it is decided inside the
//! sidecar. This is the tool that shows what it decided, rather than
//! inferring it from a ranking.
//!
//! ```text
//! analyze_probe --addr=http://127.0.0.1:59202 --stemmer=2 --source=2 \
//!     --text="COURT court Court COURTS courts"
//! ```

use turbovec_search::analyzer;
use turbovec_search::pb::AnalysisSpec;

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = arg("addr", "http://127.0.0.1:59202");
    let text = arg(
        "text",
        "COURT court Court COURTS courts Appellant APPELLANT appellant",
    );
    let spec = AnalysisSpec {
        tokenizer: arg("tokenizer", "1").parse()?,
        stemmer: arg("stemmer", "2").parse()?,
        term_vector_mode: arg("mode", "1").parse()?,
        term_vector_source: arg("source", "2").parse()?,
        normalizer_rungs: match arg("rungs", "").as_str() {
            "" => Vec::new(),
            r => r
                .split(',')
                .map(|s| s.trim().parse())
                .collect::<Result<_, _>>()?,
        },
    };
    println!(
        "spec: tokenizer={} stemmer={} mode={} source={} rungs={:?}",
        spec.tokenizer,
        spec.stemmer,
        spec.term_vector_mode,
        spec.term_vector_source,
        spec.normalizer_rungs
    );
    println!("text: {text:?}");
    let doc = analyzer::analyze_document(&addr, &text, Some(&spec)).await?;
    let mut terms: Vec<(String, u32)> = doc
        .fields
        .first()
        .map(|f| f.terms.iter().map(|(t, tf, _)| (t.clone(), *tf)).collect())
        .unwrap_or_default();
    terms.sort();
    println!(
        "length: {} tokens, {} DISTINCT terms",
        doc.fields.first().map_or(0, |f| f.length),
        terms.len()
    );
    for (term, tf) in &terms {
        println!("  {term:<24} tf={tf}");
    }
    Ok(())
}
