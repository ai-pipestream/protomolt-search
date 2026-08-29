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

use pipestream_search::analyzer;
use pipestream_search::pb::AnalysisSpec;

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
    // `--analyzer=<name>` probes a REAL analyzer from the one place they
    // are defined, rather than a hand-typed reconstruction of it. The
    // difference matters: this tool exists to answer "what does the
    // index actually contain", and a probe that retypes the spec can
    // agree with itself while disagreeing with the corpus.
    let named = arg("analyzer", "");
    let spec = if named.is_empty() {
        AnalysisSpec {
            tokenizer: arg("tokenizer", "1").parse()?,
            stemmer: arg("stemmer", "2").parse()?,
            term_vector_mode: arg("mode", "1").parse()?,
            term_vector_source: arg("source", "2").parse()?,
            char_filters: match arg("char-filters", &arg("rungs", "")).as_str() {
                "" => Vec::new(),
                r => r
                    .split(',')
                    .map(|s| s.trim().parse())
                    .collect::<Result<_, _>>()?,
            },
        }
    } else {
        match analyzer::analyzer_by_name(&named)? {
            Some(spec) => spec,
            None => {
                return Err(format!(
                    "analyzer {named:?} means \"whatever the sidecar defaults to\", \
                     which has no spec to print; probe it with explicit flags"
                )
                .into())
            }
        }
    };
    println!(
        "spec: {}tokenizer={} stemmer={} mode={} source={} char_filters={:?}",
        if named.is_empty() {
            String::new()
        } else {
            format!("[{named}] ")
        },
        spec.tokenizer,
        spec.stemmer,
        spec.term_vector_mode,
        spec.term_vector_source,
        spec.char_filters
    );
    println!(
        "fingerprint: 0x{:016x}",
        analyzer::analysis_fingerprint(Some(&spec))
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

    // Embedding is a SEPARATE capability of the same sidecar, and the one
    // that fails silently at deploy time: a sidecar started without
    // OPENNLP_EMBEDDINGS_DIR analyzes perfectly, builds a perfectly good
    // index, and then cannot embed a single query. Checking it here means
    // finding out before the cluster is serving rather than from the
    // first hybrid search.
    if std::env::args().any(|a| a == "--embed") {
        match analyzer::embed_text(&addr, &text).await {
            Ok(v) => {
                let norm = v
                    .iter()
                    .map(|x| f64::from(*x) * f64::from(*x))
                    .sum::<f64>()
                    .sqrt();
                println!("embedding: dim {}, L2 norm {norm:.4}", v.len());
                if v.iter().any(|x| !x.is_finite()) {
                    println!("  WARNING: embedding has non-finite coordinates");
                }
            }
            Err(e) => {
                println!("embedding: FAILED -- {e}");
                println!("  the sidecar analyzes but cannot embed; check OPENNLP_EMBEDDINGS_DIR");
                std::process::exit(1);
            }
        }
    }
    Ok(())
}
