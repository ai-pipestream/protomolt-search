//! Price the count-then-rank facet traversal
//! (`docs/plans/track-1-features.md` section 2, the "measure before
//! committing" TODO): a facet query pays one full doc-run walk per
//! scored term plus one ords lookup per matched document. This builds a
//! synthetic v7 shard at a controllable size, opens it through the
//! production mmap reader, and times the two phases separately — the
//! union walk and the ordinal counting — so the per-posting and per-doc
//! costs read straight off.
//!
//! ```text
//! cargo run --release --example facet_walk_probe -- --docs=2000000 --values=200
//! ```

use std::io::Write as _;
use std::time::Instant;

use pipestream_search::postings::{AnalyzedDoc, Bm25Reader, SpillBuilder};

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let n_docs: u32 = arg("docs", "2000000").parse()?;
    let n_values: u32 = arg("values", "200").parse()?;
    let dir = std::env::temp_dir().join(format!("facet_walk_probe_{}", std::process::id()));
    let path = dir.join("probe.bm25");

    // Corpus shape: "common" in every doc, "half" in every 2nd,
    // "rare" in every 1000th; a court value on 90% of docs, cycling
    // n_values distinct courts. Texts are empty-ish (the walk never
    // touches them).
    eprint!("building {n_docs} docs ... ");
    std::io::stderr().flush()?;
    let t_build = Instant::now();
    let mut builder = SpillBuilder::create_with_fields(&dir.join("build"), &["body"])?
        .with_facet_fields(&["court"]);
    for doc in 0..n_docs {
        let mut terms = vec![("common".to_string(), 1u32, Vec::new())];
        if doc % 2 == 0 {
            terms.push(("half".to_string(), 1, Vec::new()));
        }
        if doc % 1000 == 0 {
            terms.push(("rare".to_string(), 1, Vec::new()));
        }
        let len = terms.len() as u32;
        builder.add_document_with_lineage(
            doc,
            ".".to_string(),
            AnalyzedDoc::body(terms, len),
            None,
        )?;
        if doc % 10 != 9 {
            builder.set_facet(0, doc, &format!("c{:04}", doc % n_values));
        }
    }
    builder.finish(&path)?;
    eprintln!("done in {:.1}s", t_build.elapsed().as_secs_f64());

    let reader = Bm25Reader::open(&path)?;
    let body = reader.field(0);
    let court = reader.facet_index("court").expect("declared above");

    for terms in [
        vec!["common"],
        vec!["common", "half"],
        vec!["rare"],
        vec!["half", "rare"],
    ] {
        use pipestream_search::postings::Bm25Index;
        // Phase 1: the union walk (one doc-run pass per term).
        let mut bits = vec![0u64; (n_docs as usize).div_ceil(64)];
        let mut postings = 0u64;
        let t_walk = Instant::now();
        for term in &terms {
            body.for_each_doc_tf(term, &mut |doc_id, _tf| {
                bits[doc_id as usize / 64] |= 1u64 << (doc_id % 64);
                postings += 1;
            });
        }
        let walk = t_walk.elapsed();
        // Phase 2: one ords lookup per matched doc.
        let t_count = Instant::now();
        let mut counts = vec![0u64; reader.facet_value_count(court)];
        let mut matched = 0u64;
        for (wi, &word) in bits.iter().enumerate() {
            let mut w = word;
            while w != 0 {
                let doc = (wi * 64) as u32 + w.trailing_zeros();
                if let Some(ord) = reader.facet_ord(court, doc) {
                    counts[ord as usize] += 1;
                }
                matched += 1;
                w &= w - 1;
            }
        }
        let count = t_count.elapsed();
        let top = counts.iter().max().copied().unwrap_or(0);
        println!(
            "terms {terms:?}: walk {:7.1} ms ({postings} postings, {:.2} ns/posting) | \
             count {:7.1} ms ({matched} matched, {:.2} ns/doc) | top value count {top}",
            walk.as_secs_f64() * 1e3,
            walk.as_nanos() as f64 / postings.max(1) as f64,
            count.as_secs_f64() * 1e3,
            count.as_nanos() as f64 / matched.max(1) as f64,
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
