//! Price the proximity payloads on real text (`docs/phrase-proximity.md`).
//!
//! Reads NDJSON records with a `text` field (the CourtListener chunk files
//! under `/work/court-corpus` have that shape), analyzes each natively
//! under `body_spec`, and writes three shards through the bounded-memory
//! spill builder: body only, body with token positions, body with its
//! bigram column, and body with sentence spans (`docs/highlighting.md`).
//! It then reports every section's bytes and the bytes per document each
//! payload adds, read back from the files' integrity tables — the same
//! accounting the cost gates in `tests/phrase_proximity.rs` and
//! `tests/highlighting.rs` pin on synthetic corpora.
//!
//! ```bash
//! cargo run --release --example bigram_cost -- \
//!     --input=/work/court-corpus/canary-chunks.ndjson --limit=20000
//! ```

use std::io::BufRead;
use std::path::{Path, PathBuf};

use pipestream_search::analyzer::{analyze_document_native, body_spec};
use pipestream_search::postings::{AnalyzedDoc, Bm25Reader, SpillBuilder};
use pipestream_search::proximity::derive_bigrams;

fn arg(name: &str) -> Option<String> {
    let prefix = format!("--{name}=");
    std::env::args().find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
}

/// The `text` value of one NDJSON line, without a JSON dependency: the
/// key is located, then the string literal is decoded (escapes included).
fn text_field(line: &str) -> Option<String> {
    let key = "\"text\":";
    let at = line.find(key)? + key.len();
    let rest = line[at..].trim_start();
    let mut chars = rest.chars();
    if chars.next()? != '"' {
        return None;
    }
    let mut out = String::new();
    let mut escaped = false;
    for ch in chars {
        if escaped {
            match ch {
                'n' => out.push('\n'),
                't' => out.push('\t'),
                'r' => out.push('\r'),
                'b' => out.push('\u{8}'),
                'f' => out.push('\u{c}'),
                'u' => return None, // \uXXXX is rare in this corpus; skip the record
                other => out.push(other),
            }
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == '"' {
            return Some(out);
        } else {
            out.push(ch);
        }
    }
    None
}

fn sections(path: &Path) -> Vec<(String, u64)> {
    Bm25Reader::open(path)
        .unwrap_or_else(|e| panic!("open {}: {e}", path.display()))
        .integrity_sections()
}

fn main() {
    let input = arg("input").expect("--input=<ndjson with a text field>");
    let limit: usize = arg("limit").map_or(20_000, |v| v.parse().expect("--limit"));
    let out = PathBuf::from(arg("out").unwrap_or_else(|| {
        std::env::temp_dir()
            .join(format!("bigram-cost-{}", std::process::id()))
            .display()
            .to_string()
    }));
    let _ = std::fs::remove_dir_all(&out);
    std::fs::create_dir_all(&out).expect("create out dir");

    let names = [
        ("plain", &["body"][..]),
        ("positions", &["body"][..]),
        ("bigrams", &["body", "body.bigrams"][..]),
        ("sentences", &["body"][..]),
    ];
    let mut builders: Vec<SpillBuilder> = names
        .iter()
        .map(|(tag, fields)| {
            let b = SpillBuilder::create_with_fields(&out.join(format!("build-{tag}")), fields)
                .expect("spill dir");
            match *tag {
                "positions" => b.with_position_fields(&["body"]),
                "sentences" => b.with_sentence_fields(&["body"]),
                _ => b,
            }
        })
        .collect();

    let file = std::fs::File::open(&input).expect("open input");
    let spec = body_spec();
    let mut docs = 0u32;
    let mut occurrences = 0u64;
    let mut distinct_terms_per_doc = 0u64;
    let mut bigram_postings = 0u64;
    let mut sentences = 0u64;
    let mut text_bytes = 0u64;
    let started = std::time::Instant::now();
    for line in std::io::BufReader::new(file).lines() {
        if docs as usize >= limit {
            break;
        }
        let line = line.expect("read line");
        let Some(text) = text_field(&line) else {
            continue;
        };
        if text.trim().is_empty() || text.len() > 1024 * 1024 {
            continue;
        }
        let doc = match analyze_document_native(&text, Some(&spec)) {
            Ok(doc) => doc,
            Err(_) => continue,
        };
        let body = doc.fields[0].clone();
        occurrences += body
            .terms
            .iter()
            .map(|(_, _, o)| o.len() as u64)
            .sum::<u64>();
        distinct_terms_per_doc += body.terms.len() as u64;
        let column = derive_bigrams(&body).expect("native analysis carries positions");
        bigram_postings += column.terms.len() as u64;
        sentences += body.sentences.as_ref().map_or(0, |s| s.len() as u64);
        text_bytes += text.len() as u64;

        builders[0]
            .add_document_with_lineage(
                docs,
                text.clone(),
                AnalyzedDoc::body(body.terms.clone(), body.length),
                None,
            )
            .expect("plain add");
        builders[1]
            .add_document_with_lineage(docs, text.clone(), doc, None)
            .expect("positional add");
        let mut with_sentences = AnalyzedDoc::body(body.terms.clone(), body.length);
        with_sentences.fields[0].sentences = body.sentences.clone();
        builders[3]
            .add_document_with_lineage(docs, text.clone(), with_sentences, None)
            .expect("sentence add");
        let mut both = AnalyzedDoc::body(body.terms, body.length);
        both.fields.push(column);
        builders[2]
            .add_document_with_lineage(docs, text, both, None)
            .expect("bigram add");
        docs += 1;
    }
    let analyzed_in = started.elapsed();
    let paths: Vec<PathBuf> = names
        .iter()
        .map(|(tag, _)| out.join(format!("{tag}.bm25")))
        .collect();
    for (builder, path) in builders.iter_mut().zip(&paths) {
        builder.finish(path).expect("finish");
    }
    let d = f64::from(docs.max(1));
    println!(
        "documents {docs}  text {:.1} B/doc  distinct terms {:.1}/doc  occurrences {:.1}/doc  \
         bigram postings {:.1}/doc  sentences {:.2}/doc  analyzed+built in {:.1}s",
        text_bytes as f64 / d,
        distinct_terms_per_doc as f64 / d,
        occurrences as f64 / d,
        bigram_postings as f64 / d,
        sentences as f64 / d,
        analyzed_in.as_secs_f64()
    );
    let mut totals = Vec::new();
    for ((tag, _), path) in names.iter().zip(&paths) {
        let file_len = std::fs::metadata(path).expect("metadata").len();
        totals.push(file_len);
        println!(
            "\n{tag}: {} bytes total, {:.1} B/doc",
            file_len,
            file_len as f64 / d
        );
        for (name, len) in sections(path) {
            if name == "header" || name == "text_index" || name == "lineages" {
                continue;
            }
            println!("  {name:<36} {len:>12}  {:>9.1} B/doc", len as f64 / d);
        }
    }
    let plain = sections(&paths[0]);
    let body_postings: u64 = plain
        .iter()
        .filter(|(n, _)| n.starts_with("field:body:"))
        .map(|(_, l)| *l)
        .sum();
    println!(
        "\nbody index (lengths+postings+directory) {:.1} B/doc\n\
         positions add {:.1} B/doc (+{:.1}% of the body index, {:.1}% of the file)\n\
         bigram column adds {:.1} B/doc (+{:.1}% of the body index, {:.1}% of the file)\n\
         sentence spans add {:.1} B/doc (+{:.1}% of the body index, {:.1}% of the file)",
        body_postings as f64 / d,
        (totals[1] - totals[0]) as f64 / d,
        100.0 * (totals[1] - totals[0]) as f64 / body_postings as f64,
        100.0 * (totals[1] - totals[0]) as f64 / totals[0] as f64,
        (totals[2] - totals[0]) as f64 / d,
        100.0 * (totals[2] - totals[0]) as f64 / body_postings as f64,
        100.0 * (totals[2] - totals[0]) as f64 / totals[0] as f64,
        (totals[3] - totals[0]) as f64 / d,
        100.0 * (totals[3] - totals[0]) as f64 / body_postings as f64,
        100.0 * (totals[3] - totals[0]) as f64 / totals[0] as f64,
    );
    println!("\nfiles left under {}", out.display());
}
