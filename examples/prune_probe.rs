//! Run the single-field and fused pruners over the SAME shard file and
//! the same terms, and print what each one actually did.
//!
//! `Bm25Search` routes to `fanout_bm25_seeded` when the request carries
//! no field list and to `fanout_bm25_fused` when it does, and the two end
//! in different node-side scorers. Both are documented as the same
//! MaxScore algorithm over the same postings, so a large cost gap between
//! them is a bug in one of them -- but a gap measured through the
//! coordinator could equally be fan-out, analysis or page cache. This
//! runs both in one process against one file, so the only variable left
//! is the scorer.
//!
//! ```text
//! prune_probe --bm25=/work/court-corpus/shards-v7/shard-0.bm25 \
//!             --terms=court,establish --field=body
//! ```
//!
//! Global df/N default to this shard's local values, which is not what
//! the fleet scores with but IS the same for both scorers -- the point is
//! the comparison, not the absolute score. Pass `--dfs=` and `--n=` to
//! reproduce a fleet query exactly.

use std::time::Instant;

use turbovec_search::bm25::{
    self, Bm25Params, CorpusStats, FieldQuery, PruneStats,
};
use turbovec_search::postings::{Bm25Index, Bm25Reader};

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

fn show(
    label: &str,
    ms: f64,
    p: &PruneStats,
    top: &[(u32, f64)],
    view: &dyn Bm25Index,
    text: bool,
) {
    println!(
        "{label:<10} {ms:9.1} ms   candidates {:>12}   postings scored {:>12}",
        p.candidates_evaluated, p.postings_scored
    );
    println!(
        "{:<10} {:>12} of {} level-0 blocks skipped, {} level-1 groups leapt",
        "", p.blocks_skipped, p.blocks_total, p.l1_groups_skipped
    );
    // Document length is the whole question for a length-normalization
    // sweep, so it is reported per hit and as the mean over the top-k.
    let mut total = 0u64;
    for (i, (doc, score)) in top.iter().enumerate() {
        let dl = view.doc_length(*doc);
        total += u64::from(dl);
        let snippet = if text {
            view.text(*doc)
                .map(|t| {
                    let one: String = t.split_whitespace().collect::<Vec<_>>().join(" ");
                    let cut: String = one.chars().take(70).collect();
                    format!("  {cut:?}")
                })
                .unwrap_or_else(|| "  <no stored text>".to_string())
        } else {
            String::new()
        };
        println!("{:<10}   {:>2}. {doc:<12} {score:.6}  dl {dl:>6}{snippet}", "", i + 1);
    }
    if !top.is_empty() {
        println!(
            "{:<10}   mean top-{} document length: {:.1} terms",
            "",
            top.len(),
            total as f64 / top.len() as f64
        );
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = arg("bm25", "");
    if path.is_empty() {
        eprintln!("--bm25=<path to a .bm25 file> is required");
        std::process::exit(2);
    }
    let field = arg("field", "body");
    let terms: Vec<String> = arg("terms", "court,establish")
        .split(',')
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    let k: usize = arg("k", "5").parse()?;
    let show_text = std::env::args().any(|a| a == "--text");
    let params = Bm25Params {
        k1: arg("k1", "1.2").parse()?,
        b: arg("b", "0.75").parse()?,
    };

    let reader = Bm25Reader::open(std::path::Path::new(&path))?;
    let fi = reader
        .field_index(&field)
        .ok_or_else(|| format!("{path}: no field {field:?}"))?;
    let view = reader.field(fi);

    // Local stats unless overridden, and the SAME values into both
    // scorers: a difference in stats would be a difference in the
    // question, not in the answer.
    let doc_count: u64 = match arg("n", "").as_str() {
        "" => Bm25Index::doc_count(&view),
        s => s.parse()?,
    };
    let total_doc_length: u64 = match arg("total-len", "").as_str() {
        "" => view.total_doc_length(),
        s => s.parse()?,
    };
    let dfs: Vec<u32> = match arg("dfs", "").as_str() {
        "" => terms.iter().map(|t| view.df(t)).collect(),
        s => s.split(',').map(str::parse).collect::<Result<_, _>>()?,
    };
    let stats = CorpusStats {
        doc_count,
        total_doc_length,
        dfs: dfs.clone(),
    };

    println!("{path}");
    println!(
        "  field {field:?} (index {fi} of {}), N={doc_count}, avgdl={:.2}, k1={}, b={}",
        reader.field_count(),
        stats.avgdl(),
        params.k1,
        params.b
    );
    for (t, df) in terms.iter().zip(&dfs) {
        println!(
            "  term {t:<16} df {df:>12}   local df {:>12}   impacts {}",
            view.df(t),
            view.has_impacts(t)
        );
    }
    println!();

    // The single-field scorer sees the reader itself, which scores as
    // field 0. Comparing any other field against it would be comparing
    // two different postings sets.
    if fi == 0 {
        let mut p = PruneStats::default();
        let t = Instant::now();
        let hits = bm25::top_k_pruned_stats(
            &reader,
            &terms,
            &stats,
            params,
            k,
            f64::NEG_INFINITY,
            &mut p,
        );
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        let top: Vec<(u32, f64)> = hits.iter().map(|h| (h.doc_id, h.score)).collect();
        show("single", ms, &p, &top, &view, show_text);
    } else {
        println!("single     skipped: the single-field scorer is field 0 only");
    }
    println!();

    let queries = vec![FieldQuery {
        index: &view,
        terms: &terms,
        stats: stats.clone(),
        params,
        weight: 1.0,
    }];
    let mut p = PruneStats::default();
    let t = Instant::now();
    let hits = bm25::top_k_fused_pruned_stats(&queries, k, f64::NEG_INFINITY, &mut p);
    let ms = t.elapsed().as_secs_f64() * 1000.0;
    let top: Vec<(u32, f64)> = hits.iter().map(|h| (h.doc_id, h.score)).collect();
    show("fused", ms, &p, &top, &view, show_text);
    Ok(())
}
