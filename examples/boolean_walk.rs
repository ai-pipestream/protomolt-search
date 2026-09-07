//! Time the phases of a shard-side Boolean evaluation offline, over a
//! sealed segment catalog, the way `EvaluateBoolean` runs them on a
//! node: the lexical leaf's membership walk, the filter leaf's column
//! reads, the group AND, the member materialization, the whole-shard
//! accumulator, the candidate scorer, the top-k pass, and the second
//! scorer pass over the ranked candidates.
//!
//! Usage: `boolean_walk <catalog root> --terms=a,b [--year-ge=N]
//!         [--depth=N] [--walk=auto|candidates|postings] [--repeat=N]
//!         [--filter-scan=domain|full]`
//!
//! Terms are the stems the analyzer stores (`bm25_tables` prints the
//! tables; a df of 0 means the term is spelled differently). The filter
//! phase defaults to the node's narrowing: the MUST filter leaf reads
//! its column over the lexical sibling's members only; `full` is the
//! A/B baseline, the whole-shard column scan.
use pipestream_search::bm25::{self, Bm25Params, CandidateWalk, CorpusStats};
use pipestream_search::boolean_bits::Bits;
use pipestream_search::postings::{Bm25Index, Bm25Reader, Bm25Store};
use pipestream_search::segmented::SegmentedShard;
use pipestream_search::segments::{OpenedSegmentSet, SegmentCatalog};
use std::collections::BinaryHeap;
use std::path::Path;
use std::time::Instant;

fn names(count: usize, name: impl Fn(usize) -> String) -> Vec<String> {
    (0..count).map(name).collect()
}

fn strs(v: &[String]) -> Vec<&str> {
    v.iter().map(String::as_str).collect()
}

/// An empty tail declared with the catalog's own tables.
fn tail_of(root: &Path, set: &OpenedSegmentSet) -> Result<Bm25Store, String> {
    if set.is_empty() {
        return Err(format!(
            "{}: the catalog has no sealed segment",
            root.display()
        ));
    }
    let r: &Bm25Reader = set.bm25(0);
    let fields = names(r.field_count(), |f| r.field_name(f).to_string());
    let positions: Vec<String> = (0..r.field_count())
        .filter(|&f| r.field_has_positions(f))
        .map(|f| r.field_name(f).to_string())
        .collect();
    let sentences: Vec<String> = (0..r.field_count())
        .filter(|&f| r.field_has_sentences(f))
        .map(|f| r.field_name(f).to_string())
        .collect();
    let facets = names(r.facet_count(), |i| r.facet_name(i).to_string());
    let numerics = names(r.numeric_count(), |i| r.numeric_name(i).to_string());
    let integers = names(r.integer_count(), |i| r.integer_name(i).to_string());
    let unsigned = names(r.unsigned_integer_count(), |i| {
        r.unsigned_integer_name(i).to_string()
    });
    let geo = names(r.geo_count(), |i| r.geo_name(i).to_string());
    let map_facets = names(r.map_facet_count(), |i| r.map_facet_name(i).to_string());
    let map_numerics = names(r.map_numeric_count(), |i| r.map_numeric_name(i).to_string());
    eprintln!(
        "tables: fields {fields:?} (positions {positions:?}, sentences {sentences:?}) facets \
         {facets:?} integers {integers:?} numerics {numerics:?}"
    );
    let mut tail = Bm25Store::with_fields(&strs(&fields))
        .with_positions(&strs(&positions))
        .with_sentences(&strs(&sentences))
        .with_facets(&strs(&facets))
        .with_numerics(&strs(&numerics))
        .with_integers(&strs(&integers))
        .with_unsigned_integers(&strs(&unsigned))
        .with_geos(&strs(&geo))
        .with_map_facets(&strs(&map_facets))
        .with_map_numerics(&strs(&map_numerics));
    // The segments' derived-column declaration, if any, is the tail's.
    tail.set_derived(r.derived().cloned());
    Ok(tail)
}

#[derive(Clone, Copy, PartialEq)]
struct Ranked {
    score: f32,
    slot: u32,
}
impl Eq for Ranked {}
impl PartialOrd for Ranked {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Ranked {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| other.slot.cmp(&self.slot))
    }
}

fn ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1e3
}

fn main() {
    if let Err(e) = run() {
        eprintln!("boolean_walk: {e}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let root = args
        .iter()
        .find(|a| !a.starts_with("--"))
        .ok_or("usage: boolean_walk <catalog root> --terms=a,b [--year-ge=N] [--depth=N] [--walk=auto|candidates|postings] [--repeat=N] [--filter-scan=domain|full]")?;
    let opt = |key: &str| {
        args.iter()
            .find_map(|a| a.strip_prefix(&format!("--{key}=")).map(str::to_string))
    };
    let terms: Vec<String> = opt("terms")
        .ok_or("--terms=a,b is required")?
        .split(',')
        .filter(|t| !t.is_empty())
        .map(str::to_string)
        .collect();
    let year_ge: Option<i64> = opt("year-ge")
        .map(|v| v.parse().map_err(|e| format!("--year-ge: {e}")))
        .transpose()?;
    let depth: usize =
        opt("depth").map_or(Ok(10), |v| v.parse().map_err(|e| format!("--depth: {e}")))?;
    let repeat: usize =
        opt("repeat").map_or(Ok(2), |v| v.parse().map_err(|e| format!("--repeat: {e}")))?;
    let walk = match opt("walk").as_deref() {
        None | Some("auto") => CandidateWalk::Auto,
        Some("candidates") => CandidateWalk::Candidates,
        Some("postings") => CandidateWalk::Postings,
        Some(other) => return Err(format!("--walk={other}: auto, candidates, or postings")),
    };
    let filter_domain = match opt("filter-scan").as_deref() {
        None | Some("domain") => true,
        Some("full") => false,
        Some(other) => return Err(format!("--filter-scan={other}: domain or full")),
    };
    let root = Path::new(root);
    let t = Instant::now();
    // Open the catalog once: a reader verifies its store on open, and
    // that is a pass over the whole file.
    let catalog = SegmentCatalog::open(root)?;
    let tail = tail_of(root, &catalog.snapshot())?;
    let shard = SegmentedShard::open_catalog(catalog, tail)?;
    let n = shard.next_doc_id() as usize;
    let index: &dyn Bm25Index = &shard;
    eprintln!(
        "opened {} rows, {} sealed parts, {:.0} ms",
        n,
        shard.sealed_parts(),
        ms(t)
    );
    let dfs: Vec<u32> = terms.iter().map(|t| index.df(t)).collect();
    for (term, df) in terms.iter().zip(&dfs) {
        eprintln!("term {term:?} df {df}");
    }
    let stats = CorpusStats {
        doc_count: index.doc_count(),
        total_doc_length: index.total_doc_length(),
        dfs: dfs.clone(),
    };
    let params = Bm25Params {
        k1: bm25::DEFAULT_K1,
        b: bm25::DEFAULT_B,
    };
    let year_ii = match year_ge {
        Some(_) => Some(
            shard
                .integer_index("year")
                .ok_or("the catalog declares no integer column \"year\"")?,
        ),
        None => None,
    };
    for round in 1..=repeat {
        eprintln!("-- round {round}");
        // Lexical leaf membership: every posting of every term.
        let t = Instant::now();
        let mut lexical = Bits::empty(n);
        for term in &terms {
            index.for_each_doc_tf(term, &mut |doc, _tf| {
                if (doc as usize) < n {
                    lexical.set(doc as usize);
                }
            });
        }
        let t_lex = ms(t);
        // Filter leaf membership: the column reads, over the lexical
        // sibling's members (the node's narrowing) or over every row
        // (the A/B baseline).
        let t = Instant::now();
        let filter = match (year_ge, year_ii) {
            (Some(bound), Some(ii)) => {
                let mut bits = Bits::empty(n);
                if filter_domain {
                    for doc in lexical.iter() {
                        if shard
                            .integer_value(ii, doc as u32)
                            .is_some_and(|y| y >= bound)
                        {
                            bits.set(doc);
                        }
                    }
                } else {
                    for doc in 0..n as u32 {
                        if shard.integer_value(ii, doc).is_some_and(|y| y >= bound) {
                            bits.set(doc as usize);
                        }
                    }
                }
                Some(bits)
            }
            _ => None,
        };
        let t_filter = ms(t);
        // The group: MUST lexical, MUST filter.
        let t = Instant::now();
        let mut members = lexical.clone();
        if let Some(filter) = &filter {
            members.and_with(filter);
        }
        let t_and = ms(t);
        let t = Instant::now();
        let member_slots: Vec<u32> = members.iter().map(|s| s as u32).collect();
        let candidates: Vec<u32> = member_slots
            .iter()
            .copied()
            .filter(|&s| lexical.test(s as usize))
            .collect();
        let t_materialize = ms(t);
        let t = Instant::now();
        let mut acc = vec![0.0f32; n];
        let mut scored = Bits::empty(n);
        let t_alloc = ms(t);
        let t = Instant::now();
        let scores =
            bm25::score_candidates_plain_walk(index, &terms, &stats, params, &candidates, walk);
        let t_score = ms(t);
        let t = Instant::now();
        let mut missing = 0usize;
        for (slot, score) in candidates.iter().zip(&scores) {
            match score {
                Some(score) => {
                    acc[*slot as usize] += *score as f32;
                    scored.set(*slot as usize);
                }
                None => missing += 1,
            }
        }
        let mut heap: BinaryHeap<std::cmp::Reverse<Ranked>> = BinaryHeap::with_capacity(depth + 1);
        for slot in members.iter() {
            let entry = Ranked {
                score: if scored.test(slot) { acc[slot] } else { 0.0 },
                slot: slot as u32,
            };
            if heap.len() < depth {
                heap.push(std::cmp::Reverse(entry));
            } else if heap.peek().is_some_and(|w| entry > w.0) {
                heap.pop();
                heap.push(std::cmp::Reverse(entry));
            }
        }
        let mut ranked: Vec<Ranked> = heap.into_iter().map(|r| r.0).collect();
        ranked.sort_by(|a, b| b.cmp(a));
        let t_topk = ms(t);
        // Provenance: the ranked candidates scored again.
        let t = Instant::now();
        let mut top_slots: Vec<u32> = ranked.iter().map(|r| r.slot).collect();
        top_slots.sort_unstable();
        let again =
            bm25::score_candidates_plain_walk(index, &terms, &stats, params, &top_slots, walk);
        let t_again = ms(t);
        let total = t_lex + t_filter + t_and + t_materialize + t_alloc + t_score + t_topk + t_again;
        println!(
            "terms={terms:?} year_ge={year_ge:?} walk={walk:?} scan={} n={n} lexical={} filter={} members={} \
             candidates={} unscored={missing} | lexical {t_lex:.1} filter {t_filter:.1} and {t_and:.1} \
             materialize {t_materialize:.1} alloc {t_alloc:.1} score {t_score:.1} topk {t_topk:.1} \
             again {t_again:.1} | total {total:.1} ms",
            if filter_domain { "domain" } else { "full" },
            lexical.count(),
            filter.as_ref().map_or(n as u64, Bits::count),
            members.count(),
            candidates.len(),
        );
        println!(
            "top: {:?}",
            ranked.iter().map(|r| (r.slot, r.score)).collect::<Vec<_>>()
        );
        let _ = again;
    }
    Ok(())
}
