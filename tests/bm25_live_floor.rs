//! The live-floor scorer contract (`docs/block-max.md`, the bidi relay):
//! `top_k_pruned_chained_filtered_stats_live` with a hook is the seeded-
//! floor contract delivered mid-scan. A hook that never raises must be
//! bit-identical to the plain pruned scorer; a hook that raises to an
//! emission-safe seed at the first poll must be bit-identical to seeding
//! the same value up front; and the k-th bests offered for publication
//! must be monotone and never exceed the true k-th best's seed.

use std::path::PathBuf;

use pipestream_search::bm25::{self, Bm25Params, CorpusStats, PruneStats, ScoredDoc};
use pipestream_search::postings::{AnalyzedDoc, Bm25Index, Bm25Reader, Bm25Store, DocTerms};

/// Hand-rolled deterministic RNG (LCG, same style as the unit tests).
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_add(0x9E3779B97F4A7C15))
    }
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn test_dir(name: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target/tmp")
        .join(format!("live_floor_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A random corpus big enough that common terms span multiple 128-
/// posting skip blocks, so raises actually have blocks to skip.
fn build_store(rng: &mut Lcg, n_docs: u64, vocab: &[String]) -> Bm25Store {
    let mut store = Bm25Store::new();
    let mut id = 0u32;
    for _ in 0..n_docs {
        id += 1 + rng.below(3) as u32;
        let mut terms: DocTerms = Vec::new();
        let mut length = 0;
        for term in vocab {
            if rng.below(3) != 0 {
                continue;
            }
            let tf = 1 + rng.below(4) as u32;
            length += tf;
            terms.push((term.clone(), tf, Vec::new()));
        }
        store.add_document(id, format!("doc {id}"), AnalyzedDoc::body(terms, length));
    }
    store
}

fn sig(hits: &[ScoredDoc]) -> Vec<(u32, u64)> {
    hits.iter().map(|h| (h.doc_id, h.score.to_bits())).collect()
}

#[allow(clippy::too_many_arguments)]
fn run_live(
    reader: &Bm25Reader,
    terms: &[String],
    stats: &CorpusStats,
    params: Bm25Params,
    k: usize,
    floor: f64,
    hook: &mut dyn FnMut(Option<f32>) -> Option<f32>,
) -> Vec<ScoredDoc> {
    let mut prune = PruneStats::default();
    bm25::top_k_pruned_chained_filtered_stats_live(
        reader,
        terms,
        stats,
        params,
        k,
        floor,
        None,
        None,
        &mut prune,
        Some(hook),
    )
}

#[test]
fn live_floor_matches_the_seeded_contract_exactly() {
    let dir = test_dir("contract");
    let vocab: Vec<String> = (0..10).map(|i| format!("t{i}")).collect();
    let mut rng = Lcg::new(0xF10_0D5);
    let store = build_store(&mut rng, 3000, &vocab);
    let path = dir.join("shard.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    assert!(reader.has_impacts("t0"), "no block-max surface, no test");

    let params = Bm25Params::default();
    let queries: Vec<Vec<String>> = vec![
        vec!["t0".into()],
        vec!["t1".into(), "t3".into()],
        vec!["t0".into(), "t5".into(), "t6".into(), "t9".into()],
    ];
    for terms in &queries {
        let stats = CorpusStats {
            doc_count: Bm25Index::doc_count(&reader),
            total_doc_length: reader.total_doc_length(),
            dfs: terms.iter().map(|t| Bm25Index::df(&reader, t)).collect(),
        };
        for k in [1usize, 10, 100] {
            let reference =
                bm25::top_k_pruned(&reader, terms, &stats, params, k, f64::NEG_INFINITY);

            // A hook that never raises is the plain scorer, bit for bit,
            // and the seeds it is offered are monotone and never exceed
            // the true k-th best's emission-safe seed.
            let mut offered: Vec<f32> = Vec::new();
            let mut silent = |seed: Option<f32>| {
                if let Some(s) = seed {
                    offered.push(s);
                }
                None
            };
            let live = run_live(
                &reader,
                terms,
                &stats,
                params,
                k,
                f64::NEG_INFINITY,
                &mut silent,
            );
            assert_eq!(
                sig(&reference),
                sig(&live),
                "silent hook ({terms:?}, k={k})"
            );
            assert!(
                offered.windows(2).all(|w| w[0] <= w[1]),
                "offered seeds must be monotone ({terms:?}, k={k})"
            );
            if reference.len() == k {
                let kth_seed = bm25::floor_seed(reference[k - 1].score as f32);
                assert!(
                    offered.iter().all(|&s| s <= kth_seed),
                    "an offered seed exceeded the true k-th best's seed ({terms:?}, k={k})"
                );
            }

            // A hook that answers the true k-th best's seed from the very
            // first poll (before any insertion) is exactly the seeded
            // scorer: this is the strongest floor the relay could ever
            // deliver, arriving at the earliest possible moment.
            if reference.len() == k {
                let seed = bm25::floor_seed(reference[k - 1].score as f32);
                let seeded = bm25::top_k_pruned(&reader, terms, &stats, params, k, f64::from(seed));
                let mut eager = |_seed: Option<f32>| Some(seed);
                let live = run_live(
                    &reader,
                    terms,
                    &stats,
                    params,
                    k,
                    f64::NEG_INFINITY,
                    &mut eager,
                );
                assert_eq!(sig(&seeded), sig(&live), "eager hook ({terms:?}, k={k})");
                assert_eq!(
                    sig(&reference),
                    sig(&live),
                    "an emission-safe seed must lose nothing ({terms:?}, k={k})"
                );
            }

            // A raise that arrives mid-scan (after the first poll but at
            // a deterministic point: once the heap has filled) can only
            // skip work, never change what the coordinator would merge:
            // every hit at or above the floor survives identically.
            if reference.len() == k {
                let seed = bm25::floor_seed(reference[k - 1].score as f32);
                let mut late = |offered: Option<f32>| offered.is_some().then_some(seed);
                let live = run_live(
                    &reader,
                    terms,
                    &stats,
                    params,
                    k,
                    f64::NEG_INFINITY,
                    &mut late,
                );
                assert_eq!(sig(&reference), sig(&live), "late hook ({terms:?}, k={k})");
            }
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}
