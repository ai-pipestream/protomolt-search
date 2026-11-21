//! Block-max (docs/block-max.md, stage 2) acceptance gates:
//!
//! - `top_k_pruned` is bit-identical to the `top_k_exhaustive` oracle
//!   over random corpora, random k / term counts / k1 / b, and floor
//!   seeds (unseeded, exact k-th best, above k-th best, mid-range);
//! - ties at the floor and at the k-th slot survive exactly (the `<=`
//!   skip test and strict-greater heap replacement);
//! - the skip run actually skips blocks on a seeded high-df query and
//!   (almost) never skips on an unseeded rare-term query;
//! - the v5 `score_candidates` shallow-advance path is bitwise equal to
//!   the merge-join fallback.
//!
//! All assertions are exact-equality on `(doc_id, score.to_bits(),
//! term_offsets)` sequences; RNG is a fixed-seed hand-rolled LCG.

#[path = "blockmax/walks.rs"]
mod walks;

use std::path::PathBuf;

use pipestream_search::bm25::{self, Bm25Params, CorpusStats, PruneStats, ScoredDoc};
use pipestream_search::postings::{AnalyzedDoc, Bm25Index, Bm25Reader, Bm25Store, DocTerms};

/// Hand-rolled deterministic RNG (LCG, same style as
/// `harness::unit_vectors`) — no proptest dependency.
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
        .join(format!("blockmax_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A random corpus: gappy ids, tf skew, mixed empty offsets, and a
/// doc-length distribution (constant for tie engineering when asked).
/// `wide` makes every vocab term mid-df (~25% of docs) so queries form
/// MaxScore essential/non-essential partitions in competitive windows.
fn random_corpus(
    rng: &mut Lcg,
    n_docs: u64,
    vocab: &[String],
    constant_dl: bool,
    wide: bool,
) -> Vec<(u32, String, AnalyzedDoc)> {
    let mut docs = Vec::new();
    let mut id = 0u32;
    for _ in 0..n_docs {
        id += 1 + rng.below(2) as u32;
        let mut terms: DocTerms = Vec::new();
        let mut length = 0;
        if wide {
            for term in vocab {
                if rng.below(4) != 0 {
                    continue;
                }
                let tf = 1 + rng.below(3) as u32;
                let offsets: Vec<(u32, u32)> = if rng.below(2) == 0 {
                    Vec::new()
                } else {
                    (0..tf).map(|o| (o * 10, o * 10 + 4)).collect()
                };
                length += tf;
                terms.push((term.clone(), tf, offsets));
            }
        } else {
            for _ in 0..1 + rng.below(4) {
                let term = vocab[rng.below(vocab.len() as u64) as usize].clone();
                if terms.iter().any(|(t, _, _)| *t == term) {
                    continue;
                }
                let tf = 1 + rng.below(5) as u32;
                let offsets: Vec<(u32, u32)> = if rng.below(2) == 0 {
                    Vec::new()
                } else {
                    (0..tf).map(|o| (o * 10, o * 10 + 4)).collect()
                };
                length += tf;
                terms.push((term, tf, offsets));
            }
        }
        if constant_dl {
            length = 100;
        }
        docs.push((id, format!("doc {id}"), AnalyzedDoc::body(terms, length)));
    }
    docs
}

fn build_store(corpus: &[(u32, String, AnalyzedDoc)]) -> Bm25Store {
    let mut store = Bm25Store::new();
    for (id, text, doc) in corpus {
        store.add_document(*id, text.clone(), doc.clone());
    }
    store
}

type Sig = Vec<(u32, u64, Vec<(usize, Vec<(u32, u32)>)>)>;

fn sig(hits: &[ScoredDoc]) -> Sig {
    hits.iter()
        .map(|h| (h.doc_id, h.score.to_bits(), h.term_offsets.clone()))
        .collect()
}

fn check_pruned_round(
    rng: &mut Lcg,
    round: u64,
    wide: bool,
    vocab: &[String],
    store: &Bm25Store,
    reader: &Bm25Reader,
) {
    let k1s = [0.0, 1.2, 2.5];
    let bs = [0.0, 0.5, 1.0];
    for _ in 0..4 {
        let max_terms = if wide { 8 } else { vocab.len().min(8) };
        let n_terms = 1 + rng.below(max_terms as u64) as usize;
        let terms: Vec<String> = vocab
            .iter()
            .filter(|_| rng.below(2) == 0)
            .take(n_terms.max(1))
            .cloned()
            .collect();
        let terms = if terms.is_empty() {
            vec![vocab[0].clone()]
        } else {
            terms
        };
        let stats = CorpusStats {
            doc_count: store.doc_count(),
            total_doc_length: store.total_doc_length(),
            dfs: terms.iter().map(|t| Bm25Index::df(store, t)).collect(),
        };
        let params = Bm25Params {
            k1: k1s[rng.below(3) as usize],
            b: bs[rng.below(3) as usize],
        };
        let k = 1 + rng.below(40) as usize;
        let oracle = bm25::top_k_exhaustive(store, &terms, &stats, params, k);
        let mut floors = vec![f64::NEG_INFINITY, 1e9];
        if oracle.len() == k {
            floors.push(oracle[k - 1].score);
        }
        if let Some(last) = oracle.last() {
            floors.push(last.score);
            floors.push(last.score + 1e-6);
        }
        if oracle.len() > 2 {
            floors.push(oracle[oracle.len() / 2].score);
        }
        for floor in floors {
            let want = bm25::top_k_exhaustive(
                store,
                &terms,
                &stats,
                params,
                bm25::TopK::new(k).floor(floor),
            );
            let mut prune = PruneStats::default();
            let got = bm25::top_k(
                reader,
                &terms,
                &stats,
                params,
                bm25::TopK::new(k).floor(floor).prune_stats(&mut prune),
            );
            assert_eq!(
                sig(&want),
                sig(&got),
                "round {round}, terms {terms:?}, k {k}, params {params:?}, floor {floor:e}"
            );
        }
    }
}

/// The bitwise property gate: pruned == exhaustive(+floor filter).
#[test]
fn pruned_matches_exhaustive_property() {
    let dir = test_dir("prop");
    let mut rng = Lcg::new(0xB10C4A25);
    for round in 0..30 {
        let wide = round % 3 == 0;
        let n_vocab = if wide { 12 } else { 2 + rng.below(30) };
        let vocab: Vec<String> = (0..n_vocab).map(|i| format!("t{i}")).collect();
        let n_docs = if wide { 300 } else { 40 + rng.below(500) };
        let constant_dl = !wide && rng.below(4) == 0;
        let corpus = random_corpus(&mut rng, n_docs, &vocab, constant_dl, wide);
        let store = build_store(&corpus);
        let path = dir.join(format!("r{round}.bm25"));
        store.save(&path).unwrap();
        let reader = Bm25Reader::open(&path).unwrap();
        check_pruned_round(&mut rng, round, wide, &vocab, &store, &reader);
        let _ = std::fs::remove_file(&path);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

fn assert_identical_doc_ties(dir: &std::path::Path) {
    let mut store = Bm25Store::new();
    for i in 0..400u32 {
        store.add_document(
            i,
            format!("doc {i}"),
            AnalyzedDoc::body(vec![("court".to_string(), 2, vec![(0, 4), (10, 14)])], 3),
        );
    }
    let path = dir.join("a.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let terms = vec!["court".to_string()];
    let stats = CorpusStats {
        doc_count: store.doc_count(),
        total_doc_length: store.total_doc_length(),
        dfs: vec![400],
    };
    let params = Bm25Params::default();
    let oracle = bm25::top_k_exhaustive(&store, &terms, &stats, params, 10);
    assert_eq!(oracle.len(), 10);
    let tie_score = oracle[0].score;
    assert!(oracle.iter().all(|h| h.score == tie_score));
    let want_ids: Vec<u32> = oracle.iter().map(|h| h.doc_id).collect();
    for floor in [f64::NEG_INFINITY, tie_score] {
        let got = bm25::top_k(
            &reader,
            &terms,
            &stats,
            params,
            bm25::TopK::new(10).floor(floor),
        );
        assert_eq!(
            sig(&oracle),
            sig(&got),
            "identical-doc corpus, floor {floor:e}"
        );
        assert_eq!(got.iter().map(|h| h.doc_id).collect::<Vec<_>>(), want_ids);
    }
    let got = bm25::top_k(
        &reader,
        &terms,
        &stats,
        params,
        bm25::TopK::new(10).floor(tie_score + 1e-9),
    );
    assert!(
        got.is_empty(),
        "floor above every score must return nothing"
    );
}

fn assert_cross_block_ties(dir: &std::path::Path) {
    let mut store = Bm25Store::new();
    for i in 0..300u32 {
        let tf = if i % 25 == 0 { 2 } else { 1 };
        store.add_document(
            i,
            format!("doc {i}"),
            AnalyzedDoc::body(vec![("court".to_string(), tf, vec![(0, 4)])], 3),
        );
    }
    let path = dir.join("b.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let terms = vec!["court".to_string()];
    let params = Bm25Params::default();
    let stats = CorpusStats {
        doc_count: store.doc_count(),
        total_doc_length: store.total_doc_length(),
        dfs: vec![300],
    };
    let oracle = bm25::top_k_exhaustive(&store, &terms, &stats, params, 10);
    let want_ids: Vec<u32> = (0..10u32).map(|i| i * 25).collect();
    assert_eq!(
        oracle.iter().map(|h| h.doc_id).collect::<Vec<_>>(),
        want_ids,
        "oracle sanity: ten smallest high-tf ids"
    );
    assert!(oracle.iter().all(|h| h.score == oracle[0].score));
    for (floor, want_evals) in [(f64::NEG_INFINITY, 19u64), (oracle[9].score, 10)] {
        let mut prune = PruneStats::default();
        let got = bm25::top_k(
            &reader,
            &terms,
            &stats,
            params,
            bm25::TopK::new(10).floor(floor).prune_stats(&mut prune),
        );
        assert_eq!(sig(&oracle), sig(&got), "cross-block tie, floor {floor:e}");
        assert_eq!(
            prune.candidates_evaluated, want_evals,
            "cross-block tie, floor {floor:e}: evaluations"
        );
    }
}

/// Tie torture: identical scores inside and across blocks.
#[test]
fn ties_at_floor_and_kth_slot() {
    let dir = test_dir("ties");
    assert_identical_doc_ties(&dir);
    assert_cross_block_ties(&dir);
    let _ = std::fs::remove_dir_all(&dir);
}

fn assert_skip_rates(store: &Bm25Store, reader: &Bm25Reader, n: u32) {
    let params = Bm25Params::default();
    let terms = vec!["court".to_string()];
    let stats = CorpusStats {
        doc_count: store.doc_count(),
        total_doc_length: store.total_doc_length(),
        dfs: vec![n],
    };
    let k = 10;
    let oracle = bm25::top_k_exhaustive(store, &terms, &stats, params, k);
    let seed = oracle[k - 1].score;
    let mut seeded = PruneStats::default();
    let got = bm25::top_k(
        reader,
        &terms,
        &stats,
        params,
        bm25::TopK::new(k).floor(seed).prune_stats(&mut seeded),
    );
    assert_eq!(sig(&oracle), sig(&got), "seeded result diverged");
    let seeded_frac = seeded.blocks_skipped as f64 / seeded.blocks_total as f64;
    assert!(
        seeded_frac > 0.5,
        "seeded high-df query should skip most blocks, got {seeded_frac:.2} ({}/{})",
        seeded.blocks_skipped,
        seeded.blocks_total
    );

    let mut unseeded = PruneStats::default();
    let got = bm25::top_k(
        reader,
        &terms,
        &stats,
        params,
        bm25::TopK::new(k).prune_stats(&mut unseeded),
    );
    assert_eq!(sig(&oracle), sig(&got));
    assert!(
        seeded.blocks_skipped > unseeded.blocks_skipped,
        "seeding must skip strictly more blocks ({} vs {})",
        seeded.blocks_skipped,
        unseeded.blocks_skipped
    );

    let rare_terms = vec!["rare3".to_string()];
    let rare_stats = CorpusStats {
        dfs: vec![Bm25Index::df(store, "rare3")],
        ..stats
    };
    let mut rare = PruneStats::default();
    let got = bm25::top_k(
        reader,
        &rare_terms,
        &rare_stats,
        params,
        bm25::TopK::new(k).prune_stats(&mut rare),
    );
    assert_eq!(rare.blocks_skipped, 0, "rare unseeded query must not skip");
    let oracle = bm25::top_k_exhaustive(store, &rare_terms, &rare_stats, params, k);
    assert_eq!(sig(&oracle), sig(&got));
}

#[test]
fn blocks_actually_skip() {
    let dir = test_dir("skip");
    let mut store = Bm25Store::new();
    let n = 3000u32;
    for i in 0..n {
        store.add_document(
            i,
            format!("doc {i}"),
            AnalyzedDoc::body(
                vec![
                    ("court".to_string(), 1 + (i / 128) % 5, vec![(0, 4)]),
                    (format!("rare{}", i % 97), 1, vec![(0, 4)]),
                ],
                100 + i % 50,
            ),
        );
    }
    let path = dir.join("s.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    assert_skip_rates(&store, &reader, n);
    let _ = std::fs::remove_dir_all(&dir);
}

/// The v5 score_candidates shallow-advance path is bitwise equal to the
/// merge-join fallback (heap store), over random candidate sets with
/// duplicates, unsorted input, and candidates with no postings.
#[test]
fn score_candidates_advance_matches_merge_join() {
    let dir = test_dir("cand");
    let mut rng = Lcg::new(0xCA5CADE);
    for round in 0..10 {
        let n_vocab = 3 + rng.below(10);
        let vocab: Vec<String> = (0..n_vocab).map(|i| format!("t{i}")).collect();
        let n_docs = 50 + rng.below(200);
        let corpus = random_corpus(&mut rng, n_docs, &vocab, false, false);
        let store = build_store(&corpus);
        let path = dir.join(format!("c{round}.bm25"));
        store.save(&path).unwrap();
        let reader = Bm25Reader::open(&path).unwrap();
        let n_take = 1 + rng.below(3) as usize;
        let terms: Vec<String> = vocab
            .iter()
            .filter(|_| rng.below(2) == 0)
            .take(n_take)
            .cloned()
            .collect();
        let terms = if terms.is_empty() {
            vec![vocab[0].clone()]
        } else {
            terms
        };
        let stats = CorpusStats {
            doc_count: store.doc_count(),
            total_doc_length: store.total_doc_length(),
            dfs: terms.iter().map(|t| Bm25Index::df(&store, t)).collect(),
        };
        let params = Bm25Params::default();
        // Random candidate set: unsorted, duplicates, gap slots, and
        // ids beyond the store entirely.
        let n_cand = rng.below(80);
        let candidates: Vec<u32> = (0..n_cand)
            .map(|_| rng.below(u64::from(store.next_doc_id()) + 20) as u32)
            .collect();
        let heap = bm25::score_candidates(&store, &terms, &stats, params, &candidates);
        let disk = bm25::score_candidates(&reader, &terms, &stats, params, &candidates);
        assert_eq!(sig(&heap), sig(&disk), "round {round}");
        let _ = std::fs::remove_file(&path);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// One `score_candidates` answer as the plain scorer's shape: the summed
/// score of every candidate a term occurs in, `None` for the rest.
fn plain_of(hits: &[ScoredDoc], candidates: &[u32]) -> Vec<Option<u64>> {
    candidates
        .iter()
        .map(|c| {
            hits.iter()
                .find(|h| h.doc_id == *c)
                .map(|h| h.score.to_bits())
        })
        .collect()
}

fn bits_of(scores: &[Option<f64>]) -> Vec<Option<u64>> {
    scores.iter().map(|s| s.map(f64::to_bits)).collect()
}

/// The plain candidate scorer answers bitwise the same whichever walk
/// visits a term's postings: the candidate walk (`advance_shallow` per
/// candidate), the postings walk (the doc run merge-joined against the
/// candidates), and the automatic choice between them; and the same as
/// the heap store's merge-join and as `score_candidates` with offsets.
/// Over random corpora with candidates outside every posting and past
/// the store, an empty candidate list, terms with df 0, and a segment
/// catalog whose parts each lack some term (the chained cursor skips
/// parts) with a heap tail holding one term (no cursor: the fallback).
#[test]
fn score_candidates_plain_walks_are_bitwise_equal() {
    use pipestream_search::bm25::CandidateWalk;
    let dir = test_dir("walks");
    let mut rng = Lcg::new(0x5C0BE);
    let params = Bm25Params::default();
    let all_walks = [
        CandidateWalk::Auto,
        CandidateWalk::Candidates,
        CandidateWalk::Postings,
    ];
    walks::assert_random_walks_match(&dir, &mut rng, params, all_walks);
    let (shard, whole, base) = walks::build_three_part_catalog(&dir, &mut rng);
    let terms: Vec<String> = (0..6).map(|i| format!("t{i}")).collect();
    let stats = CorpusStats {
        doc_count: Bm25Index::doc_count(&shard),
        total_doc_length: Bm25Index::total_doc_length(&shard),
        dfs: terms.iter().map(|t| Bm25Index::df(&shard, t)).collect(),
    };
    assert!(
        Bm25Index::impacts(&shard, "t0").is_none() && Bm25Index::impacts(&shard, "t1").is_some(),
        "the tail holds t0 (no cursor) and no other term"
    );
    for bound in [0u32, 7, 64, base as u32 + 3] {
        let candidates: Vec<u32> = (0..=bound).filter(|c| c % 2 == 0 || c % 5 == 0).collect();
        let oracle = plain_of(
            &bm25::score_candidates(&whole, &terms, &stats, params, &candidates),
            &candidates,
        );
        for walk in all_walks {
            let got = bm25::score_candidates_plain_walk(
                &shard,
                &terms,
                &stats,
                params,
                &candidates,
                walk,
            );
            assert_eq!(bits_of(&got), oracle, "catalog, bound {bound}, {walk:?}");
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}
