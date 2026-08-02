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

use std::path::PathBuf;

use turbovec_search::bm25::{self, Bm25Params, CorpusStats, PruneStats, ScoredDoc};
use turbovec_search::postings::{AnalyzedDoc, Bm25Index, Bm25Reader, Bm25Store, DocTerms};

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

/// The bitwise property gate: pruned == exhaustive(+floor filter).
#[test]
fn pruned_matches_exhaustive_property() {
    let dir = test_dir("prop");
    let mut rng = Lcg::new(0xB10C4A25);
    let k1s = [0.0, 1.2, 2.5];
    let bs = [0.0, 0.5, 1.0];
    for round in 0..30 {
        // Every third round is "wide": many mid-df terms and wide
        // queries, so competitive windows form essential/non-essential
        // partitions rather than resolving by skips alone.
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
                dfs: terms.iter().map(|t| Bm25Index::df(&store, t)).collect(),
            };
            let params = Bm25Params {
                k1: k1s[rng.below(3) as usize],
                b: bs[rng.below(3) as usize],
            };
            let k = 1 + rng.below(40) as usize;
            let oracle = bm25::top_k_exhaustive(&store, &terms, &stats, params, k);
            let mut floors = vec![f64::NEG_INFINITY, 1e9];
            if oracle.len() == k {
                floors.push(oracle[k - 1].score); // exact k-th best
            }
            if let Some(last) = oracle.last() {
                floors.push(last.score); // exact weakest hit
                floors.push(last.score + 1e-6); // just above it
            }
            if oracle.len() > 2 {
                floors.push(oracle[oracle.len() / 2].score); // mid-range
            }
            for floor in floors {
                let want = bm25::filter_to_floor(oracle.clone(), floor);
                let mut prune = PruneStats::default();
                let got =
                    bm25::top_k_pruned_stats(&reader, &terms, &stats, params, k, floor, &mut prune);
                assert_eq!(
                    sig(&want),
                    sig(&got),
                    "round {round}, terms {terms:?}, k {k}, params {params:?}, floor {floor:e}"
                );
            }
        }
        let _ = std::fs::remove_file(&path);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Tie torture: identical scores inside and across blocks. The pruned
/// path must keep exactly the docs the exhaustive path keeps — ties at
/// the floor survive, and heap displacement is strictly-greater so the
/// smaller doc id always wins.
#[test]
fn ties_at_floor_and_kth_slot() {
    let dir = test_dir("ties");

    // Corpus A: 400 fully identical docs (same score everywhere, blocks
    // packed with ties).
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
    // Unseeded, exact-floor, and mid-tie floors all return the same ten.
    for floor in [f64::NEG_INFINITY, tie_score] {
        let got = bm25::top_k_pruned(&reader, &terms, &stats, params, 10, floor);
        assert_eq!(
            sig(&oracle),
            sig(&got),
            "identical-doc corpus, floor {floor:e}"
        );
        assert_eq!(got.iter().map(|h| h.doc_id).collect::<Vec<_>>(), want_ids);
    }
    // A hair above the tie: nothing survives.
    let got = bm25::top_k_pruned(&reader, &terms, &stats, params, 10, tie_score + 1e-9);
    assert!(
        got.is_empty(),
        "floor above every score must return nothing"
    );

    // Corpus B: the k-th slot ties ACROSS blocks: 12 high docs (tf=2)
    // spread over 300 tf=1 docs; k=10 keeps the ten smallest ids.
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
    // Every oracle hit has the same score: the k-th slot is a tie with
    // the two dropped high-tf docs.
    assert!(oracle.iter().all(|h| h.score == oracle[0].score));
    // Unseeded: 19 evaluations (the 10 initial docs plus the 9 tied
    // high-tf docs that displace; every other candidate is dropped by
    // the candidate test — ties at the k-th score can never displace).
    // Seeded at the tie score: 10 evaluations (the heap fills with the
    // ten tied winners; everything else is inert on arrival).
    for (floor, want_evals) in [(f64::NEG_INFINITY, 19u64), (oracle[9].score, 10)] {
        let mut prune = PruneStats::default();
        let got = bm25::top_k_pruned_stats(&reader, &terms, &stats, params, 10, floor, &mut prune);
        assert_eq!(sig(&oracle), sig(&got), "cross-block tie, floor {floor:e}");
        assert_eq!(
            prune.candidates_evaluated, want_evals,
            "cross-block tie, floor {floor:e}: evaluations"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Skip accounting: a seeded floor on a high-df term skips most blocks;
/// an unseeded rare-term query skips (almost) none — the doc's stage-2
/// prediction.
#[test]
fn blocks_actually_skip() {
    let dir = test_dir("skip");
    let mut store = Bm25Store::new();
    let n = 3000u32;
    for i in 0..n {
        // Block-correlated tf (whole 128-posting blocks share one tf,
        // cycling 1..=5) so block maxes genuinely differ: stationary tf
        // would put the same max in every block and nothing could skip.
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
    let params = Bm25Params::default();

    // High-df term, seeded with the true k-th best (the realistic
    // coordinator case): most blocks cannot reach the floor.
    let terms = vec!["court".to_string()];
    let stats = CorpusStats {
        doc_count: store.doc_count(),
        total_doc_length: store.total_doc_length(),
        dfs: vec![n],
    };
    let k = 10;
    let oracle = bm25::top_k_exhaustive(&store, &terms, &stats, params, k);
    let seed = oracle[k - 1].score;
    let mut seeded = PruneStats::default();
    let got = bm25::top_k_pruned_stats(&reader, &terms, &stats, params, k, seed, &mut seeded);
    assert_eq!(sig(&oracle), sig(&got), "seeded result diverged");
    let seeded_frac = seeded.blocks_skipped as f64 / seeded.blocks_total as f64;
    assert!(
        seeded_frac > 0.5,
        "seeded high-df query should skip most blocks, got {seeded_frac:.2} \
         ({}/{})",
        seeded.blocks_skipped,
        seeded.blocks_total
    );

    // Same query unseeded: fewer skips (the floor starts at -inf), but
    // the result is identical.
    let mut unseeded = PruneStats::default();
    let got = bm25::top_k_pruned_stats(
        &reader,
        &terms,
        &stats,
        params,
        k,
        f64::NEG_INFINITY,
        &mut unseeded,
    );
    assert_eq!(sig(&oracle), sig(&got));
    assert!(
        seeded.blocks_skipped > unseeded.blocks_skipped,
        "seeding must skip strictly more blocks ({} vs {})",
        seeded.blocks_skipped,
        unseeded.blocks_skipped
    );

    // Rare term, unseeded: one block total, nothing to skip — and the
    // floor never outruns a single-block list.
    let rare_terms = vec!["rare3".to_string()];
    let rare_stats = CorpusStats {
        dfs: vec![Bm25Index::df(&store, "rare3")],
        ..stats.clone()
    };
    let mut rare = PruneStats::default();
    let got = bm25::top_k_pruned_stats(
        &reader,
        &rare_terms,
        &rare_stats,
        params,
        k,
        f64::NEG_INFINITY,
        &mut rare,
    );
    assert_eq!(rare.blocks_skipped, 0, "rare unseeded query must not skip");
    let oracle = bm25::top_k_exhaustive(&store, &rare_terms, &rare_stats, params, k);
    assert_eq!(sig(&oracle), sig(&got));

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

/// Seeded-floor f32 round trip (review issue: `f32(kth)` rounds up half
/// the time and filters the boundary hit). `bm25::floor_seed` emits one
/// ULP below the wire k-th best, so seeding with the emitted value is
/// lossless: seeded == unseeded for EVERY k, tie clusters included.
/// Expected lost boundary hits across the whole sweep: 0.
#[test]
fn seeded_round_trip_never_loses_boundary_hits() {
    let dir = test_dir("roundtrip");
    let mut rng = Lcg::new(0x5EED_CAFE);
    let mut checked = 0usize;
    for round in 0..15 {
        let n_vocab = 2 + rng.below(12);
        let vocab: Vec<String> = (0..n_vocab).map(|i| format!("t{i}")).collect();
        let n_docs = 30 + rng.below(200);
        let constant_dl = rng.below(2) == 0;
        let wide = rng.below(3) == 0;
        let corpus = random_corpus(&mut rng, n_docs, &vocab, constant_dl, wide);
        let store = build_store(&corpus);
        let path = dir.join(format!("rt{round}.bm25"));
        store.save(&path).unwrap();
        let reader = Bm25Reader::open(&path).unwrap();
        let n_take = 1 + rng.below(4) as usize;
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
        let params = Bm25Params {
            k1: 0.4 + rng.below(20) as f64 / 10.0,
            b: rng.below(10) as f64 / 10.0,
        };
        let k_max = (store.doc_count() as usize).min(60);
        for k in 1..=k_max {
            let unseeded =
                bm25::top_k_pruned(&reader, &terms, &stats, params, k, f64::NEG_INFINITY);
            if unseeded.len() < k {
                break; // heap cannot fill: no kth_best emitted for larger k
            }
            // The wire emission: f32 of the k-th score, one ULP down.
            let emitted = bm25::floor_seed(unseeded[k - 1].score as f32);
            let seeded = bm25::top_k_pruned(&reader, &terms, &stats, params, k, f64::from(emitted));
            assert_eq!(
                sig(&unseeded),
                sig(&seeded),
                "round {round} k {k}: boundary hit lost (emitted {emitted:e})"
            );
            checked += 1;
        }
        let _ = std::fs::remove_file(&path);
    }
    assert!(checked > 200, "round-trip sweep too thin: {checked}");

    // Engineered tie cluster at the boundary: identical docs, identical
    // scores — the exact case f32 rounding used to drop.
    let mut store = Bm25Store::new();
    for i in 0..200u32 {
        store.add_document(
            i,
            format!("doc {i}"),
            AnalyzedDoc::body(vec![("court".to_string(), 2, vec![(0, 4), (10, 14)])], 3),
        );
    }
    let path = dir.join("ties.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let terms = vec!["court".to_string()];
    let stats = CorpusStats {
        doc_count: store.doc_count(),
        total_doc_length: store.total_doc_length(),
        dfs: vec![200],
    };
    let params = Bm25Params::default();
    let unseeded = bm25::top_k_pruned(&reader, &terms, &stats, params, 10, f64::NEG_INFINITY);
    assert!(unseeded.iter().all(|h| h.score == unseeded[0].score));
    let emitted = bm25::floor_seed(unseeded[9].score as f32);
    assert!(
        f64::from(emitted) < unseeded[9].score,
        "seed must sit strictly below the tie score"
    );
    let seeded = bm25::top_k_pruned(&reader, &terms, &stats, params, 10, f64::from(emitted));
    assert_eq!(sig(&unseeded), sig(&seeded), "tie cluster at the boundary");

    let _ = std::fs::remove_dir_all(&dir);
}

/// Level-1-scale fuzz: corpora big enough for terms to span multiple
/// level-1 groups (128 blocks x 128 postings), heavy tie pressure from
/// cloned doc patterns, duplicate and absent query terms, and floors
/// including exact-kth and mid-range. Bitwise vs the exhaustive oracle.
/// With debug assertions armed (the default for `cargo test`), the
/// doc-order invariant inside `top_k_pruned` is checked on every
/// candidate selection — this fuzz must produce zero out-of-order
/// selections. (Adopted shape from the review's level-1 harness.)
#[test]
fn pruned_level1_scale_fuzz() {
    let dir = test_dir("l1fuzz");
    let mut rng = Lcg::new(0x1E11_F022);
    // Cloned doc patterns are the tie pressure: every clone produces an
    // identical score for the terms it carries.
    for round in 0..24u64 {
        let n_vocab = 6 + rng.below(10);
        let vocab: Vec<String> = (0..n_vocab).map(|i| format!("t{i}")).collect();
        let n_patterns = 30 + rng.below(30);
        let patterns: Vec<AnalyzedDoc> = (0..n_patterns)
            .map(|_| {
                let mut terms: DocTerms = Vec::new();
                let mut length = 0;
                for _ in 0..1 + rng.below(4) {
                    let term = vocab[rng.below(vocab.len() as u64) as usize].clone();
                    if terms.iter().any(|(t, _, _)| *t == term) {
                        continue;
                    }
                    let tf = 1 + rng.below(3) as u32;
                    let offsets = (0..tf).map(|o| (o * 10, o * 10 + 4)).collect();
                    length += tf;
                    terms.push((term, tf, offsets));
                }
                AnalyzedDoc::body(terms, length)
            })
            .collect();
        let n_docs = 20_000 + rng.below(24_000);
        let mut store = Bm25Store::new();
        let mut id = 0u32;
        for _ in 0..n_docs {
            id += 1 + rng.below(2) as u32;
            let doc = patterns[rng.below(patterns.len() as u64) as usize].clone();
            store.add_document(id, format!("doc {id}"), doc);
        }
        let path = dir.join(format!("f{round}.bm25"));
        store.save(&path).unwrap();
        let reader = Bm25Reader::open(&path).unwrap();

        for _ in 0..40 {
            // Duplicate and absent terms on purpose.
            let n_terms = 1 + rng.below(4) as usize;
            let mut terms: Vec<String> = (0..n_terms)
                .map(|_| vocab[rng.below(vocab.len() as u64) as usize].clone())
                .collect();
            if rng.below(3) == 0 && !terms.is_empty() {
                let dup = terms[0].clone();
                terms.push(dup);
            }
            if rng.below(4) == 0 {
                terms.push("absent-term".to_string());
            }
            let stats = CorpusStats {
                doc_count: store.doc_count(),
                total_doc_length: store.total_doc_length(),
                dfs: terms.iter().map(|t| Bm25Index::df(&store, t)).collect(),
            };
            let params = Bm25Params {
                k1: [0.0, 1.2, 2.0][rng.below(3) as usize],
                b: [0.0, 0.75, 1.0][rng.below(3) as usize],
            };
            let k = 1 + rng.below(30) as usize;
            let oracle = bm25::top_k_exhaustive(&store, &terms, &stats, params, k);
            let mut floors = vec![f64::NEG_INFINITY];
            if oracle.len() == k {
                floors.push(oracle[k - 1].score); // exact k-th best
            }
            if oracle.len() > 2 {
                floors.push(oracle[oracle.len() / 2].score); // mid-range
            }
            for floor in floors {
                let want = bm25::filter_to_floor(oracle.clone(), floor);
                let mut prune = PruneStats::default();
                let got =
                    bm25::top_k_pruned_stats(&reader, &terms, &stats, params, k, floor, &mut prune);
                assert_eq!(
                    sig(&want),
                    sig(&got),
                    "round {round}, terms {terms:?}, k {k}, floor {floor:e}"
                );
            }
        }
        let _ = std::fs::remove_file(&path);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// A term that lives on ANOTHER shard must not forfeit pruning here.
///
/// `CorpusStats.dfs` is the GLOBAL df the coordinator computed, so a rare
/// term is non-zero there even on the shards that do not contain it.
/// Those shards have no impact surface to open for it, and the scorer
/// used to read that as "this index lacks impacts" and drop the WHOLE
/// query to the exhaustive path -- walking every posting of the common
/// terms it could otherwise have skipped.
///
/// On a sharded corpus this fires for exactly the rare, discriminative
/// terms that make pruning worth having. Measured on the live 86.6M-chunk
/// fleet before the fix: "of 12b6" took 2710 ms where "of" alone took 9,
/// because 7 of 8 shards lacked the rare term and each then walked all
/// 83.7M postings of "of".
#[test]
fn a_term_absent_from_this_shard_does_not_disable_pruning() {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("bm_absent_term");
    std::fs::create_dir_all(&dir).unwrap();

    // This shard holds only "court". "12b6" exists elsewhere in the
    // cluster, so its GLOBAL df is non-zero while its local df is 0.
    let mut store = Bm25Store::new();
    for i in 0..2000u32 {
        let tf = if i % 97 == 0 { 5 } else { 1 };
        store.add_document(
            i,
            format!("doc {i}"),
            AnalyzedDoc::body(vec![("court".to_string(), tf, vec![(0, 5)])], 4),
        );
    }
    let path = dir.join("absent.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();

    let params = Bm25Params::default();
    let terms = vec!["court".to_string(), "12b6".to_string()];
    let stats = CorpusStats {
        doc_count: store.doc_count(),
        total_doc_length: store.total_doc_length(),
        // Global dfs: the rare term is present in the CLUSTER.
        dfs: vec![2000, 10],
    };

    let oracle = bm25::top_k_exhaustive(&store, &terms, &stats, params, 10);
    let mut prune = PruneStats::default();
    let got = bm25::top_k_pruned_stats(
        &reader,
        &terms,
        &stats,
        params,
        10,
        f64::NEG_INFINITY,
        &mut prune,
    );

    // Correctness first: skipping a locally-absent term is only valid
    // because it contributes 0 to every document here.
    assert_eq!(
        sig(&oracle),
        sig(&got),
        "skipping a locally-absent term must not change a single score"
    );

    // And the point of the fix: pruning actually ran. The exhaustive
    // fallback reports no blocks at all, so a non-zero total proves the
    // pruned path was taken rather than silently bypassed.
    assert!(
        prune.blocks_total > 0,
        "the query fell back to exhaustive despite every PRESENT term having impacts"
    );
    // blocks_skipped is deliberately NOT asserted. How much this fixture
    // skips depends on its score distribution, not on the fix: the bug
    // was that the pruned scorer was never ENTERED, and blocks_total
    // proves entry (the exhaustive fallback walks no blocks and reports
    // none). Other gates in this file cover skip effectiveness.
    let seeded = bm25::top_k_pruned(&reader, &terms, &stats, params, 10, oracle[9].score);
    assert_eq!(
        sig(&bm25::filter_to_floor(oracle.clone(), oracle[9].score)),
        sig(&seeded),
        "a seeded floor must not change the surviving hits"
    );

    // The single-term query is the control: it always pruned, and adding
    // a term this shard does not hold must not make things worse.
    let mut solo = PruneStats::default();
    let _ = bm25::top_k_pruned_stats(
        &reader,
        &["court".to_string()],
        &CorpusStats {
            doc_count: store.doc_count(),
            total_doc_length: store.total_doc_length(),
            dfs: vec![2000],
        },
        params,
        10,
        f64::NEG_INFINITY,
        &mut solo,
    );
    assert_eq!(
        prune.blocks_total, solo.blocks_total,
        "a locally-absent term should add no blocks to walk"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
