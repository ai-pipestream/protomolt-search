//! Block-max boundary round-trip, multi-group level-1 fuzz, and shard-absent term gates.

use std::path::{Path, PathBuf};

use pipestream_search::bm25::{self, Bm25Params, CorpusStats, PruneStats, ScoredDoc, TopK};
use pipestream_search::postings::{AnalyzedDoc, Bm25Index, Bm25Reader, Bm25Store, DocTerms};

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
        .join(format!("blockmax_scale_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

type Sig = Vec<(u32, u64, Vec<(usize, Vec<(u32, u32)>)>)>;

fn sig(hits: &[ScoredDoc]) -> Sig {
    hits.iter()
        .map(|h| (h.doc_id, h.score.to_bits(), h.term_offsets.clone()))
        .collect()
}

fn random_corpus(
    rng: &mut Lcg,
    n_docs: u64,
    vocab: &[String],
    constant_dl: bool,
    wide: bool,
) -> Bm25Store {
    let mut store = Bm25Store::new();
    let mut id = 0u32;
    for _ in 0..n_docs {
        id += 1 + rng.below(2) as u32;
        let (terms, raw_len) = sample_doc_terms(rng, vocab, wide);
        let length = if constant_dl { 100 } else { raw_len };
        store.add_document(id, format!("doc {id}"), AnalyzedDoc::body(terms, length));
    }
    store
}

fn sample_doc_terms(rng: &mut Lcg, vocab: &[String], wide: bool) -> (DocTerms, u32) {
    let mut terms: DocTerms = Vec::new();
    let mut length = 0;
    if wide {
        for term in vocab {
            if rng.below(4) != 0 {
                continue;
            }
            let tf = 1 + rng.below(3) as u32;
            let offsets = if rng.below(2) == 0 {
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
            let offsets = if rng.below(2) == 0 {
                Vec::new()
            } else {
                (0..tf).map(|o| (o * 10, o * 10 + 4)).collect()
            };
            length += tf;
            terms.push((term, tf, offsets));
        }
    }
    (terms, length)
}

fn assert_tie_cluster_round_trip(dir: &Path) {
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
    let unseeded = bm25::top_k(&reader, &terms, &stats, params, 10);
    assert!(unseeded.iter().all(|h| h.score == unseeded[0].score));
    let emitted = bm25::floor_seed(unseeded[9].score as f32);
    assert!(
        f64::from(emitted) < unseeded[9].score,
        "seed must sit strictly below the tie score"
    );
    let seeded = bm25::top_k(
        &reader,
        &terms,
        &stats,
        params,
        TopK::new(10).floor(f64::from(emitted)),
    );
    assert_eq!(sig(&unseeded), sig(&seeded), "tie cluster at the boundary");
}

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
        let store = random_corpus(&mut rng, n_docs, &vocab, constant_dl, wide);
        let path = dir.join(format!("rt{round}.bm25"));
        store.save(&path).unwrap();
        let reader = Bm25Reader::open(&path).unwrap();
        let n_take = 1 + rng.below(4) as usize;
        let mut terms: Vec<String> = vocab
            .iter()
            .filter(|_| rng.below(2) == 0)
            .take(n_take)
            .cloned()
            .collect();
        if terms.is_empty() {
            terms.push(vocab[0].clone());
        }
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
            let unseeded = bm25::top_k(&reader, &terms, &stats, params, k);
            if unseeded.len() < k {
                break;
            }
            let emitted = bm25::floor_seed(unseeded[k - 1].score as f32);
            let seeded = bm25::top_k(
                &reader,
                &terms,
                &stats,
                params,
                TopK::new(k).floor(f64::from(emitted)),
            );
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
    assert_tie_cluster_round_trip(&dir);
    let _ = std::fs::remove_dir_all(&dir);
}

fn build_patterned_store(rng: &mut Lcg, vocab: &[String]) -> Bm25Store {
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
    store
}

fn check_level1_fuzz_queries(
    rng: &mut Lcg,
    round: u64,
    vocab: &[String],
    store: &Bm25Store,
    reader: &Bm25Reader,
) {
    for _ in 0..40 {
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
            dfs: terms.iter().map(|t| Bm25Index::df(store, t)).collect(),
        };
        let params = Bm25Params {
            k1: [0.0, 1.2, 2.0][rng.below(3) as usize],
            b: [0.0, 0.75, 1.0][rng.below(3) as usize],
        };
        let k = 1 + rng.below(30) as usize;
        let oracle = bm25::top_k_exhaustive(store, &terms, &stats, params, k);
        let mut floors = vec![f64::NEG_INFINITY];
        if oracle.len() == k {
            floors.push(oracle[k - 1].score);
        }
        if oracle.len() > 2 {
            floors.push(oracle[oracle.len() / 2].score);
        }
        for floor in floors {
            let want =
                bm25::top_k_exhaustive(store, &terms, &stats, params, TopK::new(k).floor(floor));
            let mut prune = PruneStats::default();
            let got = bm25::top_k(
                reader,
                &terms,
                &stats,
                params,
                TopK::new(k).floor(floor).prune_stats(&mut prune),
            );
            assert_eq!(
                sig(&want),
                sig(&got),
                "round {round}, terms {terms:?}, k {k}, floor {floor:e}"
            );
        }
    }
}

#[test]
fn pruned_level1_scale_fuzz() {
    let dir = test_dir("l1fuzz");
    let mut rng = Lcg::new(0x1E11_F022);
    for round in 0..24u64 {
        let n_vocab = 6 + rng.below(10);
        let vocab: Vec<String> = (0..n_vocab).map(|i| format!("t{i}")).collect();
        let store = build_patterned_store(&mut rng, &vocab);
        let path = dir.join(format!("f{round}.bm25"));
        store.save(&path).unwrap();
        let reader = Bm25Reader::open(&path).unwrap();
        check_level1_fuzz_queries(&mut rng, round, &vocab, &store, &reader);
        let _ = std::fs::remove_file(&path);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_term_absent_from_this_shard_does_not_disable_pruning() {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("bm_absent_term");
    std::fs::create_dir_all(&dir).unwrap();
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
        dfs: vec![2000, 10],
    };
    let oracle = bm25::top_k_exhaustive(&store, &terms, &stats, params, 10);
    let mut prune = PruneStats::default();
    let got = bm25::top_k(
        &reader,
        &terms,
        &stats,
        params,
        TopK::new(10).prune_stats(&mut prune),
    );
    assert_eq!(
        sig(&oracle),
        sig(&got),
        "skipping a locally-absent term must not change a single score"
    );
    assert!(
        prune.blocks_total > 0,
        "the query fell back to exhaustive despite every PRESENT term having impacts"
    );
    let floor = oracle[9].score;
    let seeded = bm25::top_k(&reader, &terms, &stats, params, TopK::new(10).floor(floor));
    assert_eq!(
        sig(&bm25::filter_to_floor(oracle, floor)),
        sig(&seeded),
        "a seeded floor must not change the surviving hits"
    );

    let mut solo = PruneStats::default();
    let solo_stats = CorpusStats {
        doc_count: store.doc_count(),
        total_doc_length: store.total_doc_length(),
        dfs: vec![2000],
    };
    let _ = bm25::top_k(
        &reader,
        &["court".to_string()],
        &solo_stats,
        params,
        TopK::new(10).prune_stats(&mut solo),
    );
    assert_eq!(
        prune.blocks_total, solo.blocks_total,
        "a locally-absent term should add no blocks to walk"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
