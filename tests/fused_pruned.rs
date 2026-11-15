//! Fused pruned scorer (docs/multi-field.md, build order step 3)
//! acceptance gates — contract 2 for the fused weighted sum:
//!
//! - `top_k_fused_pruned` is bit-identical to the
//!   `top_k_fused_exhaustive` oracle over random two-field corpora,
//!   random per-field k1 / b / weights (zero weight included), random
//!   k, and floor seeds (unseeded, exact k-th best, above the weakest
//!   hit, mid-range);
//! - a single body field at weight 1.0 reproduces `top_k_pruned` bit
//!   for bit (the degenerate identity that lets the fused path serve
//!   single-field queries);
//! - fused ties at the floor and at the k-th slot survive exactly;
//! - the fused skip machinery actually skips blocks on a seeded query;
//! - the fallbacks (no impacts, negative weight) equal the
//!   floor-filtered exhaustive scorer;
//! - fused pruned distributed equals monolithic with per-field merged
//!   global stats, unseeded and seeded at the global k-th best.
//!
//! All assertions are exact-equality on `(doc_id, score.to_bits(),
//! term_offsets)` sequences; RNG is a fixed-seed hand-rolled LCG.

use std::path::PathBuf;

use pipestream_search::bm25::{self, Bm25Params, CorpusStats, FieldQuery, FusedDoc, PruneStats};
use pipestream_search::postings::{
    AnalyzedDoc, AnalyzedField, Bm25Index, Bm25Reader, Bm25Store, DocTerms,
};

/// Hand-rolled deterministic RNG (LCG, same as `tests/blockmax.rs`).
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
        .join(format!("fusedpruned_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A random two-field corpus ("body", "name"): gappy ids, tf skew,
/// mixed empty offsets, terms shared across fields with different
/// postings, and docs without the name field. `wide` makes every vocab
/// term mid-df so MaxScore partitions form in competitive windows.
fn random_two_field_corpus(
    rng: &mut Lcg,
    n_docs: u64,
    body_vocab: &[String],
    name_vocab: &[String],
    wide: bool,
) -> Vec<(u32, String, AnalyzedDoc)> {
    let mut docs = Vec::new();
    let mut id = 0u32;
    for _ in 0..n_docs {
        id += 1 + rng.below(2) as u32;
        let mut body: DocTerms = Vec::new();
        let mut body_len = 0;
        if wide {
            for term in body_vocab {
                if rng.below(4) != 0 {
                    continue;
                }
                let tf = 1 + rng.below(3) as u32;
                let offsets: Vec<(u32, u32)> = if rng.below(2) == 0 {
                    Vec::new()
                } else {
                    (0..tf).map(|o| (o * 10, o * 10 + 4)).collect()
                };
                body_len += tf;
                body.push((term.clone(), tf, offsets));
            }
        } else {
            for _ in 0..1 + rng.below(4) {
                let term = body_vocab[rng.below(body_vocab.len() as u64) as usize].clone();
                if body.iter().any(|(t, _, _)| *t == term) {
                    continue;
                }
                let tf = 1 + rng.below(5) as u32;
                let offsets: Vec<(u32, u32)> = if rng.below(2) == 0 {
                    Vec::new()
                } else {
                    (0..tf).map(|o| (o * 10, o * 10 + 4)).collect()
                };
                body_len += tf;
                body.push((term, tf, offsets));
            }
        }
        let mut fields = vec![AnalyzedField {
            sentences: None,
            terms: body,
            length: body_len,
            positions: None,
            ..Default::default()
        }];
        // Two docs in three carry a name field, shorter and flatter.
        if rng.below(3) != 2 {
            let mut name: DocTerms = Vec::new();
            let mut name_len = 0;
            for term in name_vocab {
                if rng.below(if wide { 3 } else { 4 }) != 0 {
                    continue;
                }
                let tf = 1 + rng.below(2) as u32;
                name_len += tf;
                name.push((term.clone(), tf, vec![(0, 4)]));
            }
            fields.push(AnalyzedField {
                sentences: None,
                terms: name,
                length: name_len,
                positions: None,
                ..Default::default()
            });
        }
        docs.push((
            id,
            format!("doc {id}"),
            AnalyzedDoc {
                fields,
                ..Default::default()
            },
        ));
    }
    docs
}

fn build_store(corpus: &[(u32, String, AnalyzedDoc)]) -> Bm25Store {
    let mut store = Bm25Store::with_fields(&["body", "name"]);
    for (id, text, doc) in corpus {
        store.add_document(*id, text.clone(), doc.clone());
    }
    store
}

/// Global stats for one field of one store — the monolithic case where
/// the store's own share IS the global.
fn field_stats(store: &Bm25Store, f: usize, terms: &[String]) -> CorpusStats {
    CorpusStats {
        doc_count: store.doc_count(),
        total_doc_length: store.field(f).total_doc_length(),
        dfs: terms.iter().map(|t| store.field(f).df(t)).collect(),
    }
}

type Sig = Vec<(u32, u64, Vec<(usize, usize, Vec<(u32, u32)>)>)>;

fn sig(hits: &[FusedDoc]) -> Sig {
    hits.iter()
        .map(|h| (h.doc_id, h.score.to_bits(), h.term_offsets.clone()))
        .collect()
}

/// The floor palette the single-field property gate uses: unseeded,
/// unreachable, exact k-th best, exact weakest hit, just above it, and
/// mid-range.
fn floor_palette(oracle: &[FusedDoc], k: usize) -> Vec<f64> {
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
    floors
}

fn two_field_queries<'a>(
    body: &'a dyn Bm25Index,
    name: &'a dyn Bm25Index,
    body_q: (&'a [String], &CorpusStats, Bm25Params, f64),
    name_q: (&'a [String], &CorpusStats, Bm25Params, f64),
) -> [FieldQuery<'a>; 2] {
    [
        FieldQuery {
            index: body,
            terms: body_q.0,
            stats: body_q.1.clone(),
            params: body_q.2,
            weight: body_q.3,
        },
        FieldQuery {
            index: name,
            terms: name_q.0,
            stats: name_q.1.clone(),
            params: name_q.2,
            weight: name_q.3,
        },
    ]
}

fn check_fused_pruned_round(
    rng: &mut Lcg,
    round: u64,
    wide: bool,
    vocabs: (&[String], &[String]),
    store: &Bm25Store,
    reader: &Bm25Reader,
) {
    let k1s = [0.0, 1.2, 2.5];
    let bs = [0.0, 0.5, 1.0];
    let weights = [0.0, 0.35, 1.0, 1.75, 2.5];
    let (body_vocab, name_vocab) = vocabs;
    for _ in 0..4 {
        let max_terms = if wide { 6 } else { body_vocab.len().min(6) };
        let n_terms = 1 + rng.below(max_terms as u64) as usize;
        let mut body_terms: Vec<String> = body_vocab
            .iter()
            .filter(|_| rng.below(2) == 0)
            .take(n_terms)
            .cloned()
            .collect();
        if body_terms.is_empty() {
            body_terms.push(body_vocab[0].clone());
        }
        let mut name_terms: Vec<String> = name_vocab
            .iter()
            .filter(|_| rng.below(2) == 0)
            .take(4)
            .cloned()
            .collect();
        if name_terms.is_empty() {
            name_terms.push(name_vocab[0].clone());
        }
        let body_stats = field_stats(store, 0, &body_terms);
        let name_stats = field_stats(store, 1, &name_terms);
        let body_params = Bm25Params {
            k1: k1s[rng.below(3) as usize],
            b: bs[rng.below(3) as usize],
        };
        let name_params = Bm25Params {
            k1: k1s[rng.below(3) as usize],
            b: bs[rng.below(3) as usize],
        };
        let (w_body, w_name) = (
            weights[rng.below(5) as usize],
            weights[rng.below(5) as usize],
        );
        let k = 1 + rng.below(40) as usize;
        let s0 = store.field(0);
        let s1 = store.field(1);
        let r0 = reader.field(0);
        let r1 = reader.field(1);
        let store_fields = two_field_queries(
            &s0,
            &s1,
            (&body_terms, &body_stats, body_params, w_body),
            (&name_terms, &name_stats, name_params, w_name),
        );
        let reader_fields = two_field_queries(
            &r0,
            &r1,
            (&body_terms, &body_stats, body_params, w_body),
            (&name_terms, &name_stats, name_params, w_name),
        );
        let oracle = bm25::top_k_fused_exhaustive(&store_fields, k, None);
        for floor in floor_palette(&oracle, k) {
            let want =
                bm25::top_k_fused_exhaustive(&store_fields, bm25::TopK::new(k).floor(floor), None);
            let mut prune = PruneStats::default();
            let got = bm25::top_k_fused(
                &reader_fields,
                bm25::TopK::new(k).floor(floor).prune_stats(&mut prune),
            );
            assert_eq!(sig(&want), sig(&got), "round {round} k {k} floor {floor:e}");
        }
    }
}

/// The bitwise property gate: fused pruned == fused exhaustive (+floor filter).
#[test]
fn fused_pruned_matches_fused_exhaustive_property() {
    let dir = test_dir("prop");
    let mut rng = Lcg::new(0xF05EDB10);
    for round in 0..20 {
        let wide = round % 3 == 0;
        let n_body_vocab = if wide { 10 } else { 2 + rng.below(20) };
        let body_vocab: Vec<String> = (0..n_body_vocab).map(|i| format!("t{i}")).collect();
        let name_vocab: Vec<String> = ["t0".to_string(), "t1".to_string()]
            .into_iter()
            .chain((0..4).map(|i| format!("n{i}")))
            .collect();
        let n_docs = if wide { 300 } else { 40 + rng.below(400) };
        let corpus = random_two_field_corpus(&mut rng, n_docs, &body_vocab, &name_vocab, wide);
        let store = build_store(&corpus);
        let path = dir.join(format!("r{round}.bm25"));
        store.save(&path).unwrap();
        let reader = Bm25Reader::open(&path).unwrap();
        check_fused_pruned_round(
            &mut rng,
            round,
            wide,
            (&body_vocab, &name_vocab),
            &store,
            &reader,
        );
        let _ = std::fs::remove_file(&path);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// The degenerate identity: a single body field at weight 1.0 through
/// the fused pruned path is bit-identical to `top_k_pruned` — the door
/// through which the fused path can serve single-field queries.
#[test]
fn fused_pruned_single_field_degenerate_identity() {
    let dir = test_dir("degen");
    let mut rng = Lcg::new(0xDE6E0F05);
    let mut store = Bm25Store::new();
    for i in 0..500u32 {
        let mut terms: DocTerms = vec![(
            "court".to_string(),
            1 + (rng.below(4)) as u32,
            vec![(i, i + 4)],
        )];
        if rng.below(2) == 0 {
            terms.push((format!("t{}", rng.below(6)), 1, Vec::new()));
        }
        let length: u32 = terms.iter().map(|t| t.1).sum();
        store.add_document(i * 2, format!("doc {i}"), AnalyzedDoc::body(terms, length));
    }
    let path = dir.join("single.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();

    let terms = vec!["court".to_string(), "t3".to_string()];
    let stats = CorpusStats {
        doc_count: store.doc_count(),
        total_doc_length: store.total_doc_length(),
        dfs: terms.iter().map(|t| Bm25Index::df(&store, t)).collect(),
    };
    let params = Bm25Params::default();
    for k in [1usize, 7, 40] {
        let single = bm25::top_k(&reader, &terms, &stats, params, k);
        let floors = [
            f64::NEG_INFINITY,
            single.last().map_or(1e9, |h| h.score),
            single[single.len() / 2].score,
        ];
        for floor in floors {
            let want = bm25::filter_to_floor(single.clone(), floor);
            let got = bm25::top_k_fused(
                &[FieldQuery {
                    index: &reader,
                    terms: &terms,
                    stats: stats.clone(),
                    params,
                    weight: 1.0,
                }],
                bm25::TopK::new(k).floor(floor),
            );
            assert_eq!(want.len(), got.len(), "k {k}, floor {floor:e}");
            for (w, g) in want.iter().zip(&got) {
                assert_eq!(g.doc_id, w.doc_id, "k {k}, floor {floor:e}");
                assert_eq!(
                    g.score.to_bits(),
                    w.score.to_bits(),
                    "doc {}, k {k}, floor {floor:e}",
                    w.doc_id
                );
                let mapped: Vec<pipestream_search::bm25::FusedTermOffset> = w
                    .term_offsets
                    .iter()
                    .map(|(ti, o)| (0, *ti, o.clone()))
                    .collect();
                assert_eq!(g.term_offsets, mapped, "doc {}", w.doc_id);
            }
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

fn check_identical_doc_ties(dir: &std::path::Path) {
    let body_terms = vec!["court".to_string()];
    let name_terms = vec!["smith".to_string()];
    let name_params = Bm25Params { k1: 0.5, b: 0.0 };
    let mut store = Bm25Store::with_fields(&["body", "name"]);
    for i in 0..400u32 {
        let f0 = AnalyzedField {
            terms: vec![("court".to_string(), 2, vec![(0, 4), (10, 14)])],
            length: 3,
            ..Default::default()
        };
        let f1 = AnalyzedField {
            terms: vec![("smith".to_string(), 1, vec![(0, 5)])],
            length: 1,
            ..Default::default()
        };
        let doc = AnalyzedDoc {
            fields: vec![f0, f1],
            ..Default::default()
        };
        store.add_document(i, format!("doc {i}"), doc);
    }
    let path = dir.join("a.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let body_stats = field_stats(&store, 0, &body_terms);
    let name_stats = field_stats(&store, 1, &name_terms);
    let (s0, s1) = (store.field(0), store.field(1));
    let sq = two_field_queries(
        &s0,
        &s1,
        (&body_terms, &body_stats, Bm25Params::default(), 1.0),
        (&name_terms, &name_stats, name_params, 2.0),
    );
    let oracle = bm25::top_k_fused_exhaustive(&sq, 10, None);
    assert_eq!(oracle.len(), 10);
    let tie_score = oracle[0].score;
    assert!(oracle.iter().all(|h| h.score == tie_score));
    assert_eq!(
        oracle.iter().map(|h| h.doc_id).collect::<Vec<_>>(),
        (0..10).collect::<Vec<_>>()
    );
    let (r0, r1) = (reader.field(0), reader.field(1));
    let rq = two_field_queries(
        &r0,
        &r1,
        (&body_terms, &body_stats, Bm25Params::default(), 1.0),
        (&name_terms, &name_stats, name_params, 2.0),
    );
    for floor in [f64::NEG_INFINITY, tie_score] {
        let got = bm25::top_k_fused(&rq, bm25::TopK::new(10).floor(floor));
        assert_eq!(
            sig(&oracle),
            sig(&got),
            "identical-doc corpus, floor {floor:e}"
        );
    }
    let got = bm25::top_k_fused(&rq, bm25::TopK::new(10).floor(tie_score + 1e-9));
    assert!(
        got.is_empty(),
        "floor above every score must return nothing"
    );
}

fn check_cross_block_ties(dir: &std::path::Path) {
    let body_terms = vec!["court".to_string()];
    let name_terms = vec!["smith".to_string()];
    let name_params = Bm25Params { k1: 0.5, b: 0.0 };
    let mut store = Bm25Store::with_fields(&["body", "name"]);
    for i in 0..300u32 {
        let mut fields = vec![AnalyzedField {
            terms: vec![("court".to_string(), 1, vec![(0, 4)])],
            length: 3,
            ..Default::default()
        }];
        if i % 25 == 0 {
            fields.push(AnalyzedField {
                terms: vec![("smith".to_string(), 1, vec![(0, 5)])],
                length: 1,
                ..Default::default()
            });
        }
        store.add_document(
            i,
            format!("doc {i}"),
            AnalyzedDoc {
                fields,
                ..Default::default()
            },
        );
    }
    let path = dir.join("b.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let body_stats = field_stats(&store, 0, &body_terms);
    let name_stats = field_stats(&store, 1, &name_terms);
    let (s0, s1) = (store.field(0), store.field(1));
    let sq = two_field_queries(
        &s0,
        &s1,
        (&body_terms, &body_stats, Bm25Params::default(), 1.0),
        (&name_terms, &name_stats, name_params, 2.0),
    );
    let oracle = bm25::top_k_fused_exhaustive(&sq, 10, None);
    let want_ids: Vec<u32> = (0..10u32).map(|i| i * 25).collect();
    assert_eq!(
        oracle.iter().map(|h| h.doc_id).collect::<Vec<_>>(),
        want_ids
    );
    assert!(oracle.iter().all(|h| h.score == oracle[0].score));
    let (r0, r1) = (reader.field(0), reader.field(1));
    let rq = two_field_queries(
        &r0,
        &r1,
        (&body_terms, &body_stats, Bm25Params::default(), 1.0),
        (&name_terms, &name_stats, name_params, 2.0),
    );
    for floor in [f64::NEG_INFINITY, oracle[9].score] {
        let got = bm25::top_k_fused(&rq, bm25::TopK::new(10).floor(floor));
        assert_eq!(sig(&oracle), sig(&got), "cross-block tie, floor {floor:e}");
    }
}

/// Fused tie torture: identical fused scores inside and across blocks
/// keep exactly the docs the exhaustive path keeps — ties at the floor
/// survive, and heap displacement is strictly-greater so the smaller
/// doc id always wins.
#[test]
fn fused_ties_at_floor_and_kth_slot() {
    let dir = test_dir("ties");
    check_identical_doc_ties(&dir);
    check_cross_block_ties(&dir);
    let _ = std::fs::remove_dir_all(&dir);
}

fn build_skip_store(path: &std::path::Path) -> Bm25Store {
    let mut store = Bm25Store::with_fields(&["body", "name"]);
    for i in 0..3000u32 {
        let f0 = AnalyzedField {
            terms: vec![
                ("court".to_string(), 1 + (i / 128) % 5, vec![(0, 4)]),
                (format!("rare{}", i % 97), 1, vec![(0, 4)]),
            ],
            length: 100 + i % 50,
            ..Default::default()
        };
        let f1 = AnalyzedField {
            terms: vec![("smith".to_string(), 1 + (i / 128) % 3, vec![(0, 5)])],
            length: 10 + i % 5,
            ..Default::default()
        };
        store.add_document(
            i,
            format!("doc {i}"),
            AnalyzedDoc {
                fields: vec![f0, f1],
                ..Default::default()
            },
        );
    }
    store.save(path).unwrap();
    store
}

/// Fused skip accounting: a seeded two-field query skips most blocks;
/// seeding skips strictly more than not seeding; a rare single-block
/// pair skips nothing. Results stay exact throughout.
#[test]
fn fused_blocks_actually_skip() {
    let dir = test_dir("skip");
    let path = dir.join("s.bm25");
    let store = build_skip_store(&path);
    let reader = Bm25Reader::open(&path).unwrap();

    let body_terms = vec!["court".to_string()];
    let name_terms = vec!["smith".to_string()];
    let body_stats = field_stats(&store, 0, &body_terms);
    let name_stats = field_stats(&store, 1, &name_terms);
    let k = 10;
    let (s0, s1) = (store.field(0), store.field(1));
    let sq = two_field_queries(
        &s0,
        &s1,
        (&body_terms, &body_stats, Bm25Params::default(), 1.0),
        (&name_terms, &name_stats, Bm25Params::default(), 0.5),
    );
    let oracle = bm25::top_k_fused_exhaustive(&sq, k, None);
    let (r0, r1) = (reader.field(0), reader.field(1));
    let rq = two_field_queries(
        &r0,
        &r1,
        (&body_terms, &body_stats, Bm25Params::default(), 1.0),
        (&name_terms, &name_stats, Bm25Params::default(), 0.5),
    );

    let mut seeded = PruneStats::default();
    let got = bm25::top_k_fused(
        &rq,
        bm25::TopK::new(k)
            .floor(oracle[k - 1].score)
            .prune_stats(&mut seeded),
    );
    assert_eq!(sig(&oracle), sig(&got), "seeded result diverged");
    let seeded_frac = seeded.blocks_skipped as f64 / seeded.blocks_total as f64;
    assert!(
        seeded_frac > 0.5,
        "seeded fused query should skip most blocks, got {seeded_frac:.2}"
    );

    let mut unseeded = PruneStats::default();
    let got = bm25::top_k_fused(
        &rq,
        bm25::TopK::new(k)
            .floor(f64::NEG_INFINITY)
            .prune_stats(&mut unseeded),
    );
    assert_eq!(sig(&oracle), sig(&got));
    assert!(seeded.blocks_skipped > unseeded.blocks_skipped);

    let rare_terms = vec!["rare3".to_string()];
    let rare_stats = field_stats(&store, 0, &rare_terms);
    let mut rare = PruneStats::default();
    let got = bm25::top_k_fused(
        &[FieldQuery {
            index: &r0,
            terms: &rare_terms,
            stats: rare_stats.clone(),
            params: Bm25Params::default(),
            weight: 1.0,
        }],
        bm25::TopK::new(k).prune_stats(&mut rare),
    );
    assert_eq!(rare.blocks_skipped, 0, "rare unseeded query must not skip");
    let rare_oracle = bm25::top_k_fused_exhaustive(
        &[FieldQuery {
            index: &s0,
            terms: &rare_terms,
            stats: rare_stats,
            params: Bm25Params::default(),
            weight: 1.0,
        }],
        k,
        None,
    );
    assert_eq!(sig(&rare_oracle), sig(&got));
    let _ = std::fs::remove_dir_all(&dir);
}

/// The safety-net fallbacks return the floor-filtered exhaustive
/// result: a heap store (no impact surface) and a negative weight (the
/// bound algebra needs `w_f >= 0`) both stay total and exact.
#[test]
fn fused_fallbacks_match_exhaustive() {
    let dir = test_dir("fallback");
    let mut rng = Lcg::new(0xFA11BACC);
    let body_vocab: Vec<String> = (0..8).map(|i| format!("t{i}")).collect();
    let name_vocab: Vec<String> = (0..3).map(|i| format!("n{i}")).collect();
    let store = build_store(&random_two_field_corpus(
        &mut rng,
        120,
        &body_vocab,
        &name_vocab,
        false,
    ));
    let body_terms = vec!["t0".to_string(), "t3".to_string()];
    let name_terms = vec!["n1".to_string()];
    let body_stats = field_stats(&store, 0, &body_terms);
    let name_stats = field_stats(&store, 1, &name_terms);
    let k = 15;
    let (s0, s1) = (store.field(0), store.field(1));

    let pos_sq = two_field_queries(
        &s0,
        &s1,
        (&body_terms, &body_stats, Bm25Params::default(), 1.0),
        (&name_terms, &name_stats, Bm25Params::default(), 2.0),
    );
    let oracle = bm25::top_k_fused_exhaustive(&pos_sq, k, None);
    for floor in floor_palette(&oracle, k) {
        let want = bm25::filter_fused_to_floor(oracle.clone(), floor);
        let got = bm25::top_k_fused(&pos_sq, bm25::TopK::new(k).floor(floor));
        assert_eq!(
            sig(&want),
            sig(&got),
            "heap-store fallback, floor {floor:e}"
        );
    }

    let path = dir.join("f.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let (r0, r1) = (reader.field(0), reader.field(1));
    let neg_sq = two_field_queries(
        &s0,
        &s1,
        (&body_terms, &body_stats, Bm25Params::default(), 1.0),
        (&name_terms, &name_stats, Bm25Params::default(), -1.0),
    );
    let neg_rq = two_field_queries(
        &r0,
        &r1,
        (&body_terms, &body_stats, Bm25Params::default(), 1.0),
        (&name_terms, &name_stats, Bm25Params::default(), -1.0),
    );
    let oracle = bm25::top_k_fused_exhaustive(&neg_sq, k, None);
    for floor in [f64::NEG_INFINITY, 0.0] {
        let want = bm25::filter_fused_to_floor(oracle.clone(), floor);
        let got = bm25::top_k_fused(&neg_rq, bm25::TopK::new(k).floor(floor));
        assert_eq!(
            sig(&want),
            sig(&got),
            "negative-weight fallback, floor {floor:e}"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}
