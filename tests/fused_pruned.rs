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
            terms: body,
            length: body_len,
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
                terms: name,
                length: name_len,
            });
        }
        docs.push((
            id,
            format!("doc {id}"),
            AnalyzedDoc {
                fields,
                quality: None,
                geography: None,
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

/// The bitwise property gate: fused pruned == fused exhaustive
/// (+floor filter) over random corpora, weights, params, and floors.
#[test]
fn fused_pruned_matches_fused_exhaustive_property() {
    let dir = test_dir("prop");
    let mut rng = Lcg::new(0xF05EDB10);
    let k1s = [0.0, 1.2, 2.5];
    let bs = [0.0, 0.5, 1.0];
    let weights = [0.0, 0.35, 1.0, 1.75, 2.5];
    for round in 0..20 {
        // Every third round is "wide": mid-df terms and wide queries,
        // so competitive windows form essential/non-essential
        // partitions rather than resolving by skips alone.
        let wide = round % 3 == 0;
        let n_body_vocab = if wide { 10 } else { 2 + rng.below(20) };
        let body_vocab: Vec<String> = (0..n_body_vocab).map(|i| format!("t{i}")).collect();
        // The name vocab overlaps the body vocab ("t0", "t1") so shared
        // terms score in both fields with different postings.
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
        for _ in 0..4 {
            let max_terms = if wide { 6 } else { body_vocab.len().min(6) };
            let n_terms = 1 + rng.below(max_terms as u64) as usize;
            let body_terms: Vec<String> = body_vocab
                .iter()
                .filter(|_| rng.below(2) == 0)
                .take(n_terms)
                .cloned()
                .collect();
            let body_terms = if body_terms.is_empty() {
                vec![body_vocab[0].clone()]
            } else {
                body_terms
            };
            let name_terms: Vec<String> = name_vocab
                .iter()
                .filter(|_| rng.below(2) == 0)
                .take(4)
                .cloned()
                .collect();
            let name_terms = if name_terms.is_empty() {
                vec![name_vocab[0].clone()]
            } else {
                name_terms
            };
            let body_stats = field_stats(&store, 0, &body_terms);
            let name_stats = field_stats(&store, 1, &name_terms);
            let body_params = Bm25Params {
                k1: k1s[rng.below(3) as usize],
                b: bs[rng.below(3) as usize],
            };
            let name_params = Bm25Params {
                k1: k1s[rng.below(3) as usize],
                b: bs[rng.below(3) as usize],
            };
            let w_body = weights[rng.below(5) as usize];
            let w_name = weights[rng.below(5) as usize];
            let k = 1 + rng.below(40) as usize;
            let oracle = bm25::top_k_fused_exhaustive(
                &[
                    FieldQuery {
                        index: &store.field(0),
                        terms: &body_terms,
                        stats: body_stats.clone(),
                        params: body_params,
                        weight: w_body,
                    },
                    FieldQuery {
                        index: &store.field(1),
                        terms: &name_terms,
                        stats: name_stats.clone(),
                        params: name_params,
                        weight: w_name,
                    },
                ],
                k,
            );
            for floor in floor_palette(&oracle, k) {
                let want = bm25::filter_fused_to_floor(oracle.clone(), floor);
                let mut prune = PruneStats::default();
                let got = bm25::top_k_fused_pruned_stats(
                    &[
                        FieldQuery {
                            index: &reader.field(0),
                            terms: &body_terms,
                            stats: body_stats.clone(),
                            params: body_params,
                            weight: w_body,
                        },
                        FieldQuery {
                            index: &reader.field(1),
                            terms: &name_terms,
                            stats: name_stats.clone(),
                            params: name_params,
                            weight: w_name,
                        },
                    ],
                    k,
                    floor,
                    &mut prune,
                );
                assert_eq!(
                    sig(&want),
                    sig(&got),
                    "round {round}, body {body_terms:?} w {w_body} {body_params:?}, \
                     name {name_terms:?} w {w_name} {name_params:?}, k {k}, floor {floor:e}"
                );
            }
        }
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
        let single = bm25::top_k_pruned(&reader, &terms, &stats, params, k, f64::NEG_INFINITY);
        let floors = [
            f64::NEG_INFINITY,
            single.last().map_or(1e9, |h| h.score),
            single[single.len() / 2].score,
        ];
        for floor in floors {
            let want = bm25::filter_to_floor(single.clone(), floor);
            let got = bm25::top_k_fused_pruned(
                &[FieldQuery {
                    index: &reader,
                    terms: &terms,
                    stats: stats.clone(),
                    params,
                    weight: 1.0,
                }],
                k,
                floor,
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

/// Fused tie torture: identical fused scores inside and across blocks
/// keep exactly the docs the exhaustive path keeps — ties at the floor
/// survive, and heap displacement is strictly-greater so the smaller
/// doc id always wins.
#[test]
fn fused_ties_at_floor_and_kth_slot() {
    let dir = test_dir("ties");
    let body_terms = vec!["court".to_string()];
    let name_terms = vec!["smith".to_string()];
    let name_params = Bm25Params { k1: 0.5, b: 0.0 };

    // Corpus A: 400 docs identical in BOTH fields (every fused score
    // equal, blocks packed with ties).
    let mut store = Bm25Store::with_fields(&["body", "name"]);
    for i in 0..400u32 {
        store.add_document(
            i,
            format!("doc {i}"),
            AnalyzedDoc {
                fields: vec![
                    AnalyzedField {
                        terms: vec![("court".to_string(), 2, vec![(0, 4), (10, 14)])],
                        length: 3,
                    },
                    AnalyzedField {
                        terms: vec![("smith".to_string(), 1, vec![(0, 5)])],
                        length: 1,
                    },
                ],
                quality: None,
                geography: None,
            },
        );
    }
    let path = dir.join("a.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let body_stats = field_stats(&store, 0, &body_terms);
    let name_stats = field_stats(&store, 1, &name_terms);
    let oracle = bm25::top_k_fused_exhaustive(
        &[
            FieldQuery {
                index: &store.field(0),
                terms: &body_terms,
                stats: body_stats.clone(),
                params: Bm25Params::default(),
                weight: 1.0,
            },
            FieldQuery {
                index: &store.field(1),
                terms: &name_terms,
                stats: name_stats.clone(),
                params: name_params,
                weight: 2.0,
            },
        ],
        10,
    );
    assert_eq!(oracle.len(), 10);
    let tie_score = oracle[0].score;
    assert!(oracle.iter().all(|h| h.score == tie_score));
    let want_ids: Vec<u32> = (0..10).collect();
    assert_eq!(
        oracle.iter().map(|h| h.doc_id).collect::<Vec<_>>(),
        want_ids
    );
    let pruned = |floor: f64| {
        bm25::top_k_fused_pruned(
            &[
                FieldQuery {
                    index: &reader.field(0),
                    terms: &body_terms,
                    stats: body_stats.clone(),
                    params: Bm25Params::default(),
                    weight: 1.0,
                },
                FieldQuery {
                    index: &reader.field(1),
                    terms: &name_terms,
                    stats: name_stats.clone(),
                    params: name_params,
                    weight: 2.0,
                },
            ],
            10,
            floor,
        )
    };
    for floor in [f64::NEG_INFINITY, tie_score] {
        let got = pruned(floor);
        assert_eq!(
            sig(&oracle),
            sig(&got),
            "identical-doc corpus, floor {floor:e}"
        );
    }
    // A hair above the tie: nothing survives.
    let got = pruned(tie_score + 1e-9);
    assert!(
        got.is_empty(),
        "floor above every score must return nothing"
    );

    // Corpus B: the k-th slot ties ACROSS blocks — 12 high docs (name
    // field present) spread over 300 body-only docs; k=10 keeps the
    // ten smallest high ids at identical fused scores.
    let mut store = Bm25Store::with_fields(&["body", "name"]);
    for i in 0..300u32 {
        let mut fields = vec![AnalyzedField {
            terms: vec![("court".to_string(), 1, vec![(0, 4)])],
            length: 3,
        }];
        if i % 25 == 0 {
            fields.push(AnalyzedField {
                terms: vec![("smith".to_string(), 1, vec![(0, 5)])],
                length: 1,
            });
        }
        store.add_document(
            i,
            format!("doc {i}"),
            AnalyzedDoc {
                fields,
                quality: None,
                geography: None,
            },
        );
    }
    let path = dir.join("b.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let body_stats = field_stats(&store, 0, &body_terms);
    let name_stats = field_stats(&store, 1, &name_terms);
    let oracle = bm25::top_k_fused_exhaustive(
        &[
            FieldQuery {
                index: &store.field(0),
                terms: &body_terms,
                stats: body_stats.clone(),
                params: Bm25Params::default(),
                weight: 1.0,
            },
            FieldQuery {
                index: &store.field(1),
                terms: &name_terms,
                stats: name_stats.clone(),
                params: name_params,
                weight: 2.0,
            },
        ],
        10,
    );
    let want_ids: Vec<u32> = (0..10u32).map(|i| i * 25).collect();
    assert_eq!(
        oracle.iter().map(|h| h.doc_id).collect::<Vec<_>>(),
        want_ids,
        "oracle sanity: ten smallest name-carrying ids"
    );
    assert!(oracle.iter().all(|h| h.score == oracle[0].score));
    for floor in [f64::NEG_INFINITY, oracle[9].score] {
        let got = bm25::top_k_fused_pruned(
            &[
                FieldQuery {
                    index: &reader.field(0),
                    terms: &body_terms,
                    stats: body_stats.clone(),
                    params: Bm25Params::default(),
                    weight: 1.0,
                },
                FieldQuery {
                    index: &reader.field(1),
                    terms: &name_terms,
                    stats: name_stats.clone(),
                    params: name_params,
                    weight: 2.0,
                },
            ],
            10,
            floor,
        );
        assert_eq!(sig(&oracle), sig(&got), "cross-block tie, floor {floor:e}");
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Fused skip accounting: a seeded two-field query skips most blocks;
/// seeding skips strictly more than not seeding; a rare single-block
/// pair skips nothing. Results stay exact throughout.
#[test]
fn fused_blocks_actually_skip() {
    let dir = test_dir("skip");
    let mut store = Bm25Store::with_fields(&["body", "name"]);
    let n = 3000u32;
    for i in 0..n {
        // Block-correlated tf in both fields (whole 128-posting blocks
        // share one tf) so block maxes genuinely differ.
        let fields = vec![
            AnalyzedField {
                terms: vec![
                    ("court".to_string(), 1 + (i / 128) % 5, vec![(0, 4)]),
                    (format!("rare{}", i % 97), 1, vec![(0, 4)]),
                ],
                length: 100 + i % 50,
            },
            AnalyzedField {
                terms: vec![("smith".to_string(), 1 + (i / 128) % 3, vec![(0, 5)])],
                length: 10 + i % 5,
            },
        ];
        store.add_document(
            i,
            format!("doc {i}"),
            AnalyzedDoc {
                fields,
                quality: None,
                geography: None,
            },
        );
    }
    let path = dir.join("s.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();

    let body_terms = vec!["court".to_string()];
    let name_terms = vec!["smith".to_string()];
    let body_stats = field_stats(&store, 0, &body_terms);
    let name_stats = field_stats(&store, 1, &name_terms);
    let k = 10;
    let oracle = bm25::top_k_fused_exhaustive(
        &[
            FieldQuery {
                index: &store.field(0),
                terms: &body_terms,
                stats: body_stats.clone(),
                params: Bm25Params::default(),
                weight: 1.0,
            },
            FieldQuery {
                index: &store.field(1),
                terms: &name_terms,
                stats: name_stats.clone(),
                params: Bm25Params::default(),
                weight: 0.5,
            },
        ],
        k,
    );
    let pruned = |floor: f64, prune: &mut PruneStats| {
        bm25::top_k_fused_pruned_stats(
            &[
                FieldQuery {
                    index: &reader.field(0),
                    terms: &body_terms,
                    stats: body_stats.clone(),
                    params: Bm25Params::default(),
                    weight: 1.0,
                },
                FieldQuery {
                    index: &reader.field(1),
                    terms: &name_terms,
                    stats: name_stats.clone(),
                    params: Bm25Params::default(),
                    weight: 0.5,
                },
            ],
            k,
            floor,
            prune,
        )
    };

    // Seeded with the true k-th best (the realistic coordinator case):
    // most blocks cannot reach the floor.
    let seed = oracle[k - 1].score;
    let mut seeded = PruneStats::default();
    let got = pruned(seed, &mut seeded);
    assert_eq!(sig(&oracle), sig(&got), "seeded result diverged");
    let seeded_frac = seeded.blocks_skipped as f64 / seeded.blocks_total as f64;
    assert!(
        seeded_frac > 0.5,
        "seeded fused query should skip most blocks, got {seeded_frac:.2} ({}/{})",
        seeded.blocks_skipped,
        seeded.blocks_total
    );

    // Same query unseeded: identical result, strictly fewer skips.
    let mut unseeded = PruneStats::default();
    let got = pruned(f64::NEG_INFINITY, &mut unseeded);
    assert_eq!(sig(&oracle), sig(&got));
    assert!(
        seeded.blocks_skipped > unseeded.blocks_skipped,
        "seeding must skip strictly more blocks ({} vs {})",
        seeded.blocks_skipped,
        unseeded.blocks_skipped
    );

    // Rare body pair over a single block, unseeded: nothing to skip.
    let rare_terms = vec!["rare3".to_string()];
    let rare_stats = field_stats(&store, 0, &rare_terms);
    let mut rare = PruneStats::default();
    let got = bm25::top_k_fused_pruned_stats(
        &[FieldQuery {
            index: &reader.field(0),
            terms: &rare_terms,
            stats: rare_stats.clone(),
            params: Bm25Params::default(),
            weight: 1.0,
        }],
        k,
        f64::NEG_INFINITY,
        &mut rare,
    );
    assert_eq!(rare.blocks_skipped, 0, "rare unseeded query must not skip");
    let rare_oracle = bm25::top_k_fused_exhaustive(
        &[FieldQuery {
            index: &store.field(0),
            terms: &rare_terms,
            stats: rare_stats,
            params: Bm25Params::default(),
            weight: 1.0,
        }],
        k,
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
    let corpus = random_two_field_corpus(&mut rng, 120, &body_vocab, &name_vocab, false);
    let store = build_store(&corpus);
    let body_terms = vec!["t0".to_string(), "t3".to_string()];
    let name_terms = vec!["n1".to_string()];
    let body_stats = field_stats(&store, 0, &body_terms);
    let name_stats = field_stats(&store, 1, &name_terms);
    let k = 15;
    let exhaustive = |w_name: f64| {
        bm25::top_k_fused_exhaustive(
            &[
                FieldQuery {
                    index: &store.field(0),
                    terms: &body_terms,
                    stats: body_stats.clone(),
                    params: Bm25Params::default(),
                    weight: 1.0,
                },
                FieldQuery {
                    index: &store.field(1),
                    terms: &name_terms,
                    stats: name_stats.clone(),
                    params: Bm25Params::default(),
                    weight: w_name,
                },
            ],
            k,
        )
    };

    // Heap store: no impacts anywhere, the whole query falls back.
    let oracle = exhaustive(2.0);
    for floor in floor_palette(&oracle, k) {
        let want = bm25::filter_fused_to_floor(oracle.clone(), floor);
        let got = bm25::top_k_fused_pruned(
            &[
                FieldQuery {
                    index: &store.field(0),
                    terms: &body_terms,
                    stats: body_stats.clone(),
                    params: Bm25Params::default(),
                    weight: 1.0,
                },
                FieldQuery {
                    index: &store.field(1),
                    terms: &name_terms,
                    stats: name_stats.clone(),
                    params: Bm25Params::default(),
                    weight: 2.0,
                },
            ],
            k,
            floor,
        );
        assert_eq!(
            sig(&want),
            sig(&got),
            "heap-store fallback, floor {floor:e}"
        );
    }

    // Reader with impacts but a negative weight: same fallback.
    let path = dir.join("f.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let oracle = exhaustive(-1.0);
    for floor in [f64::NEG_INFINITY, 0.0] {
        let want = bm25::filter_fused_to_floor(oracle.clone(), floor);
        let got = bm25::top_k_fused_pruned(
            &[
                FieldQuery {
                    index: &reader.field(0),
                    terms: &body_terms,
                    stats: body_stats.clone(),
                    params: Bm25Params::default(),
                    weight: 1.0,
                },
                FieldQuery {
                    index: &reader.field(1),
                    terms: &name_terms,
                    stats: name_stats.clone(),
                    params: Bm25Params::default(),
                    weight: -1.0,
                },
            ],
            k,
            floor,
        );
        assert_eq!(
            sig(&want),
            sig(&got),
            "negative-weight fallback, floor {floor:e}"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Contracts 2 + 3 together on the pruned path: fused pruned scoring
/// over two shard READERS with per-field merged global stats, merged
/// by (score desc, global id asc), equals monolithic fused pruned
/// scoring of the union corpus bit for bit — unseeded, and seeded at
/// the global k-th best (the coordinator round trip).
#[test]
fn fused_pruned_distributed_equals_monolithic() {
    let dir = test_dir("dist");
    // The document for global id `g`, identical however sharded.
    fn doc_for(g: u32) -> AnalyzedDoc {
        let body = vec![
            ("rust".to_string(), 1 + g % 3, vec![(g, g + 2)]),
            (format!("t{}", g % 4), 1, vec![(g + 3, g + 5)]),
        ];
        let body_len: u32 = body.iter().map(|t| t.1).sum();
        let mut fields = vec![AnalyzedField {
            terms: body,
            length: body_len,
        }];
        if g % 3 != 1 {
            fields.push(AnalyzedField {
                terms: vec![
                    ("smith".to_string(), 1, vec![(0, 5)]),
                    (format!("n{}", g % 2), 1, vec![]),
                ],
                length: 2,
            });
        }
        AnalyzedDoc {
            fields,
            quality: None,
            geography: None,
        }
    }
    fn build(range: std::ops::Range<u32>, offset: u32) -> Bm25Store {
        let mut store = Bm25Store::with_fields(&["body", "name"]);
        for g in range {
            store.add_document(g - offset, format!("doc {g}"), doc_for(g));
        }
        store
    }
    let monolith = build(0..700, 0);
    let shards = [(build(0..311, 0), 0u32), (build(311..700, 311), 311u32)];

    let body_terms = vec!["rust".to_string(), "t2".to_string()];
    let name_terms = vec!["smith".to_string(), "n1".to_string()];
    let body_params = Bm25Params::default();
    let name_params = Bm25Params { k1: 0.8, b: 0.3 };
    let weights = [1.0, 1.7];

    // Per-field global stats from per-shard shares (the TermStats
    // flow).
    let global = |f: usize, terms: &[String]| {
        let shares: Vec<(u64, u64, Vec<u32>)> = shards
            .iter()
            .map(|(s, _)| {
                (
                    s.doc_count(),
                    s.field(f).total_doc_length(),
                    terms.iter().map(|t| s.field(f).df(t)).collect(),
                )
            })
            .collect();
        bm25::merge_stats(&shares)
    };
    let body_stats = global(0, &body_terms);
    let name_stats = global(1, &name_terms);

    let readers: Vec<Bm25Reader> = std::iter::once(&monolith)
        .chain(shards.iter().map(|(s, _)| s))
        .enumerate()
        .map(|(i, s)| {
            let path = dir.join(format!("{i}.bm25"));
            s.save(&path).unwrap();
            Bm25Reader::open(&path).unwrap()
        })
        .collect();

    let k = 12;
    let run = |reader: &Bm25Reader, floor: f64| {
        bm25::top_k_fused_pruned(
            &[
                FieldQuery {
                    index: &reader.field(0),
                    terms: &body_terms,
                    stats: body_stats.clone(),
                    params: body_params,
                    weight: weights[0],
                },
                FieldQuery {
                    index: &reader.field(1),
                    terms: &name_terms,
                    stats: name_stats.clone(),
                    params: name_params,
                    weight: weights[1],
                },
            ],
            k,
            floor,
        )
    };
    let merge = |mut hits: Vec<FusedDoc>| {
        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.doc_id.cmp(&b.doc_id))
        });
        hits.truncate(k);
        hits
    };

    let want = run(&readers[0], f64::NEG_INFINITY);
    // Sanity: the pruned monolith equals the exhaustive monolith.
    let oracle = bm25::top_k_fused_exhaustive(
        &[
            FieldQuery {
                index: &monolith.field(0),
                terms: &body_terms,
                stats: body_stats.clone(),
                params: body_params,
                weight: weights[0],
            },
            FieldQuery {
                index: &monolith.field(1),
                terms: &name_terms,
                stats: name_stats.clone(),
                params: name_params,
                weight: weights[1],
            },
        ],
        k,
    );
    assert_eq!(sig(&oracle), sig(&want), "monolith pruned vs exhaustive");

    let distributed = |floor: f64| {
        let hits: Vec<FusedDoc> = readers[1..]
            .iter()
            .zip(shards.iter().map(|&(_, off)| off))
            .flat_map(|(r, off)| {
                run(r, floor).into_iter().map(move |mut d| {
                    d.doc_id += off;
                    d
                })
            })
            .collect();
        merge(hits)
    };
    // Unseeded round trip.
    let merged = distributed(f64::NEG_INFINITY);
    assert_eq!(sig(&want), sig(&merged), "unseeded distributed merge");
    // Seeded at the global k-th best: every boundary tie survives and
    // the merge reproduces the monolithic top-k exactly.
    let merged = distributed(want[k - 1].score);
    assert_eq!(sig(&want), sig(&merged), "seeded distributed merge");

    let _ = std::fs::remove_dir_all(&dir);
}

/// A term that lives on ANOTHER shard must not forfeit fused pruning here.
///
/// The single-field twin of this gate is
/// `blockmax.rs::a_term_absent_from_this_shard_does_not_disable_pruning`.
/// The fused scorer had the same defect and outlived the fix, because
/// `FieldQuery.stats.dfs` is the GLOBAL df the coordinator computed: a
/// rare term is non-zero there even on the shards that do not hold it,
/// those shards have no impact surface to open for it, and the scorer
/// read that as "this index lacks impacts" and dropped the WHOLE query
/// to the exhaustive scorer.
///
/// Measured on the live 86.6M-chunk fleet before the fix: a two-term
/// query whose second term existed on exactly one of eight shards took
/// 3557 ms through the fused route and 10.6 ms after, because the other
/// seven each walked every posting of the common term. The single-field
/// route answered the identical query in 6 ms throughout, which is what
/// made the gap visible at all.
#[test]
fn a_term_absent_from_this_shard_does_not_disable_fused_pruning() {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("fused_absent_term");
    std::fs::create_dir_all(&dir).unwrap();

    // This shard holds only "court" in body. "12b6" is elsewhere in the
    // cluster, so its GLOBAL df is non-zero while its local df is 0.
    let mut store = Bm25Store::with_fields(&["body", "name"]);
    for i in 0..2000u32 {
        let tf = if i % 97 == 0 { 5 } else { 1 };
        store.add_document(
            i,
            format!("doc {i}"),
            AnalyzedDoc {
                fields: vec![
                    AnalyzedField {
                        terms: vec![("court".to_string(), tf, vec![(0, 5)])],
                        length: 4,
                    },
                    AnalyzedField {
                        terms: vec![("smith".to_string(), 1, vec![(0, 5)])],
                        length: 1,
                    },
                ],
                quality: None,
                geography: None,
            },
        );
    }
    let path = dir.join("absent.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();

    let body_terms = vec!["court".to_string(), "12b6".to_string()];
    let name_terms = vec!["smith".to_string()];
    let params = Bm25Params::default();
    // Global dfs: the rare body term is present in the CLUSTER.
    let body_stats = CorpusStats {
        doc_count: store.doc_count(),
        total_doc_length: store.field(0).total_doc_length(),
        dfs: vec![2000, 10],
    };
    let name_stats = field_stats(&store, 1, &name_terms);

    fn queries<'a>(
        body: &'a dyn Bm25Index,
        name: &'a dyn Bm25Index,
        body_terms: &'a [String],
        name_terms: &'a [String],
        body_stats: &CorpusStats,
        name_stats: &CorpusStats,
        params: Bm25Params,
    ) -> Vec<FieldQuery<'a>> {
        vec![
            FieldQuery {
                index: body,
                terms: body_terms,
                stats: body_stats.clone(),
                params,
                weight: 1.0,
            },
            FieldQuery {
                index: name,
                terms: name_terms,
                stats: name_stats.clone(),
                params,
                weight: 2.0,
            },
        ]
    }
    macro_rules! q {
        ($body:expr, $name:expr) => {
            queries(
                &$body,
                &$name,
                &body_terms,
                &name_terms,
                &body_stats,
                &name_stats,
                params,
            )
        };
    }

    let oracle = bm25::top_k_fused_exhaustive(&q!(store.field(0), store.field(1)), 10);
    let mut prune = PruneStats::default();
    let got = bm25::top_k_fused_pruned_stats(
        &q!(reader.field(0), reader.field(1)),
        10,
        f64::NEG_INFINITY,
        &mut prune,
    );

    // Correctness first: skipping a locally-absent pair is only valid
    // because it contributes 0 to every document on this shard.
    assert_eq!(
        sig(&oracle),
        sig(&got),
        "skipping a locally-absent term must not change a single fused score"
    );

    // And the point of the fix: the pruned scorer was ENTERED. Only its
    // candidate loop touches `candidates_evaluated`; the exhaustive
    // fallback is handed no PruneStats at all, so a non-zero count is a
    // precise witness of entry. (`blocks_total` is NOT: the setup loop
    // has already counted the present term's blocks by the time the
    // absent one triggers the fallback.) Skip effectiveness is a
    // property of the fixture's score distribution and is gated
    // elsewhere.
    assert!(
        prune.candidates_evaluated > 0,
        "the fused query fell back to exhaustive despite every PRESENT term having impacts"
    );

    // A seeded floor must still hold the filtered-oracle contract.
    let seeded =
        bm25::top_k_fused_pruned(&q!(reader.field(0), reader.field(1)), 10, oracle[9].score);
    assert_eq!(
        sig(&bm25::filter_fused_to_floor(
            oracle.clone(),
            oracle[9].score
        )),
        sig(&seeded),
        "a seeded floor must not change the surviving fused hits"
    );

    // Control: dropping the absent term entirely must walk the same
    // blocks. If it does not, the absent term is still costing work.
    let present_only = vec!["court".to_string()];
    let mut solo = PruneStats::default();
    let _ = bm25::top_k_fused_pruned_stats(
        &[
            FieldQuery {
                index: &reader.field(0),
                terms: &present_only,
                stats: CorpusStats {
                    doc_count: store.doc_count(),
                    total_doc_length: store.field(0).total_doc_length(),
                    dfs: vec![2000],
                },
                params,
                weight: 1.0,
            },
            FieldQuery {
                index: &reader.field(1),
                terms: &name_terms,
                stats: name_stats.clone(),
                params,
                weight: 2.0,
            },
        ],
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
