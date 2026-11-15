use std::path::PathBuf;

use super::codec::pareto_frontier;
use super::*;

// --- Block-max postings & frontier tests -------------------------------

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

pub(super) fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("tvbm25_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A random corpus: gappy ids, a small vocabulary so terms repeat
/// across docs, mixed tf, some SCORING_ONLY postings (no offsets).
fn random_corpus(rng: &mut Lcg, n_docs: u64, vocab: &[String]) -> Vec<(u32, String, AnalyzedDoc)> {
    let mut docs = Vec::new();
    let mut id = 0u32;
    for _ in 0..n_docs {
        id += 1 + rng.below(3) as u32;
        let mut terms: DocTerms = Vec::new();
        let mut length = 0;
        for _ in 0..1 + rng.below(6) {
            let term = vocab[rng.below(vocab.len() as u64) as usize].clone();
            if terms.iter().any(|(t, _, _)| *t == term) {
                continue;
            }
            let tf = 1 + rng.below(4) as u32;
            let offsets: Vec<(u32, u32)> = if rng.below(3) == 0 {
                Vec::new()
            } else {
                (0..tf)
                    .map(|o| {
                        let start = o * 10 + rng.below(5) as u32;
                        (start, start + 1 + rng.below(4) as u32)
                    })
                    .collect()
            };
            length += tf;
            terms.push((term, tf, offsets));
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

fn vocab(rng: &mut Lcg, n: u64) -> Vec<String> {
    (0..n.max(1))
        .map(|i| format!("t{}", i + rng.below(1)))
        .collect()
}

fn assert_query_parity(store: &Bm25Store, reader: &Bm25Reader, rng: &mut Lcg, voc: &[String]) {
    let k1s = [0.0, 0.6, 1.2, 2.0];
    let bs = [0.0, 0.4, 0.75, 1.0];
    let sig = |hits: &[crate::bm25::ScoredDoc]| {
        hits.iter()
            .map(|h| (h.doc_id, h.score.to_bits(), h.term_offsets.clone()))
            .collect::<Vec<_>>()
    };
    for _ in 0..6 {
        let n_terms = 1 + rng.below(4) as usize;
        let mut terms: Vec<String> = voc
            .iter()
            .filter(|_| rng.below(2) == 0)
            .take(n_terms.max(1))
            .cloned()
            .collect();
        if terms.is_empty() {
            terms.push(voc[0].clone());
        }
        let stats = crate::bm25::CorpusStats {
            doc_count: store.doc_count(),
            total_doc_length: store.total_doc_length(),
            dfs: terms.iter().map(|t| store.df(t)).collect(),
        };
        let params = crate::bm25::Bm25Params {
            k1: k1s[rng.below(k1s.len() as u64) as usize],
            b: bs[rng.below(bs.len() as u64) as usize],
        };
        let k = 1 + rng.below(30) as usize;
        let heap = sig(&crate::bm25::top_k(store, &terms, &stats, params, k));
        let oracle = sig(&crate::bm25::top_k_exhaustive(
            store, &terms, &stats, params, k,
        ));
        let disk = sig(&crate::bm25::top_k(reader, &terms, &stats, params, k));
        let pruned = sig(&crate::bm25::top_k(
            reader,
            &terms,
            &stats,
            params,
            crate::bm25::TopK::new(k).floor(0.0),
        ));
        assert_eq!(heap, oracle, "top_k diverged from exhaustive oracle");
        assert_eq!(heap, disk, "reader top_k diverged from heap");
        assert_eq!(heap, pruned, "reader top_k_pruned diverged from heap");
    }
}

/// Bitwise cross-engine property test: the same random corpus indexed in
/// `Bm25Store` and streamed through `SpillBuilder` emits identical `.bm25`
/// bytes and answers `top_k` / `top_k_pruned` with identical
/// `(doc_id, score.to_bits(), term_offsets)` sequences across random `k`,
/// term counts, and `k1`/`b`.
#[test]
fn store_spill_reader_top_k_bitwise_identical() {
    let dir = test_dir("store_spill_parity");
    let mut rng = Lcg::new(0xB10C);
    for round in 0..24 {
        let n_voc = 3 + rng.below(20);
        let voc = vocab(&mut rng, n_voc);
        let n_docs = 30 + rng.below(170);
        let corpus = random_corpus(&mut rng, n_docs, &voc);
        let store = build_store(&corpus);
        let mut spill = SpillBuilder::create(&dir.join(format!("work_{round}")))
            .unwrap()
            .with_buffer_bytes(256);
        for (id, text, doc) in &corpus {
            spill
                .add_document_with_lineage(*id, text.clone(), doc.clone(), None)
                .unwrap();
        }
        let store_path = dir.join(format!("r{round}.store.bm25"));
        let spill_path = dir.join(format!("r{round}.spill.bm25"));
        store.save(&store_path).unwrap();
        spill.finish(&spill_path).unwrap();
        assert_eq!(
            std::fs::read(&store_path).unwrap(),
            std::fs::read(&spill_path).unwrap(),
            "round {round}: store and spill bytes diverged"
        );
        let reader = Bm25Reader::open(&spill_path).unwrap();
        assert_query_parity(&store, &reader, &mut rng, &voc);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn pareto_frontier_prunes_dominated() {
    let f = pareto_frontier(&[(5, 10), (3, 10), (5, 12), (1, 1)]);
    assert_eq!(f, vec![(1, 1), (5, 10)]);
    // Empty block, single pair, duplicates.
    assert!(pareto_frontier(&[]).is_empty());
    assert_eq!(pareto_frontier(&[(7, 7), (7, 7)]), vec![(7, 7)]);
}

#[test]
fn pareto_frontier_collapses_never_drops() {
    // A 20-point staircase is entirely on the frontier and forces
    // collapsing down to the cap.
    let pairs: Vec<(u32, u32)> = (1..=20u32).map(|i| (i, i * 10)).collect();
    let f = pareto_frontier(&pairs);
    assert!(f.len() <= MAX_FRONTIER, "frontier not capped: {f:?}");
    // Bound property: every original point is weakly dominated by
    // some stored pair (tf' >= tf, dl' <= dl) — collapsing only ever
    // raises the bound, it never drops coverage.
    for &(tf, dl) in &pairs {
        assert!(
            f.iter().any(|&(t, d)| t >= tf && d <= dl),
            "({tf},{dl}) not bounded by collapsed frontier {f:?}"
        );
    }
    // The staircase stays strictly increasing in both coordinates.
    for w in f.windows(2) {
        assert!(w[1].0 > w[0].0 && w[1].1 > w[0].1, "not a staircase: {f:?}");
    }
}

fn decode_l0_records(bytes: &[u8], skip_off: u64, df: u32) -> Vec<(u32, Vec<(u32, u32)>)> {
    let n_blocks = (df as usize).div_ceil(BLOCK);
    let mut cur = skip_off as usize + 8;
    let mut out = Vec::with_capacity(n_blocks);
    for _ in 0..n_blocks {
        let last_doc_id = u32::from_le_bytes(bytes[cur..cur + 4].try_into().unwrap());
        let n_pairs = bytes[cur + 4] as usize;
        cur += 5;
        let mut pairs = Vec::with_capacity(n_pairs);
        for _ in 0..n_pairs {
            let tf = u32::from_le_bytes(bytes[cur..cur + 4].try_into().unwrap());
            let dl = u32::from_le_bytes(bytes[cur + 4..cur + 8].try_into().unwrap());
            pairs.push((tf, dl));
            cur += 8;
        }
        out.push((last_doc_id, pairs));
    }
    out
}

fn check_frontier_terms(store: &Bm25Store, reader: &Bm25Reader, terms: &[String]) -> bool {
    use crate::bm25::{tf_norm, Bm25Params};
    let params_list = [
        Bm25Params { k1: 0.0, b: 0.0 },
        Bm25Params { k1: 1.2, b: 0.75 },
        Bm25Params { k1: 2.0, b: 1.0 },
    ];
    let avgdl = store.total_doc_length() as f64 / store.doc_count() as f64;
    let mut saw_collapsed = false;
    for term in terms {
        let Some((_, skip_off, _, df)) = reader.directory_lookup(&reader.fields[0], term) else {
            continue;
        };
        let records = decode_l0_records(&reader.map, skip_off, df);
        let postings = store.postings(term).unwrap();
        assert_eq!(records.len(), postings.len().div_ceil(BLOCK));
        for (block_i, (_, pairs)) in records.iter().enumerate() {
            assert!(!pairs.is_empty() && pairs.len() <= MAX_FRONTIER);
            saw_collapsed |= pairs.len() == MAX_FRONTIER;
            let block = &postings[block_i * BLOCK..((block_i + 1) * BLOCK).min(postings.len())];
            for p in block {
                let dl = store.doc_length(p.doc_id);
                assert!(
                    pairs.iter().any(|&(t, d)| t >= p.tf && d <= dl),
                    "{term} block {block_i}: posting ({},{dl}) not bounded by {pairs:?}",
                    p.tf
                );
            }
            for params in params_list {
                for a in [avgdl, 1.0, 50.0, 1000.0] {
                    let true_max = block
                        .iter()
                        .map(|p| tf_norm(params, p.tf, store.doc_length(p.doc_id), a))
                        .fold(f64::NEG_INFINITY, f64::max);
                    let stored_max = pairs
                        .iter()
                        .map(|&(t, d)| tf_norm(params, t, d, a))
                        .fold(f64::NEG_INFINITY, f64::max);
                    assert!(
                        stored_max >= true_max,
                        "{term} block {block_i} underbounds at avgdl {a}, {params:?}: \
                         stored {stored_max} < true {true_max}"
                    );
                }
            }
        }
    }
    saw_collapsed
}

/// Frontier safety gate: for every level-0 block of every term, the
/// stored (possibly collapsed) frontier's max over tf_norm is >= the
/// true block max, at several avgdl values and k1/b combos.
#[test]
fn frontier_never_underbounds() {
    let dir = test_dir("frontier");
    let mut rng = Lcg::new(0xFE07);
    let voc = vocab(&mut rng, 4);
    let corpus = random_corpus(&mut rng, 900, &voc);
    let store = build_store(&corpus);
    let path = dir.join("random.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    check_frontier_terms(&store, &reader, &voc);

    let mut stair = Bm25Store::new();
    for i in 0..300u32 {
        stair.add_document(
            i,
            format!("doc {i}"),
            AnalyzedDoc::body(
                vec![("stair".to_string(), 1 + i % 150, vec![(i, i + 2)])],
                100 + i,
            ),
        );
    }
    let path = dir.join("stair.bm25");
    stair.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    assert!(
        check_frontier_terms(&stair, &reader, &["stair".to_string()]),
        "collapse path never exercised"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Laziness proof: corrupt the occurrence run of a COPY of a `.bm25` file;
/// the for_each_doc_tf-based scored path (top_k) must return identical
/// scores/doc ids, while posting_offsets returns the garbage — the
/// scored path never touched the occurrence bytes.
#[test]
fn scored_path_never_reads_occurrence_run() {
    let dir = test_dir("occ_lazy");
    let path = dir.join("shard.bm25");
    let mut store = Bm25Store::new();
    for i in 0..50u32 {
        let tf = 1 + i % 3;
        store.add_document(
            i,
            format!("doc {i}"),
            AnalyzedDoc::body(
                vec![(
                    "court".to_string(),
                    tf,
                    (0..tf).map(|o| (o * 10 + i, o * 10 + i + 4)).collect(),
                )],
                tf,
            ),
        );
    }
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let (_, skip_off, occ_off, _) = reader.directory_lookup(&reader.fields[0], "court").unwrap();

    let corrupted = dir.join("corrupted.bm25");
    let mut bytes = std::fs::read(&path).unwrap();
    for b in &mut bytes[occ_off as usize..skip_off as usize] {
        *b ^= 0xFF;
    }
    std::fs::write(&corrupted, &bytes).unwrap();
    drop(reader);
    let good = Bm25Reader::open(&path).unwrap();
    let bad = Bm25Reader::open(&corrupted).unwrap();

    let terms = vec!["court".to_string()];
    let stats = crate::bm25::CorpusStats {
        doc_count: store.doc_count(),
        total_doc_length: store.total_doc_length(),
        dfs: vec![store.df("court")],
    };
    let params = crate::bm25::Bm25Params::default();
    let sig = |hits: &[crate::bm25::ScoredDoc]| -> Vec<(u32, u64)> {
        hits.iter().map(|h| (h.doc_id, h.score.to_bits())).collect()
    };
    let a = crate::bm25::top_k(&good, &terms, &stats, params, 10);
    let b = crate::bm25::top_k(&bad, &terms, &stats, params, 10);
    assert_eq!(sig(&a), sig(&b), "scored path touched occurrence bytes");
    assert_ne!(
        a.iter().map(|h| &h.term_offsets).collect::<Vec<_>>(),
        b.iter().map(|h| &h.term_offsets).collect::<Vec<_>>(),
        "corruption did not reach the occurrence run"
    );
    let good_offs = good.posting_offsets("court", 7);
    let bad_offs = bad.posting_offsets("court", 7);
    assert_eq!(good_offs.len(), bad_offs.len());
    assert_ne!(good_offs, bad_offs);

    let _ = std::fs::remove_dir_all(&dir);
}

// --- v6 (TVBM2506) tests -------------------------------------------
