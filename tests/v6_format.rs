//! Format v6 (TVBM2506) increment-1 query contract
//! (`docs/multi-field.md` build order step 1): a single-field v6 file
//! answers every query identically to the v5 file of the same corpus,
//! bit for bit, through every scorer — the plain scored walk, the
//! exhaustive oracle, and the block-max pruned path (whose impact
//! cursors run over the v6 file's rebased run offsets).

use std::path::PathBuf;

use pipestream_search::bm25::{self, Bm25Params, CorpusStats, ScoredDoc};
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
        .join(format!("v6fmt_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A random corpus big enough that common terms span multiple 128-
/// posting skip blocks: gappy ids, tf skew, mixed empty offsets, some
/// docs without lineage.
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
            let offsets: Vec<(u32, u32)> = if rng.below(2) == 0 {
                Vec::new()
            } else {
                (0..tf).map(|o| (o * 10, o * 10 + 4)).collect()
            };
            length += tf;
            terms.push((term.clone(), tf, offsets));
        }
        store.add_document(id, format!("doc {id}"), AnalyzedDoc::body(terms, length));
    }
    store
}

type Sig = Vec<(u32, u64, Vec<(usize, Vec<(u32, u32)>)>)>;

fn sig(hits: &[ScoredDoc]) -> Sig {
    hits.iter()
        .map(|h| (h.doc_id, h.score.to_bits(), h.term_offsets.clone()))
        .collect()
}

#[test]
fn v6_reader_answers_identically_to_v5() {
    let dir = test_dir("identity");
    let vocab: Vec<String> = (0..10).map(|i| format!("t{i}")).collect();
    let mut rng = Lcg::new(0x51F0_0D06);
    let store = build_store(&mut rng, 3000, &vocab);

    let v5_path = dir.join("shard.v5.bm25");
    let v6_path = dir.join("shard.v6.bm25");
    store.save_v5(&v5_path).unwrap();
    store.save(&v6_path).unwrap();
    let v5 = Bm25Reader::open(&v5_path).unwrap();
    let v6 = Bm25Reader::open(&v6_path).unwrap();

    // Common terms span several skip blocks, so the pruned path runs
    // the real cursor machinery, not a trivial single block.
    assert!(
        Bm25Index::df(&store, "t0") > 256,
        "corpus too small to be honest"
    );
    assert!(
        v6.has_impacts("t0"),
        "v6 reader must expose the block-max surface"
    );

    let queries: Vec<Vec<String>> = vec![
        vec!["t0".into()],
        vec!["t1".into(), "t3".into()],
        vec!["t2".into(), "t4".into(), "t7".into(), "missing".into()],
        vec![
            "t0".into(),
            "t5".into(),
            "t6".into(),
            "t8".into(),
            "t9".into(),
        ],
        vec!["missing".into()],
    ];
    let params = Bm25Params::default();
    for terms in &queries {
        let stats = CorpusStats {
            doc_count: store.doc_count(),
            total_doc_length: store.total_doc_length(),
            dfs: terms.iter().map(|t| Bm25Index::df(&store, t)).collect(),
        };
        for k in [1usize, 10, 100] {
            let a = bm25::top_k(&v5, terms, &stats, params, k);
            let b = bm25::top_k(&v6, terms, &stats, params, k);
            assert_eq!(sig(&a), sig(&b), "top_k({terms:?}, k={k})");

            let ae = bm25::top_k_exhaustive(&v5, terms, &stats, params, k);
            let be = bm25::top_k_exhaustive(&v6, terms, &stats, params, k);
            assert_eq!(sig(&ae), sig(&be), "top_k_exhaustive({terms:?}, k={k})");

            // Pruned, unseeded and seeded with the true kth-best score
            // (the coordinator's floor-seed shape). The v5-vs-v6
            // identity is the contract here; pruned-vs-exhaustive
            // exactness is blockmax.rs's.
            for floor in [
                f64::NEG_INFINITY,
                a.last().map_or(f64::NEG_INFINITY, |h| h.score),
            ] {
                let ap = bm25::top_k_pruned(&v5, terms, &stats, params, k, floor);
                let bp = bm25::top_k_pruned(&v6, terms, &stats, params, k, floor);
                assert_eq!(
                    sig(&ap),
                    sig(&bp),
                    "top_k_pruned({terms:?}, k={k}, floor={floor})"
                );
            }
        }
    }

    // The document plane agrees too.
    for slot in (0..store.next_doc_id()).step_by(97) {
        assert_eq!(
            Bm25Index::text(&v5, slot),
            Bm25Index::text(&v6, slot),
            "text({slot})"
        );
        assert_eq!(
            Bm25Index::lineage(&v5, slot),
            Bm25Index::lineage(&v6, slot),
            "lineage({slot})"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}
