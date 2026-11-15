//! Format v6/v8 query contract: a `.bm25` shard produced via either
//! `Bm25Store::save` or `SpillBuilder::finish` emits identical bytes and
//! answers every query identically to the heap store, bit for bit, across
//! the plain scored walk, the exhaustive oracle, and the block-max pruned
//! scorer (unseeded and floor-seeded).

use std::path::{Path, PathBuf};

use pipestream_search::bm25::{self, Bm25Params, CorpusStats, ScoredDoc};
use pipestream_search::postings::{
    AnalyzedDoc, Bm25Index, Bm25Reader, Bm25Store, DocTerms, SpillBuilder,
};

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
    let dir = std::env::temp_dir().join(format!("v6fmt_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Build a random multi-block corpus in both `Bm25Store` and `SpillBuilder`:
/// gappy ids, tf skew, and mixed empty/populated offsets.
fn build_store_and_spill(
    rng: &mut Lcg,
    n_docs: u64,
    vocab: &[String],
    work_dir: &Path,
) -> (Bm25Store, SpillBuilder) {
    let mut store = Bm25Store::new();
    let mut spill = SpillBuilder::create(work_dir)
        .unwrap()
        .with_buffer_bytes(2048);
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
        let doc = AnalyzedDoc::body(terms, length);
        let text = format!("doc {id}");
        store.add_document(id, text.clone(), doc.clone());
        spill
            .add_document_with_lineage(id, text, doc, None)
            .unwrap();
    }
    (store, spill)
}

type Sig = Vec<(u32, u64, Vec<(usize, Vec<(u32, u32)>)>)>;

fn sig(hits: &[ScoredDoc]) -> Sig {
    hits.iter()
        .map(|h| (h.doc_id, h.score.to_bits(), h.term_offsets.clone()))
        .collect()
}

fn assert_scorer_parity(store: &Bm25Store, reader: &Bm25Reader, terms: &[String]) {
    let params = Bm25Params::default();
    let stats = CorpusStats {
        doc_count: store.doc_count(),
        total_doc_length: store.total_doc_length(),
        dfs: terms.iter().map(|t| Bm25Index::df(store, t)).collect(),
    };
    for k in [1usize, 10, 100] {
        let heap = bm25::top_k(store, terms, &stats, params, k);
        let disk = bm25::top_k(reader, terms, &stats, params, k);
        assert_eq!(sig(&heap), sig(&disk), "top_k({terms:?}, k={k})");

        let heap_ex = bm25::top_k_exhaustive(store, terms, &stats, params, k);
        let disk_ex = bm25::top_k_exhaustive(reader, terms, &stats, params, k);
        assert_eq!(sig(&heap), sig(&heap_ex), "exhaustive({terms:?}, k={k})");
        assert_eq!(
            sig(&heap_ex),
            sig(&disk_ex),
            "reader exhaustive({terms:?}, k={k})"
        );

        for floor in [
            f64::NEG_INFINITY,
            heap.last().map_or(f64::NEG_INFINITY, |h| h.score),
        ] {
            let pruned = bm25::top_k(
                reader,
                terms,
                &stats,
                params,
                bm25::TopK::new(k).floor(floor),
            );
            assert_eq!(
                sig(&heap),
                sig(&pruned),
                "top_k_pruned({terms:?}, k={k}, floor={floor})"
            );
        }
    }
}

#[test]
fn v6_reader_answers_identically_to_store_and_spill() {
    let dir = test_dir("identity");
    let vocab: Vec<String> = (0..10).map(|i| format!("t{i}")).collect();
    let mut rng = Lcg::new(0x51F0_0D06);
    let (store, mut spill) = build_store_and_spill(&mut rng, 3000, &vocab, &dir.join("spill_work"));

    let store_path = dir.join("shard.store.bm25");
    let spill_path = dir.join("shard.spill.bm25");
    store.save(&store_path).unwrap();
    spill.finish(&spill_path).unwrap();
    assert_eq!(
        std::fs::read(&store_path).unwrap(),
        std::fs::read(&spill_path).unwrap(),
    );
    let reader = Bm25Reader::open(&spill_path).unwrap();

    assert!(Bm25Index::df(&store, "t0") > 256, "corpus too small");
    assert!(reader.has_impacts("t0"), "reader must expose block-max");

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
    for terms in &queries {
        assert_scorer_parity(&store, &reader, terms);
    }

    for slot in (0..store.next_doc_id()).step_by(97) {
        assert_eq!(
            Bm25Index::text(&reader, slot),
            store.text(slot).map(str::to_string),
            "text({slot})"
        );
        assert_eq!(
            Bm25Index::lineage(&reader, slot),
            store.lineage(slot),
            "lineage({slot})"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}
