//! Signed score ties and echoed live floors preserve exact lexical winners.

use super::HeapEntry;
use crate::bm25::{
    self, Bm25Params, CorpusStats, PruneStats, ScoredDoc, ScoringQuery, TopKOptions,
};
use crate::postings::{AnalyzedDoc, Bm25Index, Bm25Reader, Bm25Store};
use crate::scorefn::{ColumnRef, NumericRead, ScoreChain, Stage, StageOp};
use std::collections::BinaryHeap;
use std::path::PathBuf;

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "bm25-signed-ranking-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).expect("remove signed-score fixture");
    }
}

struct Fixture {
    reader: Bm25Reader,
    stats: CorpusStats,
    terms: Vec<String>,
    chain: ScoreChain,
    _directory: Directory,
}

impl Fixture {
    fn new(scores: &[f64]) -> Self {
        let directory = Directory::new();
        let rows = scores.len() as u32;
        let base = bm25::idf(u64::from(rows), rows);
        let mut store = Bm25Store::new().with_numerics(&["cancel_base", "score"]);
        for (doc, &score) in scores.iter().enumerate() {
            let doc = doc as u32;
            let analyzed = AnalyzedDoc::body(vec![("x".into(), 1, vec![(0, 1)])], 1);
            store.add_document(doc, "x".into(), analyzed);
            store.set_numeric(0, doc, -base);
            store.set_numeric(1, doc, score);
        }
        let path = directory.0.join("documents.bm25");
        store.save(&path).unwrap();
        let reader = Bm25Reader::open(&path).unwrap();
        assert!(
            reader.has_impacts("x"),
            "the regression must exercise pruning"
        );
        let chain = ScoreChain {
            stages: (0..2)
                .map(|column| Stage {
                    op: StageOp::AddLinear { weight: 1.0 },
                    column: Some(ColumnRef::Numeric(column)),
                    min_max: reader.numeric_min_max(column),
                })
                .collect(),
        };
        Self {
            reader,
            stats: CorpusStats {
                doc_count: u64::from(rows),
                total_doc_length: u64::from(rows),
                dfs: vec![rows],
            },
            terms: vec!["x".into()],
            chain,
            _directory: directory,
        }
    }

    fn query<'a>(&'a self, columns: &'a ReaderNumerics<'a>) -> ScoringQuery<'a> {
        ScoringQuery {
            store: &self.reader,
            terms: &self.terms,
            stats: &self.stats,
            params: Bm25Params::default(),
            chain: Some((&self.chain, columns)),
            filter: None,
        }
    }

    fn exhaustive(&self, columns: &ReaderNumerics<'_>) -> Vec<ScoredDoc> {
        bm25::top_k_chained_filtered(
            &self.reader,
            &self.terms,
            &self.stats,
            Bm25Params::default(),
            1,
            Some((&self.chain, columns)),
            None,
        )
    }
}

struct ReaderNumerics<'a>(&'a Bm25Reader);

impl NumericRead for ReaderNumerics<'_> {
    fn value(&self, column: usize, doc: u32) -> Option<f64> {
        self.0.numeric_value(column, doc)
    }
    fn map_value(&self, column: usize, key: u32, doc: u32) -> Option<f64> {
        self.0.map_numeric_value(column, key, doc)
    }
    fn map_int_value(&self, column: usize, key: u32, doc: u32) -> Option<i64> {
        self.0.map_integer_value(column, key, doc)
    }
    fn map_uint_value(&self, column: usize, key: u32, doc: u32) -> Option<u64> {
        self.0.map_unsigned_integer_value(column, key, doc)
    }
    fn int_value(&self, column: usize, doc: u32) -> Option<i64> {
        self.0.integer_value(column, doc)
    }
    fn uint_value(&self, column: usize, doc: u32) -> Option<u64> {
        self.0.unsigned_integer_value(column, doc)
    }
    fn geo_value(&self, column: usize, doc: u32) -> Option<(f64, f64)> {
        self.0.geo_value(column, doc)
    }
    fn facet_ord(&self, column: usize, doc: u32) -> Option<u32> {
        self.0.facet_ord(column, doc)
    }
    fn map_facet_value_ord(&self, column: usize, key: u32, doc: u32) -> Option<u32> {
        self.0.map_facet_value_ord(column, key, doc)
    }
}

fn signature(hits: &[ScoredDoc]) -> Vec<(u32, u64)> {
    hits.iter()
        .map(|hit| (hit.doc_id, hit.score.to_bits()))
        .collect()
}

#[test]
fn echoed_negative_live_floor_keeps_later_better_negative_hit() {
    let fixture = Fixture::new(&[-5.0, -1.0, -2.0]);
    let columns = ReaderNumerics(&fixture.reader);
    let expected = fixture.exhaustive(&columns);
    assert_eq!(signature(&expected), [(1, (-1.0f64).to_bits())]);
    let mut offered = Vec::new();
    let mut echo = |seed: Option<f32>| {
        if let Some(seed) = seed {
            offered.push(seed);
        }
        seed
    };
    let mut stats = PruneStats::default();
    let actual = bm25::top_k_pruned_chained_filtered_stats_streaming(
        fixture.query(&columns),
        TopKOptions {
            k: 1,
            floor: f64::NEG_INFINITY,
            block_max: true,
        },
        &mut stats,
        Some(&mut echo),
        None,
    );
    assert_eq!(signature(&actual), signature(&expected));
    assert_eq!(offered.first().copied(), Some((-5.0f32).next_down()));
    assert_eq!(offered.last().copied(), Some((-1.0f32).next_down()));
    assert!(offered.windows(2).all(|pair| pair[0] <= pair[1]));
    assert!(stats.blocks_total > 0);
}

#[test]
fn signed_zero_heap_and_pruning_bounds_use_the_same_tie() {
    let negative = HeapEntry {
        score: -0.0,
        doc_id: 0,
        mask: 1,
    };
    let positive = HeapEntry {
        score: 0.0,
        doc_id: 1,
        mask: 1,
    };
    let heap = BinaryHeap::from([negative, positive]);
    assert_eq!(
        heap.peek().unwrap().doc_id,
        1,
        "higher doc id is the worst tied survivor"
    );
    assert_eq!(
        super::compare((0, -0.0), (0, 0.0)),
        std::cmp::Ordering::Equal
    );
    for (bound, kth) in [(0.0, -0.0), (-0.0, 0.0)] {
        let cutoff = crate::bm25::pruned_scan::Cutoff {
            floor: f64::NEG_INFINITY,
            kth: Some(kth),
            chain: None,
        };
        assert!(
            cutoff.inert(bound),
            "a later equal-zero candidate cannot replace a lower id"
        );
    }
}

#[test]
fn mmap_signed_zero_scores_keep_the_lower_id_and_raw_score_bits() {
    let mut fixture = Fixture::new(&[-5.0, 1.0]);
    fixture.chain.stages.push(Stage {
        op: StageOp::MultExpDecay {
            origin: 1000.0,
            scale: 1.0,
        },
        column: Some(ColumnRef::Numeric(0)),
        min_max: fixture.reader.numeric_min_max(0),
    });
    let columns = ReaderNumerics(&fixture.reader);
    let base = bm25::idf(2, 2);
    assert_eq!(
        fixture.chain.eval(base, 0, &columns).to_bits(),
        (-0.0f64).to_bits()
    );
    assert_eq!(
        fixture.chain.eval(base, 1, &columns).to_bits(),
        0.0f64.to_bits()
    );
    let expected = fixture.exhaustive(&columns);
    assert_eq!(signature(&expected), [(0, (-0.0f64).to_bits())]);
    let mut emitted = Vec::new();
    let mut sink = |doc, score: f32| emitted.push((doc, score.to_bits()));
    let mut stats = PruneStats::default();
    let actual = bm25::top_k_pruned_chained_filtered_stats_streaming(
        fixture.query(&columns),
        TopKOptions {
            k: 1,
            floor: f64::NEG_INFINITY,
            block_max: true,
        },
        &mut stats,
        None,
        Some(&mut sink),
    );
    assert_eq!(signature(&actual), signature(&expected));
    assert!(emitted.contains(&(0, (-0.0f32).to_bits())));
    assert!(stats.blocks_total > 0);
}
