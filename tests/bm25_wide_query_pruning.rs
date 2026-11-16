use std::cell::Cell;
use std::path::PathBuf;

use pipestream_search::bm25::{self, Bm25Params, CorpusStats, FieldQuery, TopK};
use pipestream_search::postings::{
    AnalyzedDoc, Bm25Index, Bm25Reader, Bm25Store, DocLineage, ImpactCursor, PostingCallback,
};

const COMMON_TERMS: usize = 160;
const WIDE_DOCS: u32 = 2_048;
const PARAMS: Bm25Params = Bm25Params { k1: 1.2, b: 0.75 };

fn test_dir(name: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "bm25_wide_query_pruning_{name}_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn wide_fixture() -> (Bm25Store, Vec<String>, CorpusStats) {
    let terms: Vec<String> = (0..COMMON_TERMS)
        .map(|term| format!("common{term}"))
        .chain(std::iter::once("rare-anchor".to_string()))
        .collect();
    let mut store = Bm25Store::new();
    for doc in 0..WIDE_DOCS {
        let mut postings: Vec<_> = terms[..COMMON_TERMS]
            .iter()
            .map(|term| (term.clone(), 1, vec![(0, 1)]))
            .collect();
        if doc == WIDE_DOCS - 1 {
            postings.push(("rare-anchor".to_string(), 1, vec![(160, 166)]));
        }
        let length = postings.iter().map(|(_, tf, _)| *tf).sum();
        store.add_document(
            doc,
            format!("doc {doc}"),
            AnalyzedDoc::body(postings, length),
        );
    }
    let stats = CorpusStats {
        doc_count: store.doc_count(),
        total_doc_length: store.total_doc_length(),
        dfs: terms.iter().map(|term| store.df(term)).collect(),
    };
    (store, terms, stats)
}

#[test]
fn wide_query_prunes_and_preserves_every_matched_term() {
    let dir = test_dir("wide");
    let path = dir.join("wide.bm25");
    let (store, terms, stats) = wide_fixture();
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let unseeded = bm25::top_k_exhaustive(
        &store,
        &terms,
        &stats,
        PARAMS,
        TopK::new(1).include_offsets(false),
    );
    assert_eq!(unseeded[0].doc_id, WIDE_DOCS - 1);
    let floor = unseeded[0].score;
    let expected = bm25::top_k_exhaustive(
        &store,
        &terms,
        &stats,
        PARAMS,
        TopK::new(1).floor(floor).include_offsets(false),
    );
    let mut prune = bm25::PruneStats::default();
    let actual = bm25::top_k(
        &reader,
        &terms,
        &stats,
        PARAMS,
        TopK::new(1)
            .floor(floor)
            .include_offsets(false)
            .prune_stats(&mut prune),
    );
    assert_eq!(actual, expected);
    assert!(prune.blocks_total > 0);
    assert!(prune.blocks_skipped > 0, "wide query did not skip blocks");
    assert!(
        prune.candidates_evaluated < u64::from(WIDE_DOCS),
        "wide query exhaustively evaluated every document"
    );

    let expected = bm25::top_k_exhaustive(
        &store,
        &terms,
        &stats,
        PARAMS,
        TopK::new(1).floor(floor).include_offsets(true),
    );
    let actual = bm25::top_k(
        &reader,
        &terms,
        &stats,
        PARAMS,
        TopK::new(1).floor(floor).include_offsets(true),
    );
    assert_eq!(actual, expected);
    assert!(actual[0]
        .term_offsets
        .iter()
        .any(|(term, offsets)| *term == COMMON_TERMS && *offsets == vec![(160, 166)]));
    let _ = std::fs::remove_dir_all(&dir);
}

struct CountingIndex<'a> {
    inner: &'a Bm25Reader,
    offset_reads: &'a Cell<usize>,
    posting_walks: &'a Cell<usize>,
}

impl Bm25Index for CountingIndex<'_> {
    fn doc_count(&self) -> u64 {
        self.inner.doc_count()
    }

    fn total_doc_length(&self) -> u64 {
        self.inner.total_doc_length()
    }

    fn doc_length(&self, doc_id: u32) -> u32 {
        self.inner.doc_length(doc_id)
    }

    fn df(&self, term: &str) -> u32 {
        self.inner.df(term)
    }

    fn for_each_posting(&self, term: &str, f: &mut PostingCallback<'_>) {
        self.posting_walks.set(self.posting_walks.get() + 1);
        self.inner.for_each_posting(term, f);
    }

    fn for_each_doc_tf(&self, term: &str, f: &mut dyn FnMut(u32, u32)) {
        self.inner.for_each_doc_tf(term, f);
    }

    fn posting_offsets(&self, term: &str, doc_id: u32) -> Vec<(u32, u32)> {
        self.offset_reads.set(self.offset_reads.get() + 1);
        self.inner.posting_offsets(term, doc_id)
    }

    fn impacts(&self, term: &str) -> Option<ImpactCursor<'_>> {
        self.inner.impacts(term)
    }

    fn text(&self, doc_id: u32) -> Option<String> {
        self.inner.text(doc_id)
    }

    fn lineage(&self, doc_id: u32) -> Option<DocLineage> {
        self.inner.lineage(doc_id)
    }

    fn expand_prefix(&self, prefix: &str, cap: usize) -> Result<Vec<String>, usize> {
        self.inner.expand_prefix(prefix, cap)
    }

    fn suggest_prefix(&self, prefix: &str, max_scan: usize) -> Result<Vec<(String, u32)>, usize> {
        self.inner.suggest_prefix(prefix, max_scan)
    }

    fn prefix_terms(&self, prefix: &str) -> Box<dyn Iterator<Item = String> + '_> {
        self.inner.prefix_terms(prefix)
    }
}

fn small_fixture() -> (Bm25Store, Vec<String>, CorpusStats) {
    let terms = vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()];
    let mut store = Bm25Store::new();
    for doc in 0..512u32 {
        let mut postings = vec![("alpha".to_string(), 1, vec![(0, 5)])];
        if doc % 3 == 0 {
            postings.push(("beta".to_string(), 2, vec![(6, 10), (11, 15)]));
        }
        if doc == 511 {
            postings.push(("gamma".to_string(), 1, vec![(16, 21)]));
        }
        let length = postings.iter().map(|(_, tf, _)| *tf).sum();
        store.add_document(
            doc,
            format!("doc {doc}"),
            AnalyzedDoc::body(postings, length),
        );
    }
    let stats = CorpusStats {
        doc_count: store.doc_count(),
        total_doc_length: store.total_doc_length(),
        dfs: terms.iter().map(|term| store.df(term)).collect(),
    };
    (store, terms, stats)
}

fn field<'a>(index: &'a dyn Bm25Index, terms: &'a [String], stats: &CorpusStats) -> FieldQuery<'a> {
    FieldQuery {
        index,
        terms,
        stats: stats.clone(),
        params: PARAMS,
        weight: 1.0,
    }
}

#[test]
fn disabled_offsets_avoid_positional_reads_on_every_scorer() {
    let dir = test_dir("offsets");
    let path = dir.join("offsets.bm25");
    let (store, terms, stats) = small_fixture();
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let offset_reads = Cell::new(0);
    let posting_walks = Cell::new(0);
    let index = CountingIndex {
        inner: &reader,
        offset_reads: &offset_reads,
        posting_walks: &posting_walks,
    };
    let expected = bm25::top_k_exhaustive(
        &store,
        &terms,
        &stats,
        PARAMS,
        TopK::new(4).include_offsets(false),
    );
    let actual = bm25::top_k(
        &index,
        &terms,
        &stats,
        PARAMS,
        TopK::new(4).include_offsets(false),
    );
    assert_eq!(actual, expected);
    let expected = bm25::top_k_fused_exhaustive(
        &[field(&store, &terms, &stats)],
        TopK::new(4).include_offsets(false),
        None,
    );
    let actual = bm25::top_k_fused(
        &[field(&index, &terms, &stats)],
        TopK::new(4).include_offsets(false),
    );
    assert_eq!(actual, expected);
    let actual = bm25::top_k_fused_exhaustive(
        &[field(&index, &terms, &stats)],
        TopK::new(4).include_offsets(false),
        None,
    );
    assert_eq!(actual, expected);
    assert!(actual.iter().all(|doc| doc.term_offsets.is_empty()));
    assert_eq!(offset_reads.get(), 0);
    assert_eq!(posting_walks.get(), 0);

    let with_offsets = bm25::top_k(
        &index,
        &terms,
        &stats,
        PARAMS,
        TopK::new(4).include_offsets(true),
    );
    assert!(with_offsets.iter().any(|doc| !doc.term_offsets.is_empty()));
    assert!(offset_reads.get() > 0);
    let _ = bm25::top_k_fused_exhaustive(
        &[field(&index, &terms, &stats)],
        TopK::new(4).include_offsets(true),
        None,
    );
    assert!(posting_walks.get() > 0);
    let _ = std::fs::remove_dir_all(&dir);
}
