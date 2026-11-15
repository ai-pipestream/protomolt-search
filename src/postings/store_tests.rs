use super::format_tests::test_dir;
use super::*;

/// A deterministic synthetic corpus: gappy ids, mixed lineages,
/// offsets on some terms, shared and unique terms across docs.
pub(super) fn synthetic_corpus() -> Vec<(u32, String, AnalyzedDoc, Option<DocLineage>)> {
    let vocab = ["court", "plaintiff", "rust", "search", "vector", "quant"];
    let mut docs = Vec::new();
    let mut id = 0u32;
    for i in 0..200u32 {
        // Every 7th id is a gap (vector-side slot).
        id += if i % 7 == 0 { 2 } else { 1 };
        let mut terms: DocTerms = Vec::new();
        let mut length = 0;
        for (vi, term) in vocab.iter().enumerate() {
            if (i as usize + vi).is_multiple_of(vi + 2) {
                let tf = 1 + (i % 3);
                let offsets = if vi % 2 == 0 {
                    (0..tf).map(|o| (o * 10, o * 10 + 4)).collect()
                } else {
                    Vec::new()
                };
                terms.push((term.to_string(), tf, offsets));
                length += tf;
            }
        }
        let lineage = (i % 3 == 0).then_some(DocLineage {
            parent_id: u64::from(i) * 17,
            group_id: u64::from(i) * 31,
            span_start: i,
            span_end: i + 100,
        });
        docs.push((
            id,
            format!("document {i} body text"),
            AnalyzedDoc::body(terms, length),
            lineage,
        ));
    }
    docs
}

fn doc_a() -> AnalyzedDoc {
    AnalyzedDoc::body(
        vec![
            ("rust".to_string(), 2, vec![(0, 4), (10, 14)]),
            ("search".to_string(), 1, vec![(5, 11)]),
        ],
        3,
    )
}

fn doc_b() -> AnalyzedDoc {
    AnalyzedDoc::body(
        vec![
            ("rust".to_string(), 1, vec![(0, 4)]),
            ("vector".to_string(), 2, vec![(5, 11), (12, 18)]),
        ],
        3,
    )
}

#[test]
fn build_and_query_postings() {
    let mut store = Bm25Store::new();
    store.add_document(0, "rust search rust".to_string(), doc_a());
    store.add_document(1, "rust vector vector".to_string(), doc_b());

    assert_eq!(store.doc_count(), 2);
    assert_eq!(store.total_doc_length(), 6);
    assert_eq!(store.doc_length(1), 3);
    let postings = store.postings("rust").unwrap();
    assert_eq!(postings.len(), 2);
    assert_eq!(
        postings[0],
        Posting {
            doc_id: 0,
            tf: 2,
            offsets: vec![(0, 4), (10, 14)],
            positions: Vec::new(),
            pos_lengths: Vec::new(),
        }
    );
    assert_eq!(store.postings("vector").unwrap()[0].tf, 2);
    assert!(store.postings("missing").is_none());
    assert_eq!(store.text(1), Some("rust vector vector"));
}

#[test]
fn sparse_slots_for_shared_id_space() {
    let mut store = Bm25Store::new();
    // Ids 0..2 were consumed by the vector side; first document lands
    // at id 2, leaving slots 0..1 sparse.
    store.add_document(
        2,
        "hello world".to_string(),
        AnalyzedDoc::body(vec![("hello".to_string(), 1, vec![(0, 5)])], 2),
    );
    assert_eq!(store.next_doc_id(), 3);
    assert_eq!(store.doc_count(), 1);
    assert_eq!(store.doc_length(0), 0);
    assert_eq!(store.doc_length(2), 2);
    assert!(store.text(0).is_none());
    assert_eq!(store.text(2), Some("hello world"));
}

#[test]
fn round_trip_through_disk() {
    let dir = test_dir("roundtrip");
    let path = dir.join("shard.tv.bm25");

    let mut store = Bm25Store::new();
    store.add_document(0, "rust search rust".to_string(), doc_a());
    store.add_document(2, "rust vector vector".to_string(), doc_b());
    store.save(&path).unwrap();

    let loaded = Bm25Store::load(&path).unwrap();
    assert_eq!(loaded.fields[0].doc_lengths, store.fields[0].doc_lengths);
    assert_eq!(loaded.fields[0].total_length, store.fields[0].total_length);
    assert_eq!(loaded.texts, store.texts);
    assert_eq!(loaded.fields[0].postings, store.fields[0].postings);

    let _ = std::fs::remove_dir_all(&dir);
}

/// Regression test for the reader's lineage lookup: the on-disk
/// lineage section is variable stride (1 B absent, 25 B present),
/// so indexing must walk variable-width entries across gap slots
/// and missing lineages rather than assuming a fixed 25 B stride.
#[test]
fn reader_lineage_with_gaps_and_missing_entries() {
    let dir = test_dir("lineage");
    let path = dir.join("shard.bm25");
    let mut store = Bm25Store::new();
    let lineage = |i: u32| DocLineage {
        parent_id: 1000 + u64::from(i),
        group_id: 2000 + u64::from(i),
        span_start: i * 7,
        span_end: i * 7 + 90,
    };
    // Gap slots at 0..1, then a mix of present and missing lineages.
    store.add_document_with_lineage(
        2,
        "a".to_string(),
        AnalyzedDoc::body(vec![("rust".into(), 1, vec![(0, 4)])], 1),
        Some(lineage(2)),
    );
    store.add_document_with_lineage(
        3,
        "b".to_string(),
        AnalyzedDoc::body(vec![("rust".into(), 2, vec![(0, 4), (6, 10)])], 2),
        None,
    );
    store.add_document_with_lineage(
        7,
        "c".to_string(),
        AnalyzedDoc::body(vec![("search".into(), 1, vec![(0, 6)])], 1),
        Some(lineage(7)),
    );
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    for slot in 0..store.next_doc_id() {
        assert_eq!(reader.lineage(slot), store.lineage(slot), "lineage({slot})");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Binary-searched posting_offsets lookup over the fixed-stride doc run
/// across first, middle, last, gap, out-of-range, and unknown-term probes.
#[test]
fn posting_offsets_lookup_correctness() {
    let dir = test_dir("posting_offsets");
    let path = dir.join("shard.bm25");
    let mut store = Bm25Store::new();
    let n = 5000u32;
    for i in 0..n {
        store.add_document(
            i * 2, // gaps: even ids only
            format!("doc {i}"),
            AnalyzedDoc::body(vec![("court".to_string(), 1, vec![(i, i + 3)])], 5),
        );
    }
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    // First posting of the list.
    assert_eq!(reader.posting_offsets("court", 0), vec![(0, 3)]);
    // Middle and last.
    assert_eq!(reader.posting_offsets("court", 4998), vec![(2499, 2502)]);
    assert_eq!(reader.posting_offsets("court", 9998), vec![(4999, 5002)]);
    // Absent: a gap id, beyond the last, and an unknown term.
    assert!(reader.posting_offsets("court", 9997).is_empty());
    assert!(reader.posting_offsets("court", 2_000_000).is_empty());
    assert!(reader.posting_offsets("nope", 0).is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn load_rejects_garbage() {
    let dir = test_dir("bad_garbage");
    let path = dir.join("bad.bm25");
    std::fs::write(&path, b"not a postings file").unwrap();
    assert!(Bm25Store::load(&path).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}
