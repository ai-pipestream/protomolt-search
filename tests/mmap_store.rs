//! Acceptance tests for the disk-resident (mmap) BM25 store: v3 round
//! trip must be bit-identical to the heap builder, and opening the
//! resident reader must not pull the file into heap.

mod common;

use turbovec_search::bm25::{self, Bm25Params, CorpusStats};
use turbovec_search::postings::{
    AnalyzedDoc, Bm25Index, Bm25Reader, Bm25Store, DocLineage, DocTerms,
};

fn build_store(n_docs: usize, text_len: usize) -> Bm25Store {
    let mut store = Bm25Store::new();
    let filler = "x".repeat(text_len.saturating_sub(80));
    for i in 0..n_docs as u32 {
        let text =
            format!("document {i} about rust search engines and the law of vectors {filler}");
        let terms: DocTerms = vec![
            ("rust".to_string(), 1 + i % 3, vec![(9, 13)]),
            ("search".to_string(), 1, vec![(14, 20)]),
            (format!("topic{}", i % 17), 2, vec![(30, 34), (40, 44)]),
        ];
        let length: u32 = terms.iter().map(|t| t.1).sum();
        store.add_document_with_lineage(
            i,
            text,
            AnalyzedDoc::body(terms, length),
            Some(DocLineage {
                parent_id: 1000 + u64::from(i),
                group_id: 2000 + u64::from(i % 50),
                span_start: i * 7,
                span_end: i * 7 + 100,
            }),
        );
    }
    store
}

fn rss_bytes() -> usize {
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap();
    let resident_pages: usize = statm.split_whitespace().nth(1).unwrap().parse().unwrap();
    resident_pages * 4096
}

#[test]
fn resident_matches_heap_bit_identical() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("tvbm25_mmap_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("shard.tv.bm25");

    let store = build_store(500, 2_000);
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();

    // Corpus stats identical.
    let heap: &dyn Bm25Index = &store;
    let disk: &dyn Bm25Index = &reader;
    assert_eq!(heap.doc_count(), disk.doc_count());
    assert_eq!(heap.total_doc_length(), disk.total_doc_length());
    for doc_id in [0, 17, 250, 499] {
        assert_eq!(heap.doc_length(doc_id), disk.doc_length(doc_id));
        assert_eq!(heap.text(doc_id), disk.text(doc_id));
        assert_eq!(heap.lineage(doc_id), disk.lineage(doc_id));
    }
    for term in ["rust", "search", "topic3", "topic16", "absent"] {
        assert_eq!(heap.df(term), disk.df(term), "df for {term}");
    }

    // Scoring identical, bitwise, through the shared scorer.
    let terms: Vec<String> = ["rust", "search", "topic3", "absent"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let stats = CorpusStats {
        doc_count: store.doc_count(),
        total_doc_length: store.total_doc_length(),
        dfs: terms.iter().map(|t| heap.df(t)).collect(),
    };
    let heap_hits = bm25::top_k(heap, &terms, &stats, Bm25Params::default(), 20);
    let disk_hits = bm25::top_k(disk, &terms, &stats, Bm25Params::default(), 20);
    assert_eq!(heap_hits.len(), disk_hits.len());
    for (h, d) in heap_hits.iter().zip(disk_hits.iter()) {
        assert_eq!(h.doc_id, d.doc_id);
        assert_eq!(
            h.score.to_bits(),
            d.score.to_bits(),
            "score bits for doc {}",
            h.doc_id
        );
        assert_eq!(h.term_offsets, d.term_offsets);
    }

    // Candidate scoring identical too.
    let ids: Vec<u32> = (0..40).collect();
    let heap_c = bm25::score_candidates(heap, &terms, &stats, Bm25Params::default(), &ids);
    let disk_c = bm25::score_candidates(disk, &terms, &stats, Bm25Params::default(), &ids);
    assert_eq!(heap_c.len(), disk_c.len());
    for (h, d) in heap_c.iter().zip(disk_c.iter()) {
        assert_eq!(h.doc_id, d.doc_id);
        assert_eq!(h.score.to_bits(), d.score.to_bits());
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn resident_open_does_not_grow_rss_like_a_heap_load() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("tvbm25_rss_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("big.tv.bm25");

    // ~160MB of text + postings. Large enough that a heap load is
    // unmistakable in RSS, small enough to write quickly.
    let store = build_store(40_000, 4_000);
    store.save(&path).unwrap();
    let file_len = path.metadata().unwrap().len() as usize;
    eprintln!("file size: {:.1} MiB", file_len as f64 / 1e6);

    let rss_before = rss_bytes();
    let reader = Bm25Reader::open(&path).unwrap();
    // Exercise the read paths a query would hit.
    let terms = vec!["rust".to_string(), "topic3".to_string()];
    let stats = CorpusStats {
        doc_count: reader.doc_count(),
        total_doc_length: reader.total_doc_length(),
        dfs: terms.iter().map(|t| reader.df(t)).collect(),
    };
    let _ = bm25::top_k(&reader, &terms, &stats, Bm25Params::default(), 10);
    for doc_id in [0, 10_000, 20_000, 39_999] {
        let _ = reader.text(doc_id);
        let _ = reader.lineage(doc_id);
    }
    let rss_resident = rss_bytes();
    drop(reader);

    let heap = turbovec_search::postings::Bm25Store::load(&path).unwrap();
    let rss_heap = rss_bytes();
    drop(heap);

    let resident_growth = rss_resident.saturating_sub(rss_before);
    let heap_growth = rss_heap.saturating_sub(rss_resident);
    eprintln!(
        "rss growth: resident +{:.1} MiB, heap-load +{:.1} MiB",
        resident_growth as f64 / 1e6,
        heap_growth as f64 / 1e6
    );
    assert!(
        resident_growth < 32 * 1024 * 1024,
        "resident open grew RSS by {resident_growth} bytes (heap tables must stay small)"
    );
    assert!(
        heap_growth > file_len / 2,
        "heap load should grow RSS by roughly the file size ({heap_growth} vs {file_len})"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
