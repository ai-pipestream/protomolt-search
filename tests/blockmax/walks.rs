use super::*;
use pipestream_search::segmented::SegmentedShard;

/// Twelve randomized rounds: every walk choice and the heap merge-join
/// must reproduce the plain scorer bit for bit.
pub(super) fn assert_random_walks_match(
    dir: &std::path::Path,
    rng: &mut Lcg,
    params: Bm25Params,
    all_walks: [pipestream_search::bm25::CandidateWalk; 3],
) {
    for round in 0..12 {
        let n_vocab = 3 + rng.below(8);
        let vocab: Vec<String> = (0..n_vocab).map(|i| format!("t{i}")).collect();
        let n_docs = 40 + rng.below(300);
        let corpus = random_corpus(&mut *rng, n_docs, &vocab, false, round % 3 == 0);
        let store = build_store(&corpus);
        let path = dir.join(format!("w{round}.bm25"));
        store.save(&path).unwrap();
        let reader = Bm25Reader::open(&path).unwrap();
        let n_take = 1 + rng.below(3) as usize;
        let mut terms: Vec<String> = vocab
            .iter()
            .filter(|_| rng.below(2) == 0)
            .take(n_take)
            .cloned()
            .collect();
        terms.push("absent".to_string());
        if terms.len() == 1 {
            terms.insert(0, vocab[0].clone());
        }
        let stats = CorpusStats {
            doc_count: store.doc_count(),
            total_doc_length: store.total_doc_length(),
            dfs: terms.iter().map(|t| Bm25Index::df(&store, t)).collect(),
        };
        // Sorted distinct candidates: gap slots, ids past the store, and
        // in one round of three every slot of the store.
        let mut candidates: Vec<u32> = if round % 3 == 1 {
            (0..store.next_doc_id() + 5).collect()
        } else {
            (0..rng.below(120))
                .map(|_| rng.below(u64::from(store.next_doc_id()) + 20) as u32)
                .collect()
        };
        candidates.sort_unstable();
        candidates.dedup();
        if round == 5 {
            candidates.clear();
        }
        let oracle = plain_of(
            &bm25::score_candidates(&reader, &terms, &stats, params, &candidates),
            &candidates,
        );
        let heap = bm25::score_candidates_plain(&store, &terms, &stats, params, &candidates);
        assert_eq!(bits_of(&heap), oracle, "round {round}: heap merge-join");
        for walk in all_walks {
            let got = bm25::score_candidates_plain_walk(
                &reader,
                &terms,
                &stats,
                params,
                &candidates,
                walk,
            );
            assert_eq!(bits_of(&got), oracle, "round {round}: v5 reader, {walk:?}");
        }
    }
}

/// A catalog of three sealed parts whose vocabularies interleave, plus
/// the whole-corpus store and the tail document appended to both.
/// Returns the segmented shard, the whole store, and the row base.
pub(super) fn build_three_part_catalog(
    dir: &std::path::Path,
    rng: &mut Lcg,
) -> (pipestream_search::segmented::SegmentedShard, Bm25Store, u64) {
    use pipestream_search::live_docs::LiveDocs;
    use pipestream_search::segments::{SegmentCatalog, SegmentSource};
    // A catalog of three sealed parts: part p holds only the terms whose
    // index is not p (mod 3), so every term's chained cursor skips one
    // part; the tail holds one document with t0, so t0 has no cursor at
    // all and merge-joins whatever the walk.
    let vocab: Vec<String> = (0..6).map(|i| format!("t{i}")).collect();
    let root = dir.join("catalog");
    let catalog = SegmentCatalog::open(&root).unwrap();
    let mut base = 0u64;
    let mut whole = Bm25Store::new();
    for part in 0..3usize {
        let part_vocab: Vec<String> = vocab
            .iter()
            .enumerate()
            .filter(|(i, _)| i % 3 != part)
            .map(|(_, t)| t.clone())
            .collect();
        let part_docs = 60 + rng.below(100);
        let corpus = random_corpus(&mut *rng, part_docs, &part_vocab, false, true);
        let store = build_store(&corpus);
        for (id, text, doc) in &corpus {
            whole.add_document(base as u32 + *id, text.clone(), doc.clone());
        }
        let stage = dir.join(format!("part-{part}"));
        std::fs::create_dir_all(&stage).unwrap();
        let bm25_path = stage.join("documents.bm25");
        let live = stage.join("live.bin");
        store.save(&bm25_path).unwrap();
        let rows = u64::from(store.next_doc_id());
        LiveDocs::default().write(&live, rows).unwrap();
        catalog
            .append(SegmentSource {
                multi_res_path: None,
                segment_id: &format!("part-{part}"),
                generation: part as u64 + 1,
                base_label: base,
                backend_kind: "",
                vector_path: None,
                exact_vector_path: None,
                bm25_path: &bm25_path,
                live_docs_path: &live,
                partition_column: None,
                parents_colocated: false,
            })
            .unwrap();
        base += rows;
    }
    let mut shard = SegmentedShard::open(&root, Bm25Store::new()).unwrap();
    let tail_doc = AnalyzedDoc::body(vec![("t0".into(), 2, vec![])], 2);
    shard
        .add_document(base as u32, "tail".into(), tail_doc.clone(), None)
        .unwrap();
    whole.add_document(base as u32, "tail".into(), tail_doc);
    (shard, whole, base)
}
