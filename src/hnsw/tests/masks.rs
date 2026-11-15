//! Tail insertion, masks, floors, and candidate depth on the unary search path.

use super::*;

#[test]
fn tail_insertion_keeps_recall_in_both_modes() {
    let dim = 32;
    let (vectors, queries) = corpus_with_queries(5000, 100, dim, 0x3141);
    for mode in [BuildMode::FixedOrder, BuildMode::Parallel] {
        let mut index = HnswIndex::new(dim, config(mode)).unwrap();
        index.add(&vectors[..3000 * dim], dim).unwrap();
        // One by one, then a small batch, then one below and one above the
        // parallel threshold: appends stay unlinked until `prepare()` / `write()`.
        for row in 3000..3010 {
            index
                .add(&vectors[row * dim..(row + 1) * dim], dim)
                .unwrap();
        }
        index.add(&vectors[3010 * dim..3100 * dim], dim).unwrap();
        index.add(&vectors[3100 * dim..3300 * dim], dim).unwrap();
        index.add(&vectors[3300 * dim..5000 * dim], dim).unwrap();
        assert_eq!(index.len(), 5000);
        assert!(!index.is_graph_linked());

        // Before `prepare()`, the unlinked tail enumerates with exact recall.
        let unlinked = index
            .search(&queries[..dim], 10, VectorSearchOptions::new())
            .unwrap();
        assert_eq!(unlinked.tail_enumerated_rows, 5000);
        let r_unlinked = recall(&index, &vectors, &queries, dim, 10, 64);
        assert_eq!(r_unlinked, 1.0, "{mode:?}: unlinked recall@10 {r_unlinked}");

        // Sealing / preparing builds the graph in bulk across all appended rows.
        index.prepare().unwrap();
        assert!(index.is_graph_linked());
        let linked = index
            .search(&queries[..dim], 10, VectorSearchOptions::new())
            .unwrap();
        assert_eq!(linked.tail_enumerated_rows, 0);
        let r = recall(&index, &vectors, &queries, dim, 10, 64);
        assert!(r >= 0.95, "{mode:?}: recall@10 {r} after seal-time build");
        // The tail rows themselves are found in the sealed graph.
        let mut tail_found = 0;
        for row in 4900..5000 {
            let (hits, _) = index
                .search_with_stats(
                    &vectors[row * dim..(row + 1) * dim],
                    1,
                    16,
                    VectorSearchOptions::new(),
                )
                .unwrap();
            tail_found += usize::from(hits.first().map(|h| h.0) == Some(row as u32));
        }
        assert!(
            tail_found >= 95,
            "{mode:?}: {tail_found}/100 tail rows found as their own nearest"
        );
    }
    let mut index = HnswIndex::new(dim, config(BuildMode::FixedOrder)).unwrap();
    assert!(index.add(&vectors[..dim + 1], dim).is_err());
    assert!(index.add(&vectors[..dim], dim + 1).is_err());
    assert!(index.add(&[f32::NAN; 32], dim).is_err());
    index.add(&[], dim).unwrap();
    assert_eq!(index.len(), 0);
}

fn masked_results_are_allowed_and_recalled(fx: &Fixture, allow: &[bool]) {
    let (dim, vectors, queries, index) = (fx.dim, &fx.vectors[..], &fx.queries[..], &fx.index);
    let results = index
        .search(queries, 5, VectorSearchOptions::new().with_mask(allow))
        .unwrap();
    assert_eq!(results.result_count, 5);
    for q in 0..10 {
        let query = &queries[q * dim..(q + 1) * dim];
        let slots = results.slots_for_query(q);
        assert!(
            slots.iter().all(|&s| s >= 0 && allow[s as usize]),
            "{slots:?}"
        );
        // Masked recall against the exact masked truth.
        let truth: Vec<u32> = {
            let mut all: Vec<(f32, u32)> = vectors
                .chunks_exact(dim)
                .enumerate()
                .filter(|(i, _)| allow[*i])
                .map(|(i, row)| (row.iter().zip(query).map(|(a, b)| a * b).sum(), i as u32))
                .collect();
            all.sort_by(|a, b| b.0.total_cmp(&a.0));
            all.iter().take(5).map(|h| h.1).collect()
        };
        let hit = slots
            .iter()
            .filter(|s| truth.contains(&(**s as u32)))
            .count();
        assert!(hit >= 3, "query {q}: masked hits {hit}/5");
    }
}

fn empty_and_misfit_masks_pad_or_refuse(fx: &Fixture, allow: &[bool]) {
    let (dim, queries, index) = (fx.dim, &fx.queries[..], &fx.index);
    // A mask that admits nothing returns padding; a mask of the wrong
    // length refuses.
    let none = vec![false; 2000];
    let results = index
        .search(
            &queries[..dim],
            3,
            VectorSearchOptions::new().with_mask(&none),
        )
        .unwrap();
    assert_eq!(results.slots, vec![-1, -1, -1]);
    assert!(results.scores.iter().all(|s| *s == f32::NEG_INFINITY));
    assert!(index
        .search(
            &queries[..dim],
            3,
            VectorSearchOptions::new().with_mask(&allow[..10])
        )
        .is_err());
}

fn floors_and_depth_filter_results_only(fx: &Fixture) {
    let (dim, queries, index) = (fx.dim, &fx.queries[..], &fx.index);
    // The floor filters: the top-k above the unfloored k-th best is the
    // same set, and a floor above everything yields padding.
    let plain = index
        .search(&queries[..dim], 5, VectorSearchOptions::new())
        .unwrap();
    let kth = plain.scores[4];
    let floored = index
        .search(
            &queries[..dim],
            5,
            VectorSearchOptions::new().with_minimum_score(kth),
        )
        .unwrap();
    assert_eq!(floored.slots, plain.slots);
    let above = index
        .search(
            &queries[..dim],
            5,
            VectorSearchOptions::new().with_minimum_score(plain.scores[0] + 1.0),
        )
        .unwrap();
    assert_eq!(above.slots, vec![-1; 5]);
    // A floor between the 2nd and 3rd best keeps two and pads three.
    let between = (plain.scores[1] + plain.scores[2]) / 2.0;
    let two = index
        .search(
            &queries[..dim],
            5,
            VectorSearchOptions::new().with_minimum_score(between),
        )
        .unwrap();
    assert_eq!(&two.slots[..2], &plain.slots[..2]);
    assert_eq!(&two.slots[2..], &[-1, -1, -1]);
    // k = 0 is an empty, well-formed answer.
    let zero = index
        .search(&queries[..dim], 0, VectorSearchOptions::new())
        .unwrap();
    assert_eq!(zero.result_count, 0);
    assert!(zero.slots.is_empty());
    // A candidate depth widens the beam.
    let (_, narrow) = index
        .search_with_stats(&queries[..dim], 1, 1, VectorSearchOptions::new())
        .unwrap();
    let (_, wide) = index
        .search_with_stats(&queries[..dim], 1, 200, VectorSearchOptions::new())
        .unwrap();
    assert!(wide.stats.scored > narrow.stats.scored);
    let deep = index
        .search(
            &queries[..dim],
            1,
            VectorSearchOptions::new().with_candidate_depth(200),
        )
        .unwrap();
    assert_eq!(deep.slots[0], plain.slots[0]);
}

#[test]
fn masks_and_floors_filter_results_only() {
    let fx = Fixture::new(2000, 10, 16, 0x2718, BuildMode::FixedOrder);
    let allow: Vec<bool> = (0..2000).map(|i| i % 3 == 0).collect();
    masked_results_are_allowed_and_recalled(&fx, &allow);
    empty_and_misfit_masks_pad_or_refuse(&fx, &allow);
    floors_and_depth_filter_results_only(&fx);
}
