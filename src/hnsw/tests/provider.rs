//! The provider surface: dispatch by kind, query validation, exact scores,
//! row identity, and the full beam.

use super::*;

#[test]
fn vectors_of_varied_length_keep_recall_under_inner_product() {
    let dim = 32;
    let (mut vectors, queries) = corpus_with_queries(4000, 100, dim, 0x8080);
    let mut rng = Rng(0x8081);
    for row in vectors.chunks_exact_mut(dim) {
        let scale = (0.25 + 3.75 * rng.unit()) as f32; // lengths in 0.25..4
        for x in row {
            *x *= scale;
        }
    }
    for mode in [BuildMode::FixedOrder, BuildMode::Parallel] {
        let index = built(&vectors, dim, mode);
        let r = recall(&index, &vectors, &queries, dim, 10, 128);
        assert!(
            r >= 0.9,
            "{mode:?}: recall@10 {r} with varied lengths at ef 128"
        );
    }
    // Zero rows are tolerated both before and after graph build: they score 0
    // against everything.
    let mut index = HnswIndex::new(dim, config(BuildMode::FixedOrder)).unwrap();
    let mut with_zero = vectors[..300 * dim].to_vec();
    with_zero.extend(std::iter::repeat_n(0.0, dim));
    index.add(&with_zero, dim).unwrap();
    assert_eq!(index.len(), 301);
    assert!(!index.is_graph_linked());
    let results = index
        .search(
            &queries[..dim],
            301,
            VectorSearchOptions::new().with_candidate_depth(400),
        )
        .unwrap();
    assert!(results.slots.contains(&300));
    index.prepare().unwrap();
    assert!(index.is_graph_linked());
    let results = index
        .search(
            &queries[..dim],
            301,
            VectorSearchOptions::new().with_candidate_depth(400),
        )
        .unwrap();
    assert!(results.slots.contains(&300));
}

#[test]
fn provider_dispatches_by_kind_and_advertises_configured_ann() {
    let dim = 16;
    let mut index = VectorIndex::create(HNSW, dim, 4).unwrap();
    let descriptor = index.descriptor();
    assert_eq!(descriptor.backend_kind, HNSW);
    assert_eq!(descriptor.quality_contract, QualityContract::ConfiguredAnn);
    assert_eq!(descriptor.metric, "inner_product");
    assert_eq!(descriptor.bits_per_dimension, Some(32));
    assert_eq!(descriptor.dimension, Some(dim));
    assert!(!descriptor
        .capabilities
        .contains(&VectorCapability::ExhaustiveCompletion));
    assert!(descriptor
        .capabilities
        .contains(&VectorCapability::RawVectorRebuild));
    assert!(descriptor
        .capabilities
        .contains(&VectorCapability::DenseMask));
    assert!(!descriptor.scoring_fingerprint.is_empty());
    assert!(!index.is_mapped());

    let config = index.backend_config().unwrap();
    assert_eq!(config.backend_kind, HNSW);
    assert_eq!(
        HnswConfig::from_backend_config(&config).unwrap(),
        HnswConfig::default()
    );
    let rebuilt = VectorIndex::from_backend_config(dim, &config).unwrap();
    assert_eq!(rebuilt.descriptor(), descriptor);
    let fitted = VectorIndex::fit_backend_config(HNSW, dim, 4, &[0.0; 64]).unwrap();
    assert_eq!(fitted, config);
    assert!(VectorIndex::from_backend_config(
        dim,
        &VectorBackendConfig {
            payload: vec![1, 2, 3, 4, 5],
            ..config
        }
    )
    .is_err());

    let vectors = corpus(500, dim, 0x0F0F);
    index.add(&vectors, dim).unwrap();
    index.prepare().unwrap();
    assert_eq!(index.len(), 500);
    let dir = temp_dir("dispatch");
    let path = dir.join("image");
    index.write(&path).unwrap();
    let loaded = VectorIndex::load(HNSW, &path).unwrap();
    let mapped = VectorIndex::load_mapped(HNSW, &path).unwrap();
    assert!(!loaded.is_mapped() && mapped.is_mapped());
    assert_eq!(loaded.len(), 500);
    let results = mapped.search(&vectors[..dim], 1, VectorSearchOptions::new());
    assert_eq!(results.slots, vec![0]);
    assert_eq!(
        mapped.row_transcript(3).unwrap(),
        index.row_transcript(3).unwrap()
    );
    assert!(VectorIndex::load(EMBEDDED_TURBOVEC, &path).is_err());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn query_validation_refuses_malformed_input() {
    let dim = 8;
    let index = built(&corpus(100, dim, 1), dim, BuildMode::FixedOrder);
    assert!(index
        .search(&[1.0; 9], 1, VectorSearchOptions::new())
        .is_err());
    assert!(index
        .search(&[f32::INFINITY; 8], 1, VectorSearchOptions::new())
        .is_err());
    assert!(index
        .search_with_stats(&[1.0; 16], 1, 16, VectorSearchOptions::new())
        .is_err());
    let depth_zero = index
        .search_streaming_controlled(
            &[1.0; 8],
            VectorSearchOptions::new().with_candidate_depth(0),
            &mut |_| VectorStreamControl::Continue,
            &mut || VectorStreamControl::Continue,
        )
        .unwrap_err();
    assert!(depth_zero.to_string().contains("depth 0"));
}

/// Rows of varied length and sign at dimensions that straddle every kernel
/// lane width: every score the provider reports, unary or streamed, is bit
/// for bit `exact_vectors::dot` of the stored row and the query, on
/// whichever kernel this machine runs. Equal scores order by row.
#[test]
fn reported_scores_are_bit_identical_to_the_scalar_scorer_and_ties_order_by_row() {
    for &dim in &[17usize, 65] {
        let mut rng = Rng(0xB175 + dim as u64);
        let rows = 1200;
        let mut vectors: Vec<f32> = (0..rows * dim)
            .map(|_| rng.normal() * (1.0 + 3.0 * rng.unit() as f32))
            .collect();
        // Exact duplicates: rows 40..60 repeat rows 20..40, so the top of
        // every result holds ties that only the row order can break.
        let dup: Vec<f32> = vectors[20 * dim..40 * dim].to_vec();
        vectors[40 * dim..60 * dim].copy_from_slice(&dup);
        let queries: Vec<f32> = (0..4 * dim).map(|_| rng.normal()).collect();
        let index = built(&vectors, dim, BuildMode::Parallel);
        let exact = |row: i64, query: &[f32]| {
            crate::exact_vectors::dot(&vectors[row as usize * dim..][..dim], query)
        };
        let results = index
            .search(
                &queries,
                50,
                VectorSearchOptions::new().with_candidate_depth(128),
            )
            .unwrap();
        for q in 0..4 {
            let query = &queries[q * dim..(q + 1) * dim];
            let slots = results.slots_for_query(q);
            let scores = results.scores_for_query(q);
            for (&slot, &score) in slots.iter().zip(scores) {
                assert!(slot >= 0, "dim {dim} query {q} came back short");
                assert_eq!(
                    score.to_bits(),
                    exact(slot, query).to_bits(),
                    "dim {dim} query {q} row {slot}: {score} is not the scalar score"
                );
            }
            for w in slots.windows(2).zip(scores.windows(2)) {
                let ((a, b), (sa, sb)) = ((w.0[0], w.0[1]), (w.1[0], w.1[1]));
                assert!(
                    sa > sb || (sa == sb && a < b),
                    "dim {dim} query {q}: ({a}, {sa}) before ({b}, {sb})"
                );
            }
        }
        let mut streamed: Vec<(usize, i64, f32)> = Vec::new();
        let summary = index
            .search_streaming_controlled(
                &queries,
                VectorSearchOptions::new().with_candidate_depth(128),
                &mut |batch| {
                    for w in batch.slots.windows(2).zip(batch.scores.windows(2)) {
                        let ((a, b), (sa, sb)) = ((w.0[0], w.0[1]), (w.1[0], w.1[1]));
                        assert!(sa > sb || (sa == sb && a < b), "batch order");
                    }
                    for (&slot, &score) in batch.slots.iter().zip(batch.scores) {
                        streamed.push((batch.query_index, slot, score));
                    }
                    VectorStreamControl::Continue
                },
                &mut || VectorStreamControl::Continue,
            )
            .unwrap();
        assert!(summary.completed && !streamed.is_empty());
        for &(q, slot, score) in &streamed {
            let query = &queries[q * dim..(q + 1) * dim];
            assert_eq!(score.to_bits(), exact(slot, query).to_bits());
        }
    }
}

/// The rows behind the graph carry the identity a shared store must
/// match: the same dimension, count and content digest whether the index is
/// on the heap, loaded from its image or served from the map, and the digest
/// is the one the `.exact` sidecar keeps over the same rows.
#[test]
fn row_source_identity_is_stable_and_matches_the_exact_sidecar() {
    let dim = 24;
    let vectors = corpus(700, dim, 0x1D);
    let index = built(&vectors, dim, BuildMode::FixedOrder);
    let identity = index.row_identity();
    assert_eq!((identity.dim, identity.rows), (dim, 700));
    index.with_row_source(|rows| {
        assert_eq!(rows.row(3), &vectors[3 * dim..4 * dim]);
        assert_eq!(rows.values(), &vectors[..]);
    });
    let dir = temp_dir("rows");
    let image = dir.join("image");
    index.write(&image).unwrap();
    assert_eq!(HnswIndex::load(&image).unwrap().row_identity(), identity);
    assert_eq!(
        HnswIndex::load_mapped(&image).unwrap().row_identity(),
        identity
    );
    let exact_path = dir.join("rows.exact");
    crate::exact_vectors::ExactVectorStore::from_values(dim, vectors.clone())
        .unwrap()
        .write(&exact_path)
        .unwrap();
    let sidecar = crate::exact_vectors::ExactVectorStore::open(&exact_path).unwrap();
    assert_eq!(sidecar.payload_sha256(), Some(identity.content_digest));
    // Different rows, different digest.
    let other = built(&corpus(700, dim, 0x1E), dim, BuildMode::FixedOrder).row_identity();
    assert_ne!(other.content_digest, identity.content_digest);
    std::fs::remove_dir_all(&dir).unwrap();
}

/// A beam as wide as the graph reaches every row: the unary result is the
/// whole corpus in exact score order, and the stream emits every row once.
/// A hop that finds no unvisited neighbor does not end the walk early.
#[test]
fn a_full_beam_visits_every_reachable_row() {
    let dim = 12;
    let n = 900;
    let (vectors, queries) = corpus_with_queries(n, 2, dim, 0xF011);
    for mode in [BuildMode::FixedOrder, BuildMode::Parallel] {
        let index = built(&vectors, dim, mode);
        for query in queries.chunks_exact(dim) {
            let (hits, stats) = index
                .search_with_stats(query, n, n, VectorSearchOptions::new())
                .unwrap();
            assert_eq!(hits.len(), n, "{mode:?}: not every row was reached");
            // Layer 0 scores every row once; the descent above it scores a few more.
            assert!(stats.stats.scored as usize >= n);
            let truth = exact_top(&vectors, dim, query, n);
            let got: Vec<u32> = hits.iter().map(|h| h.0).collect();
            assert_eq!(got, truth, "{mode:?}: full-beam order is the exact order");
            let mut emitted: Vec<i64> = Vec::new();
            let summary = index
                .search_streaming_controlled(
                    query,
                    VectorSearchOptions::new().with_candidate_depth(n),
                    &mut |batch| {
                        emitted.extend_from_slice(batch.slots);
                        VectorStreamControl::Continue
                    },
                    &mut || VectorStreamControl::Continue,
                )
                .unwrap();
            assert!(summary.completed);
            emitted.sort_unstable();
            assert_eq!(emitted, (0..n as i64).collect::<Vec<_>>(), "{mode:?}");
        }
    }
}
