//! Unit tests for parallel cold-start skeleton bootstrap and slice insertion.

use crate::harness::unit_vectors;
use crate::hnsw::config::{BuildMode, HnswConfig};
use crate::hnsw::{HnswIndex, EF_FLOOR};
use crate::vector::{VectorProvider, VectorSearchOptions};

use super::super::distance::Kernel;
use super::build::{GraphLinks, LinkBuilder, Store};
use super::NO_NODE;

fn exact_top_k(corpus: &[f32], dim: usize, query: &[f32], k: usize) -> Vec<u32> {
    let mut ranked: Vec<(u32, f32)> = corpus
        .chunks_exact(dim)
        .enumerate()
        .map(|(row, vec)| {
            let score: f32 = vec.iter().zip(query).map(|(a, b)| a * b).sum();
            (row as u32, score)
        })
        .collect();
    ranked.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    ranked.truncate(k);
    ranked.into_iter().map(|(row, _)| row).collect()
}

#[test]
fn bootstrap_prefix_runs_only_while_graph_has_no_entry() {
    let dim = 16;
    let store_rows = 600;
    let vectors = unit_vectors(store_rows, dim, 0x4A5E_1001);
    let mut store = Store::new(dim, 16, 200, 7, Kernel::detect());
    store.append_unlinked(&vectors).unwrap();

    let empty_links = GraphLinks::from_parts(
        vec![0; store_rows],
        vec![NO_NODE; store_rows],
        Vec::new(),
        (0..store_rows * 33)
            .map(|_| std::sync::atomic::AtomicU32::new(0))
            .collect(),
        Vec::new(),
        NO_NODE,
        0,
    );
    let builder = LinkBuilder::new(&store, &empty_links);
    assert_eq!(builder.bootstrap_prefix_len(store_rows), 200);
    assert_eq!(builder.bootstrap_prefix_len(120), 120);

    let nodes: Vec<u32> = (0..320).collect();
    builder.insert_slice(&nodes, true);
    assert_ne!(empty_links.entry_state().0, NO_NODE);
    assert_eq!(builder.bootstrap_prefix_len(store_rows), 0);
}

#[test]
fn parallel_cold_start_keeps_reachability_and_recall_across_repeated_builds() {
    const DIM: usize = 32;
    const SHARD_ROWS: usize = 1_100;
    const K: usize = 5;

    let corpus = unit_vectors(SHARD_ROWS, DIM, 0x4A5E_0001);
    let queries = unit_vectors(8, DIM, 0x4A5E_0003);
    let truths: Vec<Vec<u32>> = queries
        .as_chunks::<DIM>()
        .0
        .iter()
        .map(|q| exact_top_k(&corpus, DIM, q, K))
        .collect();

    for iteration in 0..16 {
        let mut index = HnswIndex::new(
            DIM,
            HnswConfig {
                build_mode: BuildMode::Parallel,
                ..HnswConfig::default()
            },
        )
        .unwrap();
        index.add(&corpus, DIM).unwrap();
        index.prepare().unwrap();
        assert_eq!(
            index.unreachable_from_entry(),
            0,
            "iteration {iteration}: parallel cold-start left unreachable layer-0 nodes"
        );

        let mut hits_found = 0usize;
        for (query, truth) in queries.as_chunks::<DIM>().0.iter().zip(&truths) {
            let (hits, _) = index
                .search_with_stats(query, K, EF_FLOOR, VectorSearchOptions::new())
                .unwrap();
            hits_found += hits.iter().filter(|(row, _)| truth.contains(row)).count();
        }
        assert!(
            hits_found >= 36,
            "iteration {iteration}: recall@5 {hits_found}/40 fell below floor"
        );
    }
}
