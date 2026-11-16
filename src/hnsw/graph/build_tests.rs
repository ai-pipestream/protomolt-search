//! Unit tests for parallel cold-start skeleton bootstrap, doubling waves,
//! slice connectivity repair, and `Probe::local_share`.

use std::sync::atomic::{AtomicU32, Ordering as Atomic};

use crate::boolean_bits::Bits;
use crate::harness::unit_vectors;
use crate::hnsw::config::{BuildMode, HnswConfig};
use crate::hnsw::{HnswIndex, EF_FLOOR};
use crate::vector::{VectorProvider, VectorSearchOptions};

use super::super::distance::Kernel;
use super::probe::Probe;
use super::{GraphLinks, LinkBuilder, Store, NO_NODE};

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

fn reachable_layer0_count(store: &Store, links: &GraphLinks) -> usize {
    let (entry, _) = links.entry_state();
    if entry == NO_NODE {
        return 0;
    }
    let builder = LinkBuilder::new(store, links);
    let view = builder.view();
    let mut visited = Bits::empty(store.n());
    let mut queue = vec![entry];
    visited.set(entry as usize);
    let mut nbrs = Vec::with_capacity(store.m0);
    let mut head = 0usize;
    while head < queue.len() {
        let u = queue[head];
        head += 1;
        view.copy_links(u, 0, None, &mut nbrs);
        for &v in &nbrs {
            if !visited.test(v as usize) {
                visited.set(v as usize);
                queue.push(v);
            }
        }
    }
    visited.count() as usize
}

#[test]
fn probe_local_share_guards_zero_sample() {
    let empty = Probe {
        sample: 0,
        hits: 0,
        share: 0.5,
    };
    assert_eq!(empty.local_share(), 0.0);
    assert!(!empty.local_share().is_nan());

    let populated = Probe {
        sample: 8,
        hits: 2,
        share: 0.5,
    };
    assert_eq!(populated.local_share(), 0.25);
}

#[test]
fn bootstrap_prefix_and_partial_parallel_slice_keep_populated_nodes_reachable() {
    let dim = 16;
    let store_rows = 600;
    let vectors = unit_vectors(store_rows, dim, 0x4A5E_1001);
    let mut store = Store::new(dim, 8, 32, 7, Kernel::detect());
    store.append_unlinked(&vectors).unwrap();

    let stride0 = store.m0 + 1;
    let links = GraphLinks::from_parts(
        vec![0; store_rows],
        vec![NO_NODE; store_rows],
        Vec::new(),
        (0..store_rows * stride0)
            .map(|_| AtomicU32::new(0))
            .collect(),
        Vec::new(),
        NO_NODE,
        0,
    );
    let builder = LinkBuilder::new(&store, &links);
    assert_eq!(builder.bootstrap_prefix_len(store_rows), 32);
    assert_eq!(builder.bootstrap_prefix_len(20), 20);

    let first_half: Vec<u32> = (0..320).collect();
    builder.insert_slice(&first_half, true);
    assert_ne!(links.entry_state().0, NO_NODE);
    assert_eq!(builder.bootstrap_prefix_len(store_rows), 0);
    assert_eq!(reachable_layer0_count(&store, &links), 320);
    for u in 320..store_rows {
        assert_eq!(links.links0[u * stride0].load(Atomic::Relaxed), 0);
    }

    let second_half: Vec<u32> = (320..store_rows as u32).collect();
    builder.insert_slice(&second_half, true);
    assert_eq!(reachable_layer0_count(&store, &links), store_rows);
}

#[test]
fn high_contention_small_m_parallel_build_leaves_zero_unreachable_nodes() {
    const DIM: usize = 24;
    const ROWS: usize = 1_200;
    let corpus = unit_vectors(ROWS, DIM, 0x4A5E_7711);

    for seed in 0..8u64 {
        let mut index = HnswIndex::new(
            DIM,
            HnswConfig {
                m: 4,
                ef_construction: 24,
                seed: 100 + seed,
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
            "seed {seed}: small-M parallel build left unreachable layer-0 nodes"
        );
    }
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

#[test]
fn parallel_waves_keep_upper_layers_deterministic_under_contention() {
    const DIM: usize = 24;
    const ROWS: usize = 1_100;
    let corpus = unit_vectors(ROWS, DIM, 0x4A5E_9922);

    let mut store = Store::new(DIM, 16, 200, 42, Kernel::detect());
    store.append_unlinked(&corpus).unwrap();

    let baseline = store.build_links(true).unwrap();
    let base_entry = baseline.entry_state();
    let base_upper: Vec<u32> = baseline
        .upper
        .iter()
        .map(|c| c.load(Atomic::Relaxed))
        .collect();
    assert!(!base_upper.is_empty());

    std::thread::scope(|s| {
        for _ in 0..4 {
            let store_ref = &store;
            let want_upper = &base_upper;
            s.spawn(move || {
                for _ in 0..4 {
                    let links = store_ref.build_links(true).unwrap();
                    assert_eq!(links.entry_state(), base_entry);
                    let got: Vec<u32> = links
                        .upper
                        .iter()
                        .map(|c| c.load(Atomic::Relaxed))
                        .collect();
                    assert_eq!(&got, want_upper);
                    assert_eq!(reachable_layer0_count(store_ref, &links), ROWS);
                }
            });
        }
    });
}
