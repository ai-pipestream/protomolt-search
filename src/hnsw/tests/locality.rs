//! Locality reordering: determinism, image round trips, and metrics against
//! the unordered index.

use std::path::PathBuf;

use super::super::config::LocalityOrder;
use super::super::LocalityMetrics;
use super::locality_identity::{
    appending_unpermutes_a_reordered_index, external_identity_matches_the_unordered_index,
    healing_translates_external_masks, hostile_node_maps_are_refused,
    merging_reordered_segments_keeps_identity,
};
use super::*;

/// The unordered index every reordering is compared against.
pub(super) struct LocalityCase {
    pub(super) dim: usize,
    pub(super) n: usize,
    pub(super) qn: usize,
    pub(super) vectors: Vec<f32>,
    pub(super) queries: Vec<f32>,
    pub(super) base_cfg: HnswConfig,
    pub(super) base: HnswIndex,
    pub(super) dir: PathBuf,
    pub(super) none_mapped: HnswIndex,
    pub(super) none_metrics: LocalityMetrics,
    pub(super) none_avg_pages: f64,
    pub(super) base_tx: Vec<(usize, Vec<u8>)>,
    pub(super) sparse_mask: Vec<bool>,
    pub(super) mid_mask: Vec<bool>,
    pub(super) dense_mask: Vec<bool>,
}

/// One locality order applied seal-locally: on the heap, reloaded, and mapped.
pub(super) struct Reordered {
    pub(super) order: LocalityOrder,
    pub(super) seal_cfg: HnswConfig,
    pub(super) local_path: PathBuf,
    pub(super) local_heap: HnswIndex,
    pub(super) local_loaded: HnswIndex,
    pub(super) local_mapped: HnswIndex,
}

fn locality_case() -> LocalityCase {
    let dim = 64;
    let n = 1_200usize;
    let qn = 24usize;
    let (vectors, queries) = corpus_with_queries(n, qn, dim, 0x9800_10CA);
    let base_cfg = HnswConfig {
        m: 12,
        ef_construction: 96,
        seed: 77,
        build_mode: BuildMode::FixedOrder,
        locality_order: LocalityOrder::None,
    };
    let mut base = HnswIndex::new(dim, base_cfg).unwrap();
    base.add(&vectors, dim).unwrap();
    base.prepare().unwrap();
    assert!(!base.has_node_map());

    let dir = temp_dir("locality-reorder");
    let none_path = dir.join("none.hnsw");
    base.write(&none_path).unwrap();
    let none_bytes = std::fs::read(&none_path).unwrap();
    let raw_top = u32::from_le_bytes(none_bytes[68..72].try_into().unwrap());
    assert_eq!(
        raw_top >> 8,
        0,
        "LocalityOrder::None must leave upper 24 bits of offset 68 zero"
    );
    let none_mapped = HnswIndex::load_mapped(&none_path).unwrap();
    assert!(!none_mapped.has_node_map());

    let none_metrics = none_mapped.locality_metrics();
    let none_avg_pages: f64 = (0..qn)
        .map(|qi| none_mapped.walk_row_4k_pages(&queries[qi * dim..(qi + 1) * dim], 48) as f64)
        .sum::<f64>()
        / qn as f64;

    let mut base_tx = Vec::new();
    none_mapped
        .row_transcripts(0..n, &mut |r, b| base_tx.push((r, b.to_vec())))
        .unwrap();

    let sparse_mask: Vec<bool> = (0..n).map(|i| i % 25 == 3).collect();
    let mid_mask: Vec<bool> = (0..n).map(|i| i % 5 == 1).collect();
    let dense_mask: Vec<bool> = (0..n).map(|i| i % 3 != 0).collect();
    LocalityCase {
        dim,
        n,
        qn,
        vectors,
        queries,
        base_cfg,
        base,
        dir,
        none_mapped,
        none_metrics,
        none_avg_pages,
        base_tx,
        sparse_mask,
        mid_mask,
        dense_mask,
    }
}

fn the_permutation_is_thread_count_deterministic(
    c: &LocalityCase,
    order: LocalityOrder,
) -> HnswIndex {
    let base = &c.base;
    // 1. Determinism across 1 vs 4 Rayon worker threads.
    let pool1 = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .unwrap();
    let pool4 = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let (_, perm1) = pool1.install(|| base.permute_from_linked(order, true).unwrap());
    let (local_heap, perm4) = pool4.install(|| base.permute_from_linked(order, true).unwrap());
    assert_eq!(
        perm1, perm4,
        "{order:?} permutation must be thread-count deterministic"
    );
    assert!(local_heap.has_node_map());
    local_heap
}

fn local_and_global_images_reload(
    c: &LocalityCase,
    order: LocalityOrder,
    local_heap: HnswIndex,
) -> Reordered {
    let (base, dir) = (&c.base, &c.dir);
    // 2. Write + reload (both heap and mapped) for seal-local (`has_node_map = true`)
    //    and compaction-global (`has_node_map = false`).
    let local_path = dir.join(format!("{order:?}-local.hnsw"));
    local_heap.write(&local_path).unwrap();
    let local_loaded = HnswIndex::load(&local_path).unwrap();
    let local_mapped = HnswIndex::load_mapped(&local_path).unwrap();
    assert!(local_loaded.has_node_map() && local_mapped.has_node_map());
    assert_eq!(local_mapped.config().locality_order, order);
    assert_eq!(local_mapped.unreachable_from_entry(), 0);

    let (global_heap, _) = base.permute_from_linked(order, false).unwrap();
    assert!(!global_heap.has_node_map());
    let global_path = dir.join(format!("{order:?}-global.hnsw"));
    global_heap.write(&global_path).unwrap();
    let global_mapped = HnswIndex::load_mapped(&global_path).unwrap();
    assert!(!global_mapped.has_node_map());
    assert_eq!(global_mapped.config().locality_order, order);
    assert_eq!(global_mapped.unreachable_from_entry(), 0);
    let mut seal_cfg = c.base_cfg;
    seal_cfg.locality_order = order;
    Reordered {
        order,
        seal_cfg,
        local_path,
        local_heap,
        local_loaded,
        local_mapped,
    }
}

fn a_seal_write_matches_the_local_permutation(c: &LocalityCase, r: &Reordered) {
    let (dim, vectors, dir) = (c.dim, &c.vectors[..], &c.dir);
    let (order, seal_cfg, local_path) = (r.order, r.seal_cfg, &r.local_path);
    // Also verify `HnswIndex::write(&self)` on an unpermuted heap store configured
    // with `order` produces byte-identical seal output without mutating `self`.
    let mut unsealed = HnswIndex::new(dim, seal_cfg).unwrap();
    unsealed.add(vectors, dim).unwrap();
    let seal_path = dir.join(format!("{order:?}-seal.hnsw"));
    unsealed.write(&seal_path).unwrap();
    assert!(
        !unsealed.has_node_map(),
        "write(&self) must not mutate shared frozen-tail store"
    );
    assert_eq!(
        std::fs::read(&seal_path).unwrap(),
        std::fs::read(local_path).unwrap(),
        "{order:?}: seal write(&self) must match permute_from_linked(true)"
    );
}

fn locality_metrics_beat_insertion_order(c: &LocalityCase, r: &Reordered) {
    let (dim, qn, queries) = (c.dim, c.qn, &c.queries[..]);
    let (none_metrics, none_avg_pages) = (&c.none_metrics, c.none_avg_pages);
    let (order, local_mapped) = (r.order, &r.local_mapped);
    // 3. Locality metrics improve strictly over random insertion order;
    //    RecursiveBisection cuts across small-world shortcuts far more
    //    deeply than FIFO breadth-first RCM.
    let m = local_mapped.locality_metrics();
    assert_eq!(m.edges, none_metrics.edges);
    let gap_ratio = match order {
        LocalityOrder::Rcm => 0.90,
        LocalityOrder::RecursiveBisection => 0.65,
        LocalityOrder::None => 1.0,
    };
    assert!(
        m.mean_id_gap < none_metrics.mean_id_gap * gap_ratio,
        "{order:?}: mean_id_gap {:.1} did not beat None {:.1} * {gap_ratio}",
        m.mean_id_gap,
        none_metrics.mean_id_gap
    );
    assert!(
        m.same_4k_row_page_frac > none_metrics.same_4k_row_page_frac * 1.5,
        "{order:?}: same_4k_row_page_frac {:.4} did not beat None {:.4}",
        m.same_4k_row_page_frac,
        none_metrics.same_4k_row_page_frac
    );
    let avg_pages: f64 = (0..qn)
        .map(|qi| local_mapped.walk_row_4k_pages(&queries[qi * dim..(qi + 1) * dim], 48) as f64)
        .sum::<f64>()
        / qn as f64;
    assert!(
        avg_pages < none_avg_pages,
        "{order:?}: avg 4KB pages {avg_pages:.2} did not beat None {none_avg_pages:.2}"
    );
}

#[test]
fn locality_reordering_preserves_exact_identity_transcripts_regimes_and_refuses_bad_maps() {
    let c = locality_case();
    for order in [LocalityOrder::Rcm, LocalityOrder::RecursiveBisection] {
        let local_heap = the_permutation_is_thread_count_deterministic(&c, order);
        let r = local_and_global_images_reload(&c, order, local_heap);
        a_seal_write_matches_the_local_permutation(&c, &r);
        locality_metrics_beat_insertion_order(&c, &r);
        external_identity_matches_the_unordered_index(&c, &r);
        appending_unpermutes_a_reordered_index(&c, order);
        healing_translates_external_masks(&c, &r);
        merging_reordered_segments_keeps_identity(&c, &r);
    }
    hostile_node_maps_are_refused(&c);

    let _ = std::fs::remove_dir_all(&c.dir);
}
