//! Connectivity repair and tombstone healing.

use std::sync::atomic::Ordering as Atomic;

use super::super::distance::Kernel;
use super::super::graph::{GraphLinks, LinkBuilder, Store};
use super::super::merge::repair_connectivity;
use super::*;
use crate::vector::Mask;

/// `2 * half` unit rows in two clusters, A near `+e_0` and B near `-e_0`,
/// with sixteen queries into cluster B.
fn two_clusters(dim: usize, half: usize) -> (Vec<f32>, Vec<f32>) {
    let total = half * 2;
    let mut vectors = vec![0.0f32; total * dim];
    // Cluster A centered near +e_0, Cluster B centered near -e_0.
    let (noise, queries_b) = corpus_with_queries(total, 16, dim, 0x99A1_0001);
    for i in 0..total {
        let sign = if i < half { 1.0f32 } else { -1.0f32 };
        let row = &mut vectors[i * dim..(i + 1) * dim];
        row.copy_from_slice(&noise[i * dim..(i + 1) * dim]);
        row[0] = sign * 4.0 + row[0] * 0.25;
        let norm: f32 = row.iter().map(|v| v * v).sum::<f32>().sqrt();
        for x in row.iter_mut() {
            *x /= norm;
        }
    }
    let mut cluster_b_queries = vec![0.0f32; 16 * dim];
    for q in 0..16 {
        let dst = &mut cluster_b_queries[q * dim..(q + 1) * dim];
        dst.copy_from_slice(&queries_b[q * dim..(q + 1) * dim]);
        dst[0] = -4.0 + dst[0] * 0.25;
        let norm: f32 = dst.iter().map(|v| v * v).sum::<f32>().sqrt();
        for x in dst.iter_mut() {
            *x /= norm;
        }
    }
    (vectors, cluster_b_queries)
}

/// Drop every edge between `0..half` and `half..`, at layer 0 and above.
fn sever_cross_cluster_links(links: &GraphLinks, half: usize, m: usize) {
    let total = links.level.len();
    let stride0 = 2 * m + 1;
    let stride_up = m + 1;
    for u in 0..total {
        let cluster_a = u < half;
        let s0 = u * stride0;
        let c0 = links.links0[s0].load(Atomic::Relaxed) as usize;
        let mut kept0 = Vec::new();
        for i in 0..c0 {
            let v = links.links0[s0 + 1 + i].load(Atomic::Relaxed) as usize;
            if (v < half) == cluster_a {
                kept0.push(v as u32);
            }
        }
        links.links0[s0].store(kept0.len() as u32, Atomic::Relaxed);
        for (i, &v) in kept0.iter().enumerate() {
            links.links0[s0 + 1 + i].store(v, Atomic::Relaxed);
        }

        let lvl = links.level[u] as usize;
        if lvl > 0 {
            let slot = links.upper_slot[u] as usize;
            let base = links.upper_base[slot] as usize;
            for layer in 1..=lvl {
                let st = base + (layer - 1) * stride_up;
                let cu = links.upper[st].load(Atomic::Relaxed) as usize;
                let mut keptu = Vec::new();
                for i in 0..cu {
                    let v = links.upper[st + 1 + i].load(Atomic::Relaxed) as usize;
                    if (v < half) == cluster_a {
                        keptu.push(v as u32);
                    }
                }
                links.upper[st].store(keptu.len() as u32, Atomic::Relaxed);
                for (i, &v) in keptu.iter().enumerate() {
                    links.upper[st + 1 + i].store(v, Atomic::Relaxed);
                }
            }
        }
    }
}

/// Build links, then sever every cross-cluster edge so `half..` is an
/// unreachable island from an entry in `0..half`.
fn severed_index(vectors: &[f32], dim: usize, half: usize, cfg: HnswConfig) -> HnswIndex {
    let mut store = Store::new(
        dim,
        cfg.m as usize,
        cfg.ef_construction as usize,
        cfg.seed,
        Kernel::detect(),
    );
    store.append_unlinked(vectors).unwrap();
    let links = store.build_links(false).unwrap();
    sever_cross_cluster_links(&links, half, cfg.m as usize);
    store.install_links(links);
    let severed = HnswIndex {
        dim,
        config: cfg,
        kernel: Kernel::detect(),
        backing: super::super::Backing::Heap(store),
    };
    assert!(
        severed.unreachable_from_entry() >= half,
        "expected severed island >= {half} unreachable nodes, got {}",
        severed.unreachable_from_entry()
    );
    severed
}

fn direct_repair_reconnects_the_island(severed: &HnswIndex) {
    // Ensure `repair_connectivity` also runs directly on an un-tombstoned severed store.
    if let super::super::Backing::Heap(st) = &severed.backing {
        let links_ref = st.links().unwrap();
        let builder = LinkBuilder::new(st, links_ref);
        let repaired = repair_connectivity(&builder);
        assert!(
            repaired >= 1,
            "expected >= 1 island repaired, got {repaired}"
        );
    }
    assert_eq!(severed.unreachable_from_entry(), 0);
}

fn healing_reconnects_with_and_without_the_entry(
    severed: &HnswIndex,
    live_flags: &[bool],
    orig_entry: usize,
) -> (HnswIndex, Vec<u32>) {
    // 1. With `orig_entry` kept alive, Cluster B has zero incoming edges from
    //    Cluster A and must be repaired by `repair_connectivity` (`islands_repaired >= 1`).
    let mut live_keep_entry = live_flags.to_vec();
    live_keep_entry[orig_entry] = true;
    let (healed_keep, _, stats_keep) = severed
        .heal_tombstones(Mask::Bools(&live_keep_entry))
        .unwrap();
    assert!(stats_keep.islands_repaired >= 1);
    assert_eq!(healed_keep.unreachable_from_entry(), 0);

    // 2. With `orig_entry` deleted (`live_flags[orig_entry] == false`),
    //    `heal_tombstones` promotes a surviving entry, inherits highways, and
    //    leaves zero unreachable nodes.
    let (healed, survivors, _stats) = severed.heal_tombstones(Mask::Bools(live_flags)).unwrap();
    assert!(!survivors.contains(&(orig_entry as u32)));
    assert_eq!(healed.len(), survivors.len());
    assert_eq!(healed.unreachable_from_entry(), 0);
    (healed, survivors)
}

fn the_healed_image_recalls_the_repaired_cluster(
    healed: &HnswIndex,
    survivors: &[u32],
    vectors: &[f32],
    cluster_b_queries: &[f32],
    dim: usize,
) {
    // Persist and reopen through `load_mapped` to exercise full PMHNSW01 structural validation.
    let dir = temp_dir("forced-island-heal");
    let img = dir.join("healed.hnsw");
    healed.write(&img).unwrap();
    let mapped = HnswIndex::load_mapped(&img).unwrap();
    assert_eq!(mapped.unreachable_from_entry(), 0);

    // Queries into Cluster B (the severed island) must achieve high live-only recall.
    let mut survivor_vectors = Vec::with_capacity(survivors.len() * dim);
    for &old in survivors {
        let at = old as usize * dim;
        survivor_vectors.extend_from_slice(&vectors[at..at + dim]);
    }
    let res = mapped
        .search(
            cluster_b_queries,
            10,
            VectorSearchOptions::new().with_candidate_depth(64),
        )
        .unwrap();
    let mut hits_ok = 0usize;
    let mut expected_total = 0usize;
    for q_idx in 0..16 {
        let q = &cluster_b_queries[q_idx * dim..(q_idx + 1) * dim];
        let truth = exact_top(&survivor_vectors, dim, q, 10);
        expected_total += truth.len();
        let got = res.slots_for_query(q_idx);
        hits_ok += truth
            .iter()
            .filter(|&&id| got.contains(&i64::from(id)))
            .count();
    }
    drop(mapped);
    let _ = std::fs::remove_dir_all(&dir);
    let recall = hits_ok as f64 / expected_total as f64;
    assert!(
        recall >= 0.95,
        "expected >= 0.95 recall on repaired island cluster, got {recall:.4}"
    );
}

#[test]
fn forced_island_connectivity_repair_and_deleted_entry_tombstone_healing() {
    let dim = 24;
    let half = 300usize;
    let total = half * 2;
    let (vectors, cluster_b_queries) = two_clusters(dim, half);
    let cfg = HnswConfig {
        m: 12,
        ef_construction: 80,
        seed: 0x7699,
        build_mode: BuildMode::FixedOrder,
        locality_order: super::super::config::LocalityOrder::None,
    };
    let severed = severed_index(&vectors, dim, half, cfg);
    // Repair via `heal_tombstones` while ALSO deleting the current entry node
    // and 20% of rows across both clusters.
    let orig_entry = match &severed.backing {
        super::super::Backing::Heap(st) => st.entry_state().0 as usize,
        super::super::Backing::Mapped(_) => unreachable!(),
    };
    let mut live_flags: Vec<bool> = (0..total).map(|i| i % 5 != 2).collect();
    live_flags[orig_entry] = false;
    direct_repair_reconnects_the_island(&severed);

    // Re-sever cross-cluster links across both layer 0 and upper layers.
    if let super::super::Backing::Heap(st) = &severed.backing {
        sever_cross_cluster_links(st.links().unwrap(), half, cfg.m as usize);
    }
    assert!(severed.unreachable_from_entry() >= half);
    let (healed, survivors) =
        healing_reconnects_with_and_without_the_entry(&severed, &live_flags, orig_entry);
    the_healed_image_recalls_the_repaired_cluster(
        &healed,
        &survivors,
        &vectors,
        &cluster_b_queries,
        dim,
    );
}

fn online_masked_search_keeps_live_only_recall(fx: &Fixture, live: &[bool], tag: &str) {
    let (dim, vectors, queries, index) = (fx.dim, &fx.vectors[..], &fx.queries[..], &fx.index);
    // 1. Online masked search never returns deleted rows and maintains live-only recall.
    let online = index
        .search(
            queries,
            10,
            VectorSearchOptions::new()
                .with_allowlist(live)
                .with_candidate_depth(80),
        )
        .unwrap();
    let mut online_hits = 0usize;
    let mut truth_total = 0usize;
    for q_idx in 0..24 {
        let q = &queries[q_idx * dim..(q_idx + 1) * dim];
        let truth = exact_top_masked(vectors, dim, q, live, 10);
        truth_total += truth.len();
        for &slot in online.slots_for_query(q_idx) {
            assert!(
                slot >= 0 && live[slot as usize],
                "{tag}: returned deleted slot {slot}"
            );
        }
        online_hits += truth
            .iter()
            .filter(|&&(id, _)| online.slots_for_query(q_idx).contains(&i64::from(id)))
            .count();
    }
    let online_recall = online_hits as f64 / truth_total as f64;
    assert!(
        online_recall >= 0.95,
        "{tag}: online recall {online_recall:.4}"
    );
}

fn the_healed_graph_matches_a_rebuild(fx: &Fixture, cfg: HnswConfig, live: &[bool], tag: &str) {
    let (dim, vectors, queries, index) = (fx.dim, &fx.vectors[..], &fx.queries[..], &fx.index);
    // 2. Healed compacted graph matches rebuild recall within 0.005 and preserves transcripts.
    let (healed, survivors, stats) = index.heal_tombstones(Mask::Bools(live)).unwrap();
    assert_eq!(healed.unreachable_from_entry(), 0);
    assert!(stats.donor_healed_lists > 0);

    let mut survivor_flat = Vec::with_capacity(survivors.len() * dim);
    for &old in &survivors {
        let at = old as usize * dim;
        survivor_flat.extend_from_slice(&vectors[at..at + dim]);
    }
    let mut rebuilt = HnswIndex::new(dim, cfg).unwrap();
    rebuilt.add(&survivor_flat, dim).unwrap();
    rebuilt.prepare().unwrap();

    assert_eq!(healed.row_identity(), rebuilt.row_identity());
    let mut healed_tx = Vec::new();
    let mut rebuilt_tx = Vec::new();
    healed
        .row_transcripts(0..healed.len(), &mut |r, b| healed_tx.push((r, b.to_vec())))
        .unwrap();
    rebuilt
        .row_transcripts(0..rebuilt.len(), &mut |r, b| {
            rebuilt_tx.push((r, b.to_vec()))
        })
        .unwrap();
    assert_eq!(healed_tx, rebuilt_tx);

    let healed_res = healed
        .search(
            queries,
            10,
            VectorSearchOptions::new().with_candidate_depth(80),
        )
        .unwrap();
    let rebuilt_res = rebuilt
        .search(
            queries,
            10,
            VectorSearchOptions::new().with_candidate_depth(80),
        )
        .unwrap();
    let mut healed_hits = 0usize;
    let mut rebuilt_hits = 0usize;
    for q_idx in 0..24 {
        let q = &queries[q_idx * dim..(q_idx + 1) * dim];
        let truth = exact_top(&survivor_flat, dim, q, 10);
        healed_hits += truth
            .iter()
            .filter(|&&id| healed_res.slots_for_query(q_idx).contains(&i64::from(id)))
            .count();
        rebuilt_hits += truth
            .iter()
            .filter(|&&id| rebuilt_res.slots_for_query(q_idx).contains(&i64::from(id)))
            .count();
    }
    let healed_recall = healed_hits as f64 / (24.0 * 10.0);
    let rebuilt_recall = rebuilt_hits as f64 / (24.0 * 10.0);
    assert!(
        healed_recall + 0.005 >= rebuilt_recall,
        "{tag}: healed recall {healed_recall:.4} vs rebuilt {rebuilt_recall:.4}"
    );
}

#[test]
fn tombstone_10pct_and_30pct_live_only_recall_and_transcript_identity() {
    let dim = 32;
    let n = 1200usize;
    let cfg = HnswConfig {
        m: 16,
        ef_construction: 120,
        seed: 42,
        build_mode: BuildMode::FixedOrder,
        locality_order: super::super::config::LocalityOrder::None,
    };
    let fx = Fixture::with_config(n, 24, dim, 0x7610_3000, cfg);
    for (tag, mod_step, drop_rem) in [("10pct", 10usize, 1usize), ("30pct", 10usize, 3usize)] {
        let mut live: Vec<bool> = (0..n).map(|i| (i % mod_step) >= drop_rem).collect();
        // Target node 0 and high-degree early hubs as well.
        live[0] = false;
        live[1] = false;
        online_masked_search_keeps_live_only_recall(&fx, &live, tag);
        the_healed_graph_matches_a_rebuild(&fx, cfg, &live, tag);
    }
}
