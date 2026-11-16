//! The join-set merge of the non-donor rows: a greedy `K`-dominating join
//! set per linked segment is inserted by beam search, and the remaining
//! layer-0 rows are grafted from their neighborhoods.

use std::cmp::Reverse;

use crate::vector::VectorProvider;

use super::super::graph::{LinkBuilder, LinkCells, View, NO_NODE};
use super::assemble::MergePlan;
use super::graft::{graft_non_join_nodes, JoinSnapshot};
use super::{MergeStats, JOIN_DOMINATION_K};

/// How the surviving rows of one non-donor linked segment enter the output.
struct NonDonorPlan {
    /// Output nodes inserted by full beam search.
    join_nodes: Vec<u32>,
    /// `(output node, its surviving intra-segment neighbors)` to graft.
    graft_tasks: Vec<(u32, Vec<u32>)>,
    /// Output nodes with no surviving or no join-set neighbor: inserted by
    /// full beam search after the join set.
    fallback_insert_nodes: Vec<u32>,
}

/// Interleave `lists` round-robin into a vector with room for `capacity`
/// items.
fn round_robin<T: Clone>(lists: &[Vec<T>], capacity: usize) -> Vec<T> {
    let mut out = Vec::with_capacity(capacity);
    let longest = lists.iter().map(Vec::len).max().unwrap_or(0);
    for idx in 0..longest {
        for list in lists {
            if let Some(item) = list.get(idx) {
                out.push(item.clone());
            }
        }
    }
    out
}

/// Link every non-donor row: insert the join set of each qualifying segment
/// (interleaved round-robin across segments, largest first, so early
/// insertions span every segment), then the unlinked and fallback rows, and
/// graft the rest.
pub(super) fn join_set_merge(
    builder: &LinkBuilder<'_>,
    plan: &MergePlan<'_>,
    stats: &mut MergeStats,
) {
    let sources = plan.sources;
    let mut per_seg_pairs: Vec<Vec<(u32, u32)>> = vec![Vec::new(); sources.len()];
    let mut unlinked_nodes: Vec<u32> = Vec::new();
    for (new_u, source) in plan.row_sources.iter().enumerate() {
        match *source {
            Some((seg, _)) if seg == plan.donor_seg => {}
            Some((seg, old_u)) if plan.qualifies[seg] => {
                per_seg_pairs[seg].push((old_u, new_u as u32));
            }
            _ => unlinked_nodes.push(new_u as u32),
        }
    }
    let mut non_donor_segs: Vec<usize> = (0..sources.len())
        .filter(|&s| s != plan.donor_seg && !per_seg_pairs[s].is_empty())
        .collect();
    non_donor_segs.sort_by_key(|&s| (Reverse(per_seg_pairs[s].len()), s));

    let mut seg_join_lists: Vec<Vec<u32>> = Vec::new();
    let mut seg_graft_lists: Vec<Vec<(u32, Vec<u32>)>> = Vec::new();
    let mut total_graft_rows = 0usize;
    let mut fallback_inserts: Vec<u32> = Vec::new();
    let out_levels = &builder.links.level;
    for &seg in &non_donor_segs {
        let pairs = &per_seg_pairs[seg];
        let seg_plan = with_view!(sources[seg], |seg_view| {
            plan_non_donor_segment(&seg_view, sources[seg].len(), pairs, out_levels)
        });
        stats.join_set_rows += seg_plan.join_nodes.len();
        total_graft_rows += seg_plan.graft_tasks.len();
        seg_join_lists.push(seg_plan.join_nodes);
        seg_graft_lists.push(seg_plan.graft_tasks);
        fallback_inserts.extend(seg_plan.fallback_insert_nodes);
    }

    let total_inserts = stats.join_set_rows + unlinked_nodes.len() + fallback_inserts.len();
    let mut all_inserts = round_robin(&seg_join_lists, total_inserts);
    let join_only_count = all_inserts.len();
    all_inserts.extend_from_slice(&unlinked_nodes);
    all_inserts.extend_from_slice(&fallback_inserts);
    stats.reinserted_rows = unlinked_nodes.len() + fallback_inserts.len();
    builder.insert_slice(&all_inserts, plan.parallel(all_inserts.len()));

    if total_graft_rows > 0 {
        let graft_tasks = round_robin(&seg_graft_lists, total_graft_rows);
        // The merged layer-0 lists of the join set, frozen before any graft
        // writes, seed every graft's 2-hop neighborhood without a race.
        let join = JoinSnapshot::of(&builder.view(), &all_inserts[..join_only_count]);
        let parallel = plan.parallel(graft_tasks.len());
        graft_non_join_nodes(builder, &graft_tasks, &join, parallel);
        stats.grafted_rows = graft_tasks.len();
    }
}

/// The surviving layer-0 neighborhood of every surviving row of a segment,
/// as indexes into `pairs`: its surviving neighbors, then the surviving
/// neighbors of its deleted ones, up to `2 · m0` in all.
fn surviving_layer0<L: LinkCells + ?Sized>(
    seg_view: &View<'_, L>,
    seg_len: usize,
    pairs: &[(u32, u32)],
) -> Vec<Vec<u32>> {
    let surv_len = pairs.len();
    let mut old_to_local = vec![NO_NODE; seg_len];
    for (local_idx, &(old_u, _)) in pairs.iter().enumerate() {
        old_to_local[old_u as usize] = local_idx as u32;
    }
    let cap_cands = seg_view.m0 * 2;
    let mut local_l0: Vec<Vec<u32>> = Vec::with_capacity(surv_len);
    let mut raw_nbrs = Vec::with_capacity(seg_view.m0);
    let mut bridge_nbrs = Vec::with_capacity(seg_view.m0);
    let mut seen_local = vec![0u32; surv_len];
    let mut epoch = 0u32;
    for (local_idx, &(old_u, _)) in pairs.iter().enumerate() {
        epoch = epoch.wrapping_add(1);
        if epoch == 0 {
            seen_local.fill(0);
            epoch = 1;
        }
        seen_local[local_idx] = epoch;
        let mut first_seen = |loc: u32| {
            let seen = &mut seen_local[loc as usize];
            let fresh = *seen != epoch;
            *seen = epoch;
            fresh
        };
        seg_view.copy_links(old_u, 0, None, &mut raw_nbrs);
        let mut nbr_locals = Vec::with_capacity(raw_nbrs.len());
        let mut deleted_1hop = Vec::new();
        for &old_v in &raw_nbrs {
            let loc = old_to_local[old_v as usize];
            if loc == NO_NODE {
                deleted_1hop.push(old_v);
            } else if first_seen(loc) {
                nbr_locals.push(loc);
            }
        }
        for del_v in deleted_1hop {
            if nbr_locals.len() >= cap_cands {
                break;
            }
            seg_view.copy_links(del_v, 0, None, &mut bridge_nbrs);
            for &old_w in &bridge_nbrs {
                if nbr_locals.len() >= cap_cands {
                    break;
                }
                let loc = old_to_local[old_w as usize];
                if loc != NO_NODE && first_seen(loc) {
                    nbr_locals.push(loc);
                }
            }
        }
        local_l0.push(nbr_locals);
    }
    local_l0
}

/// The greedy `K`-dominating join set over the neighborhoods `local_l0`:
/// every row with `level >= 1` joins unconditionally; the `level == 0` rows
/// are then visited by `(surviving degree desc, new_u asc)` and join while
/// fewer than [`JOIN_DOMINATION_K`] join-set rows cover them. Returns the
/// membership and each row's cover count.
fn dominating_join_set(
    pairs: &[(u32, u32)],
    out_levels: &[u8],
    local_l0: &[Vec<u32>],
) -> (Vec<bool>, Vec<u8>) {
    let surv_len = pairs.len();
    let mut in_join = vec![false; surv_len];
    let mut dom_count = vec![0u8; surv_len];
    let mut l0_order = Vec::with_capacity(surv_len);
    let cover = |dom_count: &mut [u8], idx: usize| {
        for &nbr_loc in &local_l0[idx] {
            let slot = &mut dom_count[nbr_loc as usize];
            *slot = slot.saturating_add(1);
        }
    };
    for (idx, &(_, new_u)) in pairs.iter().enumerate() {
        if out_levels[new_u as usize] >= 1 {
            in_join[idx] = true;
            cover(&mut dom_count, idx);
        } else {
            l0_order.push(idx as u32);
        }
    }
    l0_order.sort_unstable_by_key(|&idx| {
        let i = idx as usize;
        (Reverse(local_l0[i].len()), pairs[i].1)
    });
    for idx_u32 in l0_order {
        let idx = idx_u32 as usize;
        if dom_count[idx] < JOIN_DOMINATION_K && !local_l0[idx].is_empty() {
            in_join[idx] = true;
            dom_count[idx] = JOIN_DOMINATION_K;
            cover(&mut dom_count, idx);
        }
    }
    (in_join, dom_count)
}

/// Extract surviving layer-0 neighborhoods and choose the greedy
/// `K`-dominating join set (`level >= 1` plus greedy highest-degree cover) for
/// one non-donor linked segment of `seg_len` rows. `pairs` are its surviving
/// rows as `(source node, output node)`.
fn plan_non_donor_segment<L: LinkCells + ?Sized>(
    seg_view: &View<'_, L>,
    seg_len: usize,
    pairs: &[(u32, u32)],
    out_levels: &[u8],
) -> NonDonorPlan {
    let local_l0 = surviving_layer0(seg_view, seg_len, pairs);
    let (in_join, dom_count) = dominating_join_set(pairs, out_levels, &local_l0);
    let mut plan = NonDonorPlan {
        join_nodes: Vec::new(),
        graft_tasks: Vec::new(),
        fallback_insert_nodes: Vec::new(),
    };
    for (idx, &(_, new_u)) in pairs.iter().enumerate() {
        if in_join[idx] {
            plan.join_nodes.push(new_u);
        } else if local_l0[idx].is_empty() || dom_count[idx] == 0 {
            plan.fallback_insert_nodes.push(new_u);
        } else {
            let mapped_nbrs: Vec<u32> = local_l0[idx]
                .iter()
                .map(|&loc| pairs[loc as usize].1)
                .collect();
            plan.graft_tasks.push((new_u, mapped_nbrs));
        }
    }
    plan
}
