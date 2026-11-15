//! Shared-floor candidate pruning: the rounding bound, boundary ties, a
//! starved filter, and an empty participant.

use super::super::Answer;
use super::*;
use crate::vector::FloorPruneSpec;

/// A negative inner-product manifold with boundary ties straddling `k`,
/// and the index built over it.
struct NegativeManifold {
    n: usize,
    k: usize,
    q: Vec<f32>,
    ranked: Vec<(usize, f32)>,
    cfg: HnswConfig,
    index: HnswIndex,
}

fn unit_sub(rng: &mut Rng, len: usize) -> Vec<f32> {
    let mut v: Vec<f32> = (0..len).map(|_| rng.unit() as f32).collect();
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-6);
    for x in &mut v {
        *x /= norm;
    }
    v
}

fn negative_manifold() -> NegativeManifold {
    // 1a. Negative inner-product manifold on odd `dim = 9` (`[-3.0, -1.0]`):
    //     `x = [-2.0, u_{1..9}]` (`||u|| = 1`), `q = [1.0, u_q]`. Pairwise
    //     `x · y = 4.0 + u_x · u_y` preserves smooth spherical navigability
    //     while `q · x = -2.0 + u_q · u_x` is strictly negative everywhere.
    let dim = 9usize;
    let n = 800usize;
    let mut rng = Rng(0x7272_0009);
    let q_sub = unit_sub(&mut rng, dim - 1);
    let mut q = Vec::with_capacity(dim);
    q.push(1.0_f32);
    q.extend_from_slice(&q_sub);

    let mut raw = Vec::with_capacity(n * dim);
    for _ in 0..n {
        let sub = unit_sub(&mut rng, dim - 1);
        raw.push(-2.0_f32);
        raw.extend_from_slice(&sub);
    }
    // Clone the natural 28th-ranked vector into 8 slots so exact boundary ties
    // straddle `k = 32` without breaking the smooth spherical gradient.
    let k = 32usize;
    let mut ranked: Vec<(usize, f32)> = (0..n)
        .map(|r| {
            let row = &raw[r * dim..(r + 1) * dim];
            let s: f32 = q.iter().zip(row).map(|(a, b)| a * b).sum();
            (r, s)
        })
        .collect();
    ranked.sort_by(|a, b| b.1.total_cmp(&a.1));
    let tie_src = raw[ranked[27].0 * dim..(ranked[27].0 + 1) * dim].to_vec();
    for i in 0..8usize {
        let dst = ranked[400 + i].0;
        raw[dst * dim..(dst + 1) * dim].copy_from_slice(&tie_src);
    }

    let mut cfg = config(BuildMode::FixedOrder);
    cfg.m = 16;
    cfg.ef_construction = 120;
    let mut index = HnswIndex::new(dim, cfg).unwrap();
    index.add(&raw, dim).unwrap();
    index.prepare().unwrap();
    NegativeManifold {
        n,
        k,
        q,
        ranked,
        cfg,
        index,
    }
}

fn negative_baseline(m: &NegativeManifold) -> (Vec<(u32, f32)>, Answer) {
    let (index, q, k) = (&m.index, &m.q, m.k);
    let (base_res, base_ans) = index
        .search_with_stats(q, k, 128, VectorSearchOptions::new())
        .unwrap();
    assert!(!base_ans.floor_pruned);
    assert_eq!(base_res.len(), k);
    assert!(
        base_res[0].1 < 0.0 && base_res[k - 1].1 < 0.0,
        "expected negative top-k scores, got best={} kth={}",
        base_res[0].1,
        base_res[k - 1].1
    );
    (base_res, base_ans)
}

fn a_disabled_spec_never_prunes(m: &NegativeManifold, base_res: &[(u32, f32)], base_ans: &Answer) {
    let (index, q, k) = (&m.index, &m.q, m.k);
    // Disabled `FloorPruneSpec` never prunes the walk (`stats` bit-identical
    // to `None`), even when `minimum_score` is `1.0e9`; at `floor`, both `ids`
    // and `stats` are bit-identical.
    let floor = base_res[k - 1].1.next_down();
    let disabled = FloorPruneSpec {
        enabled: false,
        floor_k: k,
        share_ppm: 1_000_000,
        greediness_ppm: 1_000_000,
        clamp_min: 1,
    };
    let (high_res, high_ans) = index
        .search_with_stats(
            q,
            k,
            128,
            VectorSearchOptions::new()
                .with_minimum_score(1.0e9)
                .with_floor_prune(disabled),
        )
        .unwrap();
    assert!(!high_ans.floor_pruned);
    assert!(high_res.is_empty());
    assert_eq!(high_ans.stats, base_ans.stats);

    let (off_res, off_ans) = index
        .search_with_stats(
            q,
            k,
            128,
            VectorSearchOptions::new()
                .with_minimum_score(floor)
                .with_floor_prune(disabled),
        )
        .unwrap();
    assert!(!off_ans.floor_pruned);
    let base_ids: Vec<u32> = base_res.iter().map(|&(id, _)| id).collect();
    let off_ids: Vec<u32> = off_res.iter().map(|&(id, _)| id).collect();
    assert_eq!(off_ids, base_ids);
    assert_eq!(off_ans.stats, base_ans.stats);
}

fn an_enabled_spec_prunes_and_keeps_boundary_ties(
    m: &NegativeManifold,
    base_res: &[(u32, f32)],
    base_ans: &Answer,
) {
    let (index, q, k) = (&m.index, &m.q, m.k);
    let ranked = &m.ranked;
    let floor = base_res[k - 1].1.next_down();
    // At `kth.next_down()`, both `gateK` (`g = 0`, `gate = 62`) and clamped
    // pruning (`g = 250_000`, `gate = 47`) prune sub-floor candidates in the
    // `ef = 128` beam tail while retaining boundary ties at `floor` and high
    // recall (graph floor pruning is a bridge heuristic, not an exact
    // proof, whereas no reached candidate `>= floor` is ever rejected by SIMD
    // rounding).
    let base_tie_score = ranked[27].1;
    for g_ppm in [0u32, 250_000u32] {
        let spec = FloorPruneSpec {
            enabled: true,
            floor_k: k,
            share_ppm: 500_000,
            greediness_ppm: g_ppm,
            clamp_min: 8,
        };
        let (pruned_res, pruned_ans) = index
            .search_with_stats(
                q,
                k,
                128,
                VectorSearchOptions::new()
                    .with_minimum_score(floor)
                    .with_floor_prune(spec),
            )
            .unwrap();
        assert!(pruned_ans.floor_pruned, "g={g_ppm}");
        assert!(
            pruned_ans.stats.scored < base_ans.stats.scored,
            "expected floor pruning to reduce scored rows at g={g_ppm}: {} vs {}",
            pruned_ans.stats.scored,
            base_ans.stats.scored
        );
        assert_eq!(pruned_res.len(), k, "g={g_ppm}");
        for &(_, got) in &pruned_res {
            assert!(
                got >= floor,
                "returned hit {got} fell below floor {floor} at g={g_ppm}"
            );
        }
        let tie_survivors = pruned_res
            .iter()
            .filter(|&&(_, got)| (got - base_tie_score).abs() <= 1e-5)
            .count();
        assert!(
            tie_survivors >= 4,
            "expected boundary ties at {base_tie_score} to survive floor={floor} at g={g_ppm}, got {tie_survivors}"
        );
    }
}

fn the_rounding_bound_holds_on_variable_norms(k: usize, cfg: HnswConfig) {
    // 1b. Extreme variable norms (`0.05..500.0`) on odd `dim = 17` SIMD tail:
    //     Higham's `4 * gamma(d) * |q| * |x|` bound is exact on `Enumerate`
    //     (zero survivors dropped at `kth.next_down()`) and preserves high
    //     recall on the graph walk.
    let vdim = 17usize;
    let vn = 600usize;
    let mut vrng = Rng(0x7272_0017);
    let vq = unit_sub(&mut vrng, vdim);
    let mut vraw = Vec::with_capacity(vn * vdim);
    for i in 0..vn {
        let scale = match i % 4 {
            0 => 0.05_f32,
            1 => 1.0_f32,
            2 => 25.0_f32,
            _ => 500.0_f32,
        };
        let dir = unit_sub(&mut vrng, vdim);
        for &c in &dir {
            vraw.push(scale * c);
        }
    }
    let mut vindex = HnswIndex::new(vdim, cfg).unwrap();
    vindex.add(&vraw, vdim).unwrap();
    // Unlinked (`Enumerate`) exact Higham bound check at `kth.next_down()`:
    let (venum_base, _) = vindex
        .search_with_stats(&vq, k, 128, VectorSearchOptions::new())
        .unwrap();
    let vfloor = venum_base[k - 1].1.next_down();
    let (venum_pruned, _) = vindex
        .search_with_stats(
            &vq,
            k,
            128,
            VectorSearchOptions::new().with_minimum_score(vfloor),
        )
        .unwrap();
    assert_eq!(venum_pruned, venum_base);

    vindex.prepare().unwrap();
    let (vpruned, vans) = vindex
        .search_with_stats(
            &vq,
            k,
            128,
            VectorSearchOptions::new()
                .with_minimum_score(vfloor)
                .with_floor_prune(FloorPruneSpec {
                    enabled: true,
                    floor_k: k,
                    share_ppm: 500_000,
                    greediness_ppm: 0,
                    clamp_min: 8,
                }),
        )
        .unwrap();
    assert!(vans.floor_pruned);
    assert!(
        vpruned.len() >= k - 3,
        "expected >= {}/{} recall on variable-norm graph walk, got {}",
        k - 3,
        k,
        vpruned.len()
    );
}

fn a_starved_filtered_walk_never_falls_back_to_enumerate(m: &NegativeManifold) {
    let (index, q, k) = (&m.index, &m.q, m.k);
    let n = m.n;
    // 2. Filtered walk starved by a high global floor returns `Walk::FloorPruned`
    //    and NEVER triggers a fallback to `View::enumerate`.
    let allow: Vec<bool> = (0..n).map(|i| i % 2 == 0).collect();
    let (_, starved_ans) = index
        .search_with_stats(
            q,
            k,
            64,
            VectorSearchOptions::new()
                .with_allowlist(&allow)
                .with_enumerate_max_rows(32)
                .with_minimum_score(1.0e6)
                .with_floor_prune(FloorPruneSpec {
                    enabled: true,
                    floor_k: k,
                    share_ppm: 250_000,
                    greediness_ppm: 500_000,
                    clamp_min: 4,
                }),
        )
        .unwrap();
    assert!(starved_ans.floor_pruned);
    // Half the rows allowed: routed plain.
    assert_eq!(starved_ans.regime, Some(crate::vector::FilterRegime::Plain));
    assert!(!starved_ans.budget_handoff);
    assert!(
        starved_ans.stats.scored < 120,
        "expected floor-starved walk to stop without enumerating 300 allowed rows, scored={}",
        starved_ans.stats.scored
    );
}

fn an_empty_participant_completes_cleanly(m: &NegativeManifold) {
    let (index, q, k) = (&m.index, &m.q, m.k);
    let n = m.n;
    // 3. Empty participant (`allowed == 0` / `share_ppm == 0`) completes cleanly.
    let empty_allow = vec![false; n];
    let mut emitted = 0u64;
    let summary = index
        .search_streaming_controlled(
            q,
            VectorSearchOptions::new()
                .with_allowlist(&empty_allow)
                .with_candidate_depth(64)
                .with_minimum_score(0.5)
                .with_floor_prune(FloorPruneSpec {
                    enabled: true,
                    floor_k: k,
                    share_ppm: 0,
                    greediness_ppm: 0,
                    clamp_min: 8,
                }),
            &mut |batch| {
                emitted += batch.slots.len() as u64;
                VectorStreamControl::Continue
            },
            &mut || VectorStreamControl::Continue,
        )
        .unwrap();
    assert_eq!(emitted, 0);
    assert_eq!(summary.emitted, 0);
}

#[test]
fn floor_prune_respects_higham_bound_boundary_ties_starved_filter_and_determinism() {
    let m = negative_manifold();
    let (base_res, base_ans) = negative_baseline(&m);
    a_disabled_spec_never_prunes(&m, &base_res, &base_ans);
    an_enabled_spec_prunes_and_keeps_boundary_ties(&m, &base_res, &base_ans);
    the_rounding_bound_holds_on_variable_norms(m.k, m.cfg);
    a_starved_filtered_walk_never_falls_back_to_enumerate(&m);
    an_empty_participant_completes_cleanly(&m);
}
