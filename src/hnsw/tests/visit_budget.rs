//! The visit budget of a filter-aware walk whose mask defeats the probe.

use std::collections::HashSet;

use super::super::graph::{with_scratch, Filter, LinkCells, NoHook, Scored, View, Walk};
use super::*;
use crate::vector::{FilterRegimeCounts, Mask};

const K: usize = 10;
const EF: usize = 32;

/// The mask built to defeat the probe, with the descent facts it rests on.
struct Trap {
    ep: u32,
    descent_scored: u64,
    allow: Vec<bool>,
}

/// The trapped query with its mask's cardinality and exact answer.
struct BudgetCase<'a> {
    fx: &'a Fixture,
    trap: &'a Trap,
    allowed: usize,
    truth: Vec<(u32, f32)>,
}

fn draw_isotropic(rng: &mut Rng, count: usize, dim: usize) -> Vec<f32> {
    let mut out = Vec::with_capacity(count * dim);
    for _ in 0..count {
        let mut row: Vec<f32> = (0..dim).map(|_| rng.normal()).collect();
        let norm = row.iter().map(|x| x * x).sum::<f32>().sqrt();
        for x in &mut row {
            *x /= norm;
        }
        out.extend_from_slice(&row);
    }
    out
}

/// Unit rows without topic structure behind a narrow graph (`m = 6`).
fn narrow_isotropic_fixture() -> Fixture {
    let dim = 32;
    let n = 12_000;
    let mut rng = Rng(0x94B0);
    let vectors = draw_isotropic(&mut rng, n, dim);
    let queries = draw_isotropic(&mut rng, 1, dim);
    let mut cfg = config(BuildMode::FixedOrder);
    cfg.m = 6;
    cfg.ef_construction = 48;
    let mut index = HnswIndex::new(dim, cfg).unwrap();
    index.add(&vectors, dim).unwrap();
    index.prepare().unwrap();
    Fixture {
        dim,
        vectors,
        queries,
        index,
    }
}

fn neighbors0<L: LinkCells + ?Sized>(view: &View<'_, L>, node: u32) -> Vec<u32> {
    let start = node as usize * (view.m0 + 1);
    let count = (LinkCells::get(view.links0, start) as usize).min(view.m0);
    (0..count)
        .map(|i| LinkCells::get(view.links0, start + 1 + i))
        .collect()
}

fn ball_2hop<L: LinkCells + ?Sized>(view: &View<'_, L>, seeds: &[u32]) -> HashSet<u32> {
    let neighbors0 = |node: u32| neighbors0(view, node);
    let mut out: HashSet<u32> = seeds.iter().copied().collect();
    let mut ring1 = Vec::new();
    for &u in seeds {
        for v in neighbors0(u) {
            if out.insert(v) {
                ring1.push(v);
            }
        }
    }
    for u in ring1 {
        for v in neighbors0(u) {
            out.insert(v);
        }
    }
    out
}

/// The node the upper-layer descent lands on.
fn descent_landing<L: LinkCells + ?Sized>(view: &View<'_, L>, query: &[f32]) -> Scored {
    let mut ep = Scored {
        score: view.kernel.dot(query, view.row(view.entry)),
        node: view.entry,
    };
    for layer in (1..=view.top as usize).rev() {
        loop {
            let u = view.upper_slot[ep.node as usize] as usize;
            let start = view.upper_base[u] as usize + (layer - 1) * (view.m + 1);
            let count = (LinkCells::get(view.upper, start) as usize).min(view.m);
            let mut improved = false;
            for i in 0..count {
                let v = LinkCells::get(view.upper, start + 1 + i);
                let s = view.kernel.dot(query, view.row(v));
                if s > ep.score {
                    ep = Scored { score: s, node: v };
                    improved = true;
                }
            }
            if !improved {
                break;
            }
        }
    }
    ep
}

/// Run a zero-budget walk to read the exact descent score count.
fn zero_budget_descent_scored<L: LinkCells + ?Sized>(
    view: &View<'_, L>,
    query: &[f32],
    n: usize,
) -> u64 {
    let dummy = vec![false; n];
    let (_, zero_stats, zero_walk) = with_scratch(|scratch| {
        view.walk(
            query,
            16,
            super::super::graph::Steering::filtered(Filter::Aware {
                allow: Mask::Bools(&dummy),
                allowed: 0,
                route: None,
                budget: Some(0),
            }),
            scratch,
            &mut NoHook,
        )
    });
    assert_eq!(zero_walk, Walk::Budget);
    zero_stats.scored
}

/// Allow 20 mid-low-scoring nodes in `ep`'s 2-hop layer-0 ball (`>= ef = 16`,
/// so `Walk::Short` cannot fire and the 2-hop `far` probe clears easily),
/// leaving thousands of rows strictly below `lowest_decoy_score` for
/// non-interfering cardinality padding and thousands strictly above
/// `best_decoy_score` for uphill wave steps.
fn pick_decoys(ep_reach: &HashSet<u32>, exact_score: impl Fn(u32) -> f32) -> (Vec<u32>, f32) {
    let mut ep_ball: Vec<u32> = ep_reach.iter().copied().collect();
    ep_ball.sort_by(|&a, &b| exact_score(a).total_cmp(&exact_score(b)).then(a.cmp(&b)));
    let decoy_offset = ep_ball.len() / 4;
    let decoys: Vec<u32> = ep_ball.into_iter().skip(decoy_offset).take(20).collect();
    assert_eq!(decoys.len(), 20);
    let best_decoy_score = decoys
        .iter()
        .map(|&u| exact_score(u))
        .fold(f32::NEG_INFINITY, f32::max);
    (decoys, best_decoy_score)
}

/// The 2-hop incoming predecessors of `pocket`.
fn moat_around<L: LinkCells + ?Sized>(
    view: &View<'_, L>,
    pocket: &[u32],
    n: usize,
) -> HashSet<u32> {
    let neighbors0 = |node: u32| neighbors0(view, node);
    let pocket_set: HashSet<u32> = pocket.iter().copied().collect();
    let mut in_1hop: HashSet<u32> = HashSet::new();
    for u in 0..n as u32 {
        if !pocket_set.contains(&u) && neighbors0(u).iter().any(|v| pocket_set.contains(v)) {
            in_1hop.insert(u);
        }
    }
    let mut moat = in_1hop.clone();
    for u in 0..n as u32 {
        if !pocket_set.contains(&u)
            && neighbors0(u)
                .iter()
                .any(|v| pocket_set.contains(v) || in_1hop.contains(v))
        {
            moat.insert(u);
        }
    }
    moat
}

/// Default threshold at `m = 6` (`m0 = 12`), `ef = 32` is `4 * 32 * 12 = 1536`,
/// so `target_allowed = 1550` (`12.9%` uncorrelated sparsity outside `moat`)
/// sets `budget = ceil(1550 / 3.5) = 443` layer-0 work units
/// (`layer0_scored + bridges`), while at `dim = 64` full `m0` lists drive
/// ~40 hops of bridge + neighbor expansions (~700+ work units >> 443).
fn padded_allow(
    n: usize,
    pocket: &[u32],
    decoys: &[u32],
    moat: &HashSet<u32>,
    ranked: &[u32],
) -> Vec<bool> {
    let target_allowed = 1_550usize;
    let mut allow = vec![false; n];
    for &u in pocket {
        allow[u as usize] = true;
    }
    for &u in decoys {
        allow[u as usize] = true;
    }
    for u in 0..n as u32 {
        if allow.iter().filter(|&&on| on).count() >= target_allowed {
            break;
        }
        if !allow[u as usize] && !moat.contains(&u) && u % 32 == 0 {
            allow[u as usize] = true;
        }
    }
    for &u in ranked.iter().rev() {
        if allow.iter().filter(|&&on| on).count() >= target_allowed {
            break;
        }
        if !allow[u as usize] && !moat.contains(&u) {
            allow[u as usize] = true;
        }
    }
    allow
}

/// Inspect the upper-layer descent landing point and layer-0 adjacency so
/// the mask deterministically defeats both `Walk::Far` (2-hop probe) and
/// `Walk::Short` (unfilled beam) while walling off the true top-k pocket.
fn defeating_mask(fx: &Fixture) -> Trap {
    let (dim, vectors, index) = (fx.dim, &fx.vectors[..], &fx.index);
    let query = &fx.queries[..fx.dim];
    let n = fx.rows();
    let super::super::Backing::Heap(store) = &index.backing else {
        unreachable!()
    };
    let view = store.view();
    let ep = descent_landing(&view, query);
    let descent_scored = zero_budget_descent_scored(&view, query, n);
    let ranked = exact_top(vectors, dim, query, n);
    let exact_score =
        |row: u32| crate::exact_vectors::dot(&vectors[row as usize * dim..][..dim], query);
    let ep_reach = ball_2hop(&view, &[ep.node]);
    let (decoys, best_decoy_score) = pick_decoys(&ep_reach, exact_score);
    let mut start_reach = ball_2hop(&view, &decoys);
    start_reach.extend(ep_reach);
    // Isolate a single top hit outside the 2-hop outgoing reach of `ep`
    // and `decoys`, walled off by its 2-hop incoming predecessors (~100
    // rows) so no directed 1-hop bridge step `c -> bridge -> p` can reach
    // it while leaving 99.5% of the positive hemisphere open to the beam.
    let pocket: Vec<u32> = ranked
        .iter()
        .copied()
        .filter(|u| exact_score(*u) > best_decoy_score && !start_reach.contains(u))
        .take(1)
        .collect();
    assert_eq!(pocket.len(), 1, "expected an isolated top-1 pocket row");
    let moat = moat_around(&view, &pocket, n);
    Trap {
        ep: ep.node,
        descent_scored,
        allow: padded_allow(n, &pocket, &decoys, &moat, &ranked),
    }
}

fn the_budget_hands_off_where_a_pinned_walk_misses(case: &BudgetCase<'_>) {
    let (index, query) = (&case.fx.index, &case.fx.queries[..case.fx.dim]);
    let (allow, allowed, truth) = (&case.trap.allow[..], case.allowed, &case.truth[..]);
    let (k, ef, descent_scored) = (K, EF, case.trap.descent_scored);
    let ep_allowed = u64::from(allow[case.trap.ep as usize]);
    // 1. Pinned `FilterAware` ignores both the probe and the budget: it fills
    // the beam (`>= k`, not stranded), burns heavy work, and misses the
    // moated pocket.
    let pinned_opts = VectorSearchOptions::new()
        .with_mask(allow)
        .with_filter_regime(FilterRegime::FilterAware);
    let (pinned_hits, pinned_answer) = index.search_with_stats(query, k, ef, pinned_opts).unwrap();
    assert_eq!(pinned_answer.regime, Some(FilterRegime::FilterAware));
    assert!(!pinned_answer.budget_handoff);
    assert_eq!(pinned_hits.len(), k);
    assert_ne!(
        pinned_hits, truth,
        "moated pocket should be unreachable without budget handoff"
    );

    // 2. Adaptive (`ANN` default): `far` probe runs and does NOT trip
    // (`hops > 0`, `bridges > 0`), `Walk::Budget` fires mid-walk, skips every
    // allowed row the walk already scored, and returns the exact top `k`.
    let adaptive_opts = VectorSearchOptions::new().with_mask(allow);
    let (hits, answer) = index
        .search_with_stats(query, k, ef, adaptive_opts)
        .unwrap();
    assert_eq!(
        answer.regime,
        Some(FilterRegime::Enumerate),
        "answer={answer:?}, pinned={pinned_answer:?}, descent={descent_scored}"
    );
    assert!(
        answer.budget_handoff,
        "expected Walk::Budget handoff: {answer:?}"
    );
    assert!(
        answer.stats.probed > 0,
        "far probe must have run and cleared"
    );
    assert!(answer.stats.hops > 0 && answer.stats.bridges > 0);
    assert!(
        answer.stats.hops < pinned_answer.stats.hops,
        "budget must stop the walk earlier than pinned ({} vs {})",
        answer.stats.hops,
        pinned_answer.stats.hops
    );
    assert_eq!(
        answer.stats.scored - descent_scored + ep_allowed,
        allowed as u64,
        "every allowed row must be scored at descent/layer-0 + enumerate_skipping exactly once"
    );
    assert_eq!(hits, truth);
}

fn the_unary_search_reports_the_handoff(case: &BudgetCase<'_>) {
    let (index, query) = (&case.fx.index, &case.fx.queries[..case.fx.dim]);
    let (allow, k, ef) = (&case.trap.allow[..], K, EF);
    // `VectorProvider::search` reports both `enumerate` and `budget_handoffs`.
    let unary = index
        .search(
            query,
            k,
            VectorSearchOptions::new()
                .with_mask(allow)
                .with_candidate_depth(ef),
        )
        .unwrap();
    assert_eq!(
        unary.regimes,
        FilterRegimeCounts {
            enumerate: 1,
            filter_aware: 0,
            plain: 0,
            budget_handoffs: 1,
        }
    );
}

fn the_stream_emits_every_allowed_row_once(case: &BudgetCase<'_>) {
    let (dim, vectors, index) = (case.fx.dim, &case.fx.vectors[..], &case.fx.index);
    let query = &case.fx.queries[..dim];
    let (allow, allowed, ef) = (&case.trap.allow[..], case.allowed, EF);
    // 3. Streaming: `Walk::Budget` emits what the walk reached and streams the
    // remaining allowed rows with zero duplicates, reporting `budget_handoffs = 1`.
    let mut streamed: Vec<(i64, f32)> = Vec::new();
    let summary = index
        .search_streaming_controlled(
            query,
            VectorSearchOptions::new()
                .with_mask(allow)
                .with_candidate_depth(ef),
            &mut |batch| {
                streamed.extend(
                    batch
                        .slots
                        .iter()
                        .copied()
                        .zip(batch.scores.iter().copied()),
                );
                VectorStreamControl::Continue
            },
            &mut || VectorStreamControl::Continue,
        )
        .unwrap();
    assert!(summary.completed);
    assert_eq!(summary.emitted, allowed);
    assert_eq!(
        summary.regimes,
        FilterRegimeCounts {
            enumerate: 1,
            filter_aware: 0,
            plain: 0,
            budget_handoffs: 1,
        }
    );
    let mut seen_slots: Vec<i64> = streamed.iter().map(|r| r.0).collect();
    seen_slots.sort_unstable();
    seen_slots.dedup();
    assert_eq!(seen_slots.len(), allowed, "stream emitted a row twice");
    for (slot, score) in &streamed {
        assert!(allow[*slot as usize]);
        let exact = crate::exact_vectors::dot(&vectors[*slot as usize * dim..][..dim], query);
        assert_eq!(score.to_bits(), exact.to_bits());
    }
}

fn enumerate_max_rows_scales_the_budget(case: &BudgetCase<'_>) {
    let (index, query) = (&case.fx.index, &case.fx.queries[..case.fx.dim]);
    let (allow, k, ef) = (&case.trap.allow[..], K, EF);
    // 4. Policy `enumerate_max_rows` scales the budget: a higher threshold
    // implies cheaper enumeration relative to walking and stops in fewer hops.
    let (_, loose) = index
        .search_with_stats(
            query,
            k,
            ef,
            VectorSearchOptions::new()
                .with_mask(allow)
                .with_enumerate_max_rows(400),
        )
        .unwrap();
    let (_, tight) = index
        .search_with_stats(
            query,
            k,
            ef,
            VectorSearchOptions::new()
                .with_mask(allow)
                .with_enumerate_max_rows(1_536),
        )
        .unwrap();
    assert!(tight.budget_handoff);
    assert!(
        tight.stats.hops < loose.stats.hops,
        "tighter enumerate_max_rows budget should stop in fewer hops ({} vs {})",
        tight.stats.hops,
        loose.stats.hops
    );
}

#[test]
fn a_walk_that_defeats_the_probe_stops_at_its_visit_budget() {
    let fx = narrow_isotropic_fixture();
    let trap = defeating_mask(&fx);
    let allowed = trap.allow.iter().filter(|&&on| on).count();
    assert_eq!(allowed, 1_550);
    let truth = exact_top_masked(&fx.vectors, fx.dim, &fx.queries[..fx.dim], &trap.allow, K);
    let case = BudgetCase {
        fx: &fx,
        trap: &trap,
        allowed,
        truth,
    };
    the_budget_hands_off_where_a_pinned_walk_misses(&case);
    the_unary_search_reports_the_handoff(&case);
    the_stream_emits_every_allowed_row_once(&case);
    enumerate_max_rows_scales_the_budget(&case);
}
