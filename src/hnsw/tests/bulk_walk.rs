//! Bulk neighbor-list scoring against a reference single-row-dot walk.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use super::super::distance::Kernel;
use super::super::graph::{
    with_scratch, Filter, FloorPrune, Hook, LayerSearch, LinkCells, Patience, Scored, SearchStats,
    Store, View,
};
use super::walk_identity::tie_corpus;
use super::*;
use crate::vector::Mask;

struct Trace(Vec<(u32, u32)>);
impl Hook for Trace {
    fn scored(&mut self, node: u32, score: f32, _row: &[f32]) {
        self.0.push((node, score.to_bits()));
    }
    fn hop(&mut self) -> bool {
        true
    }
}

type ScoreTrace = Vec<(u32, u32)>;

/// The beam state of the reference walk.
struct ReferenceBeam {
    ef: usize,
    visited: Vec<bool>,
    cand: BinaryHeap<Scored>,
    res: BinaryHeap<Reverse<Scored>>,
    trace: ScoreTrace,
}

impl ReferenceBeam {
    /// Record a freshly scored node: traced and kept as a result only when
    /// `allowed`, queued as a candidate whenever it beats the beam.
    fn offer(&mut self, node: u32, score: f32, allowed: bool) {
        if allowed {
            self.trace.push((node, score.to_bits()));
        }
        if self.res.len() < self.ef || score > self.res.peek().unwrap().0.score {
            let sc = Scored { score, node };
            self.cand.push(sc);
            if allowed {
                self.res.push(Reverse(sc));
                if self.res.len() > self.ef {
                    self.res.pop();
                }
            }
        }
    }
}

fn links_of<L: LinkCells + ?Sized>(view: &View<'_, L>, node: u32, layer: usize) -> Vec<u32> {
    let (cells, cap, start) = if layer == 0 {
        (view.links0, view.m0, node as usize * (view.m0 + 1))
    } else {
        let u = view.upper_slot[node as usize] as usize;
        let base = view.upper_base[u] as usize + (layer - 1) * (view.m + 1);
        (view.upper, view.m, base)
    };
    let count = (cells.get(start) as usize).min(cap);
    (0..count).map(|i| cells.get(start + 1 + i)).collect()
}

/// Greedy descent through the upper layers to the layer-0 entry point.
fn descend<L: LinkCells + ?Sized>(view: &View<'_, L>, q: &[f32]) -> Scored {
    let mut ep = Scored {
        score: view.kernel.dot(q, view.row(view.entry)),
        node: view.entry,
    };
    for layer in (1..=view.top as usize).rev() {
        loop {
            let mut improved = false;
            for v in links_of(view, ep.node, layer) {
                let s = view.kernel.dot(q, view.row(v));
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

/// The filter-aware expansion of `node`: its allowed unvisited neighbors,
/// then allowed rows one hop behind each disallowed neighbor, up to a full
/// layer-0 list.
fn expand_aware<L: LinkCells + ?Sized>(
    view: &View<'_, L>,
    q: &[f32],
    node: u32,
    mask: Mask<'_>,
    beam: &mut ReferenceBeam,
) {
    let mut routes = Vec::new();
    let mut fresh = Vec::new();
    for v in links_of(view, node, 0) {
        if mask.test(v as usize) {
            if !beam.visited[v as usize] {
                beam.visited[v as usize] = true;
                fresh.push(v);
            }
        } else if !beam.visited[v as usize] {
            routes.push(v);
        }
    }
    for bridge in routes {
        if fresh.len() >= view.m0 {
            break;
        }
        if beam.visited[bridge as usize] {
            continue;
        }
        beam.visited[bridge as usize] = true;
        for w in links_of(view, bridge, 0) {
            if fresh.len() >= view.m0 {
                break;
            }
            if mask.test(w as usize) && !beam.visited[w as usize] {
                beam.visited[w as usize] = true;
                fresh.push(w);
            }
        }
    }
    for v in fresh {
        let s = view.kernel.dot(q, view.row(v));
        beam.offer(v, s, true);
    }
}

/// The plain expansion of `node`: every unvisited neighbor is scored and
/// routed through, and kept only when `mask` allows it.
fn expand_plain<L: LinkCells + ?Sized>(
    view: &View<'_, L>,
    q: &[f32],
    node: u32,
    mask: Option<Mask<'_>>,
    beam: &mut ReferenceBeam,
) {
    for v in links_of(view, node, 0) {
        if beam.visited[v as usize] {
            continue;
        }
        beam.visited[v as usize] = true;
        let s = view.kernel.dot(q, view.row(v));
        let ok = mask.is_none_or(|m| m.test(v as usize));
        beam.offer(v, s, ok);
    }
}

/// Reference single-row-dot walk (unfiltered, plain, and filter-aware) to
/// pin that bulk neighbor-list scoring changes neither the visited path
/// nor a single traversal score bit on any kernel or odd dimension.
fn single_row_walk<L: LinkCells + ?Sized>(
    view: &View<'_, L>,
    q: &[f32],
    ef: usize,
    allow: Option<(Mask<'_>, bool)>,
) -> (ScoreTrace, ScoreTrace) {
    seeded_walk(view, q, ef, allow, &[descend(view, q)])
}

/// The reference layer-0 walk from already scored `entries`: each distinct
/// entry is a candidate, the allowed ones are traced and kept, and the
/// results are trimmed to `ef` once every entry is in.
fn seeded_walk<L: LinkCells + ?Sized>(
    view: &View<'_, L>,
    q: &[f32],
    ef: usize,
    allow: Option<(Mask<'_>, bool)>,
    entries: &[Scored],
) -> (ScoreTrace, ScoreTrace) {
    let mut beam = ReferenceBeam {
        ef,
        visited: vec![false; view.n()],
        cand: BinaryHeap::new(),
        res: BinaryHeap::new(),
        trace: Vec::new(),
    };
    for &entry in entries {
        if std::mem::replace(&mut beam.visited[entry.node as usize], true) {
            continue;
        }
        beam.cand.push(entry);
        if allow.is_none_or(|(m, _)| m.test(entry.node as usize)) {
            beam.trace.push((entry.node, entry.score.to_bits()));
            beam.res.push(Reverse(entry));
        }
    }
    while beam.res.len() > ef {
        beam.res.pop();
    }
    while let Some(c) = beam.cand.pop() {
        if beam.res.len() >= ef && c.score < beam.res.peek().unwrap().0.score {
            break;
        }
        match allow {
            Some((mask, true)) => expand_aware(view, q, c.node, mask, &mut beam),
            plain => expand_plain(view, q, c.node, plain.map(|(m, _)| m), &mut beam),
        }
    }
    let mut out: Vec<Scored> = beam.res.into_iter().map(|r| r.0).collect();
    out.sort_unstable_by(|a, b| b.cmp(a));
    (
        beam.trace,
        out.into_iter()
            .map(|s| (s.node, s.score.to_bits()))
            .collect(),
    )
}

/// The provider's walk at beam `ef`: every allowed row it scored in order,
/// then its results best first.
fn traced_walk<L: LinkCells + ?Sized>(
    view: &View<'_, L>,
    q: &[f32],
    ef: usize,
    filter: Filter<'_>,
) -> (ScoreTrace, ScoreTrace) {
    let mut trace = Trace(Vec::new());
    let (hits, _, _) = with_scratch(|scratch| {
        view.walk(
            q,
            ef,
            super::super::graph::Steering::filtered(filter),
            scratch,
            &mut trace,
        )
    });
    let hits = hits
        .into_iter()
        .map(|s| (s.node, s.score.to_bits()))
        .collect();
    (trace.0, hits)
}

#[test]
fn bulk_walk_and_enumerate_visit_identical_nodes_and_scores_to_single_row_dot() {
    for dim in [17usize, 33, 64, 65] {
        let (vectors, queries) = corpus_with_queries(600, 6, dim, 0x9600 + dim as u64);
        let allow_flags: Vec<bool> = (0..600).map(|i| i % 3 != 0).collect();
        let mask = Mask::Bools(&allow_flags);
        let allowed = mask.count();
        for kernel in Kernel::available() {
            let mut store = Store::new(dim, 16, 64, 7, kernel);
            store.append_unlinked(&vectors).unwrap();
            let links = store.build_links(false).unwrap();
            store.install_links(links);
            let view = store.view();
            for q in queries.chunks_exact(dim) {
                for (filter, spec) in [
                    (Filter::None, None),
                    (Filter::Plain(mask), Some((mask, false))),
                    (
                        Filter::Aware {
                            allow: mask,
                            allowed,
                            route: None,
                            budget: None,
                        },
                        Some((mask, true)),
                    ),
                ] {
                    let (got_trace, got_hits) = traced_walk(&view, q, 24, filter);
                    let (exp_trace, exp_hits) = single_row_walk(&view, q, 24, spec);
                    assert_eq!(got_trace, exp_trace, "{} dim={dim}", kernel.name());
                    assert_eq!(got_hits, exp_hits, "{} dim={dim}", kernel.name());
                }
                let mut surv = Vec::new();
                let count = view.enumerate(q, mask, 10, 1.0e-5, &mut surv);
                assert_eq!(count, allowed as u64);
                for s in &surv {
                    assert_eq!(
                        s.score.to_bits(),
                        kernel.dot(q, view.row(s.node)).to_bits(),
                        "{} dim={dim} enumerate",
                        kernel.name()
                    );
                }
            }
        }
    }
}

/// A constantly tying corpus where every forty-first row holds one value
/// that is not a number or not finite, so its scores are too.
fn poisoned_tie_corpus(rows: usize, dim: usize) -> Vec<f32> {
    let mut vectors = tie_corpus(rows, dim, 0x5EED_0006);
    for row in (5..rows).step_by(41) {
        vectors[row * dim + row % dim] = match row % 3 {
            0 => f32::NAN,
            1 => f32::INFINITY,
            _ => f32::NEG_INFINITY,
        };
    }
    vectors
}

#[test]
fn walks_order_tied_infinite_and_nan_scores_as_the_reference_walk_does() {
    let (dim, rows) = (8, 700);
    let vectors = poisoned_tie_corpus(rows, dim);
    let queries = tie_corpus(6, dim, 0x5EED_0007);
    let allow_flags: Vec<bool> = (0..rows).map(|i| i % 3 != 0).collect();
    let mask = Mask::Bools(&allow_flags);
    let aware = Filter::Aware {
        allow: mask,
        allowed: mask.count(),
        route: None,
        budget: None,
    };
    for kernel in Kernel::available() {
        let mut store = Store::new(dim, 8, 40, 7, kernel);
        store.append_unlinked(&vectors).unwrap();
        let links = store.build_links(false).unwrap();
        store.install_links(links);
        let view = store.view();
        for q in queries.chunks_exact(dim) {
            // A beam of zero walks as a beam of one.
            for ef in [0usize, 1, 2, 7, 24, 100, 900] {
                for (filter, spec) in [
                    (Filter::None, None),
                    (Filter::Plain(mask), Some((mask, false))),
                    (aware, Some((mask, true))),
                ] {
                    let got = traced_walk(&view, q, ef, filter);
                    let expected = single_row_walk(&view, q, ef.max(1), spec);
                    assert_eq!(got, expected, "{} ef={ef}", kernel.name());
                }
            }
        }
    }
}

/// Every distinct entry of a layer search is a candidate, and the results
/// keep the best `ef` of the allowed ones, however many are handed in.
#[test]
fn layer_searches_seed_from_many_and_repeated_entries_as_the_reference_walk_does() {
    let (dim, rows) = (8, 500);
    let mut store = Store::new(dim, 8, 40, 7, Kernel::Scalar);
    store
        .append_unlinked(&tie_corpus(rows, dim, 0x5EED_0008))
        .unwrap();
    let links = store.build_links(false).unwrap();
    store.install_links(links);
    let view = store.view();
    let allow_flags: Vec<bool> = (0..rows).map(|i| i % 3 != 0).collect();
    let mask = Mask::Bools(&allow_flags);
    for q in tie_corpus(4, dim, 0x5EED_0009).chunks_exact(dim) {
        let entries: Vec<Scored> = [3u32, 77, 150, 3, 211, 77, 402, 9, 150]
            .into_iter()
            .map(|node| Scored {
                score: view.kernel.dot(q, view.row(node)),
                node,
            })
            .collect();
        for ef in [1usize, 2, 3, 5, 16] {
            for (allow, spec) in [(None, None), (Some(mask), Some((mask, false)))] {
                let search = LayerSearch {
                    q,
                    ef,
                    layer: 0,
                    allow,
                    patience: Patience::OFF,
                    floor_prune: FloorPrune::OFF,
                    locks: None,
                };
                let mut trace = Trace(Vec::new());
                let mut stats = SearchStats::default();
                let hits = with_scratch(|scratch| {
                    view.search_layer(&search, &entries, scratch, &mut trace, &mut stats);
                    scratch.drain_results()
                });
                let hits: ScoreTrace = hits.iter().map(|s| (s.node, s.score.to_bits())).collect();
                let expected = seeded_walk(&view, q, ef, spec, &entries);
                assert_eq!((trace.0, hits), expected, "ef={ef}");
            }
        }
    }
}
