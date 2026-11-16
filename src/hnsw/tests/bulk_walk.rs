//! Bulk neighbor-list scoring against a reference single-row-dot walk.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use super::super::distance::Kernel;
use super::super::graph::{with_scratch, Filter, Hook, LinkCells, Scored, Store, View};
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
    let ep = descend(view, q);
    let mut beam = ReferenceBeam {
        ef,
        visited: vec![false; view.n()],
        cand: BinaryHeap::new(),
        res: BinaryHeap::new(),
        trace: Vec::new(),
    };
    beam.visited[ep.node as usize] = true;
    beam.cand.push(ep);
    if allow.is_none_or(|(m, _)| m.test(ep.node as usize)) {
        beam.trace.push((ep.node, ep.score.to_bits()));
        beam.res.push(Reverse(ep));
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
                    let mut trace = Trace(Vec::new());
                    let hits = with_scratch(|scratch| {
                        view.walk(
                            q,
                            24,
                            super::super::graph::Steering::filtered(filter),
                            scratch,
                            &mut trace,
                        )
                        .0
                    });
                    let got_hits: Vec<(u32, u32)> = hits
                        .into_iter()
                        .map(|s| (s.node, s.score.to_bits()))
                        .collect();
                    let (exp_trace, exp_hits) = single_row_walk(&view, q, 24, spec);
                    assert_eq!(trace.0, exp_trace, "{} dim={dim}", kernel.name());
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
