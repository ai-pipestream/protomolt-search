//! Flat and fused block-max scans share one loop and one emission path.

use super::pruned_scan::{Cutoff, Scan};
use super::ranking::HeapEntry;
use super::request::{Controls, Flat, Hooks, TopK};
use super::{exhaustive, passes, FieldQuery, FusedDoc, PruneStats, ScoredDoc};

/// The block-max loop over every (field, term) pair, or `None` when
/// pruning is unavailable and the caller must fall back.
fn run(
    fields: &[FieldQuery<'_>],
    mut controls: Controls<'_>,
    prune: &mut PruneStats,
    hooks: &mut Hooks<'_>,
) -> Option<Vec<HeapEntry>> {
    if !controls.block_max {
        return None;
    }
    let pairs = super::pruned_cursor::open(fields, prune)?;
    let count = fields.iter().map(|field| field.terms.len()).sum();
    let mut scan = Scan::new(pairs, count);
    while !scan.pairs.is_empty() {
        let kth = (scan.heap.len() >= controls.k)
            .then(|| super::ranking::wire_score(scan.heap.peek().expect("full heap").score));
        if let Some(hook) = hooks.live.as_mut() {
            if let Some(external) = hook(kth.map(f32::next_down)) {
                if f64::from(external) > controls.floor {
                    controls.floor = f64::from(external);
                }
            }
        }
        let cutoff = Cutoff {
            floor: controls.floor,
            kth,
            chain: controls.chain,
        };
        if scan.whole_inert(&cutoff) {
            break;
        }
        if scan.skip_bounds(&cutoff) {
            continue;
        }
        let Some(doc) = scan.candidate(&cutoff) else {
            continue;
        };
        if cutoff.inert(scan.bound(fields, doc)) {
            scan.advance(doc, true);
            continue;
        }
        let score = scan.score(doc, controls.chain, prune);
        if passes(controls.filter, doc) && score >= controls.floor {
            if let Some(sink) = hooks.candidate.as_mut() {
                sink(doc, score as f32);
            }
            scan.admit(doc, score, controls.k, controls.include_offsets);
        }
        scan.advance(doc, false);
    }
    Some(scan.finish(prune))
}

/// Decode offsets for the matched `(field, term)` pairs in query order.
fn matched(fields: &[FieldQuery<'_>], entry: &HeapEntry) -> Vec<super::FusedTermOffset> {
    if entry.matched.is_empty() {
        return Vec::new();
    }
    let pairs: Vec<_> = fields
        .iter()
        .enumerate()
        .flat_map(|(field, query)| (0..query.terms.len()).map(move |term| (field, term)))
        .collect();
    entry
        .matched
        .iter()
        .map(|&position| {
            let (field, term) = pairs[position];
            let query = &fields[field];
            let offsets = query
                .index
                .posting_offsets(&query.terms[term], entry.doc_id);
            (field, term, offsets)
        })
        .collect()
}

/// Run the block-max loop with the request's skip counters, if any.
fn scan(
    fields: &[FieldQuery<'_>],
    controls: Controls<'_>,
    prune: Option<&mut PruneStats>,
    hooks: &mut Hooks<'_>,
) -> Option<Vec<HeapEntry>> {
    let mut scratch = PruneStats::default();
    let prune = match prune {
        Some(prune) => prune,
        None => &mut scratch,
    };
    run(fields, controls, prune, hooks)
}

pub(super) fn flat(query: &Flat<'_>, request: TopK<'_>) -> Vec<ScoredDoc> {
    let (controls, mut hooks, prune) = request.split();
    if controls.k == 0 {
        return Vec::new();
    }
    let fields = [FieldQuery {
        index: query.store,
        terms: query.terms,
        stats: query.stats.clone(),
        params: query.params,
        weight: 1.0,
    }];
    let Some(entries) = scan(&fields, controls, prune, &mut hooks) else {
        let docs = exhaustive::flat(query, &controls);
        hooks.emit_all(docs.iter().map(|doc| (doc.doc_id, doc.score)));
        return docs;
    };
    entries
        .iter()
        .map(|entry| ScoredDoc {
            doc_id: entry.doc_id,
            score: entry.score,
            term_offsets: matched(&fields, entry)
                .into_iter()
                .map(|(_, term, offsets)| (term, offsets))
                .collect(),
        })
        .collect()
}

pub(super) fn fused(fields: &[FieldQuery<'_>], request: TopK<'_>) -> Vec<FusedDoc> {
    let (controls, mut hooks, prune) = request.split();
    if controls.k == 0 {
        return Vec::new();
    }
    let Some(entries) = scan(fields, controls, prune, &mut hooks) else {
        let docs = exhaustive::fused(fields, &controls, None);
        hooks.emit_all(docs.iter().map(|doc| (doc.doc_id, doc.score)));
        return docs;
    };
    entries
        .iter()
        .map(|entry| FusedDoc {
            doc_id: entry.doc_id,
            score: entry.score,
            term_offsets: matched(fields, entry),
        })
        .collect()
}
