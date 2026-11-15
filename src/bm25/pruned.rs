//! Flat and fused lexical scans share a rounded-score top-k contract.

use super::pruned_scan::{Cutoff, Scan};
use super::ranking::HeapEntry;
use super::{
    floor_seed, passes, CandidateHook, ChainCtx, FieldQuery, FilterCtx, LiveFloorHook, PruneStats,
    TopKOptions,
};

pub(super) struct Controls<'a> {
    pub k: usize,
    pub floor: f64,
    pub chain: ChainCtx<'a>,
    pub filter: FilterCtx<'a>,
}

pub(super) fn run(
    fields: &[FieldQuery<'_>],
    mut controls: Controls<'_>,
    prune: &mut PruneStats,
    live: &mut Option<LiveFloorHook<'_>>,
    candidate: &mut Option<CandidateHook<'_>>,
) -> Option<Vec<HeapEntry>> {
    if controls.k == 0 {
        return Some(Vec::new());
    }
    let pairs = super::pruned_cursor::open(fields, prune)?;
    let count = fields.iter().map(|field| field.terms.len()).sum();
    let mut scan = Scan::new(pairs, count);
    while !scan.pairs.is_empty() {
        let kth = (scan.heap.len() >= controls.k)
            .then(|| scan.heap.peek().expect("full heap").score as f32);
        if let Some(hook) = live.as_mut() {
            if let Some(external) = hook(kth.map(floor_seed)) {
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
        let entry = scan.score(doc, controls.chain, prune);
        if passes(controls.filter, doc) && entry.score >= controls.floor {
            if let Some(sink) = candidate.as_mut() {
                sink(doc, entry.score as f32);
            }
            if scan.heap.len() < controls.k {
                scan.heap.push(entry);
            } else if entry < *scan.heap.peek().expect("full heap") {
                scan.heap.pop();
                scan.heap.push(entry);
            }
        }
        scan.advance(doc, false);
    }
    Some(scan.finish(prune))
}

pub(super) fn flat(
    query: super::exhaustive::Query<'_>,
    options: TopKOptions,
    prune: &mut PruneStats,
    mut live: Option<LiveFloorHook<'_>>,
    mut candidate: Option<CandidateHook<'_>>,
) -> Vec<super::ScoredDoc> {
    let TopKOptions {
        k,
        floor,
        block_max,
    } = options;
    if k == 0 {
        return Vec::new();
    }
    let fields = [FieldQuery {
        index: query.store,
        terms: query.terms,
        stats: query.stats.clone(),
        params: query.params,
        weight: 1.0,
    }];
    let controls = Controls {
        k,
        floor,
        chain: query.chain,
        filter: query.filter,
    };
    let entries = block_max
        .then(|| run(&fields, controls, prune, &mut live, &mut candidate))
        .flatten();
    let Some(entries) = entries else {
        let docs = query.above(k, floor);
        if let Some(sink) = candidate.as_mut() {
            for doc in &docs {
                sink(doc.doc_id, doc.score as f32);
            }
        }
        return docs;
    };
    entries
        .into_iter()
        .map(|entry| super::ScoredDoc {
            doc_id: entry.doc_id,
            score: entry.score,
            term_offsets: (0..query.terms.len())
                .filter(|&term| entry.mask >> term & 1 == 1)
                .map(|term| {
                    (
                        term,
                        query
                            .store
                            .posting_offsets(&query.terms[term], entry.doc_id),
                    )
                })
                .collect(),
        })
        .collect()
}

pub(super) fn fused(
    fields: &[FieldQuery<'_>],
    options: TopKOptions,
    filter: FilterCtx<'_>,
    prune: &mut PruneStats,
    mut live: Option<LiveFloorHook<'_>>,
    mut candidate: Option<CandidateHook<'_>>,
) -> Vec<super::FusedDoc> {
    let TopKOptions {
        k,
        floor,
        block_max,
    } = options;
    if k == 0 {
        return Vec::new();
    }
    let controls = Controls {
        k,
        floor,
        chain: None,
        filter,
    };
    let entries = block_max
        .then(|| run(fields, controls, prune, &mut live, &mut candidate))
        .flatten();
    let Some(entries) = entries else {
        let docs = super::exhaustive::fused(fields, k, filter, floor.is_finite().then_some(floor));
        if let Some(sink) = candidate.as_mut() {
            for doc in &docs {
                sink(doc.doc_id, doc.score as f32);
            }
        }
        return docs;
    };
    let pairs: Vec<_> = fields
        .iter()
        .enumerate()
        .flat_map(|(field, query)| (0..query.terms.len()).map(move |term| (field, term)))
        .collect();
    entries
        .into_iter()
        .map(|entry| super::FusedDoc {
            doc_id: entry.doc_id,
            score: entry.score,
            term_offsets: pairs
                .iter()
                .enumerate()
                .filter(|&(position, _)| entry.mask >> position & 1 == 1)
                .map(|(_, &(field, term))| {
                    (
                        field,
                        term,
                        fields[field]
                            .index
                            .posting_offsets(&fields[field].terms[term], entry.doc_id),
                    )
                })
                .collect(),
        })
        .collect()
}
