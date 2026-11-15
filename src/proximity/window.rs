//! The ordered-window phrase predicate over stored token positions.

use crate::postings::Bm25Index;

/// The ordered-window phrase predicate: whether `doc_id` contains the
/// term sequence `terms` (indexes into `terms`' own vocabulary via
/// `sequence`) in order, with at most `slop` intervening token positions
/// over the whole window. `slop == 0` is exact adjacency.
///
/// Exact by construction: for every occurrence of the first term as a
/// window start, the earliest later occurrence of each next term gives
/// the tightest window from that start (choosing a later occurrence can
/// only widen it), so the minimum over starts is the minimum over all
/// ordered assignments. A repeated term in the sequence is fine: each
/// step demands a strictly later ordinal, so one token never serves two
/// slots.
///
/// The field must carry positions (`Bm25Index::has_positions`); callers
/// refuse the request by name before reaching here, and this returns
/// false rather than guessing if they did not.
pub fn phrase_matches(
    index: &dyn Bm25Index,
    terms: &[String],
    sequence: &[usize],
    slop: u32,
    doc_id: u32,
) -> bool {
    if sequence.is_empty() || !index.has_positions() {
        return false;
    }
    if index.has_pos_lengths() {
        return lattice_window(index, terms, sequence, slop, doc_id);
    }
    linear_window(index, terms, sequence, slop, doc_id)
}

/// The window over lattice arcs: each slot continues from where an arc
/// of the previous slot ended, and the gaps are the slop spent.
fn lattice_window(
    index: &dyn Bm25Index,
    terms: &[String],
    sequence: &[usize],
    slop: u32,
    doc_id: u32,
) -> bool {
    let mut per_term: Vec<Option<Vec<(u32, u32)>>> = vec![None; terms.len()];
    for &ti in sequence {
        if per_term[ti].is_none() {
            let Some(positions) = index.posting_positions(&terms[ti], doc_id) else {
                return false;
            };
            if positions.is_empty() {
                return false;
            }
            let lengths = index
                .posting_pos_lengths(&terms[ti], doc_id)
                .filter(|pl| pl.len() == positions.len());
            let arcs: Vec<(u32, u32)> = match lengths {
                Some(pl) => positions
                    .into_iter()
                    .zip(pl)
                    .map(|(p, l)| (p, l.max(1)))
                    .collect(),
                None => positions.into_iter().map(|p| (p, 1)).collect(),
            };
            per_term[ti] = Some(arcs);
        }
    }
    let slots: Vec<&[(u32, u32)]> = sequence
        .iter()
        .map(|&ti| per_term[ti].as_deref().expect("filled above"))
        .collect();
    let max_slop = u64::from(slop);
    let mut frontier: std::collections::BTreeMap<u32, u64> = std::collections::BTreeMap::new();
    for &(pos, len) in slots[0] {
        frontier.entry(pos.saturating_add(len)).or_insert(0);
    }
    for slot in &slots[1..] {
        let mut next_frontier: std::collections::BTreeMap<u32, u64> =
            std::collections::BTreeMap::new();
        for (&cursor, &used_slop) in &frontier {
            let first = slot.partition_point(|&(p, _)| p < cursor);
            for &(pos, len) in &slot[first..] {
                let gap = u64::from(pos - cursor);
                let total_slop = used_slop + gap;
                if total_slop > max_slop {
                    break;
                }
                let end = pos.saturating_add(len);
                next_frontier
                    .entry(end)
                    .and_modify(|prev| *prev = (*prev).min(total_slop))
                    .or_insert(total_slop);
            }
        }
        if next_frontier.is_empty() {
            return false;
        }
        frontier = next_frontier;
    }
    !frontier.is_empty()
}

/// The window over linear token ordinals.
fn linear_window(
    index: &dyn Bm25Index,
    terms: &[String],
    sequence: &[usize],
    slop: u32,
    doc_id: u32,
) -> bool {
    // Positions per sequence slot; a term missing from the document ends
    // it. Terms repeated in the sequence read their positions once.
    let mut per_term: Vec<Option<Vec<u32>>> = vec![None; terms.len()];
    for &ti in sequence {
        if per_term[ti].is_none() {
            match index.posting_positions(&terms[ti], doc_id) {
                Some(positions) if !positions.is_empty() => per_term[ti] = Some(positions),
                _ => return false,
            }
        }
    }
    let slots: Vec<&[u32]> = sequence
        .iter()
        .map(|&ti| per_term[ti].as_deref().expect("filled above"))
        .collect();
    let n = slots.len() as u64;
    let budget = u64::from(slop) + n; // max allowed span = n + slop
    'starts: for &start in slots[0] {
        let mut last = start;
        for slot in &slots[1..] {
            // Earliest occurrence strictly after `last`.
            let next = match slot.binary_search(&(last + 1)) {
                Ok(i) => slot[i],
                Err(i) if i < slot.len() => slot[i],
                Err(_) => continue 'starts,
            };
            last = next;
            if u64::from(last - start) + 1 > budget {
                continue 'starts;
            }
        }
        return true;
    }
    false
}
