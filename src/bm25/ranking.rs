//! Lexical ranking uses the same rounded scores as its wire candidates.

use std::cmp::Ordering;

pub(super) fn wire_score(score: f64) -> f32 {
    (score as f32) + 0.0
}

pub(super) fn compare(a: (u32, f64), b: (u32, f64)) -> Ordering {
    wire_score(b.1)
        .total_cmp(&wire_score(a.1))
        .then(a.0.cmp(&b.0))
}

pub(super) fn sort_truncate<T>(docs: &mut Vec<(u32, f64, T)>, k: usize) {
    docs.sort_by(|a, b| compare((a.0, a.1), (b.0, b.1)));
    docs.truncate(k);
}

#[derive(Debug, Clone, Copy)]
pub(super) struct HeapEntry {
    pub score: f64,
    pub doc_id: u32,
    pub mask: u128,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for HeapEntry {}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        compare((self.doc_id, self.score), (other.doc_id, other.score))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
#[path = "ranking_tests.rs"]
mod tests;
