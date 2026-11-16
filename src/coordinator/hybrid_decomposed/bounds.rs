//! Fused lower bounds that drive the decomposed vector floor.

use super::*;

pub(super) struct LowerBounds {
    pub(super) k: usize,
    pub(super) vector_weight: f64,
    pub(super) bm25_weight: f64,
    pub(super) weighted_bm25_max: f64,
    pub(super) minimum_vector: Option<f32>,
    pub(super) heap: std::collections::BinaryHeap<std::cmp::Reverse<F64Ord>>,
    pub(super) lower: Option<f64>,
    #[cfg(feature = "net")]
    pub(super) last_floor: f32,
}

impl LowerBounds {
    pub(super) fn new(round: HybridRound<'_>) -> Self {
        let k = round.query().input().k() as usize;
        let legs = round.query().legs();
        Self {
            k,
            vector_weight: f64::from(legs.vector_weight),
            bm25_weight: f64::from(legs.bm25_weight),
            weighted_bm25_max: 0.0,
            minimum_vector: (legs.min_vector_score > 0.0).then_some(legs.min_vector_score),
            heap: std::collections::BinaryHeap::with_capacity(k + 1),
            lower: None,
            #[cfg(feature = "net")]
            last_floor: f32::NEG_INFINITY,
        }
    }

    pub(super) fn fused(&self, vector: f32, bm25: f32) -> f64 {
        self.vector_weight * f64::from(vector) + self.bm25_weight * f64::from(bm25)
    }

    pub(super) fn push(&mut self, vector: f32, bm25: f32) {
        if self.minimum_vector.is_some_and(|minimum| vector < minimum) {
            return;
        }
        let value = self.fused(vector, bm25);
        if self.heap.len() < self.k {
            self.heap.push(std::cmp::Reverse(F64Ord(value)));
        } else if self.heap.peek().is_some_and(|bound| value > bound.0 .0) {
            self.heap.pop();
            self.heap.push(std::cmp::Reverse(F64Ord(value)));
        }
    }

    pub(super) fn initial_floor(&mut self) -> Option<f32> {
        self.lower = (self.heap.len() == self.k).then(|| self.heap.peek().expect("full heap").0 .0);
        let floor = self
            .lower
            .map(|lower| decomposed_floor(lower, self.weighted_bm25_max, self.vector_weight));
        let initial = match (floor, self.minimum_vector) {
            (Some(f), Some(m)) => Some(f.max(m)),
            (a, b) => a.or(b),
        };
        #[cfg(feature = "net")]
        {
            self.last_floor = initial.unwrap_or(f32::NEG_INFINITY);
        }
        initial
    }

    #[cfg(feature = "net")]
    pub(super) fn raised_floor(&mut self) -> Option<f32> {
        if self.heap.len() == self.k {
            let lower = self.heap.peek().expect("full heap").0 .0;
            if self.lower.is_none_or(|previous| lower > previous) {
                self.lower = Some(lower);
                let floor = decomposed_floor(lower, self.weighted_bm25_max, self.vector_weight);
                if floor > self.last_floor {
                    self.last_floor = floor;
                    return Some(floor);
                }
            }
        }
        None
    }
}
