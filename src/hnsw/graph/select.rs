//! The neighbor heuristic.

use super::super::distance::prefetch;
use super::{LinkCells, Scored, View};

impl<L: LinkCells + ?Sized> View<'_, L> {
    /// Paper Algorithm 4 on directions. Candidates arrive best first by
    /// inner product with the base row (the ranking the metric gives);
    /// a candidate is kept when its direction is closer to the base than
    /// to every neighbor already kept: `cos(c, base) > cos(c, s)`. Both
    /// sides share the factor `1/|c|`, so the test multiplies the known
    /// inner products by the other row's `1/|row|` only. With unit rows this
    /// is the paper's test on inner products; with rows of different
    /// lengths a long row cannot crowd out its neighbors just by being long.
    pub(in crate::hnsw) fn select(
        &self,
        base_inv_norm: f32,
        pool: &[Scored],
        cap: usize,
        sel: &mut Vec<u32>,
    ) {
        sel.clear();
        let lines = self.prefetch_lines();
        for (idx, c) in pool.iter().enumerate() {
            if sel.len() >= cap {
                break;
            }
            if let Some(ahead) = pool.get(idx + 2) {
                prefetch(self.row(ahead.node), lines);
            }
            let to_base = c.score * base_inv_norm;
            let row_c = self.row(c.node);
            let mut keep = true;
            let mut at = 0usize;
            let mut dots = [0.0f32; 4];
            // Short-circuit in 4-neighbor batches on targets with software
            // prefetch: rejected candidates fail against an early,
            // higher-scoring neighbor without scoring the rest of `sel`, while
            // kept candidates score four neighbors per pass over `row_c`.
            // `sel` stays cache-resident across pool iterations, so `lines = 0`
            // avoids redundant prefetches; on 128-bit NEON (`lines == 0`)
            // per-neighbor short-circuiting avoids extra FMAs on early
            // rejections.
            while lines > 0 && at + 4 <= sel.len() {
                self.kernel
                    .score_nodes(row_c, self.rows, self.dim, &sel[at..at + 4], 0, &mut dots);
                let s0 = sel[at] as usize;
                let s1 = sel[at + 1] as usize;
                let s2 = sel[at + 2] as usize;
                let s3 = sel[at + 3] as usize;
                let closer = [
                    dots[0] * self.inv_norm[s0] < to_base,
                    dots[1] * self.inv_norm[s1] < to_base,
                    dots[2] * self.inv_norm[s2] < to_base,
                    dots[3] * self.inv_norm[s3] < to_base,
                ];
                if !closer.into_iter().all(|ok| ok) {
                    keep = false;
                    break;
                }
                at += 4;
            }
            if keep {
                keep = sel[at..].iter().all(|&s| {
                    let to_s = self.kernel.dot(row_c, self.row(s)) * self.inv_norm[s as usize];
                    to_s < to_base
                });
            }
            if keep {
                sel.push(c.node);
            }
        }
    }
}
