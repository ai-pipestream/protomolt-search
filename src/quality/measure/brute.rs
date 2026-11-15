//! The exhaustive FP32 ground truth: one multi-threaded pass over the
//! rows keeping a bounded top-`k` per query under the rerank's order.

/// One candidate under the rerank's total order: a higher score wins,
/// then the lower id.
#[derive(Clone, Copy, PartialEq)]
struct Candidate {
    score: f32,
    id: u64,
}

impl Eq for Candidate {}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| other.id.cmp(&self.id))
    }
}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// A bounded top-`k` under [`Candidate`]'s order: a min-heap whose root
/// is the worst candidate kept.
struct TopK {
    k: usize,
    heap: std::collections::BinaryHeap<std::cmp::Reverse<Candidate>>,
}

impl TopK {
    fn new(k: usize) -> Self {
        TopK {
            k,
            heap: std::collections::BinaryHeap::with_capacity(k + 1),
        }
    }

    fn offer(&mut self, candidate: Candidate) {
        if self.heap.len() < self.k {
            self.heap.push(std::cmp::Reverse(candidate));
        } else if let Some(worst) = self.heap.peek() {
            if candidate > worst.0 {
                self.heap.pop();
                self.heap.push(std::cmp::Reverse(candidate));
            }
        }
    }

    fn merge(&mut self, other: TopK) {
        for std::cmp::Reverse(candidate) in other.heap {
            self.offer(candidate);
        }
    }

    /// Best first.
    fn ids(self) -> Vec<u64> {
        let mut all: Vec<Candidate> = self.heap.into_iter().map(|r| r.0).collect();
        all.sort_by(|a, b| b.cmp(a));
        all.into_iter().map(|c| c.id).collect()
    }
}

/// Exhaustive FP32 top-`k` over `rows` for every query at once: the
/// rerank's own dot product and total order (score descending, id
/// ascending). One pass over the rows, split across the machine's
/// threads, each thread keeping a bounded top-`k` per query; a corpus
/// of 87 million rows is read once, not once per query.
pub(super) fn brute_topk_many(
    rows: &[f32],
    dim: usize,
    queries: &[f32],
    k: usize,
) -> Vec<Vec<u64>> {
    let queries: Vec<&[f32]> = queries.chunks_exact(dim).collect();
    let n_rows = rows.len() / dim;
    let threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .clamp(1, 64)
        .min(n_rows.max(1));
    let per_thread = n_rows.div_ceil(threads);
    let merged: Vec<TopK> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let queries = &queries;
                let first = t * per_thread;
                let last = ((t + 1) * per_thread).min(n_rows);
                scope.spawn(move || {
                    let mut tops: Vec<TopK> = queries.iter().map(|_| TopK::new(k)).collect();
                    for id in first..last {
                        let row = &rows[id * dim..(id + 1) * dim];
                        for (top, query) in tops.iter_mut().zip(queries.iter()) {
                            top.offer(Candidate {
                                score: crate::exact_vectors::dot(row, query),
                                id: id as u64,
                            });
                        }
                    }
                    tops
                })
            })
            .collect();
        let mut merged: Vec<TopK> = queries.iter().map(|_| TopK::new(k)).collect();
        for handle in handles {
            for (into, part) in merged.iter_mut().zip(handle.join().expect("brute thread")) {
                into.merge(part);
            }
        }
        merged
    });
    merged.into_iter().map(TopK::ids).collect()
}

#[cfg(test)]
mod brute_tests {
    #[test]
    fn brute_topk_many_keeps_the_total_order_across_threads() {
        // 1000 rows of dim 2, three queries; the answer must equal a
        // full sort under (score desc, id asc) regardless of thread split.
        let dim = 2;
        let rows: Vec<f32> = (0..1000u32)
            .flat_map(|i| {
                [
                    ((i * 7919) % 101) as f32 / 101.0,
                    ((i * 104729) % 97) as f32 / 97.0,
                ]
            })
            .collect();
        let queries = [1.0f32, 0.0, 0.0, 1.0, 0.5, 0.5];
        let k = 17;
        let got = super::brute_topk_many(&rows, dim, &queries, k);
        for (q, ids) in queries.chunks_exact(dim).zip(&got) {
            let mut scored: Vec<(u64, f32)> = rows
                .chunks_exact(dim)
                .enumerate()
                .map(|(id, row)| (id as u64, crate::exact_vectors::dot(row, q)))
                .collect();
            scored.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            let want: Vec<u64> = scored.iter().take(k).map(|(id, _)| *id).collect();
            assert_eq!(ids, &want);
        }
    }
}
