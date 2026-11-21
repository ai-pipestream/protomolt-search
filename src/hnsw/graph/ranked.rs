//! The two ranked sets of a layer search: the results kept so far and the
//! candidates still to expand. Both are binary heaps over one integer per
//! entry that orders exactly as [`Scored`] does, so a sift compares
//! integers and a search takes the same entries in the same order on
//! every run.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use super::Scored;

/// A [`Scored`] as one integer that orders as it does: the score's place
/// in `f32::total_cmp` order in the high half, the complement of the node
/// in the low half (among equal scores the smaller node is the greater
/// entry). The mapping is one to one, so an entry comes back bit for bit.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Key(u64);

impl Key {
    #[inline]
    fn of(item: Scored) -> Self {
        // Flipping the magnitude bits of a negative score orders the bits
        // as signed integers; flipping the sign bit orders them unsigned.
        let bits = item.score.to_bits() as i32;
        let rank = (bits ^ ((bits >> 31) as u32 >> 1) as i32) as u32 ^ SIGN;
        Self(u64::from(rank) << 32 | u64::from(!item.node))
    }

    #[inline]
    fn scored(self) -> Scored {
        let rank = ((self.0 >> 32) as u32 ^ SIGN) as i32;
        let bits = rank ^ ((rank >> 31) as u32 >> 1) as i32;
        Scored {
            score: f32::from_bits(bits as u32),
            node: !(self.0 as u32),
        }
    }
}

const SIGN: u32 = 1 << 31;

/// What became of an entry offered to [`Results::keep`].
#[derive(Clone, Copy)]
#[cfg_attr(test, derive(Debug, PartialEq))]
pub(in crate::hnsw) enum Kept {
    /// It entered and nothing left.
    Added,
    /// It entered a full set and the worst entry, carried here, left.
    Replaced(Scored),
    /// It does not beat the worst entry of a full set.
    Rejected,
}

impl Kept {
    #[inline]
    pub(in crate::hnsw) fn entered(self) -> bool {
        !matches!(self, Self::Rejected)
    }
}

/// The best entries a search has kept: a min-heap bounded at the beam, so
/// the worst is on top, an entry that cannot beat it is turned away on one
/// comparison, and one that does takes its place with a single sift.
#[derive(Default)]
pub(in crate::hnsw) struct Results(BinaryHeap<Reverse<Key>>);

impl Results {
    #[inline]
    pub(in crate::hnsw) fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub(in crate::hnsw) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(in crate::hnsw) fn clear(&mut self) {
        self.0.clear();
    }

    #[inline]
    pub(in crate::hnsw) fn worst(&self) -> Option<Scored> {
        self.0.peek().map(|worst| worst.0.scored())
    }

    /// Offer `item` to the best `ef` entries. While fewer are kept it
    /// enters; after that it enters only by beating the worst, which leaves.
    #[inline]
    pub(in crate::hnsw) fn keep(&mut self, ef: usize, item: Scored) -> Kept {
        let key = Key::of(item);
        if self.0.len() < ef {
            self.0.push(Reverse(key));
            return Kept::Added;
        }
        match self.0.peek_mut() {
            Some(mut top) if key > top.0 => {
                let worst = top.0;
                *top = Reverse(key);
                Kept::Replaced(worst.scored())
            }
            _ => Kept::Rejected,
        }
    }

    /// Move every entry onto the end of `out`, best first.
    pub(in crate::hnsw) fn drain_into(&mut self, out: &mut Vec<Scored>) {
        let start = out.len();
        out.extend(self.0.drain().map(|kept| kept.0.scored()));
        out[start..].sort_unstable_by(|a, b| b.cmp(a));
    }
}

/// The candidates a search has yet to expand, taken best first.
#[derive(Default)]
pub(in crate::hnsw) struct Frontier(BinaryHeap<Key>);

impl Frontier {
    pub(in crate::hnsw) fn clear(&mut self) {
        self.0.clear();
    }

    /// The candidate [`Self::pop`] would take.
    #[inline]
    pub(in crate::hnsw) fn peek(&self) -> Option<Scored> {
        self.0.peek().map(|best| best.scored())
    }

    /// Take the best candidate.
    #[inline]
    pub(in crate::hnsw) fn pop(&mut self) -> Option<Scored> {
        self.0.pop().map(Key::scored)
    }

    #[inline]
    pub(in crate::hnsw) fn push(&mut self, item: Scored) {
        self.0.push(Key::of(item));
    }
}

#[cfg(test)]
mod tests {
    use super::{Frontier, Kept, Key, Results, Reverse, Scored};
    use std::collections::BinaryHeap;

    /// Scores that tie, repeat, are infinite, signed zeros, subnormal, or
    /// NaNs of either sign and several payloads.
    const SCORES: [f32; 18] = [
        f32::NAN,
        -f32::NAN,
        f32::from_bits(0x7fc0_0001),
        f32::from_bits(0xff80_0001),
        f32::INFINITY,
        f32::NEG_INFINITY,
        0.0,
        -0.0,
        1.0,
        1.0,
        -1.0,
        2.5,
        2.5,
        f32::MIN_POSITIVE,
        f32::from_bits(1),
        f32::from_bits(0x8000_0001),
        f32::MAX,
        f32::MIN,
    ];
    const NODES: [u32; 6] = [0, 1, 2, 3, u32::MAX - 1, u32::MAX];

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        /// An entry from few scores and fewer nodes, so entries tie on
        /// score and repeat outright.
        fn scored(&mut self) -> Scored {
            Scored {
                score: SCORES[self.next() as usize % SCORES.len()],
                node: NODES[self.next() as usize % NODES.len()],
            }
        }
    }

    fn bits(item: Scored) -> (u32, u32) {
        (item.score.to_bits(), item.node)
    }

    #[test]
    fn a_key_orders_as_its_entry_and_gives_it_back_bit_for_bit() {
        let entries: Vec<Scored> = SCORES
            .iter()
            .flat_map(|&score| NODES.iter().map(move |&node| Scored { score, node }))
            .collect();
        for &a in &entries {
            assert_eq!(bits(Key::of(a).scored()), bits(a));
            for &b in &entries {
                assert_eq!(Key::of(a).cmp(&Key::of(b)), a.cmp(&b), "{a:?} {b:?}");
            }
        }
        let mut rng = Rng(0x5EED_000A);
        for _ in 0..200_000 {
            let draw = rng.next();
            let (a, b) = (
                Scored {
                    score: f32::from_bits(draw as u32),
                    node: (draw >> 32) as u32 >> (draw % 32),
                },
                rng.scored(),
            );
            assert_eq!(bits(Key::of(a).scored()), bits(a));
            assert_eq!(Key::of(a).cmp(&Key::of(b)), a.cmp(&b), "{a:?} {b:?}");
        }
    }

    /// Offer `item` to both and require the same entry to leave both.
    fn keep_in_both(
        results: &mut Results,
        heap: &mut BinaryHeap<Reverse<Scored>>,
        ef: usize,
        item: Scored,
    ) {
        let enters = heap.len() < ef || heap.peek().is_some_and(|worst| item > worst.0);
        heap.push(Reverse(item));
        let left = if heap.len() > ef {
            heap.pop().map(|worst| bits(worst.0))
        } else {
            None
        };
        let kept = results.keep(ef, item);
        assert_eq!(kept.entered(), enters, "ef={ef} {item:?}");
        match kept {
            Kept::Added => assert_eq!(left, None, "ef={ef} {item:?}"),
            Kept::Replaced(worst) => assert_eq!(Some(bits(worst)), left, "ef={ef} {item:?}"),
            Kept::Rejected => assert_eq!(Some(bits(item)), left, "ef={ef} {item:?}"),
        }
        assert_eq!(results.len(), heap.len());
        assert_eq!(results.is_empty(), heap.is_empty());
        assert_eq!(
            results.worst().map(bits),
            heap.peek().map(|worst| bits(worst.0))
        );
    }

    #[test]
    fn results_keep_what_a_trimmed_heap_of_entries_keeps() {
        for ef in [0usize, 1, 2, 3, 8, 64, 1000] {
            let mut rng = Rng(0x5EED_000B + ef as u64);
            let mut results = Results::default();
            let mut heap = BinaryHeap::new();
            for round in 0..3 {
                for _ in 0..1500 {
                    keep_in_both(&mut results, &mut heap, ef, rng.scored());
                }
                let mut expected: Vec<Scored> = heap.drain().map(|kept| kept.0).collect();
                expected.sort_unstable_by(|a, b| b.cmp(a));
                // Drained onto what `out` already holds, best first.
                let first = Scored {
                    score: 9.0,
                    node: round,
                };
                let mut out = vec![first];
                results.drain_into(&mut out);
                assert!(results.is_empty());
                assert_eq!(bits(out[0]), bits(first));
                assert_eq!(
                    out[1..].iter().copied().map(bits).collect::<Vec<_>>(),
                    expected.into_iter().map(bits).collect::<Vec<_>>(),
                    "ef={ef}"
                );
            }
        }
    }

    #[test]
    fn a_full_set_turns_away_what_cannot_beat_its_worst() {
        let mut results = Results::default();
        let entry = |score: f32, node: u32| Scored { score, node };
        assert_eq!(results.keep(0, entry(1.0, 0)), Kept::Rejected);
        assert!(results.is_empty());
        assert_eq!(results.keep(2, entry(1.0, 4)), Kept::Added);
        assert_eq!(results.keep(2, entry(1.0, 2)), Kept::Added);
        // A tie on score is settled by the smaller node.
        assert_eq!(results.keep(2, entry(1.0, 9)), Kept::Rejected);
        assert_eq!(results.keep(2, entry(1.0, 4)), Kept::Rejected);
        let kept = results.keep(2, entry(1.0, 3));
        assert_eq!(kept, Kept::Replaced(entry(1.0, 4)));
        assert_eq!(results.keep(2, entry(-0.0, 0)), Kept::Rejected);
        assert_eq!(results.worst().map(bits), Some(bits(entry(1.0, 3))));
    }

    #[test]
    fn the_frontier_gives_candidates_in_the_order_of_a_heap_of_entries() {
        for seed in 0..200u64 {
            let mut rng = Rng(0x5EED_1000 + seed);
            let mut frontier = Frontier::default();
            let mut heap = BinaryHeap::new();
            for _ in 0..600 {
                if rng.next().is_multiple_of(3) {
                    assert_eq!(frontier.pop().map(bits), heap.pop().map(bits));
                } else {
                    let item = rng.scored();
                    frontier.push(item);
                    heap.push(item);
                }
                assert_eq!(frontier.peek().map(bits), heap.peek().copied().map(bits));
            }
            while let Some(expected) = heap.pop() {
                assert_eq!(frontier.pop().map(bits), Some(bits(expected)), "{seed}");
            }
            assert_eq!(frontier.pop().map(bits), None);
            frontier.push(rng.scored());
            frontier.clear();
            assert_eq!(frontier.peek().map(bits), None);
        }
    }
}
