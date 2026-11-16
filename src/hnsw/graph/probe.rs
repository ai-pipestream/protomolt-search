//! The far probe: a links-only sample of the rows around a query's landing
//! point, judged against the mask's share of the index.

use crate::vector::Mask;

use super::{LinkCells, Scratch, SearchStats, View};

/// The far probe trips when the mask's share of the rows predicts at least
/// this many allowed rows in the sample, and fewer than one in
/// `FAR_PROBE_RATIO` of them are there. Below the minimum the sample says
/// nothing (a Poisson count of mean 8 is under 2 three times in a
/// thousand).
const FAR_PROBE_MIN_EXPECTED: f64 = 8.0;
const FAR_PROBE_RATIO: f64 = 4.0;
/// The probe stops reading neighbor lists once the sample predicts this
/// many allowed rows: a wide mask is judged from a few lists.
const FAR_PROBE_ENOUGH_EXPECTED: f64 = 32.0;

/// What the far probe saw: `hits` allowed rows in a `sample` of the rows
/// around the query, against the mask's `share` of the whole index.
pub(super) struct Probe {
    pub(super) sample: usize,
    pub(super) hits: usize,
    pub(super) share: f64,
}

impl Probe {
    /// The neighborhood is all but empty of allowed rows.
    pub(super) fn far(&self) -> bool {
        let expected = self.sample as f64 * self.share;
        expected >= FAR_PROBE_MIN_EXPECTED && (self.hits as f64) * FAR_PROBE_RATIO < expected
    }

    /// The allowed share of the sample (`0.0` when `sample == 0`).
    pub(super) fn local_share(&self) -> f64 {
        if self.sample == 0 {
            0.0
        } else {
            self.hits as f64 / self.sample as f64
        }
    }
}

impl<L: LinkCells + ?Sized> View<'_, L> {
    /// The far probe: how many of the rows around where the descent landed
    /// does the mask allow? The sample is `entry`'s layer-0 neighbors and
    /// theirs, read from the links alone (no row is touched), and no larger
    /// than the mask's share needs to be judged
    /// (`FAR_PROBE_ENOUGH_EXPECTED`). A mask spread
    /// through the graph holds its share of the sample, one gathered around
    /// the query holds more; a mask on a region away from the query (one
    /// court, one tenant) holds almost none of it, and a walk from here
    /// would have to cross rows it may not score to rows it cannot rank.
    pub(super) fn probe(
        &self,
        entry: u32,
        allow: Mask<'_>,
        allowed: usize,
        scratch: &mut Scratch,
        stats: &mut SearchStats,
    ) -> Probe {
        scratch.begin(self.n());
        scratch.visit(entry);
        let mut near = std::mem::take(&mut scratch.routes);
        let mut nbrs = std::mem::take(&mut scratch.nbrs);
        self.copy_links(entry, 0, None, &mut near);
        near.retain(|&v| scratch.visit(v));
        let mut sample = 1 + near.len();
        let mut hits = usize::from(allow.test(entry as usize))
            + near.iter().filter(|&&v| allow.test(v as usize)).count();
        let share = allowed as f64 / self.n() as f64;
        let enough = (FAR_PROBE_ENOUGH_EXPECTED / share).ceil();
        for &v in near.iter() {
            if sample as f64 >= enough {
                break;
            }
            self.copy_links(v, 0, None, &mut nbrs);
            for &w in nbrs.iter() {
                if scratch.visit(w) {
                    sample += 1;
                    hits += usize::from(allow.test(w as usize));
                }
            }
        }
        scratch.routes = near;
        scratch.nbrs = nbrs;
        stats.probed += sample as u64;
        Probe {
            sample,
            hits,
            share,
        }
    }
}
