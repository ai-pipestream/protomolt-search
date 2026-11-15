//! One channel's window state, and the merge of shard-level snapshots.

use std::collections::HashMap;

use super::count_min::CountMinSketch;
use super::heavy_hitters::{HeavyHitter, HeavyHitters};
use super::hyper_log_log::HyperLogLog;
use crate::pb::storage::{VocabChannel, VocabChannelSnapshot, VocabHeavyHitter, VocabSnapshot};

/// The per-channel state of one vocabulary window: the three sketches plus
/// the document and occurrence counters. Interior mutability is the
/// listener's job (`VocabularyListener` holds the lock).
pub struct WindowState {
    channel: VocabChannel,
    count_min: CountMinSketch,
    hyper_log_log: HyperLogLog,
    heavy_hitters: HeavyHitters,
    documents: u64,
    occurrences: u64,
}

impl WindowState {
    /// A fresh window for `channel` with a `top_k` heavy-hitter list.
    pub fn new(channel: VocabChannel, top_k: usize) -> Self {
        Self {
            channel,
            count_min: CountMinSketch::new(),
            hyper_log_log: HyperLogLog::new(),
            heavy_hitters: HeavyHitters::new(top_k.max(1)),
            documents: 0,
            occurrences: 0,
        }
    }

    /// Adds one term with its frequency from one document.
    pub fn add_term(&mut self, term: &str, frequency: u64) {
        self.count_min.add(term, frequency);
        self.hyper_log_log.add(term);
        self.heavy_hitters.add(term, frequency);
        self.occurrences += frequency;
    }

    /// Counts one more document into the window.
    pub fn add_document(&mut self) {
        self.documents += 1;
    }

    /// The channel this state belongs to.
    pub fn channel(&self) -> VocabChannel {
        self.channel
    }

    /// Documents accumulated in this window.
    pub fn documents(&self) -> u64 {
        self.documents
    }

    /// Total occurrences (with multiplicity) accumulated in this window.
    pub fn occurrences(&self) -> u64 {
        self.occurrences
    }

    /// The estimated distinct-term cardinality of this window.
    pub fn cardinality_estimate(&self) -> f64 {
        self.hyper_log_log.estimate()
    }

    /// The heavy hitters, ordered by count descending.
    pub fn heavy_hitters(&self) -> Vec<HeavyHitter> {
        self.heavy_hitters.snapshot()
    }

    /// The estimated frequency of `term`: its heavy-hitter count when it is
    /// on the list, otherwise the count-min point query. This is the count a
    /// drift comparison uses for every term on the union of the two top-K
    /// lists.
    pub fn estimate(&self, term: &str) -> u64 {
        let heavy = self.heavy_hitters.estimate(term);
        if heavy > 0 {
            heavy
        } else {
            self.count_min.estimate(term)
        }
    }

    /// A detached copy of the cardinality sketch, for union computations.
    pub fn cardinality_sketch(&self) -> HyperLogLog {
        self.hyper_log_log.clone()
    }

    /// Serializes this window's channel state into its persisted form.
    pub fn to_proto(&self) -> VocabChannelSnapshot {
        VocabChannelSnapshot {
            channel: self.channel as i32,
            documents: self.documents,
            term_occurrences: self.occurrences,
            hll_precision: self.hyper_log_log.precision() as u32,
            hll_registers: self.hyper_log_log.to_bytes(),
            cms_depth: self.count_min.depth() as u32,
            cms_width: self.count_min.width() as u32,
            cms_table: self.count_min.to_bytes(),
            heavy_hitters: self
                .heavy_hitters
                .snapshot()
                .into_iter()
                .map(|entry| VocabHeavyHitter {
                    term: entry.term,
                    count: entry.count,
                })
                .collect(),
        }
    }

    /// Restores a window's channel state from its persisted form.
    pub fn from_proto(snapshot: &VocabChannelSnapshot) -> Result<Self, String> {
        let heavy_hitters: Vec<HeavyHitter> = snapshot
            .heavy_hitters
            .iter()
            .map(|h| HeavyHitter {
                term: h.term.clone(),
                count: h.count,
            })
            .collect();
        let mut state = Self::new(
            VocabChannel::try_from(snapshot.channel)
                .map_err(|_| format!("unknown vocab channel {}", snapshot.channel))?,
            snapshot.heavy_hitters.len().max(1),
        );
        state.documents = snapshot.documents;
        state.occurrences = snapshot.term_occurrences;
        state.hyper_log_log =
            HyperLogLog::from_bytes(snapshot.hll_precision as usize, &snapshot.hll_registers)?;
        state.count_min = CountMinSketch::from_bytes(
            snapshot.cms_depth as usize,
            snapshot.cms_width as usize,
            &snapshot.cms_table,
            snapshot.term_occurrences,
        )?;
        state.heavy_hitters.restore(heavy_hitters);
        Ok(state)
    }

    /// Merges `other` (same channel) into this window: cardinality and
    /// count-min merge cell-wise, heavy hitters merge by re-running
    /// space-saving over the combined lists (this is what coordinator-side
    /// aggregation of shard-level snapshots does). Counters sum.
    pub fn merge(&mut self, other: &Self) -> Result<(), String> {
        if other.channel != self.channel {
            return Err(format!(
                "channels differ: {:?} vs {:?}",
                other.channel, self.channel
            ));
        }
        self.hyper_log_log.merge(&other.hyper_log_log)?;
        self.count_min.merge(&other.count_min)?;
        // Re-run space-saving over the combined lists: each list's counts
        // are near-exact for its own heaviest terms, and their per-term
        // sums are the best estimate of the combined distribution's head.
        let capacity = self
            .heavy_hitters
            .capacity()
            .max(other.heavy_hitters.capacity());
        let mut combined: HashMap<String, u64> = HashMap::new();
        for entry in self
            .heavy_hitters
            .snapshot()
            .into_iter()
            .chain(other.heavy_hitters.snapshot())
        {
            *combined.entry(entry.term).or_insert(0) += entry.count;
        }
        let mut entries: Vec<HeavyHitter> = combined
            .into_iter()
            .map(|(term, count)| HeavyHitter { term, count })
            .collect();
        entries.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.term.cmp(&b.term)));
        entries.truncate(capacity);
        self.heavy_hitters = HeavyHitters::new(capacity);
        self.heavy_hitters.restore(entries);
        self.documents += other.documents;
        self.occurrences += other.occurrences;
        Ok(())
    }
}

/// Merges shard-level snapshots into one (coordinator-side aggregation):
/// same-channel states merge per [`WindowState::merge`], timestamps span
/// the earliest start to the latest seal, and the sequence is the maximum
/// seen. Channels present in some snapshots but not others carry through
/// as-is.
pub fn merge_snapshots(snapshots: &[VocabSnapshot]) -> Result<VocabSnapshot, String> {
    let mut by_channel: HashMap<i32, WindowState> = HashMap::new();
    let mut sequence = 0u64;
    let mut started = i64::MAX;
    let mut sealed = i64::MIN;
    for snapshot in snapshots {
        sequence = sequence.max(snapshot.sequence);
        started = started.min(snapshot.started_epoch_millis);
        sealed = sealed.max(snapshot.sealed_epoch_millis);
        for channel_snapshot in &snapshot.channels {
            let state = WindowState::from_proto(channel_snapshot)?;
            by_channel
                .entry(channel_snapshot.channel)
                .and_modify(|merged| {
                    // Same-channel merge cannot fail on shape: snapshots of
                    // one deployment share sketch parameters.
                    if let Err(e) = merged.merge(&state) {
                        eprintln!("vocab: channel merge failed: {e}");
                    }
                })
                .or_insert(state);
        }
    }
    let mut channels: Vec<VocabChannelSnapshot> = by_channel
        .into_values()
        .map(|state| state.to_proto())
        .collect();
    channels.sort_by_key(|c| c.channel);
    Ok(VocabSnapshot {
        sequence,
        started_epoch_millis: started,
        sealed_epoch_millis: sealed,
        channels,
    })
}
