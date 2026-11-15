//! The space-saving heavy-hitter list.

use std::collections::HashMap;

/// One heavy hitter: a term and its attributed count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeavyHitter {
    /// The term or token surface form.
    pub term: String,
    /// Number of occurrences the space-saving sketch attributes to it.
    pub count: u64,
}

/// A space-saving top-K sketch: the heaviest terms of the window with
/// near-exact counts. When the table is full, a new term evicts the current
/// minimum and inherits its count plus its own — the classic space-saving
/// rule, which bounds every entry's overestimate by the minimum count in
/// the table. Not thread-safe.
pub struct HeavyHitters {
    capacity: usize,
    counts: HashMap<String, u64>,
}

impl HeavyHitters {
    /// The default list size.
    pub const DEFAULT_CAPACITY: usize = 1024;

    /// A sketch holding at most `capacity` entries (must be positive).
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            capacity,
            counts: HashMap::new(),
        }
    }

    /// Adds `count` occurrences of `term`, evicting the minimum entry when
    /// the table is full.
    pub fn add(&mut self, term: &str, count: u64) {
        assert!(count > 0, "count must be positive");
        if let Some(current) = self.counts.get_mut(term) {
            *current += count;
        } else if self.counts.len() < self.capacity {
            self.counts.insert(term.to_string(), count);
        } else {
            // Space-saving eviction: the newcomer takes the minimum entry's
            // slot and count. A linear scan is fine at K ~ 1024 — it runs
            // only for terms the table has never seen.
            let (min_term, min_count) = self
                .counts
                .iter()
                .min_by_key(|(_, &count)| count)
                .map(|(term, &count)| (term.clone(), count))
                .expect("full table has a minimum");
            self.counts.remove(&min_term);
            self.counts.insert(term.to_string(), min_count + count);
        }
    }

    /// The attributed count of `term`, or 0 when the term is not on the
    /// list. Absence means "not heavy enough to track", not "never seen" —
    /// point queries belong on the count-min sketch.
    pub fn estimate(&self, term: &str) -> u64 {
        self.counts.get(term).copied().unwrap_or(0)
    }

    /// The list, ordered by count descending (ties by term, so snapshots
    /// are deterministic).
    pub fn snapshot(&self) -> Vec<HeavyHitter> {
        let mut entries: Vec<HeavyHitter> = self
            .counts
            .iter()
            .map(|(term, &count)| HeavyHitter {
                term: term.clone(),
                count,
            })
            .collect();
        entries.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.term.cmp(&b.term)));
        entries
    }

    /// The list size K.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Restores a list from a snapshot, replacing any current content.
    pub fn restore(&mut self, entries: Vec<HeavyHitter>) {
        self.counts.clear();
        for entry in entries {
            self.counts.insert(entry.term, entry.count);
        }
    }
}
