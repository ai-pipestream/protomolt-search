//! The coordinator's ring of recent public requests.

use super::*;

/// The coordinator's bounded ring of recent public requests.
#[derive(Debug, Default)]
pub struct RecentRing {
    pub(super) entries: Mutex<VecDeque<RecentQuery>>,
    pub(super) total_seen: AtomicU64,
}

impl RecentRing {
    pub fn push(&self, query: RecentQuery) {
        self.total_seen.fetch_add(1, Ordering::Relaxed);
        let mut entries = self.entries.lock().expect("recent ring mutex poisoned");
        if entries.len() == RECENT_RING {
            entries.pop_front();
        }
        entries.push_back(query);
    }

    /// The newest `limit` entries, newest first.
    pub fn recent(&self, limit: usize) -> Vec<RecentQuery> {
        let entries = self.entries.lock().expect("recent ring mutex poisoned");
        entries.iter().rev().take(limit).cloned().collect()
    }

    pub fn total_seen(&self) -> u64 {
        self.total_seen.load(Ordering::Relaxed)
    }
}

/// Milliseconds since the Unix epoch, for ring entries and snapshots.
pub fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Figures a public response contributes to its ring entry.
#[derive(Debug, Default, Clone)]
pub struct RecentFigures {
    pub hits: u32,
    pub segments_total: u32,
    pub segments_skipped: u32,
    pub candidates_collected: u64,
    pub executed: String,
}
