//! The bounded window of recent scan costs behind the reported scan rate.

/// Scans the window keeps at most.
pub const SCAN_WINDOW_SAMPLES: usize = 64;
/// A sample older than this leaves the window.
pub const SCAN_WINDOW_MS: u64 = 10 * 60 * 1000;
/// Fewer samples than this report an unknown rate.
pub const SCAN_WINDOW_MIN_SAMPLES: usize = 4;

/// One scan's cost as the window keeps it.
#[derive(Debug, Clone, Copy)]
pub(in crate::node) struct ScanSample {
    pub(in crate::node) at: std::time::Instant,
    pub(in crate::node) unix_ms: u64,
    pub(in crate::node) bytes: u64,
    pub(in crate::node) nanos: u64,
}

/// Observed scan throughput over recent windowed scans (`0` until at least
/// [`SCAN_WINDOW_MIN_SAMPLES`] non-zero kernel passes have been recorded).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScanRate {
    pub bytes_per_second: u64,
    /// When the newest sample was taken (unix ms); 0 with no sample.
    pub observed_unix_ms: u64,
    pub samples: u32,
    pub window_ms: u64,
}

/// The bounded window of recent scans behind [`scan_rate`].
pub struct ScanWindow {
    pub(in crate::node) samples: std::sync::Mutex<std::collections::VecDeque<ScanSample>>,
}

impl ScanWindow {
    pub const fn new() -> Self {
        ScanWindow {
            samples: std::sync::Mutex::new(std::collections::VecDeque::new()),
        }
    }

    /// Record one scan: the bytes its kernel calls streamed and their
    /// active time. A scan that streamed nothing (a lazy index, or a
    /// filter that emptied it) is not a rate sample.
    pub fn observe(&self, bytes: u64, nanos: u64) {
        if bytes == 0 {
            return;
        }
        let now = std::time::Instant::now();
        let sample = ScanSample {
            at: now,
            unix_ms: crate::diagnostics::unix_ms(),
            bytes,
            nanos,
        };
        let mut samples = self.samples.lock().expect("scan window poisoned");
        Self::expire(&mut samples, now);
        if samples.len() == SCAN_WINDOW_SAMPLES {
            samples.pop_front();
        }
        samples.push_back(sample);
    }

    pub(in crate::node) fn expire(
        samples: &mut std::collections::VecDeque<ScanSample>,
        now: std::time::Instant,
    ) {
        let horizon = std::time::Duration::from_millis(SCAN_WINDOW_MS);
        while let Some(oldest) = samples.front() {
            if now.duration_since(oldest.at) > horizon {
                samples.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn rate(&self) -> ScanRate {
        let mut samples = self.samples.lock().expect("scan window poisoned");
        Self::expire(&mut samples, std::time::Instant::now());
        let count = samples.len();
        let observed_unix_ms = samples.back().map_or(0, |s| s.unix_ms);
        let (bytes, nanos) = samples.iter().fold((0u128, 0u128), |(b, n), s| {
            (b + u128::from(s.bytes), n + u128::from(s.nanos))
        });
        let bytes_per_second = if count < SCAN_WINDOW_MIN_SAMPLES || nanos == 0 {
            0
        } else {
            u64::try_from(bytes * 1_000_000_000 / nanos).unwrap_or(u64::MAX)
        };
        ScanRate {
            bytes_per_second,
            observed_unix_ms,
            samples: count as u32,
            window_ms: SCAN_WINDOW_MS,
        }
    }

    #[cfg(test)]
    pub(in crate::node) fn clear(&self) {
        self.samples.lock().expect("scan window poisoned").clear();
    }
}

impl Default for ScanWindow {
    fn default() -> Self {
        Self::new()
    }
}

/// Process-wide scan throughput window reported on lease renewal.
static SCAN_WINDOW: ScanWindow = ScanWindow::new();

/// The node's observed scan rate for the lease renewal.
pub fn scan_rate() -> ScanRate {
    SCAN_WINDOW.rate()
}

/// Account one scan over this shard: the process counters take the pass
/// bytes and time once, and the window takes them as one sample.
pub fn observe_scan(bytes: u64, nanos: u64) {
    crate::metrics::record_scan_pass(bytes, nanos);
    SCAN_WINDOW.observe(bytes, nanos);
}
