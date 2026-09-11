//! The rejection log line sink: one line per rejection, on stderr for
//! the operator and into a ring the tests drain. The watcher and the
//! receiver call `record` at the point a rejection is recorded; the line
//! on stderr is the production behavior, the ring is the test seam.
use std::collections::VecDeque;
use std::sync::Mutex;

static RING: Mutex<VecDeque<String>> = Mutex::new(VecDeque::new());
const RING_LEN: usize = 64;

/// Record one rejection line: stderr always, the ring for tests.
pub fn record(line: String) {
    eprintln!("{line}");
    if let Ok(mut ring) = RING.lock() {
        if ring.len() >= RING_LEN {
            ring.pop_front();
        }
        ring.push_back(line);
    }
}

/// Drain the ring. A test seam; the line on stderr is the behavior.
#[cfg(feature = "fault-injection")]
pub fn take() -> Vec<String> {
    RING.lock()
        .map(|mut ring| std::mem::take(&mut *ring).into_iter().collect())
        .unwrap_or_default()
}
