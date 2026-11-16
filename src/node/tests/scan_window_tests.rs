//! Scan-rate window sampling.

use super::*;

/// The window reports unknown below the minimum sample count, then
/// the ratio of the bytes and active time it holds; a scan that
/// streamed nothing is not a sample.
#[test]
fn the_window_reports_unknown_until_it_holds_enough_scans() {
    let window = ScanWindow::new();
    assert_eq!(
        window.rate(),
        ScanRate {
            window_ms: SCAN_WINDOW_MS,
            ..Default::default()
        }
    );
    window.observe(0, 5_000);
    assert_eq!(window.rate().samples, 0, "an empty scan is not a sample");
    for _ in 0..(SCAN_WINDOW_MIN_SAMPLES - 1) {
        window.observe(1_000, 1_000_000);
    }
    let rate = window.rate();
    assert_eq!(rate.samples as usize, SCAN_WINDOW_MIN_SAMPLES - 1);
    assert_eq!(rate.bytes_per_second, 0, "below the minimum is unknown");
    assert!(rate.observed_unix_ms > 0);
    window.observe(1_000, 1_000_000);
    let rate = window.rate();
    assert_eq!(rate.samples as usize, SCAN_WINDOW_MIN_SAMPLES);
    // 4,000 bytes in 4 ms is one million bytes per second.
    assert_eq!(rate.bytes_per_second, 1_000_000);
    // The window keeps at most SCAN_WINDOW_SAMPLES scans.
    for _ in 0..(2 * SCAN_WINDOW_SAMPLES) {
        window.observe(2_000, 1_000_000);
    }
    let rate = window.rate();
    assert_eq!(rate.samples as usize, SCAN_WINDOW_SAMPLES);
    assert_eq!(
        rate.bytes_per_second, 2_000_000,
        "only the newest scans remain"
    );
    window.clear();
    assert_eq!(window.rate().samples, 0);
}
