//! Tests for filter selectivity math and bounds.

use super::*;

#[test]
fn selectivity_is_floored_parts_per_million() {
    assert_eq!(selectivity_ppm(0, 1000), 0);
    assert_eq!(selectivity_ppm(1, 3_000_000), 1);
    assert_eq!(selectivity_ppm(250, 1000), 250_000);
    assert_eq!(selectivity_ppm(999_999, 1_000_000), 999_999);
    assert_eq!(selectivity_ppm(1000, 1000), UNFILTERED_PPM);
    assert_eq!(selectivity_ppm(1001, 1000), UNFILTERED_PPM);
    assert_eq!(selectivity_ppm(5, 0), UNFILTERED_PPM);
    assert_eq!(MAX_FINISH_SETTLE_US, 60_000_000);
}
