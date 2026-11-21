//! Filter selectivity math and settle bounds for dense execution planning.

#[cfg(test)]
mod tests;

/// Selectivity of a request without a filter: the whole corpus.
pub const UNFILTERED_PPM: u32 = 1_000_000;

/// Upper bound on `finish_settle_us` in a policy point (60 seconds).
pub const MAX_FINISH_SETTLE_US: u32 = 60_000_000;

/// Filter selectivity in parts per million: `admitted` rows of `rows`,
/// floored, never below 1 when anything is admitted, and the whole corpus
/// (1,000,000) when nothing filters.
pub fn selectivity_ppm(admitted: u64, rows: u64) -> u32 {
    if rows == 0 || admitted >= rows {
        return UNFILTERED_PPM;
    }
    if admitted == 0 {
        return 0;
    }
    let ppm = admitted.saturating_mul(u64::from(UNFILTERED_PPM)) / rows;
    u32::try_from(ppm.max(1)).unwrap_or(UNFILTERED_PPM)
}
