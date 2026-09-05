//! Calendar bucketing for date histograms (`docs/aggregations.md`,
//! "Date histograms"): proleptic Gregorian civil-date arithmetic over
//! epoch microseconds, the unit the timestamp column stores
//! (`TimestampValue`). Hand-rolled from Howard Hinnant's
//! `days_from_civil` / `civil_from_days`, so the engine carries no
//! calendar dependency and the bucket boundaries are exact integers.
//!
//! A bucket is named by its start instant: the first microsecond of
//! the minute, hour, day, ISO week (Monday), month, quarter, or year
//! that contains the value, in a fixed offset from UTC. Shards fold by
//! that start instant, so the coordinator merges buckets by key the
//! same way it merges fixed-interval indexes.

use crate::pb::CalendarInterval;

/// Microseconds per second: the timestamp column's unit against the
/// second, the unit civil arithmetic runs in.
pub const MICROS_PER_SECOND: i64 = 1_000_000;

const SECONDS_PER_DAY: i64 = 86_400;

/// The widest offset from UTC any zone uses, in minutes (UTC+14 and
/// UTC-12 exist; the bound is symmetric for simplicity).
pub const MAX_UTC_OFFSET_MINUTES: i32 = 18 * 60;

/// Floor division: the quotient rounded toward negative infinity.
pub fn floor_div(a: i64, b: i64) -> i64 {
    let q = a / b;
    if (a % b != 0) && ((a < 0) != (b < 0)) {
        q - 1
    } else {
        q
    }
}

/// Floor modulus: always in `[0, b)` for positive `b`.
pub fn floor_mod(a: i64, b: i64) -> i64 {
    a - floor_div(a, b) * b
}

/// Days since 1970-01-01 of a proleptic Gregorian civil date
/// (`m` in 1..=12, `d` in 1..=31).
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = floor_div(y, 400);
    let yoe = y - era * 400;
    let mp = i64::from((m + 9) % 12);
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The civil date `(year, month, day)` of a day count since
/// 1970-01-01.
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = floor_div(z, 146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Decode the wire calendar unit; `Unspecified` and unknown values
/// are `None`.
pub fn interval_of(raw: i32) -> Option<CalendarInterval> {
    match CalendarInterval::try_from(raw) {
        Ok(CalendarInterval::Unspecified) | Err(_) => None,
        Ok(unit) => Some(unit),
    }
}

/// The lowercase unit name for messages.
pub fn interval_name(unit: CalendarInterval) -> &'static str {
    match unit {
        CalendarInterval::Unspecified => "unspecified",
        CalendarInterval::Minute => "minute",
        CalendarInterval::Hour => "hour",
        CalendarInterval::Day => "day",
        CalendarInterval::Week => "week",
        CalendarInterval::Month => "month",
        CalendarInterval::Quarter => "quarter",
        CalendarInterval::Year => "year",
    }
}

/// The start instant, in epoch micros, of the calendar bucket holding
/// `micros` in the zone at `utc_offset_minutes`. `None` when the
/// arithmetic leaves i64 (an instant within a few hours of either end
/// of the representable range).
pub fn bucket_start(micros: i64, unit: CalendarInterval, utc_offset_minutes: i32) -> Option<i64> {
    let offset = i64::from(utc_offset_minutes)
        .checked_mul(60)?
        .checked_mul(MICROS_PER_SECOND)?;
    let local = micros.checked_add(offset)?;
    let secs = floor_div(local, MICROS_PER_SECOND);
    let days = floor_div(secs, SECONDS_PER_DAY);
    let start_secs = match unit {
        CalendarInterval::Unspecified => return None,
        CalendarInterval::Minute => secs - floor_mod(secs, 60),
        CalendarInterval::Hour => secs - floor_mod(secs, 3600),
        CalendarInterval::Day => days.checked_mul(SECONDS_PER_DAY)?,
        CalendarInterval::Week => {
            // 1970-01-01 was a Thursday; Monday is 0.
            let weekday = floor_mod(days + 3, 7);
            (days - weekday).checked_mul(SECONDS_PER_DAY)?
        }
        CalendarInterval::Month => {
            let (y, m, _) = civil_from_days(days);
            days_from_civil(y, m, 1).checked_mul(SECONDS_PER_DAY)?
        }
        CalendarInterval::Quarter => {
            let (y, m, _) = civil_from_days(days);
            let first = ((m - 1) / 3) * 3 + 1;
            days_from_civil(y, first, 1).checked_mul(SECONDS_PER_DAY)?
        }
        CalendarInterval::Year => {
            let (y, _, _) = civil_from_days(days);
            days_from_civil(y, 1, 1).checked_mul(SECONDS_PER_DAY)?
        }
    };
    start_secs
        .checked_mul(MICROS_PER_SECOND)?
        .checked_sub(offset)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn micros(y: i64, m: u32, d: u32, hh: i64, mm: i64, ss: i64) -> i64 {
        (days_from_civil(y, m, d) * SECONDS_PER_DAY + hh * 3600 + mm * 60 + ss) * MICROS_PER_SECOND
    }

    #[test]
    fn civil_round_trips_across_eras_and_leap_days() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(days_from_civil(2000, 2, 29), 11_016);
        assert_eq!(civil_from_days(11_016), (2000, 2, 29));
        assert_eq!(civil_from_days(days_from_civil(2000, 3, 1)), (2000, 3, 1));
        // 1900 is not a leap year; 2000 is; 2024 is.
        assert_eq!(
            days_from_civil(1900, 3, 1) - days_from_civil(1900, 2, 28),
            1
        );
        assert_eq!(
            days_from_civil(2000, 3, 1) - days_from_civil(2000, 2, 28),
            2
        );
        assert_eq!(
            days_from_civil(2024, 3, 1) - days_from_civil(2024, 2, 28),
            2
        );
        for z in [-1_000_000, -719_468, -366, -1, 1, 365, 60_000, 2_932_896] {
            let (y, m, d) = civil_from_days(z);
            assert_eq!(days_from_civil(y, m, d), z, "day {z}");
        }
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
        assert_eq!(
            civil_from_days(days_from_civil(-4713, 11, 24)),
            (-4713, 11, 24)
        );
    }

    #[test]
    fn floor_division_rounds_toward_negative_infinity() {
        assert_eq!(floor_div(7, 2), 3);
        assert_eq!(floor_div(-7, 2), -4);
        assert_eq!(floor_mod(-7, 2), 1);
        assert_eq!(floor_div(-8, 2), -4);
        assert_eq!(floor_mod(-8, 2), 0);
        assert_eq!(floor_div(-1, MICROS_PER_SECOND), -1);
        assert_eq!(floor_mod(-1, MICROS_PER_SECOND), MICROS_PER_SECOND - 1);
    }

    #[test]
    fn buckets_start_at_calendar_boundaries_in_utc() {
        let t = micros(2024, 2, 29, 13, 47, 5) + 123_456;
        assert_eq!(
            bucket_start(t, CalendarInterval::Minute, 0),
            Some(micros(2024, 2, 29, 13, 47, 0))
        );
        assert_eq!(
            bucket_start(t, CalendarInterval::Hour, 0),
            Some(micros(2024, 2, 29, 13, 0, 0))
        );
        assert_eq!(
            bucket_start(t, CalendarInterval::Day, 0),
            Some(micros(2024, 2, 29, 0, 0, 0))
        );
        // 2024-02-29 is a Thursday; the ISO week began Monday the 26th.
        assert_eq!(
            bucket_start(t, CalendarInterval::Week, 0),
            Some(micros(2024, 2, 26, 0, 0, 0))
        );
        assert_eq!(
            bucket_start(t, CalendarInterval::Month, 0),
            Some(micros(2024, 2, 1, 0, 0, 0))
        );
        assert_eq!(
            bucket_start(t, CalendarInterval::Quarter, 0),
            Some(micros(2024, 1, 1, 0, 0, 0))
        );
        assert_eq!(
            bucket_start(t, CalendarInterval::Year, 0),
            Some(micros(2024, 1, 1, 0, 0, 0))
        );
        // A Monday is its own week start; a Sunday belongs to the
        // Monday six days back.
        let monday = micros(2024, 3, 4, 0, 0, 0);
        assert_eq!(
            bucket_start(monday, CalendarInterval::Week, 0),
            Some(monday)
        );
        let sunday = micros(2024, 3, 10, 23, 59, 59);
        assert_eq!(
            bucket_start(sunday, CalendarInterval::Week, 0),
            Some(monday)
        );
        // Quarter boundaries.
        assert_eq!(
            bucket_start(micros(2023, 12, 31, 23, 0, 0), CalendarInterval::Quarter, 0),
            Some(micros(2023, 10, 1, 0, 0, 0))
        );
        assert_eq!(
            bucket_start(micros(2023, 7, 1, 0, 0, 0), CalendarInterval::Quarter, 0),
            Some(micros(2023, 7, 1, 0, 0, 0))
        );
    }

    #[test]
    fn negative_epochs_bucket_before_1970() {
        let t = micros(1969, 12, 31, 23, 59, 59) + 999_999;
        assert_eq!(
            bucket_start(t, CalendarInterval::Day, 0),
            Some(micros(1969, 12, 31, 0, 0, 0))
        );
        assert_eq!(
            bucket_start(t, CalendarInterval::Month, 0),
            Some(micros(1969, 12, 1, 0, 0, 0))
        );
        assert_eq!(
            bucket_start(t, CalendarInterval::Year, 0),
            Some(micros(1969, 1, 1, 0, 0, 0))
        );
        // 1969-12-31 was a Wednesday; its week began Monday the 29th.
        assert_eq!(
            bucket_start(t, CalendarInterval::Week, 0),
            Some(micros(1969, 12, 29, 0, 0, 0))
        );
        assert_eq!(
            bucket_start(-1, CalendarInterval::Minute, 0),
            Some(micros(1969, 12, 31, 23, 59, 0))
        );
    }

    #[test]
    fn an_offset_moves_the_boundary_and_the_key_is_an_instant() {
        // 2024-03-01T02:30Z is still February 29th in UTC-5, and the
        // bucket key is the instant that local midnight is in UTC.
        let t = micros(2024, 3, 1, 2, 30, 0);
        assert_eq!(
            bucket_start(t, CalendarInterval::Day, -300),
            Some(micros(2024, 2, 29, 5, 0, 0))
        );
        assert_eq!(
            bucket_start(t, CalendarInterval::Month, -300),
            Some(micros(2024, 2, 1, 5, 0, 0))
        );
        // In UTC+5:30 the same instant is 08:00 on March 1st.
        assert_eq!(
            bucket_start(t, CalendarInterval::Day, 330),
            Some(micros(2024, 2, 29, 18, 30, 0))
        );
        assert_eq!(
            bucket_start(t, CalendarInterval::Hour, 330),
            Some(micros(2024, 3, 1, 2, 30, 0))
        );
        // Hour buckets under a half-hour offset start on the local
        // hour, which is the UTC half hour.
        assert_eq!(
            bucket_start(micros(2024, 3, 1, 2, 45, 0), CalendarInterval::Hour, 330),
            Some(micros(2024, 3, 1, 2, 30, 0))
        );
    }

    #[test]
    fn the_ends_of_the_range_are_unbucketable_not_wrong() {
        assert_eq!(bucket_start(i64::MIN, CalendarInterval::Day, 0), None);
        assert_eq!(bucket_start(i64::MAX, CalendarInterval::Day, 1), None);
        assert!(bucket_start(i64::MAX, CalendarInterval::Minute, 0).is_some());
        assert!(bucket_start(
            i64::MIN + MICROS_PER_SECOND * 60,
            CalendarInterval::Minute,
            0
        )
        .is_some());
    }
}
