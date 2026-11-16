//! RFC 3339 to epoch microseconds, by exact integer math.

use super::CelError;

use super::ast::Ast;
use super::refuse;

pub(super) fn timestamp_argument(args: &[Ast]) -> Result<i64, CelError> {
    match args {
        [Ast::Str(s)] => parse_rfc3339_micros(s),
        _ => Err(refuse(
            "timestamp() takes one RFC 3339 string, e.g. \
             timestamp(\"2015-01-01T00:00:00Z\")",
        )),
    }
}

/// Days from 1970-01-01 for a civil date (Howard Hinnant's
/// days_from_civil): pure integer arithmetic, exact over the whole i64
/// range this can produce.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (i64::from(if m > 2 { m - 3 } else { m + 9 })) + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// An RFC 3339 text under parse, with the refusals that name the part of
/// it that is wrong.
struct Instant<'a> {
    text: &'a str,
    bytes: &'a [u8],
}

impl Instant<'_> {
    fn bad(&self, what: &str) -> CelError {
        refuse(format!("timestamp {:?}: {what}", self.text))
    }

    fn digits(&self, range: std::ops::Range<usize>) -> Result<i64, CelError> {
        let part = self.bytes.get(range).ok_or_else(|| self.bad("truncated"))?;
        if !part.iter().all(u8::is_ascii_digit) {
            return Err(self.bad("expected digits"));
        }
        Ok(std::str::from_utf8(part)
            .expect("ascii digits")
            .parse::<i64>()
            .expect("bounded digit run"))
    }

    fn sep(&self, i: usize, c: u8) -> Result<(), CelError> {
        if self.bytes.get(i) == Some(&c) {
            Ok(())
        } else {
            Err(self.bad(&format!("expected {:?} at byte {i}", c as char)))
        }
    }

    /// The `YYYY-MM-DDTHH:MM:SS` in the first 19 bytes, as seconds since the
    /// epoch before any offset applies.
    fn civil_seconds(&self) -> Result<i64, CelError> {
        let year = self.digits(0..4)?;
        self.sep(4, b'-')?;
        let month = self.digits(5..7)? as u32;
        self.sep(7, b'-')?;
        let day = self.digits(8..10)? as u32;
        if !matches!(self.bytes.get(10), Some(b'T') | Some(b't')) {
            return Err(self.bad("expected 'T' between date and time"));
        }
        let hour = self.digits(11..13)?;
        self.sep(13, b':')?;
        let minute = self.digits(14..16)?;
        self.sep(16, b':')?;
        let second = self.digits(17..19)?;
        if !(1..=12).contains(&month) {
            return Err(self.bad("month is outside 1..=12"));
        }
        if day < 1 || day > days_in_month(year, month) {
            return Err(self.bad("day is outside the month"));
        }
        if hour > 23 || minute > 59 {
            return Err(self.bad("time of day is outside 23:59"));
        }
        if second > 59 {
            // RFC 3339 admits :60 leap seconds; epoch math cannot place
            // one honestly, so it refuses rather than aliasing it onto a
            // neighbor.
            return Err(self.bad("second is outside 0..=59 (leap seconds do not compile)"));
        }
        let days = days_from_civil(year, month, day);
        Ok(days * 86_400 + hour * 3_600 + minute * 60 + second)
    }

    /// An optional fractional second at `*i`, in microseconds. The first
    /// six digits count; the remainder floors away, as at ingest.
    fn fraction(&self, i: &mut usize) -> Result<i64, CelError> {
        let b = self.bytes;
        let mut micros_frac: i64 = 0;
        if b.get(*i) == Some(&b'.') {
            *i += 1;
            let start = *i;
            while b.get(*i).is_some_and(u8::is_ascii_digit) {
                *i += 1;
            }
            if *i == start {
                return Err(self.bad("empty fractional second"));
            }
            for (n, c) in b[start..*i].iter().take(6).enumerate() {
                micros_frac += i64::from(c - b'0') * 10_i64.pow(5 - n as u32);
            }
        }
        Ok(micros_frac)
    }

    /// The zone at `*i`: `Z`, or a `+HH:MM` / `-HH:MM` offset in seconds.
    fn offset(&self, i: &mut usize) -> Result<i64, CelError> {
        match self.bytes.get(*i) {
            Some(b'Z') | Some(b'z') => {
                *i += 1;
                Ok(0)
            }
            Some(&sign @ (b'+' | b'-')) => {
                let oh = self.digits(*i + 1..*i + 3)?;
                self.sep(*i + 3, b':')?;
                let om = self.digits(*i + 4..*i + 6)?;
                if oh > 23 || om > 59 {
                    return Err(self.bad("offset is outside 23:59"));
                }
                *i += 6;
                let magnitude = oh * 3600 + om * 60;
                Ok(if sign == b'+' { magnitude } else { -magnitude })
            }
            _ => Err(self.bad("expected 'Z' or a +HH:MM offset")),
        }
    }
}

/// Parse an RFC 3339 instant (`YYYY-MM-DDTHH:MM:SS[.frac][Z|+HH:MM]`)
/// into epoch microseconds, refusing malformed input by the part that
/// is wrong. Fractional digits beyond microseconds floor away, the
/// same drop (and the same direction) as
/// [`crate::node::timestamp_to_epoch_micros`] at ingest, so a filter
/// bound and a stored value truncate identically.
pub(super) fn parse_rfc3339_micros(s: &str) -> Result<i64, CelError> {
    let instant = Instant {
        text: s,
        bytes: s.as_bytes(),
    };
    let local_seconds = instant.civil_seconds()?;
    let mut i = 19;
    let micros_frac = instant.fraction(&mut i)?;
    let offset_seconds = instant.offset(&mut i)?;
    if i != s.len() {
        return Err(instant.bad("trailing input after the offset"));
    }
    let seconds = local_seconds - offset_seconds;
    seconds
        .checked_mul(1_000_000)
        .and_then(|us| us.checked_add(micros_frac))
        .ok_or_else(|| instant.bad("does not fit epoch microseconds"))
}
