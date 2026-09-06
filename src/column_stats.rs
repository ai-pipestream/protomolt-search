//! Typed column statistics with exact integer partials and legacy double views.
use crate::{
    filter::NumBound,
    pb::{
        column_stats::ExactInteger, ColumnStats, ScalarValueType as Type, SignedColumnStats,
        UnsignedColumnStats,
    },
};
use tonic::Status;

fn signed_sum(s: &SignedColumnStats) -> i128 {
    (i128::from(s.sum_hi) << 64) | i128::from(s.sum_lo)
}
fn unsigned_sum(s: &UnsignedColumnStats) -> u128 {
    (u128::from(s.sum_hi) << 64) | u128::from(s.sum_lo)
}
fn set_signed(s: &mut SignedColumnStats, sum: i128) {
    s.sum_hi = (sum >> 64) as i64;
    s.sum_lo = sum as u64;
}
fn set_unsigned(s: &mut UnsignedColumnStats, sum: u128) {
    s.sum_hi = (sum >> 64) as u64;
    s.sum_lo = sum as u64;
}
fn invalid(field: &str, reason: &str) -> Status {
    Status::failed_precondition(format!("stats column {field:?}: {reason}"))
}
fn overflow(field: &str) -> Status {
    Status::out_of_range(format!("stats column {field:?}: count or sum overflow"))
}

pub(crate) struct Collector(ColumnStats);
impl Collector {
    pub(crate) fn new(field: &str, ty: Type) -> Self {
        Self(ColumnStats {
            field: field.into(),
            known: ty != Type::Unspecified,
            value_type: ty as i32,
            exact_integer: match ty {
                Type::Integer => Some(ExactInteger::Signed(SignedColumnStats::default())),
                Type::UnsignedInteger => {
                    Some(ExactInteger::Unsigned(UnsignedColumnStats::default()))
                }
                _ => None,
            },
            ..Default::default()
        })
    }
    pub(crate) fn observe(&mut self, value: NumBound) -> Result<(), Status> {
        let out = &mut self.0;
        let first = out.count == 0;
        let x = match (value, out.exact_integer.as_mut()) {
            (NumBound::I(v), Some(ExactInteger::Signed(s))) => {
                let sum = signed_sum(s)
                    .checked_add(i128::from(v))
                    .ok_or_else(|| overflow(&out.field))?;
                set_signed(s, sum);
                s.min = if first { v } else { s.min.min(v) };
                s.max = if first { v } else { s.max.max(v) };
                v as f64
            }
            (NumBound::U(v), Some(ExactInteger::Unsigned(s))) => {
                let sum = unsigned_sum(s)
                    .checked_add(u128::from(v))
                    .ok_or_else(|| overflow(&out.field))?;
                set_unsigned(s, sum);
                s.min = if first { v } else { s.min.min(v) };
                s.max = if first { v } else { s.max.max(v) };
                v as f64
            }
            (NumBound::F(v), None) if out.value_type == Type::Number as i32 => v,
            _ => {
                return Err(invalid(
                    &out.field,
                    "value does not match declared numeric type",
                ))
            }
        };
        out.count = out
            .count
            .checked_add(1)
            .ok_or_else(|| overflow(&out.field))?;
        out.min = if first { x } else { out.min.min(x) };
        out.max = if first { x } else { out.max.max(x) };
        out.sum += x;
        if !out.sum.is_finite() {
            return Err(overflow(&out.field));
        }
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<ColumnStats, Status> {
        validate(&self.0)?;
        Ok(self.0)
    }
}

fn validate(s: &ColumnStats) -> Result<(), Status> {
    let bad = |reason| invalid(&s.field, reason);
    let ty = Type::try_from(s.value_type).map_err(|_| bad("unknown value_type"))?;
    if !s.known {
        return if ty == Type::Unspecified
            && s.count == 0
            && s.exact_integer.is_none()
            && s.min == 0.0
            && s.max == 0.0
            && s.sum == 0.0
            && s.mean == 0.0
        {
            Ok(())
        } else {
            Err(bad("unknown column must have no values or typed summary"))
        };
    }
    if ![s.min, s.max, s.sum, s.mean].iter().all(|v| v.is_finite()) || s.min > s.max {
        return Err(bad("double summary must be finite and ordered"));
    }
    if s.count == 0 && (s.min != 0.0 || s.max != 0.0 || s.sum != 0.0 || s.mean != 0.0) {
        return Err(bad("empty summary must be zero"));
    }
    match (ty, &s.exact_integer) {
        (Type::Number, None) => Ok(()),
        (Type::Integer, Some(ExactInteger::Signed(exact))) => {
            let sum = signed_sum(exact);
            if s.count == 0 {
                if exact.min != 0 || exact.max != 0 || sum != 0 {
                    return Err(bad("empty signed summary must be zero"));
                }
            } else {
                if exact.min > exact.max || (s.count == 1 && exact.min != exact.max) {
                    return Err(bad("invalid signed extrema"));
                }
                // Both extrema must actually occur. Products fit i128 for a u64 count.
                let n = i128::from(s.count - 1);
                if sum < i128::from(exact.min) * n + i128::from(exact.max)
                    || sum > i128::from(exact.max) * n + i128::from(exact.min)
                {
                    return Err(bad("signed sum is impossible for the count and extrema"));
                }
            }
            if s.min != exact.min as f64 || s.max != exact.max as f64 {
                return Err(bad("double extrema disagree with signed summary"));
            }
            Ok(())
        }
        (Type::UnsignedInteger, Some(ExactInteger::Unsigned(exact))) => {
            let sum = unsigned_sum(exact);
            if s.count == 0 {
                if exact.min != 0 || exact.max != 0 || sum != 0 {
                    return Err(bad("empty unsigned summary must be zero"));
                }
            } else {
                if exact.min > exact.max || (s.count == 1 && exact.min != exact.max) {
                    return Err(bad("invalid unsigned extrema"));
                }
                let n = u128::from(s.count - 1);
                if sum < u128::from(exact.min) * n + u128::from(exact.max)
                    || sum > u128::from(exact.max) * n + u128::from(exact.min)
                {
                    return Err(bad("unsigned sum is impossible for the count and extrema"));
                }
            }
            if s.min != exact.min as f64 || s.max != exact.max as f64 {
                return Err(bad("double extrema disagree with unsigned summary"));
            }
            Ok(())
        }
        _ => Err(bad(
            "known column needs a numeric type and its matching exact summary",
        )),
    }
}

pub(crate) fn merge(
    requested: &[String],
    shares: &[Vec<ColumnStats>],
) -> Result<Vec<ColumnStats>, Status> {
    let mut out: Vec<_> = requested
        .iter()
        .map(|name| Collector::new(name, Type::Unspecified).0)
        .collect();
    for share in shares {
        if share.len() != out.len() {
            return Err(Status::failed_precondition(
                "stats response has wrong field count",
            ));
        }
        for (acc, s) in out.iter_mut().zip(share) {
            if acc.field != s.field {
                return Err(invalid(&acc.field, "child returned a different field"));
            }
            validate(s)?;
            if !s.known {
                continue;
            }
            if !acc.known {
                *acc = s.clone();
                continue;
            }
            if acc.value_type != s.value_type {
                return Err(invalid(
                    &acc.field,
                    "incompatible numeric types across shards",
                ));
            }
            let count = acc
                .count
                .checked_add(s.count)
                .ok_or_else(|| overflow(&acc.field))?;
            match (&mut acc.exact_integer, &s.exact_integer) {
                (Some(ExactInteger::Signed(a)), Some(ExactInteger::Signed(b))) => {
                    let sum = signed_sum(a)
                        .checked_add(signed_sum(b))
                        .ok_or_else(|| overflow(&acc.field))?;
                    set_signed(a, sum);
                    if s.count > 0 {
                        a.min = if acc.count == 0 {
                            b.min
                        } else {
                            a.min.min(b.min)
                        };
                        a.max = if acc.count == 0 {
                            b.max
                        } else {
                            a.max.max(b.max)
                        };
                    }
                }
                (Some(ExactInteger::Unsigned(a)), Some(ExactInteger::Unsigned(b))) => {
                    let sum = unsigned_sum(a)
                        .checked_add(unsigned_sum(b))
                        .ok_or_else(|| overflow(&acc.field))?;
                    set_unsigned(a, sum);
                    if s.count > 0 {
                        a.min = if acc.count == 0 {
                            b.min
                        } else {
                            a.min.min(b.min)
                        };
                        a.max = if acc.count == 0 {
                            b.max
                        } else {
                            a.max.max(b.max)
                        };
                    }
                }
                (None, None) => {}
                _ => return Err(invalid(&acc.field, "incompatible exact summaries")),
            }
            if s.count > 0 {
                acc.min = if acc.count == 0 {
                    s.min
                } else {
                    acc.min.min(s.min)
                };
                acc.max = if acc.count == 0 {
                    s.max
                } else {
                    acc.max.max(s.max)
                };
                acc.sum += s.sum;
                if !acc.sum.is_finite() {
                    return Err(overflow(&acc.field));
                }
            }
            acc.count = count;
        }
    }
    for acc in &mut out {
        if !acc.known {
            return Err(Status::invalid_argument(format!(
            "no shard has stats column {:?}: check --numeric-fields / --integer-fields / --unsigned-integer-fields",acc.field)));
        }
        acc.mean = if acc.count == 0 {
            0.0
        } else {
            acc.sum / acc.count as f64
        };
        validate(acc)?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn collect(ty: Type, values: &[NumBound]) -> ColumnStats {
        let mut c = Collector::new("value", ty);
        for &v in values {
            c.observe(v).unwrap();
        }
        c.finish().unwrap()
    }
    #[test]
    fn wide_signed_and_unsigned_sums_and_empty_declarations() {
        for ty in [Type::Integer, Type::UnsignedInteger, Type::Number] {
            let empty = collect(ty, &[]);
            assert!(empty.known);
            assert_eq!(empty.value_type, ty as i32);
            assert_eq!(empty.count, 0);
            assert_eq!(empty.min, 0.0);
            assert_eq!(empty.max, 0.0);
            assert_eq!(
                merge(&["value".into()], &[vec![empty.clone()]]).unwrap()[0],
                empty
            );
        }
        let signed = collect(
            Type::Integer,
            &[
                NumBound::I(i64::MIN),
                NumBound::I(i64::MIN),
                NumBound::I(i64::MAX),
            ],
        );
        let unsigned = collect(
            Type::UnsignedInteger,
            &[NumBound::U(u64::MAX), NumBound::U(u64::MAX), NumBound::U(1)],
        );
        let Some(ExactInteger::Signed(s)) = &signed.exact_integer else {
            panic!()
        };
        assert_eq!(
            signed_sum(s),
            i128::from(i64::MIN) * 2 + i128::from(i64::MAX)
        );
        let Some(ExactInteger::Unsigned(u)) = &unsigned.exact_integer else {
            panic!()
        };
        assert_eq!(unsigned_sum(u), u128::from(u64::MAX) * 2 + 1);
        for (ty, full) in [(Type::Integer, signed), (Type::UnsignedInteger, unsigned)] {
            let absent = collect(Type::Unspecified, &[]);
            let empty = collect(ty, &[]);
            for shares in [
                vec![
                    vec![absent.clone()],
                    vec![empty.clone()],
                    vec![full.clone()],
                ],
                vec![
                    vec![full.clone()],
                    vec![empty.clone()],
                    vec![absent.clone()],
                ],
            ] {
                let merged = merge(&["value".into()], &shares).unwrap().remove(0);
                assert_eq!(merged.count, full.count);
                assert_eq!(merged.exact_integer, full.exact_integer);
            }
        }
    }
    #[test]
    fn maximum_count_summaries_fit_and_count_overflow_refuses() {
        let count = u64::MAX;
        for ty in [Type::Integer, Type::UnsignedInteger] {
            let value = if ty == Type::Integer {
                NumBound::I(i64::MIN)
            } else {
                NumBound::U(u64::MAX)
            };
            let one = collect(ty, &[value]);
            let mut many = one.clone();
            many.count = count;
            match many.exact_integer.as_mut().unwrap() {
                ExactInteger::Signed(s) => set_signed(s, i128::from(i64::MIN) * i128::from(count)),
                ExactInteger::Unsigned(s) => {
                    set_unsigned(s, u128::from(u64::MAX) * u128::from(count))
                }
            }
            many.sum = one.sum * count as f64;
            use prost::Message;
            let decoded = ColumnStats::decode(many.encode_to_vec().as_slice()).unwrap();
            assert_eq!(decoded, many);
            validate(&many).unwrap();
            assert_eq!(
                merge(&["value".into()], &[vec![many.clone()]]).unwrap()[0].exact_integer,
                many.exact_integer
            );
            assert_eq!(
                merge(&["value".into()], &[vec![many], vec![one]])
                    .unwrap_err()
                    .code(),
                tonic::Code::OutOfRange
            );
        }
    }
    #[test]
    fn malformed_partials_and_empty_type_conflicts_refuse() {
        let full = collect(
            Type::UnsignedInteger,
            &[NumBound::U(u64::MAX), NumBound::U(0)],
        );
        let mut bad = vec![];
        let mut s = full.clone();
        s.field = "wrong".into();
        bad.push(s);
        let mut s = full.clone();
        s.value_type = 999;
        bad.push(s);
        let mut s = full.clone();
        s.value_type = 0;
        bad.push(s);
        let mut s = full.clone();
        s.known = false;
        bad.push(s);
        let mut s = full.clone();
        s.exact_integer = None;
        bad.push(s);
        let mut s = full.clone();
        s.count = 1;
        bad.push(s);
        let mut s = full.clone();
        s.count = 0;
        bad.push(s);
        let mut s = full.clone();
        s.min = 1.0;
        bad.push(s);
        let mut s = full.clone();
        s.sum = f64::INFINITY;
        bad.push(s);
        let mut s = full.clone();
        let Some(ExactInteger::Unsigned(e)) = s.exact_integer.as_mut() else {
            panic!()
        };
        e.sum_hi = u64::MAX;
        bad.push(s);
        let mut s = full.clone();
        let Some(ExactInteger::Unsigned(e)) = s.exact_integer.as_mut() else {
            panic!()
        };
        e.min = u64::MAX;
        e.max = 0;
        bad.push(s);
        for s in bad {
            assert_eq!(
                merge(&["value".into()], &[vec![s]]).unwrap_err().code(),
                tonic::Code::FailedPrecondition
            );
        }
        for ty in [Type::Integer, Type::Number] {
            assert_eq!(
                merge(
                    &["value".into()],
                    &[
                        vec![collect(Type::UnsignedInteger, &[])],
                        vec![collect(ty, &[])]
                    ]
                )
                .unwrap_err()
                .code(),
                tonic::Code::FailedPrecondition
            );
        }
        assert!(merge(&["value".into()], &[vec![]]).is_err());
        let mut c = Collector::new("value", Type::Number);
        c.observe(NumBound::F(f64::MAX)).unwrap();
        assert_eq!(
            c.observe(NumBound::F(f64::MAX)).unwrap_err().code(),
            tonic::Code::OutOfRange
        );
        let a = collect(Type::Number, &[NumBound::F(f64::MAX)]);
        assert_eq!(
            merge(&["value".into()], &[vec![a.clone()], vec![a]])
                .unwrap_err()
                .code(),
            tonic::Code::OutOfRange
        );
    }
}
