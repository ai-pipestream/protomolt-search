//! The function vocabulary: names, signatures, arity, and evaluation over
//! present arguments.

use super::ValuesError;

use super::refuse;
use super::types::Val;
use crate::pb;

/// Source-form name of one function, for refusals.
pub(super) fn fn_display(f: pb::ValueFn) -> &'static str {
    match f {
        pb::ValueFn::Abs => "math.abs()",
        pb::ValueFn::Sign => "math.sign()",
        pb::ValueFn::Ceil => "math.ceil()",
        pb::ValueFn::Floor => "math.floor()",
        pb::ValueFn::Round => "math.round()",
        pb::ValueFn::Trunc => "math.trunc()",
        pb::ValueFn::Sqrt => "math.sqrt()",
        pb::ValueFn::IsNan => "math.isNaN()",
        pb::ValueFn::IsInf => "math.isInf()",
        pb::ValueFn::IsFinite => "math.isFinite()",
        pb::ValueFn::Greatest => "math.greatest()",
        pb::ValueFn::Least => "math.least()",
        pb::ValueFn::Ln => "engine.ln()",
        pb::ValueFn::Exp => "engine.exp()",
        pb::ValueFn::Log10 => "engine.log10()",
        pb::ValueFn::Pow => "engine.pow()",
        pb::ValueFn::Fnv64 => "hash.fnv64()",
        pb::ValueFn::CalendarYear => "calendar.year()",
        pb::ValueFn::CalendarMonth => "calendar.month()",
        pb::ValueFn::CalendarDay => "calendar.day()",
        pb::ValueFn::Unspecified => "unspecified()",
    }
}

/// The argument and result shape of one function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FnSignature {
    /// The math and engine vocabulary: numeric arguments of one type;
    /// `type_preserving` results keep it, the rest take and yield
    /// doubles (or bools for the classifiers).
    Numeric { type_preserving: bool },
    /// `hash.fnv64(s)`: one facet string in, a uint out. Ingest-only.
    StringToUint,
    /// `calendar.*(t)`: one int of epoch microseconds in, an int out.
    IntToInt,
}

/// The signature of one function, shared by the compiler, resolution
/// and both evaluators so they cannot disagree.
pub fn fn_signature(f: pb::ValueFn) -> FnSignature {
    match f {
        pb::ValueFn::Fnv64 => FnSignature::StringToUint,
        pb::ValueFn::CalendarYear | pb::ValueFn::CalendarMonth | pb::ValueFn::CalendarDay => {
            FnSignature::IntToInt
        }
        pb::ValueFn::Abs | pb::ValueFn::Sign | pb::ValueFn::Greatest | pb::ValueFn::Least => {
            FnSignature::Numeric {
                type_preserving: true,
            }
        }
        _ => FnSignature::Numeric {
            type_preserving: false,
        },
    }
}

/// FNV-1a 64 over bytes: the coordinator's stable routing hash
/// (`coordinator::stable_routing_hash`) and `hash.fnv64()`'s function.
/// Distinct from the WAL bucket hash, which hashes little-endian row
/// ids and selects high bits.
pub fn fnv1a64_bytes(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// The proleptic Gregorian UTC civil date of an instant in epoch
/// microseconds, exact over the i64 range.
pub fn civil_of_micros(micros: i64) -> (i64, u32, u32) {
    crate::calendar::civil_from_days(crate::calendar::floor_div(micros, 86_400_000_000))
}

/// Arity check, shared by both evaluation paths.
pub(super) fn check_fn_arity(f: pb::ValueFn, n: usize) -> Result<(), ValuesError> {
    let ok = match f {
        pb::ValueFn::Greatest | pb::ValueFn::Least => n >= 1,
        pb::ValueFn::Pow => n == 2,
        _ => n == 1,
    };
    if ok {
        Ok(())
    } else {
        let want = match f {
            pb::ValueFn::Greatest | pb::ValueFn::Least => "at least one argument",
            pb::ValueFn::Pow => "exactly two arguments",
            _ => "exactly one argument",
        };
        Err(refuse(format!("{} takes {want}", fn_display(f))))
    }
}

/// Apply one function to its PRESENT, type-agreeing arguments. The
/// semantics are the official CEL math extension's where the name is
/// `math.*`: round is half away from zero, sign preserves NaN and the
/// argument's type, abs of i64::MIN evaluates absent where stock CEL
/// errors (the engine's documented deviation), and the transcendental
/// results are IEEE values, never errors.
pub(super) fn eval_fn(f: pb::ValueFn, vals: &[Val]) -> Option<Val> {
    use pb::ValueFn as F;

    match f {
        F::Abs => match vals[0] {
            Val::Int(v) => v.checked_abs().map(Val::Int),
            Val::Uint(v) => Some(Val::Uint(v)),
            Val::Double(v) => Some(Val::Double(v.abs())),
            _ => unreachable!("resolution typed the argument numeric"),
        },
        F::Sign => match vals[0] {
            Val::Int(v) => Some(Val::Int(v.signum())),
            Val::Uint(v) => Some(Val::Uint(u64::from(v != 0))),
            Val::Double(v) => Some(Val::Double(if v.is_nan() {
                v
            } else if v > 0.0 {
                1.0
            } else if v < 0.0 {
                -1.0
            } else {
                0.0
            })),
            _ => unreachable!("resolution typed the argument numeric"),
        },
        F::Ceil => Some(Val::Double(as_double(&vals[0]).ceil())),
        F::Floor => Some(Val::Double(as_double(&vals[0]).floor())),
        F::Round => Some(Val::Double(as_double(&vals[0]).round())),
        F::Trunc => Some(Val::Double(as_double(&vals[0]).trunc())),
        F::Sqrt => Some(Val::Double(as_double(&vals[0]).sqrt())),
        F::Ln => Some(Val::Double(as_double(&vals[0]).ln())),
        F::Exp => Some(Val::Double(as_double(&vals[0]).exp())),
        F::Log10 => Some(Val::Double(as_double(&vals[0]).log10())),
        F::IsNan => Some(Val::Bool(as_double(&vals[0]).is_nan())),
        F::IsInf => Some(Val::Bool(as_double(&vals[0]).is_infinite())),
        F::IsFinite => Some(Val::Bool(as_double(&vals[0]).is_finite())),
        F::Pow => Some(Val::Double(as_double(&vals[0]).powf(as_double(&vals[1])))),
        F::Fnv64 => unreachable!("hash.fnv64() evaluates at ingest over the document's string"),
        F::CalendarYear | F::CalendarMonth | F::CalendarDay => {
            let Val::Int(micros) = vals[0] else {
                unreachable!("resolution typed the argument int")
            };
            let (y, m, day) = civil_of_micros(micros);
            Some(Val::Int(match f {
                F::CalendarYear => y,
                F::CalendarMonth => i64::from(m),
                _ => i64::from(day),
            }))
        }

        F::Greatest | F::Least => extremum(f == F::Greatest, vals),

        F::Unspecified => unreachable!("resolution decoded the function"),
    }
}

fn as_double(v: &Val) -> f64 {
    match v {
        Val::Double(x) => *x,
        _ => unreachable!("resolution typed the argument double"),
    }
}

/// The greatest or least of type-agreeing arguments; a NaN among doubles
/// makes the result NaN.
fn extremum(greatest: bool, vals: &[Val]) -> Option<Val> {
    match vals[0] {
        Val::Int(first) => {
            let mut acc = first;
            for v in &vals[1..] {
                let Val::Int(x) = v else {
                    unreachable!("resolution typed the arguments alike")
                };
                if (greatest && *x > acc) || (!greatest && *x < acc) {
                    acc = *x;
                }
            }
            Some(Val::Int(acc))
        }
        Val::Uint(first) => {
            let mut acc = first;
            for v in &vals[1..] {
                let Val::Uint(x) = v else {
                    unreachable!("resolution typed the arguments alike")
                };
                if (greatest && *x > acc) || (!greatest && *x < acc) {
                    acc = *x;
                }
            }
            Some(Val::Uint(acc))
        }
        Val::Double(first) => {
            let mut acc = first;
            for v in &vals[1..] {
                let x = as_double(v);
                if x.is_nan() || acc.is_nan() {
                    acc = f64::NAN;
                } else if (greatest && x > acc) || (!greatest && x < acc) {
                    acc = x;
                }
            }
            Some(Val::Double(acc))
        }
        _ => unreachable!("resolution typed the arguments numeric"),
    }
}
