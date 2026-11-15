//! Per-shard numeric, string, histogram, and quantile aggregations.

mod buckets;
mod run;

pub(crate) use self::buckets::agg_op_name;
pub(in crate::node) use self::buckets::{rankable_bits, resolve_rankable};
use self::buckets::{
    resolve_agg_specs, resolve_hist_specs, Bucketing, PctAcc, ResolvedAggExpr, ResolvedHistSpec,
};

use crate::node::*;

/// Decode one wire aggregate op.
pub(crate) fn agg_op_of(op: i32) -> Result<crate::pb::AggregateOp, Status> {
    match crate::pb::AggregateOp::try_from(op) {
        Ok(crate::pb::AggregateOp::Unspecified) | Err(_) => Err(Status::invalid_argument(
            "aggregation with an unspecified operation",
        )),
        Ok(op) => Ok(op),
    }
}

/// One shard's exact accumulator for one aggregation, typed at
/// resolution. Every statistic for the type folds in the single pass
/// (they are a handful of flops each); the response carries them all
/// and the coordinator reads the ones the op needs.
pub(super) enum AggAcc {
    /// The expression can never hold a value on this shard.
    Absent,
    Int {
        present: u64,
        sum: i128,
        min: i64,
        max: i64,
        /// CARDINALITY: the distinct values seen.
        distinct: Option<std::collections::HashSet<i64>>,
    },
    Uint {
        present: u64,
        sum: u128,
        min: u64,
        max: u64,
        distinct: Option<std::collections::HashSet<u64>>,
    },
    Double {
        present: u64,
        /// Neumaier-compensated running sum.
        sum: f64,
        compensation: f64,
        /// NaN-propagating extrema, the math.least/greatest rule.
        min: f64,
        max: f64,
        /// Welford.
        mean: f64,
        m2: f64,
        /// CARDINALITY: the distinct canonical bit patterns seen.
        distinct: Option<std::collections::HashSet<u64>>,
    },
    Str {
        present: u64,
        /// CARDINALITY: the distinct (map, column, ordinal) reads seen;
        /// rendered against the dictionary on the way out.
        distinct: Option<std::collections::HashSet<(bool, usize, u32)>>,
    },
    /// CARDINALITY over a boolean expression: which of the values
    /// occurred.
    Bool {
        present: u64,
        seen_true: bool,
        seen_false: bool,
    },
}

/// One bit pattern per double value for the distinct count: a single
/// NaN and a single zero, so `-0.0` and `0.0` are one value and every
/// NaN payload is one value. Everything else is its own bits.
fn canonical_double_bits(x: f64) -> u64 {
    if x.is_nan() {
        f64::NAN.to_bits()
    } else if x == 0.0 {
        0
    } else {
        x.to_bits()
    }
}

fn push_int_acc<T: Copy + Ord + std::hash::Hash>(
    present: &mut u64,
    min: &mut T,
    max: &mut T,
    distinct: &mut Option<std::collections::HashSet<T>>,
    x: T,
) {
    if *present == 0 {
        *min = x;
        *max = x;
    } else {
        *min = (*min).min(x);
        *max = (*max).max(x);
    }
    *present += 1;
    if let Some(set) = distinct {
        set.insert(x);
    }
}

fn sorted_set<T: Copy + Ord>(set: &Option<std::collections::HashSet<T>>) -> Vec<T> {
    let Some(set) = set else {
        return Vec::new();
    };
    let mut values: Vec<T> = set.iter().copied().collect();
    values.sort_unstable();
    values
}

fn sorted_facet_strings(
    distinct: &Option<std::collections::HashSet<(bool, usize, u32)>>,
    store: Option<&Bm25Shard>,
) -> Vec<String> {
    let (Some(set), Some(store)) = (distinct, store) else {
        return Vec::new();
    };
    let mut values: Vec<String> = set
        .iter()
        .map(|&(map, column, ord)| {
            if map {
                store.map_facet_value(column, ord).to_string()
            } else {
                store.facet_value(column, ord).to_string()
            }
        })
        .collect();
    values.sort_unstable();
    values.dedup();
    values
}

fn push_str_ord(
    present: &mut u64,
    distinct: &mut Option<std::collections::HashSet<(bool, usize, u32)>>,
    is_map: bool,
    column: usize,
    ord: u32,
) {
    *present += 1;
    if let Some(set) = distinct {
        set.insert((is_map, column, ord));
    }
}

impl AggAcc {
    fn push_double(
        present: &mut u64,
        sum_comp: (&mut f64, &mut f64),
        min_max: (&mut f64, &mut f64),
        mean_m2: (&mut f64, &mut f64),
        distinct: &mut Option<std::collections::HashSet<u64>>,
        x: f64,
    ) {
        let (sum, compensation) = sum_comp;
        let (min, max) = min_max;
        let (mean, m2) = mean_m2;
        if let Some(set) = distinct {
            set.insert(canonical_double_bits(x));
        }
        if *present == 0 {
            *min = x;
            *max = x;
        } else if x.is_nan() || min.is_nan() {
            *min = f64::NAN;
            *max = f64::NAN;
        } else {
            *min = (*min).min(x);
            *max = (*max).max(x);
        }
        *present += 1;
        let t = *sum + x;
        *compensation += if sum.abs() >= x.abs() {
            (*sum - t) + x
        } else {
            (x - t) + *sum
        };
        *sum = t;
        let n = *present as f64;
        let delta = x - *mean;
        *mean += delta / n;
        *m2 += delta * (x - *mean);
    }

    pub(in crate::node) fn push(&mut self, v: crate::values::Val) {
        match (self, v) {
            (AggAcc::Absent, _) => unreachable!("absent expressions never evaluate"),
            (
                AggAcc::Int {
                    present,
                    sum,
                    min,
                    max,
                    distinct,
                },
                crate::values::Val::Int(x),
            ) => {
                push_int_acc(present, min, max, distinct, x);
                *sum += i128::from(x);
            }
            (
                AggAcc::Uint {
                    present,
                    sum,
                    min,
                    max,
                    distinct,
                },
                crate::values::Val::Uint(x),
            ) => {
                push_int_acc(present, min, max, distinct, x);
                *sum += u128::from(x);
            }
            (
                AggAcc::Double {
                    present,
                    sum,
                    compensation,
                    min,
                    max,
                    mean,
                    m2,
                    distinct,
                },
                crate::values::Val::Double(x),
            ) => {
                Self::push_double(
                    present,
                    (sum, compensation),
                    (min, max),
                    (mean, m2),
                    distinct,
                    x,
                );
            }
            (AggAcc::Str { present, distinct }, crate::values::Val::FacetOrd { column, ord }) => {
                push_str_ord(present, distinct, false, column, ord);
            }
            (
                AggAcc::Str { present, distinct },
                crate::values::Val::MapFacetOrd { column, ord },
            ) => {
                push_str_ord(present, distinct, true, column, ord);
            }
            (
                AggAcc::Bool {
                    present,
                    seen_true,
                    seen_false,
                },
                crate::values::Val::Bool(b),
            ) => {
                *present += 1;
                *if b { seen_true } else { seen_false } = true;
            }
            _ => unreachable!("resolution typed the accumulator"),
        }
    }

    /// This shard's distinct-value count, when CARDINALITY asked for
    /// one.
    pub(in crate::node) fn distinct_len(&self) -> Option<usize> {
        match self {
            AggAcc::Absent => None,
            AggAcc::Int { distinct, .. } => distinct.as_ref().map(|s| s.len()),
            AggAcc::Uint { distinct, .. } => distinct.as_ref().map(|s| s.len()),
            AggAcc::Double { distinct, .. } => distinct.as_ref().map(|s| s.len()),
            AggAcc::Str { distinct, .. } => distinct.as_ref().map(|s| s.len()),
            AggAcc::Bool {
                seen_true,
                seen_false,
                ..
            } => Some(usize::from(*seen_true) + usize::from(*seen_false)),
        }
    }

    fn numeric_partial(&self) -> Option<crate::pb::AggregatePartial> {
        use crate::pb::{AggregatePartial as P, AggregateValueType as T};
        match self {
            AggAcc::Int {
                present,
                sum,
                min,
                max,
                distinct,
            } => Some(P {
                vtype: T::Int as i32,
                present: *present,
                int_sum_hi: (*sum >> 64) as i64,
                int_sum_lo: *sum as u64,
                int_min: *min,
                int_max: *max,
                distinct_ints: sorted_set(distinct),
                ..Default::default()
            }),
            AggAcc::Uint {
                present,
                sum,
                min,
                max,
                distinct,
            } => Some(P {
                vtype: T::Uint as i32,
                present: *present,
                uint_sum_hi: (*sum >> 64) as u64,
                uint_sum_lo: *sum as u64,
                uint_min: *min,
                uint_max: *max,
                distinct_uints: sorted_set(distinct),
                ..Default::default()
            }),
            AggAcc::Double {
                present,
                sum,
                compensation,
                min,
                max,
                mean,
                m2,
                distinct,
            } => Some(P {
                vtype: T::Double as i32,
                present: *present,
                double_sum: *sum,
                double_compensation: *compensation,
                double_min: *min,
                double_max: *max,
                mean: *mean,
                m2: *m2,
                distinct_double_bits: sorted_set(distinct),
                ..Default::default()
            }),
            _ => None,
        }
    }

    /// The wire partial. `store` renders string ordinals; only a
    /// document-less shard, whose accumulators are all absent, has
    /// none to offer.
    pub(in crate::node) fn partial(
        &self,
        store: Option<&Bm25Shard>,
    ) -> crate::pb::AggregatePartial {
        use crate::pb::{AggregatePartial as P, AggregateValueType as T};
        if let Some(num) = self.numeric_partial() {
            return num;
        }
        match self {
            AggAcc::Absent => P {
                vtype: T::Absent as i32,
                ..Default::default()
            },
            AggAcc::Str { present, distinct } => P {
                vtype: T::String as i32,
                present: *present,
                distinct_strings: sorted_facet_strings(distinct, store),
                ..Default::default()
            },
            AggAcc::Bool {
                present,
                seen_true,
                seen_false,
            } => P {
                vtype: T::Bool as i32,
                present: *present,
                distinct_bools: [(*seen_false, false), (*seen_true, true)]
                    .into_iter()
                    .filter_map(|(seen, v)| seen.then_some(v))
                    .collect(),
                ..Default::default()
            },
            _ => unreachable!("numeric variants handled above"),
        }
    }
}

/// A fresh accumulator for one resolved type; `cardinality` adds the
/// distinct set the op counts.
pub(super) fn acc_of(vt: crate::values::ValueType, cardinality: bool) -> AggAcc {
    match vt {
        crate::values::ValueType::Unknown => AggAcc::Absent,
        crate::values::ValueType::Uint => AggAcc::Uint {
            present: 0,
            sum: 0,
            min: 0,
            max: 0,
            distinct: cardinality.then(Default::default),
        },
        crate::values::ValueType::Int => AggAcc::Int {
            present: 0,
            sum: 0,
            min: 0,
            max: 0,
            distinct: cardinality.then(Default::default),
        },
        crate::values::ValueType::Double => AggAcc::Double {
            present: 0,
            sum: 0.0,
            compensation: 0.0,
            min: 0.0,
            max: 0.0,
            mean: 0.0,
            m2: 0.0,
            distinct: cardinality.then(Default::default),
        },
        crate::values::ValueType::Str => AggAcc::Str {
            present: 0,
            distinct: cardinality.then(Default::default),
        },
        crate::values::ValueType::Bool => {
            debug_assert!(
                cardinality,
                "check_agg_type admits booleans under CARDINALITY only"
            );
            AggAcc::Bool {
                present: 0,
                seen_true: false,
                seen_false: false,
            }
        }
    }
}
