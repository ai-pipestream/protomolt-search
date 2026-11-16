//! Typed fleet-wide aggregation merging.

use super::*;

/// One aggregation's merged fleet-wide statistics: a type vote plus
/// every fold, gated per type. Extrema and moments fold only over
/// shards that HELD values; the type vote counts on any shard whose
/// columns resolve, so cross-shard type disagreement stays loud even
/// when one side is empty.
pub(crate) struct AggMerge {
    pub(in crate::coordinator) vt: Option<crate::pb::AggregateValueType>,
    pub(in crate::coordinator) present: u64,
    /// CARDINALITY: the fleet-wide union of the shards' distinct
    /// values, typed by the vote. A BTreeSet so the union is
    /// deterministic whatever the shard order.
    pub(in crate::coordinator) distinct: Distinct,
    pub(in crate::coordinator) uint_sum: u128,
    pub(in crate::coordinator) uint_min: u64,
    pub(in crate::coordinator) uint_max: u64,
    pub(in crate::coordinator) int_sum: i128,
    pub(in crate::coordinator) int_min: i64,
    pub(in crate::coordinator) int_max: i64,
    pub(in crate::coordinator) dsum: f64,
    pub(in crate::coordinator) dcomp: f64,
    pub(in crate::coordinator) dmin: f64,
    pub(in crate::coordinator) dmax: f64,
    pub(in crate::coordinator) mean: f64,
    pub(in crate::coordinator) m2: f64,
}

/// The exact distinct-value union behind CARDINALITY, one set per
/// type; doubles by canonical bits, strings by dictionary term.
#[derive(Default)]
pub(in crate::coordinator) struct Distinct {
    pub(in crate::coordinator) ints: std::collections::BTreeSet<i64>,
    pub(in crate::coordinator) uints: std::collections::BTreeSet<u64>,
    pub(in crate::coordinator) doubles: std::collections::BTreeSet<u64>,
    pub(in crate::coordinator) strings: std::collections::BTreeSet<String>,
    pub(in crate::coordinator) bools: std::collections::BTreeSet<bool>,
}

impl Distinct {
    pub(in crate::coordinator) fn len(&self) -> usize {
        self.ints.len()
            + self.uints.len()
            + self.doubles.len()
            + self.strings.len()
            + self.bools.len()
    }
}

impl AggMerge {
    pub(crate) fn new() -> Self {
        Self {
            vt: None,
            present: 0,
            distinct: Distinct::default(),
            uint_sum: 0,
            uint_min: 0,
            uint_max: 0,
            int_sum: 0,
            int_min: 0,
            int_max: 0,
            dsum: 0.0,
            dcomp: 0.0,
            dmin: 0.0,
            dmax: 0.0,
            mean: 0.0,
            m2: 0.0,
        }
    }

    /// Neumaier step: fold one addend into (sum, compensation).
    pub(in crate::coordinator) fn neumaier(&mut self, x: f64) {
        let t = self.dsum + x;
        self.dcomp += if self.dsum.abs() >= x.abs() {
            (self.dsum - t) + x
        } else {
            (x - t) + self.dsum
        };
        self.dsum = t;
    }

    /// Fold one shard's partial in. Shard order is the caller's
    /// contract; every fold here is deterministic given that order.
    pub(crate) fn fold(
        &mut self,
        p: &crate::pb::AggregatePartial,
        agg: &crate::pb::CompiledAggregation,
    ) -> Result<(), Status> {
        use crate::pb::AggregateValueType as T;
        let name = agg.name.as_str();
        let vt = match T::try_from(p.vtype) {
            Ok(T::Absent) => return Ok(()),
            Ok(T::Unspecified) | Err(_) => {
                return Err(Status::internal(
                    "shard answered an aggregation partial without a type",
                ));
            }
            Ok(vt) => vt,
        };
        match self.vt {
            None => self.vt = Some(vt),
            Some(prev) if prev != vt => {
                return Err(Status::failed_precondition(format!(
                    "aggregation {name:?}: shards disagree on the expression's type \
                     ({} against {}); the column families diverge across shards",
                    agg_vt_name(prev),
                    agg_vt_name(vt)
                )));
            }
            Some(_) => {}
        }
        if p.present == 0 {
            return Ok(());
        }
        let next_present = self.present.checked_add(p.present).ok_or_else(|| {
            Status::failed_precondition(format!(
                "aggregation {name:?}: present count overflows u64"
            ))
        })?;
        match vt {
            T::Int => self.fold_int(p, name)?,
            T::Uint => self.fold_uint(p, name)?,
            T::Double => self.fold_double(p),
            T::String | T::Bool => {}
            T::Absent | T::Unspecified => unreachable!("handled above"),
        }
        self.present = next_present;
        if agg.op == crate::pb::AggregateOp::Cardinality as i32 {
            self.fold_distinct(p, agg)?;
        }
        Ok(())
    }

    /// Fold an int partial's exact sum and extrema; `self.present` still
    /// counts only the shards folded before this one.
    fn fold_int(&mut self, p: &crate::pb::AggregatePartial, name: &str) -> Result<(), Status> {
        let sum = (i128::from(p.int_sum_hi) << 64) | i128::from(p.int_sum_lo);
        self.int_sum = self.int_sum.checked_add(sum).ok_or_else(|| {
            Status::failed_precondition(format!(
                "aggregation {name:?}: int partial sum overflows i128"
            ))
        })?;
        if self.present == 0 {
            self.int_min = p.int_min;
            self.int_max = p.int_max;
        } else {
            self.int_min = self.int_min.min(p.int_min);
            self.int_max = self.int_max.max(p.int_max);
        }
        Ok(())
    }

    /// Fold a uint partial's exact sum and extrema, as `fold_int` does.
    fn fold_uint(&mut self, p: &crate::pb::AggregatePartial, name: &str) -> Result<(), Status> {
        let sum = (u128::from(p.uint_sum_hi) << 64) | u128::from(p.uint_sum_lo);
        self.uint_sum = self.uint_sum.checked_add(sum).ok_or_else(|| {
            Status::failed_precondition(format!(
                "aggregation {name:?}: uint partial sum overflows u128"
            ))
        })?;
        if self.present == 0 {
            self.uint_min = p.uint_min;
            self.uint_max = p.uint_max;
        } else {
            self.uint_min = self.uint_min.min(p.uint_min);
            self.uint_max = self.uint_max.max(p.uint_max);
        }
        Ok(())
    }

    /// Fold a double partial's compensated sum, extrema and moments.
    fn fold_double(&mut self, p: &crate::pb::AggregatePartial) {
        // The shard's compensated sum folds as its two exact
        // halves, keeping the coordinator's own compensation.
        self.neumaier(p.double_sum);
        self.neumaier(p.double_compensation);
        if self.present == 0 {
            self.dmin = p.double_min;
            self.dmax = p.double_max;
        } else if p.double_min.is_nan() || self.dmin.is_nan() {
            self.dmin = f64::NAN;
            self.dmax = f64::NAN;
        } else {
            self.dmin = self.dmin.min(p.double_min);
            self.dmax = self.dmax.max(p.double_max);
        }
        // Chan's parallel Welford merge.
        let (na, nb) = (self.present, p.present);
        if na == 0 {
            self.mean = p.mean;
            self.m2 = p.m2;
        } else {
            let n = (na + nb) as f64;
            let delta = p.mean - self.mean;
            self.mean += delta * (nb as f64 / n);
            self.m2 += p.m2 + delta * delta * (na as f64 * nb as f64 / n);
        }
    }

    /// Union a CARDINALITY partial's distinct values, refusing past the cap.
    fn fold_distinct(
        &mut self,
        p: &crate::pb::AggregatePartial,
        agg: &crate::pb::CompiledAggregation,
    ) -> Result<(), Status> {
        let name = agg.name.as_str();
        self.distinct.ints.extend(p.distinct_ints.iter().copied());
        self.distinct.uints.extend(p.distinct_uints.iter().copied());
        self.distinct
            .doubles
            .extend(p.distinct_double_bits.iter().copied());
        self.distinct
            .strings
            .extend(p.distinct_strings.iter().cloned());
        self.distinct.bools.extend(p.distinct_bools.iter().copied());
        if self.distinct.len() > agg.max_distinct as usize {
            return Err(Status::failed_precondition(format!(
                "aggregation {name:?}: more than {} distinct values across the \
                 fleet; raise max_distinct or tighten the filter",
                agg.max_distinct
            )));
        }
        Ok(())
    }

    /// The merged state as one shard's partial (`PctMerge::partial`
    /// says why): the exact int sum split into its halves, the
    /// compensated double sum as its pair, the distinct sets as sorted
    /// lists.
    pub(crate) fn partial(&self) -> crate::pb::AggregatePartial {
        crate::pb::AggregatePartial {
            vtype: self
                .vt
                .unwrap_or(crate::pb::AggregateValueType::Absent)
                .into(),
            present: self.present,
            int_sum_hi: (self.int_sum >> 64) as i64,
            int_sum_lo: self.int_sum as u64,
            double_sum: self.dsum,
            double_compensation: self.dcomp,
            int_min: self.int_min,
            int_max: self.int_max,
            double_min: self.dmin,
            double_max: self.dmax,
            mean: self.mean,
            m2: self.m2,
            distinct_ints: self.distinct.ints.iter().copied().collect(),
            distinct_double_bits: self.distinct.doubles.iter().copied().collect(),
            distinct_strings: self.distinct.strings.iter().cloned().collect(),
            distinct_bools: self.distinct.bools.iter().copied().collect(),
            uint_sum_hi: (self.uint_sum >> 64) as u64,
            uint_sum_lo: self.uint_sum as u64,
            uint_min: self.uint_min,
            uint_max: self.uint_max,
            distinct_uints: self.distinct.uints.iter().copied().collect(),
        }
    }

    /// The final result for one op. `present == 0` reports no value
    /// (COUNT aside, which reports the zero).
    pub(in crate::coordinator) fn result(
        &self,
        name: &str,
        op: crate::pb::AggregateOp,
    ) -> Result<crate::pb::AggregateResult, Status> {
        use crate::pb::aggregate_result::Value as W;
        use crate::pb::{AggregateOp as O, AggregateValueType as T};
        let int_typed = self.vt == Some(T::Int);
        let uint_typed = self.vt == Some(T::Uint);
        let value = match op {
            O::Count => Some(W::IntValue(i64::try_from(self.present).map_err(|_| {
                Status::failed_precondition(format!("aggregation {name:?}: count does not fit the int result; tighten the filter"))
            })?)),
            O::Cardinality => Some(W::IntValue(self.distinct.len() as i64)),
            _ if self.present == 0 => None,
            O::Sum if int_typed => match i64::try_from(self.int_sum) {
                Ok(v) => Some(W::IntValue(v)),
                Err(_) => {
                    return Err(Status::failed_precondition(format!(
                        "aggregation {name:?}: the exact int sum {} does not fit i64; \
                         aggregate double(...) for an IEEE sum, or ask for the mean",
                        self.int_sum
                    )));
                }
            },
            O::Sum if uint_typed => Some(W::UintValue(u64::try_from(self.uint_sum).map_err(|_| {
                Status::failed_precondition(format!(
                    "aggregation {name:?}: the exact uint sum {} does not fit u64; aggregate double(...) for an IEEE sum",
                    self.uint_sum
                ))
            })?)),
            O::Sum => Some(W::DoubleValue(self.dsum + self.dcomp)),
            O::Min if uint_typed => Some(W::UintValue(self.uint_min)),
            O::Min if int_typed => Some(W::IntValue(self.int_min)),
            O::Min => Some(W::DoubleValue(self.dmin)),
            O::Max if uint_typed => Some(W::UintValue(self.uint_max)),
            O::Max if int_typed => Some(W::IntValue(self.int_max)),
            O::Max => Some(W::DoubleValue(self.dmax)),
            O::Mean => Some(W::DoubleValue(self.mean)),
            O::Variance => Some(W::DoubleValue(self.m2 / self.present as f64)),
            O::Stddev => Some(W::DoubleValue((self.m2 / self.present as f64).sqrt())),
            O::Unspecified => unreachable!("compile refused the unspecified op"),
        };
        Ok(crate::pb::AggregateResult {
            name: name.to_string(),
            present: self.present,
            value,
        })
    }
}

/// Type name for the disagreement refusal.
pub(in crate::coordinator) fn agg_vt_name(vt: crate::pb::AggregateValueType) -> &'static str {
    use crate::pb::AggregateValueType as T;
    match vt {
        T::Int => "int",
        T::Uint => "uint",
        T::Double => "double",
        T::String => "string",
        T::Bool => "bool",
        T::Absent | T::Unspecified => "absent",
    }
}
