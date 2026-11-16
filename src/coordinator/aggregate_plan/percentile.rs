//! Percentile rank arithmetic and phase-1 percentile statistics merging.

use super::*;

/// Exact nearest rank for a validated IEEE percentile in [0, 100].
/// Integer arithmetic avoids rounding the count or p/100 before taking ceil.
pub(in crate::coordinator) fn nearest_percentile_rank(p: f64, count: u64) -> u64 {
    if count == 0 {
        return 0;
    }
    let bits = p.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as u32;
    let fraction = bits & ((1u64 << 52) - 1);
    let (mantissa, shift) = if exponent == 0 {
        (fraction, 1074)
    } else {
        (fraction | (1u64 << 52), 1075 - exponent)
    };
    // The numerator has at most 117 bits. A denominator too large for
    // u128 therefore yields a fractional rank below one (or zero).
    if shift > 121 {
        return 1;
    }
    let numerator = u128::from(mantissa) * u128::from(count);
    let denominator = 100u128 << shift;
    (numerator.div_ceil(denominator) as u64).clamp(1, count)
}

/// One percentile expression's merged phase-1 statistics.
pub(crate) struct PctMerge {
    pub(in crate::coordinator) vt: Option<crate::pb::AggregateValueType>,
    pub(in crate::coordinator) present: u64,
    pub(in crate::coordinator) unrankable: u64,
    pub(in crate::coordinator) min_bits: u64,
    pub(in crate::coordinator) max_bits: u64,
}

impl PctMerge {
    pub(crate) fn new() -> Self {
        Self {
            vt: None,
            present: 0,
            unrankable: 0,
            min_bits: 0,
            max_bits: 0,
        }
    }

    pub(crate) fn fold(
        &mut self,
        p: &crate::pb::PercentilePartial,
        name: &str,
    ) -> Result<(), Status> {
        use crate::pb::AggregateValueType as T;
        let vt = match T::try_from(p.vtype) {
            Ok(T::Absent) => return Ok(()),
            Ok(T::Int) => T::Int,
            Ok(T::Uint) => T::Uint,
            Ok(T::Double) => T::Double,
            _ => {
                return Err(Status::internal(
                    "shard answered a percentile partial without a numeric type",
                ));
            }
        };
        match self.vt {
            None => self.vt = Some(vt),
            Some(prev) if prev != vt => {
                return Err(Status::failed_precondition(format!(
                    "percentile {name:?}: shards disagree on the expression's type \
                     ({} against {}); the column families diverge across shards",
                    agg_vt_name(prev),
                    agg_vt_name(vt)
                )));
            }
            Some(_) => {}
        }
        self.unrankable = self.unrankable.checked_add(p.unrankable).ok_or_else(|| {
            Status::failed_precondition(format!(
                "percentile {name:?}: unrankable count overflows u64"
            ))
        })?;
        if p.present == 0 {
            return Ok(());
        }
        if self.present == 0 {
            self.min_bits = p.min_bits;
            self.max_bits = p.max_bits;
        } else {
            self.min_bits = self.min_bits.min(p.min_bits);
            self.max_bits = self.max_bits.max(p.max_bits);
        }
        self.present = self.present.checked_add(p.present).ok_or_else(|| {
            Status::failed_precondition(format!("percentile {name:?}: present count overflows u64"))
        })?;
        Ok(())
    }

    /// The merged state as one shard's partial: what a relay answers
    /// its parent after folding its children in child order, so the
    /// parent folds it as it folds a shard's.
    pub(crate) fn partial(&self) -> crate::pb::PercentilePartial {
        crate::pb::PercentilePartial {
            vtype: self
                .vt
                .unwrap_or(crate::pb::AggregateValueType::Absent)
                .into(),
            present: self.present,
            unrankable: self.unrankable,
            min_bits: self.min_bits,
            max_bits: self.max_bits,
        }
    }
}
