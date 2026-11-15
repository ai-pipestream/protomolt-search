//! Compiled aggregation plans and typed result merging.

use super::*;

/// What a faceted BM25 fan-out returns: the global top-k plus the
/// merged counts of each facet kind that was asked for (empty lists for
/// the kinds that were not). Named because three of the fan-out
/// entry points share the shape and a bare triple reads as noise at
/// every call site.
pub(super) type FacetedHits = (
    Vec<Bm25Hit>,
    Vec<crate::pb::FacetFieldCounts>,
    Vec<crate::pb::RangeFacetCounts>,
);

/// [`FacetedHits`] plus merged match-set column stats, exact distinct-value
/// counts, and segment-pruning telemetry.
pub(super) type AggregatedHits = (
    Vec<Bm25Hit>,
    Vec<crate::pb::FacetFieldCounts>,
    Vec<crate::pb::RangeFacetCounts>,
    Vec<crate::pb::ColumnStats>,
    Vec<crate::pb::FacetCardinality>,
    crate::segment_prune::PruneStats,
);

/// The evaluated values for one candidate set (`fetch_values`).
pub struct FetchedValues {
    /// Agreed declaration types, including empty nodes and absent candidate values.
    pub projection_types: Vec<crate::pb::ScalarValueType>,
    /// Explicit identity/absence records when requested, under the same receipt.
    pub identities: HashMap<u64, Option<crate::pb::DocumentIdentity>>,
    /// doc -> projected values, aligned with the request projections.
    pub rows: HashMap<u64, Vec<crate::pb::ProjectedValue>>,
    /// Per stage: doc -> identity-score contribution. A doc absent
    /// from a map has no value for that stage's column.
    pub stage_rows: Vec<HashMap<u64, f64>>,
    /// Versions read, in node order. Empty only when an unscoped request had
    /// neither projections nor stages and performed no fan-out.
    pub epochs: Vec<StatsClaim>,
}

/// Compile the public projection list once at the coordinator, enforcing
/// non-empty, request-unique names.
pub(crate) fn compile_projections(
    projections: &[crate::pb::NamedProjection],
) -> Result<Vec<crate::pb::CompiledProjection>, Status> {
    let mut names = std::collections::HashSet::new();
    let mut compiled = Vec::with_capacity(projections.len());
    for p in projections {
        if p.name.is_empty() {
            return Err(Status::invalid_argument(
                "projection: a projection needs a non-empty name",
            ));
        }
        if !names.insert(p.name.as_str()) {
            return Err(Status::invalid_argument(format!(
                "projection: duplicate projection name {:?}",
                p.name
            )));
        }
        let expr = crate::cel::compile_value(&p.expression).map_err(|e| {
            Status::invalid_argument(format!("projection {:?}: {}", p.name, e.message()))
        })?;
        compiled.push(crate::pb::CompiledProjection {
            name: p.name.clone(),
            expr: Some(expr),
        });
    }
    Ok(compiled)
}

/// The compiled aggregate request: everything the fan-out sends and
/// the merge needs.
#[derive(Clone)]
pub(crate) struct CompiledAggregate {
    pub(crate) aggregations: Vec<crate::pb::CompiledAggregation>,
    pub(crate) histograms: Vec<crate::pb::CompiledHistogram>,
    pub(crate) percentiles: Vec<crate::pb::CompiledPercentile>,
    pub(crate) percentile_specs: Vec<crate::pb::PercentileSpec>,
    pub(crate) group_by: String,
    pub(crate) max_groups: u32,
}

/// Compile the public aggregation list: names checked, ops decoded,
/// expressions compiled once into the ValueExpr IR the shards resolve.
pub(crate) fn compile_aggregations(
    req: &crate::pb::AggregateRequest,
) -> Result<CompiledAggregate, Status> {
    let (aggregations, histograms, percentiles) =
        (&req.aggregations, &req.histograms, &req.percentiles);
    if aggregations.is_empty() && histograms.is_empty() && percentiles.is_empty() {
        return Err(Status::invalid_argument(
            "aggregate requires at least one aggregation, histogram, or percentile",
        ));
    }
    if percentiles.len() > 8 {
        return Err(Status::invalid_argument(format!(
            "aggregate takes at most 8 percentile specs per request, got {}",
            percentiles.len()
        )));
    }
    if aggregations.len() > 32 {
        return Err(Status::invalid_argument(format!(
            "aggregate takes at most 32 aggregations per request, got {}",
            aggregations.len()
        )));
    }
    if histograms.len() > 8 {
        return Err(Status::invalid_argument(format!(
            "aggregate takes at most 8 histograms per request, got {}",
            histograms.len()
        )));
    }
    // One name namespace across aggregations and histograms.
    let mut names = std::collections::HashSet::new();
    let mut compiled = Vec::with_capacity(aggregations.len());
    for a in aggregations {
        if a.name.is_empty() {
            return Err(Status::invalid_argument(
                "aggregation: an aggregation needs a non-empty name",
            ));
        }
        if !names.insert(a.name.as_str()) {
            return Err(Status::invalid_argument(format!(
                "aggregation: duplicate aggregation name {:?}",
                a.name
            )));
        }
        let op = crate::node::agg_op_of(a.op).map_err(|e| {
            Status::invalid_argument(format!("aggregation {:?}: {}", a.name, e.message()))
        })?;
        let cardinality = op == crate::pb::AggregateOp::Cardinality;
        if a.max_distinct != 0 && !cardinality {
            return Err(Status::invalid_argument(format!(
                "aggregation {:?}: max_distinct applies to CARDINALITY, not {}",
                a.name,
                crate::node::agg_op_name(op)
            )));
        }
        let expr = crate::cel::compile_value(&a.expression).map_err(|e| {
            Status::invalid_argument(format!("aggregation {:?}: {}", a.name, e.message()))
        })?;
        compiled.push(crate::pb::CompiledAggregation {
            expr: Some(expr),
            op: a.op,
            name: a.name.clone(),
            max_distinct: match (cardinality, a.max_distinct) {
                (false, _) => 0,
                (true, 0) => DEFAULT_MAX_DISTINCT,
                (true, cap) => cap,
            },
        });
    }
    let mut compiled_hists = Vec::with_capacity(histograms.len());
    for h in histograms {
        if h.name.is_empty() {
            return Err(Status::invalid_argument(
                "histogram: a histogram needs a non-empty name",
            ));
        }
        if !names.insert(h.name.as_str()) {
            return Err(Status::invalid_argument(format!(
                "aggregation: duplicate aggregation name {:?}",
                h.name
            )));
        }
        let calendar = if h.calendar != 0 {
            let unit = crate::calendar::interval_of(h.calendar).ok_or_else(|| {
                Status::invalid_argument(format!(
                    "histogram {:?}: unknown calendar interval {}",
                    h.name, h.calendar
                ))
            })?;
            if h.interval != 0.0 {
                return Err(Status::invalid_argument(format!(
                    "histogram {:?}: a calendar histogram buckets by {}; its fixed \
                     interval must be zero, got {}",
                    h.name,
                    crate::calendar::interval_name(unit),
                    h.interval
                )));
            }
            if h.utc_offset_minutes.abs() > crate::calendar::MAX_UTC_OFFSET_MINUTES {
                return Err(Status::invalid_argument(format!(
                    "histogram {:?}: utc_offset_minutes {} is outside +-{}",
                    h.name,
                    h.utc_offset_minutes,
                    crate::calendar::MAX_UTC_OFFSET_MINUTES
                )));
            }
            Some(unit)
        } else {
            if h.utc_offset_minutes != 0 {
                return Err(Status::invalid_argument(format!(
                    "histogram {:?}: utc_offset_minutes applies to a calendar histogram; \
                     name a calendar interval",
                    h.name
                )));
            }
            if !(h.interval > 0.0 && h.interval.is_finite()) {
                return Err(Status::invalid_argument(format!(
                    "histogram {:?}: the interval must be positive and finite, got {}",
                    h.name, h.interval
                )));
            }
            None
        };
        let expr = crate::cel::compile_value(&h.expression).map_err(|e| {
            Status::invalid_argument(format!("histogram {:?}: {}", h.name, e.message()))
        })?;
        compiled_hists.push(crate::pb::CompiledHistogram {
            expr: Some(expr),
            interval: h.interval,
            max_buckets: if h.max_buckets == 0 {
                1024
            } else {
                h.max_buckets
            },
            name: h.name.clone(),
            calendar: calendar.map_or(0, |unit| unit as i32),
            utc_offset_minutes: h.utc_offset_minutes,
        });
    }
    let mut compiled_pcts = Vec::with_capacity(percentiles.len());
    for spec in percentiles {
        if spec.name.is_empty() {
            return Err(Status::invalid_argument(
                "percentile: a percentile spec needs a non-empty name",
            ));
        }
        if !names.insert(spec.name.as_str()) {
            return Err(Status::invalid_argument(format!(
                "aggregation: duplicate aggregation name {:?}",
                spec.name
            )));
        }
        if spec.percentiles.is_empty() || spec.percentiles.len() > 16 {
            return Err(Status::invalid_argument(format!(
                "percentile {:?}: 1 to 16 percentile values per spec, got {}",
                spec.name,
                spec.percentiles.len()
            )));
        }
        for &p in &spec.percentiles {
            if !(p.is_finite() && (0.0..=100.0).contains(&p)) {
                return Err(Status::invalid_argument(format!(
                    "percentile {:?}: {p} is not a percentile; values are finite in \
                     [0, 100]",
                    spec.name
                )));
            }
        }
        let expr = crate::cel::compile_value(&spec.expression).map_err(|e| {
            Status::invalid_argument(format!("percentile {:?}: {}", spec.name, e.message()))
        })?;
        compiled_pcts.push(crate::pb::CompiledPercentile {
            expr: Some(expr),
            name: spec.name.clone(),
        });
    }
    Ok(CompiledAggregate {
        aggregations: compiled,
        histograms: compiled_hists,
        percentiles: compiled_pcts,
        percentile_specs: req.percentiles.clone(),
        group_by: req.group_by.clone(),
        max_groups: if req.max_groups == 0 {
            1000
        } else {
            req.max_groups
        },
    })
}

/// Exact nearest rank for a validated IEEE percentile in [0, 100].
/// Integer arithmetic avoids rounding the count or p/100 before taking ceil.
pub(super) fn nearest_percentile_rank(p: f64, count: u64) -> u64 {
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
    pub(super) vt: Option<crate::pb::AggregateValueType>,
    pub(super) present: u64,
    pub(super) unrankable: u64,
    pub(super) min_bits: u64,
    pub(super) max_bits: u64,
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

/// One aggregation's merged fleet-wide statistics: a type vote plus
/// every fold, gated per type. Extrema and moments fold only over
/// shards that HELD values; the type vote counts on any shard whose
/// columns resolve, so cross-shard type disagreement stays loud even
/// when one side is empty.
pub(crate) struct AggMerge {
    pub(super) vt: Option<crate::pb::AggregateValueType>,
    pub(super) present: u64,
    /// CARDINALITY: the fleet-wide union of the shards' distinct
    /// values, typed by the vote. A BTreeSet so the union is
    /// deterministic whatever the shard order.
    pub(super) distinct: Distinct,
    pub(super) uint_sum: u128,
    pub(super) uint_min: u64,
    pub(super) uint_max: u64,
    pub(super) int_sum: i128,
    pub(super) int_min: i64,
    pub(super) int_max: i64,
    pub(super) dsum: f64,
    pub(super) dcomp: f64,
    pub(super) dmin: f64,
    pub(super) dmax: f64,
    pub(super) mean: f64,
    pub(super) m2: f64,
}

/// The exact distinct-value union behind CARDINALITY, one set per
/// type; doubles by canonical bits, strings by dictionary term.
#[derive(Default)]
pub(super) struct Distinct {
    pub(super) ints: std::collections::BTreeSet<i64>,
    pub(super) uints: std::collections::BTreeSet<u64>,
    pub(super) doubles: std::collections::BTreeSet<u64>,
    pub(super) strings: std::collections::BTreeSet<String>,
    pub(super) bools: std::collections::BTreeSet<bool>,
}

impl Distinct {
    pub(super) fn len(&self) -> usize {
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
    pub(super) fn neumaier(&mut self, x: f64) {
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
            T::Int => {
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
            }
            T::Uint => {
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
            }
            T::Double => {
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
            T::String | T::Bool => {}
            T::Absent | T::Unspecified => unreachable!("handled above"),
        }
        self.present = next_present;
        if agg.op == crate::pb::AggregateOp::Cardinality as i32 {
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
    pub(super) fn result(
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
pub(super) fn agg_vt_name(vt: crate::pb::AggregateValueType) -> &'static str {
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

/// Merge per-shard column stats: counts and sums add, mins and maxes fold,
/// `mean` is computed at the coordinator, and an unknown column refuses by name.
pub(super) fn merge_column_stats(
    requested: &[String],
    shard_stats: &[Vec<crate::pb::ColumnStats>],
) -> Result<Vec<crate::pb::ColumnStats>, Status> {
    crate::column_stats::merge(requested, shard_stats)
}

/// Union per-shard distinct facet values into exact global cardinalities
/// (comparing string values rather than shard-local ordinals).
pub(super) fn merge_cardinality(
    requested: &[String],
    shard_distinct: &[Vec<crate::pb::FacetDistinct>],
) -> Result<Vec<crate::pb::FacetCardinality>, Status> {
    let mut known = vec![false; requested.len()];
    let mut sets: Vec<std::collections::HashSet<String>> =
        requested.iter().map(|_| Default::default()).collect();
    for shard in shard_distinct {
        if shard.len() != requested.len() {
            return Err(Status::internal(format!(
                "shard answered {} cardinality columns for {} requested",
                shard.len(),
                requested.len()
            )));
        }
        for ((k, set), d) in known.iter_mut().zip(&mut sets).zip(shard) {
            *k |= d.known;
            set.extend(d.values.iter().cloned());
        }
    }
    requested
        .iter()
        .zip(known)
        .zip(sets)
        .map(|((name, known), set)| {
            if !known {
                return Err(Status::invalid_argument(format!(
                    "no shard has facet column {:?} for cardinality: check the spelling, \
                     or the nodes' --facet-fields",
                    name
                )));
            }
            Ok(crate::pb::FacetCardinality {
                field: name.clone(),
                cardinality: set.len() as u64,
            })
        })
        .collect()
}

/// Merge per-shard facet counts into global counts: the plain per-value
/// sum (counts are additive — no node's count depends on another's, so
/// there is no analog of the global-df trap), `known` when at least one
/// shard has the field, sorted count-descending with ties by value
/// ascending. A facet field NO shard knows is refused — the same rule
/// as an unknown scoring field: zeros everywhere would make a typo read
/// as "no results per anything".
pub(crate) fn merge_facet_counts(
    requested: &[String],
    map_requested: &[crate::pb::MapFacetField],
    shard_facets: &[Vec<crate::pb::FacetFieldCounts>],
    require_known: bool,
) -> Result<Vec<crate::pb::FacetFieldCounts>, Status> {
    let targets: Vec<(&str, Option<&str>)> = requested
        .iter()
        .map(|name| (name.as_str(), None))
        .chain(
            map_requested
                .iter()
                .map(|entry| (entry.column.as_str(), Some(entry.key.as_str()))),
        )
        .collect();
    let mut known = vec![false; targets.len()];
    let mut sums: Vec<HashMap<String, u64>> = targets.iter().map(|_| HashMap::new()).collect();
    for share in shard_facets {
        if share.len() != targets.len() {
            return Err(Status::failed_precondition(
                "facet response has wrong field count",
            ));
        }
        for (i, (response, &(field, key))) in share.iter().zip(&targets).enumerate() {
            let legacy_map = key.is_some_and(|key| !key.is_empty()) && response.map_key.is_none();
            if response.field != field
                || response.key != key.unwrap_or_default()
                || (response.map_key.as_deref() != key && !legacy_map)
                || (!response.known && !response.counts.is_empty())
            {
                return Err(Status::failed_precondition(format!(
                    "facet response does not match requested column {field:?} and map key {key:?}"
                )));
            }
            known[i] |= response.known;
            for count in &response.counts {
                let total = sums[i].entry(count.value.clone()).or_default();
                *total = total
                    .checked_add(count.count)
                    .ok_or_else(|| Status::out_of_range("facet count overflows u64"))?;
            }
        }
    }
    targets
        .into_iter()
        .zip(known)
        .zip(sums)
        .map(|(((field, key), known), sum)| {
            if require_known && !known {
                let target = match key {
                    Some(key) => format!("{field:?}[{key:?}]"),
                    None => format!("{field:?}"),
                };
                return Err(Status::invalid_argument(format!(
                    "no shard has facet field {target}; check --facet-fields / --map-facet-fields"
                )));
            }
            let mut counts: Vec<_> = sum
                .into_iter()
                .map(|(value, count)| crate::pb::FacetCount { value, count })
                .collect();
            counts.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.value.cmp(&b.value)));
            Ok(crate::pb::FacetFieldCounts {
                field: field.to_owned(),
                key: key.unwrap_or_default().to_owned(),
                map_key: key.map(str::to_owned),
                known,
                counts,
            })
        })
        .collect()
}

/// Merge per-shard range-facet counts positionally by bucket, refusing any
/// column unknown across all shards.
pub(super) fn merge_range_counts(
    requested: &[crate::pb::RangeFacetField],
    shard_ranges: &[Vec<crate::pb::RangeFacetCounts>],
) -> Result<Vec<crate::pb::RangeFacetCounts>, Status> {
    crate::rangefacet::merge(requested, shard_ranges, true)
}

/// Refuse a geo-filter column that no shard declares; partially known columns
/// remain valid across heterogeneous fleets.
pub(super) fn refuse_unknown_geo_columns(
    filters: &[crate::pb::GeoFilter],
    known: &[bool],
) -> Result<(), Status> {
    let unknown: Vec<String> = filters
        .iter()
        .zip(known)
        .filter(|(_, k)| !**k)
        .map(|(f, _)| format!("{:?}", f.column))
        .collect();
    if !unknown.is_empty() {
        return Err(Status::invalid_argument(format!(
            "no shard has geo column {}: filtering on an unknown column would remove every \
             document and read as an empty result set. Check the spelling, or the nodes' \
             --geo-fields.",
            unknown.join(", ")
        )));
    }
    Ok(())
}

/// The typo rule for compiled filter trees: a leaf NO shard can
/// resolve is a name spelled wrong (or a literal whose type picked the
/// wrong table), and filtering on it would read as an empty result
/// set. Known flags are positional over
/// [`crate::filter::walk_leaves`] order; the nodes derive theirs from
/// the same walk, so the zip below cannot misattribute a flag.
pub(super) fn refuse_unknown_filter_leaves(
    filter: Option<&crate::pb::FilterExpr>,
    known: &[bool],
) -> Result<(), Status> {
    let Some(expr) = filter else {
        return Ok(());
    };
    let mut leaves = Vec::new();
    crate::filter::walk_leaves(expr, &mut |l| leaves.push(l));
    let unknown: Vec<String> = leaves
        .iter()
        .zip(known)
        .filter(|(_, k)| !**k)
        .map(|(l, _)| l.describe())
        .collect();
    if !unknown.is_empty() {
        return Err(Status::invalid_argument(format!(
            "no shard can resolve filter {}: filtering on an unknown name would read as \
             an empty result set. Check the spelling and the literal's type (a string \
             literal selects the facet tables, a number the i64/u64/f64 tables), or the \
             nodes' --facet-fields / --numeric-fields / --integer-fields / --unsigned-integer-fields / \
             --map-facet-fields / --map-numeric-fields / --geo-fields.",
            unknown.join(", ")
        )));
    }
    Ok(())
}
