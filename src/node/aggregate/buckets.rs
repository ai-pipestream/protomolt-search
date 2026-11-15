use crate::node::*;

/// How one histogram buckets: a fixed width over doubles, or a
/// calendar unit over epoch-micros ints (docs/aggregations.md).
#[derive(Clone, Copy)]
pub(super) enum Bucketing {
    Fixed(f64),
    Calendar {
        unit: crate::pb::CalendarInterval,
        utc_offset_minutes: i32,
    },
}

/// One shard's sparse histogram fold.
#[derive(Default)]
pub(super) struct HistAcc {
    pub(in crate::node) buckets: std::collections::HashMap<i64, u64>,
    pub(in crate::node) present: u64,
    pub(in crate::node) unbucketable: u64,
}

impl HistAcc {
    /// Fold one present value. `Err` = this shard alone exceeds the
    /// bucket cap.
    pub(in crate::node) fn push(
        &mut self,
        x: f64,
        interval: f64,
        cap: usize,
        name: &str,
    ) -> Result<(), Status> {
        self.present += 1;
        let idx_f = (x / interval).floor();
        // Any integral float in [-2^63, 2^63) casts exactly; NaN,
        // infinities, and indexes outside i64 have no honest bucket.
        let lo = -(2f64.powi(63));
        let hi = 2f64.powi(63);
        if !idx_f.is_finite() || idx_f < lo || idx_f >= hi {
            self.unbucketable += 1;
            return Ok(());
        }
        self.count(idx_f as i64, cap, name)
    }

    /// Fold one present epoch-micros value into its calendar bucket,
    /// keyed by the bucket's start instant. `Err` = this shard alone
    /// exceeds the bucket cap.
    pub(in crate::node) fn push_calendar(
        &mut self,
        micros: i64,
        unit: crate::pb::CalendarInterval,
        utc_offset_minutes: i32,
        cap: usize,
        name: &str,
    ) -> Result<(), Status> {
        self.present += 1;
        let Some(start) = crate::calendar::bucket_start(micros, unit, utc_offset_minutes) else {
            self.unbucketable += 1;
            return Ok(());
        };
        self.count(start, cap, name)
    }

    pub(in crate::node) fn count(
        &mut self,
        key: i64,
        cap: usize,
        name: &str,
    ) -> Result<(), Status> {
        let n = self.buckets.len();
        let slot = self.buckets.entry(key).or_insert(0);
        *slot += 1;
        if *slot == 1 && n == cap {
            return Err(Status::failed_precondition(format!(
                "histogram {name:?} exceeds {cap} buckets on one shard; use a coarser \
                 interval or a tighter filter"
            )));
        }
        Ok(())
    }

    pub(in crate::node) fn response(&self) -> crate::pb::ShardHistogram {
        let mut rows: Vec<(i64, u64)> = self.buckets.iter().map(|(k, v)| (*k, *v)).collect();
        rows.sort_unstable();
        crate::pb::ShardHistogram {
            bucket_index: rows.iter().map(|r| r.0).collect(),
            bucket_count: rows.iter().map(|r| r.1).collect(),
            present: self.present,
            unbucketable: self.unbucketable,
        }
    }
}

/// One shard's phase-1 percentile fold.
#[derive(Default)]
pub(super) struct PctAcc {
    pub(in crate::node) present: u64,
    pub(in crate::node) unrankable: u64,
    pub(in crate::node) min_bits: u64,
    pub(in crate::node) max_bits: u64,
}

impl PctAcc {
    pub(in crate::node) fn push(&mut self, bits: Option<u64>) {
        let Some(bits) = bits else {
            self.unrankable += 1;
            return;
        };
        if self.present == 0 {
            self.min_bits = bits;
            self.max_bits = bits;
        } else {
            self.min_bits = self.min_bits.min(bits);
            self.max_bits = self.max_bits.max(bits);
        }
        self.present += 1;
    }
}

/// Resolve one percentile expression: int, uint and double rank.
/// `Ok(None)` = the expression can never hold a value on this shard.
pub(in crate::node) fn resolve_rankable(
    store: &Bm25Shard,
    expr: Option<&crate::pb::ValueExpr>,
    name: &str,
    what: &str,
) -> Result<Option<(crate::values::ResolvedValue, crate::pb::AggregateValueType)>, Status> {
    let expr = expr.ok_or_else(|| {
        Status::invalid_argument(format!("{what} {name:?} without an expression"))
    })?;
    let (rv, vt) = crate::values::resolve(expr, store)
        .map_err(|e| Status::invalid_argument(format!("{what} {name:?}: {}", e.message())))?;
    match vt {
        crate::values::ValueType::Uint => Ok(Some((rv, crate::pb::AggregateValueType::Uint))),
        crate::values::ValueType::Int => Ok(Some((rv, crate::pb::AggregateValueType::Int))),
        crate::values::ValueType::Double => Ok(Some((rv, crate::pb::AggregateValueType::Double))),
        crate::values::ValueType::Unknown => Ok(None),
        crate::values::ValueType::Str => Err(Status::invalid_argument(format!(
            "{what} {name:?} ranks numbers; a string does not order here"
        ))),
        crate::values::ValueType::Bool => Err(Status::invalid_argument(format!(
            "{what} {name:?} ranks numbers, not booleans"
        ))),
    }
}

/// Order bits of one evaluated rankable value; `None` = computed NaN
/// (present but unrankable).
pub(in crate::node) fn rankable_bits(
    v: crate::values::Val,
    vtype: crate::pb::AggregateValueType,
) -> Option<u64> {
    match v {
        crate::values::Val::Int(x) => {
            debug_assert_eq!(vtype, crate::pb::AggregateValueType::Int);
            Some(i64_order_bits(x))
        }
        crate::values::Val::Uint(x) => {
            debug_assert_eq!(vtype, crate::pb::AggregateValueType::Uint);
            Some(x)
        }
        crate::values::Val::Double(x) => {
            if x.is_nan() {
                None
            } else {
                Some(f64_order_bits(x))
            }
        }
        _ => unreachable!("resolution typed the expression numeric"),
    }
}

/// The op-versus-type admission rule, shared wording with the
/// coordinator's literal-pinned precheck.
pub(super) fn check_agg_type(
    name: &str,
    op: crate::pb::AggregateOp,
    vt: crate::values::ValueType,
) -> Result<(), Status> {
    use crate::pb::AggregateOp as O;
    use crate::values::ValueType as V;
    if op == O::Cardinality {
        // Distinct values exist for every type, booleans included.
        return Ok(());
    }
    match vt {
        V::Bool => Err(Status::invalid_argument(format!(
            "aggregation {name:?}: a boolean aggregates nowhere; filter on the \
             expression instead and read `matched`"
        ))),
        V::Str if op != O::Count => Err(Status::invalid_argument(format!(
            "aggregation {name:?}: a string expression aggregates only under COUNT"
        ))),
        V::Int | V::Uint if matches!(op, O::Mean | O::Variance | O::Stddev) => {
            Err(Status::invalid_argument(format!(
                "aggregation {name:?}: {} takes a double; convert explicitly with double()",
                agg_op_name(op)
            )))
        }
        _ => Ok(()),
    }
}

/// Lowercase op name for refusals.
pub(crate) fn agg_op_name(op: crate::pb::AggregateOp) -> &'static str {
    use crate::pb::AggregateOp as O;
    match op {
        O::Unspecified => "unspecified",
        O::Count => "count",
        O::Sum => "sum",
        O::Min => "min",
        O::Max => "max",
        O::Mean => "mean",
        O::Variance => "variance",
        O::Stddev => "stddev",
        O::Cardinality => "cardinality",
    }
}

pub(super) type ResolvedAggExpr = (crate::values::ResolvedValue, crate::values::ValueType, bool);
pub(super) type ResolvedHistSpec = (
    Option<crate::values::ResolvedValue>,
    Bucketing,
    usize,
    String,
    HistAcc,
);

pub(super) fn resolve_agg_specs(
    store: &Bm25Shard,
    specs: &[crate::pb::CompiledAggregation],
) -> Result<(Vec<ResolvedAggExpr>, Vec<super::AggAcc>), Status> {
    let mut exprs = Vec::with_capacity(specs.len());
    let mut totals = Vec::with_capacity(specs.len());
    for agg in specs {
        let op = super::agg_op_of(agg.op)?;
        let cardinality = op == crate::pb::AggregateOp::Cardinality;
        if cardinality && agg.max_distinct == 0 {
            return Err(Status::internal(format!(
                "aggregation {:?} arrived without a distinct cap",
                agg.name
            )));
        }
        let expr = agg.expr.as_ref().ok_or_else(|| {
            Status::invalid_argument(format!("aggregation {:?} without an expression", agg.name))
        })?;
        let (rv, vt) = crate::values::resolve(expr, store).map_err(|e| {
            Status::invalid_argument(format!("aggregation {:?}: {}", agg.name, e.message()))
        })?;
        check_agg_type(&agg.name, op, vt)?;
        totals.push(super::acc_of(vt, cardinality));
        exprs.push((rv, vt, cardinality));
    }
    Ok((exprs, totals))
}

pub(super) fn resolve_hist_specs(
    store: &Bm25Shard,
    specs: &[crate::pb::CompiledHistogram],
) -> Result<Vec<ResolvedHistSpec>, Status> {
    let type_name = |vt: crate::values::ValueType| match vt {
        crate::values::ValueType::Str => "string",
        crate::values::ValueType::Bool => "boolean",
        crate::values::ValueType::Int => "int",
        crate::values::ValueType::Uint => "uint",
        crate::values::ValueType::Double => "double",
        crate::values::ValueType::Unknown => "unknown",
    };
    let mut hists = Vec::with_capacity(specs.len());
    for h in specs {
        let bucketing = if h.calendar != 0 {
            let unit = crate::calendar::interval_of(h.calendar).ok_or_else(|| {
                Status::internal(format!(
                    "histogram {:?} arrived with an unvalidated calendar unit",
                    h.name
                ))
            })?;
            Bucketing::Calendar {
                unit,
                utc_offset_minutes: h.utc_offset_minutes,
            }
        } else if h.interval > 0.0 && h.interval.is_finite() {
            Bucketing::Fixed(h.interval)
        } else {
            return Err(Status::internal(format!(
                "histogram {:?} arrived with an unvalidated interval",
                h.name
            )));
        };
        let expr = h.expr.as_ref().ok_or_else(|| {
            Status::invalid_argument(format!("histogram {:?} without an expression", h.name))
        })?;
        let (rv, vt) = crate::values::resolve(expr, store).map_err(|e| {
            Status::invalid_argument(format!("histogram {:?}: {}", h.name, e.message()))
        })?;
        let rv = match (bucketing, vt) {
            (_, crate::values::ValueType::Unknown) => None,
            (Bucketing::Fixed(_), crate::values::ValueType::Double) => Some(rv),
            (Bucketing::Fixed(_), crate::values::ValueType::Int) => {
                return Err(Status::invalid_argument(format!(
                    "histogram {:?} takes a double expression; convert explicitly with double()",
                    h.name
                )))
            }
            (Bucketing::Fixed(_), other) => {
                return Err(Status::invalid_argument(format!(
                    "histogram {:?} takes a double expression, not a {}",
                    h.name,
                    type_name(other)
                )))
            }
            (Bucketing::Calendar { .. }, crate::values::ValueType::Int) => Some(rv),
            (Bucketing::Calendar { .. }, other) => {
                return Err(Status::invalid_argument(format!(
                    "histogram {:?} buckets by calendar over an int expression in \
                     epoch micros (a timestamp column), not a {}",
                    h.name,
                    type_name(other)
                )))
            }
        };
        hists.push((
            rv,
            bucketing,
            h.max_buckets as usize,
            h.name.clone(),
            HistAcc::default(),
        ));
    }
    Ok(hists)
}
