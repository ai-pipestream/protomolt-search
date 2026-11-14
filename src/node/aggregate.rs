//! Per-shard numeric, string, histogram, and quantile aggregations.

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
enum AggAcc {
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

impl AggAcc {
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
                if *present == 0 {
                    *min = x;
                    *max = x;
                } else {
                    *min = (*min).min(x);
                    *max = (*max).max(x);
                }
                *present += 1;
                *sum += i128::from(x);
                if let Some(set) = distinct {
                    set.insert(x);
                }
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
                if *present == 0 {
                    *min = x;
                    *max = x;
                } else {
                    *min = (*min).min(x);
                    *max = (*max).max(x);
                }
                *present += 1;
                // A shard has at most u32::MAX rows, so u128 cannot overflow.
                *sum += u128::from(x);
                if let Some(set) = distinct {
                    set.insert(x);
                }
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
                // Neumaier: the compensation catches whichever addend
                // the naive add truncated.
                let t = *sum + x;
                *compensation += if sum.abs() >= x.abs() {
                    (*sum - t) + x
                } else {
                    (x - t) + *sum
                };
                *sum = t;
                // Welford, in doc order.
                let n = *present as f64;
                let delta = x - *mean;
                *mean += delta / n;
                *m2 += delta * (x - *mean);
            }
            (AggAcc::Str { present, distinct }, crate::values::Val::FacetOrd { column, ord }) => {
                *present += 1;
                if let Some(set) = distinct {
                    set.insert((false, column, ord));
                }
            }
            (
                AggAcc::Str { present, distinct },
                crate::values::Val::MapFacetOrd { column, ord },
            ) => {
                *present += 1;
                if let Some(set) = distinct {
                    set.insert((true, column, ord));
                }
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
                if b {
                    *seen_true = true;
                } else {
                    *seen_false = true;
                }
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

    /// The wire partial. `store` renders string ordinals; only a
    /// document-less shard, whose accumulators are all absent, has
    /// none to offer.
    pub(in crate::node) fn partial(
        &self,
        store: Option<&Bm25Shard>,
    ) -> crate::pb::AggregatePartial {
        use crate::pb::AggregateValueType as T;
        let mut p = crate::pb::AggregatePartial {
            vtype: T::Absent as i32,
            ..Default::default()
        };
        match self {
            AggAcc::Absent => {}
            AggAcc::Int {
                present,
                sum,
                min,
                max,
                distinct,
            } => {
                p.vtype = T::Int as i32;
                p.present = *present;
                p.int_sum_hi = (*sum >> 64) as i64;
                p.int_sum_lo = *sum as u64;
                p.int_min = *min;
                p.int_max = *max;
                if let Some(set) = distinct {
                    let mut values: Vec<i64> = set.iter().copied().collect();
                    values.sort_unstable();
                    p.distinct_ints = values;
                }
            }
            AggAcc::Uint {
                present,
                sum,
                min,
                max,
                distinct,
            } => {
                p.vtype = T::Uint as i32;
                p.present = *present;
                p.uint_sum_hi = (*sum >> 64) as u64;
                p.uint_sum_lo = *sum as u64;
                p.uint_min = *min;
                p.uint_max = *max;
                if let Some(set) = distinct {
                    let mut values: Vec<u64> = set.iter().copied().collect();
                    values.sort_unstable();
                    p.distinct_uints = values;
                }
            }
            AggAcc::Double {
                present,
                sum,
                compensation,
                min,
                max,
                mean,
                m2,
                distinct,
            } => {
                p.vtype = T::Double as i32;
                p.present = *present;
                p.double_sum = *sum;
                p.double_compensation = *compensation;
                p.double_min = *min;
                p.double_max = *max;
                p.mean = *mean;
                p.m2 = *m2;
                if let Some(set) = distinct {
                    let mut values: Vec<u64> = set.iter().copied().collect();
                    values.sort_unstable();
                    p.distinct_double_bits = values;
                }
            }
            AggAcc::Str { present, distinct } => {
                p.vtype = T::String as i32;
                p.present = *present;
                if let (Some(set), Some(store)) = (distinct, store) {
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
                    p.distinct_strings = values;
                }
            }
            AggAcc::Bool {
                present,
                seen_true,
                seen_false,
            } => {
                p.vtype = T::Bool as i32;
                p.present = *present;
                if *seen_false {
                    p.distinct_bools.push(false);
                }
                if *seen_true {
                    p.distinct_bools.push(true);
                }
            }
        }
        p
    }
}

/// A fresh accumulator for one resolved type; `cardinality` adds the
/// distinct set the op counts.
fn acc_of(vt: crate::values::ValueType, cardinality: bool) -> AggAcc {
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

/// How one histogram buckets: a fixed width over doubles, or a
/// calendar unit over epoch-micros ints (docs/aggregations.md).
#[derive(Clone, Copy)]
enum Bucketing {
    Fixed(f64),
    Calendar {
        unit: crate::pb::CalendarInterval,
        utc_offset_minutes: i32,
    },
}

/// One shard's sparse histogram fold.
#[derive(Default)]
struct HistAcc {
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
struct PctAcc {
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
fn check_agg_type(
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

impl NodeServiceImpl {
    /// The fold of one `AggregateShard` request over this shard, under
    /// `guard`. With `membership`, the admitted set is that bitmap (the
    /// shard-side Boolean planner's match set) and the request's own
    /// filters and id allowlist must be empty.
    pub(in crate::node) fn run_aggregate_shard(
        &self,
        guard: &ShardState,
        req: crate::pb::AggregateShardRequest,
        membership: Option<&crate::boolean_bits::Bits>,
    ) -> Result<crate::pb::AggregateShardResponse, Status> {
        let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
        let filter =
            crate::visibility::intersect_filter(req.visibility.as_ref(), req.filter.clone())?;
        if req.aggregations.is_empty() && req.histograms.is_empty() && req.percentiles.is_empty() {
            return Err(Status::invalid_argument(
                "aggregate requires at least one aggregation, histogram, or percentile",
            ));
        }
        let grouping = !req.group_by.is_empty();
        let group_cap = req.max_groups as usize;
        let geo_regions = validate_geo_filters(&req.geo_filters)?;
        guard.check_stats_epoch(req.expected_stats_epoch, &req.expected_stats_incarnation)?;
        let (_, visibility_columns_known) = filter_known_flags(
            guard.bm25.as_ref(),
            &[],
            req.visibility
                .as_ref()
                .and_then(|view| view.filter.as_ref()),
        );
        let (geo_columns_known, filter_columns_known) =
            filter_known_flags(guard.bm25.as_ref(), &req.geo_filters, req.filter.as_ref());
        // Expression column leaves: aggregations first, then
        // histograms, request order then depth-first — the projection
        // typo contract.
        let mut leaves = Vec::new();
        for expr in req
            .aggregations
            .iter()
            .filter_map(|a| a.expr.as_ref())
            .chain(req.histograms.iter().filter_map(|h| h.expr.as_ref()))
            .chain(req.percentiles.iter().filter_map(|p| p.expr.as_ref()))
        {
            crate::values::column_leaves(expr, &mut leaves);
        }
        let Some(store) = guard.bm25.as_ref() else {
            // A document-less shard holds no values and no columns; its
            // all-absent partials and all-false flags feed the merge
            // and the typo rule like everywhere.
            return Ok(crate::pb::AggregateShardResponse {
                stats_epoch: guard.stats_epoch,
                stats_incarnation: guard.stats_incarnation.bytes()?,
                visibility_fingerprint: scope.fingerprint().to_vec(),
                visibility_columns_known: visibility_columns_known.clone(),
                segments_total: 0,
                segments_skipped: 0,
                partials: req
                    .aggregations
                    .iter()
                    .map(|_| AggAcc::Absent.partial(None))
                    .collect(),
                matched: 0,
                geo_columns_known,
                filter_columns_known,
                expr_leaves_known: vec![false; leaves.len()],
                groups: Vec::new(),
                ungrouped: 0,
                group_column_known: false,
                histograms: req
                    .histograms
                    .iter()
                    .map(|_| crate::pb::ShardHistogram::default())
                    .collect(),
                percentile_partials: req
                    .percentiles
                    .iter()
                    .map(|_| crate::pb::PercentilePartial {
                        vtype: crate::pb::AggregateValueType::Absent as i32,
                        ..Default::default()
                    })
                    .collect(),
            });
        };
        if store.as_index().is_none() {
            return Err(Status::failed_precondition(
                "bm25 bulk build in progress; Flush before aggregating",
            ));
        }
        let expr_leaves_known: Vec<bool> = leaves
            .iter()
            .map(|l| crate::values::leaf_known(l, store))
            .collect();
        let group_facet = grouping.then(|| store.facet_index(&req.group_by)).flatten();
        let group_column_known = group_facet.is_some();
        // Resolve every expression and admit its op against the
        // resolved type BEFORE touching any document: a type conflict
        // refuses the request, it never mis-aggregates.
        let mut exprs: Vec<(crate::values::ResolvedValue, crate::values::ValueType, bool)> =
            Vec::with_capacity(req.aggregations.len());
        let mut totals: Vec<AggAcc> = Vec::with_capacity(req.aggregations.len());
        for agg in &req.aggregations {
            let op = agg_op_of(agg.op)?;
            let cardinality = op == crate::pb::AggregateOp::Cardinality;
            if cardinality && agg.max_distinct == 0 {
                return Err(Status::internal(format!(
                    "aggregation {:?} arrived without a distinct cap",
                    agg.name
                )));
            }
            let expr = agg.expr.as_ref().ok_or_else(|| {
                Status::invalid_argument(format!(
                    "aggregation {:?} without an expression",
                    agg.name
                ))
            })?;
            let (rv, vt) = crate::values::resolve(expr, store).map_err(|e| {
                Status::invalid_argument(format!("aggregation {:?}: {}", agg.name, e.message()))
            })?;
            check_agg_type(&agg.name, op, vt)?;
            totals.push(acc_of(vt, cardinality));
            exprs.push((rv, vt, cardinality));
        }
        let mut hists: Vec<(
            Option<crate::values::ResolvedValue>,
            Bucketing,
            usize,
            String,
            HistAcc,
        )> = Vec::with_capacity(req.histograms.len());
        for h in &req.histograms {
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
            } else {
                if !(h.interval > 0.0 && h.interval.is_finite()) {
                    return Err(Status::internal(format!(
                        "histogram {:?} arrived with an unvalidated interval",
                        h.name
                    )));
                }
                Bucketing::Fixed(h.interval)
            };
            let expr = h.expr.as_ref().ok_or_else(|| {
                Status::invalid_argument(format!("histogram {:?} without an expression", h.name))
            })?;
            let (rv, vt) = crate::values::resolve(expr, store).map_err(|e| {
                Status::invalid_argument(format!("histogram {:?}: {}", h.name, e.message()))
            })?;
            let type_name = |vt: crate::values::ValueType| match vt {
                crate::values::ValueType::Str => "string",
                crate::values::ValueType::Bool => "boolean",
                crate::values::ValueType::Int => "int",
                crate::values::ValueType::Uint => "uint",
                crate::values::ValueType::Double => "double",
                crate::values::ValueType::Unknown => "unknown",
            };
            let rv = match (bucketing, vt) {
                (_, crate::values::ValueType::Unknown) => None,
                (Bucketing::Fixed(_), crate::values::ValueType::Double) => Some(rv),
                (Bucketing::Fixed(_), crate::values::ValueType::Int) => {
                    return Err(Status::invalid_argument(format!(
                        "histogram {:?} takes a double expression; convert explicitly \
                     with double()",
                        h.name
                    )));
                }
                (Bucketing::Fixed(_), other) => {
                    return Err(Status::invalid_argument(format!(
                        "histogram {:?} takes a double expression, not a {}",
                        h.name,
                        type_name(other)
                    )));
                }
                (Bucketing::Calendar { .. }, crate::values::ValueType::Int) => Some(rv),
                (Bucketing::Calendar { .. }, other) => {
                    return Err(Status::invalid_argument(format!(
                        "histogram {:?} buckets by calendar over an int expression in \
                         epoch micros (a timestamp column), not a {}",
                        h.name,
                        type_name(other)
                    )));
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
        let mut pcts: Vec<(
            Option<(crate::values::ResolvedValue, crate::pb::AggregateValueType)>,
            PctAcc,
        )> = Vec::with_capacity(req.percentiles.len());
        for spec in &req.percentiles {
            let resolved = resolve_rankable(store, spec.expr.as_ref(), &spec.name, "percentile")?;
            pcts.push((resolved, PctAcc::default()));
        }
        let doc_filter = crate::filter::DocFilter {
            deleted: guard.live_docs.words(),
            geo: store.resolve_geo_filters(&req.geo_filters, &geo_regions),
            pred: filter
                .as_ref()
                .map(|f| store.resolve_filter(f))
                .transpose()?,
            phrase: Vec::new(),
        };
        let cols = ShardNumericRead(store);
        let n = u64::from(store.next_doc_id());
        let prune = prune_segments(
            store,
            doc_filter.pred.as_ref(),
            self.knobs.segment_pruning(),
        );
        let id_allowlist = req.restrict_doc_ids.then(|| {
            req.doc_ids
                .iter()
                .filter_map(|id| id.checked_sub(self.config.slot_offset))
                .filter_map(|local| u32::try_from(local).ok())
                .collect::<std::collections::HashSet<_>>()
        });
        let mut matched = 0u64;
        let mut ungrouped = 0u64;
        // Group accumulators by facet ordinal; each holds (matched,
        // one accumulator per aggregation).
        let mut groups: std::collections::HashMap<u32, (u64, Vec<AggAcc>)> =
            std::collections::HashMap::new();
        // One pass in doc order: the fold orders themselves are part
        // of the determinism contract (Neumaier and Welford both fold
        // exactly this sequence on every run).
        for doc in prune.admitted_slots(n as u32) {
            if id_allowlist
                .as_ref()
                .is_some_and(|allowlist| !allowlist.contains(&doc))
            {
                continue;
            }
            if membership.is_some_and(|members| !members.test(doc as usize)) {
                continue;
            }
            if !doc_filter.passes(doc, &cols) {
                continue;
            }
            matched += 1;
            let group = if grouping {
                match group_facet.and_then(|fi| store.facet_ord(fi, doc)) {
                    Some(ord) => {
                        let n_groups = groups.len();
                        let entry = groups.entry(ord).or_insert_with(|| {
                            (
                                0,
                                exprs
                                    .iter()
                                    .map(|(_, vt, cardinality)| acc_of(*vt, *cardinality))
                                    .collect(),
                            )
                        });
                        if entry.0 == 0 && n_groups == group_cap {
                            return Err(Status::failed_precondition(format!(
                                "group_by {:?} exceeds {group_cap} distinct values on \
                             one shard; tighten the filter or raise max_groups",
                                req.group_by
                            )));
                        }
                        entry.0 += 1;
                        Some(entry)
                    }
                    None => {
                        ungrouped += 1;
                        None
                    }
                }
            } else {
                None
            };
            let mut group_accs = group.map(|g| &mut g.1);
            for (i, (rv, _, _)) in exprs.iter().enumerate() {
                // Absent-typed totals imply an absent-typed group
                // accumulator: the type is the expression's, not the
                // group's.
                if matches!(totals[i], AggAcc::Absent) {
                    continue;
                }
                if let Some(v) = crate::values::eval(rv, doc, &cols) {
                    totals[i].push(v);
                    if let Some(accs) = group_accs.as_deref_mut() {
                        accs[i].push(v);
                    }
                }
            }
            for (rv, bucketing, cap, name, acc) in hists.iter_mut() {
                let Some(rv) = rv else { continue };
                match (*bucketing, crate::values::eval(rv, doc, &cols)) {
                    (Bucketing::Fixed(interval), Some(crate::values::Val::Double(x))) => {
                        acc.push(x, interval, *cap, name)?;
                    }
                    (
                        Bucketing::Calendar {
                            unit,
                            utc_offset_minutes,
                        },
                        Some(crate::values::Val::Int(micros)),
                    ) => {
                        acc.push_calendar(micros, unit, utc_offset_minutes, *cap, name)?;
                    }
                    _ => {}
                }
            }
            for (resolved, acc) in pcts.iter_mut() {
                let Some((rv, vtype)) = resolved else {
                    continue;
                };
                if let Some(v) = crate::values::eval(rv, doc, &cols) {
                    acc.push(rankable_bits(v, *vtype));
                }
            }
        }
        for (agg, acc) in req.aggregations.iter().zip(&totals) {
            if let Some(n) = acc.distinct_len() {
                if n > agg.max_distinct as usize {
                    return Err(Status::failed_precondition(format!(
                        "aggregation {:?}: more than {} distinct values on one shard; \
                         raise max_distinct or tighten the filter",
                        agg.name, agg.max_distinct
                    )));
                }
            }
        }
        let mut group_rows: Vec<(u32, u64, Vec<AggAcc>)> = groups
            .into_iter()
            .map(|(ord, (m, accs))| (ord, m, accs))
            .collect();
        group_rows.sort_unstable_by_key(|r| r.0);
        let groups = group_facet
            .map(|fi| {
                group_rows
                    .iter()
                    .map(|(ord, m, accs)| crate::pb::AggregateShardGroup {
                        value: store.facet_value(fi, *ord).to_string(),
                        matched: *m,
                        partials: accs.iter().map(|acc| acc.partial(Some(store))).collect(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(crate::pb::AggregateShardResponse {
            stats_epoch: guard.stats_epoch,
            stats_incarnation: guard.stats_incarnation.bytes()?,
            visibility_fingerprint: scope.fingerprint().to_vec(),
            visibility_columns_known: visibility_columns_known.clone(),
            segments_total: prune.stats.segments_total,
            segments_skipped: prune.stats.segments_skipped,
            partials: totals.iter().map(|acc| acc.partial(Some(store))).collect(),
            matched,
            geo_columns_known,
            filter_columns_known,
            expr_leaves_known,
            groups,
            ungrouped,
            group_column_known,
            histograms: hists
                .iter()
                .map(|(_, _, _, _, acc)| acc.response())
                .collect(),
            percentile_partials: pcts
                .iter()
                .map(|(resolved, acc)| {
                    use crate::pb::AggregateValueType as T;
                    crate::pb::PercentilePartial {
                        vtype: match resolved {
                            None => T::Absent as i32,
                            Some((_, vtype)) => *vtype as i32,
                        },
                        present: acc.present,
                        unrankable: acc.unrankable,
                        min_bits: acc.min_bits,
                        max_bits: acc.max_bits,
                    }
                })
                .collect(),
        })
    }
}
