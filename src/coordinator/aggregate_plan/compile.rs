//! Compilation of public projection and aggregation requests.

use super::*;

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
    check_spec_counts(req)?;
    // One name namespace across aggregations and histograms.
    let mut names = std::collections::HashSet::new();
    let mut compiled = Vec::with_capacity(req.aggregations.len());
    for a in &req.aggregations {
        compiled.push(compile_aggregation(a, &mut names)?);
    }
    let mut compiled_hists = Vec::with_capacity(req.histograms.len());
    for h in &req.histograms {
        compiled_hists.push(compile_histogram(h, &mut names)?);
    }
    let mut compiled_pcts = Vec::with_capacity(req.percentiles.len());
    for spec in &req.percentiles {
        compiled_pcts.push(compile_percentile(spec, &mut names)?);
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

/// Refuse an empty request and one past the per-request spec ceilings.
fn check_spec_counts(req: &crate::pb::AggregateRequest) -> Result<(), Status> {
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
    Ok(())
}

fn compile_aggregation<'a>(
    a: &'a crate::pb::Aggregation,
    names: &mut std::collections::HashSet<&'a str>,
) -> Result<crate::pb::CompiledAggregation, Status> {
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
    Ok(crate::pb::CompiledAggregation {
        expr: Some(expr),
        op: a.op,
        name: a.name.clone(),
        max_distinct: match (cardinality, a.max_distinct) {
            (false, _) => 0,
            (true, 0) => DEFAULT_MAX_DISTINCT,
            (true, cap) => cap,
        },
    })
}

fn compile_histogram<'a>(
    h: &'a crate::pb::HistogramSpec,
    names: &mut std::collections::HashSet<&'a str>,
) -> Result<crate::pb::CompiledHistogram, Status> {
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
    let calendar = histogram_calendar(h)?;
    let expr = crate::cel::compile_value(&h.expression).map_err(|e| {
        Status::invalid_argument(format!("histogram {:?}: {}", h.name, e.message()))
    })?;
    Ok(crate::pb::CompiledHistogram {
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
    })
}

/// The calendar unit a histogram buckets by, or `None` for a fixed
/// interval; each form refuses the other's parameters.
fn histogram_calendar(
    h: &crate::pb::HistogramSpec,
) -> Result<Option<crate::pb::CalendarInterval>, Status> {
    if h.calendar != 0 {
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
        Ok(Some(unit))
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
        Ok(None)
    }
}

fn compile_percentile<'a>(
    spec: &'a crate::pb::PercentileSpec,
    names: &mut std::collections::HashSet<&'a str>,
) -> Result<crate::pb::CompiledPercentile, Status> {
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
    Ok(crate::pb::CompiledPercentile {
        expr: Some(expr),
        name: spec.name.clone(),
    })
}
