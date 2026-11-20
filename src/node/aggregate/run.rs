use crate::node::*;

use super::{
    acc_of, resolve_agg_specs, resolve_hist_specs, AggAcc, Bucketing, PctAcc, ResolvedAggExpr,
    ResolvedHistSpec,
};

type ResolvedPctSpec = (
    Option<(crate::values::ResolvedValue, crate::pb::AggregateValueType)>,
    PctAcc,
);

struct GroupSpec<'a> {
    name: &'a str,
    facet: Option<usize>,
    cap: usize,
}

struct MetricAccumulators {
    totals: Vec<AggAcc>,
    hists: Vec<ResolvedHistSpec>,
    pcts: Vec<ResolvedPctSpec>,
}

struct AggregateRunState {
    exprs: Vec<ResolvedAggExpr>,
    accs: MetricAccumulators,
    groups: std::collections::HashMap<u32, (u64, Vec<AggAcc>)>,
    matched: u64,
    ungrouped: u64,
}

struct ResponseMeta {
    visibility_fingerprint: Vec<u8>,
    visibility_columns_known: Vec<bool>,
    geo_columns_known: Vec<bool>,
    filter_columns_known: Vec<bool>,
}

fn empty_aggregate_response(
    guard: &ShardState,
    req: &crate::pb::AggregateShardRequest,
    meta: ResponseMeta,
    leaf_count: usize,
) -> Result<crate::pb::AggregateShardResponse, Status> {
    Ok(crate::pb::AggregateShardResponse {
        stats_epoch: guard.stats_epoch,
        stats_incarnation: guard.stats_incarnation.bytes()?,
        visibility_fingerprint: meta.visibility_fingerprint,
        visibility_columns_known: meta.visibility_columns_known,
        segments_total: 0,
        segments_skipped: 0,
        partials: req
            .aggregations
            .iter()
            .map(|_| AggAcc::Absent.partial(None))
            .collect(),
        matched: 0,
        geo_columns_known: meta.geo_columns_known,
        filter_columns_known: meta.filter_columns_known,
        expr_leaves_known: vec![false; leaf_count],
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
    })
}

fn push_doc_metrics(
    doc: u32,
    cols: &ShardNumericRead<'_>,
    exprs: &[ResolvedAggExpr],
    mut group_accs: Option<&mut Vec<AggAcc>>,
    accs: &mut MetricAccumulators,
) -> Result<(), Status> {
    for (i, (rv, _, _)) in exprs.iter().enumerate() {
        if !matches!(accs.totals[i], AggAcc::Absent) {
            if let Some(v) = crate::values::eval(rv, doc, cols) {
                accs.totals[i].push(v);
                if let Some(g) = group_accs.as_deref_mut() {
                    g[i].push(v);
                }
            }
        }
    }
    for (rv, bucketing, cap, name, acc) in &mut accs.hists {
        let Some(rv) = rv else { continue };
        match (*bucketing, crate::values::eval(rv, doc, cols)) {
            (Bucketing::Fixed(interval), Some(crate::values::Val::Double(x))) => {
                acc.push(x, interval, *cap, name)?;
            }
            (
                Bucketing::Calendar {
                    unit,
                    utc_offset_minutes,
                },
                Some(crate::values::Val::Int(m)),
            ) => {
                acc.push_calendar(m, unit, utc_offset_minutes, *cap, name)?;
            }
            _ => {}
        }
    }
    for (resolved, acc) in &mut accs.pcts {
        if let Some((rv, vtype)) = resolved {
            if let Some(v) = crate::values::eval(rv, doc, cols) {
                acc.push(rankable_bits(v, *vtype));
            }
        }
    }
    Ok(())
}

fn fold_aggregate_docs(
    store: &Bm25Shard,
    pass: &AdmittedDocPass<'_>,
    membership: Option<&crate::boolean_bits::Bits>,
    group_spec: GroupSpec<'_>,
    state: &mut AggregateRunState,
) -> Result<(), Status> {
    let grouping = !group_spec.name.is_empty();
    for doc in pass.prune.admitted_slots(pass.rows) {
        if !pass.admits(doc, membership) {
            continue;
        }
        state.matched += 1;
        let group = if grouping {
            match group_spec.facet.and_then(|fi| store.facet_ord(fi, doc)) {
                Some(ord) => {
                    let n_groups = state.groups.len();
                    let entry = state.groups.entry(ord).or_insert_with(|| {
                        let accs = state
                            .exprs
                            .iter()
                            .map(|(_, vt, c)| acc_of(*vt, *c))
                            .collect();
                        (0, accs)
                    });
                    if entry.0 == 0 && n_groups == group_spec.cap {
                        return Err(Status::failed_precondition(format!(
                            "group_by {:?} exceeds {} distinct values on \
                             one shard; tighten the filter or raise max_groups",
                            group_spec.name, group_spec.cap
                        )));
                    }
                    entry.0 += 1;
                    Some(entry)
                }
                None => {
                    state.ungrouped += 1;
                    None
                }
            }
        } else {
            None
        };
        push_doc_metrics(
            doc,
            &pass.cols,
            &state.exprs,
            group.map(|g| &mut g.1),
            &mut state.accs,
        )?;
    }
    Ok(())
}

fn finalize_groups_and_caps(
    store: &Bm25Shard,
    specs: &[crate::pb::CompiledAggregation],
    totals: &[AggAcc],
    group_facet: Option<usize>,
    groups: std::collections::HashMap<u32, (u64, Vec<AggAcc>)>,
) -> Result<Vec<crate::pb::AggregateShardGroup>, Status> {
    for (agg, acc) in specs.iter().zip(totals) {
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
    Ok(group_facet
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
        .unwrap_or_default())
}

fn prepare_aggregate_state(
    store: &Bm25Shard,
    req: &crate::pb::AggregateShardRequest,
) -> Result<AggregateRunState, Status> {
    let (exprs, totals) = resolve_agg_specs(store, &req.aggregations)?;
    let hists = resolve_hist_specs(store, &req.histograms)?;
    let mut pcts = Vec::with_capacity(req.percentiles.len());
    for spec in &req.percentiles {
        pcts.push((
            resolve_rankable(store, spec.expr.as_ref(), &spec.name, "percentile")?,
            PctAcc::default(),
        ));
    }
    Ok(AggregateRunState {
        exprs,
        accs: MetricAccumulators {
            totals,
            hists,
            pcts,
        },
        groups: std::collections::HashMap::new(),
        matched: 0,
        ungrouped: 0,
    })
}

fn collect_aggregate_leaves(
    req: &crate::pb::AggregateShardRequest,
) -> Vec<crate::values::ValueLeaf> {
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
    leaves
}

fn build_response_meta(
    guard: &ShardState,
    req: &crate::pb::AggregateShardRequest,
) -> Result<ResponseMeta, Status> {
    let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
    let (_, visibility_columns_known) = filter_known_flags(
        guard.bm25.as_ref(),
        &[],
        req.visibility.as_ref().and_then(|v| v.filter.as_ref()),
    );
    let (geo_columns_known, filter_columns_known) =
        filter_known_flags(guard.bm25.as_ref(), &req.geo_filters, req.filter.as_ref());
    Ok(ResponseMeta {
        visibility_fingerprint: scope.fingerprint().to_vec(),
        visibility_columns_known,
        geo_columns_known,
        filter_columns_known,
    })
}

impl AggregateRunState {
    fn into_response(
        self,
        guard: &ShardState,
        store: &Bm25Shard,
        prune: crate::segment_prune::PruneStats,
        expr_leaves_known: Vec<bool>,
        group_facet: Option<usize>,
        groups: Vec<crate::pb::AggregateShardGroup>,
    ) -> Result<crate::pb::AggregateShardResponse, Status> {
        Ok(crate::pb::AggregateShardResponse {
            stats_epoch: guard.stats_epoch,
            stats_incarnation: guard.stats_incarnation.bytes()?,
            visibility_fingerprint: Vec::new(),
            visibility_columns_known: Vec::new(),
            segments_total: prune.segments_total,
            segments_skipped: prune.segments_skipped,
            partials: self
                .accs
                .totals
                .iter()
                .map(|acc| acc.partial(Some(store)))
                .collect(),
            matched: self.matched,
            geo_columns_known: Vec::new(),
            filter_columns_known: Vec::new(),
            expr_leaves_known,
            groups,
            ungrouped: self.ungrouped,
            group_column_known: group_facet.is_some(),
            histograms: self
                .accs
                .hists
                .iter()
                .map(|(_, _, _, _, acc)| acc.response())
                .collect(),
            percentile_partials: self
                .accs
                .pcts
                .iter()
                .map(|(resolved, acc)| crate::pb::PercentilePartial {
                    vtype: resolved
                        .as_ref()
                        .map_or(crate::pb::AggregateValueType::Absent as i32, |(_, vt)| {
                            *vt as i32
                        }),
                    present: acc.present,
                    unrankable: acc.unrankable,
                    min_bits: acc.min_bits,
                    max_bits: acc.max_bits,
                })
                .collect(),
        })
    }
}

impl NodeServiceImpl {
    /// Folds one `AggregateShard` request over this shard under `guard`.
    pub(in crate::node) fn run_aggregate_shard(
        &self,
        guard: &ShardState,
        req: crate::pb::AggregateShardRequest,
        membership: Option<&crate::boolean_bits::Bits>,
    ) -> Result<crate::pb::AggregateShardResponse, Status> {
        let meta = build_response_meta(guard, &req)?;
        let filter =
            crate::visibility::intersect_filter(req.visibility.as_ref(), req.filter.clone())?;
        if req.aggregations.is_empty() && req.histograms.is_empty() && req.percentiles.is_empty() {
            return Err(Status::invalid_argument(
                "aggregate requires at least one aggregation, histogram, or percentile",
            ));
        }
        let geo_regions = validate_geo_filters(&req.geo_filters)?;
        guard.check_stats_epoch(req.expected_stats_epoch, &req.expected_stats_incarnation)?;
        let leaves = collect_aggregate_leaves(&req);
        let Some(store) = guard.bm25.as_ref() else {
            return empty_aggregate_response(guard, &req, meta, leaves.len());
        };
        let expr_leaves_known = leaves
            .iter()
            .map(|l| crate::values::leaf_known(l, store))
            .collect();
        let group_facet = (!req.group_by.is_empty())
            .then(|| store.facet_index(&req.group_by))
            .flatten();
        let mut state = prepare_aggregate_state(store, &req)?;
        let pass = self.admitted_doc_pass(
            guard,
            store,
            DocPassSpec {
                geo_filters: &req.geo_filters,
                geo_regions: &geo_regions,
                filter: filter.as_ref(),
                doc_ids: req.restrict_doc_ids.then_some(req.doc_ids.as_slice()),
            },
        )?;
        fold_aggregate_docs(
            store,
            &pass,
            membership,
            GroupSpec {
                name: &req.group_by,
                facet: group_facet,
                cap: req.max_groups as usize,
            },
            &mut state,
        )?;
        let groups = finalize_groups_and_caps(
            store,
            &req.aggregations,
            &state.accs.totals,
            group_facet,
            std::mem::take(&mut state.groups),
        )?;
        let mut resp = state.into_response(
            guard,
            store,
            pass.prune.stats,
            expr_leaves_known,
            group_facet,
            groups,
        )?;
        resp.visibility_fingerprint = meta.visibility_fingerprint;
        resp.visibility_columns_known = meta.visibility_columns_known;
        resp.geo_columns_known = meta.geo_columns_known;
        resp.filter_columns_known = meta.filter_columns_known;
        Ok(resp)
    }
}
