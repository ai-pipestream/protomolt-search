//! Composed value routes: documents, stored values, aggregates, and browse pages.

use tonic::{Request, Response, Status};

use crate::metrics::Route;

use super::merge_lexical::{
    add_count, add_count32, child_of_id, child_ranges, merge_known, route_ids,
};
use super::{fan_out, grpc_timeout, in_caller_order, read_metadata, RelayService};

/// A per-column scalar type the children report beside a known flag:
/// the children that know the column must agree on the type, and a
/// child that does not know it reports `Unspecified`.
fn merge_column_types(
    what: &str,
    shard: usize,
    acc: &mut Option<Vec<i32>>,
    share: &[i32],
) -> Result<(), Status> {
    match acc {
        None => *acc = Some(share.to_vec()),
        Some(types) => {
            if types.len() != share.len() {
                return Err(Status::internal(format!(
                    "relay: child {shard} answered {} {what} types while an earlier child \
                     answered {}",
                    share.len(),
                    types.len()
                )));
            }
            let unspecified = crate::pb::ScalarValueType::Unspecified as i32;
            for (column, (held, &reported)) in types.iter_mut().zip(share).enumerate() {
                if reported == unspecified {
                    continue;
                }
                if *held == unspecified {
                    *held = reported;
                } else if *held != reported {
                    return Err(Status::failed_precondition(format!(
                        "relay: child {shard} types {what} column {column} as {reported} while \
                         an earlier child typed it {held}; the column families diverge across \
                         the children"
                    )));
                }
            }
        }
    }
    Ok(())
}

/// The children's `AggregateShard` answers folded into one shard's
/// answer, in child order: each partial folds through the root's own
/// fold ([`crate::coordinator::AggMerge`]) and comes back out as a
/// partial, groups join by value, histograms sum by bucket, percentile
/// partials fold, the match counts add with a check, and the known
/// flags OR as every child sizes them. The caps a root applies
/// (`max_groups`, `max_buckets`, `max_distinct`) apply here, so a relay
/// refuses where the root would. The receipt fields are the caller's to
/// fill. A relay whose children are a prefix of the root's shard order
/// (one relay over all of them, or a chain) folds bit for bit as the
/// root folds the shards; relays side by side fold their doubles
/// compensated at each level, which can differ from the flat fold in
/// the last bit (ints, counts, extrema, histograms, and percentile
/// partials are exact in any grouping).
pub fn merge_aggregate_shares(
    aggregations: &[crate::pb::CompiledAggregation],
    group_by: &str,
    max_groups: u32,
    histograms: &[crate::pb::CompiledHistogram],
    percentiles: &[crate::pb::CompiledPercentile],
    shares: &[crate::pb::AggregateShardResponse],
) -> Result<crate::pb::AggregateShardResponse, Status> {
    use crate::coordinator::{AggMerge, PctMerge};
    use std::collections::BTreeMap;
    let mut geo_known: Option<Vec<bool>> = None;
    let mut filter_known: Option<Vec<bool>> = None;
    let mut leaves_known: Option<Vec<bool>> = None;
    let mut matched = 0u64;
    let mut ungrouped = 0u64;
    let mut group_column_known = false;
    let mut merged: Vec<AggMerge> = aggregations.iter().map(|_| AggMerge::new()).collect();
    let mut groups: BTreeMap<String, (u64, Vec<AggMerge>)> = BTreeMap::new();
    let mut buckets: Vec<BTreeMap<i64, u64>> = histograms.iter().map(|_| BTreeMap::new()).collect();
    let mut present = vec![0u64; histograms.len()];
    let mut unbucketable = vec![0u64; histograms.len()];
    let mut pct: Vec<PctMerge> = percentiles.iter().map(|_| PctMerge::new()).collect();
    let mut segments_total = 0u32;
    let mut segments_skipped = 0u32;
    for (child, share) in shares.iter().enumerate() {
        merge_known("geo", child, &mut geo_known, &share.geo_columns_known)?;
        merge_known(
            "filter",
            child,
            &mut filter_known,
            &share.filter_columns_known,
        )?;
        merge_known(
            "expression-leaf",
            child,
            &mut leaves_known,
            &share.expr_leaves_known,
        )?;
        if share.partials.len() != aggregations.len() {
            return Err(Status::internal(format!(
                "relay: child {child} answered {} aggregation partials for {} aggregations",
                share.partials.len(),
                aggregations.len()
            )));
        }
        if share.histograms.len() != histograms.len() {
            return Err(Status::internal(format!(
                "relay: child {child} answered {} histograms for {} requested",
                share.histograms.len(),
                histograms.len()
            )));
        }
        if share.percentile_partials.len() != percentiles.len() {
            return Err(Status::internal(format!(
                "relay: child {child} answered {} percentile partials for {} requested",
                share.percentile_partials.len(),
                percentiles.len()
            )));
        }
        group_column_known |= share.group_column_known;
        add_count(&mut matched, share.matched, child, "aggregate match count")?;
        add_count(&mut ungrouped, share.ungrouped, child, "ungrouped count")?;
        add_count32(
            &mut segments_total,
            share.segments_total,
            child,
            "segment count",
        )?;
        add_count32(
            &mut segments_skipped,
            share.segments_skipped,
            child,
            "skipped segment count",
        )?;
        for (m, (p, agg)) in merged
            .iter_mut()
            .zip(share.partials.iter().zip(aggregations))
        {
            m.fold(p, agg)?;
        }
        for group in &share.groups {
            if group.partials.len() != aggregations.len() {
                return Err(Status::internal(format!(
                    "relay: child {child} answered {} group partials for {} aggregations",
                    group.partials.len(),
                    aggregations.len()
                )));
            }
            let entry = groups
                .entry(group.value.clone())
                .or_insert_with(|| (0, aggregations.iter().map(|_| AggMerge::new()).collect()));
            add_count(&mut entry.0, group.matched, child, "group match count")?;
            for (m, (p, agg)) in entry
                .1
                .iter_mut()
                .zip(group.partials.iter().zip(aggregations))
            {
                m.fold(p, agg)?;
            }
            if groups.len() > max_groups as usize {
                return Err(Status::failed_precondition(format!(
                    "group_by {group_by:?} exceeds {max_groups} distinct values across the \
                     relay's children; tighten the filter or raise max_groups"
                )));
            }
        }
        for (i, (hist, spec)) in share.histograms.iter().zip(histograms).enumerate() {
            if hist.bucket_index.len() != hist.bucket_count.len() {
                return Err(Status::internal(format!(
                    "relay: child {child} answered a histogram with mismatched columns"
                )));
            }
            add_count(
                &mut present[i],
                hist.present,
                child,
                "histogram present count",
            )?;
            add_count(
                &mut unbucketable[i],
                hist.unbucketable,
                child,
                "histogram unbucketable count",
            )?;
            for (&index, &count) in hist.bucket_index.iter().zip(&hist.bucket_count) {
                add_count(
                    buckets[i].entry(index).or_insert(0),
                    count,
                    child,
                    "histogram bucket count",
                )?;
            }
            if buckets[i].len() > spec.max_buckets as usize {
                return Err(Status::failed_precondition(format!(
                    "histogram {:?} exceeds {} buckets across the relay's children; use a \
                     coarser interval or a tighter filter",
                    spec.name, spec.max_buckets
                )));
            }
        }
        for (m, (p, spec)) in pct
            .iter_mut()
            .zip(share.percentile_partials.iter().zip(percentiles))
        {
            m.fold(p, &spec.name)?;
        }
    }
    Ok(crate::pb::AggregateShardResponse {
        partials: merged.iter().map(AggMerge::partial).collect(),
        matched,
        geo_columns_known: geo_known.unwrap_or_default(),
        filter_columns_known: filter_known.unwrap_or_default(),
        expr_leaves_known: leaves_known.unwrap_or_default(),
        groups: groups
            .into_iter()
            .map(|(value, (matched, ms))| crate::pb::AggregateShardGroup {
                value,
                matched,
                partials: ms.iter().map(AggMerge::partial).collect(),
            })
            .collect(),
        ungrouped,
        group_column_known,
        histograms: buckets
            .iter()
            .enumerate()
            .map(|(i, b)| crate::pb::ShardHistogram {
                bucket_index: b.keys().copied().collect(),
                bucket_count: b.values().copied().collect(),
                present: present[i],
                unbucketable: unbucketable[i],
            })
            .collect(),
        percentile_partials: pct.iter().map(PctMerge::partial).collect(),
        segments_total,
        segments_skipped,
        stats_epoch: 0,
        stats_incarnation: Vec::new(),
        visibility_fingerprint: Vec::new(),
        visibility_columns_known: Vec::new(),
    })
}

/// The children's browse pages as one page: the rows sorted by the
/// request's keys then id (id order alone when unsorted, the order the
/// root sorts a node's page in), cut to `k`, each row's keys re-emitted
/// as its child sent them; known flags OR, the sort column types must
/// agree where known, segment counts add with a check. The receipt
/// fields are the caller's to fill.
pub fn merge_browse_pages(
    req: &crate::pb::BrowseShardRequest,
    shares: &[crate::pb::BrowseShardResponse],
) -> Result<crate::pb::BrowseShardResponse, Status> {
    use crate::sortkeys::{cmp_rows, Key};
    struct Row {
        keys: Vec<Key>,
        id: u64,
        row: Option<crate::pb::SortKeyRow>,
    }
    for sort in &req.sort {
        crate::sortkeys::target_field(&sort.column, sort.map.as_ref())?;
    }
    let sorted = !req.sort.is_empty();
    let mut geo_known: Option<Vec<bool>> = None;
    let mut filter_known: Option<Vec<bool>> = None;
    let mut sort_known: Option<Vec<bool>> = None;
    let mut sort_types: Option<Vec<i32>> = None;
    let mut segments_total = 0u32;
    let mut segments_skipped = 0u32;
    let mut rows: Vec<Row> = Vec::new();
    for (child, share) in shares.iter().enumerate() {
        crate::sortkeys::validate_browse_metadata(&req.sort, share)?;
        merge_known("geo", child, &mut geo_known, &share.geo_columns_known)?;
        merge_known(
            "filter",
            child,
            &mut filter_known,
            &share.filter_columns_known,
        )?;
        merge_known(
            "sort-column",
            child,
            &mut sort_known,
            &share.sort_columns_known,
        )?;
        merge_column_types("sort", child, &mut sort_types, &share.sort_column_types)?;
        add_count32(
            &mut segments_total,
            share.segments_total,
            child,
            "segment count",
        )?;
        add_count32(
            &mut segments_skipped,
            share.segments_skipped,
            child,
            "skipped segment count",
        )?;
        if !sorted {
            rows.extend(share.doc_ids.iter().map(|&id| Row {
                keys: Vec::new(),
                id,
                row: None,
            }));
            continue;
        }
        if share.sort_rows.len() != share.doc_ids.len() {
            return Err(Status::internal(format!(
                "relay: child {child} answered a sorted browse with {} key rows for {} ids",
                share.sort_rows.len(),
                share.doc_ids.len()
            )));
        }
        for (&id, row) in share.doc_ids.iter().zip(&share.sort_rows) {
            let keys: Option<Vec<Key>> = row.keys.iter().map(Key::from_pb).collect();
            let Some(keys) = keys else {
                return Err(Status::internal(format!(
                    "relay: child {child} answered a sorted browse with an empty key"
                )));
            };
            if keys.len() != req.sort.len() {
                return Err(Status::internal(format!(
                    "relay: child {child} answered {} keys for {} sort columns",
                    keys.len(),
                    req.sort.len()
                )));
            }
            if row.values.len() != req.sort.len() {
                return Err(Status::failed_precondition(
                    "relay: sorted row has the wrong value width",
                ));
            }
            for ((key, value), &raw) in keys.iter().zip(&row.values).zip(&share.sort_column_types) {
                let kind = crate::pb::ScalarValueType::try_from(raw).expect("validated metadata");
                if !crate::sortkeys::key_matches_type(key, kind)
                    || crate::sortkeys::value_from_pb(value).is_none_or(|v| v.column_type() != kind)
                {
                    return Err(Status::failed_precondition(
                        "relay: sort row disagrees with its declared column type",
                    ));
                }
            }
            rows.push(Row {
                keys,
                id,
                row: Some(row.clone()),
            });
        }
    }
    let descending: Vec<bool> = req.sort.iter().map(|s| s.descending).collect();
    rows.sort_by(|a, b| cmp_rows(&a.keys, a.id, &b.keys, b.id, &descending));
    rows.truncate(req.k as usize);
    Ok(crate::pb::BrowseShardResponse {
        sort_contract_version: crate::sortkeys::MAP_SORT_CONTRACT_VERSION,
        doc_ids: rows.iter().map(|r| r.id).collect(),
        geo_columns_known: geo_known.unwrap_or_default(),
        filter_columns_known: filter_known.unwrap_or_default(),
        sort_rows: if sorted {
            rows.into_iter().filter_map(|r| r.row).collect()
        } else {
            Vec::new()
        },
        sort_columns_known: sort_known.unwrap_or_default(),
        segments_total,
        segments_skipped,
        sort_column_types: sort_types.unwrap_or_default(),
        stats_epoch: 0,
        stats_incarnation: Vec::new(),
        visibility_fingerprint: Vec::new(),
        visibility_columns_known: Vec::new(),
    })
}

impl RelayService {
    pub(super) async fn handle_get_documents(
        &self,
        request: Request<crate::pb::GetDocumentsRequest>,
    ) -> Result<Response<crate::pb::GetDocumentsResponse>, Status> {
        crate::metrics::timed(Route::GetDocuments, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let (pinned, frozen, children, _) = self.open_children("GetDocuments", None, 0, &[])?;
            // Each id goes to the child whose slot range holds it; a
            // child asked for none is not asked.
            let reports = self.children_health(&frozen, timeout).await?;
            let ranges = child_ranges(&self.collection, &children, &reports)?;
            let requests = route_ids(&ranges, &req.doc_ids)?
                .into_iter()
                .map(|doc_ids| {
                    (!doc_ids.is_empty()).then_some(crate::pb::GetDocumentsRequest { doc_ids })
                })
                .collect();
            let shares = fan_out(
                &frozen,
                &children,
                requests,
                timeout,
                "get documents",
                |mut link, request| async move { link.get_documents(request).await },
            )
            .await?;
            let documents = in_caller_order(
                &req.doc_ids,
                shares
                    .into_iter()
                    .flat_map(|(_, share)| share.documents)
                    .collect(),
                |d| d.doc_id,
            );
            self.still_current(&pinned, "GetDocuments")?;
            Ok(Response::new(crate::pb::GetDocumentsResponse { documents }))
        })
        .await
    }

    pub(super) async fn handle_resolve_parents(
        &self,
        request: Request<crate::pb::ResolveParentsRequest>,
    ) -> Result<Response<crate::pb::ResolveParentsResponse>, Status> {
        crate::metrics::timed(Route::ResolveParents, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let (pinned, frozen, children, claims) = self.open_children(
                "ResolveParents",
                req.visibility.as_ref(),
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
            )?;
            // Ids routed by slot range; every child answers, an empty
            // list included, since the receipt needs every child.
            let reports = self.children_health(&frozen, timeout).await?;
            let ranges = child_ranges(&self.collection, &children, &reports)?;
            let requests = route_ids(&ranges, &req.doc_ids)?
                .into_iter()
                .enumerate()
                .map(|(shard, doc_ids)| {
                    Some(crate::pb::ResolveParentsRequest {
                        doc_ids,
                        visibility: req.visibility.clone(),
                        expected_stats_epoch: claims[shard].epoch,
                        expected_stats_incarnation: claims[shard].incarnation(),
                        fields: req.fields.clone(),
                    })
                })
                .collect();
            let shares = fan_out(
                &frozen,
                &children,
                requests,
                timeout,
                "resolve parents",
                |mut link, request| async move { link.resolve_parents(request).await },
            )
            .await?;
            let mut receipts = Vec::with_capacity(children.len());
            let mut parents = Vec::new();
            for (shard, share) in shares {
                if share.fields != req.fields {
                    return Err(Status::failed_precondition(format!(
                        "relay: child {shard} answered lineage fields {:?} for a request \
                         naming {:?}",
                        share.fields, req.fields
                    )));
                }
                receipts.push(read_metadata(&share));
                parents.extend(share.parents);
            }
            let parents = in_caller_order(&req.doc_ids, parents, |p| p.doc_id);
            let receipt = self.read_receipt(
                &pinned,
                children,
                req.visibility.as_ref(),
                Some(&claims),
                &receipts,
            )?;
            Ok(Response::new(crate::pb::ResolveParentsResponse {
                parents,
                stats_epoch: receipt.stats_epoch,
                stats_incarnation: receipt.stats_incarnation,
                visibility_fingerprint: receipt.visibility_fingerprint,
                visibility_columns_known: receipt.visibility_columns_known,
                fields: req.fields,
            }))
        })
        .await
    }

    pub(super) async fn handle_fetch_values(
        &self,
        request: Request<crate::pb::FetchValuesRequest>,
    ) -> Result<Response<crate::pb::FetchValuesResponse>, Status> {
        crate::metrics::timed(Route::FetchValues, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            if req.include_identities && req.candidate_ids.len() > 1_000_000 {
                return Err(Status::resource_exhausted(
                    "identity fetch exceeds 1000000 input IDs",
                ));
            }
            let (pinned, frozen, children, claims) = self.open_children(
                "FetchValues",
                req.visibility.as_ref(),
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
            )?;
            // Ids routed by slot range; every child answers, since the
            // known flags the root's typo rule runs on come from every
            // shard, candidates or none.
            let reports = self.children_health(&frozen, timeout).await?;
            let ranges = child_ranges(&self.collection, &children, &reports)?;
            let requests = route_ids(&ranges, &req.candidate_ids)?
                .into_iter()
                .enumerate()
                .map(|(shard, candidate_ids)| {
                    Some(crate::pb::FetchValuesRequest {
                        candidate_ids,
                        include_identities: req.include_identities,
                        projections: req.projections.clone(),
                        stages: req.stages.clone(),
                        visibility: req.visibility.clone(),
                        expected_stats_epoch: claims[shard].epoch,
                        expected_stats_incarnation: claims[shard].incarnation(),
                    })
                })
                .collect();
            let shares = fan_out(
                &frozen,
                &children,
                requests,
                timeout,
                "fetch values",
                |mut link, request| async move { link.fetch_values(request).await },
            )
            .await?;
            let mut receipts = Vec::with_capacity(children.len());
            let mut stage_known: Option<Vec<bool>> = None;
            let mut projection_known: Option<Vec<bool>> = None;
            let mut projection_types =
                vec![crate::pb::ScalarValueType::Unspecified; req.projections.len()];
            let mut rows = Vec::new();
            let mut identities = Vec::new();
            let mut identity_bytes = 0usize;
            let requested: std::collections::HashSet<_> =
                req.candidate_ids.iter().copied().collect();
            let mut identity_ids = std::collections::HashSet::new();
            for (shard, share) in shares {
                crate::values::validate_map_fetch_contract(
                    &req.projections,
                    share.typed_map_contract_version,
                )?;
                if share.identities_included != req.include_identities
                    || (!req.include_identities && !share.identities.is_empty())
                {
                    return Err(Status::failed_precondition(
                        "child omitted or changed identity fetch certificate",
                    ));
                }
                for row in &share.identities {
                    if !requested.contains(&row.doc_id)
                        || child_of_id(&ranges, row.doc_id) != Some(shard)
                        || !identity_ids.insert(row.doc_id)
                    {
                        return Err(Status::failed_precondition(
                            "child returned an unrequested or duplicate identity",
                        ));
                    }
                    crate::query_identity::charge_candidate_identity(row, &mut identity_bytes)?;
                }
                merge_known(
                    "stage-column",
                    shard,
                    &mut stage_known,
                    &share.stage_columns_known,
                )?;
                merge_known(
                    "projection-leaf",
                    shard,
                    &mut projection_known,
                    &share.projection_leaves_known,
                )?;
                crate::values::merge_projection_types(
                    &req.projections,
                    &mut projection_types,
                    &share.projection_types,
                )?;
                receipts.push(read_metadata(&share));
                rows.extend(share.rows);
                identities.extend(share.identities);
            }
            let rows = in_caller_order(&req.candidate_ids, rows, |r| r.doc_id);
            let receipt = self.read_receipt(
                &pinned,
                children,
                req.visibility.as_ref(),
                Some(&claims),
                &receipts,
            )?;
            Ok(Response::new(crate::pb::FetchValuesResponse {
                typed_map_contract_version: crate::values::TYPED_MAP_FETCH_VERSION,
                rows,
                identities,
                identities_included: req.include_identities,
                stage_columns_known: stage_known.unwrap_or_default(),
                projection_leaves_known: projection_known.unwrap_or_default(),
                projection_types: projection_types.into_iter().map(i32::from).collect(),
                visibility_fingerprint: receipt.visibility_fingerprint,
                visibility_columns_known: receipt.visibility_columns_known,
                stats_epoch: receipt.stats_epoch,
                stats_incarnation: receipt.stats_incarnation,
            }))
        })
        .await
    }

    pub(super) async fn handle_aggregate_shard(
        &self,
        request: Request<crate::pb::AggregateShardRequest>,
    ) -> Result<Response<crate::pb::AggregateShardResponse>, Status> {
        crate::metrics::timed(Route::AggregateShard, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let (pinned, frozen, children, claims) = self.open_children(
                "AggregateShard",
                req.visibility.as_ref(),
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
            )?;
            // Every child folds the request as sent; an id list names
            // rows a child may not serve, which it leaves out as a node
            // leaves out an id outside its range.
            let requests = claims
                .iter()
                .map(|claim| {
                    let mut child_req = req.clone();
                    child_req.expected_stats_epoch = claim.epoch;
                    child_req.expected_stats_incarnation = claim.incarnation();
                    Some(child_req)
                })
                .collect();
            let shares: Vec<crate::pb::AggregateShardResponse> = fan_out(
                &frozen,
                &children,
                requests,
                timeout,
                "aggregate",
                |mut link, request| async move { link.aggregate_shard(request).await },
            )
            .await?
            .into_iter()
            .map(|(_, share)| share)
            .collect();
            let mut merged = merge_aggregate_shares(
                &req.aggregations,
                &req.group_by,
                req.max_groups,
                &req.histograms,
                &req.percentiles,
                &shares,
            )?;
            let receipt = self.read_receipt(
                &pinned,
                children,
                req.visibility.as_ref(),
                Some(&claims),
                &shares,
            )?;
            merged.stats_epoch = receipt.stats_epoch;
            merged.stats_incarnation = receipt.stats_incarnation;
            merged.visibility_fingerprint = receipt.visibility_fingerprint;
            merged.visibility_columns_known = receipt.visibility_columns_known;
            Ok(Response::new(merged))
        })
        .await
    }

    pub(super) async fn handle_quantile_counts(
        &self,
        request: Request<crate::pb::QuantileCountsRequest>,
    ) -> Result<Response<crate::pb::QuantileCountsResponse>, Status> {
        crate::metrics::timed(Route::QuantileCounts, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let (pinned, frozen, children, claims) = self.open_children(
                "QuantileCounts",
                req.visibility.as_ref(),
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
            )?;
            // A Boolean round carries the plan's own claim, translated
            // per child as the plan route translates it.
            let plan_claims = match req.boolean.as_ref() {
                Some(plan) => Some(self.child_claims(
                    plan.expected_stats_epoch,
                    &plan.expected_stats_incarnation,
                    children.len(),
                )?),
                None => None,
            };
            let requests = claims
                .iter()
                .enumerate()
                .map(|(shard, claim)| {
                    let mut child_req = req.clone();
                    child_req.expected_stats_epoch = claim.epoch;
                    child_req.expected_stats_incarnation = claim.incarnation();
                    if let (Some(plan), Some(plan_claims)) =
                        (child_req.boolean.as_mut(), plan_claims.as_ref())
                    {
                        plan.expected_stats_epoch = plan_claims[shard].epoch;
                        plan.expected_stats_incarnation = plan_claims[shard].incarnation();
                    }
                    Some(child_req)
                })
                .collect();
            let shares: Vec<crate::pb::QuantileCountsResponse> = fan_out(
                &frozen,
                &children,
                requests,
                timeout,
                "quantile counts",
                |mut link, request| async move { link.quantile_counts(request).await },
            )
            .await?
            .into_iter()
            .map(|(_, share)| share)
            .collect();
            let mut counts = vec![0u64; req.targets.len()];
            for (shard, share) in shares.iter().enumerate() {
                if share.counts.len() != counts.len() {
                    return Err(Status::internal(format!(
                        "relay: child {shard} answered {} quantile counts for {} targets",
                        share.counts.len(),
                        counts.len()
                    )));
                }
                for (total, &count) in counts.iter_mut().zip(&share.counts) {
                    add_count(total, count, shard, "quantile count")?;
                }
            }
            let receipt = self.read_receipt(
                &pinned,
                children,
                req.visibility.as_ref(),
                Some(&claims),
                &shares,
            )?;
            Ok(Response::new(crate::pb::QuantileCountsResponse {
                counts,
                stats_epoch: receipt.stats_epoch,
                stats_incarnation: receipt.stats_incarnation,
                visibility_fingerprint: receipt.visibility_fingerprint,
                visibility_columns_known: receipt.visibility_columns_known,
            }))
        })
        .await
    }

    pub(super) async fn handle_browse_shard(
        &self,
        request: Request<crate::pb::BrowseShardRequest>,
    ) -> Result<Response<crate::pb::BrowseShardResponse>, Status> {
        crate::metrics::timed(Route::BrowseShard, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            if req.k == 0 {
                return Err(Status::invalid_argument("browse requires k > 0"));
            }
            for sort in &req.sort {
                crate::sortkeys::target_field(&sort.column, sort.map.as_ref())?;
            }
            let (pinned, frozen, children, claims) = self.open_children(
                "BrowseShard",
                req.visibility.as_ref(),
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
            )?;
            // The boundary is a row's keys and id, which each child
            // applies to its rows as sent.
            let requests = claims
                .iter()
                .map(|claim| {
                    let mut child_req = req.clone();
                    child_req.expected_stats_epoch = claim.epoch;
                    child_req.expected_stats_incarnation = claim.incarnation();
                    Some(child_req)
                })
                .collect();
            let shares: Vec<crate::pb::BrowseShardResponse> = fan_out(
                &frozen,
                &children,
                requests,
                timeout,
                "browse",
                |mut link, request| async move { link.browse_shard(request).await },
            )
            .await?
            .into_iter()
            .map(|(_, share)| share)
            .collect();
            let mut merged = merge_browse_pages(&req, &shares)?;
            let receipt = self.read_receipt(
                &pinned,
                children,
                req.visibility.as_ref(),
                Some(&claims),
                &shares,
            )?;
            merged.stats_epoch = receipt.stats_epoch;
            merged.stats_incarnation = receipt.stats_incarnation;
            merged.visibility_fingerprint = receipt.visibility_fingerprint;
            merged.visibility_columns_known = receipt.visibility_columns_known;
            Ok(Response::new(merged))
        })
        .await
    }
}
