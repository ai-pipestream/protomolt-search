use super::map::{
    add_count, add_count32, child_error, child_of_id, child_ranges, fan_out, in_caller_order,
    merge_column_types, route_ids,
};
use super::merge_lexical::merge_known;
use super::merge_vector::KnownFlags;
use super::*;
pub(super) fn health_span(report: &HealthResponse) -> u64 {
    report.num_vectors.max(report.bm25_docs)
}
pub fn merge_health(
    collection: &str,
    children: &[String],
    reports: &[HealthResponse],
) -> Result<HealthResponse, Status> {
    if reports.is_empty() {
        return Err(Status::failed_precondition(
            "relay: a relay coordinator has no children",
        ));
    }
    let mut order: Vec<usize> = (0..reports.len()).collect();
    order.sort_by_key(|&i| reports[i].slot_offset);
    for pair in order.windows(2) {
        let (a, b) = (pair[0], pair[1]);
        let end = reports[a].slot_offset + health_span(&reports[a]);
        if end != reports[b].slot_offset {
            let relation = if end > reports[b].slot_offset {
                "overlaps"
            } else {
                "leaves a gap before"
            };
            return Err(Status::failed_precondition(format!(
                "relay: child {a} ({}) covers slots {}..{end} and {relation} child {b} ({}) at \
                 slot {}; a relay serves contiguous children only, so the range the parent \
                 derives from one health report is real",
                children.get(a).map_or("", String::as_str),
                reports[a].slot_offset,
                children.get(b).map_or("", String::as_str),
                reports[b].slot_offset
            )));
        }
    }
    let first = &reports[order[0]];
    let last = &reports[*order.last().expect("non-empty")];
    let mut merged = HealthResponse {
        collection: collection.to_string(),
        slot_offset: first.slot_offset,
        dim: first.dim,
        bits_per_dimension: first.bits_per_dimension,
        vector_backend: first.vector_backend.clone(),
        scoring_fingerprint: first.scoring_fingerprint.clone(),
        quality_contract: first.quality_contract.clone(),
        exact_vectors_available: true,
        exact_vectors_mmap: true,
        ..Default::default()
    };
    for (shard, report) in reports.iter().enumerate() {
        for (name, mine, theirs) in [
            (
                "vector_backend",
                &merged.vector_backend,
                &report.vector_backend,
            ),
            (
                "scoring_fingerprint",
                &merged.scoring_fingerprint,
                &report.scoring_fingerprint,
            ),
            (
                "quality_contract",
                &merged.quality_contract,
                &report.quality_contract,
            ),
        ] {
            if mine != theirs {
                return Err(Status::failed_precondition(format!(
                    "relay: child {shard} reports {name} {theirs:?} while child {} reports \
                     {mine:?}; one relay serves one score space",
                    order[0]
                )));
            }
        }
        if report.dim != 0 && merged.dim != 0 && report.dim != merged.dim {
            return Err(Status::failed_precondition(format!(
                "relay: child {shard} has dimension {} while child {} has {}",
                report.dim, order[0], merged.dim
            )));
        }
        if merged.dim == 0 {
            merged.dim = report.dim;
            merged.bits_per_dimension = report.bits_per_dimension;
        }
        merged.num_vectors += report.num_vectors;
        merged.bm25_docs += report.bm25_docs;
        merged.bm25_building |= report.bm25_building;
        merged.ingest_active |= report.ingest_active;
        merged.exact_vectors_available &= report.exact_vectors_available;
        merged.exact_vectors_mmap &= report.exact_vectors_mmap;
        merged.exact_vector_rows += report.exact_vector_rows;
        merged.live_docs += report.live_docs;
        merged.deleted_docs += report.deleted_docs;
    }
    merged.document_slots = (last.slot_offset - first.slot_offset) + last.document_slots;
    merged.live_revision = 0;
    merged.wal_generation = 0;
    merged.wal_high_watermark = 0;
    merged.wal_clocked = false;
    Ok(merged)
}
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
    pub(super) async fn health(
        &self,
        request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let timeout = grpc_timeout(request.metadata());
        let (pinned, frozen) = self.pin();
        let reports = self.children_health(&frozen, timeout).await?;
        let merged = merge_health(&self.collection, frozen.node_addresses(), &reports)?;
        self.still_current(&pinned, "Health")?;
        Ok(Response::new(merged))
    }
    pub(super) async fn get_documents(
        &self,
        request: Request<crate::pb::GetDocumentsRequest>,
    ) -> Result<Response<crate::pb::GetDocumentsResponse>, Status> {
        crate::metrics::timed(Route::GetDocuments, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let (pinned, frozen, children, _) = self.open_children("GetDocuments", None, 0, &[])?;
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
    pub(super) async fn resolve_parents(
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
    pub(super) async fn fetch_values(
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
    pub(super) async fn aggregate_shard(
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
    pub(super) async fn quantile_counts(
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
    pub(super) async fn resolve_filter_bitmap(
        &self,
        request: Request<crate::pb::FilterBitmapRequest>,
    ) -> Result<Response<crate::pb::FilterBitmapResponse>, Status> {
        crate::metrics::timed(Route::ResolveFilterBitmap, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            crate::node::validate_geo_filters(&req.geo_filters)?;
            if let Some(f) = req.filter.as_ref() {
                crate::filter::validate_filter(f)?;
            }
            let filters = RequestFilters {
                geo: req.geo_filters.clone(),
                tree: req.filter.clone(),
            };
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let reports = self.children_health(&frozen, timeout).await?;
            let ranges = child_ranges(&self.collection, &children, &reports)?;
            let mask = req
                .visibility
                .is_none()
                .then(|| frozen.shard_mask(filters.tree.as_ref()))
                .flatten();
            let mut known = KnownFlags::new(&filters, mask.as_ref());
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, addr) in children.iter().enumerate() {
                if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                    tasks.push(None);
                    continue;
                }
                let mut link = frozen.node_client(addr)?;
                let child_req = crate::pb::FilterBitmapRequest {
                    visibility: req.visibility.clone(),
                    geo_filters: filters.geo.clone(),
                    filter: CoordinatorServiceImpl::shard_filter_tree(
                        &filters,
                        mask.as_ref(),
                        shard,
                    ),
                };
                let addr = addr.clone();
                tasks.push(Some(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.resolve_filter_bitmap(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "filter bitmap", status))
                })));
            }
            let mut shares = Vec::with_capacity(children.len());
            let mut receipts = Vec::with_capacity(children.len());
            let mut segments_total: u32 = 0;
            let mut segments_skipped: u32 = 0;
            for (shard, task) in tasks.into_iter().enumerate() {
                let Some(task) = task else {
                    let mut request = Request::new(TermStatsRequest {
                        version_only: true,
                        ..Default::default()
                    });
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    let share = frozen
                        .node_client(&children[shard])?
                        .term_stats(request)
                        .await?
                        .into_inner();
                    crate::visibility::validate_stats_mode(true, &share)?;
                    receipts.push(crate::pb::VectorReadReceipt {
                        stats_epoch: share.stats_epoch,
                        stats_incarnation: share.stats_incarnation,
                        visibility_fingerprint: share.visibility_fingerprint,
                        visibility_columns_known: share.visibility_columns_known,
                        ..Default::default()
                    });
                    shares.push(None);
                    continue;
                };
                let share = task
                    .await
                    .map_err(|e| Status::internal(format!("relay filter bitmap task: {e}")))??;
                receipts.push(read_metadata(&share));
                known.merge(
                    shard,
                    mask.as_ref(),
                    &share.geo_columns_known,
                    &share.filter_columns_known,
                )?;
                add_count32(
                    &mut segments_total,
                    share.segments_total,
                    shard,
                    "segment count",
                )?;
                add_count32(
                    &mut segments_skipped,
                    share.segments_skipped,
                    shard,
                    "skipped segment count",
                )?;
                shares.push(Some(ChildBitmap {
                    base_label: share.base_label,
                    label_count: share.label_count,
                    bits: share.bits,
                }));
            }
            let (base_label, label_count, bits) = concat_bitmaps(&children, &ranges, &shares)?;
            self.still_current(&pinned, "ResolveFilterBitmap")?;
            let receipt =
                self.read_receipt(&pinned, children, req.visibility.as_ref(), None, &receipts)?;
            Ok(Response::new(crate::pb::FilterBitmapResponse {
                stats_epoch: receipt.stats_epoch,
                stats_incarnation: receipt.stats_incarnation,
                visibility_fingerprint: receipt.visibility_fingerprint,
                visibility_columns_known: receipt.visibility_columns_known,
                base_label,
                label_count,
                bits,
                geo_columns_known: known.geo,
                filter_columns_known: known.tree,
                segments_total,
                segments_skipped,
            }))
        })
        .await
    }
    pub(super) async fn vector_rescore(
        &self,
        request: Request<crate::pb::VectorRescoreRequest>,
    ) -> Result<Response<crate::pb::VectorRescoreResponse>, Status> {
        crate::metrics::timed(Route::VectorRescore, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let claims = self.child_claims(
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
                children.len(),
            )?;
            let reports = self.children_health(&frozen, timeout).await?;
            let ranges = child_ranges(&self.collection, &children, &reports)?;
            let by_child = route_ids(&ranges, &req.candidate_ids)?;
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, ids) in by_child.into_iter().enumerate() {
                let mut link = frozen.node_client(&children[shard])?;
                let child_req = crate::pb::VectorRescoreRequest {
                    vector: req.vector.clone(),
                    candidate_ids: ids,
                    field: req.field.clone(),
                    visibility: req.visibility.clone(),
                    expected_stats_epoch: claims[shard].epoch,
                    expected_stats_incarnation: claims[shard].incarnation(),
                };
                let addr = children[shard].clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.vector_rescore(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "vector rescore", status))
                }));
            }
            let mut receipts = Vec::with_capacity(children.len());
            let mut binding = None;
            let mut hits = Vec::new();
            for task in tasks {
                let share = task
                    .await
                    .map_err(|e| Status::internal(format!("relay vector rescore task: {e}")))??;
                crate::vector_read::check_binding(
                    &req.field,
                    share.vector_binding.as_ref(),
                    &mut binding,
                )?;
                receipts.push(read_metadata(&share));
                hits.extend(share.hits);
            }
            hits.sort_by(|a, b| {
                b.score
                    .total_cmp(&a.score)
                    .then_with(|| a.doc_id.cmp(&b.doc_id))
            });
            self.still_current(&pinned, "VectorRescore")?;
            let receipt = self.read_receipt(
                &pinned,
                children,
                req.visibility.as_ref(),
                Some(&claims),
                &receipts,
            )?;
            Ok(Response::new(crate::pb::VectorRescoreResponse {
                hits,
                vector_binding: binding.flatten(),
                stats_epoch: receipt.stats_epoch,
                stats_incarnation: receipt.stats_incarnation,
                visibility_fingerprint: receipt.visibility_fingerprint,
                visibility_columns_known: receipt.visibility_columns_known,
            }))
        })
        .await
    }
    pub(super) async fn exact_vector_rescore(
        &self,
        request: Request<crate::pb::ExactVectorRescoreRequest>,
    ) -> Result<Response<crate::pb::ExactVectorRescoreResponse>, Status> {
        crate::metrics::timed(Route::ExactVectorRescore, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let claims = self.child_claims(
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
                children.len(),
            )?;
            let reports = self.children_health(&frozen, timeout).await?;
            let ranges = child_ranges(&self.collection, &children, &reports)?;
            let mut requested = req.candidate_ids.clone();
            let mut seen = std::collections::HashSet::with_capacity(requested.len());
            requested.retain(|id| seen.insert(*id));
            let by_child = route_ids(&ranges, &requested)?;
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, ids) in by_child.into_iter().enumerate() {
                let mut link = frozen.node_client(&children[shard])?;
                let child_req = crate::pb::ExactVectorRescoreRequest {
                    vector: req.vector.clone(),
                    candidate_ids: ids,
                    field: req.field.clone(),
                    visibility: req.visibility.clone(),
                    expected_stats_epoch: claims[shard].epoch,
                    expected_stats_incarnation: claims[shard].incarnation(),
                    max_logical_bytes: req.max_logical_bytes,
                };
                let addr = children[shard].clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.exact_vector_rescore(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "exact vector rescore", status))
                }));
            }
            let mut receipts = Vec::with_capacity(children.len());
            let mut binding = None;
            let mut merged = crate::pb::ExactVectorRescoreResponse::default();
            let mut by_id = HashMap::new();
            for (i, task) in tasks.into_iter().enumerate() {
                let share = task.await.map_err(|e| {
                    Status::internal(format!("relay exact vector rescore task: {e}"))
                })??;
                crate::vector_read::check_binding(
                    &req.field,
                    share.vector_binding.as_ref(),
                    &mut binding,
                )?;
                receipts.push(read_metadata(&share));
                add_count(
                    &mut merged.logical_bytes,
                    share.logical_bytes,
                    i,
                    "logical bytes",
                )?;
                add_count(
                    &mut merged.pages_touched,
                    share.pages_touched,
                    i,
                    "page count",
                )?;
                merged.tasks = merged.tasks.saturating_add(share.tasks);
                for hit in share.hits {
                    if by_id.insert(hit.doc_id, hit).is_some() {
                        return Err(Status::failed_precondition(
                            "relay: exact vector rescore candidate answered by more than one \
                             child; slot ranges overlap",
                        ));
                    }
                }
            }
            merged.hits = requested.iter().filter_map(|id| by_id.remove(id)).collect();
            self.still_current(&pinned, "ExactVectorRescore")?;
            let receipt = self.read_receipt(
                &pinned,
                children,
                req.visibility.as_ref(),
                Some(&claims),
                &receipts,
            )?;
            merged.vector_binding = binding.flatten();
            merged.stats_epoch = receipt.stats_epoch;
            merged.stats_incarnation = receipt.stats_incarnation;
            merged.visibility_fingerprint = receipt.visibility_fingerprint;
            merged.visibility_columns_known = receipt.visibility_columns_known;
            Ok(Response::new(merged))
        })
        .await
    }
    pub(super) async fn resolve_vector_bitmap(
        &self,
        request: Request<crate::pb::VectorBitmapRequest>,
    ) -> Result<Response<crate::pb::MembershipBitmapResponse>, Status> {
        let timeout = grpc_timeout(request.metadata());
        let req = request.into_inner();
        crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
        let (pinned, frozen) = self.pin();
        let children = frozen.node_addresses().to_vec();
        if children.is_empty() {
            return Err(Status::failed_precondition(
                "relay: a relay coordinator has no children",
            ));
        }
        let reports = self.children_health(&frozen, timeout).await?;
        let ranges = child_ranges(&self.collection, &children, &reports)?;
        let mut tasks = Vec::with_capacity(children.len());
        for (shard, addr) in children.iter().enumerate() {
            let mut link = frozen.node_client(addr)?;
            let addr = addr.clone();
            let req = req.clone();
            tasks.push(tokio::spawn(async move {
                let mut request = Request::new(req);
                if let Some(timeout) = timeout {
                    request.set_timeout(timeout);
                }
                link.resolve_vector_bitmap(request)
                    .await
                    .map(|r| r.into_inner())
                    .map_err(|status| child_error(shard, &addr, "vector bitmap", status))
            }));
        }
        let mut shares = Vec::with_capacity(children.len());
        let mut receipts = Vec::with_capacity(children.len());
        let mut binding = None;
        for task in tasks {
            let share = task
                .await
                .map_err(|e| Status::internal(format!("relay vector bitmap task: {e}")))??;
            crate::vector_read::check_binding(
                &req.field,
                share.vector_binding.as_ref(),
                &mut binding,
            )?;
            receipts.push(read_metadata(&share));
            shares.push(Some(ChildBitmap {
                base_label: share.base_label,
                label_count: share.label_count,
                bits: share.bits,
            }));
        }
        let (base_label, label_count, bits) = concat_bitmaps(&children, &ranges, &shares)?;
        self.still_current(&pinned, "ResolveVectorBitmap")?;
        let receipt =
            self.read_receipt(&pinned, children, req.visibility.as_ref(), None, &receipts)?;
        Ok(Response::new(crate::pb::MembershipBitmapResponse {
            vector_binding: binding.flatten(),
            stats_incarnation: receipt.stats_incarnation,
            visibility_fingerprint: receipt.visibility_fingerprint,
            visibility_columns_known: receipt.visibility_columns_known,
            base_label,
            label_count,
            bits,
            stats_epoch: receipt.stats_epoch,
            segments_total: 0,
            segments_skipped: 0,
        }))
    }
}
