//! Shard browse, value projection, document fetch, and parent resolution RPCs.

use crate::node::*;

impl NodeServiceImpl {
    /// Filter-only browse: admitted ids above the floor, ascending, at
    /// most k, or ordered by a column under `sort`. Match-all over the DOCUMENT space (bm25 postings), gated
    /// by the same resolved filter every scored route admits through;
    /// no scoring, no heap — the order is the id order.
    pub(super) async fn handle_browse_shard(
        &self,
        request: Request<crate::pb::BrowseShardRequest>,
    ) -> Result<Response<crate::pb::BrowseShardResponse>, Status> {
        crate::metrics::timed(Route::BrowseShard, request, |request| async move {
            let req = request.into_inner();
            for sort in &req.sort {
                crate::sortkeys::target_field(&sort.column, sort.map.as_ref())?;
            }
            let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let filter =
                crate::visibility::intersect_filter(req.visibility.as_ref(), req.filter.clone())?;
            if req.k == 0 {
                return Err(Status::invalid_argument("browse requires k > 0"));
            }
            let geo_regions = validate_geo_filters(&req.geo_filters)?;
            let guard = read_shard(&self.state);
            guard.check_stats_epoch(req.expected_stats_epoch, &req.expected_stats_incarnation)?;
            let (_, visibility_columns_known) = filter_known_flags(
                guard.bm25.as_ref(),
                &[],
                req.visibility.as_ref().and_then(|view| view.filter.as_ref()),
            );
            if !req.lexical_terms.is_empty() { guard.check_query_analysis("body", req.analysis_fingerprint)?; }
            let (geo_columns_known, filter_columns_known) =
                filter_known_flags(guard.bm25.as_ref(), &req.geo_filters, req.filter.as_ref());
            let slot_offset = self.config.slot_offset;
            let Some(store) = guard.bm25.as_ref() else {
                // A document-less shard admits nothing; its all-false known
                // flags feed the coordinator's typo rule like everywhere.
                return Ok(Response::new(crate::pb::BrowseShardResponse {
sort_contract_version: crate::sortkeys::MAP_SORT_CONTRACT_VERSION,
                    stats_epoch: guard.stats_epoch,
                    stats_incarnation: guard.stats_incarnation.bytes()?,
                    visibility_fingerprint: scope.fingerprint().to_vec(),
                    visibility_columns_known: visibility_columns_known.clone(),
                    segments_total: 0,
                    segments_skipped: 0,
                    doc_ids: Vec::new(),
                    geo_columns_known,
                    filter_columns_known,
                    sort_rows: Vec::new(),
                    sort_columns_known: vec![false; req.sort.len()],
                    sort_column_types: req.sort.iter().map(|sort| {
                        use crate::pb::ScalarValueType as Type;
                        let Some(map) = &sort.map else { return Type::Unspecified as i32; };
                        let name = &map.column;
                        (if self.config.map_integer_fields.contains(name) { Type::Integer }
                        else if self.config.map_unsigned_integer_fields.contains(name) { Type::UnsignedInteger }
                        else if self.config.map_numeric_fields.contains(name) { Type::Number }
                        else if self.config.map_facet_fields.contains(name) { Type::Text }
                        else { Type::Unspecified }) as i32
                    }).collect(),
                }));
            };
            if store.as_index().is_none() {
                return Err(Status::failed_precondition(
                    "bm25 bulk build in progress; Flush before browsing",
                ));
            }
            let n = u64::from(store.next_doc_id());
            // The lexical membership predicate (the terms of one lexical
            // leaf, OR): the same bitmap ResolveLexicalBitmap answers,
            // built here so the walk reads it in place.
            let lexical: Option<Vec<u8>> = if req.lexical_terms.is_empty() {
                None
            } else {
                let index = store.as_index().ok_or_else(|| {
                    Status::failed_precondition("bm25 bulk build in progress; Flush first")
                })?;
                let count = n as usize;
                let mut bits = vec![0u8; count.div_ceil(8)];
                for term in &req.lexical_terms {
                    index.for_each_doc_tf(term, &mut |doc_id, _tf| {
                        let position = doc_id as usize;
                        if position < count {
                            bits[position / 8] |= 1 << (position % 8);
                        }
                    });
                }
                Some(bits)
            };
            let in_lexical = |local: u32| -> bool {
                match &lexical {
                    None => true,
                    Some(bits) => {
                        let p = local as usize;
                        bits.get(p / 8).is_some_and(|b| b & (1 << (p % 8)) != 0)
                    }
                }
            };
            // The exclusive floor in local id space. The first page carries
            // no floor at all (proto3 cannot distinguish after = 0 from
            // unset, so the request says which it is).
            let start = if req.first_page || req.after < slot_offset {
                0
            } else {
                (req.after - slot_offset + 1).min(n)
            };
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
            let prune = prune_segments(
                store,
                doc_filter.pred.as_ref(),
                self.knobs.segment_pruning(),
            );
            if !req.sort.is_empty() {
                // Column-ordered browse: walk the FULL admitted set with a
                // k-bounded heap. Exhaustive by construction, so the
                // exactness certificate is trivial; per-shard exact top-k
                // by key means the coordinator's merged union contains the
                // global top-k (local rank <= global rank).
                use crate::sortkeys::{cmp_candidate, Key, KeyRef, Value};
                enum Column {
                    Numeric(usize),
                    Integer(usize),
                    UnsignedInteger(usize),
                    Facet(usize),
                    MapNumeric(usize, Option<u32>),
                    MapInteger(usize, Option<u32>),
                    MapUnsignedInteger(usize, Option<u32>),
                    MapFacet(usize, Option<u32>),
                    Parent,
                    Group,
                }
                let mut columns = Vec::with_capacity(req.sort.len());
                let mut known = Vec::with_capacity(req.sort.len());
                let mut column_types = Vec::with_capacity(req.sort.len());
                for sort in &req.sort {
                    let families = [store.numeric_index(&sort.column), store.integer_index(&sort.column), store.unsigned_integer_index(&sort.column), store.facet_index(&sort.column)];
                    if families.iter().filter(|c| c.is_some()).count() > 1 {
                        return Err(Status::invalid_argument(format!("sort column {:?} exists in more than one family on this shard", sort.column)));
                    }
                    let column = if let Some(map) = &sort.map {
                        let families = [store.map_numeric_index(&map.column), store.map_integer_index(&map.column), store.map_unsigned_integer_index(&map.column), store.map_facet_index(&map.column)];
                        if families.iter().filter(|c| c.is_some()).count() > 1 {
                            return Err(Status::invalid_argument(format!("sort map column {:?} exists in more than one family on this shard", map.column)));
                        }
                        if let Some(ci) = families[0] { Some(Column::MapNumeric(ci, store.map_numeric_key_ord(ci, &map.key))) }
                        else if let Some(ci) = families[1] { Some(Column::MapInteger(ci, store.map_integer_key_ord(ci, &map.key))) }
                        else if let Some(ci) = families[2] { Some(Column::MapUnsignedInteger(ci, store.map_unsigned_integer_key_ord(ci, &map.key))) }
                        else { families[3].map(|ci| Column::MapFacet(ci, store.map_facet_key_ord(ci, &map.key))) }
                    } else if let Some(ni) = store.numeric_index(&sort.column) {
                        Some(Column::Numeric(ni))
                    } else if let Some(ii) = store.integer_index(&sort.column) {
                        Some(Column::Integer(ii))
                    } else if let Some(ui) = store.unsigned_integer_index(&sort.column) {
                        Some(Column::UnsignedInteger(ui))
                    } else if let Some(fi) = store.facet_index(&sort.column) {
                        Some(Column::Facet(fi))
                    } else if sort.column == "parent_id" {
                        Some(Column::Parent)
                    } else if sort.column == "group_id" {
                        Some(Column::Group)
                    } else {
                        None
                    };
                    column_types.push(match column {
                        Some(Column::Integer(_) | Column::MapInteger(..)) => crate::pb::ScalarValueType::Integer,
                        Some(Column::Numeric(_) | Column::MapNumeric(..)) => crate::pb::ScalarValueType::Number,
                        Some(Column::UnsignedInteger(_) | Column::MapUnsignedInteger(..) | Column::Parent | Column::Group) => crate::pb::ScalarValueType::UnsignedInteger,
                        Some(Column::Facet(_) | Column::MapFacet(..)) => crate::pb::ScalarValueType::Text,
                        None => crate::pb::ScalarValueType::Unspecified,
                    } as i32);
                    known.push(match column {
                        Some(Column::MapNumeric(_, key) | Column::MapInteger(_, key) | Column::MapUnsignedInteger(_, key) | Column::MapFacet(_, key)) => key.is_some(),
                        _ => column.is_some(),
                    });
                    columns.push(column);
                }
                if columns.iter().any(Option::is_none) {
                    // Unknown here; the coordinator's typo rule refuses only
                    // when NO shard knows a column. A shard that lacks any
                    // key holds no value for it on any document, so it
                    // contributes no rows.
                    return Ok(Response::new(crate::pb::BrowseShardResponse {
sort_contract_version: crate::sortkeys::MAP_SORT_CONTRACT_VERSION,
                        stats_epoch: guard.stats_epoch,
                        stats_incarnation: guard.stats_incarnation.bytes()?,
                        visibility_fingerprint: scope.fingerprint().to_vec(),
                        visibility_columns_known: visibility_columns_known.clone(),
                        segments_total: prune.stats.segments_total,
                        segments_skipped: prune.stats.segments_skipped,
                        doc_ids: Vec::new(),
                        geo_columns_known,
                        filter_columns_known,
                        sort_rows: Vec::new(),
                        sort_columns_known: known,
                        sort_column_types: column_types,
                    }));
                }
                let columns: Vec<Column> = columns.into_iter().flatten().collect();
                let descending: Vec<bool> = req.sort.iter().map(|s| s.descending).collect();
                let index = store.as_index();
                // A candidate's keys, borrowed where the column lets them
                // be, and its reported values; None when any key is absent.
                let keys_of = |doc: u32| -> Option<(Vec<KeyRef<'_>>, Vec<Value>)> {
                    let mut keys = Vec::with_capacity(columns.len());
                    let mut values = Vec::with_capacity(columns.len());
                    for (column, desc) in columns.iter().zip(&descending) {
                        let adjust = |bits: u64| if *desc { !bits } else { bits };
                        match column {
                            Column::Numeric(ni) => {
                                let v = store.numeric_value(*ni, doc)?;
                                keys.push(KeyRef::Bits(adjust(f64_order_bits(v))));
                                values.push(Value::Number(v));
                            }
                            Column::Integer(ii) => {
                                let v = store.integer_value(*ii, doc)?;
                                keys.push(KeyRef::Bits(adjust(i64_order_bits(v))));
                                values.push(Value::Integer(v));
                            }
                            Column::UnsignedInteger(ui) => {
                                let v = store.unsigned_integer_value(*ui, doc)?;
                                keys.push(KeyRef::UnsignedBits(adjust(v)));
                                values.push(Value::UnsignedInteger(v));
                            }
                            Column::Facet(fi) => {
                                let ord = store.facet_ord(*fi, doc)?;
                                let text = store.facet_value(*fi, ord);
                                keys.push(KeyRef::Text(text));
                                values.push(Value::Text(text.to_string()));
                            }
                            Column::MapNumeric(ci, key) => {
                                let v = store.map_numeric_value(*ci, (*key)?, doc)?;
                                keys.push(KeyRef::Bits(adjust(f64_order_bits(v))));
                                values.push(Value::Number(v));
                            }
                            Column::MapInteger(ci, key) => {
                                let v = store.map_integer_value(*ci, (*key)?, doc)?;
                                keys.push(KeyRef::Bits(adjust(i64_order_bits(v))));
                                values.push(Value::Integer(v));
                            }
                            Column::MapUnsignedInteger(ci, key) => {
                                let v = store.map_unsigned_integer_value(*ci, (*key)?, doc)?;
                                keys.push(KeyRef::UnsignedBits(adjust(v)));
                                values.push(Value::UnsignedInteger(v));
                            }
                            Column::MapFacet(ci, key) => {
                                let ord = store.map_facet_value_ord(*ci, (*key)?, doc)?;
                                let text = store.map_facet_value(*ci, ord);
                                keys.push(KeyRef::Text(text));
                                values.push(Value::Text(text.to_string()));
                            }
                            Column::Parent | Column::Group => {
                                let lineage = index.and_then(|index| index.lineage(doc))?;
                                let v = if matches!(column, Column::Parent) {
                                    lineage.parent_id
                                } else {
                                    lineage.group_id
                                };
                                keys.push(KeyRef::UnsignedBits(adjust(v)));
                                values.push(Value::UnsignedInteger(v));
                            }
                        }
                    }
                    Some((keys, values))
                };
                let boundary: Option<Vec<Key>> = if req.first_page {
                    None
                } else {
                    let keys: Option<Vec<Key>> = req.after_keys.iter().map(Key::from_pb).collect();
                    let keys = keys.ok_or_else(|| {
                        Status::invalid_argument("sorted browse boundary carries an empty key")
                    })?;
                    if keys.len() != req.sort.len() {
                        return Err(Status::invalid_argument(format!(
                            "sorted browse boundary has {} keys for {} sort columns",
                            keys.len(),
                            req.sort.len()
                        )));
                    }
                    for (key, kind) in keys.iter().zip(&column_types) {
                        let kind = crate::pb::ScalarValueType::try_from(*kind).expect("resolved column type");
                        if !crate::sortkeys::key_matches_type(key, kind) {
                            return Err(Status::invalid_argument("sorted browse boundary key type disagrees with the sort column; restart from the first page"));
                        }
                    }
                    Some(keys)
                };
                // The k best rows so far, worst last (a small k: the heap's
                // work is the comparison against the worst kept row, and
                // an insertion is a shift of at most k entries).
                struct Row {
                    keys: Vec<Key>,
                    values: Vec<Value>,
                    id: u64,
                }
                let k = req.k as usize;
                let mut kept: Vec<Row> = Vec::with_capacity(k + 1);
                for doc in prune.admitted_slots(n as u32) {
                    if !in_lexical(doc) || !doc_filter.passes(doc, &cols) {
                        continue;
                    }
                    // A document without a value has no honest position in
                    // a column order: excluded, same stance as the filters.
                    let Some((keys, values)) = keys_of(doc) else {
                        continue;
                    };
                    let id = slot_offset + u64::from(doc);
                    if let Some(b) = &boundary {
                        if cmp_candidate(&keys, id, b, req.after, &descending)
                            != std::cmp::Ordering::Greater
                        {
                            continue;
                        }
                    }
                    if kept.len() == k {
                        let worst = &kept[k - 1];
                        if cmp_candidate(&keys, id, &worst.keys, worst.id, &descending)
                            != std::cmp::Ordering::Less
                        {
                            continue;
                        }
                    }
                    let row = Row {
                        keys: keys.into_iter().map(KeyRef::to_owned).collect(),
                        values,
                        id,
                    };
                    let at = kept.partition_point(|r| {
                        crate::sortkeys::cmp_rows(&r.keys, r.id, &row.keys, row.id, &descending)
                            == std::cmp::Ordering::Less
                    });
                    kept.insert(at, row);
                    if kept.len() > k {
                        kept.pop();
                    }
                }
                let mut doc_ids = Vec::with_capacity(kept.len());
                let mut sort_rows = Vec::with_capacity(kept.len());
                for row in kept {
                    doc_ids.push(row.id);
                    sort_rows.push(crate::pb::SortKeyRow {
                        keys: row.keys.iter().map(Key::to_pb).collect(),
                        values: row.values.iter().map(Value::to_pb).collect(),
                    });
                }
                return Ok(Response::new(crate::pb::BrowseShardResponse {
sort_contract_version: crate::sortkeys::MAP_SORT_CONTRACT_VERSION,
                    stats_epoch: guard.stats_epoch,
                    stats_incarnation: guard.stats_incarnation.bytes()?,
                    visibility_fingerprint: scope.fingerprint().to_vec(),
                    visibility_columns_known: visibility_columns_known.clone(),
                    segments_total: prune.stats.segments_total,
                    segments_skipped: prune.stats.segments_skipped,
                    doc_ids,
                    geo_columns_known,
                    filter_columns_known,
                    sort_rows,
                    sort_columns_known: known,
                    sort_column_types: column_types,
                }));
            }
            let mut doc_ids = Vec::new();
            for local in prune.admitted_slots_from(start as u32, n as u32) {
                if in_lexical(local) && doc_filter.passes(local, &cols) {
                    doc_ids.push(slot_offset + u64::from(local));
                    if doc_ids.len() == req.k as usize {
                        break;
                    }
                }
            }
            Ok(Response::new(crate::pb::BrowseShardResponse {
sort_contract_version: crate::sortkeys::MAP_SORT_CONTRACT_VERSION,
                stats_epoch: guard.stats_epoch,
                stats_incarnation: guard.stats_incarnation.bytes()?,
                visibility_fingerprint: scope.fingerprint().to_vec(),
                visibility_columns_known: visibility_columns_known.clone(),
                segments_total: prune.stats.segments_total,
                segments_skipped: prune.stats.segments_skipped,
                doc_ids,
                geo_columns_known,
                filter_columns_known,
                sort_rows: Vec::new(),
                sort_columns_known: Vec::new(),
                sort_column_types: Vec::new(),
            }))
        })
        .await
    }

    pub(super) async fn handle_fetch_values(
        &self,
        request: Request<crate::pb::FetchValuesRequest>,
    ) -> Result<Response<crate::pb::FetchValuesResponse>, Status> {
        crate::metrics::timed(Route::FetchValues, request, |request| async move {
            let req = request.into_inner();
            let offset = self.config.slot_offset;
            let state = self.state.clone();
            let config = self.config.clone();
            let resp = tokio::task::spawn_blocking(
                move || -> Result<crate::pb::FetchValuesResponse, Status> {
                    // Stage parameters validate everywhere they arrive; a
                    // malformed stage is a request error, not a shard gap.
                    let specs = parse_score_stages(&req.stages)?;
                    let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
                    let visibility = req
                        .visibility
                        .as_ref()
                        .and_then(|view| view.filter.as_ref());
                    let guard = read_shard(&state);
                    guard.check_stats_epoch(
                        req.expected_stats_epoch,
                        &req.expected_stats_incarnation,
                    )?;
                    let (_, visibility_columns_known) =
                        filter_known_flags(guard.bm25.as_ref(), &[], visibility);
                    let mut response = crate::pb::FetchValuesResponse {
                        typed_map_contract_version: crate::values::TYPED_MAP_FETCH_VERSION,
                        stats_epoch: guard.stats_epoch,
                        stats_incarnation: guard.stats_incarnation.bytes()?,
                        visibility_fingerprint: scope.fingerprint().to_vec(),
                        visibility_columns_known,
                        ..Default::default()
                    };
                    response.identities_included = req.include_identities;
                    if req.include_identities {
                        if req.candidate_ids.len() > 1_000_000 {
                            return Err(Status::resource_exhausted(
                                "identity fetch exceeds 1000000 input IDs",
                            ));
                        }
                        let slots = guard.visible_candidate_slots(
                            req.visibility.as_ref(),
                            &req.candidate_ids,
                            offset,
                            usize::try_from(physical_rows(&guard)).map_err(|_| {
                                Status::failed_precondition(
                                    "identity row span exceeds this host address space",
                                )
                            })?,
                        )?;
                        let mut bytes = 0usize;
                        for slot in slots {
                            let row = crate::pb::CandidateIdentity {
                                doc_id: offset + slot as u64,
                                identity: guard.bm25.as_ref().and_then(|store| {
                                    u32::try_from(slot)
                                        .ok()
                                        .and_then(|local| store.document_identity(local))
                                }),
                            };
                            crate::query_identity::charge_candidate_identity(&row, &mut bytes)?;
                            response.identities.push(row);
                        }
                    }
                    if req.include_identities && req.projections.is_empty() && req.stages.is_empty()
                    {
                        return Ok(response);
                    }
                    let projection_leaves = {
                        let mut leaves = Vec::new();
                        for p in &req.projections {
                            if let Some(expr) = p.expr.as_ref() {
                                crate::values::column_leaves(expr, &mut leaves);
                            }
                        }
                        leaves
                    };
                    // No column tables at all: this shard holds none of the
                    // candidates' values and resolves no column.
                    let Some(store) = guard.bm25.as_ref() else {
                        response.stage_columns_known = vec![false; req.stages.len()];
                        let columns = EmptyColumns(&config);
                        response.projection_leaves_known = projection_leaves
                            .iter()
                            .map(|leaf| crate::values::leaf_known(leaf, &columns))
                            .collect();
                        response.projection_types = req
                            .projections
                            .iter()
                            .map(|projection| {
                                let expr = projection.expr.as_ref().ok_or_else(|| {
                                    Status::invalid_argument(
                                        "projection: empty compiled expression",
                                    )
                                })?;
                                crate::values::resolve(expr, &columns)
                                    .map(|(_, ty)| crate::pb::ScalarValueType::from(ty) as i32)
                            })
                            .collect::<Result<_, Status>>()?;
                        return Ok(response);
                    };
                    if store.as_index().is_none() {
                        return Err(Status::failed_precondition(
                            "bm25 bulk build in progress; Flush before fetching values",
                        ));
                    }
                    let doc_filter = crate::filter::DocFilter {
                        deleted: None,
                        geo: Default::default(),
                        pred: visibility
                            .map(|filter| store.resolve_filter(filter))
                            .transpose()?,
                        phrase: Vec::new(),
                    };
                    let projection_leaves_known: Vec<bool> = projection_leaves
                        .iter()
                        .map(|leaf| crate::values::leaf_known(leaf, store))
                        .collect();
                    // Projections resolve against this shard's tables once
                    // per request; type conflicts refuse here, by name —
                    // the same rule as the lexical route.
                    let resolved: Vec<(crate::values::ResolvedValue, crate::values::ValueType)> =
                        req.projections
                            .iter()
                            .map(|p| {
                                let expr = p.expr.as_ref().ok_or_else(|| {
                                    Status::invalid_argument(
                                        "projection: empty compiled expression",
                                    )
                                })?;
                                crate::values::resolve(expr, store)
                            })
                            .collect::<Result<_, Status>>()?;
                    let projection_types = resolved
                        .iter()
                        .map(|(_, vt)| crate::pb::ScalarValueType::from(*vt) as i32)
                        .collect();
                    let chain = store.resolve_chain(&specs);
                    let stage_columns_known =
                        chain.stages.iter().map(|s| s.column.is_some()).collect();
                    let numeric_read = ShardNumericRead(store);
                    let n = u64::from(store.next_doc_id());
                    let mut ids: Vec<u64> = req
                        .candidate_ids
                        .iter()
                        .copied()
                        .filter(|&id| {
                            id >= offset
                                && id - offset < n
                                && !guard.live_docs.is_deleted((id - offset) as usize)
                                && doc_filter.passes((id - offset) as u32, &numeric_read)
                        })
                        .collect();
                    ids.sort_unstable();
                    ids.dedup();
                    let rows = ids
                        .into_iter()
                        .map(|id| {
                            let local = (id - offset) as u32;
                            crate::pb::FetchedRow {
                                doc_id: id,
                                values: resolved
                                    .iter()
                                    .map(|(rv, _)| {
                                        projected_value(
                                            crate::values::eval(rv, local, &numeric_read),
                                            store,
                                        )
                                    })
                                    .collect(),
                                stage_values: chain
                                    .stages
                                    .iter()
                                    .map(|s| crate::pb::ProjectedValue {
                                        value: s
                                            .contribution(local, &numeric_read)
                                            .map(crate::pb::projected_value::Value::DoubleValue),
                                    })
                                    .collect(),
                            }
                        })
                        .collect();
                    response.rows = rows;
                    response.stage_columns_known = stage_columns_known;
                    response.projection_leaves_known = projection_leaves_known;
                    response.projection_types = projection_types;
                    Ok(response)
                },
            )
            .await
            .map_err(|e| Status::internal(format!("fetch values task failed: {e}")))??;
            Ok(Response::new(resp))
        })
        .await
    }

    pub(super) async fn handle_get_documents(
        &self,
        request: Request<GetDocumentsRequest>,
    ) -> Result<Response<GetDocumentsResponse>, Status> {
        crate::metrics::timed(Route::GetDocuments, request, |request| async move {
            let req = request.into_inner();
            let offset = self.config.slot_offset;
            let guard = read_shard(&self.state);
            let mut documents = Vec::new();
            if let Some(store) = guard.bm25.as_ref() {
                let source_metadata = store;
                let store = store.as_index().ok_or_else(|| {
                    Status::failed_precondition("bm25 bulk build in progress; Flush first")
                })?;
                for id in req.doc_ids {
                    if id < offset {
                        continue;
                    }
                    let Ok(local) = u32::try_from(id - offset) else {
                        continue;
                    };
                    if guard.live_docs.is_deleted(local as usize) {
                        continue;
                    }
                    if let Some(text) = store.text(local) {
                        documents.push(StoredDocument {
                            doc_id: id,
                            identity: source_metadata.document_identity(local),
                            sentence_spans: store
                                .doc_sentences(local)
                                .unwrap_or_default()
                                .into_iter()
                                .map(|(start, end)| crate::pb::OffsetSpan { start, end })
                                .collect(),
                            text,
                            lineage: store.lineage(local).map(|l| crate::pb::DocLineage {
                                parent_id: l.parent_id,
                                group_id: l.group_id,
                                span_start: l.span_start,
                                span_end: l.span_end,
                            }),
                        });
                    }
                }
            }
            Ok(Response::new(GetDocumentsResponse { documents }))
        })
        .await
    }

    pub(super) async fn handle_resolve_parents(
        &self,
        request: Request<ResolveParentsRequest>,
    ) -> Result<Response<ResolveParentsResponse>, Status> {
        crate::metrics::timed(Route::ResolveParents, request, |request| async move {
            const SELF_PARENT_TAG: u64 = 1 << 63;
            let req = request.into_inner();
            let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let selection = crate::lineage::LineageSelection::new(&req.fields)?;
            let offset = self.config.slot_offset;
            let guard = read_shard(&self.state);
            guard.check_stats_epoch(req.expected_stats_epoch, &req.expected_stats_incarnation)?;
            let visibility = req
                .visibility
                .as_ref()
                .and_then(|view| view.filter.as_ref());
            let (_, visibility_columns_known) =
                filter_known_flags(guard.bm25.as_ref(), &[], visibility);
            if guard
                .bm25
                .as_ref()
                .is_some_and(|store| store.as_index().is_none())
            {
                return Err(Status::failed_precondition(
                    "bm25 bulk build in progress; Flush before reading lineage",
                ));
            }
            let doc_filter = crate::filter::DocFilter {
                deleted: None,
                geo: Default::default(),
                pred: guard
                    .bm25
                    .as_ref()
                    .and_then(|store| visibility.map(|filter| store.resolve_filter(filter)))
                    .transpose()?,
                phrase: Vec::new(),
            };
            let rows = guard
                .index
                .as_ref()
                .map_or(0, |index| index.len() as u64)
                .max(
                    guard
                        .bm25
                        .as_ref()
                        .map_or(0, |store| u64::from(store.next_doc_id())),
                );
            let store = guard.bm25.as_ref().and_then(|store| store.as_index());
            let mut parents = Vec::new();
            let mut seen = std::collections::HashSet::new();
            for doc_id in req.doc_ids {
                if !seen.insert(doc_id) {
                    continue;
                }
                let Some(local) = doc_id.checked_sub(offset) else {
                    continue;
                };
                if local >= rows {
                    continue;
                }
                if guard.live_docs.is_deleted(local as usize) {
                    continue;
                }
                if req.visibility.is_some() {
                    let Some(metadata) = guard.bm25.as_ref() else {
                        continue;
                    };
                    if local >= u64::from(metadata.next_doc_id())
                        || !doc_filter.passes(local as u32, &ShardNumericRead(metadata))
                    {
                        continue;
                    }
                }
                let lineage = u32::try_from(local)
                    .ok()
                    .and_then(|local| store.and_then(|store| store.lineage(local)));
                let (parent_id, group_id) =
                    lineage.map_or((SELF_PARENT_TAG | doc_id, 0), |l| (l.parent_id, l.group_id));
                parents.push(ResolvedParent {
                    doc_id,
                    parent_id: if selection.contains(crate::pb::LineageField::ParentId) {
                        parent_id
                    } else {
                        0
                    },
                    group_id: if selection.contains(crate::pb::LineageField::GroupId) {
                        group_id
                    } else {
                        0
                    },
                });
            }
            Ok(Response::new(ResolveParentsResponse {
                parents,
                fields: selection.wire(),
                stats_epoch: guard.stats_epoch,
                stats_incarnation: guard.stats_incarnation.bytes()?,
                visibility_fingerprint: scope.fingerprint().to_vec(),
                visibility_columns_known,
            }))
        })
        .await
    }
}
