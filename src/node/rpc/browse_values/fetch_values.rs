//! Candidate value projection, score-stage contributions, and identity fetch.

use crate::node::*;

/// One FetchValues request against a held shard state.
struct FetchScope<'a> {
    req: &'a crate::pb::FetchValuesRequest,
    guard: &'a ShardState,
    offset: u64,
    visibility: Option<&'a crate::pb::FilterExpr>,
}

impl FetchScope<'_> {
    fn push_identities(&self, response: &mut crate::pb::FetchValuesResponse) -> Result<(), Status> {
        let (req, guard, offset) = (self.req, self.guard, self.offset);
        if req.candidate_ids.len() > 1_000_000 {
            return Err(Status::resource_exhausted(
                "identity fetch exceeds 1000000 input IDs",
            ));
        }
        let slots = guard.visible_candidate_slots(
            req.visibility.as_ref(),
            &req.candidate_ids,
            offset,
            usize::try_from(physical_rows(guard)).map_err(|_| {
                Status::failed_precondition("identity row span exceeds this host address space")
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
        Ok(())
    }

    fn projection_leaves(&self) -> Vec<crate::values::ValueLeaf> {
        let mut leaves = Vec::new();
        for p in &self.req.projections {
            if let Some(expr) = p.expr.as_ref() {
                crate::values::column_leaves(expr, &mut leaves);
            }
        }
        leaves
    }

    /// No column tables at all: this shard holds none of the candidates'
    /// values and resolves no column.
    fn fill_without_store(
        &self,
        config: &NodeConfig,
        projection_leaves: &[crate::values::ValueLeaf],
        response: &mut crate::pb::FetchValuesResponse,
    ) -> Result<(), Status> {
        let req = self.req;
        response.stage_columns_known = vec![false; req.stages.len()];
        let columns = EmptyColumns(config);
        response.projection_leaves_known = projection_leaves
            .iter()
            .map(|leaf| crate::values::leaf_known(leaf, &columns))
            .collect();
        response.projection_types = req
            .projections
            .iter()
            .map(|projection| {
                let expr = projection.expr.as_ref().ok_or_else(|| {
                    Status::invalid_argument("projection: empty compiled expression")
                })?;
                crate::values::resolve(expr, &columns)
                    .map(|(_, ty)| crate::pb::ScalarValueType::from(ty) as i32)
            })
            .collect::<Result<_, Status>>()?;
        Ok(())
    }

    /// Projections resolve against this shard's tables once per request; type
    /// conflicts refuse here, by name — the same rule as the lexical route.
    fn resolve_projections(
        &self,
        store: &Bm25Shard,
    ) -> Result<Vec<(crate::values::ResolvedValue, crate::values::ValueType)>, Status> {
        self.req
            .projections
            .iter()
            .map(|p| {
                let expr = p.expr.as_ref().ok_or_else(|| {
                    Status::invalid_argument("projection: empty compiled expression")
                })?;
                crate::values::resolve(expr, store)
            })
            .collect::<Result<_, Status>>()
    }

    /// Candidate ids this shard holds that are live and visible, ascending
    /// and distinct.
    fn visible_ids(
        &self,
        store: &Bm25Shard,
        doc_filter: &crate::filter::DocFilter<'_>,
    ) -> Vec<u64> {
        let (req, guard, offset) = (self.req, self.guard, self.offset);
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
        ids
    }

    fn fill_from_store(
        &self,
        store: &Bm25Shard,
        specs: &[ScoreStageSpec],
        projection_leaves: &[crate::values::ValueLeaf],
        response: &mut crate::pb::FetchValuesResponse,
    ) -> Result<(), Status> {
        let (offset, visibility) = (self.offset, self.visibility);
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
        let resolved = self.resolve_projections(store)?;
        let projection_types = resolved
            .iter()
            .map(|(_, vt)| crate::pb::ScalarValueType::from(*vt) as i32)
            .collect();
        let chain = store.resolve_chain(specs);
        let stage_columns_known = chain.stages.iter().map(|s| s.column.is_some()).collect();
        let numeric_read = ShardNumericRead(store);
        let ids = self.visible_ids(store, &doc_filter);
        let rows = ids
            .into_iter()
            .map(|id| {
                let local = (id - offset) as u32;
                crate::pb::FetchedRow {
                    doc_id: id,
                    values: resolved
                        .iter()
                        .map(|(rv, _)| {
                            projected_value(crate::values::eval(rv, local, &numeric_read), store)
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
        Ok(())
    }
}

fn fetch_values(
    state: &RwLock<ShardState>,
    config: &NodeConfig,
    req: &crate::pb::FetchValuesRequest,
) -> Result<crate::pb::FetchValuesResponse, Status> {
    // Stage parameters validate everywhere they arrive; a
    // malformed stage is a request error, not a shard gap.
    let specs = parse_score_stages(&req.stages)?;
    let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
    let visibility = req
        .visibility
        .as_ref()
        .and_then(|view| view.filter.as_ref());
    let guard = read_shard(state);
    guard.check_stats_epoch(req.expected_stats_epoch, &req.expected_stats_incarnation)?;
    let (_, visibility_columns_known) = filter_known_flags(guard.bm25.as_ref(), &[], visibility);
    let mut response = crate::pb::FetchValuesResponse {
        typed_map_contract_version: crate::values::TYPED_MAP_FETCH_VERSION,
        stats_epoch: guard.stats_epoch,
        stats_incarnation: guard.stats_incarnation.bytes()?,
        visibility_fingerprint: scope.fingerprint().to_vec(),
        visibility_columns_known,
        ..Default::default()
    };
    let fetch = FetchScope {
        req,
        guard: &guard,
        offset: config.slot_offset,
        visibility,
    };
    response.identities_included = req.include_identities;
    if req.include_identities {
        fetch.push_identities(&mut response)?;
    }
    if req.include_identities && req.projections.is_empty() && req.stages.is_empty() {
        return Ok(response);
    }
    let projection_leaves = fetch.projection_leaves();
    match guard.bm25.as_ref() {
        None => fetch.fill_without_store(config, &projection_leaves, &mut response)?,
        Some(store) => fetch.fill_from_store(store, &specs, &projection_leaves, &mut response)?,
    }
    Ok(response)
}

impl NodeServiceImpl {
    pub(in crate::node::rpc) async fn handle_fetch_values(
        &self,
        request: Request<crate::pb::FetchValuesRequest>,
    ) -> Result<Response<crate::pb::FetchValuesResponse>, Status> {
        crate::metrics::timed(Route::FetchValues, request, |request| async move {
            let req = request.into_inner();
            let state = self.state.clone();
            let config = self.config.clone();
            let resp = tokio::task::spawn_blocking(move || fetch_values(&state, &config, &req))
                .await
                .map_err(|e| Status::internal(format!("fetch values task failed: {e}")))??;
            Ok(Response::new(resp))
        })
        .await
    }
}
