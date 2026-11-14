//! SearchService transport delegation and administrative handlers.

use super::*;

#[tonic::async_trait]
impl SearchService for CoordinatorServiceImpl {
    type QueryStreamStream =
        crate::metrics::Timed<ReceiverStream<Result<crate::pb::QueryStreamResponse, Status>>>;

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        self.rpc_search(request).await
    }

    async fn bm25_search(
        &self,
        request: Request<Bm25SearchRequest>,
    ) -> Result<Response<Bm25SearchResponse>, Status> {
        self.rpc_bm25_search(request).await
    }

    async fn phrase_search(
        &self,
        request: Request<crate::pb::PhraseSearchRequest>,
    ) -> Result<Response<Bm25SearchResponse>, Status> {
        self.rpc_phrase_search(request).await
    }

    async fn hybrid_search(
        &self,
        request: Request<HybridSearchRequest>,
    ) -> Result<Response<HybridSearchResponse>, Status> {
        self.rpc_hybrid_search(request).await
    }

    async fn query(
        &self,
        request: Request<crate::pb::QueryRequest>,
    ) -> Result<Response<crate::pb::QueryResponse>, Status> {
        self.rpc_query(request).await
    }

    async fn query_stream(
        &self,
        request: Request<crate::pb::QueryStreamRequest>,
    ) -> Result<Response<Self::QueryStreamStream>, Status> {
        self.rpc_query_stream(request).await
    }

    async fn plan_index(
        &self,
        request: Request<crate::pb::PlanIndexRequest>,
    ) -> Result<Response<crate::pb::PlanIndexResponse>, Status> {
        self.rpc_plan_index(request).await
    }

    async fn describe_schema(
        &self,
        request: Request<crate::pb::DescribeSchemaRequest>,
    ) -> Result<Response<crate::pb::DescribeSchemaResponse>, Status> {
        self.rpc_describe_schema(request).await
    }

    async fn routed_ingest_mapped(
        &self,
        request: Request<Streaming<RoutedIngestMappedRequest>>,
    ) -> Result<Response<RoutedIngestMappedResponse>, Status> {
        self.rpc_routed_ingest_mapped(request).await
    }

    async fn freeze_topology_writes(
        &self,
        request: Request<FreezeTopologyWritesRequest>,
    ) -> Result<Response<FreezeTopologyWritesResponse>, Status> {
        self.rpc_freeze_topology_writes(request).await
    }

    async fn publish_topology(
        &self,
        request: Request<PublishTopologyRequest>,
    ) -> Result<Response<PublishTopologyResponse>, Status> {
        self.rpc_publish_topology(request).await
    }

    async fn abort_topology_cutover(
        &self,
        request: Request<AbortTopologyCutoverRequest>,
    ) -> Result<Response<AbortTopologyCutoverResponse>, Status> {
        self.rpc_abort_topology_cutover(request).await
    }

    async fn aggregate(
        &self,
        request: Request<crate::pb::AggregateRequest>,
    ) -> Result<Response<crate::pb::AggregateResponse>, Status> {
        self.rpc_aggregate(request).await
    }

    async fn plan_placement(
        &self,
        request: Request<crate::pb::PlanPlacementRequest>,
    ) -> Result<Response<crate::pb::PlanPlacementResponse>, Status> {
        self.rpc_plan_placement(request).await
    }

    async fn suggest(
        &self,
        request: Request<crate::pb::SuggestRequest>,
    ) -> Result<Response<crate::pb::SuggestResponse>, Status> {
        self.rpc_suggest(request).await
    }

    async fn term_suggest(
        &self,
        request: Request<crate::pb::TermSuggestRequest>,
    ) -> Result<Response<crate::pb::TermSuggestResponse>, Status> {
        self.rpc_term_suggest(request).await
    }

    async fn cluster_health(
        &self,
        _request: Request<ClusterHealthRequest>,
    ) -> Result<Response<ClusterHealthResponse>, Status> {
        self.rpc_cluster_health(_request).await
    }

    async fn broadcast_vector_backend(
        &self,
        request: Request<BroadcastVectorBackendRequest>,
    ) -> Result<Response<BroadcastVectorBackendResponse>, Status> {
        self.rpc_broadcast_vector_backend(request).await
    }

    async fn variant_search(
        &self,
        request: Request<VariantSearchRequest>,
    ) -> Result<Response<VariantSearchResponse>, Status> {
        self.rpc_variant_search(request).await
    }
}

impl CoordinatorServiceImpl {
    pub(super) async fn rpc_plan_index(
        &self,
        request: Request<crate::pb::PlanIndexRequest>,
    ) -> Result<Response<crate::pb::PlanIndexResponse>, Status> {
        crate::metrics::timed(Route::PlanIndex, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::plan_index(
                    &snapshot,
                    crate::metrics::nested(request),
                ))
                .await;
            }
            // Derivation is local and deterministic (docs/descriptor-mappings.md):
            // nothing fans out, nothing binds, and the same request returns the
            // same fingerprint on every coordinator.
            let req = request.into_inner();
            let plan = crate::mapping::derive_plan_with_definition(
                &req.descriptor_set,
                &req.message_type,
                req.index_definition.as_ref(),
            )?;
            Ok(Response::new(crate::pb::PlanIndexResponse {
                plan: Some(plan),
            }))
        })
        .await
    }

    pub(super) async fn rpc_describe_schema(
        &self,
        request: Request<crate::pb::DescribeSchemaRequest>,
    ) -> Result<Response<crate::pb::DescribeSchemaResponse>, Status> {
        crate::metrics::timed(Route::DescribeSchema, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::describe_schema(
                    &snapshot,
                    crate::metrics::nested(request),
                ))
                .await;
            }
            let req = request.into_inner();
            crate::mapping::describe_schema(&req.descriptor_set, &req.message_type)
                .map(Response::new)
        })
        .await
    }

    pub(super) async fn rpc_routed_ingest_mapped(
        &self,
        request: Request<Streaming<RoutedIngestMappedRequest>>,
    ) -> Result<Response<RoutedIngestMappedResponse>, Status> {
        crate::metrics::timed(Route::RoutedIngestMapped, request, |request| async move {
            let mut inbound = request.into_inner();
            let bind = Self::routed_bind(&mut inbound).await?;
            self.admit(&bind.collection)?;
            self.routed_ingest_mapped_bound(bind, inbound)
                .await
                .map(Response::new)
        })
        .await
    }

    /// Placement dry run (`docs/placement.md`, "The dry run"): what a
    /// proposed tree would do to the rows this topology holds, from
    /// filtered counts (`src/placement_plan.rs` has the arithmetic). It
    /// reads only. A predicate naming a column no shard holds is refused
    /// by name, the rule every filtered route applies to a typo.
    pub(super) async fn rpc_plan_placement(
        &self,
        request: Request<crate::pb::PlanPlacementRequest>,
    ) -> Result<Response<crate::pb::PlanPlacementResponse>, Status> {
        crate::metrics::timed(Route::PlanPlacement, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::plan_placement(
                    &snapshot,
                    crate::metrics::nested(request),
                ))
                .await;
            }
            let req = request.into_inner();
            let proposed = req.proposed.as_ref().ok_or_else(|| {
                Status::invalid_argument("plan_placement: proposed tree is absent")
            })?;
            let config = crate::placement::PlacementTreeConfig::from_proto(proposed);
            let placement =
                crate::placement::Placement::validate(&config).map_err(Status::invalid_argument)?;
            let plan = crate::placement_plan::plan(&placement).map_err(Status::invalid_argument)?;
            let base = RequestFilters::compile(&[], &req.filter)?;
            let mut counter = PlanCounter {
                coordinator: self,
                base: base.tree,
                column: placement.column().to_string(),
                memo: std::collections::HashMap::new(),
            };
            let mut cells = Vec::new();
            let mut rows = 0u64;
            let mut moving_rows = 0u64;
            let mut defaulted_rows = 0u64;
            let mut stack: Vec<(Vec<usize>, &crate::placement_plan::PlanNode)> =
                plan.iter().enumerate().map(|(i, n)| (vec![i], n)).collect();
            // Leaves in code order: depth-first, chain order.
            let mut leaves: Vec<(Vec<usize>, &crate::placement_plan::PlanNode)> = Vec::new();
            stack.reverse();
            while let Some((path, node)) = stack.pop() {
                if node.is_leaf() {
                    leaves.push((path, node));
                } else {
                    for (i, child) in node.children.iter().enumerate().rev() {
                        let mut p = path.clone();
                        p.push(i);
                        stack.push((p, child));
                    }
                }
            }
            for (path, leaf) in leaves {
                let per_shard = counter.first(&plan, &path, None).await?;
                let staying = counter
                    .first(
                        &plan,
                        &path,
                        Some(crate::placement_plan::code_equals(
                            &counter.column.clone(),
                            leaf.code,
                        )),
                    )
                    .await?;
                for (shard, (count, stay)) in per_shard.iter().zip(&staying).enumerate() {
                    if *count == 0 {
                        continue;
                    }
                    let moving = count.saturating_sub(*stay);
                    rows += count;
                    moving_rows += moving;
                    if leaf.is_default {
                        defaulted_rows += count;
                    }
                    cells.push(crate::pb::PlacementCell {
                        shard: shard as u32,
                        code: leaf.code as u64,
                        leaf: leaf.name.clone(),
                        rows: *count,
                        moving_rows: moving,
                    });
                }
            }
            Ok(Response::new(crate::pb::PlanPlacementResponse {
                topology_generation: self.current_topology_generation(),
                cells,
                rows,
                moving_rows,
                defaulted_rows,
            }))
        })
        .await
    }
}

impl CoordinatorServiceImpl {
    /// Exact aggregates over the filtered corpus
    /// (docs/aggregations.md). CEL compiles ONCE, here: the filter
    /// into the predicate IR, each expression into the ValueExpr IR;
    /// no shard ever sees text.
    pub(super) async fn rpc_aggregate(
        &self,
        request: Request<crate::pb::AggregateRequest>,
    ) -> Result<Response<crate::pb::AggregateResponse>, Status> {
        crate::metrics::timed(Route::Aggregate, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::aggregate(
                    &snapshot,
                    crate::metrics::nested(request),
                ))
                .await;
            }
            let req = request.into_inner();
            let filters = RequestFilters::compile(&req.geo_filters, &req.filter)?;
            let compiled = compile_aggregations(&req)?;
            self.fanout_aggregate(&filters, &compiled, None)
                .await
                .map(Response::new)
        })
        .await
    }

    pub(super) async fn rpc_cluster_health(
        &self,
        _request: Request<ClusterHealthRequest>,
    ) -> Result<Response<ClusterHealthResponse>, Status> {
        crate::metrics::timed(Route::ClusterHealth, _request, |_request| async move {
            self.admit(&_request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::cluster_health(
                    &snapshot,
                    crate::metrics::nested(Request::new(ClusterHealthRequest {
                        collection: String::new(),
                    })),
                ))
                .await;
            }
            // Probe every primary, then every configured replica. Unreachable
            // is a reported outcome, never an error; a 2s probe timeout keeps
            // one filtered port from stalling the whole report.
            let mut probes: Vec<(u32, String, bool)> = Vec::new();
            for (shard, addr) in self.node_addrs.iter().enumerate() {
                probes.push((shard as u32, addr.clone(), false));
            }
            for (shard, replica) in self.replica_addrs.iter().enumerate() {
                if let Some(addr) = replica {
                    probes.push((shard as u32, addr.clone(), true));
                }
            }
            let mut tasks = Vec::with_capacity(probes.len());
            for (shard, addr, is_replica) in probes {
                let client = self.node_client(&addr);
                tasks.push(tokio::spawn(async move {
                    let outcome = match client {
                        Ok(mut client) => {
                            match tokio::time::timeout(
                                Duration::from_secs(2),
                                client.health(HealthRequest {}),
                            )
                            .await
                            {
                                Ok(reply) => reply.map(|r| r.into_inner()),
                                Err(_) => Err(Status::deadline_exceeded("health probe timed out")),
                            }
                        }
                        Err(e) => Err(e),
                    };
                    match outcome {
                        Ok(health) => ShardHealth {
                            shard,
                            addr,
                            is_replica,
                            reachable: true,
                            error: String::new(),
                            health: Some(health),
                        },
                        Err(e) => ShardHealth {
                            shard,
                            addr,
                            is_replica,
                            reachable: false,
                            error: e.message().to_string(),
                            health: None,
                        },
                    }
                }));
            }
            let mut targets = Vec::with_capacity(tasks.len());
            for task in tasks {
                match task.await {
                    Ok(target) => targets.push(target),
                    Err(e) => {
                        return Err(Status::internal(format!("health probe task failed: {e}")))
                    }
                }
            }
            // A reachable node that serves another collection is a
            // misconfiguration, named here rather than counted here.
            for target in &mut targets {
                if let Some(health) = &target.health {
                    if health.collection != self.collection {
                        target.error = format!(
                            "node serves collection {:?}, but this coordinator is {:?}",
                            health.collection, self.collection
                        );
                    }
                }
            }
            let provider_mismatch = provider_mismatch_of(&targets);
            #[cfg(feature = "net")]
            let clustered_vector = if let Some(backend) = &self.clustered_vectors {
                Some(match backend.health().await {
                    Ok(health) => {
                        // The score-space identity a quality profile binds to;
                        // a servable cluster that cannot state it reports that
                        // as its error rather than an empty fingerprint.
                        let (scoring_fingerprint, dimensions, error) = if health.servable {
                            match backend.quality_identity().await {
                                Ok(identity) => (
                                    identity.scoring_fingerprint,
                                    identity.dimensions,
                                    health.error,
                                ),
                                Err(status) => (String::new(), 0, status.message().to_string()),
                            }
                        } else {
                            (String::new(), 0, health.error)
                        };
                        ClusteredVectorHealth {
                            backend_kind: "clustered-turbovec".to_string(),
                            transport: backend.transport_name().to_string(),
                            reachable: true,
                            servable: health.servable,
                            error,
                            rows: health.rows,
                            topology_generation: health.topology_generation,
                            scoring_fingerprint,
                            dimensions,
                        }
                    }
                    Err(status) => ClusteredVectorHealth {
                        backend_kind: "clustered-turbovec".to_string(),
                        transport: backend.transport_name().to_string(),
                        reachable: false,
                        servable: false,
                        error: status.to_string(),
                        rows: 0,
                        topology_generation: 0,
                        scoring_fingerprint: String::new(),
                        dimensions: 0,
                    },
                })
            } else {
                None
            };
            #[cfg(not(feature = "net"))]
            let clustered_vector: Option<ClusteredVectorHealth> = None;
            Ok(Response::new(ClusterHealthResponse {
                collections: Vec::new(),
                targets,
                clustered_vector,
                provider_mismatch,
                topology_generation: self.topology_generation,
            }))
        })
        .await
    }

    pub(super) async fn rpc_broadcast_vector_backend(
        &self,
        request: Request<BroadcastVectorBackendRequest>,
    ) -> Result<Response<BroadcastVectorBackendResponse>, Status> {
        crate::metrics::timed(
            Route::BroadcastVectorBackend,
            request,
            |request| async move {
                self.admit(&request.get_ref().collection)?;
                if let Some(snapshot) = self.request_snapshot() {
                    return Box::pin(SearchService::broadcast_vector_backend(
                        &snapshot,
                        crate::metrics::nested(request),
                    ))
                    .await;
                }
                let req = request.into_inner();
                if req.dim == 0 || req.config.is_none() {
                    return Err(Status::invalid_argument(
                        "positive dim and vector backend config are required",
                    ));
                }
                let results = self.fanout_vector_backend(&req).await;
                Ok(Response::new(BroadcastVectorBackendResponse { results }))
            },
        )
        .await
    }

    pub(super) async fn rpc_variant_search(
        &self,
        request: Request<VariantSearchRequest>,
    ) -> Result<Response<VariantSearchResponse>, Status> {
        crate::metrics::timed(Route::VariantSearch, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::variant_search(
                    &snapshot,
                    crate::metrics::nested(request),
                ))
                .await;
            }
            let req = request.into_inner();
            if req.variants.len() < 2 {
                return Err(Status::invalid_argument(format!(
                    "variant search compares configurations: at least 2 variants required, got {}",
                    req.variants.len()
                )));
            }
            // 0 = unset selects max_k, like every other client-facing k.
            let k = self.resolve_k(req.k)?;
            // Labels carry the whole result: a blank or duplicated one makes
            // the diffs unreadable, so reject rather than disambiguate.
            let mut seen: Vec<&str> = Vec::with_capacity(req.variants.len());
            for (i, v) in req.variants.iter().enumerate() {
                if v.label.is_empty() {
                    return Err(Status::invalid_argument(format!(
                        "variant {i} has an empty label; every arm must be named"
                    )));
                }
                if seen.contains(&v.label.as_str()) {
                    return Err(Status::invalid_argument(format!(
                        "duplicate variant label {:?}: labels identify arms in the diffs",
                        v.label
                    )));
                }
                seen.push(&v.label);
                if v.query.is_none() {
                    return Err(Status::invalid_argument(format!(
                        "variant {:?} has no query set (expected bm25 or hybrid)",
                        v.label
                    )));
                }
            }
            // RBO's persistence is a probability; 1.0 would never terminate
            // its weighting and is as much an error as 2.0.
            let rbo_p = if req.rbo_p == 0.0 {
                0.9
            } else {
                f64::from(req.rbo_p)
            };
            if !(rbo_p.is_finite() && rbo_p > 0.0 && rbo_p < 1.0) {
                return Err(Status::invalid_argument(format!(
                    "rbo_p must be in (0, 1); got {}",
                    req.rbo_p
                )));
            }
            if req.interleave && req.variants.len() != 2 {
                return Err(Status::invalid_argument(format!(
                    "interleaving is a two-way method (team draft); got {} variants. \
                 Compare more arms with diffs, or interleave them pairwise.",
                    req.variants.len()
                )));
            }
            let request_id = if req.request_id.is_empty() {
                format!(
                    "req-{}-{}",
                    std::process::id(),
                    REQUEST_COUNTER.fetch_add(1, AtomicOrdering::Relaxed)
                )
            } else {
                req.request_id.clone()
            };

            // Sequential, not concurrent. Running the arms together would
            // make each one's `elapsed_ms` a measure of how hard the other
            // arms were hitting the same shards, and per-arm cost is part of
            // what an A/B is asked to report.
            let mut results = Vec::with_capacity(req.variants.len());
            for variant in &req.variants {
                let query = variant
                    .query
                    .as_ref()
                    .expect("query presence checked above");
                let started = std::time::Instant::now();
                let hits = self.run_variant(query, k).await.map_err(|e| {
                    // Name the arm: with several in flight, an unadorned
                    // status leaves the caller guessing which one failed.
                    Status::new(
                        e.code(),
                        format!("variant {:?}: {}", variant.label, e.message()),
                    )
                })?;
                results.push(VariantResult {
                    label: variant.label.clone(),
                    hits,
                    elapsed_ms: started.elapsed().as_secs_f32() * 1000.0,
                });
            }

            let diffs: Vec<RankingDiff> = results[1..]
                .iter()
                .map(|v| diff_against(&results[0], v, k as usize, rbo_p))
                .collect();

            let interleaving = req.interleave.then(|| {
                let a: Vec<u64> = results[0].hits.iter().map(|h| h.doc_id).collect();
                let b: Vec<u64> = results[1].hits.iter().map(|h| h.doc_id).collect();
                // A seed derived from the query text keeps a re-run of the
                // same query byte-identical while still varying across
                // queries, so determinism does not become a first-position
                // bias for one arm.
                let seed = if req.interleave_seed == 0 {
                    crate::interleave::seed_for(variant_text(&req.variants[0]))
                } else {
                    req.interleave_seed
                };
                let merged = crate::interleave::team_draft(&a, &b, k as usize, seed);
                Interleaving {
                    doc_ids: merged.ids,
                    teams: merged
                        .team
                        .into_iter()
                        .map(|t| match t {
                            crate::interleave::Team::A => InterleaveTeam::A as i32,
                            crate::interleave::Team::B => InterleaveTeam::B as i32,
                        })
                        .collect(),
                    seed,
                }
            });

            Ok(Response::new(VariantSearchResponse {
                request_id,
                results,
                diffs,
                interleaving,
            }))
        })
        .await
    }
}

impl CoordinatorServiceImpl {
    pub(super) async fn rpc_freeze_topology_writes(
        &self,
        request: Request<FreezeTopologyWritesRequest>,
    ) -> Result<Response<FreezeTopologyWritesResponse>, Status> {
        crate::metrics::timed(Route::FreezeTopologyWrites, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if self.live_topology.is_none() {
                return Err(Status::failed_precondition(
                    "topology cutover requires a generation-stamped hot shard map",
                ));
            }
            let requested = request.into_inner().required_topology_generation;
            if requested == 0 {
                return Err(Status::invalid_argument(
                    "freeze requires a nonzero topology generation",
                ));
            }
            if requested != self.current_topology_generation() {
                return Err(Status::failed_precondition(format!(
                    "freeze requires topology generation {requested}, live {}",
                    self.current_topology_generation()
                )));
            }
            if self
                .cutover_pending
                .compare_exchange(false, true, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
                .is_err()
            {
                return Err(Status::already_exists(
                    "another topology cutover is already pending",
                ));
            }
            let guard = self.write_gate.clone().write_owned().await;
            if requested != self.current_topology_generation() {
                self.cutover_pending.store(false, AtomicOrdering::Release);
                drop(guard);
                return Err(Status::failed_precondition(
                    "topology changed while the write barrier was being acquired",
                ));
            }
            let token = floor_token();
            *self
                .cutover_guard
                .lock()
                .expect("cutover guard mutex poisoned") = Some((token, guard));
            Ok(Response::new(FreezeTopologyWritesResponse {
                topology_generation: requested,
                cutover_token: token,
            }))
        })
        .await
    }

    pub(super) async fn rpc_publish_topology(
        &self,
        request: Request<PublishTopologyRequest>,
    ) -> Result<Response<PublishTopologyResponse>, Status> {
        crate::metrics::timed(Route::PublishTopology, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            let req = request.into_inner();
            let routes = req
                .shards
                .into_iter()
                .map(|shard| TopologyRoute {
                    addr: crate::config::normalize_addr(shard.addr),
                    replica: (!shard.replica.is_empty())
                        .then(|| crate::config::normalize_addr(shard.replica)),
                    hash_range: Some((shard.hash_lo, shard.hash_hi)),
                    placement: shard.has_placement.then_some(shard.placement as i64),
                })
                .collect();
            let tree = req
                .placement
                .as_ref()
                .map(crate::placement::PlacementTreeConfig::from_proto);
            let mut held = self
                .cutover_guard
                .lock()
                .expect("cutover guard mutex poisoned");
            let Some((token, _)) = held.as_ref() else {
                return Err(Status::failed_precondition(
                    "no topology cutover write freeze is active",
                ));
            };
            if *token != req.cutover_token {
                return Err(Status::permission_denied("cutover token does not match"));
            }
            self.publish_topology_inner(req.generation, routes, tree.as_ref())
                .map_err(Status::invalid_argument)?;
            held.take();
            self.cutover_pending.store(false, AtomicOrdering::Release);
            Ok(Response::new(PublishTopologyResponse {
                topology_generation: req.generation,
            }))
        })
        .await
    }

    pub(super) async fn rpc_abort_topology_cutover(
        &self,
        request: Request<AbortTopologyCutoverRequest>,
    ) -> Result<Response<AbortTopologyCutoverResponse>, Status> {
        crate::metrics::timed(Route::AbortTopologyCutover, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            let token = request.into_inner().cutover_token;
            let mut held = self
                .cutover_guard
                .lock()
                .expect("cutover guard mutex poisoned");
            let Some((expected, _)) = held.as_ref() else {
                return Err(Status::failed_precondition(
                    "no topology cutover write freeze is active",
                ));
            };
            if *expected != token {
                return Err(Status::permission_denied("cutover token does not match"));
            }
            held.take();
            self.cutover_pending.store(false, AtomicOrdering::Release);
            Ok(Response::new(AbortTopologyCutoverResponse {
                topology_generation: self.current_topology_generation(),
            }))
        })
        .await
    }
}
