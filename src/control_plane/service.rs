//! The `ClusterControl` gRPC service over a `DurableControlPlane`.

use super::*;

/// Whether a call carries cluster membership: a client certificate the
/// listener verified against the cluster CA. One function for the
/// control and operator services; each names its own refusal.
pub(crate) fn cluster_membership<T>(request: &Request<T>, required: bool) -> bool {
    if !required {
        return true;
    }
    #[cfg(feature = "tls")]
    if request.peer_certs().is_some_and(|certs| !certs.is_empty()) {
        return true;
    }
    #[cfg(not(feature = "tls"))]
    let _ = request;
    false
}

impl ClusterControlService {
    pub fn new(plane: DurableControlPlane) -> Self {
        Self {
            plane,
            coordinator: None,
            require_client_cert: false,
        }
    }

    /// Demand a client certificate on every call.
    pub fn with_client_cert_required(mut self, required: bool) -> Self {
        self.require_client_cert = required;
        self
    }

    /// Cluster membership: a client certificate the listener verified
    /// against the cluster CA. A missing one refuses by name.
    fn membership<T>(&self, request: &Request<T>) -> Result<(), Status> {
        if cluster_membership(request, self.require_client_cert) {
            return Ok(());
        }
        Err(Status::unauthenticated(
            "cluster control requires a client certificate from the cluster CA; a bearer \
             token is not membership",
        ))
    }

    pub fn with_coordinator(mut self, coordinator: CoordinatorServiceImpl) -> Self {
        self.coordinator = Some(coordinator);
        self
    }

    /// The collection this plane governs (`docs/collections.md`).
    pub fn collection(&self) -> String {
        self.plane.collection()
    }

    /// Admit a request only for this plane's collection (the same rule
    /// as `CoordinatorServiceImpl::admit`).
    pub(super) fn admit(&self, requested: &str) -> Result<(), Status> {
        let own = self.plane.lock_state()?.collection.clone();
        if requested.is_empty() || requested == own {
            return Ok(());
        }
        Err(if own.is_empty() {
            Status::invalid_argument(format!(
                "unknown collection {requested:?}: this control plane governs one unnamed dataset"
            ))
        } else {
            Status::invalid_argument(format!(
                "this control plane governs collection {own:?}, not {requested:?}"
            ))
        })
    }

    pub fn into_server(self, max_message_bytes: usize) -> ClusterControlServer<Self> {
        ClusterControlServer::new(self)
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes)
    }

    pub fn publish_current_topology(&self) -> Result<(), Status> {
        // Keep publication serialized with persistence failures. A route
        // snapshot read before an ambiguous write must not be published after it.
        let state = self.plane.lock_state()?;
        let Some(coordinator) = &self.coordinator else {
            return Ok(());
        };
        let (generation, routes) = DurableControlPlane::topology_routes_of(&state);
        // The durable control state holds hash ranges and addresses, not
        // placement codes: publishing over a placed generation would
        // strip the tree, so it is refused until the plane carries it.
        if coordinator.current_placement().is_some() {
            return Err(Status::failed_precondition(
                "the live topology has a placement tree (docs/placement.md); the control \
                 plane does not publish placed topologies yet",
            ));
        }
        let current = coordinator.current_topology_generation();
        if generation == current {
            if routes == coordinator.current_topology_routes() {
                return Ok(());
            }
            return Err(Status::failed_precondition(format!(
                "durable control topology conflicts with live coordinator generation {current}"
            )));
        }
        if generation < current {
            return Err(Status::failed_precondition(format!(
                "durable control topology generation {generation} is behind live coordinator generation {current}"
            )));
        }
        coordinator
            .reload_topology(generation, routes, None)
            .map_err(|error| Status::failed_precondition(format!("publish topology: {error}")))
    }

    fn publish_if_needed(&self) -> Result<(), Status> {
        self.publish_current_topology()
    }

    /// What the planner needs beyond the durable state: the encoded row
    /// bytes of the provider (one geometry cluster-wide, read from the
    /// live shards' health and required to agree) and each shard's
    /// placement pool (its leaf's node set, matched to registered nodes
    /// by advertised address or node id). Needs the coordinator: without
    /// one the plane cannot express any load in the rate's units.
    async fn balance_context(
        &self,
        collection: &str,
    ) -> Result<(u64, BTreeMap<String, BalancePool>), Status> {
        let Some(coordinator) = &self.coordinator else {
            return Err(Status::failed_precondition(
                "plan_balance: this control plane has no coordinator attached, so the \
                 provider geometry and the placement pools are unknown",
            ));
        };
        let health = crate::pb::search_service_server::SearchService::cluster_health(
            coordinator,
            Request::new(crate::pb::ClusterHealthRequest {
                collection: collection.to_string(),
            }),
        )
        .await?
        .into_inner();
        let mut geometry: Option<(u32, u32)> = None;
        for target in &health.targets {
            let Some(shard_health) = target.health.as_ref().filter(|_| target.reachable) else {
                continue;
            };
            if shard_health.dim == 0 || shard_health.bits_per_dimension == 0 {
                continue;
            }
            let seen = (shard_health.dim, shard_health.bits_per_dimension);
            match geometry {
                None => geometry = Some(seen),
                Some(first) if first != seen => {
                    return Err(Status::failed_precondition(format!(
                        "plan_balance: shard {} at {} reports geometry {}x{} bits but another \
                         shard reports {}x{} bits; one provider geometry is required",
                        target.shard, target.addr, seen.0, seen.1, first.0, first.1
                    )));
                }
                Some(_) => {}
            }
        }
        let row_bytes = geometry.map_or(0, |(dim, bits)| {
            crate::chunked::encoded_row_bytes(dim as usize, bits as usize)
        });
        let mut pools = BTreeMap::new();
        if let Some(placement) = coordinator.current_placement() {
            let node_ids_by_addr: BTreeMap<String, String> = {
                let state = self.plane.lock_state()?;
                state
                    .nodes
                    .values()
                    .map(|n| (bare_addr(&n.addr).to_string(), n.node_id.clone()))
                    .collect()
            };
            for route in coordinator.current_topology_routes() {
                let Some(code) = route.placement else {
                    continue;
                };
                let Some(leaf) = placement.leaf_by_code(code) else {
                    continue;
                };
                let node_ids = (!leaf.nodes.is_empty()).then(|| {
                    leaf.nodes
                        .iter()
                        .map(|entry| {
                            node_ids_by_addr
                                .get(bare_addr(entry))
                                .cloned()
                                .unwrap_or_else(|| entry.clone())
                        })
                        .collect::<BTreeSet<String>>()
                });
                pools.insert(
                    bare_addr(&route.addr).to_string(),
                    BalancePool {
                        leaf: leaf.name.clone(),
                        node_ids,
                    },
                );
            }
        }
        Ok((row_bytes, pools))
    }
}

#[tonic::async_trait]
impl ClusterControl for ClusterControlService {
    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<NodeLease>, Status> {
        crate::metrics::timed(Route::RegisterNode, request, |request| async move {
            self.membership(&request)?;
            self.admit(&request.get_ref().collection)?;
            self.plane
                .register(request.into_inner(), now_ms())
                .map(Response::new)
        })
        .await
    }

    async fn renew_node_lease(
        &self,
        request: Request<RenewNodeLeaseRequest>,
    ) -> Result<Response<NodeLease>, Status> {
        crate::metrics::timed(Route::RenewNodeLease, request, |request| async move {
            self.membership(&request)?;
            self.admit(&request.get_ref().collection)?;
            self.plane
                .renew(request.into_inner(), now_ms())
                .map(Response::new)
        })
        .await
    }

    async fn drain_node(
        &self,
        request: Request<DrainNodeRequest>,
    ) -> Result<Response<ClusterPlan>, Status> {
        crate::metrics::timed(Route::DrainNode, request, |request| async move {
            self.membership(&request)?;
            self.admit(&request.get_ref().collection)?;
            self.plane.drain(request.into_inner(), now_ms())?;
            let (plan, _changed) = self.plane.reconcile(false, now_ms())?;
            self.publish_if_needed()?;
            Ok(Response::new(plan))
        })
        .await
    }

    async fn report_shard(
        &self,
        request: Request<ReportShardRequest>,
    ) -> Result<Response<ClusterPlan>, Status> {
        crate::metrics::timed(Route::ReportShard, request, |request| async move {
            self.membership(&request)?;
            self.admit(&request.get_ref().collection)?;
            let req = request.into_inner();
            if let Some(replica) = &req.replica {
                self.admit(&replica.collection)?;
            }
            self.plane.report(req, now_ms())?;
            let (plan, _changed) = self.plane.reconcile(false, now_ms())?;
            self.publish_if_needed()?;
            Ok(Response::new(plan))
        })
        .await
    }

    async fn complete_placement_action(
        &self,
        request: Request<CompletePlacementActionRequest>,
    ) -> Result<Response<ClusterPlan>, Status> {
        crate::metrics::timed(
            Route::CompletePlacementAction,
            request,
            |request| async move {
                self.membership(&request)?;
                self.admit(&request.get_ref().collection)?;
                self.plane.complete_action(request.into_inner(), now_ms())?;
                let (plan, _changed) = self.plane.reconcile(false, now_ms())?;
                self.publish_if_needed()?;
                Ok(Response::new(plan))
            },
        )
        .await
    }

    async fn reconcile_cluster(
        &self,
        request: Request<ReconcileClusterRequest>,
    ) -> Result<Response<ClusterPlan>, Status> {
        crate::metrics::timed(Route::ReconcileCluster, request, |request| async move {
            self.membership(&request)?;
            self.admit(&request.get_ref().collection)?;
            let request = request.into_inner();
            let (plan, _changed) = self.plane.reconcile(request.dry_run, now_ms())?;
            if !request.dry_run {
                self.publish_if_needed()?;
            }
            Ok(Response::new(plan))
        })
        .await
    }

    async fn get_cluster_plan(
        &self,
        _request: Request<GetClusterPlanRequest>,
    ) -> Result<Response<ClusterPlan>, Status> {
        crate::metrics::timed(Route::GetClusterPlan, _request, |_request| async move {
            self.membership(&_request)?;
            self.admit(&_request.get_ref().collection)?;
            self.plane.plan().map(Response::new)
        })
        .await
    }

    /// Balance dry run (`docs/bandwidth-budget.md`): the provider's row
    /// geometry from the live shards' health and the placement pools
    /// from the coordinator, then the pure planner over the durable
    /// state. Cluster trust, like the other control routes.
    async fn plan_balance(
        &self,
        request: Request<crate::pb::PlanBalanceRequest>,
    ) -> Result<Response<crate::pb::PlanBalanceResponse>, Status> {
        crate::metrics::timed(Route::PlanBalance, request, |request| async move {
            self.membership(&request)?;
            self.admit(&request.get_ref().collection)?;
            let request = request.into_inner();
            let (row_bytes, pools) = self.balance_context(&request.collection).await?;
            let pool_of = |addr: &str| -> BalancePool {
                pools.get(bare_addr(addr)).cloned().unwrap_or_default()
            };
            let response = self
                .plane
                .plan_balance(&request, row_bytes, &pool_of, now_ms())?;
            Ok(Response::new(response))
        })
        .await
    }

    async fn rollback_cluster(
        &self,
        request: Request<RollbackClusterRequest>,
    ) -> Result<Response<ClusterPlan>, Status> {
        crate::metrics::timed(Route::RollbackCluster, request, |request| async move {
            self.membership(&request)?;
            self.admit(&request.get_ref().collection)?;
            let (plan, _changed) = self
                .plane
                .rollback(request.into_inner().topology_generation)?;
            self.publish_if_needed()?;
            Ok(Response::new(plan))
        })
        .await
    }
}
