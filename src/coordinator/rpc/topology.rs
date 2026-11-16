//! Topology cutover handlers: write freeze, publish and abort.

use super::*;

impl CoordinatorServiceImpl {
    pub(in crate::coordinator) async fn rpc_freeze_topology_writes(
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

    pub(in crate::coordinator) async fn rpc_publish_topology(
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

    pub(in crate::coordinator) async fn rpc_abort_topology_cutover(
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
