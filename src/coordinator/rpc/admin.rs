//! Schema planning, routed ingest, aggregate and backend broadcast handlers.

use super::*;

impl CoordinatorServiceImpl {
    pub(in crate::coordinator) async fn rpc_plan_index(
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
            // Plan derivation is local and deterministic across coordinators.
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

    pub(in crate::coordinator) async fn rpc_describe_schema(
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

    pub(in crate::coordinator) async fn rpc_routed_ingest_mapped(
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

    /// Compute exact aggregates over the filtered corpus, compiling CEL filter
    /// and `ValueExpr` expressions once at the coordinator.
    pub(in crate::coordinator) async fn rpc_aggregate(
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

    pub(in crate::coordinator) async fn rpc_broadcast_vector_backend(
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
}
