//! SearchService transport delegation and administrative handlers.

use super::*;

mod admin;
mod health;
mod placement;
mod topology;
mod variants;

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
