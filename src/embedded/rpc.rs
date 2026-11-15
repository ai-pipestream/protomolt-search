//! Search and shard RPCs of an embedded runtime, delegated to the ordinary
//! coordinator and node handlers.

use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Status};

use super::{EmbeddedError, EmbeddedSearch};
use crate::pb::search_service_server::SearchService;
use crate::pb::*;

impl EmbeddedSearch {
    pub async fn query(&self, request: QueryRequest) -> Result<QueryResponse, Status> {
        SearchService::query(&self.coordinator, Request::new(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn query_stream(
        &self,
        request: QueryStreamRequest,
    ) -> Result<crate::metrics::Timed<ReceiverStream<Result<QueryStreamResponse, Status>>>, Status>
    {
        SearchService::query_stream(&self.coordinator, Request::new(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn plan_index(&self, request: PlanIndexRequest) -> Result<PlanIndexResponse, Status> {
        SearchService::plan_index(&self.coordinator, Request::new(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn describe_schema(
        &self,
        request: DescribeSchemaRequest,
    ) -> Result<DescribeSchemaResponse, Status> {
        SearchService::describe_schema(&self.coordinator, Request::new(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn search(&self, request: SearchRequest) -> Result<SearchResponse, Status> {
        SearchService::search(&self.coordinator, Request::new(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn bm25_search(
        &self,
        request: Bm25SearchRequest,
    ) -> Result<Bm25SearchResponse, Status> {
        SearchService::bm25_search(&self.coordinator, Request::new(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn phrase_search(
        &self,
        request: PhraseSearchRequest,
    ) -> Result<Bm25SearchResponse, Status> {
        SearchService::phrase_search(&self.coordinator, Request::new(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn hybrid_search(
        &self,
        request: HybridSearchRequest,
    ) -> Result<HybridSearchResponse, Status> {
        SearchService::hybrid_search(&self.coordinator, Request::new(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn variant_search(
        &self,
        request: VariantSearchRequest,
    ) -> Result<VariantSearchResponse, Status> {
        SearchService::variant_search(&self.coordinator, Request::new(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn aggregate(&self, request: AggregateRequest) -> Result<AggregateResponse, Status> {
        SearchService::aggregate(&self.coordinator, Request::new(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn cluster_health(&self) -> Result<ClusterHealthResponse, Status> {
        SearchService::cluster_health(
            &self.coordinator,
            Request::new(ClusterHealthRequest {
                collection: String::new(),
            }),
        )
        .await
        .map(tonic::Response::into_inner)
    }

    pub async fn broadcast_vector_backend(
        &self,
        request: BroadcastVectorBackendRequest,
    ) -> Result<BroadcastVectorBackendResponse, Status> {
        SearchService::broadcast_vector_backend(&self.coordinator, Request::new(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn configure_vector_backend(
        &self,
        shard: usize,
        request: ConfigureVectorBackendRequest,
    ) -> Result<ConfigureVectorBackendResponse, EmbeddedError> {
        Ok(self
            .shard_client(shard)?
            .configure_vector_backend(request)
            .await?
            .into_inner())
    }

    pub async fn add_vectors(
        &self,
        shard: usize,
        requests: Vec<AddVectorsRequest>,
    ) -> Result<AddVectorsResponse, EmbeddedError> {
        Ok(self
            .shard_client(shard)?
            .add_vectors(tokio_stream::iter(requests))
            .await?
            .into_inner())
    }

    pub async fn add_documents(
        &self,
        shard: usize,
        requests: Vec<AddDocumentsRequest>,
    ) -> Result<AddDocumentsResponse, EmbeddedError> {
        Ok(self
            .shard_client(shard)?
            .add_documents(tokio_stream::iter(requests))
            .await?
            .into_inner())
    }

    pub async fn ingest_mapped(
        &self,
        shard: usize,
        requests: Vec<IngestMappedRequest>,
    ) -> Result<IngestMappedResponse, EmbeddedError> {
        Ok(self
            .shard_client(shard)?
            .ingest_mapped(tokio_stream::iter(requests))
            .await?
            .into_inner())
    }

    pub async fn delete_documents(
        &self,
        shard: usize,
        request: DeleteDocumentsRequest,
    ) -> Result<DeleteDocumentsResponse, EmbeddedError> {
        Ok(self
            .shard_client(shard)?
            .delete_documents(request)
            .await?
            .into_inner())
    }

    pub async fn commit_replacements(
        &self,
        shard: usize,
        request: CommitReplacementsRequest,
    ) -> Result<CommitReplacementsResponse, EmbeddedError> {
        Ok(self
            .shard_client(shard)?
            .commit_replacements(request)
            .await?
            .into_inner())
    }

    pub async fn shard_health(&self, shard: usize) -> Result<HealthResponse, EmbeddedError> {
        Ok(self
            .shard_client(shard)?
            .health(HealthRequest {})
            .await?
            .into_inner())
    }

    pub async fn flush_shard(&self, shard: usize) -> Result<FlushResponse, EmbeddedError> {
        Ok(self
            .shard_client(shard)?
            .flush(FlushRequest {})
            .await?
            .into_inner())
    }

    pub async fn flush_all(&self) -> Result<Vec<FlushResponse>, EmbeddedError> {
        let mut responses = Vec::with_capacity(self.shard_count());
        for shard in 0..self.shard_count() {
            responses.push(self.flush_shard(shard).await?);
        }
        Ok(responses)
    }
}
