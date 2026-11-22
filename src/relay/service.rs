//! The `NodeService` surface of a relay: composed routes delegate, the rest refuse.

use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::pb::node_service_server::NodeService;
use crate::pb::{
    Bm25PhraseQueryRequest, Bm25QueryRequest, Bm25QueryResponse, Bm25QueryStreamRequest,
    Bm25QueryStreamResponse, Bm25RescoreRequest, Bm25RescoreResponse, HealthRequest,
    HealthResponse, SearchShardRequest, SearchShardResponse, ShardLegsRequest, ShardLegsResponse,
    StreamSearchRequest, StreamSearchResponse, TermStatsRequest, TermStatsResponse,
};

use super::{refused, RelayService};

#[tonic::async_trait]

impl NodeService for RelayService {
    type SearchShardStream =
        crate::metrics::Timed<ReceiverStream<Result<SearchShardResponse, Status>>>;
    type StreamSearchStream =
        crate::metrics::Timed<ReceiverStream<Result<StreamSearchResponse, Status>>>;
    type Bm25QueryStreamStream =
        crate::metrics::Timed<ReceiverStream<Result<Bm25QueryStreamResponse, Status>>>;
    type ReadWalStream = ReceiverStream<Result<crate::pb::ReadWalResponse, Status>>;
    type StreamSnapshotStream = ReceiverStream<Result<crate::pb::SnapshotChunk, Status>>;

    async fn stream_search(
        &self,
        request: Request<Streaming<StreamSearchRequest>>,
    ) -> Result<Response<Self::StreamSearchStream>, Status> {
        self.handle_stream_search(request).await
    }

    async fn term_stats(
        &self,
        request: Request<TermStatsRequest>,
    ) -> Result<Response<TermStatsResponse>, Status> {
        self.handle_term_stats(request).await
    }

    async fn health(
        &self,
        request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        self.handle_health(request).await
    }

    async fn search_shard(
        &self,
        request: Request<Streaming<SearchShardRequest>>,
    ) -> Result<Response<Self::SearchShardStream>, Status> {
        self.handle_search_shard(request).await
    }

    async fn add_vectors(
        &self,
        _request: Request<Streaming<crate::pb::AddVectorsRequest>>,
    ) -> Result<Response<crate::pb::AddVectorsResponse>, Status> {
        Err(refused("AddVectors"))
    }

    async fn install_snapshot(
        &self,
        _request: Request<Streaming<crate::pb::SnapshotChunk>>,
    ) -> Result<Response<crate::pb::InstallSnapshotResponse>, Status> {
        Err(refused("InstallSnapshot"))
    }

    async fn stream_snapshot(
        &self,
        _request: Request<crate::pb::StreamSnapshotRequest>,
    ) -> Result<Response<Self::StreamSnapshotStream>, Status> {
        Err(refused("StreamSnapshot"))
    }

    async fn add_documents(
        &self,
        _request: Request<Streaming<crate::pb::AddDocumentsRequest>>,
    ) -> Result<Response<crate::pb::AddDocumentsResponse>, Status> {
        Err(refused("AddDocuments"))
    }

    async fn ingest_mapped(
        &self,
        _request: Request<Streaming<crate::pb::IngestMappedRequest>>,
    ) -> Result<Response<crate::pb::IngestMappedResponse>, Status> {
        Err(refused("IngestMapped"))
    }

    async fn bm25_query_stream(
        &self,
        request: Request<Streaming<Bm25QueryStreamRequest>>,
    ) -> Result<Response<Self::Bm25QueryStreamStream>, Status> {
        self.handle_bm25_query_stream(request).await
    }

    async fn read_wal(
        &self,
        _request: Request<crate::pb::ReadWalRequest>,
    ) -> Result<Response<Self::ReadWalStream>, Status> {
        Err(refused("ReadWal"))
    }

    async fn get_vector_backend(
        &self,
        request: Request<crate::pb::GetVectorBackendRequest>,
    ) -> Result<Response<crate::pb::GetVectorBackendResponse>, Status> {
        self.handle_get_vector_backend(request).await
    }

    async fn configure_vector_backend(
        &self,
        _request: Request<crate::pb::ConfigureVectorBackendRequest>,
    ) -> Result<Response<crate::pb::ConfigureVectorBackendResponse>, Status> {
        Err(refused("ConfigureVectorBackend"))
    }

    async fn flush(
        &self,
        _request: Request<crate::pb::FlushRequest>,
    ) -> Result<Response<crate::pb::FlushResponse>, Status> {
        Err(refused("Flush"))
    }

    async fn export_snapshot(
        &self,
        _request: Request<crate::pb::ExportSnapshotRequest>,
    ) -> Result<Response<crate::pb::ExportSnapshotResponse>, Status> {
        Err(refused("ExportSnapshot"))
    }

    async fn install_snapshot_from(
        &self,
        _request: Request<crate::pb::InstallSnapshotFromRequest>,
    ) -> Result<Response<crate::pb::InstallSnapshotResponse>, Status> {
        Err(refused("InstallSnapshotFrom"))
    }

    async fn delete_documents(
        &self,
        _request: Request<crate::pb::DeleteDocumentsRequest>,
    ) -> Result<Response<crate::pb::DeleteDocumentsResponse>, Status> {
        Err(refused("DeleteDocuments"))
    }

    async fn commit_replacements(
        &self,
        _request: Request<crate::pb::CommitReplacementsRequest>,
    ) -> Result<Response<crate::pb::CommitReplacementsResponse>, Status> {
        Err(refused("CommitReplacements"))
    }

    async fn expand_term_prefix(
        &self,
        request: Request<crate::pb::ExpandTermPrefixRequest>,
    ) -> Result<Response<crate::pb::ExpandTermPrefixResponse>, Status> {
        self.handle_expand_term_prefix(request).await
    }

    async fn suggest_terms(
        &self,
        request: Request<crate::pb::SuggestTermsRequest>,
    ) -> Result<Response<crate::pb::SuggestTermsResponse>, Status> {
        self.handle_suggest_terms(request).await
    }

    async fn bm25_query(
        &self,
        request: Request<Bm25QueryRequest>,
    ) -> Result<Response<Bm25QueryResponse>, Status> {
        self.handle_bm25_query(request).await
    }

    async fn bm25_phrase_query(
        &self,
        request: Request<Bm25PhraseQueryRequest>,
    ) -> Result<Response<Bm25QueryResponse>, Status> {
        self.handle_bm25_phrase_query(request).await
    }

    async fn get_documents(
        &self,
        request: Request<crate::pb::GetDocumentsRequest>,
    ) -> Result<Response<crate::pb::GetDocumentsResponse>, Status> {
        self.handle_get_documents(request).await
    }

    async fn resolve_parents(
        &self,
        request: Request<crate::pb::ResolveParentsRequest>,
    ) -> Result<Response<crate::pb::ResolveParentsResponse>, Status> {
        self.handle_resolve_parents(request).await
    }

    async fn bm25_rescore(
        &self,
        request: Request<Bm25RescoreRequest>,
    ) -> Result<Response<Bm25RescoreResponse>, Status> {
        self.handle_bm25_rescore(request).await
    }

    async fn fetch_values(
        &self,
        request: Request<crate::pb::FetchValuesRequest>,
    ) -> Result<Response<crate::pb::FetchValuesResponse>, Status> {
        self.handle_fetch_values(request).await
    }

    async fn aggregate_shard(
        &self,
        request: Request<crate::pb::AggregateShardRequest>,
    ) -> Result<Response<crate::pb::AggregateShardResponse>, Status> {
        self.handle_aggregate_shard(request).await
    }

    async fn quantile_counts(
        &self,
        request: Request<crate::pb::QuantileCountsRequest>,
    ) -> Result<Response<crate::pb::QuantileCountsResponse>, Status> {
        self.handle_quantile_counts(request).await
    }

    async fn vector_rescore(
        &self,
        request: Request<crate::pb::VectorRescoreRequest>,
    ) -> Result<Response<crate::pb::VectorRescoreResponse>, Status> {
        self.handle_vector_rescore(request).await
    }

    async fn exact_vector_rescore(
        &self,
        request: Request<crate::pb::ExactVectorRescoreRequest>,
    ) -> Result<Response<crate::pb::ExactVectorRescoreResponse>, Status> {
        self.handle_exact_vector_rescore(request).await
    }

    async fn hybrid_shard(
        &self,
        _request: Request<crate::pb::HybridShardRequest>,
    ) -> Result<Response<crate::pb::HybridShardResponse>, Status> {
        self.handle_hybrid_shard(_request).await
    }

    async fn shard_legs(
        &self,
        request: Request<ShardLegsRequest>,
    ) -> Result<Response<ShardLegsResponse>, Status> {
        self.handle_shard_legs(request).await
    }

    async fn browse_shard(
        &self,
        request: Request<crate::pb::BrowseShardRequest>,
    ) -> Result<Response<crate::pb::BrowseShardResponse>, Status> {
        self.handle_browse_shard(request).await
    }

    async fn resolve_filter_bitmap(
        &self,
        request: Request<crate::pb::FilterBitmapRequest>,
    ) -> Result<Response<crate::pb::FilterBitmapResponse>, Status> {
        self.handle_resolve_filter_bitmap(request).await
    }

    async fn resolve_lexical_bitmap(
        &self,
        request: Request<crate::pb::LexicalBitmapRequest>,
    ) -> Result<Response<crate::pb::MembershipBitmapResponse>, Status> {
        self.handle_resolve_lexical_bitmap(request).await
    }

    async fn resolve_vector_bitmap(
        &self,
        request: Request<crate::pb::VectorBitmapRequest>,
    ) -> Result<Response<crate::pb::MembershipBitmapResponse>, Status> {
        self.handle_resolve_vector_bitmap(request).await
    }

    async fn evaluate_boolean(
        &self,
        request: Request<crate::pb::BooleanShardRequest>,
    ) -> Result<Response<crate::pb::BooleanShardResponse>, Status> {
        self.handle_evaluate_boolean(request).await
    }

    async fn apply_wal_binding(
        &self,
        _request: Request<crate::pb::ApplyWalBindingRequest>,
    ) -> Result<Response<crate::pb::ApplyWalBindingResponse>, Status> {
        Err(refused("ApplyWalBinding"))
    }

    async fn compact_shard(
        &self,
        _request: Request<crate::pb::CompactShardRequest>,
    ) -> Result<Response<crate::pb::CompactShardResponse>, Status> {
        Err(refused("CompactShard"))
    }

    async fn score_splade(
        &self,
        request: Request<crate::pb::ScoreSpladeRequest>,
    ) -> Result<Response<crate::pb::ScoreSpladeResponse>, Status> {
        self.handle_score_splade(request).await
    }

    async fn rescore_matryoshka(
        &self,
        request: Request<crate::pb::RescoreMatryoshkaRequest>,
    ) -> Result<Response<crate::pb::RescoreMatryoshkaResponse>, Status> {
        self.handle_rescore_matryoshka(request).await
    }
}
