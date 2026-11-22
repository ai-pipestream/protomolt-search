//! [`NodeService`] gRPC trait implementation delegating to domain handlers.

mod admin_ingest;
mod boolean_agg;
mod browse_values;
mod lexical;
mod vector_search;

use crate::node::*;

#[tonic::async_trait]
impl NodeService for NodeServiceImpl {
    type SearchShardStream =
        crate::metrics::Timed<ReceiverStream<Result<SearchShardResponse, Status>>>;
    type StreamSearchStream =
        crate::metrics::Timed<ReceiverStream<Result<StreamSearchResponse, Status>>>;
    type Bm25QueryStreamStream =
        crate::metrics::Timed<ReceiverStream<Result<Bm25QueryStreamResponse, Status>>>;
    type ReadWalStream = crate::metrics::Timed<ReceiverStream<Result<ReadWalResponse, Status>>>;
    type StreamSnapshotStream =
        crate::metrics::Timed<ReceiverStream<Result<SnapshotChunk, Status>>>;

    async fn search_shard(
        &self,
        request: Request<Streaming<SearchShardRequest>>,
    ) -> Result<Response<Self::SearchShardStream>, Status> {
        self.handle_search_shard(request).await
    }

    /// Filter-only browse: admitted ids above the floor, ascending, at
    /// most k, or ordered by a column under `sort`. Match-all over the DOCUMENT space (bm25 postings), gated
    /// by the same resolved filter every scored route admits through;
    /// no scoring, no heap — the order is the id order.
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
        request: Request<LexicalBitmapRequest>,
    ) -> Result<Response<MembershipBitmapResponse>, Status> {
        self.handle_resolve_lexical_bitmap(request).await
    }

    async fn resolve_vector_bitmap(
        &self,
        request: Request<VectorBitmapRequest>,
    ) -> Result<Response<MembershipBitmapResponse>, Status> {
        self.handle_resolve_vector_bitmap(request).await
    }

    async fn evaluate_boolean(
        &self,
        request: Request<crate::pb::BooleanShardRequest>,
    ) -> Result<Response<crate::pb::BooleanShardResponse>, Status> {
        self.handle_evaluate_boolean(request).await
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

    async fn read_wal(
        &self,
        request: Request<ReadWalRequest>,
    ) -> Result<Response<Self::ReadWalStream>, Status> {
        self.handle_read_wal(request).await
    }

    async fn apply_wal_binding(
        &self,
        request: Request<crate::pb::ApplyWalBindingRequest>,
    ) -> Result<Response<crate::pb::ApplyWalBindingResponse>, Status> {
        self.handle_apply_wal_binding(request).await
    }

    async fn compact_shard(
        &self,
        request: Request<crate::pb::CompactShardRequest>,
    ) -> Result<Response<crate::pb::CompactShardResponse>, Status> {
        self.handle_compact_shard(request).await
    }

    async fn health(
        &self,
        request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        self.handle_health(request).await
    }

    async fn stream_search(
        &self,
        request: Request<Streaming<StreamSearchRequest>>,
    ) -> Result<Response<Self::StreamSearchStream>, Status> {
        self.handle_stream_search(request).await
    }

    async fn get_vector_backend(
        &self,
        request: Request<GetVectorBackendRequest>,
    ) -> Result<Response<GetVectorBackendResponse>, Status> {
        self.handle_get_vector_backend(request).await
    }

    async fn configure_vector_backend(
        &self,
        request: Request<ConfigureVectorBackendRequest>,
    ) -> Result<Response<ConfigureVectorBackendResponse>, Status> {
        self.handle_configure_vector_backend(request).await
    }

    async fn add_vectors(
        &self,
        request: Request<Streaming<AddVectorsRequest>>,
    ) -> Result<Response<AddVectorsResponse>, Status> {
        self.handle_add_vectors(request).await
    }

    async fn flush(
        &self,
        request: Request<FlushRequest>,
    ) -> Result<Response<FlushResponse>, Status> {
        self.handle_flush(request).await
    }

    async fn install_snapshot(
        &self,
        request: Request<Streaming<SnapshotChunk>>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        self.handle_install_snapshot(request).await
    }

    async fn export_snapshot(
        &self,
        request: Request<ExportSnapshotRequest>,
    ) -> Result<Response<ExportSnapshotResponse>, Status> {
        self.handle_export_snapshot(request).await
    }

    async fn stream_snapshot(
        &self,
        request: Request<StreamSnapshotRequest>,
    ) -> Result<Response<Self::StreamSnapshotStream>, Status> {
        self.handle_stream_snapshot(request).await
    }

    async fn install_snapshot_from(
        &self,
        request: Request<InstallSnapshotFromRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        self.handle_install_snapshot_from(request).await
    }

    async fn add_documents(
        &self,
        request: Request<Streaming<AddDocumentsRequest>>,
    ) -> Result<Response<AddDocumentsResponse>, Status> {
        self.handle_add_documents(request).await
    }

    async fn delete_documents(
        &self,
        request: Request<DeleteDocumentsRequest>,
    ) -> Result<Response<DeleteDocumentsResponse>, Status> {
        self.handle_delete_documents(request).await
    }

    async fn commit_replacements(
        &self,
        request: Request<CommitReplacementsRequest>,
    ) -> Result<Response<CommitReplacementsResponse>, Status> {
        self.handle_commit_replacements(request).await
    }

    async fn ingest_mapped(
        &self,
        request: Request<Streaming<IngestMappedRequest>>,
    ) -> Result<Response<IngestMappedResponse>, Status> {
        self.handle_ingest_mapped(request).await
    }

    async fn term_stats(
        &self,
        request: Request<TermStatsRequest>,
    ) -> Result<Response<TermStatsResponse>, Status> {
        self.handle_term_stats(request).await
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
        request: Request<crate::pb::Bm25PhraseQueryRequest>,
    ) -> Result<Response<Bm25QueryResponse>, Status> {
        self.handle_bm25_phrase_query(request).await
    }

    async fn bm25_query_stream(
        &self,
        request: Request<Streaming<Bm25QueryStreamRequest>>,
    ) -> Result<Response<Self::Bm25QueryStreamStream>, Status> {
        self.handle_bm25_query_stream(request).await
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

    async fn vector_rescore(
        &self,
        request: Request<VectorRescoreRequest>,
    ) -> Result<Response<VectorRescoreResponse>, Status> {
        self.handle_vector_rescore(request).await
    }

    async fn exact_vector_rescore(
        &self,
        request: Request<ExactVectorRescoreRequest>,
    ) -> Result<Response<ExactVectorRescoreResponse>, Status> {
        self.handle_exact_vector_rescore(request).await
    }

    async fn get_documents(
        &self,
        request: Request<GetDocumentsRequest>,
    ) -> Result<Response<GetDocumentsResponse>, Status> {
        self.handle_get_documents(request).await
    }

    async fn resolve_parents(
        &self,
        request: Request<ResolveParentsRequest>,
    ) -> Result<Response<ResolveParentsResponse>, Status> {
        self.handle_resolve_parents(request).await
    }

    async fn hybrid_shard(
        &self,
        request: Request<HybridShardRequest>,
    ) -> Result<Response<HybridShardResponse>, Status> {
        self.handle_hybrid_shard(request).await
    }

    async fn shard_legs(
        &self,
        request: Request<ShardLegsRequest>,
    ) -> Result<Response<ShardLegsResponse>, Status> {
        self.handle_shard_legs(request).await
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
