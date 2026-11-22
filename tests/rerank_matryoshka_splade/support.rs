use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::pb::node_service_server::{NodeService, NodeServiceServer};
use pipestream_search::pb::*;
use pipestream_search::postings::{AnalyzedDoc, AnalyzedField, Bm25Store};
use tokio::sync::{mpsc, oneshot, Barrier};
use tonic::{Request, Response, Status, Streaming};

#[path = "../common/scratch.rs"]
pub mod scratch;

pub struct ProxyNode {
    pub inner: NodeServiceImpl,
    pub get_docs_calls: Arc<AtomicUsize>,
    pub splade_barrier: Option<Arc<Barrier>>,
    pub splade_hold: Option<mpsc::UnboundedSender<oneshot::Receiver<()>>>,
}

impl ProxyNode {
    pub fn new(inner: NodeServiceImpl) -> Self {
        Self {
            inner,
            get_docs_calls: Arc::new(AtomicUsize::new(0)),
            splade_barrier: None,
            splade_hold: None,
        }
    }

    pub async fn serve(
        self,
    ) -> (
        String,
        tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        let server = NodeServiceServer::new(self)
            .max_decoding_message_size(pipestream_search::MAX_MESSAGE_BYTES)
            .max_encoding_message_size(pipestream_search::MAX_MESSAGE_BYTES);
        let handle = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(server)
                .serve_with_incoming(pipestream_search::harness::nodelay_incoming(listener))
                .await
        });
        (addr, handle)
    }
}

macro_rules! delegate_rpc {
    ($($method:ident($req:ty) -> $resp:ty),+ $(,)?) => {
        $(
            fn $method<'life0, 'async_trait>(
                &'life0 self,
                request: Request<$req>,
            ) -> core::pin::Pin<
                Box<
                    dyn core::future::Future<Output = Result<Response<$resp>, Status>>
                        + Send
                        + 'async_trait,
                >,
            >
            where
                'life0: 'async_trait,
                Self: 'async_trait,
            {
                NodeService::$method(&self.inner, request)
            }
        )+
    };
}

#[tonic::async_trait]
impl NodeService for ProxyNode {
    type SearchShardStream = <NodeServiceImpl as NodeService>::SearchShardStream;
    type StreamSearchStream = <NodeServiceImpl as NodeService>::StreamSearchStream;
    type Bm25QueryStreamStream = <NodeServiceImpl as NodeService>::Bm25QueryStreamStream;
    type ReadWalStream = <NodeServiceImpl as NodeService>::ReadWalStream;
    type StreamSnapshotStream = <NodeServiceImpl as NodeService>::StreamSnapshotStream;

    async fn get_documents(
        &self,
        request: Request<GetDocumentsRequest>,
    ) -> Result<Response<GetDocumentsResponse>, Status> {
        self.get_docs_calls.fetch_add(1, Ordering::AcqRel);
        NodeService::get_documents(&self.inner, request).await
    }

    async fn score_splade(
        &self,
        request: Request<ScoreSpladeRequest>,
    ) -> Result<Response<ScoreSpladeResponse>, Status> {
        if let Some(hold_tx) = &self.splade_hold {
            let (_drop_tx, drop_rx) = oneshot::channel::<()>();
            let _ = hold_tx.send(drop_rx);
            std::future::pending::<()>().await;
        }
        if let Some(barrier) = &self.splade_barrier {
            barrier.wait().await;
        }
        NodeService::score_splade(&self.inner, request).await
    }

    delegate_rpc! {
        search_shard(Streaming<SearchShardRequest>) -> Self::SearchShardStream,
        browse_shard(BrowseShardRequest) -> BrowseShardResponse,
        resolve_filter_bitmap(FilterBitmapRequest) -> FilterBitmapResponse,
        resolve_lexical_bitmap(LexicalBitmapRequest) -> MembershipBitmapResponse,
        resolve_vector_bitmap(VectorBitmapRequest) -> MembershipBitmapResponse,
        evaluate_boolean(BooleanShardRequest) -> BooleanShardResponse,
        aggregate_shard(AggregateShardRequest) -> AggregateShardResponse,
        quantile_counts(QuantileCountsRequest) -> QuantileCountsResponse,
        read_wal(ReadWalRequest) -> Self::ReadWalStream,
        apply_wal_binding(ApplyWalBindingRequest) -> ApplyWalBindingResponse,
        compact_shard(CompactShardRequest) -> CompactShardResponse,
        health(HealthRequest) -> HealthResponse,
        stream_search(Streaming<StreamSearchRequest>) -> Self::StreamSearchStream,
        get_vector_backend(GetVectorBackendRequest) -> GetVectorBackendResponse,
        configure_vector_backend(ConfigureVectorBackendRequest) -> ConfigureVectorBackendResponse,
        add_vectors(Streaming<AddVectorsRequest>) -> AddVectorsResponse,
        flush(FlushRequest) -> FlushResponse,
        install_snapshot(Streaming<SnapshotChunk>) -> InstallSnapshotResponse,
        export_snapshot(ExportSnapshotRequest) -> ExportSnapshotResponse,
        stream_snapshot(StreamSnapshotRequest) -> Self::StreamSnapshotStream,
        install_snapshot_from(InstallSnapshotFromRequest) -> InstallSnapshotResponse,
        add_documents(Streaming<AddDocumentsRequest>) -> AddDocumentsResponse,
        delete_documents(DeleteDocumentsRequest) -> DeleteDocumentsResponse,
        commit_replacements(CommitReplacementsRequest) -> CommitReplacementsResponse,
        ingest_mapped(Streaming<IngestMappedRequest>) -> IngestMappedResponse,
        term_stats(TermStatsRequest) -> TermStatsResponse,
        expand_term_prefix(ExpandTermPrefixRequest) -> ExpandTermPrefixResponse,
        suggest_terms(SuggestTermsRequest) -> SuggestTermsResponse,
        bm25_query(Bm25QueryRequest) -> Bm25QueryResponse,
        bm25_phrase_query(Bm25PhraseQueryRequest) -> Bm25QueryResponse,
        bm25_query_stream(Streaming<Bm25QueryStreamRequest>) -> Self::Bm25QueryStreamStream,
        bm25_rescore(Bm25RescoreRequest) -> Bm25RescoreResponse,
        fetch_values(FetchValuesRequest) -> FetchValuesResponse,
        vector_rescore(VectorRescoreRequest) -> VectorRescoreResponse,
        exact_vector_rescore(ExactVectorRescoreRequest) -> ExactVectorRescoreResponse,
        resolve_parents(ResolveParentsRequest) -> ResolveParentsResponse,
        hybrid_shard(HybridShardRequest) -> HybridShardResponse,
        shard_legs(ShardLegsRequest) -> ShardLegsResponse,
        rescore_matryoshka(RescoreMatryoshkaRequest) -> RescoreMatryoshkaResponse,
    }
}

pub fn build_splade_node(
    slot_offset: u64,
    impact: u16,
    scale: f32,
) -> (NodeServiceImpl, scratch::ScratchPath) {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let root = scratch::scratch_path(&format!(
        "splade-fanout-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&root).unwrap();
    let idx_path = root.join("index");
    let stage = root.join("stage");
    std::fs::create_dir_all(&stage).unwrap();
    let bm25_path = stage.join("source.bm25");
    let mut store = Bm25Store::new();
    for local in 0..2 {
        store.add_document(
            local,
            format!("shard-{slot_offset}-doc-{local}"),
            AnalyzedDoc {
                fields: vec![AnalyzedField {
                    terms: vec![("lithium".into(), 1, Vec::new())],
                    length: 1,
                    splade: vec![("lithium".into(), impact + local as u16)],
                    splade_scale_bits: scale.to_bits(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
    }
    store.save(&bm25_path).unwrap();
    super::common::catalog::seal_bm25_file(&idx_path, &bm25_path);
    std::fs::remove_dir_all(&stage).unwrap();
    let node = NodeServiceImpl::open(
        NodeConfig {
            slot_offset,
            index_path: Some(idx_path),
            ..Default::default()
        },
        None,
    )
    .unwrap();
    (node, root)
}

pub fn remote_rerank_spec(addr: &str) -> pipestream_search::turbo_bridge::rerank::RerankSpec {
    use pipestream_search::turbo_bridge::rerank::{RerankBackend, RerankSpec};
    let client =
        turbo_client::RemoteClient::new(turbo_client::RemoteEndpointConfig::new(addr.to_string()));
    let mut spec = RerankSpec::new(RerankBackend::Remote {
        client: Box::new(client),
        rerank_model: "bge-reranker-v2-m3".into(),
        embed_model: String::new(),
        qa_model: String::new(),
        temperature: 1.0,
    });
    spec.shortlist_size = 4;
    spec.highlight_top_k = 4;
    spec.compose_parent_lineage_offsets = true;
    spec
}

pub const DIM: usize = 16;

pub fn hit_bits(hits: &[QueryHit]) -> Vec<(u64, u32)> {
    hits.iter().map(|h| (h.doc_id, h.score.to_bits())).collect()
}

pub fn dense_req(
    request_id: &str,
    vector: &[f32],
    query_text: &str,
    highlight: bool,
) -> QueryRequest {
    QueryRequest {
        request_id: request_id.into(),
        k: 2,
        selection_k: 2,
        highlight: highlight.then(HighlightSpec::default),
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "vec".into(),
                query: Some(search_query::Query::Dense(DenseQuery {
                    vector: vector.to_vec(),
                    query_text: query_text.into(),
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    }
}

pub async fn seed_rerank_node(
    analysis_addr: &str,
    docs: &[&str],
    vecs: &[f32],
) -> Arc<NodeServiceImpl> {
    let (shift, scale) = super::common::fit_calibration(DIM, 4, vecs);
    let node = Arc::new(NodeServiceImpl::new(
        None,
        NodeConfig {
            analysis_addr: Some(analysis_addr.to_string()),
            sentence_fields: vec!["body".into()],
            ..Default::default()
        },
    ));
    let mut link = pipestream_search::link::NodeLink::local(Arc::clone(&node));
    link.configure_vector_backend(pipestream_search::harness::embedded_backend_request(
        DIM, 4, &shift, &scale,
    ))
    .await
    .unwrap();
    if !docs.is_empty() {
        let reqs: Vec<_> = docs
            .iter()
            .map(|&text| AddDocumentsRequest {
                text: text.into(),
                analysis: Some(pipestream_search::analyzer::body_spec()),
                lineage: Some(DocLineage {
                    parent_id: 90,
                    group_id: 1,
                    span_start: 300,
                    span_end: 300 + text.len() as u32,
                }),
                ..Default::default()
            })
            .collect();
        link.add_documents(tokio_stream::iter(reqs)).await.unwrap();
    }
    link.add_vectors(tokio_stream::iter([AddVectorsRequest {
        vectors: vecs.to_vec(),
        dim: DIM as u32,
        ..Default::default()
    }]))
    .await
    .unwrap();
    node
}

pub async fn matryoshka_shard(
    slot_offset: u64,
    raw_vectors: Vec<f32>,
    delete_local: Option<u64>,
) -> Arc<NodeServiceImpl> {
    let cal = super::common::unit_vectors(8, 8, 0x2470_0099);
    let (shift, scale) = super::common::fit_calibration(8, 4, &cal);
    let node = Arc::new(NodeServiceImpl::new(
        None,
        NodeConfig {
            slot_offset,
            matryoshka_prefix_dims: vec![8, 16],
            matryoshka_active_dim: Some(8),
            ..Default::default()
        },
    ));
    let mut link = pipestream_search::link::NodeLink::local(Arc::clone(&node));
    link.configure_vector_backend(pipestream_search::harness::embedded_backend_request(
        8, 4, &shift, &scale,
    ))
    .await
    .unwrap();
    link.add_vectors(tokio_stream::iter([AddVectorsRequest {
        vectors: raw_vectors,
        dim: 16,
        ..Default::default()
    }]))
    .await
    .unwrap();
    if let Some(local) = delete_local {
        link.delete_documents(DeleteDocumentsRequest {
            doc_ids: vec![slot_offset + local],
            ..Default::default()
        })
        .await
        .unwrap();
    }
    node
}
