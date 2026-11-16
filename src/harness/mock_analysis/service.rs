//! The mock's `AnalysisService`: unary and streamed analysis, the
//! reranker, and the round trips; everything else answers UNIMPLEMENTED.

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

use turbo_client::proto::result::analysis_service_server::AnalysisService;
use turbo_client::proto::result::{
    analyze_progressive_event, analyze_stream_request, analyze_stream_response, AnalyzeDocument,
    AnalyzePipelineRequest, AnalyzeProgressiveEvent, AnalyzeQueryRequest, AnalyzeQueryResponse,
    AnalyzeStreamRequest, AnalyzeStreamResponse, CapabilitiesRequest, CapabilitiesResponse,
    ChunkLevelsResult, DocumentGraphRequest, DocumentGraphResponse, DocumentWindowRequest,
    DocumentWindowResponse, ExtractionResult, FormatDocumentRequest, FormatDocumentResponse,
    InspectDocumentGraphRequest, ListOutputFormatsRequest, ListOutputFormatsResponse,
    LoadBundleRequest, LoadBundleResponse, MultiResVectorsResult, ProcessingReceipt,
    ProfileCapabilities, ProjectChunksRequest, ProjectChunksResponse, ProjectSearchDocumentRequest,
    RecordsResult, RerankAndHighlightRequest, RerankAndHighlightResponse, RerankScoresResult,
    SearchDocumentProjection, SparseTermsResult, TokenVectorsResult, TypedDocumentResult,
    UnloadBundleRequest, UnloadBundleResponse,
};

use crate::analyzer::wire::{
    analyze_failure_from_status, WireSessionOptions, NO_DUAL_CASED_PROFILE,
};

use super::analyze::{analyze_text, MockLayers};
use super::{rerank, DelayedCall, Lane, MockAnalysis, PriorityLanes};

impl MockAnalysis {
    fn layers(&self) -> MockLayers {
        MockLayers {
            ner: self.ner,
            sentence_layer: self.sentence_layer,
            dual_identity: self.dual_identity,
        }
    }
}

type StreamTx = tokio::sync::mpsc::Sender<Result<AnalyzeStreamResponse, Status>>;

/// One `AnalyzeStream` session: a header first, then documents answered
/// in pairs out of order (each later one before the one it held), the
/// last released at the end, then the optional end gate.
struct StreamSession {
    layers: MockLayers,
    calls: Arc<AtomicU64>,
    gate: Option<Arc<tokio::sync::Semaphore>>,
    /// The accelerator slot each document takes a unit of, in the lane the
    /// stream opened under.
    lanes: Option<(Arc<PriorityLanes>, Lane)>,
    tx: StreamTx,
}

impl StreamSession {
    async fn refuse(&self, message: &'static str) {
        let _ = self.tx.send(Err(Status::invalid_argument(message))).await;
    }

    fn answer(&self, doc: AnalyzeDocument) -> AnalyzeStreamResponse {
        let outcome = match std::str::from_utf8(&doc.text)
            .map_err(|_| Status::invalid_argument("document text is not UTF-8"))
            .and_then(|text| {
                let session = WireSessionOptions::from_binding(doc.binding.as_ref())?;
                let ok = analyze_text(text, &session, self.layers, &self.calls)?;
                Ok(DocumentGraphResponse {
                    graph: ok.encode_graph().into(),
                    source_text: text.to_string(),
                    ..Default::default()
                })
            }) {
            Ok(graph) => analyze_stream_response::Outcome::DocumentGraph(graph),
            Err(status) => {
                analyze_stream_response::Outcome::Failure(analyze_failure_from_status(status))
            }
        };
        AnalyzeStreamResponse {
            id: doc.id,
            outcome: Some(outcome),
            binding: doc.binding,
        }
    }

    async fn run(self, mut inbound: Streaming<AnalyzeStreamRequest>) {
        let mut saw_header = false;
        let mut held: Option<AnalyzeStreamResponse> = None;
        loop {
            let message = match inbound.message().await {
                Ok(Some(message)) => message,
                Ok(None) => break,
                Err(_) => return,
            };
            match message.kind {
                Some(analyze_stream_request::Kind::Header(_)) if saw_header => {
                    return self
                        .refuse("header may only be the first message of the stream")
                        .await;
                }
                Some(analyze_stream_request::Kind::Header(_)) => saw_header = true,
                Some(analyze_stream_request::Kind::Document(_)) if !saw_header => {
                    return self
                        .refuse("the first message of the stream must carry a header")
                        .await;
                }
                Some(analyze_stream_request::Kind::Document(doc)) => {
                    if let Some((lanes, lane)) = &self.lanes {
                        lanes.run(*lane).await;
                    }
                    let response = self.answer(doc);
                    match held.take() {
                        None => held = Some(response),
                        Some(previous) => {
                            if self.tx.send(Ok(response)).await.is_err()
                                || self.tx.send(Ok(previous)).await.is_err()
                            {
                                return;
                            }
                        }
                    }
                }
                None => {
                    return self
                        .refuse("message carries neither header nor document")
                        .await;
                }
            }
        }
        if let Some(last) = held {
            let _ = self.tx.send(Ok(last)).await;
        }
        if let Some(gate) = self.gate {
            let permit = gate.acquire().await;
            drop(permit);
        }
    }
}

#[tonic::async_trait]
impl AnalysisService for MockAnalysis {
    type AnalyzeStreamStream =
        Pin<Box<dyn Stream<Item = Result<AnalyzeStreamResponse, Status>> + Send>>;
    type AnalyzeDocumentProgressiveStream =
        Pin<Box<dyn Stream<Item = Result<AnalyzeProgressiveEvent, Status>> + Send>>;

    async fn capabilities(
        &self,
        _request: Request<CapabilitiesRequest>,
    ) -> Result<Response<CapabilitiesResponse>, Status> {
        let mut profiles = vec![ProfileCapabilities {
            name: "default-v1".to_string(),
            version: "1".to_string(),
            steps: Vec::new(),
            layers: if self.ner {
                vec!["turbo:entities".to_string(), "turbo:geo".to_string()]
            } else {
                Vec::new()
            },
            required_bundles: Vec::new(),
        }];
        if !self.dual_identity {
            profiles.push(ProfileCapabilities {
                name: NO_DUAL_CASED_PROFILE.to_string(),
                version: "1".to_string(),
                ..Default::default()
            });
        }
        Ok(Response::new(CapabilitiesResponse {
            bundles: Vec::new(),
            limits: None,
            server: None,
            profiles,
        }))
    }

    async fn analyze_document_progressive(
        &self,
        request: Request<AnalyzePipelineRequest>,
    ) -> Result<Response<Self::AnalyzeDocumentProgressiveStream>, Status> {
        self.unary_calls.fetch_add(1, Ordering::Relaxed);
        if let Some((delay, probe)) = &self.unary_delay {
            let call = DelayedCall::begin(probe.clone());
            tokio::time::sleep(*delay).await;
            call.complete();
        }
        let req = request.into_inner();
        let session = WireSessionOptions::from_binding(req.binding.as_ref())?;
        let mock_resp = analyze_text(&req.text, &session, self.layers(), &self.calls)?;
        let complete = AnalyzeProgressiveEvent {
            sequence: 1,
            event: Some(analyze_progressive_event::Event::Complete(
                DocumentGraphResponse {
                    graph: mock_resp.encode_graph().into(),
                    source_text: req.text,
                    ..Default::default()
                },
            )),
        };
        let receipt = AnalyzeProgressiveEvent {
            sequence: 2,
            event: Some(analyze_progressive_event::Event::Receipt(
                ProcessingReceipt::default(),
            )),
        };
        Ok(Response::new(Box::pin(tokio_stream::iter([
            Ok(complete),
            Ok(receipt),
        ]))))
    }

    async fn analyze_stream(
        &self,
        request: Request<Streaming<AnalyzeStreamRequest>>,
    ) -> Result<Response<Self::AnalyzeStreamStream>, Status> {
        if self.no_stream {
            return Err(Status::unimplemented("this server predates AnalyzeStream"));
        }
        self.stream_opens.fetch_add(1, Ordering::Relaxed);
        let lane = Lane::of(request.metadata());
        let inbound = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<AnalyzeStreamResponse, Status>>(16);
        let session = StreamSession {
            layers: self.layers(),
            calls: self.calls.clone(),
            gate: self.stream_end_gate.clone(),
            lanes: self.lanes.clone().map(|lanes| (lanes, lane)),
            tx,
        };
        tokio::spawn(session.run(inbound));
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn load_bundle(
        &self,
        _request: Request<LoadBundleRequest>,
    ) -> Result<Response<LoadBundleResponse>, Status> {
        Err(Status::unimplemented(
            "MockAnalysis does not serve LoadBundle",
        ))
    }

    async fn unload_bundle(
        &self,
        _request: Request<UnloadBundleRequest>,
    ) -> Result<Response<UnloadBundleResponse>, Status> {
        Err(Status::unimplemented(
            "MockAnalysis does not serve UnloadBundle",
        ))
    }

    async fn round_trip_sparse_terms(
        &self,
        req: Request<SparseTermsResult>,
    ) -> Result<Response<SparseTermsResult>, Status> {
        Ok(Response::new(req.into_inner()))
    }

    async fn round_trip_token_vectors(
        &self,
        req: Request<TokenVectorsResult>,
    ) -> Result<Response<TokenVectorsResult>, Status> {
        Ok(Response::new(req.into_inner()))
    }

    async fn round_trip_multi_res_vectors(
        &self,
        req: Request<MultiResVectorsResult>,
    ) -> Result<Response<MultiResVectorsResult>, Status> {
        Ok(Response::new(req.into_inner()))
    }

    async fn round_trip_rerank_scores(
        &self,
        req: Request<RerankScoresResult>,
    ) -> Result<Response<RerankScoresResult>, Status> {
        Ok(Response::new(req.into_inner()))
    }

    async fn round_trip_extraction(
        &self,
        req: Request<ExtractionResult>,
    ) -> Result<Response<ExtractionResult>, Status> {
        Ok(Response::new(req.into_inner()))
    }

    async fn round_trip_records(
        &self,
        req: Request<RecordsResult>,
    ) -> Result<Response<RecordsResult>, Status> {
        Ok(Response::new(req.into_inner()))
    }

    async fn round_trip_chunk_levels(
        &self,
        req: Request<ChunkLevelsResult>,
    ) -> Result<Response<ChunkLevelsResult>, Status> {
        Ok(Response::new(req.into_inner()))
    }

    async fn round_trip_document_graph(
        &self,
        _req: Request<DocumentGraphRequest>,
    ) -> Result<Response<DocumentGraphResponse>, Status> {
        Err(Status::unimplemented(
            "MockAnalysis does not round-trip TGRF",
        ))
    }

    async fn inspect_document_graph(
        &self,
        _req: Request<InspectDocumentGraphRequest>,
    ) -> Result<Response<TypedDocumentResult>, Status> {
        Err(Status::unimplemented("MockAnalysis does not inspect TGRF"))
    }

    async fn round_trip_typed_document(
        &self,
        req: Request<TypedDocumentResult>,
    ) -> Result<Response<TypedDocumentResult>, Status> {
        Ok(Response::new(req.into_inner()))
    }

    async fn project_search_document(
        &self,
        _req: Request<ProjectSearchDocumentRequest>,
    ) -> Result<Response<SearchDocumentProjection>, Status> {
        Err(Status::unimplemented("MockAnalysis does not project TGRF"))
    }

    async fn round_trip_search_projection(
        &self,
        req: Request<SearchDocumentProjection>,
    ) -> Result<Response<SearchDocumentProjection>, Status> {
        Ok(Response::new(req.into_inner()))
    }

    async fn analyze_query(
        &self,
        _req: Request<AnalyzeQueryRequest>,
    ) -> Result<Response<AnalyzeQueryResponse>, Status> {
        Err(Status::unimplemented(
            "MockAnalysis does not serve AnalyzeQuery",
        ))
    }

    async fn round_trip_analyze_query(
        &self,
        req: Request<AnalyzeQueryResponse>,
    ) -> Result<Response<AnalyzeQueryResponse>, Status> {
        Ok(Response::new(req.into_inner()))
    }

    async fn rerank_and_highlight(
        &self,
        req: Request<RerankAndHighlightRequest>,
    ) -> Result<Response<RerankAndHighlightResponse>, Status> {
        if let Some(lanes) = &self.lanes {
            lanes.run(Lane::of(req.metadata())).await;
        }
        Ok(Response::new(rerank::rerank_and_highlight(
            req.into_inner(),
        )))
    }

    async fn round_trip_rerank_and_highlight(
        &self,
        req: Request<RerankAndHighlightResponse>,
    ) -> Result<Response<RerankAndHighlightResponse>, Status> {
        Ok(Response::new(req.into_inner()))
    }

    async fn inspect_document_window(
        &self,
        _req: Request<DocumentWindowRequest>,
    ) -> Result<Response<DocumentWindowResponse>, Status> {
        Err(Status::unimplemented(
            "MockAnalysis does not serve InspectDocumentWindow",
        ))
    }

    async fn project_chunks(
        &self,
        _req: Request<ProjectChunksRequest>,
    ) -> Result<Response<ProjectChunksResponse>, Status> {
        Err(Status::unimplemented(
            "MockAnalysis does not serve ProjectChunks",
        ))
    }

    async fn list_output_formats(
        &self,
        _req: Request<ListOutputFormatsRequest>,
    ) -> Result<Response<ListOutputFormatsResponse>, Status> {
        Ok(Response::new(ListOutputFormatsResponse::default()))
    }

    async fn format_document(
        &self,
        _req: Request<FormatDocumentRequest>,
    ) -> Result<Response<FormatDocumentResponse>, Status> {
        Err(Status::unimplemented(
            "MockAnalysis does not serve FormatDocument",
        ))
    }
}
