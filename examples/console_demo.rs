//! The console over a self-contained demo cluster: four in-process
//! shards of generated court-opinion paragraphs, a coordinator with
//! diagnostics and an in-memory control plane, the mock analysis sidecar
//! extended with a hashed bag-of-words embedding, and the console bound
//! to loopback. Nothing is read from or written to disk.
//!
//! `cargo run --release --example console_demo -- [--listen=127.0.0.1:8600]`
//!
//! The embedding is a toy: two texts are close when they share stemmed
//! words. It exists so the dense, hybrid and A/B paths of the UI have
//! something to run against; it says nothing about retrieval quality.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use pipestream_search::collections::CollectionSet;
use pipestream_search::console::{Console, ConsoleConfig};
use pipestream_search::control_plane::{ClusterControlService, ControlPolicy, DurableControlPlane};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::harness::mock_analysis::{toy_stem, MockAnalysis};
use pipestream_search::harness::{fit_calibration, nodelay_incoming, start_empty_node};
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::{
    AddDocumentsRequest, AddVectorsRequest, DocLineage, FacetValue, IntegerValue, NumericValue,
    TimestampValue,
};
use pipestream_search::security::ToolClient;
use pipestream_search::MAX_MESSAGE_BYTES;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use turbo_client::proto::grpc_inference_service_server::{
    GrpcInferenceService, GrpcInferenceServiceServer,
};
use turbo_client::proto::model_infer_response::InferOutputTensor;
use turbo_client::proto::result::analysis_service_server::{
    AnalysisService, AnalysisServiceServer,
};
use turbo_client::proto::result::{
    analyze_progressive_event, analyze_stream_request, analyze_stream_response, AnalyzeFailure,
    AnalyzePipelineRequest, AnalyzeProgressiveEvent, AnalyzeQueryRequest, AnalyzeQueryResponse,
    AnalyzeStreamRequest, AnalyzeStreamResponse, CapabilitiesRequest, CapabilitiesResponse,
    ChunkLevelsResult, DocumentGraphRequest, DocumentGraphResponse, DocumentWindowRequest,
    DocumentWindowResponse, ExtractionResult, FormatDocumentRequest, FormatDocumentResponse,
    InspectDocumentGraphRequest, ListOutputFormatsRequest, ListOutputFormatsResponse,
    LoadBundleRequest, LoadBundleResponse, MultiResVectorsResult, ProjectChunksRequest,
    ProjectChunksResponse, ProjectSearchDocumentRequest, RecordsResult, RerankAndHighlightRequest,
    RerankAndHighlightResponse, RerankScoresResult, SearchDocumentProjection, SparseTermsResult,
    TokenVectorsResult, TypedDocumentResult, UnloadBundleRequest, UnloadBundleResponse,
};
use turbo_client::proto::{
    InferTensorContents, ModelInferRequest, ModelInferResponse, ModelMetadataRequest,
    ModelMetadataResponse, ModelReadyRequest, ModelReadyResponse, ServerLiveRequest,
    ServerLiveResponse, ServerMetadataRequest, ServerMetadataResponse, ServerReadyRequest,
    ServerReadyResponse,
};

const DIM: usize = 64;
const SHARDS: usize = 4;
const SHARD_DOCS: usize = 150;

const COURTS: [&str; 6] = ["scotus", "ca2", "ca9", "cafc", "cadc", "nysd"];

/// Per topic: the facet value and the sentences its opinions draw from.
const TOPICS: [(&str, [&str; 8]); 8] = [
    (
        "antitrust",
        [
            "The proposed merger would substantially lessen competition in the relevant market.",
            "Price fixing among horizontal competitors is a per se violation of the Sherman Act.",
            "The defendant maintained monopoly power through exclusive dealing agreements.",
            "Plaintiffs must define a relevant product market and a geographic market.",
            "Predatory pricing requires proof of below cost pricing and a prospect of recoupment.",
            "The tying arrangement foreclosed rivals from a substantial share of commerce.",
            "Consumer welfare is the touchstone of the rule of reason analysis.",
            "Treble damages are available to any person injured in business or property.",
        ],
    ),
    (
        "patent",
        [
            "The claims are directed to an abstract idea and lack an inventive concept.",
            "A person of ordinary skill in the art would have been motivated to combine the references.",
            "Claim construction begins with the ordinary meaning of the claim language.",
            "The accused device infringes under the doctrine of equivalents.",
            "Prior art anticipated every limitation of the asserted claims.",
            "The specification does not enable the full scope of the claimed invention.",
            "Inter partes review found the challenged claims unpatentable as obvious.",
            "A reasonable royalty is the floor for patent infringement damages.",
        ],
    ),
    (
        "privacy",
        [
            "A warrantless search of a cell phone violates the Fourth Amendment.",
            "Individuals keep a reasonable expectation of privacy in their location records.",
            "The third party doctrine does not extend to historical cell site data.",
            "The wiretap statute prohibits interception of electronic communications.",
            "Consent to data collection must be knowing and voluntary.",
            "The exclusionary rule suppresses evidence obtained through an unlawful search.",
            "Biometric identifiers may not be collected without written release.",
            "The good faith exception applies when officers rely on binding precedent.",
        ],
    ),
    (
        "labor",
        [
            "The employer committed an unfair labor practice by refusing to bargain collectively.",
            "Employees have the right to organize and to engage in concerted activity.",
            "The arbitration agreement waives class and collective proceedings.",
            "Overtime wages are owed for hours worked beyond forty in a week.",
            "The worker was misclassified as an independent contractor.",
            "Retaliation against a whistleblower violates the statute.",
            "The union breached its duty of fair representation.",
            "A hostile work environment claim requires severe or pervasive conduct.",
        ],
    ),
    (
        "environmental",
        [
            "The agency failed to prepare an environmental impact statement.",
            "The discharge of pollutants into navigable waters requires a permit.",
            "Greenhouse gas emissions fall within the statutory definition of air pollutant.",
            "The cleanup costs are recoverable from potentially responsible parties.",
            "The endangered species listing was not arbitrary or capricious.",
            "Wetlands adjacent to navigable waters are waters of the United States.",
            "The agency must consider reasonable alternatives to the proposed action.",
            "Citizen suits require notice to the alleged violator sixty days before filing.",
        ],
    ),
    (
        "speech",
        [
            "Content based restrictions on speech are subject to strict scrutiny.",
            "The ordinance is an unconstitutional prior restraint on publication.",
            "Public officials must prove actual malice to recover for defamation.",
            "Commercial speech receives intermediate protection under the First Amendment.",
            "The statute is overbroad and chills protected expression.",
            "A public forum may be regulated by reasonable time, place and manner rules.",
            "Student speech may be restricted when it substantially disrupts school activities.",
            "Compelled speech violates the freedom of thought the First Amendment protects.",
        ],
    ),
    (
        "tax",
        [
            "The transaction lacked economic substance and is disregarded for tax purposes.",
            "Income is taxable when the taxpayer has dominion and control over the funds.",
            "The deduction is allowed for ordinary and necessary business expenses.",
            "The assessment was timely within the three year limitations period.",
            "A capital gain is realized on the sale or exchange of property.",
            "The partnership allocation lacked substantial economic effect.",
            "Penalties do not apply where the taxpayer acted with reasonable cause.",
            "The state tax discriminates against interstate commerce.",
        ],
    ),
    (
        "maritime",
        [
            "The vessel owner owes seamen a duty to furnish a seaworthy ship.",
            "Maintenance and cure is owed to a seaman injured in the service of the vessel.",
            "The limitation of liability act caps damages at the value of the vessel.",
            "Salvage awards reward voluntary rescue of property in marine peril.",
            "The collision was caused by a failure to keep a proper lookout.",
            "Admiralty jurisdiction extends to torts on navigable waters.",
            "The bill of lading governs the carrier's liability for damaged cargo.",
            "Punitive damages are available for willful failure to pay maintenance and cure.",
        ],
    ),
];

/// A small deterministic generator, so every start serves the same corpus.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, bound: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 33) as usize) % bound
    }
}

struct Doc {
    text: String,
    court: &'static str,
    topic: &'static str,
    year: i64,
    filed_seconds: i64,
    citations: f64,
    parent: u64,
}

fn corpus() -> Vec<Doc> {
    let mut rng = Lcg(0x00C0_FFEE);
    (0..SHARDS * SHARD_DOCS)
        .map(|id| {
            // Three chunks per opinion, so collapse on the parent has
            // something to fold.
            let parent = (id / 3) as u64;
            let mut opinion = Lcg(0xD0C + parent);
            let (topic, sentences) = TOPICS[opinion.next(TOPICS.len())];
            let court = COURTS[opinion.next(COURTS.len())];
            let year = 1990 + opinion.next(35) as i64;
            let month = opinion.next(12) as i64;
            let citations = (opinion.next(40) * opinion.next(40)) as f64;
            // Three different sentences of the topic.
            let first = rng.next(sentences.len());
            let step = 1 + rng.next(3);
            let mut lines: Vec<String> = (0..3)
                .map(|i| sentences[(first + i * step) % sentences.len()].to_string())
                .collect();
            lines.push(format!(
                "Opinion {parent} of the {court} court, decided in {year}, part {}.",
                id % 3 + 1
            ));
            Doc {
                text: lines.join("\n"),
                court,
                topic,
                year,
                // Midnight UTC on the first of the month.
                filed_seconds: (year - 1970) * 31_556_952 + month * 2_629_746,
                citations,
                parent,
            }
        })
        .collect()
}

/// The demo embedding: each stemmed word of three letters or more adds a
/// signed one to three hashed dimensions; the sum is unit-normalized.
fn embed(text: &str) -> Vec<f32> {
    let mut v = vec![0.0f32; DIM];
    for word in text.split(|c: char| !c.is_alphanumeric()) {
        if word.len() < 3 {
            continue;
        }
        let stem = toy_stem(word);
        for seed in 0..3u64 {
            let mut h = 0xcbf2_9ce4_8422_2325u64 ^ seed;
            for b in stem.bytes() {
                h = (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3);
            }
            let sign = if (h >> 40) & 1 == 0 { 1.0 } else { -1.0 };
            v[(h % DIM as u64) as usize] += sign;
        }
    }
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut v {
            *x /= norm;
        }
    }
    v
}

/// The mock analysis sidecar with ASCII punctuation blanked before the mock
/// sees the text (the mock splits on whitespace only, so the prose's
/// "…a seaworthy ship." would otherwise index `ship.` and a query for `ship`
/// would find nothing), paired on the same loopback listener with
/// [`DemoInference`] for the console's `/api/embed` Open Inference call.
struct DemoAnalysis(Arc<MockAnalysis>);

/// `text` with each ASCII punctuation byte as a space: the same length,
/// so every span the mock reports still addresses the original.
fn unpunctuated(text: &str) -> String {
    text.chars()
        .map(|c| if c.is_ascii_punctuation() { ' ' } else { c })
        .collect()
}

fn failure_from_status(status: Status) -> AnalyzeFailure {
    AnalyzeFailure {
        turbo_code: status.code() as i32,
        field: 0,
        message: status.message().to_string(),
    }
}

async fn analyze_one(
    mock: &MockAnalysis,
    source_text: String,
    binding: Option<turbo_client::proto::result::InputBinding>,
) -> Result<DocumentGraphResponse, Status> {
    let mut stream = mock
        .analyze_document_progressive(Request::new(AnalyzePipelineRequest {
            text: unpunctuated(&source_text),
            binding,
            ..Default::default()
        }))
        .await?
        .into_inner();
    while let Some(event) = stream.next().await {
        if let Some(analyze_progressive_event::Event::Complete(mut graph)) = event?.event {
            graph.source_text = source_text;
            return Ok(graph);
        }
    }
    Err(Status::internal(
        "mock progressive analysis ended without Complete",
    ))
}

#[tonic::async_trait]
impl AnalysisService for DemoAnalysis {
    type AnalyzeStreamStream =
        Pin<Box<dyn Stream<Item = Result<AnalyzeStreamResponse, Status>> + Send>>;
    type AnalyzeDocumentProgressiveStream =
        Pin<Box<dyn Stream<Item = Result<AnalyzeProgressiveEvent, Status>> + Send>>;

    async fn capabilities(
        &self,
        request: Request<CapabilitiesRequest>,
    ) -> Result<Response<CapabilitiesResponse>, Status> {
        self.0.capabilities(request).await
    }

    async fn analyze_document_progressive(
        &self,
        mut request: Request<AnalyzePipelineRequest>,
    ) -> Result<Response<Self::AnalyzeDocumentProgressiveStream>, Status> {
        let original = std::mem::take(&mut request.get_mut().text);
        request.get_mut().text = unpunctuated(&original);
        let mut inner = self
            .0
            .analyze_document_progressive(request)
            .await?
            .into_inner();
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let mut source = Some(original);
            while let Some(item) = inner.next().await {
                let mapped = item.map(|mut ev| {
                    if let Some(analyze_progressive_event::Event::Complete(ref mut g)) = ev.event {
                        if let Some(s) = source.take() {
                            g.source_text = s;
                        }
                    }
                    ev
                });
                if tx.send(mapped).await.is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    /// Each document through the mock's progressive call, answered in order.
    async fn analyze_stream(
        &self,
        request: Request<Streaming<AnalyzeStreamRequest>>,
    ) -> Result<Response<Self::AnalyzeStreamStream>, Status> {
        let mut inbound = request.into_inner();
        let mock = self.0.clone();
        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            let mut saw_header = false;
            while let Ok(Some(message)) = inbound.message().await {
                let doc = match message.kind {
                    Some(analyze_stream_request::Kind::Header(_)) => {
                        if saw_header {
                            let _ = tx
                                .send(Err(Status::invalid_argument(
                                    "header may only be the first message of the stream",
                                )))
                                .await;
                            return;
                        }
                        saw_header = true;
                        continue;
                    }
                    Some(analyze_stream_request::Kind::Document(doc)) => {
                        if !saw_header {
                            let _ = tx
                                .send(Err(Status::invalid_argument(
                                    "the first message of the stream must carry a header",
                                )))
                                .await;
                            return;
                        }
                        doc
                    }
                    None => continue,
                };
                let outcome = match String::from_utf8(doc.text)
                    .map_err(|_| Status::invalid_argument("document text is not UTF-8"))
                    .map(|text| (text, doc.binding.clone()))
                {
                    Ok((text, binding)) => match analyze_one(&mock, text, binding).await {
                        Ok(graph) => analyze_stream_response::Outcome::DocumentGraph(graph),
                        Err(status) => {
                            analyze_stream_response::Outcome::Failure(failure_from_status(status))
                        }
                    },
                    Err(status) => {
                        analyze_stream_response::Outcome::Failure(failure_from_status(status))
                    }
                };
                let response = AnalyzeStreamResponse {
                    id: doc.id,
                    outcome: Some(outcome),
                    binding: doc.binding,
                };
                if tx.send(Ok(response)).await.is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn load_bundle(
        &self,
        request: Request<LoadBundleRequest>,
    ) -> Result<Response<LoadBundleResponse>, Status> {
        self.0.load_bundle(request).await
    }

    async fn unload_bundle(
        &self,
        request: Request<UnloadBundleRequest>,
    ) -> Result<Response<UnloadBundleResponse>, Status> {
        self.0.unload_bundle(request).await
    }

    async fn round_trip_sparse_terms(
        &self,
        req: Request<SparseTermsResult>,
    ) -> Result<Response<SparseTermsResult>, Status> {
        self.0.round_trip_sparse_terms(req).await
    }

    async fn round_trip_token_vectors(
        &self,
        req: Request<TokenVectorsResult>,
    ) -> Result<Response<TokenVectorsResult>, Status> {
        self.0.round_trip_token_vectors(req).await
    }

    async fn round_trip_multi_res_vectors(
        &self,
        req: Request<MultiResVectorsResult>,
    ) -> Result<Response<MultiResVectorsResult>, Status> {
        self.0.round_trip_multi_res_vectors(req).await
    }

    async fn round_trip_rerank_scores(
        &self,
        req: Request<RerankScoresResult>,
    ) -> Result<Response<RerankScoresResult>, Status> {
        self.0.round_trip_rerank_scores(req).await
    }

    async fn round_trip_extraction(
        &self,
        req: Request<ExtractionResult>,
    ) -> Result<Response<ExtractionResult>, Status> {
        self.0.round_trip_extraction(req).await
    }

    async fn round_trip_records(
        &self,
        req: Request<RecordsResult>,
    ) -> Result<Response<RecordsResult>, Status> {
        self.0.round_trip_records(req).await
    }

    async fn round_trip_chunk_levels(
        &self,
        req: Request<ChunkLevelsResult>,
    ) -> Result<Response<ChunkLevelsResult>, Status> {
        self.0.round_trip_chunk_levels(req).await
    }

    async fn round_trip_document_graph(
        &self,
        req: Request<DocumentGraphRequest>,
    ) -> Result<Response<DocumentGraphResponse>, Status> {
        self.0.round_trip_document_graph(req).await
    }

    async fn inspect_document_graph(
        &self,
        req: Request<InspectDocumentGraphRequest>,
    ) -> Result<Response<TypedDocumentResult>, Status> {
        self.0.inspect_document_graph(req).await
    }

    async fn round_trip_typed_document(
        &self,
        req: Request<TypedDocumentResult>,
    ) -> Result<Response<TypedDocumentResult>, Status> {
        self.0.round_trip_typed_document(req).await
    }

    async fn project_search_document(
        &self,
        req: Request<ProjectSearchDocumentRequest>,
    ) -> Result<Response<SearchDocumentProjection>, Status> {
        self.0.project_search_document(req).await
    }

    async fn round_trip_search_projection(
        &self,
        req: Request<SearchDocumentProjection>,
    ) -> Result<Response<SearchDocumentProjection>, Status> {
        self.0.round_trip_search_projection(req).await
    }

    async fn analyze_query(
        &self,
        req: Request<AnalyzeQueryRequest>,
    ) -> Result<Response<AnalyzeQueryResponse>, Status> {
        self.0.analyze_query(req).await
    }

    async fn round_trip_analyze_query(
        &self,
        req: Request<AnalyzeQueryResponse>,
    ) -> Result<Response<AnalyzeQueryResponse>, Status> {
        self.0.round_trip_analyze_query(req).await
    }

    async fn rerank_and_highlight(
        &self,
        req: Request<RerankAndHighlightRequest>,
    ) -> Result<Response<RerankAndHighlightResponse>, Status> {
        self.0.rerank_and_highlight(req).await
    }

    async fn round_trip_rerank_and_highlight(
        &self,
        req: Request<RerankAndHighlightResponse>,
    ) -> Result<Response<RerankAndHighlightResponse>, Status> {
        self.0.round_trip_rerank_and_highlight(req).await
    }

    async fn inspect_document_window(
        &self,
        req: Request<DocumentWindowRequest>,
    ) -> Result<Response<DocumentWindowResponse>, Status> {
        self.0.inspect_document_window(req).await
    }

    async fn project_chunks(
        &self,
        req: Request<ProjectChunksRequest>,
    ) -> Result<Response<ProjectChunksResponse>, Status> {
        self.0.project_chunks(req).await
    }

    async fn list_output_formats(
        &self,
        req: Request<ListOutputFormatsRequest>,
    ) -> Result<Response<ListOutputFormatsResponse>, Status> {
        self.0.list_output_formats(req).await
    }

    async fn format_document(
        &self,
        req: Request<FormatDocumentRequest>,
    ) -> Result<Response<FormatDocumentResponse>, Status> {
        self.0.format_document(req).await
    }
}

struct DemoInference;

#[tonic::async_trait]
impl GrpcInferenceService for DemoInference {
    async fn server_live(
        &self,
        _request: Request<ServerLiveRequest>,
    ) -> Result<Response<ServerLiveResponse>, Status> {
        Ok(Response::new(ServerLiveResponse { live: true }))
    }

    async fn server_ready(
        &self,
        _request: Request<ServerReadyRequest>,
    ) -> Result<Response<ServerReadyResponse>, Status> {
        Ok(Response::new(ServerReadyResponse { ready: true }))
    }

    async fn model_ready(
        &self,
        _request: Request<ModelReadyRequest>,
    ) -> Result<Response<ModelReadyResponse>, Status> {
        Ok(Response::new(ModelReadyResponse { ready: true }))
    }

    async fn server_metadata(
        &self,
        _request: Request<ServerMetadataRequest>,
    ) -> Result<Response<ServerMetadataResponse>, Status> {
        Err(Status::unimplemented("demo inference server"))
    }

    async fn model_metadata(
        &self,
        _request: Request<ModelMetadataRequest>,
    ) -> Result<Response<ModelMetadataResponse>, Status> {
        Err(Status::unimplemented("demo inference server"))
    }

    async fn model_infer(
        &self,
        request: Request<ModelInferRequest>,
    ) -> Result<Response<ModelInferResponse>, Status> {
        let inner = request.into_inner();
        let input = inner
            .inputs
            .first()
            .ok_or_else(|| Status::invalid_argument("missing texts input tensor"))?;
        let raw_texts = input
            .contents
            .as_ref()
            .map(|c| c.bytes_contents.as_slice())
            .unwrap_or(&[]);
        if raw_texts.is_empty() {
            return Err(Status::invalid_argument("empty texts input tensor"));
        }
        let mut fp32_contents = Vec::with_capacity(raw_texts.len() * DIM);
        for raw in raw_texts {
            let text = std::str::from_utf8(raw)
                .map_err(|_| Status::invalid_argument("input text is not UTF-8"))?;
            fp32_contents.extend(embed(text));
        }
        Ok(Response::new(ModelInferResponse {
            model_name: inner.model_name,
            model_version: inner.model_version,
            id: inner.id,
            parameters: HashMap::new(),
            outputs: vec![InferOutputTensor {
                name: "embeddings".into(),
                datatype: "FP32".into(),
                shape: vec![raw_texts.len() as i64, DIM as i64],
                parameters: HashMap::new(),
                contents: Some(InferTensorContents {
                    fp32_contents,
                    ..Default::default()
                }),
            }],
            raw_output_contents: Vec::new(),
        }))
    }
}

async fn serve_analysis() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        Server::builder()
            .add_service(
                AnalysisServiceServer::new(DemoAnalysis(Arc::new(MockAnalysis::default())))
                    .max_decoding_message_size(MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(MAX_MESSAGE_BYTES),
            )
            .add_service(
                GrpcInferenceServiceServer::new(DemoInference)
                    .max_decoding_message_size(MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(MAX_MESSAGE_BYTES),
            )
            .serve_with_incoming(nodelay_incoming(listener)),
    );
    format!("http://{addr}")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listen = std::env::args()
        .find_map(|a| a.strip_prefix("--listen=").map(str::to_string))
        .unwrap_or_else(|| "127.0.0.1:8600".to_string());

    let analysis = serve_analysis().await;
    let docs = corpus();
    let vectors: Vec<f32> = docs.iter().flat_map(|d| embed(&d.text)).collect();
    let (shift, scale) = fit_calibration(DIM, 4, &vectors);

    let mut nodes = Vec::new();
    for shard in 0..SHARDS {
        let (addr, _handle) = start_empty_node(NodeConfig {
            slot_offset: (shard * SHARD_DOCS) as u64,
            analysis_addr: Some(analysis.clone()),
            integer_fields: vec!["year".into(), "filed".into()],
            facet_fields: vec!["court".into(), "topic".into()],
            numeric_fields: vec!["citations".into()],
            sentence_fields: vec!["body".into()],
            position_fields: vec!["body".into()],
            ..Default::default()
        })
        .await;
        let mut client = NodeServiceClient::connect(addr.clone()).await?;
        client
            .configure_vector_backend(pipestream_search::harness::embedded_backend_request(
                DIM, 4, &shift, &scale,
            ))
            .await?;
        let start = shard * SHARD_DOCS;
        let (tx, rx) = mpsc::channel(SHARD_DOCS);
        for doc in &docs[start..start + SHARD_DOCS] {
            tx.send(AddDocumentsRequest {
                text: doc.text.clone(),
                lineage: Some(DocLineage {
                    parent_id: doc.parent,
                    group_id: doc.parent,
                    ..Default::default()
                }),
                facets: vec![
                    FacetValue {
                        field: "court".into(),
                        value: doc.court.into(),
                    },
                    FacetValue {
                        field: "topic".into(),
                        value: doc.topic.into(),
                    },
                ],
                integers: vec![IntegerValue {
                    field: "year".into(),
                    value: doc.year,
                }],
                timestamps: vec![TimestampValue {
                    field: "filed".into(),
                    value: Some(prost_types::Timestamp {
                        seconds: doc.filed_seconds,
                        nanos: 0,
                    }),
                }],
                numerics: vec![NumericValue {
                    field: "citations".into(),
                    value: doc.citations,
                }],
                ..Default::default()
            })
            .await?;
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await?;
        let (vtx, vrx) = mpsc::channel(1);
        vtx.send(AddVectorsRequest {
            materialized_rows: Vec::new(),
            vectors: vectors[start * DIM..(start + SHARD_DOCS) * DIM].to_vec(),
            dim: DIM as u32,
        })
        .await?;
        drop(vtx);
        client.add_vectors(ReceiverStream::new(vrx)).await?;
        nodes.push(addr);
    }

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let coordinator_addr = format!("http://{}", listener.local_addr()?);
    let width = u64::MAX / SHARDS as u64;
    let ranges = (0..SHARDS as u64)
        .map(|i| {
            let end = if i + 1 == SHARDS as u64 {
                u64::MAX
            } else {
                (i + 1) * width - 1
            };
            Some((i * width, end))
        })
        .collect();
    let coordinator = CoordinatorServiceImpl::new(nodes.clone())
        .with_bm25(Some(analysis.clone()), Default::default())
        .with_topology_generation(1)
        .with_hot_topology(ranges)?;
    // The public surface as the serving binary builds it: the collection
    // set records each request in the ring its diagnostics service reads.
    let search = CollectionSet::single(coordinator.clone());
    let diagnostics = search.diagnostics().into_server(MAX_MESSAGE_BYTES);
    let plane = DurableControlPlane::in_memory(ControlPolicy {
        replication_factor: 1,
        ..Default::default()
    });
    plane.bootstrap_topology(1, &coordinator.current_topology_routes())?;
    let control = ClusterControlService::new(plane)
        .with_coordinator(coordinator.clone())
        .into_server(MAX_MESSAGE_BYTES);
    tokio::spawn(
        Server::builder()
            .add_service(search.into_server(MAX_MESSAGE_BYTES))
            .add_service(diagnostics)
            .add_service(control)
            .serve_with_incoming(nodelay_incoming(listener)),
    );

    let console = Console::bind(ConsoleConfig {
        listen,
        coordinator: coordinator_addr,
        nodes,
        analysis: Some(analysis),
        security: ToolClient::from_args(Vec::<String>::new())?,
        allow_remote: false,
    })
    .await?;
    eprintln!(
        "demo cluster: {SHARDS} shards, {} documents, {DIM}-dim hashed embedding",
        docs.len()
    );
    eprintln!("{}", console.describe());
    console.serve().await?;
    Ok(())
}
