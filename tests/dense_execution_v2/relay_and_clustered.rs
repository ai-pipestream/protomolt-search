//! Mixed-child relay part-count barrier and uncertified clustered TurboVec receipts.

use super::*;
use std::ops::Range;
use std::path::Path;

use pipestream_search::clustered_turbovec::ClusteredTurboVecBackend;
use pipestream_search::harness::{serve_node, start_relay};
use pipestream_search::node::NodeServiceImpl;
use pipestream_search::vector::{
    PartCounts, VectorBackendConfig, VectorBackendDescriptor, VectorError, VectorIndex,
    VectorProvider, VectorSearchOptions, VectorSearchResults, VectorStreamBatch,
    VectorStreamControl, VectorStreamSummary,
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::Code;
use turbovec_grpc::{
    CoordinatorService, Index, IndexStore, NodeTable, ShardConfig, TurboVecService,
};

struct NonCountingHnsw(HnswIndex);

impl VectorProvider for NonCountingHnsw {
    fn descriptor(&self) -> VectorBackendDescriptor {
        self.0.descriptor()
    }
    fn backend_config(&self) -> Result<VectorBackendConfig, VectorError> {
        self.0.backend_config()
    }
    fn len(&self) -> usize {
        self.0.len()
    }
    fn dimension(&self) -> Option<usize> {
        self.0.dimension()
    }
    fn add(&mut self, vectors: &[f32], dimension: usize) -> Result<(), VectorError> {
        self.0.add(vectors, dimension)
    }
    fn prepare(&mut self) -> Result<(), VectorError> {
        self.0.prepare()
    }
    fn write(&self, path: &Path) -> Result<(), VectorError> {
        self.0.write(path)
    }
    fn search(
        &self,
        queries: &[f32],
        k: usize,
        options: VectorSearchOptions<'_>,
    ) -> Result<VectorSearchResults, VectorError> {
        let mut out = self.0.search(queries, k, options)?;
        out.parts = PartCounts::default();
        Ok(out)
    }
    fn search_streaming_controlled(
        &self,
        queries: &[f32],
        options: VectorSearchOptions<'_>,
        sink: &mut dyn FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
        control: &mut dyn FnMut() -> VectorStreamControl,
    ) -> Result<VectorStreamSummary, VectorError> {
        let mut summary = self
            .0
            .search_streaming_controlled(queries, options, sink, control)?;
        summary.parts = PartCounts::default();
        Ok(summary)
    }
    fn row_transcripts(
        &self,
        rows: Range<usize>,
        sink: &mut dyn FnMut(usize, &[u8]),
    ) -> Result<(), VectorError> {
        self.0.row_transcripts(rows, sink)
    }
}

async fn assert_mixed_relay_refuses(relay_addr: &str, qvec: &[f32], streaming: bool) {
    let coord = CoordinatorServiceImpl::new(vec![relay_addr.to_string()])
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_stream_search(streaming);
    for mode in [Mode::Exhaustive, Mode::Auto, Mode::Balanced] {
        let err = coord
            .query(Request::new(QueryRequest {
                k: 5,
                selection: Some(dense_leaf(qvec, mode, Precision::Auto)),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            tonic::Code::Internal,
            "streaming={streaming} mode={mode:?}: {err}"
        );
        assert!(
            err.message()
                .contains("not every consulted shard reported per-part execution counts"),
            "streaming={streaming} mode={mode:?}: {err}"
        );
    }
    let comp_err = coord
        .query(Request::new(composite_query(
            qvec,
            Mode::Exhaustive,
            strat::Strategy::Rrf(RrfScore::default()),
        )))
        .await
        .unwrap_err();
    assert_eq!(comp_err.code(), tonic::Code::Internal);
    let raw_err = coord
        .search(Request::new(SearchRequest {
            k: 5,
            vector: qvec.to_vec(),
            enumerate_all: true,
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(raw_err.code(), tonic::Code::Internal);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relay_with_non_counting_child_never_launders_part_counts_or_certifies() {
    let vectors = unit_vectors(128, DIM, 0x2E1A_5501);
    let qvec = vectors[..DIM].to_vec();

    let mut counting_hnsw = HnswIndex::new(DIM, HnswConfig::default()).unwrap();
    counting_hnsw.add(&vectors[..64 * DIM], DIM).unwrap();
    counting_hnsw.prepare().unwrap();
    let counting_cfg = NodeConfig {
        slot_offset: 0,
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        ..Default::default()
    };
    let (counting_addr, h0) = start_node(counting_hnsw.into_index(), counting_cfg).await;
    append_docs_only(&counting_addr, 64, 0).await;

    let mut silent_hnsw = HnswIndex::new(DIM, HnswConfig::default()).unwrap();
    silent_hnsw.add(&vectors[64 * DIM..], DIM).unwrap();
    silent_hnsw.prepare().unwrap();
    let silent_node = NodeServiceImpl::new(
        Some(VectorIndex::from_provider(NonCountingHnsw(silent_hnsw))),
        NodeConfig {
            slot_offset: 64,
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            ..Default::default()
        },
    );
    let (silent_addr, h1) = serve_node(silent_node).await;
    append_docs_only(&silent_addr, 64, 64).await;

    let (relay_addr, _relay, relay_h) =
        start_relay(vec![counting_addr.clone(), silent_addr.clone()]).await;
    for streaming in [false, true] {
        assert_mixed_relay_refuses(&relay_addr, &qvec, streaming).await;
    }
    relay_h.abort();
    h0.abort();
    h1.abort();
}

async fn append_docs_only(addr: &str, count: usize, start_id: usize) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let docs: Vec<AddDocumentsRequest> = (0..count)
        .map(|i| AddDocumentsRequest {
            text: format!("doc {} appellate opinion jurisdiction", start_id + i),
            analysis: Some(body_spec()),
            ..Default::default()
        })
        .collect();
    client
        .add_documents(tokio_stream::iter(docs))
        .await
        .unwrap();
}

async fn serve_turbovec_shard(
    corpus: &[f32],
    shift: &[f32],
    scale: &[f32],
) -> (String, String, tokio::task::JoinHandle<()>) {
    let rows = corpus.len() / DIM;
    let mut index = turbovec::TurboQuantIndex::from_parts(
        Some(DIM),
        4,
        0,
        Vec::new(),
        Vec::new(),
        shift.to_vec(),
        scale.to_vec(),
    )
    .unwrap();
    index.add_2d(corpus, DIM).unwrap();
    index.prepare();
    let store = IndexStore::new();
    let labels: Vec<u64> = (0..rows as u64).collect();
    let index_id = store.insert_labelled(Index::Positional(index), labels);
    let service = TurboVecService::new(store);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let handle = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(service.into_query_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    (address, index_id, handle)
}

async fn assert_clustered_uncertified_routes(coord: &CoordinatorServiceImpl, qvec: &[f32]) {
    for (mode, precision) in [
        (Mode::Exhaustive, Precision::Auto),
        (Mode::Exhaustive, Precision::Quantized),
        (Mode::Auto, Precision::Auto),
        (Mode::Auto, Precision::Quantized),
    ] {
        let err = coord
            .query(Request::new(QueryRequest {
                k: 5,
                selection: Some(dense_leaf(qvec, mode, precision)),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            Code::FailedPrecondition,
            "{mode:?}/{precision:?}"
        );
        assert!(err.message().contains("clustered-turbovec"), "{err}");
    }
    for mode in [Mode::Exhaustive, Mode::Auto] {
        let err = coord
            .query(Request::new(composite_query(
                qvec,
                mode,
                strat::Strategy::Rrf(RrfScore::default()),
            )))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition, "{mode:?}");
        assert!(err.message().contains("clustered-turbovec"), "{err}");
    }

    for (precision, selection_k) in [
        (Precision::Auto, 0),
        (Precision::Quantized, 0),
        (Precision::Fp32, 10),
    ] {
        let out = run_query(
            coord,
            QueryRequest {
                k: 5,
                selection_k,
                selection: Some(dense_leaf(qvec, Mode::Balanced, precision)),
                ..Default::default()
            },
        )
        .await
        .dense_execution
        .unwrap();
        assert_eq!(out.provider_backend, "clustered-turbovec");
        assert_eq!(out.resolved_mode, Mode::Balanced as i32);
        assert!(out.fallback_reason.is_empty(), "{}", out.fallback_reason);
        assert_eq!(out.coverage, DenseCoverage::CandidateSet as i32);
        assert_eq!(out.termination, DenseTermination::Unspecified as i32);
        assert!(out.techniques.is_empty(), "{:?}", out.techniques);
    }

    for strategy in [
        strat::Strategy::Rrf(RrfScore::default()),
        strat::Strategy::ScoreBlend(BlendScore::default()),
        strat::Strategy::Decomposed(DecomposedScore::default()),
        strat::Strategy::Cascade(CascadeScore {
            gate_id: "dense".into(),
        }),
    ] {
        let out = run_query(coord, composite_query(qvec, Mode::Balanced, strategy))
            .await
            .dense_execution
            .unwrap();
        assert_eq!(out.resolved_mode, Mode::Balanced as i32);
        assert!(out.fallback_reason.is_empty(), "{}", out.fallback_reason);
        assert_eq!(out.coverage, DenseCoverage::CandidateSet as i32);
        assert!(out.techniques.is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clustered_turbovec_refuses_uncertified_exhaustive_and_serves_balanced_candidate_set() {
    let rows = 64usize;
    let corpus = unit_vectors(rows, DIM, 0xC105_9901);
    let qvec = corpus[..DIM].to_vec();
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let (tv_addr, index_id, tv_h) = serve_turbovec_shard(&corpus, &shift, &scale).await;
    let table = NodeTable::new(vec![ShardConfig::with_index(tv_addr, index_id)]);
    let backend = ClusteredTurboVecBackend::in_process(CoordinatorService::new(table));

    let cfg = NodeConfig {
        slot_offset: 0,
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        facet_fields: vec!["court".into()],
        ..Default::default()
    };
    let seeded = seeded_index(DIM, 4, &shift, &scale);
    let (prod_addr, prod_h) = start_node(seeded, cfg).await;
    append_rows(&prod_addr, &corpus, 0, "ca9").await;

    let coord = CoordinatorServiceImpl::new(vec![prod_addr])
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_clustered_turbovec(backend.clone());

    assert_clustered_uncertified_routes(&coord, &qvec).await;

    for mode in [Mode::Exhaustive, Mode::Auto] {
        let local_fp32 = run_query(
            &coord,
            QueryRequest {
                k: 5,
                selection: Some(dense_leaf(&qvec, mode, Precision::Fp32)),
                ..Default::default()
            },
        )
        .await
        .dense_execution
        .unwrap();
        assert_eq!(local_fp32.resolved_mode, Mode::Exhaustive as i32);
        assert_eq!(
            local_fp32.coverage,
            DenseCoverage::ExhaustiveCertified as i32
        );
        assert_eq!(
            local_fp32.techniques,
            vec![tech_count(DenseTechnique::Fp32Scan, 1)]
        );
    }

    let sidecarless = CoordinatorServiceImpl::new(Vec::new()).with_clustered_turbovec(backend);
    let no_sidecar = sidecarless
        .query(Request::new(QueryRequest {
            k: 5,
            selection: Some(dense_leaf(&qvec, Mode::Exhaustive, Precision::Fp32)),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(no_sidecar.code(), Code::FailedPrecondition);

    prod_h.abort();
    tv_h.abort();
}
