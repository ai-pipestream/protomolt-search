//! The dense execution contract end to end: EXHAUSTIVE and AUTO run the
//! same certified scan on an exhaustive quantized provider; missing and
//! retired modes refuse; BALANCED discloses its same-precision fallback
//! where the provider has no balanced technique; FAST never substitutes;
//! a configured ANN provider walks under a built-in preset that claims no
//! measured recall, and FP32 pool rescoring never certifies coverage; AUTO
//! approximates only through a version-2 policy bound to the live identity.

mod common;

use std::path::PathBuf;

use common::{fit_calibration, start_node, unit_vectors};
use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::dense_policy::DenseExecutionPolicy;
use pipestream_search::harness::{fake_ann, seeded_index};
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    search_query, selection_query, AddDocumentsRequest, AddVectorsRequest, CompositeSearchStrategy,
    DenseCoverage, DenseExecutionMode as Mode, DenseExecutionOutcome, DenseExecutionPolicyDocument,
    DenseExecutionShape, DenseFilterStrategy, DensePolicyPoint, DensePrecisionRequest as Precision,
    DenseScorePrecision, DenseTechnique, DenseTermination, FacetValue, QueryRequest, QueryResponse,
    SelectionOperator, SelectionQuery,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

const DIM: usize = 32;
const N_DOCS: usize = 8;
const GENERATION: u64 = 7;

/// Document `id`'s court: four `ca9`, two `scotus`, two `dcc`.
fn court(id: usize) -> &'static str {
    match id {
        0..=3 => "ca9",
        4 | 5 => "scotus",
        _ => "dcc",
    }
}

struct Fixture {
    coordinator: CoordinatorServiceImpl,
    query_vector: Vec<f32>,
    /// The scoring fingerprint the fake provider advertises.
    fingerprint: String,
    handles: Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

/// `shards` nodes over the same eight documents and vectors; `fake`
/// wraps each image as the fake ANN provider, else the images serve as
/// the exhaustive provider they are.
async fn start_fixture(shards: usize, fake: bool) -> Fixture {
    let corpus = unit_vectors(N_DOCS, DIM, 0xA7A0_0001);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let per_shard = N_DOCS / shards;
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    let fingerprint = fake_ann::fingerprint_of(&seeded_index(DIM, 4, &shift, &scale));
    for shard in 0..shards {
        let inner = seeded_index(DIM, 4, &shift, &scale);
        let index = if fake {
            fake_ann::fake_ann_index(inner)
        } else {
            inner
        };
        let (addr, handle) = start_node(
            index,
            NodeConfig {
                slot_offset: (shard * per_shard) as u64,
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
                facet_fields: vec!["court".to_string()],
                ..Default::default()
            },
        )
        .await;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let (tx, rx) = mpsc::channel(16);
        for i in 0..per_shard {
            let id = shard * per_shard + i;
            tx.send(AddDocumentsRequest {
                text: format!("document {id} about the appeal"),
                analysis: Some(body_spec()),
                facets: vec![FacetValue {
                    field: "court".into(),
                    value: court(id).into(),
                }],
                ..Default::default()
            })
            .await
            .unwrap();
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
        let start = shard * per_shard;
        let (vtx, vrx) = mpsc::channel(4);
        vtx.send(AddVectorsRequest {
            materialized_rows: Vec::new(),
            vectors: corpus[start * DIM..(start + per_shard) * DIM].to_vec(),
            dim: DIM as u32,
        })
        .await
        .unwrap();
        drop(vtx);
        client.add_vectors(ReceiverStream::new(vrx)).await.unwrap();
        addrs.push(addr);
        handles.push(handle);
    }
    let coordinator = CoordinatorServiceImpl::new(addrs)
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_topology_generation(GENERATION);
    Fixture {
        coordinator,
        query_vector: unit_vectors(1, DIM, 0xA7A0_0002),
        fingerprint,
        handles,
    }
}

/// Write a policy document the way an operator installs one and load it
/// back: the coordinator sees the file, not the struct.
fn load_policy(tag: &str, bytes: &[u8]) -> Result<DenseExecutionPolicy, String> {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("dense_policy_{tag}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("policy.binpb");
    std::fs::write(&path, bytes).unwrap();
    let policy = DenseExecutionPolicy::load(&path);
    let _ = std::fs::remove_dir_all(&dir);
    policy
}

/// A version-2 document for the fake ANN fixture: k=3 walked at depth 5.
fn fake_policy(fingerprint: &str, corpus_fp: &str, generation: u64) -> Vec<u8> {
    use prost::Message;
    DenseExecutionPolicyDocument {
        format_version: pipestream_search::dense_policy::FORMAT_VERSION,
        policy_id: "court-ann-v2".into(),
        embedding_model: "e".into(),
        provider_backend: fake_ann::BACKEND_KIND.into(),
        scoring_fingerprint: fingerprint.into(),
        corpus_generation: generation,
        corpus_rows: N_DOCS as u64,
        deleted_docs: 0,
        corpus_fingerprint: corpus_fp.into(),
        dimensions: DIM as u32,
        measured_queries: 16,
        target_recall_ppm: 950_000,
        points: vec![DensePolicyPoint {
            k: 3,
            filter_selectivity_ppm_min: 1_000_000,
            filter_selectivity_ppm_max: 1_000_000,
            candidates: 5,
            measured_recall_ppm: 990_000,
            mode: Mode::Balanced as i32,
            technique: DenseTechnique::HnswWalk as i32,
            selection_precision: DenseScorePrecision::Quantized as i32,
            output_precision: DenseScorePrecision::Quantized as i32,
            filter_strategy: DenseFilterStrategy::Speed as i32,
            shape: DenseExecutionShape::Unary as i32,
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

fn dense_leaf(vector: &[f32], mode: Mode, precision: Precision) -> SelectionQuery {
    common::dense_selection("vec", vector.to_vec(), mode as i32, precision as i32)
}

fn cel_filter(cel: &str) -> SelectionQuery {
    common::cel_selection("court", cel)
}

fn request(k: u32, selection_k: u32, selection: SelectionQuery) -> QueryRequest {
    QueryRequest {
        k,
        selection_k,
        selection: Some(selection),
        ..Default::default()
    }
}

fn filtered(cel: &str, leaf: SelectionQuery) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
            operator: SelectionOperator::And as i32,
            clauses: vec![cel_filter(cel), leaf],
            scoring: None,
        })),
    }
}

async fn query(
    coordinator: &CoordinatorServiceImpl,
    req: QueryRequest,
) -> Result<QueryResponse, tonic::Status> {
    coordinator
        .query(Request::new(req))
        .await
        .map(|r| r.into_inner())
}

fn hit_bits(response: &QueryResponse) -> Vec<(u64, u32)> {
    response
        .hits
        .iter()
        .map(|h| (h.doc_id, h.score.to_bits()))
        .collect()
}

fn techniques(outcome: &DenseExecutionOutcome) -> Vec<i32> {
    outcome.techniques.iter().map(|t| t.technique).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exhaustive_and_auto_run_one_certified_quantized_scan() {
    let fixture = start_fixture(2, false).await;
    let leaf = |mode| dense_leaf(&fixture.query_vector, mode, Precision::Auto);
    let exhaustive = query(&fixture.coordinator, request(3, 0, leaf(Mode::Exhaustive)))
        .await
        .unwrap();
    let auto = query(&fixture.coordinator, request(3, 0, leaf(Mode::Auto)))
        .await
        .unwrap();
    assert_eq!(hit_bits(&auto), hit_bits(&exhaustive));
    assert_eq!(auto.hits.len(), 3);
    for (response, requested) in [(&exhaustive, Mode::Exhaustive), (&auto, Mode::Auto)] {
        let outcome = response.dense_execution.as_ref().unwrap();
        assert_eq!(outcome.requested_mode, requested as i32);
        assert_eq!(outcome.resolved_mode, Mode::Exhaustive as i32);
        assert_eq!(outcome.coverage, DenseCoverage::ExhaustiveCertified as i32);
        assert_eq!(outcome.termination, DenseTermination::Exhausted as i32);
        assert_eq!(
            outcome.output_precision,
            DenseScorePrecision::Quantized as i32
        );
        assert_eq!(
            techniques(outcome),
            vec![DenseTechnique::QuantizedScan as i32]
        );
        assert!(outcome.preset.is_empty() && !outcome.policy_qualified);
        assert!(outcome.fallback_reason.is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_retired_and_contradictory_contracts_refuse() {
    let fixture = start_fixture(1, false).await;
    for retired in [0, 1, 2, 3, 99] {
        let sel = common::dense_selection("vec", fixture.query_vector.clone(), retired, 0);
        let err = query(&fixture.coordinator, request(3, 0, sel))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "mode {retired}");
    }
    let mut leaf = dense_leaf(&fixture.query_vector, Mode::Exhaustive, Precision::Auto);
    if let Some(selection_query::Node::Search(search)) = leaf.node.as_mut() {
        if let Some(search_query::Query::Dense(dense)) = search.query.as_mut() {
            dense.filter_strategy = DenseFilterStrategy::Speed as i32;
        }
    }
    let err = query(&fixture.coordinator, request(3, 0, leaf))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("owns its routing"),
        "{}",
        err.message()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn balanced_discloses_its_fallback_and_fast_never_substitutes() {
    let fixture = start_fixture(2, false).await;
    let leaf = |mode| dense_leaf(&fixture.query_vector, mode, Precision::Auto);
    let exhaustive = query(&fixture.coordinator, request(3, 0, leaf(Mode::Exhaustive)))
        .await
        .unwrap();
    let balanced = query(&fixture.coordinator, request(3, 0, leaf(Mode::Balanced)))
        .await
        .unwrap();
    assert_eq!(hit_bits(&balanced), hit_bits(&exhaustive));
    let outcome = balanced.dense_execution.unwrap();
    assert_eq!(outcome.requested_mode, Mode::Balanced as i32);
    assert_eq!(outcome.resolved_mode, Mode::Exhaustive as i32);
    assert_eq!(outcome.coverage, DenseCoverage::ExhaustiveCertified as i32);
    assert!(
        outcome.fallback_reason.contains("same precision"),
        "{}",
        outcome.fallback_reason
    );
    let err = query(&fixture.coordinator, request(3, 0, leaf(Mode::Fast)))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message().contains("never substituted"),
        "{}",
        err.message()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_configured_ann_provider_walks_under_a_preset_and_never_certifies() {
    let fixture = start_fixture(2, true).await;
    let leaf = |mode, precision| dense_leaf(&fixture.query_vector, mode, precision);
    let err = query(
        &fixture.coordinator,
        request(3, 0, leaf(Mode::Exhaustive, Precision::Auto)),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    let walked = query(
        &fixture.coordinator,
        request(3, 0, leaf(Mode::Balanced, Precision::Auto)),
    )
    .await
    .unwrap();
    let outcome = walked.dense_execution.unwrap();
    assert_eq!(outcome.resolved_mode, Mode::Balanced as i32);
    assert_eq!(outcome.coverage, DenseCoverage::CandidateSet as i32);
    assert_eq!(outcome.termination, DenseTermination::Converged as i32);
    assert_eq!(outcome.preset, "hnsw-balanced-v1");
    assert!(!outcome.policy_qualified && outcome.policy_id.is_empty());
    assert_eq!(outcome.candidate_depth, 3);
    assert_eq!(techniques(&outcome), vec![DenseTechnique::HnswWalk as i32]);
    let pooled = query(
        &fixture.coordinator,
        request(3, 6, leaf(Mode::Balanced, Precision::Fp32)),
    )
    .await
    .unwrap();
    assert_eq!(pooled.executed, "search:fp32_pool");
    let outcome = pooled.dense_execution.unwrap();
    assert_eq!(outcome.coverage, DenseCoverage::CandidateSet as i32);
    assert_eq!(
        outcome.selection_precision,
        DenseScorePrecision::Quantized as i32
    );
    assert_eq!(outcome.output_precision, DenseScorePrecision::Fp32 as i32);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_approximates_only_through_a_bound_version_two_policy() {
    let fixture = start_fixture(2, true).await;
    let v1 = b"format_version = 1\npolicy_id = \"court-ann-v1\"\n";
    assert!(load_policy("v1", v1).unwrap_err().contains("version-2"));
    let leaf = || dense_leaf(&fixture.query_vector, Mode::Auto, Precision::Auto);
    let corpus_fp = fixture
        .coordinator
        .live_dense_identity()
        .await
        .unwrap()
        .corpus_fingerprint;
    let stale = load_policy(
        "stale",
        &fake_policy(&fixture.fingerprint, &corpus_fp, GENERATION + 1),
    )
    .unwrap();
    let err = query(
        &fixture
            .coordinator
            .clone()
            .with_dense_execution_policy(stale),
        request(3, 0, leaf()),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message().contains("corpus_generation"),
        "{}",
        err.message()
    );
    let bound = load_policy(
        "bound",
        &fake_policy(&fixture.fingerprint, &corpus_fp, GENERATION),
    )
    .unwrap();
    let coordinator = fixture
        .coordinator
        .clone()
        .with_dense_execution_policy(bound);
    let qualified = query(&coordinator, request(3, 0, leaf())).await.unwrap();
    let outcome = qualified.dense_execution.unwrap();
    assert_eq!(outcome.resolved_mode, Mode::Balanced as i32);
    assert!(outcome.policy_qualified && outcome.preset.is_empty());
    assert_eq!(
        (outcome.policy_id.as_str(), outcome.candidate_depth),
        ("court-ann-v2", 5)
    );
    assert_eq!(outcome.coverage, DenseCoverage::CandidateSet as i32);
    // A key the policy did not measure has no exhaustive technique to fall
    // back to on this provider, so AUTO refuses rather than approximating.
    let err = query(&coordinator, request(2, 0, leaf()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    let exhaustive = start_fixture(2, false).await;
    let filtered_auto = query(
        &exhaustive.coordinator,
        request(
            2,
            0,
            filtered(
                "court == 'ca9'",
                dense_leaf(&exhaustive.query_vector, Mode::Auto, Precision::Auto),
            ),
        ),
    )
    .await
    .unwrap();
    let outcome = filtered_auto.dense_execution.unwrap();
    assert_eq!(outcome.coverage, DenseCoverage::ExhaustiveCertified as i32);
    assert!(filtered_auto.hits.len() <= 2);
}
