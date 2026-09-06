//! `DENSE_EXECUTION_MODE_AUTO` through the generation-bound policy
//! (`docs/dense-execution-policy.md`), against a provider that advertises
//! a configured ANN contract over an exhaustive image
//! (`harness::fake_ann`): AUTO on an exhaustive provider equals EXACT
//! bitwise and consults no policy; AUTO on the fake provider refuses
//! without one; a policy bound to another generation or corpus refuses
//! naming the field; a qualified point runs ANN with its provenance, the
//! same over two shards as over one; a filter keys the point on its live
//! selectivity; a named candidate depth must be a measured one; explicit
//! ANN is approximate and FP32 rerank does not upgrade it.

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
    DenseExecutionMode, DenseExecutionOutcome, DensePolicyPoint, DenseQuery, DenseScoreMode,
    FacetValue, FilterQuery, QueryRequest, QueryResponse, SearchQuery, SelectionOperator,
    SelectionQuery, VectorQualityContract,
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

const POINTS: &str = r#"
[[points]]
k = 3
filter_selectivity_ppm_min = 1000000
filter_selectivity_ppm_max = 1000000
candidates = 5
measured_recall_ppm = 990000

[[points]]
k = 2
filter_selectivity_ppm_min = 400000
filter_selectivity_ppm_max = 600000
candidates = 4
measured_recall_ppm = 980000
"#;

fn policy_text(fingerprint: &str, generation: u64, rows: usize) -> String {
    format!(
        "format_version = 1\npolicy_id = \"court-ann-v1\"\nembedding_model = \"test-embed\"\n\
         corpus_generation = {generation}\ncorpus_rows = {rows}\ndimensions = {DIM}\n\
         provider_backend = \"{}\"\nscoring_fingerprint = \"{fingerprint}\"\n\
         measured_queries = 16\n{POINTS}",
        fake_ann::BACKEND_KIND
    )
}

/// Persist the policy the way an operator would and load it back: the
/// coordinator sees the file, not the text.
fn load_policy(tag: &str, text: &str) -> DenseExecutionPolicy {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("dense_policy_{tag}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("policy.toml");
    std::fs::write(&path, text).unwrap();
    let policy = DenseExecutionPolicy::load(&path).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    policy
}

fn dense_leaf(
    vector: &[f32],
    mode: DenseExecutionMode,
    score_mode: DenseScoreMode,
) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: "vec".into(),
            query: Some(search_query::Query::Dense(DenseQuery {
                vector: vector.to_vec(),
                execution_mode: mode as i32,
                score_mode: score_mode as i32,
                ..Default::default()
            })),
        })),
    }
}

fn cel_filter(cel: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Filter(FilterQuery {
            id: "court".into(),
            predicate: Some(pipestream_search::pb::filter_query::Predicate::Cel(
                cel.to_string(),
            )),
        })),
    }
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

fn qualified_point() -> DensePolicyPoint {
    DensePolicyPoint {
        k: 3,
        filter_selectivity_ppm_min: 1_000_000,
        filter_selectivity_ppm_max: 1_000_000,
        candidates: 5,
        measured_recall_ppm: 990_000,
    }
}

fn assert_ann_through_policy(outcome: &DenseExecutionOutcome, policy: &DenseExecutionPolicy) {
    assert_eq!(
        outcome.evidence_scope,
        pipestream_search::pb::DenseEvidenceScope::SelectivityBandBenchmark as i32
    );
    assert_eq!(outcome.requested_mode, DenseExecutionMode::Auto as i32);
    assert_eq!(outcome.resolved_mode, DenseExecutionMode::Ann as i32);
    assert!(
        !outcome.exhaustive_completion,
        "ANN never claims completion"
    );
    assert_eq!(
        outcome.quality_contract,
        VectorQualityContract::ConfiguredAnn as i32
    );
    assert_eq!(outcome.provider_backend, fake_ann::BACKEND_KIND);
    assert_eq!(outcome.policy_id, "court-ann-v1");
    assert_eq!(outcome.policy_fingerprint, policy.fingerprint());
    assert!(
        outcome.planner_reason.contains("policy \"court-ann-v1\"")
            && outcome.planner_reason.contains("measured recall"),
        "{}",
        outcome.planner_reason
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_on_an_exhaustive_provider_is_exact_bitwise_and_consults_no_policy() {
    let fixture = start_fixture(2, false).await;
    let policy = load_policy(
        "exhaustive",
        &policy_text(&fixture.fingerprint, GENERATION, N_DOCS),
    );
    let coordinator = fixture
        .coordinator
        .clone()
        .with_dense_execution_policy(policy);
    let leaf = |mode| dense_leaf(&fixture.query_vector, mode, DenseScoreMode::Unspecified);
    let exact = query(&coordinator, request(3, 0, leaf(DenseExecutionMode::Exact)))
        .await
        .unwrap();
    let auto = query(&coordinator, request(3, 0, leaf(DenseExecutionMode::Auto)))
        .await
        .unwrap();
    assert_eq!(hit_bits(&exact), hit_bits(&auto));
    assert_eq!(exact.hits.len(), 3);
    let outcome = auto.dense_execution.unwrap();
    assert_eq!(outcome.requested_mode, DenseExecutionMode::Auto as i32);
    assert_eq!(outcome.resolved_mode, DenseExecutionMode::Exact as i32);
    assert!(outcome.exhaustive_completion);
    assert!(outcome.policy_id.is_empty() && outcome.policy_point.is_none());
    assert_eq!(outcome.candidate_depth, 0);
    assert_eq!(outcome.filter_selectivity_ppm, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_on_a_configured_ann_provider_refuses_without_a_policy() {
    let fixture = start_fixture(2, true).await;
    let leaf = dense_leaf(
        &fixture.query_vector,
        DenseExecutionMode::Auto,
        DenseScoreMode::Unspecified,
    );
    let error = query(&fixture.coordinator, request(3, 0, leaf))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error.message().contains("fake-ann")
            && error.message().contains("--dense-execution-policy"),
        "{}",
        error.message()
    );
    // EXACT on that provider is refused too: it cannot prove completion.
    let leaf = dense_leaf(
        &fixture.query_vector,
        DenseExecutionMode::Exact,
        DenseScoreMode::Unspecified,
    );
    let error = query(&fixture.coordinator, request(3, 0, leaf))
        .await
        .unwrap_err();
    assert!(error.message().contains("EXACT"), "{}", error.message());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_refuses_a_policy_bound_to_another_generation_or_corpus() {
    let fixture = start_fixture(2, true).await;
    let leaf = || {
        dense_leaf(
            &fixture.query_vector,
            DenseExecutionMode::Auto,
            DenseScoreMode::Unspecified,
        )
    };
    // The policy measured generation 8; the coordinator serves 7.
    let stale = load_policy(
        "generation",
        &policy_text(&fixture.fingerprint, GENERATION + 1, N_DOCS),
    );
    let coordinator = fixture
        .coordinator
        .clone()
        .with_dense_execution_policy(stale);
    let error = query(&coordinator, request(3, 0, leaf()))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error
            .message()
            .contains("corpus_generation 8 does not match live 7"),
        "{}",
        error.message()
    );
    // The policy measured one row more than the shards hold.
    let grown = load_policy(
        "rows",
        &policy_text(&fixture.fingerprint, GENERATION, N_DOCS + 1),
    );
    let coordinator = fixture
        .coordinator
        .clone()
        .with_dense_execution_policy(grown);
    let error = query(&coordinator, request(3, 0, leaf()))
        .await
        .unwrap_err();
    assert!(
        error
            .message()
            .contains("corpus_rows 9 does not match live 8"),
        "{}",
        error.message()
    );
    // Another provider's scoring fingerprint.
    let foreign = load_policy(
        "fingerprint",
        &policy_text("fake-ann:elsewhere", GENERATION, N_DOCS),
    );
    let coordinator = fixture
        .coordinator
        .clone()
        .with_dense_execution_policy(foreign);
    let error = query(&coordinator, request(3, 0, leaf()))
        .await
        .unwrap_err();
    assert!(
        error.message().contains("scoring_fingerprint"),
        "{}",
        error.message()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_through_a_qualified_point_runs_ann_and_reports_it() {
    let distributed = start_fixture(2, true).await;
    let monolithic = start_fixture(1, true).await;
    assert_eq!(distributed.fingerprint, monolithic.fingerprint);
    let text = policy_text(&distributed.fingerprint, GENERATION, N_DOCS);
    let policy = load_policy("qualified", &text);
    let mut responses = Vec::new();
    for fixture in [&distributed, &monolithic] {
        let coordinator = fixture
            .coordinator
            .clone()
            .with_dense_execution_policy(policy.clone());
        let leaf = dense_leaf(
            &fixture.query_vector,
            DenseExecutionMode::Auto,
            DenseScoreMode::Unspecified,
        );
        let response = query(&coordinator, request(3, 0, leaf)).await.unwrap();
        assert_eq!(
            response.hits.len(),
            3,
            "trimmed to k from the point's depth"
        );
        let outcome = response.dense_execution.clone().unwrap();
        assert_ann_through_policy(&outcome, &policy);
        assert_eq!(outcome.policy_point, Some(qualified_point()));
        assert_eq!(outcome.filter_selectivity_ppm, 1_000_000);
        assert_eq!(outcome.candidate_depth, 5);
        // An unmeasured k refuses by name; k = 0 is not defaulted.
        let leaf = dense_leaf(
            &fixture.query_vector,
            DenseExecutionMode::Auto,
            DenseScoreMode::Unspecified,
        );
        let error = query(&coordinator, request(4, 0, leaf)).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(
            error.message().contains("no point for k=4") && error.message().contains("[2, 3]"),
            "{}",
            error.message()
        );
        let leaf = dense_leaf(
            &fixture.query_vector,
            DenseExecutionMode::Auto,
            DenseScoreMode::Unspecified,
        );
        let error = query(&coordinator, request(0, 0, leaf)).await.unwrap_err();
        assert!(
            error.message().contains("explicit k"),
            "{}",
            error.message()
        );
        responses.push(response);
    }
    // Distributed = monolithic: the hits and the provenance.
    assert_eq!(hit_bits(&responses[0]), hit_bits(&responses[1]));
    assert_eq!(responses[0].dense_execution, responses[1].dense_execution);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_filter_keys_the_point_on_its_live_selectivity() {
    let fixture = start_fixture(2, true).await;
    let policy = load_policy(
        "filtered",
        &policy_text(&fixture.fingerprint, GENERATION, N_DOCS),
    );
    let coordinator = fixture
        .coordinator
        .clone()
        .with_dense_execution_policy(policy.clone());
    let leaf = || {
        dense_leaf(
            &fixture.query_vector,
            DenseExecutionMode::Auto,
            DenseScoreMode::Unspecified,
        )
    };
    // ca9 admits four of eight rows: 500,000 ppm, inside the measured band.
    let response = query(
        &coordinator,
        request(2, 0, filtered("court == \"ca9\"", leaf())),
    )
    .await
    .unwrap();
    assert_eq!(response.hits.len(), 2);
    assert!(
        response.hits.iter().all(|h| h.doc_id < 4),
        "{:?}",
        response.hits
    );
    let outcome = response.dense_execution.unwrap();
    assert_ann_through_policy(&outcome, &policy);
    assert_eq!(outcome.filter_selectivity_ppm, 500_000);
    assert_eq!(outcome.candidate_depth, 4);
    assert_eq!(
        outcome.policy_point.map(|p| (p.k, p.candidates)),
        Some((2, 4))
    );
    // scotus admits two of eight: 250,000 ppm, outside every band for k = 2.
    let error = query(
        &coordinator,
        request(2, 0, filtered("court == \"scotus\"", leaf())),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error.message().contains("250000 ppm") && error.message().contains("400000..=600000"),
        "{}",
        error.message()
    );
    // Unfiltered k = 2 was not measured either: the band is not stretched.
    let error = query(&coordinator, request(2, 0, leaf()))
        .await
        .unwrap_err();
    assert!(
        error.message().contains("1000000 ppm"),
        "{}",
        error.message()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_named_candidate_depth_must_be_a_measured_one() {
    let fixture = start_fixture(2, true).await;
    let policy = load_policy(
        "depth",
        &policy_text(&fixture.fingerprint, GENERATION, N_DOCS),
    );
    let coordinator = fixture
        .coordinator
        .clone()
        .with_dense_execution_policy(policy);
    let leaf = || {
        dense_leaf(
            &fixture.query_vector,
            DenseExecutionMode::Auto,
            DenseScoreMode::Unspecified,
        )
    };
    let error = query(&coordinator, request(3, 6, leaf()))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error.message().contains("not at the requested 6"),
        "{}",
        error.message()
    );
    let response = query(&coordinator, request(3, 5, leaf())).await.unwrap();
    let outcome = response.dense_execution.unwrap();
    assert_eq!(outcome.resolved_mode, DenseExecutionMode::Ann as i32);
    assert_eq!(outcome.candidate_depth, 5);
    assert_eq!(response.hits.len(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_ann_is_approximate_and_fp32_rerank_does_not_upgrade_it() {
    let fixture = start_fixture(2, true).await;
    // Explicit ANN needs no policy: the caller accepted the contract.
    let leaf = dense_leaf(
        &fixture.query_vector,
        DenseExecutionMode::Ann,
        DenseScoreMode::Fp32Rerank,
    );
    let response = query(&fixture.coordinator, request(3, 5, leaf))
        .await
        .unwrap();
    assert_eq!(response.hits.len(), 3);
    let outcome = response.dense_execution.unwrap();
    assert_eq!(outcome.requested_mode, DenseExecutionMode::Ann as i32);
    assert_eq!(outcome.resolved_mode, DenseExecutionMode::Ann as i32);
    assert!(!outcome.exhaustive_completion);
    assert_eq!(
        outcome.quality_contract,
        VectorQualityContract::ConfiguredAnn as i32
    );
    assert!(
        outcome.planner_reason.contains("approximate")
            && outcome.planner_reason.contains("FP32 rerank"),
        "{}",
        outcome.planner_reason
    );
    assert!(outcome.policy_id.is_empty(), "no policy was consulted");
    // AUTO through the policy with FP32 rerank on top: still ANN.
    let policy = load_policy(
        "rerank",
        &policy_text(&fixture.fingerprint, GENERATION, N_DOCS),
    );
    let coordinator = fixture
        .coordinator
        .clone()
        .with_dense_execution_policy(policy.clone());
    let leaf = dense_leaf(
        &fixture.query_vector,
        DenseExecutionMode::Auto,
        DenseScoreMode::Fp32Rerank,
    );
    let response = query(&coordinator, request(3, 0, leaf)).await.unwrap();
    let outcome = response.dense_execution.unwrap();
    assert_ann_through_policy(&outcome, &policy);
    assert!(
        outcome.planner_reason.contains("FP32 rerank"),
        "{}",
        outcome.planner_reason
    );
    assert_eq!(outcome.candidate_depth, 5);
    assert_eq!(response.hits.len(), 3);
}
