//! The measured dense quality profile (`docs/dense-quality-profile.md`):
//! `quality::measure` produces a version 2 profile from the public
//! `Query` route over two shards, the profile loads and its points hold
//! when re-checked query by query, the two-shard ladder equals the
//! single-shard ladder, and `DENSE_EXECUTION_MODE_AUTO` with FP32 rerank
//! resolves its candidate depth through the profile's default target
//! bitwise as an explicit `DenseQualityPolicy` would — with provenance,
//! at the measured depth (the cost gate), across a shard reopen. Without
//! a profile, without a default, or on identity drift it refuses by
//! name; EXACT and UNSPECIFIED keep the caller's pool at `k`.

mod common;

use std::path::PathBuf;

use common::{fit_calibration, start_empty_node, start_opened_node, unit_vectors};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    search_query, selection_query, AddVectorsRequest, DeleteDocumentsRequest, DenseExecutionMode,
    DenseQualityPolicy, DenseQuery, DenseScoreMode, FlushRequest, HealthRequest, QueryRequest,
    QueryResponse, SearchQuery, SelectionQuery, SetCalibrationRequest,
};
use pipestream_search::quality::measure::{
    ladder_table, measure, GroundTruth, MeasureSpec, MeasuredProfile,
};
use pipestream_search::quality::DenseQualityProfile;
use tonic::Request;

const DIM: usize = 32;
const ROWS: usize = 4096;
const QUERIES: usize = 8;
const K: u32 = 10;
const DEPTHS: [u32; 7] = [10, 20, 40, 80, 160, 640, ROWS as u32];
const TARGETS: [u32; 4] = [950_000, 990_000, 999_000, 1_000_000];
const GENERATION: u64 = 5;
const DEFAULT_TARGET: u32 = 990_000;

struct Fixture {
    dir: PathBuf,
    /// `(index path, slot offset)` per shard, for reopening.
    shards: Vec<(PathBuf, u64)>,
    addrs: Vec<String>,
    handles: Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
    corpus: Vec<f32>,
    queries: Vec<f32>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// The same 4096-row corpus (one calibration, fitted on all of it) laid
/// out over `rows_per_shard`, each shard flushed so its FP32 sidecar is
/// the mapped one; held-out queries are a second seeded unit corpus.
async fn start_fixture(tag: &str, rows_per_shard: &[usize]) -> Fixture {
    assert_eq!(rows_per_shard.iter().sum::<usize>(), ROWS);
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("dense_quality_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let corpus = unit_vectors(ROWS, DIM, 0xD05E_0001);
    let queries = unit_vectors(QUERIES, DIM, 0xD05E_0002);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let mut fixture = Fixture {
        dir,
        shards: Vec::new(),
        addrs: Vec::new(),
        handles: Vec::new(),
        corpus,
        queries,
    };
    let mut start = 0usize;
    for (shard, &rows) in rows_per_shard.iter().enumerate() {
        let index_path = fixture.dir.join(format!("shard-{shard}.vector"));
        let (addr, handle) = start_empty_node(NodeConfig {
            index_path: Some(index_path.clone()),
            slot_offset: start as u64,
            ..Default::default()
        })
        .await;
        let mut node = NodeServiceClient::connect(addr.clone()).await.unwrap();
        node.set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: 4,
            shift: shift.clone(),
            scale: scale.clone(),
        })
        .await
        .unwrap();
        node.add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
            vectors: fixture.corpus[start * DIM..(start + rows) * DIM].to_vec(),
            dim: DIM as u32,
        }]))
        .await
        .unwrap();
        node.flush(FlushRequest {}).await.unwrap();
        fixture.shards.push((index_path, start as u64));
        fixture.addrs.push(addr);
        fixture.handles.push(handle);
        start += rows;
    }
    fixture
}

impl Fixture {
    fn coordinator(&self) -> CoordinatorServiceImpl {
        CoordinatorServiceImpl::new(self.addrs.clone())
            .with_max_k(ROWS as u32)
            .with_topology_generation(GENERATION)
    }

    /// Stop every node and reopen it from its persisted image (the
    /// recovery path the serving binary takes).
    async fn reopen(&mut self) {
        for handle in self.handles.drain(..) {
            handle.abort();
        }
        self.addrs.clear();
        for (index_path, slot_offset) in &self.shards {
            let (addr, handle) = start_opened_node(NodeConfig {
                index_path: Some(index_path.clone()),
                slot_offset: *slot_offset,
                ..Default::default()
            })
            .await;
            self.addrs.push(addr);
            self.handles.push(handle);
        }
    }

    async fn shard_health(&self, shard: usize) -> pipestream_search::pb::HealthResponse {
        NodeServiceClient::connect(self.addrs[shard].clone())
            .await
            .unwrap()
            .health(HealthRequest {})
            .await
            .unwrap()
            .into_inner()
    }

    fn spec<'a>(
        &'a self,
        ground_truth: GroundTruth<'a>,
        default_target_recall_ppm: Option<u32>,
    ) -> MeasureSpec<'a> {
        MeasureSpec {
            collection: String::new(),
            profile_id: "held-out-4096".into(),
            embedding_model: "test-unit-vectors".into(),
            queries: &self.queries,
            dimensions: DIM as u32,
            ks: vec![K],
            depths: DEPTHS.to_vec(),
            targets: TARGETS.to_vec(),
            default_target_recall_ppm,
            ground_truth,
        }
    }

    fn query(&self, index: usize) -> &[f32] {
        &self.queries[index * DIM..(index + 1) * DIM]
    }
}

fn dense_leaf(
    vector: &[f32],
    execution: DenseExecutionMode,
    score: DenseScoreMode,
    quality: Option<DenseQualityPolicy>,
) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: "vec".into(),
            query: Some(search_query::Query::Dense(DenseQuery {
                vector: vector.to_vec(),
                execution_mode: execution as i32,
                score_mode: score as i32,
                quality,
            })),
        })),
    }
}

fn request(k: u32, selection_k: u32, selection: SelectionQuery) -> QueryRequest {
    QueryRequest {
        k,
        selection_k,
        selection: Some(selection),
        profile: true,
        ..Default::default()
    }
}

fn policy(target_recall_ppm: u32) -> DenseQualityPolicy {
    DenseQualityPolicy {
        target_recall_ppm,
        max_candidates: 0,
        required_profile_fingerprint: String::new(),
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

fn hit_bits(response: &QueryResponse) -> Vec<(u64, u32, u32)> {
    response
        .hits
        .iter()
        .map(|h| (h.doc_id, h.score.to_bits(), h.rank))
        .collect()
}

/// The recall-bearing part of a ladder: everything but the timings.
fn recall_ladder(measured: &MeasuredProfile) -> Vec<(u32, u32, u32, u32, u32, Vec<u32>)> {
    measured
        .ladder
        .iter()
        .map(|row| {
            let m = &row.measurement;
            (
                m.k,
                m.candidates,
                m.queries,
                m.mean_recall_ppm,
                m.min_recall_ppm,
                row.recall_ppm.clone(),
            )
        })
        .collect()
}

/// `|top-k at selection_k| ∩ exhaustive top-k|` in parts per million of
/// `k`, both through the public route.
async fn recall_ppm(
    coordinator: &CoordinatorServiceImpl,
    vector: &[f32],
    k: u32,
    selection_k: u32,
) -> u32 {
    let leaf = || {
        dense_leaf(
            vector,
            DenseExecutionMode::Exact,
            DenseScoreMode::Fp32Rerank,
            None,
        )
    };
    let truth = query(coordinator, request(k, ROWS as u32, leaf()))
        .await
        .unwrap();
    let at_depth = query(coordinator, request(k, selection_k, leaf()))
        .await
        .unwrap();
    let truth: std::collections::HashSet<u64> = truth.hits.iter().map(|h| h.doc_id).collect();
    let hits = at_depth
        .hits
        .iter()
        .filter(|h| truth.contains(&h.doc_id))
        .count() as u64;
    (hits * 1_000_000 / u64::from(k)) as u32
}

async fn measure_and_save(
    fixture: &Fixture,
    ground_truth: GroundTruth<'_>,
    default_target_recall_ppm: Option<u32>,
    name: &str,
) -> (MeasuredProfile, DenseQualityProfile, PathBuf) {
    let mut coordinator = fixture.coordinator();
    let measured = measure(
        &mut coordinator,
        &fixture.spec(ground_truth, default_target_recall_ppm),
    )
    .await
    .unwrap();
    let path = fixture.dir.join(name);
    measured.profile.save(&path).unwrap();
    let loaded = DenseQualityProfile::load(&path).unwrap();
    (measured, loaded, path)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn measurement_produces_a_loadable_profile_whose_points_hold_when_rechecked() {
    let fixture = start_fixture("measure", &[2048, 2048]).await;
    let (measured, loaded, _) =
        measure_and_save(&fixture, GroundTruth::FullDepth, None, "full-depth.toml").await;
    print!("{}", ladder_table(&measured));

    // Identity: the live generation, as the coordinator's preflight sees it.
    let health = fixture.shard_health(0).await;
    assert_eq!(measured.identity.provider_backend, health.vector_backend);
    assert_eq!(
        measured.identity.scoring_fingerprint,
        health.scoring_fingerprint
    );
    assert_eq!(measured.identity.dimensions, DIM as u32);
    assert_eq!(measured.identity.rows, ROWS as u64);
    assert_eq!(measured.identity.topology_generation, GENERATION);
    let identity = loaded.identity();
    assert_eq!(identity.corpus_generation, GENERATION);
    assert_eq!(identity.corpus_rows, ROWS as u64);
    assert_eq!(identity.provider_backend, health.vector_backend);
    assert_eq!(identity.scoring_fingerprint, health.scoring_fingerprint);
    assert_eq!(loaded.measured_queries(), QUERIES as u32);
    assert_eq!(loaded.default_target_recall_ppm(), None);

    // The ladder: one rung per depth, every query counted, the worst query
    // at or below the mean, recall monotone in depth (a deeper pool is a
    // superset: |truth ∩ pool| cannot fall), and the full-depth rung exact.
    assert_eq!(measured.ladder.len(), DEPTHS.len());
    for (rung, &depth) in measured.ladder.iter().zip(&DEPTHS) {
        let m = &rung.measurement;
        assert_eq!((m.k, m.candidates, m.queries), (K, depth, QUERIES as u32));
        assert_eq!(rung.recall_ppm.len(), QUERIES);
        assert!(m.min_recall_ppm <= m.mean_recall_ppm);
        assert_eq!(m.min_recall_ppm, *rung.recall_ppm.iter().min().unwrap());
        assert!(m.p50_total_ms >= m.p50_rerank_ms && m.p50_rerank_ms >= 0.0);
    }
    for pair in measured.ladder.windows(2) {
        for q in 0..QUERIES {
            assert!(pair[0].recall_ppm[q] <= pair[1].recall_ppm[q]);
        }
    }
    let full = &measured.ladder.last().unwrap().measurement;
    assert_eq!(
        (full.mean_recall_ppm, full.min_recall_ppm),
        (1_000_000, 1_000_000)
    );
    assert_eq!(loaded.measurements(), full_ladder(&measured).as_slice());

    // Every target is either a point or reported unmet, never invented.
    assert_eq!(
        loaded.points().len() + measured.unmet.len(),
        TARGETS.len(),
        "unmet: {:?}",
        measured.unmet
    );
    assert!(
        loaded
            .points()
            .iter()
            .any(|p| p.target_recall_ppm == 1_000_000 && p.candidates == ROWS as u32)
            || loaded
                .points()
                .iter()
                .any(|p| p.target_recall_ppm == 1_000_000)
    );

    // Re-check every point through the public route: at the point's depth
    // every held-out query meets the target (the promise), and the rung
    // below it did not (the depth is the smallest measured one).
    let coordinator = fixture.coordinator();
    for point in loaded.points() {
        for q in 0..QUERIES {
            let recall =
                recall_ppm(&coordinator, fixture.query(q), point.k, point.candidates).await;
            assert!(
                recall >= point.target_recall_ppm,
                "k={} target={} candidates={} query {q} recall {recall}",
                point.k,
                point.target_recall_ppm,
                point.candidates
            );
        }
        let position = DEPTHS.iter().position(|&d| d == point.candidates).unwrap();
        if position > 0 {
            let below = &measured.ladder[position - 1].measurement;
            assert!(
                below.min_recall_ppm < point.target_recall_ppm,
                "a shallower rung ({} candidates, min {}) already met target {}",
                below.candidates,
                below.min_recall_ppm,
                point.target_recall_ppm
            );
        }
    }

    // The saved document is the profile: same bytes, same fingerprint, and
    // the same resolution as the in-memory one.
    assert_eq!(loaded.to_toml(), measured.profile.to_toml());
    assert_eq!(loaded.fingerprint(), measured.profile.fingerprint());
    for point in loaded.points() {
        assert_eq!(
            loaded
                .resolve(point.k, point.target_recall_ppm, "", 0)
                .unwrap(),
            measured
                .profile
                .resolve(point.k, point.target_recall_ppm, "", 0)
                .unwrap()
        );
    }

    // Brute ground truth over the corpus rows (the rerank's own dot
    // product) is the same exhaustive order: an identical ladder.
    let mut coordinator = fixture.coordinator();
    let brute = measure(
        &mut coordinator,
        &fixture.spec(
            GroundTruth::Brute {
                rows: &fixture.corpus,
            },
            None,
        ),
    )
    .await
    .unwrap();
    assert_eq!(recall_ladder(&brute), recall_ladder(&measured));
    assert_eq!(brute.profile.points(), loaded.points());
    assert_eq!(brute.unmet, measured.unmet);
}

fn full_ladder(measured: &MeasuredProfile) -> Vec<pipestream_search::quality::ProfileMeasurement> {
    measured
        .ladder
        .iter()
        .map(|row| row.measurement.clone())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_distributed_measurement_equals_the_single_shard_measurement() {
    let two = start_fixture("two", &[2048, 2048]).await;
    let one = start_fixture("one", &[ROWS]).await;
    let (two_measured, two_profile, _) = measure_and_save(
        &two,
        GroundTruth::FullDepth,
        Some(DEFAULT_TARGET),
        "two.toml",
    )
    .await;
    let (one_measured, one_profile, _) = measure_and_save(
        &one,
        GroundTruth::FullDepth,
        Some(DEFAULT_TARGET),
        "one.toml",
    )
    .await;
    assert_eq!(two_measured.identity, one_measured.identity);
    assert_eq!(recall_ladder(&two_measured), recall_ladder(&one_measured));
    assert_eq!(two_profile.points(), one_profile.points());
    assert_eq!(two_measured.unmet, one_measured.unmet);

    // The profiles then drive AUTO to the same depth and the same hits.
    let two_auto = two.coordinator().with_dense_quality_profile(two_profile);
    let one_auto = one.coordinator().with_dense_quality_profile(one_profile);
    for q in 0..QUERIES {
        let leaf = dense_leaf(
            two.query(q),
            DenseExecutionMode::Auto,
            DenseScoreMode::Fp32Rerank,
            None,
        );
        let a = query(&two_auto, request(K, 0, leaf.clone())).await.unwrap();
        let b = query(&one_auto, request(K, 0, leaf)).await.unwrap();
        assert_eq!(hit_bits(&a), hit_bits(&b));
        // The two files differ only in their timing rows, hence in their
        // fingerprints; every recall-bearing field agrees.
        let mut a_quality = a.dense_quality.clone().unwrap();
        let mut b_quality = b.dense_quality.clone().unwrap();
        assert_ne!(a_quality.profile_fingerprint, b_quality.profile_fingerprint);
        a_quality.profile_fingerprint.clear();
        b_quality.profile_fingerprint.clear();
        assert_eq!(a_quality, b_quality);
        assert_eq!(
            a.profile.as_ref().unwrap().rerank_rows,
            b.profile.as_ref().unwrap().rerank_rows
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_with_fp32_rerank_resolves_the_depth_through_the_profile_default() {
    let mut fixture = start_fixture("auto", &[2048, 2048]).await;
    let (measured, loaded, path) = measure_and_save(
        &fixture,
        GroundTruth::FullDepth,
        Some(DEFAULT_TARGET),
        "default.toml",
    )
    .await;
    assert_eq!(loaded.default_target_recall_ppm(), Some(DEFAULT_TARGET));
    let point = *loaded
        .points()
        .iter()
        .find(|p| p.k == K && p.target_recall_ppm == DEFAULT_TARGET)
        .expect("the default target has a point");
    let coordinator = fixture
        .coordinator()
        .with_dense_quality_profile(loaded.clone());

    let mut auto_hits = Vec::new();
    for q in 0..QUERIES {
        let vector = fixture.query(q);
        let auto = query(
            &coordinator,
            request(
                K,
                0,
                dense_leaf(
                    vector,
                    DenseExecutionMode::Auto,
                    DenseScoreMode::Fp32Rerank,
                    None,
                ),
            ),
        )
        .await
        .unwrap();
        let explicit = query(
            &coordinator,
            request(
                K,
                0,
                dense_leaf(
                    vector,
                    DenseExecutionMode::Auto,
                    DenseScoreMode::Fp32Rerank,
                    Some(policy(DEFAULT_TARGET)),
                ),
            ),
        )
        .await
        .unwrap();
        let at_depth = query(
            &coordinator,
            request(
                K,
                point.candidates,
                dense_leaf(
                    vector,
                    DenseExecutionMode::Exact,
                    DenseScoreMode::Fp32Rerank,
                    None,
                ),
            ),
        )
        .await
        .unwrap();
        assert_eq!(hit_bits(&auto), hit_bits(&explicit));
        assert_eq!(hit_bits(&auto), hit_bits(&at_depth));
        assert_eq!(auto.executed, "search:fp32_rerank");

        // Provenance: the same outcome an explicit policy reports, plus the
        // planner naming the profile and default that chose the depth.
        let outcome = auto.dense_quality.as_ref().expect("dense_quality set");
        assert_eq!(auto.dense_quality, explicit.dense_quality);
        assert_eq!(outcome.target_recall_ppm, DEFAULT_TARGET);
        assert_eq!(outcome.selection_k, point.candidates);
        assert_eq!(outcome.profile_id, "held-out-4096");
        assert_eq!(outcome.profile_fingerprint, loaded.fingerprint());
        assert_eq!(outcome.corpus_generation, GENERATION);
        assert_eq!(outcome.corpus_rows, ROWS as u64);
        let execution = auto.dense_execution.as_ref().unwrap();
        assert_eq!(execution.requested_mode, DenseExecutionMode::Auto as i32);
        assert_eq!(execution.resolved_mode, DenseExecutionMode::Exact as i32);
        assert!(execution.exhaustive_completion);
        let expected_reason = format!(
            "FP32 rerank depth selection_k={} resolved through quality profile \"held-out-4096\" \
             default_target_recall_ppm={DEFAULT_TARGET}",
            point.candidates
        );
        assert!(
            execution.planner_reason.contains(&expected_reason),
            "{}",
            execution.planner_reason
        );
        assert!(
            !explicit
                .dense_execution
                .unwrap()
                .planner_reason
                .contains("default_target"),
            "an explicit policy is not attributed to the default"
        );

        // The cost gate: the rerank ran over the measured depth, not k. A
        // regression to selection_k = k would show here as rerank_rows = k.
        let profile = auto.profile.as_ref().unwrap();
        assert_eq!(profile.rerank_rows, u64::from(point.candidates));
        assert!(profile.rerank_logical_bytes >= u64::from(point.candidates) * (DIM as u64) * 4);
        assert_eq!(auto.hits.len(), K as usize);
        auto_hits.push(hit_bits(&auto));
    }
    // The measured ladder said this depth was needed; the gate is only
    // meaningful when it exceeds k, which the seeded corpus guarantees at
    // 99% on the worst query (printed for the record).
    print!("{}", ladder_table(&measured));
    assert!(
        point.candidates > K,
        "the ladder resolved the default at selection_k = k; the gate would not bite"
    );

    // Persistence: the shards reopened from their images and the profile
    // reloaded from its file serve bitwise the same AUTO result.
    fixture.reopen().await;
    let reopened = fixture
        .coordinator()
        .with_dense_quality_profile(DenseQualityProfile::load(&path).unwrap());
    for (q, before) in auto_hits.iter().enumerate() {
        let after = query(
            &reopened,
            request(
                K,
                0,
                dense_leaf(
                    fixture.query(q),
                    DenseExecutionMode::Auto,
                    DenseScoreMode::Fp32Rerank,
                    None,
                ),
            ),
        )
        .await
        .unwrap();
        assert_eq!(&hit_bits(&after), before);
        assert_eq!(after.dense_quality.unwrap().selection_k, point.candidates);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exact_and_unspecified_with_fp32_rerank_keep_the_pool_at_k() {
    let fixture = start_fixture("exact", &[2048, 2048]).await;
    let (_, loaded, _) = measure_and_save(
        &fixture,
        GroundTruth::FullDepth,
        Some(DEFAULT_TARGET),
        "default.toml",
    )
    .await;
    let coordinator = fixture.coordinator().with_dense_quality_profile(loaded);
    let vector = fixture.query(0);
    let explicit_k = query(
        &coordinator,
        request(
            K,
            K,
            dense_leaf(
                vector,
                DenseExecutionMode::Exact,
                DenseScoreMode::Fp32Rerank,
                None,
            ),
        ),
    )
    .await
    .unwrap();
    assert!(explicit_k.dense_quality.is_none());
    assert_eq!(
        explicit_k.profile.as_ref().unwrap().rerank_rows,
        u64::from(K)
    );
    for mode in [DenseExecutionMode::Exact, DenseExecutionMode::Unspecified] {
        let response = query(
            &coordinator,
            request(
                K,
                0,
                dense_leaf(vector, mode, DenseScoreMode::Fp32Rerank, None),
            ),
        )
        .await
        .unwrap();
        assert_eq!(hit_bits(&response), hit_bits(&explicit_k));
        assert!(response.dense_quality.is_none(), "{mode:?}");
        assert_eq!(response.profile.as_ref().unwrap().rerank_rows, u64::from(K));
        let execution = response.dense_execution.unwrap();
        assert_eq!(execution.resolved_mode, DenseExecutionMode::Exact as i32);
        assert!(!execution.planner_reason.contains("quality profile"));
    }

    // AUTO with an explicit selection_k is the caller's depth, not the
    // profile's; AUTO with native scoring never consults the profile.
    let named = query(
        &coordinator,
        request(
            K,
            40,
            dense_leaf(
                vector,
                DenseExecutionMode::Auto,
                DenseScoreMode::Fp32Rerank,
                None,
            ),
        ),
    )
    .await
    .unwrap();
    assert!(named.dense_quality.is_none());
    assert_eq!(named.profile.as_ref().unwrap().rerank_rows, 40);
    let native = query(
        &coordinator,
        request(
            K,
            0,
            dense_leaf(
                vector,
                DenseExecutionMode::Auto,
                DenseScoreMode::Native,
                None,
            ),
        ),
    )
    .await
    .unwrap();
    assert!(native.dense_quality.is_none());
    assert_eq!(native.profile.as_ref().unwrap().rerank_rows, 0);
    assert_eq!(
        native.dense_execution.unwrap().resolved_mode,
        DenseExecutionMode::Exact as i32
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_with_fp32_rerank_refuses_without_a_profile_or_default_and_on_drift() {
    const NEEDS: &str = "AUTO with FP32 rerank needs a measured quality profile with \
                         default_target_recall_ppm, or an explicit DenseQualityPolicy or \
                         selection_k";
    let fixture = start_fixture("refuse", &[2048, 2048]).await;
    let (_, loaded, path) = measure_and_save(
        &fixture,
        GroundTruth::FullDepth,
        Some(DEFAULT_TARGET),
        "default.toml",
    )
    .await;
    let vector = fixture.query(0);
    let auto = || {
        request(
            K,
            0,
            dense_leaf(
                vector,
                DenseExecutionMode::Auto,
                DenseScoreMode::Fp32Rerank,
                None,
            ),
        )
    };

    // No profile at all.
    let err = query(&fixture.coordinator(), auto()).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains(NEEDS), "{}", err.message());
    assert!(
        err.message().contains("no --dense-quality-profile"),
        "{}",
        err.message()
    );

    // A version 1 profile (points only) still serves an explicit policy
    // exactly as before and has no default for AUTO to resolve through.
    let health = fixture.shard_health(0).await;
    let v1_path = fixture.dir.join("v1.toml");
    std::fs::write(
        &v1_path,
        format!(
            "format_version = 1\nprofile_id = \"v1-held-out\"\nembedding_model = \"test-unit-vectors\"\n\
             corpus_generation = {GENERATION}\ncorpus_rows = {ROWS}\ndimensions = {DIM}\n\
             provider_backend = \"{}\"\nscoring_fingerprint = \"{}\"\nmeasured_queries = 8\n\n\
             [[points]]\nk = {K}\ntarget_recall_ppm = 1000000\ncandidates = {ROWS}\n",
            health.vector_backend, health.scoring_fingerprint
        ),
    )
    .unwrap();
    let v1 = fixture
        .coordinator()
        .with_dense_quality_profile(DenseQualityProfile::load(&v1_path).unwrap());
    let err = query(&v1, auto()).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains(NEEDS), "{}", err.message());
    assert!(
        err.message()
            .contains("profile \"v1-held-out\" carries no default_target_recall_ppm"),
        "{}",
        err.message()
    );
    let explicit = query(
        &v1,
        request(
            K,
            0,
            dense_leaf(
                vector,
                DenseExecutionMode::Auto,
                DenseScoreMode::Fp32Rerank,
                Some(policy(1_000_000)),
            ),
        ),
    )
    .await
    .unwrap();
    assert_eq!(explicit.dense_quality.unwrap().selection_k, ROWS as u32);

    // The measured profile with its default: identity drift still refuses
    // — a profile does not bleed across generations, row counts, or score
    // spaces.
    let text = std::fs::read_to_string(&path).unwrap();
    for (from, to, needle) in [
        (
            format!("corpus_rows = {ROWS}"),
            format!("corpus_rows = {}", ROWS + 1),
            "generation mismatch",
        ),
        (
            format!("scoring_fingerprint = \"{}\"", health.scoring_fingerprint),
            "scoring_fingerprint = \"other-score-space\"".to_string(),
            "scoring_fingerprint \"other-score-space\" does not match live",
        ),
    ] {
        assert!(text.contains(&from), "{from}");
        let drifted_path = fixture.dir.join("drifted.toml");
        std::fs::write(&drifted_path, text.replacen(&from, &to, 1)).unwrap();
        let drifted = fixture
            .coordinator()
            .with_dense_quality_profile(DenseQualityProfile::load(&drifted_path).unwrap());
        let err = query(&drifted, auto()).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains(needle), "{}", err.message());
    }
    let other_generation = CoordinatorServiceImpl::new(fixture.addrs.clone())
        .with_max_k(ROWS as u32)
        .with_topology_generation(GENERATION + 1)
        .with_dense_quality_profile(loaded.clone());
    let err = query(&other_generation, auto()).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message().contains("generation mismatch"),
        "{}",
        err.message()
    );

    // Request shapes the rule does not cover keep their existing refusals.
    let coordinator = fixture.coordinator().with_dense_quality_profile(loaded);
    let err = query(
        &coordinator,
        request(
            K,
            40,
            dense_leaf(
                vector,
                DenseExecutionMode::Auto,
                DenseScoreMode::Fp32Rerank,
                Some(policy(DEFAULT_TARGET)),
            ),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("competing depth authorities"),
        "{}",
        err.message()
    );
    let err = query(
        &coordinator,
        request(
            0,
            0,
            dense_leaf(
                vector,
                DenseExecutionMode::Auto,
                DenseScoreMode::Fp32Rerank,
                None,
            ),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("requires explicit k"),
        "{}",
        err.message()
    );
    let err = query(
        &coordinator,
        request(
            K,
            0,
            dense_leaf(
                vector,
                DenseExecutionMode::Auto,
                DenseScoreMode::Fp32Rerank,
                Some(policy(999_999)),
            ),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("no measured point"),
        "{}",
        err.message()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_measurement_refuses_shapes_it_cannot_certify() {
    let fixture = start_fixture("shapes", &[2048, 2048]).await;
    let mut coordinator = fixture.coordinator();
    async fn refusal(spec: MeasureSpec<'_>, coordinator: &mut CoordinatorServiceImpl) -> String {
        measure(coordinator, &spec).await.unwrap_err()
    }

    let mut spec = fixture.spec(GroundTruth::FullDepth, None);
    spec.depths.push(ROWS as u32 + 1);
    let error = refusal(spec, &mut coordinator).await;
    assert!(error.contains("exceeds the corpus rows 4096"), "{error}");

    let mut spec = fixture.spec(GroundTruth::FullDepth, None);
    spec.ks = vec![K, 2000];
    spec.depths = vec![10, 100];
    let error = refusal(spec, &mut coordinator).await;
    assert!(
        error.contains("k=2000 has no depth at or above it"),
        "{error}"
    );

    let mut spec = fixture.spec(GroundTruth::FullDepth, None);
    spec.depths = vec![5, 10];
    let error = refusal(spec, &mut coordinator).await;
    assert!(
        error.contains("depth 5 is below the smallest k 10"),
        "{error}"
    );

    let wider = unit_vectors(QUERIES, DIM + 1, 0xD05E_0003);
    let mut spec = fixture.spec(GroundTruth::FullDepth, None);
    spec.queries = &wider;
    spec.dimensions = DIM as u32 + 1;
    let error = refusal(spec, &mut coordinator).await;
    assert!(error.contains("live provider serves 32"), "{error}");

    let short = &fixture.corpus[..(ROWS - 1) * DIM];
    let spec = fixture.spec(GroundTruth::Brute { rows: short }, None);
    let error = refusal(spec, &mut coordinator).await;
    assert!(error.contains("must cover the corpus exactly"), "{error}");

    let spec = fixture.spec(GroundTruth::FullDepth, Some(999_999));
    let error = refusal(spec, &mut coordinator).await;
    assert!(
        error.contains("default_target_recall_ppm 999999 names no point"),
        "{error}"
    );

    // Full-depth ground truth needs selection_k = rows within max_k; the
    // coordinator's refusal names both numbers and the tool forwards it.
    let mut bounded = CoordinatorServiceImpl::new(fixture.addrs.clone())
        .with_max_k(ROWS as u32 / 2)
        .with_topology_generation(GENERATION);
    let error = refusal(fixture.spec(GroundTruth::FullDepth, None), &mut bounded).await;
    assert!(
        error.contains("full-depth ground truth at selection_k=4096 refused"),
        "{error}"
    );
    assert!(error.contains("max_k"), "{error}");

    // A tombstone invalidates the all-live generation: the tool refuses to
    // measure it, and the coordinator refuses to serve the profile on it.
    let (_, loaded, _) = measure_and_save(
        &fixture,
        GroundTruth::FullDepth,
        Some(DEFAULT_TARGET),
        "default.toml",
    )
    .await;
    NodeServiceClient::connect(fixture.addrs[1].clone())
        .await
        .unwrap()
        .delete_documents(DeleteDocumentsRequest {
            doc_ids: vec![4095],
            expected_wal_generation: None,
        })
        .await
        .unwrap();
    let error = refusal(fixture.spec(GroundTruth::FullDepth, None), &mut coordinator).await;
    assert!(
        error.contains("slot_offset=2048 has 1 tombstoned rows"),
        "{error}"
    );
    let serving = fixture.coordinator().with_dense_quality_profile(loaded);
    let err = query(
        &serving,
        request(
            K,
            0,
            dense_leaf(
                fixture.query(0),
                DenseExecutionMode::Auto,
                DenseScoreMode::Fp32Rerank,
                None,
            ),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message().contains("compact and remeasure"),
        "{}",
        err.message()
    );
}
