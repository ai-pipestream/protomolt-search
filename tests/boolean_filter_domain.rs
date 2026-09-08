//! The Boolean filter leaf's domain narrowing (docs/query-api.md,
//! "Recursive boolean execution"): a filter leaf under MUST beside an
//! exact sibling resolves its column reads over the sibling's members
//! instead of every row of the shard. The answers — ids, scores,
//! ranks, matched clauses, and the root aggregate — are the same
//! either way; every shape here runs under both scan modes (the
//! narrowing is the default; PIPESTREAM_SEARCH_BOOLEAN_FILTER_FULL_SCAN
//! forces the whole-shard scan) and the two are pinned identical, over
//! fixed shapes and over a randomized corpus with random trees.

mod common;

use std::path::PathBuf;
use std::sync::Mutex;

use common::{fit_calibration, start_empty_node, unit_vectors};
use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::{Layout, NodeConfig};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    selection_query, AddDocumentsRequest, AddVectorsRequest, AggregateOp, AggregateRequest,
    AggregateResponse, Aggregation, BooleanQuery, CompositeSearchStrategy, DeleteDocumentsRequest,
    DenseQuery, FacetValue, FilterQuery, FlushRequest, IntegerValue, LexicalQuery, NumericValue,
    QueryRequest, QueryResponse, SearchQuery, SelectionOperator, SelectionQuery,
    SetCalibrationRequest,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

const DIM: usize = 16;
const BIT_WIDTH: usize = 4;
const ROWS: usize = 2_400;
const SHARD_ROWS: usize = ROWS / 2;
const SEAL: usize = 400;
const BLOCK: usize = 300;

/// Guards the full-scan A/B switch, which is process-wide.
static SCAN_MODE: Mutex<()> = Mutex::new(());

fn tempdir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("booldom-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn config(index_path: PathBuf, slot_offset: u64) -> NodeConfig {
    NodeConfig {
        index_path: Some(index_path),
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        layout: Layout::Segments,
        seal_tail_docs: SEAL as u32,
        wal: false,
        slot_offset,
        facet_fields: vec!["court".into()],
        integer_fields: vec!["year".into()],
        numeric_fields: vec!["pages".into()],
        ..Default::default()
    }
}

/// Deterministic RNG (the blockmax.rs LCG style).
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_add(0x9E3779B97F4A7C15))
    }
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Three-letter tokens: no stopword, no suffix a stemmer would cut.
fn vocab() -> Vec<String> {
    let consonants = b"bcdfghjklmnpqrstvwxz";
    let vowels = b"aeiou";
    (0..24)
        .map(|i| {
            let c0 = consonants[(i % 20) as usize] as char;
            let v = vowels[(i % 5) as usize] as char;
            let c1 = consonants[((i * 7 + 3) % 20) as usize] as char;
            format!("{c0}{v}{c1}")
        })
        .collect()
}

fn year(i: usize) -> i64 {
    1900 + (i % 120) as i64
}

fn court(i: usize) -> &'static str {
    ["scotus", "ca9", "ca2", "ca5", "ded"][i % 5]
}

fn pages(i: usize) -> f64 {
    (i % 211) as f64
}

fn corpus() -> Vec<f32> {
    unit_vectors(ROWS, DIM, 0xB00_1EA5)
}

/// Row `i`'s document: a skewed term draw (the first tokens are
/// common, the tail rare) with a cycling year, court, and pages.
fn document(i: usize) -> AddDocumentsRequest {
    let vocab = vocab();
    let mut slot = i as u64;
    let mut tokens = vec![i % 3];
    for _ in 0..1 + (i % 5) {
        slot = slot
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        tokens.push(4 + ((slot >> 33) as usize) % (vocab.len() - 4));
    }
    let text = format!(
        "opinion {i} about {}",
        tokens
            .iter()
            .map(|&t| vocab[t].as_str())
            .collect::<Vec<_>>()
            .join(" ")
    );
    AddDocumentsRequest {
        text,
        analysis: Some(body_spec()),
        integers: vec![IntegerValue {
            field: "year".into(),
            value: year(i),
        }],
        facets: vec![FacetValue {
            field: "court".into(),
            value: court(i).into(),
        }],
        numerics: vec![NumericValue {
            field: "pages".into(),
            value: pages(i),
        }],
        ..Default::default()
    }
}

async fn ingest(addr: &str, shard: usize, vectors: &[f32]) {
    let sample = &vectors[..vectors.len().min(64 * DIM)];
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, sample);
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    client
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: BIT_WIDTH as u32,
            shift,
            scale,
        })
        .await
        .unwrap();
    let first = shard * SHARD_ROWS;
    for block in 0..SHARD_ROWS.div_ceil(BLOCK) {
        let start = first + block * BLOCK;
        let end = (start + BLOCK).min(first + SHARD_ROWS);
        let (tx, rx) = mpsc::channel(BLOCK);
        for i in start..end {
            tx.send(document(i)).await.unwrap();
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
        let (tx, rx) = mpsc::channel(2);
        tx.send(AddVectorsRequest {
            vectors: vectors[start * DIM..end * DIM].to_vec(),
            dim: DIM as u32,
        })
        .await
        .unwrap();
        drop(tx);
        client.add_vectors(ReceiverStream::new(rx)).await.unwrap();
    }
    client.flush(FlushRequest {}).await.unwrap();
    // Tombstones: every seventeenth row of this shard.
    let deleted: Vec<u64> = (first..first + SHARD_ROWS)
        .filter(|i| i % 17 == 0)
        .map(|i| i as u64)
        .collect();
    client
        .delete_documents(DeleteDocumentsRequest {
            doc_ids: deleted,
            expected_wal_generation: None,
        })
        .await
        .unwrap();
}

struct Fleet {
    coordinator: CoordinatorServiceImpl,
    dir: PathBuf,
    handles: Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl Fleet {
    fn stop(self) {
        for handle in self.handles {
            handle.abort();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn fleet(tag: &str) -> Fleet {
    let dir = tempdir(tag);
    let vectors = corpus();
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..2 {
        let (addr, handle) = start_empty_node(config(
            dir.join(format!("shard-{shard}.tv")),
            (shard * SHARD_ROWS) as u64,
        ))
        .await;
        ingest(&addr, shard, &vectors).await;
        addrs.push(addr);
        handles.push(handle);
    }
    let coordinator = CoordinatorServiceImpl::new(addrs).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    );
    Fleet {
        coordinator,
        dir,
        handles,
    }
}

/// A fleet where vectors and documents do not line up: shard 0's last
/// VECTORLESS rows never get a vector, shard 1 holds VECTOR_ONLY
/// vectors and no document at all.
const VECTORLESS: usize = 300;
const VECTOR_ONLY: usize = 200;

async fn uneven_fleet(tag: &str) -> Fleet {
    let dir = tempdir(tag);
    let vectors = corpus();
    let sample = &vectors[..vectors.len().min(64 * DIM)];
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, sample);
    let calibration = SetCalibrationRequest {
        dim: DIM as u32,
        bit_width: BIT_WIDTH as u32,
        shift,
        scale,
    };
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    let (addr, handle) = start_empty_node(NodeConfig {
        layout: Layout::SingleImage,
        seal_tail_docs: 0,
        ..config(dir.join("shard-0.tv"), 0)
    })
    .await;
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    client.set_calibration(calibration.clone()).await.unwrap();
    let (tx, rx) = mpsc::channel(SHARD_ROWS);
    for i in 0..SHARD_ROWS {
        tx.send(document(i)).await.unwrap();
    }
    drop(tx);
    client.add_documents(ReceiverStream::new(rx)).await.unwrap();
    let with_vectors = SHARD_ROWS - VECTORLESS;
    let (tx, rx) = mpsc::channel(2);
    tx.send(AddVectorsRequest {
        vectors: vectors[..with_vectors * DIM].to_vec(),
        dim: DIM as u32,
    })
    .await
    .unwrap();
    drop(tx);
    client.add_vectors(ReceiverStream::new(rx)).await.unwrap();
    client.flush(FlushRequest {}).await.unwrap();
    client
        .delete_documents(DeleteDocumentsRequest {
            doc_ids: (0..SHARD_ROWS)
                .filter(|i| i % 13 == 0)
                .map(|i| i as u64)
                .collect(),
            expected_wal_generation: None,
        })
        .await
        .unwrap();
    addrs.push(addr);
    handles.push(handle);
    let (addr, handle) = start_empty_node(config(dir.join("shard-1.tv"), SHARD_ROWS as u64)).await;
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    client.set_calibration(calibration).await.unwrap();
    let (tx, rx) = mpsc::channel(2);
    tx.send(AddVectorsRequest {
        vectors: vectors[SHARD_ROWS * DIM..(SHARD_ROWS + VECTOR_ONLY) * DIM].to_vec(),
        dim: DIM as u32,
    })
    .await
    .unwrap();
    drop(tx);
    client.add_vectors(ReceiverStream::new(rx)).await.unwrap();
    client.flush(FlushRequest {}).await.unwrap();
    addrs.push(addr);
    handles.push(handle);
    let coordinator = CoordinatorServiceImpl::new(addrs).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    );
    Fleet {
        coordinator,
        dir,
        handles,
    }
}

fn lexical(id: &str, text: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(pipestream_search::pb::search_query::Query::Lexical(
                LexicalQuery {
                    text: text.to_string(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                },
            )),
        })),
    }
}

fn dense(id: &str, q: usize) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(pipestream_search::pb::search_query::Query::Dense(
                DenseQuery {
                    vector: corpus()[q * DIM..(q + 1) * DIM].to_vec(),
                    ..Default::default()
                },
            )),
        })),
    }
}

fn cel(id: &str, cel: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Filter(FilterQuery {
            id: id.to_string(),
            predicate: Some(pipestream_search::pb::filter_query::Predicate::Cel(
                cel.to_string(),
            )),
        })),
    }
}

fn boolean(
    must: Vec<SelectionQuery>,
    should: Vec<SelectionQuery>,
    must_not: Vec<SelectionQuery>,
    minimum_should_match: u32,
    aggregate: Option<AggregateRequest>,
) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Boolean(BooleanQuery {
            must,
            should,
            must_not,
            minimum_should_match,
            aggregate,
        })),
    }
}

fn request(selection: SelectionQuery, k: u32) -> QueryRequest {
    QueryRequest {
        request_id: "booldom".into(),
        k,
        selection: Some(selection),
        profile: false,
        ..Default::default()
    }
}

async fn query(c: &CoordinatorServiceImpl, req: QueryRequest) -> QueryResponse {
    SearchService::query(c, Request::new(req))
        .await
        .unwrap()
        .into_inner()
}

/// What the two scan modes must agree on: the page (ids, ranks, score
/// bits, per-signal bits, matched clause ids) and the root aggregate.
#[allow(clippy::type_complexity)]
fn signature(
    r: &QueryResponse,
) -> (
    Vec<(u64, u32, u32, Vec<(String, u32)>, Vec<String>)>,
    Option<AggregateResponse>,
) {
    let hits = r
        .hits
        .iter()
        .map(|h| {
            (
                h.doc_id,
                h.rank,
                h.score.to_bits(),
                h.signals
                    .iter()
                    .map(|s| (s.id.clone(), s.score.to_bits()))
                    .collect::<Vec<_>>(),
                h.matched.clone(),
            )
        })
        .collect();
    (hits, r.aggregate.clone())
}

/// The process-wide request count of one route.
fn route_count(rpc: &str) -> u64 {
    let snapshot = pipestream_search::metrics::snapshot("test", &[]);
    snapshot
        .samples
        .iter()
        .find(|s| {
            s.name == "turbovec_requests_total"
                && s.labels.iter().any(|l| l.name == "rpc" && l.value == rpc)
        })
        .map(|s| s.value as u64)
        .expect("the route is exported")
}

/// Run one selection under the default (domain-narrowed) filter scan
/// and under the forced whole-shard scan; the two answers must be
/// identical.
#[allow(clippy::await_holding_lock)] // the guard serializes the process-wide switch
async fn both_modes(c: &CoordinatorServiceImpl, selection: SelectionQuery, k: u32) {
    let _guard = SCAN_MODE.lock().unwrap();
    both_modes_locked(c, selection, k).await;
}

/// `both_modes` for a caller already holding `SCAN_MODE`.
async fn both_modes_locked(c: &CoordinatorServiceImpl, selection: SelectionQuery, k: u32) {
    let narrowed = query(c, request(selection.clone(), k)).await;
    std::env::set_var("PIPESTREAM_SEARCH_BOOLEAN_FILTER_FULL_SCAN", "1");
    let full = query(c, request(selection, k)).await;
    std::env::remove_var("PIPESTREAM_SEARCH_BOOLEAN_FILTER_FULL_SCAN");
    assert_eq!(
        signature(&narrowed),
        signature(&full),
        "narrowed and whole-shard filter scans disagree"
    );
}

fn aggregate_spec() -> AggregateRequest {
    AggregateRequest {
        aggregations: vec![
            Aggregation {
                name: "pages_sum".into(),
                expression: "pages".into(),
                op: AggregateOp::Sum as i32,
                max_distinct: 0,
            },
            Aggregation {
                name: "years".into(),
                expression: "year".into(),
                op: AggregateOp::Cardinality as i32,
                max_distinct: 0,
            },
        ],
        group_by: "court".into(),
        ..Default::default()
    }
}

/// Fixed shapes: the filter leaf beside a narrower exact sibling, in
/// every position and combination the planner narrows or declines to.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn narrowed_and_full_scans_agree_over_fixed_shapes() {
    let fleet = fleet("fixed").await;
    let c = &fleet.coordinator;
    let before = route_count("evaluate_boolean");
    let rare = vocab().pop().unwrap();
    let common = vocab()[0].clone();
    let shapes: Vec<SelectionQuery> = vec![
        // The measured shape: MUST lexical + MUST integer-range filter.
        boolean(
            vec![lexical("l", &common), cel("y", "year >= 1950")],
            vec![],
            vec![],
            0,
            None,
        ),
        // MUST lexical + MUST facet equality.
        boolean(
            vec![lexical("l", &common), cel("c", "court == \"ca9\"")],
            vec![],
            vec![],
            0,
            None,
        ),
        // MUST lexical + MUST numeric range.
        boolean(
            vec![lexical("l", &common), cel("p", "pages < 100.0")],
            vec![],
            vec![],
            0,
            None,
        ),
        // A rare term narrows the filter hardest.
        boolean(
            vec![lexical("l", &rare), cel("y", "year >= 1905")],
            vec![],
            vec![],
            0,
            None,
        ),
        // Two MUST filters beside the lexical.
        boolean(
            vec![
                lexical("l", &common),
                cel("y", "year >= 1950"),
                cel("p", "pages < 150.0"),
            ],
            vec![],
            vec![],
            0,
            None,
        ),
        // Two MUST lexicals and a filter: the intersection narrows.
        boolean(
            vec![
                lexical("a", &common),
                lexical("b", &vocab()[1]),
                cel("c", "court == \"ded\""),
            ],
            vec![],
            vec![],
            0,
            None,
        ),
        // MUST dense + MUST filter.
        boolean(
            vec![dense("d", 7), cel("y", "year >= 1950")],
            vec![],
            vec![],
            0,
            None,
        ),
        // MUST dense + MUST lexical + MUST filter.
        boolean(
            vec![
                dense("d", 11),
                lexical("l", &common),
                cel("y", "year < 2010"),
            ],
            vec![],
            vec![],
            0,
            None,
        ),
        // Nested: the filter sits one group down the MUST chain.
        boolean(
            vec![
                lexical("a", &common),
                boolean(
                    vec![lexical("b", &vocab()[2]), cel("y", "year >= 1975")],
                    vec![],
                    vec![],
                    0,
                    None,
                ),
            ],
            vec![],
            vec![],
            0,
            None,
        ),
        // A MUST_NOT filter beside the narrowed MUST filter.
        boolean(
            vec![lexical("l", &common), cel("y", "year >= 1950")],
            vec![],
            vec![cel("c", "court == \"ca5\"")],
            0,
            None,
        ),
        // A SHOULD filter under minimum-should-match: no narrowing, but
        // the count must agree.
        boolean(
            vec![lexical("l", &common)],
            vec![cel("y", "year >= 1990"), lexical("s", &vocab()[3])],
            vec![],
            1,
            None,
        ),
        // A SHOULD filter with the minimum at zero.
        boolean(
            vec![lexical("l", &common)],
            vec![cel("y", "year >= 1990")],
            vec![],
            0,
            None,
        ),
        // A filter-only MUST: no sibling, no narrowing.
        boolean(vec![cel("y", "year >= 1950")], vec![], vec![], 0, None),
        // Two filters and no exact leaf at all.
        boolean(
            vec![cel("y", "year >= 1950"), cel("c", "court == \"ca2\"")],
            vec![],
            vec![],
            0,
            None,
        ),
        // The root aggregate folds over the narrowed match set.
        boolean(
            vec![lexical("l", &common), cel("y", "year >= 1950")],
            vec![],
            vec![],
            0,
            Some(aggregate_spec()),
        ),
    ];
    for selection in shapes {
        both_modes(c, selection, 50).await;
    }
    assert!(route_count("evaluate_boolean") >= before + 30);
    fleet.stop();
}

/// Random corpora and random trees: the two scan modes agree on the
/// page and the aggregate every time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn narrowed_and_full_scans_agree_over_random_shapes() {
    let fleet = fleet("random").await;
    let c = &fleet.coordinator;
    let vocab = vocab();
    let mut rng = Lcg::new(0xD0AA_1E50);
    for round in 0..40 {
        let term = |rng: &mut Lcg| vocab[rng.below(vocab.len() as u64) as usize].clone();
        let filter = |rng: &mut Lcg, id: String| match rng.below(3) {
            0 => cel(&id, &format!("year >= {}", 1900 + rng.below(120))),
            1 => cel(
                &id,
                &format!(
                    "court == \"{}\"",
                    ["scotus", "ca9", "ca2", "ca5", "ded"][rng.below(5) as usize]
                ),
            ),
            _ => cel(&id, &format!("pages < {}", rng.below(211))),
        };
        let clause = |rng: &mut Lcg, id: String| match rng.below(4) {
            0 => filter(rng, id),
            1 => dense(&id, rng.below(ROWS as u64) as usize),
            _ => lexical(&id, &term(rng)),
        };
        let must_len = 1 + rng.below(3) as usize;
        let must: Vec<SelectionQuery> = (0..must_len)
            .map(|i| clause(&mut rng, format!("m{i}")))
            .collect();
        let should_len = rng.below(3) as usize;
        let should: Vec<SelectionQuery> = (0..should_len)
            .map(|i| clause(&mut rng, format!("s{i}")))
            .collect();
        let must_not: Vec<SelectionQuery> = (0..rng.below(2) as usize)
            .map(|i| clause(&mut rng, format!("x{i}")))
            .collect();
        let msm = if should.is_empty() {
            0
        } else {
            rng.below(should.len() as u64 + 1) as u32
        };
        let aggregate = (round % 4 == 0).then(aggregate_spec);
        both_modes(
            c,
            boolean(must, should, must_not, msm, aggregate),
            10 + rng.below(90) as u32,
        )
        .await;
    }
    fleet.stop();
}

/// Rows without a vector and rows without a document: the dense clause
/// and the filter domains agree across the gap in both modes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn narrowed_and_full_scans_agree_across_vector_gaps() {
    let fleet = uneven_fleet("gaps").await;
    let c = &fleet.coordinator;
    let common = vocab()[0].clone();
    let shapes: Vec<SelectionQuery> = vec![
        boolean(
            vec![dense("d", 5), cel("y", "year >= 1950")],
            vec![],
            vec![],
            0,
            None,
        ),
        boolean(
            vec![lexical("l", &common), cel("y", "year >= 1950")],
            vec![],
            vec![],
            0,
            None,
        ),
        boolean(
            vec![lexical("l", &common), dense("d", 9)],
            vec![cel("y", "year >= 1990")],
            vec![cel("c", "court == \"ca2\"")],
            1,
            None,
        ),
        boolean(
            vec![dense("d", 5)],
            vec![],
            vec![cel("y", "year < 1960")],
            0,
            None,
        ),
        boolean(
            vec![lexical("l", &common), cel("y", "year >= 1950")],
            vec![],
            vec![],
            0,
            Some(aggregate_spec()),
        ),
    ];
    for selection in shapes {
        both_modes(c, selection, 50).await;
    }
    fleet.stop();
}

/// The composite AND route (no bitmap route) and the boolean route
/// agree under the narrowed scan, the flat-versus-relay equivalence the
/// pushdown tests pin for the whole-shard scan.
#[allow(clippy::await_holding_lock)] // the guard serializes the counter windows
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_narrowed_scan_keeps_the_composite_equivalence() {
    let fleet = fleet("relay").await;
    let c = &fleet.coordinator;
    let _guard = SCAN_MODE.lock().unwrap();
    let common = vocab()[0].clone();
    let flat = SelectionQuery {
        node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
            operator: SelectionOperator::And as i32,
            clauses: vec![lexical("l", &common), cel("y", "year >= 1950")],
            scoring: None,
        })),
    };
    let relay = boolean(
        vec![lexical("l", &common), cel("y", "year >= 1950")],
        vec![],
        vec![],
        0,
        None,
    );
    let flat = query(c, request(flat, 50)).await;
    let relay = query(c, request(relay, 50)).await;
    assert_eq!(signature(&flat).0.len(), signature(&relay).0.len());
    let flat_ids: Vec<u64> = flat.hits.iter().map(|h| h.doc_id).collect();
    let relay_ids: Vec<u64> = relay.hits.iter().map(|h| h.doc_id).collect();
    assert_eq!(flat_ids, relay_ids);
    fleet.stop();
}

/// A MUST group whose intersection is already empty resolves its
/// remaining siblings as the empty set: the skip counter proves the
/// cheap path ran, and the answer matches the forced whole-shard scan
/// exactly.
#[allow(clippy::await_holding_lock)] // the guard serializes the counter window
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_empty_must_group_skips_the_dense_membership() {
    let fleet = fleet("shortcircuit").await;
    let c = &fleet.coordinator;
    let _guard = SCAN_MODE.lock().unwrap();
    let counter = || {
        pipestream_search::node::BOOLEAN_MUST_SHORT_CIRCUIT_SKIPS
            .load(std::sync::atomic::Ordering::Relaxed)
    };
    // A filter that matches no row: the dense membership is never built.
    let empty_filter = boolean(
        vec![dense("d", 5), cel("y", "year >= 3000")],
        vec![],
        vec![],
        0,
        None,
    );
    let before = counter();
    let narrowed = query(c, request(empty_filter.clone(), 50)).await;
    let skipped = counter() - before;
    assert!(narrowed.hits.is_empty());
    assert!(
        skipped >= 2,
        "two shards, each replacing the dense membership: {skipped}"
    );
    both_modes_locked(c, empty_filter, 50).await;
    // A term the corpus never saw empties the group the same way, and
    // the dense leaf is skipped again.
    let empty_lexical = boolean(
        vec![lexical("l", "zzq"), dense("d", 5)],
        vec![],
        vec![],
        0,
        None,
    );
    let before = counter();
    let r = query(c, request(empty_lexical, 50)).await;
    assert!(r.hits.is_empty());
    assert!(
        counter() >= before + 2,
        "the empty lexical leaf short-circuits the dense leaf on both shards"
    );
    // Control: a filter that admits rows pays the dense membership.
    let before = counter();
    let r = query(
        c,
        request(
            boolean(
                vec![dense("d", 5), cel("y", "year >= 1950")],
                vec![],
                vec![],
                0,
                None,
            ),
            50,
        ),
    )
    .await;
    assert!(!r.hits.is_empty());
    assert_eq!(
        counter(),
        before,
        "a surviving group resolves its dense leaf"
    );
    // SHOULD clauses never short-circuit: an empty SHOULD filter still
    // lets the lexical clause win the minimum.
    both_modes_locked(
        c,
        boolean(
            vec![],
            vec![cel("y", "year >= 3000"), lexical("s", &vocab()[0])],
            vec![],
            1,
            None,
        ),
        50,
    )
    .await;
    fleet.stop();
}
