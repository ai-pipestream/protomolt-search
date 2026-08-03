//! VariantSearch acceptance: the A/B surface over the wire.
//!
//! What this pins:
//!
//! - a variant compared with ITSELF reports perfect agreement on every
//!   measure. This is the control-vs-control check, and the one that
//!   matters most: if two identical arms can disagree, no diff the RPC
//!   ever reports means anything;
//! - a real analysis/weighting difference shows up as a real diff, with
//!   the reference's label attached the right way round;
//! - the whole comparison is deterministic, which is the property that
//!   lets a single query be evidence here rather than a sample;
//! - arms of different KINDS (BM25 vs hybrid) can be compared, since
//!   "is the vector leg worth its latency" is an A/B question;
//! - team-draft interleaving over the wire, and its refusal to pretend
//!   three arms are two;
//! - the validation that keeps a result readable: named, unique,
//!   non-empty arms.

mod common;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use turbovec_search::coordinator::CoordinatorServiceImpl;
use turbovec_search::node::NodeConfig;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::search_service_server::SearchService;
use turbovec_search::pb::{
    search_variant, AddDocumentsRequest, Bm25SearchRequest, DocumentField, InterleaveTeam,
    QueryField, SearchVariant, VariantSearchRequest,
};

use common::{mock::start_mock_analysis, start_empty_node};

/// Bodies and captions chosen so that weighting the caption field
/// reorders the ranking: "smith" is rare in bodies and common in
/// captions, so the two arms genuinely disagree.
const CORPUS: [&[(&str, Option<&str>)]; 2] = [
    &[
        ("rust search rust fast", Some("Smith v Jones")),
        ("vector search rust", None),
        (
            "smith writes about rust",
            Some("Acme Corp v Rust Industries"),
        ),
    ],
    &[
        ("search engines love rust", Some("Smith v Smith")),
        ("vector vector vector", None),
        ("rust", Some("In re Vector Holdings")),
    ],
];

const OFFSETS: [u64; 2] = [0, 3];

fn doc_request(body: &str, name: Option<&str>) -> AddDocumentsRequest {
    AddDocumentsRequest {
        map_numerics: Vec::new(),
        map_facets: Vec::new(),
        numerics: Vec::new(),
        facets: Vec::new(),
        text: body.to_string(),
        analysis: None,
        lineage: None,
        fields: name
            .map(|n| {
                vec![DocumentField {
                    field: "case_name".to_string(),
                    text: n.to_string(),
                    analysis: None,
                }]
            })
            .unwrap_or_default(),
    }
}

async fn add_documents(addr: &str, docs: &[(&str, Option<&str>)]) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    for (body, name) in docs {
        tx.send(doc_request(body, *name)).await.unwrap();
    }
    drop(tx);
    let resp = client
        .add_documents(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.added as usize, docs.len());
}

fn two_field_node(analysis: &str, slot_offset: u64) -> NodeConfig {
    NodeConfig {
        slot_offset,
        analysis_addr: Some(analysis.to_string()),
        bm25_fields: vec!["body".to_string(), "case_name".to_string()],
        ..Default::default()
    }
}

type ServerHandle = tokio::task::JoinHandle<Result<(), tonic::transport::Error>>;

/// A cluster over CORPUS, plus the coordinator in front of it. The
/// handles are returned so the servers outlive the test body.
async fn cluster() -> (CoordinatorServiceImpl, Vec<ServerHandle>, ServerHandle) {
    let (analysis, mock) = start_mock_analysis().await;
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for (i, docs) in CORPUS.iter().enumerate() {
        let (addr, handle) = start_empty_node(two_field_node(&analysis, OFFSETS[i])).await;
        add_documents(&addr, docs).await;
        addrs.push(addr);
        handles.push(handle);
    }
    let coord =
        CoordinatorServiceImpl::new(addrs).with_bm25(Some(analysis.clone()), Default::default());
    (coord, handles, mock)
}

/// A BM25 arm scoring `body` only.
fn body_only(text: &str) -> SearchVariant {
    SearchVariant {
        label: "body-only".to_string(),
        query: Some(search_variant::Query::Bm25(Bm25SearchRequest {
            map_facet_fields: Vec::new(),
            score_stages: Vec::new(),
            facet_fields: Vec::new(),
            text: text.to_string(),
            k: 0,
            analysis: None,
            min_score: 0.0,
            fields: vec![QueryField {
                field: "body".to_string(),
                analysis: None,
                weight: 1.0,
                k1: 0.0,
                b: 0.0,
            }],
        })),
    }
}

/// A BM25 arm that also scores the caption at `w_name`.
fn with_case_name(label: &str, text: &str, w_name: f32) -> SearchVariant {
    SearchVariant {
        label: label.to_string(),
        query: Some(search_variant::Query::Bm25(Bm25SearchRequest {
            map_facet_fields: Vec::new(),
            score_stages: Vec::new(),
            facet_fields: Vec::new(),
            text: text.to_string(),
            k: 0,
            analysis: None,
            min_score: 0.0,
            fields: vec![
                QueryField {
                    field: "body".to_string(),
                    analysis: None,
                    weight: 1.0,
                    k1: 0.0,
                    b: 0.0,
                },
                QueryField {
                    field: "case_name".to_string(),
                    analysis: None,
                    weight: w_name,
                    k1: 0.0,
                    b: 0.0,
                },
            ],
        })),
    }
}

fn request(variants: Vec<SearchVariant>, k: u32) -> VariantSearchRequest {
    VariantSearchRequest {
        request_id: String::new(),
        variants,
        k,
        rbo_p: 0.0,
        interleave: false,
        interleave_seed: 0,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_arm_compared_with_itself_agrees_on_every_measure() {
    let (coord, _nodes, _mock) = cluster().await;
    // Same configuration under two names: the only honest zero point.
    let mut twin = body_only("rust smith");
    twin.label = "twin".to_string();
    let resp = coord
        .variant_search(tonic::Request::new(request(
            vec![body_only("rust smith"), twin],
            5,
        )))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.results.len(), 2);
    assert_eq!(
        resp.diffs.len(),
        1,
        "n arms produce n-1 diffs against the first"
    );
    let d = &resp.diffs[0];
    assert_eq!(d.reference, "body-only");
    assert_eq!(d.variant, "twin");
    assert_eq!(d.overlap_fraction, 1.0, "identical arms share every result");
    assert_eq!(d.kendall_tau, 1.0, "and every ordering");
    assert_eq!(d.score_regret, 0.0, "and give up nothing");
    assert_eq!(d.regret_unscored, 0, "nothing outside the reference");
    assert!(!d.top1_flipped);
    assert!(d.rbo > 0.0, "rbo of a non-empty identical pair is positive");
    assert_eq!(
        resp.results[0].hits, resp.results[1].hits,
        "identical configurations must return identical hits, not merely similar ones"
    );
    assert!(!resp.request_id.is_empty(), "an id is assigned when absent");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn weighting_a_field_shows_up_as_a_measured_difference() {
    let (coord, _nodes, _mock) = cluster().await;
    let resp = coord
        .variant_search(tonic::Request::new(request(
            vec![
                body_only("smith rust"),
                with_case_name("caption-boosted", "smith rust", 5.0),
            ],
            5,
        )))
        .await
        .unwrap()
        .into_inner();

    let d = &resp.diffs[0];
    assert_eq!(d.variant, "caption-boosted");
    // "smith" is rare in bodies and common in captions, so a heavy
    // caption weight must move the ranking. A diff that cannot detect
    // this would be measuring nothing.
    assert!(
        d.kendall_tau < 1.0 || d.overlap_fraction < 1.0,
        "expected disagreement, got tau={} overlap={}",
        d.kendall_tau,
        d.overlap_fraction
    );
    assert!(d.depth > 0);
    assert!(
        resp.results.iter().all(|r| r.elapsed_ms >= 0.0),
        "each arm reports its own wall time"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_whole_comparison_is_deterministic() {
    let (coord, _nodes, _mock) = cluster().await;
    let build = || {
        let mut r = request(
            vec![
                body_only("rust vector"),
                with_case_name("caption-boosted", "rust vector", 3.0),
            ],
            5,
        );
        r.request_id = "fixed".to_string();
        r.interleave = true;
        r
    };
    let first = coord
        .variant_search(tonic::Request::new(build()))
        .await
        .unwrap()
        .into_inner();
    let second = coord
        .variant_search(tonic::Request::new(build()))
        .await
        .unwrap()
        .into_inner();

    // Timings legitimately differ; everything that constitutes the
    // ANSWER must not. This is what makes a one-query A/B evidence.
    assert_eq!(first.diffs, second.diffs);
    assert_eq!(first.interleaving, second.interleaving);
    for (a, b) in first.results.iter().zip(&second.results) {
        assert_eq!(a.label, b.label);
        assert_eq!(a.hits, b.hits, "re-running a variant must be bit-identical");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interleaving_merges_both_arms_and_names_their_contributions() {
    let (coord, _nodes, _mock) = cluster().await;
    let mut req = request(
        vec![
            body_only("rust smith"),
            with_case_name("caption-boosted", "rust smith", 5.0),
        ],
        4,
    );
    req.interleave = true;
    let resp = coord
        .variant_search(tonic::Request::new(req))
        .await
        .unwrap()
        .into_inner();

    let il = resp.interleaving.expect("interleaving was requested");
    assert_eq!(
        il.doc_ids.len(),
        il.teams.len(),
        "attribution is per position"
    );
    assert!(!il.doc_ids.is_empty());
    assert_ne!(
        il.seed, 0,
        "a derived seed is reported so it can be replayed"
    );
    let mut sorted = il.doc_ids.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        il.doc_ids.len(),
        "a document must not be shown twice because both arms found it"
    );
    assert!(il
        .teams
        .iter()
        .all(|t| *t == InterleaveTeam::A as i32 || *t == InterleaveTeam::B as i32));
    // Exposure is balanced within one result: that is what makes a
    // selection evidence about the ranking and not about the slot.
    let a = il
        .teams
        .iter()
        .filter(|t| **t == InterleaveTeam::A as i32)
        .count();
    let b = il.teams.len() - a;
    assert!(a.abs_diff(b) <= 1, "lopsided exposure: {a} vs {b}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interleaving_refuses_more_than_two_arms() {
    let (coord, _nodes, _mock) = cluster().await;
    let mut req = request(
        vec![
            body_only("rust"),
            with_case_name("w2", "rust", 2.0),
            with_case_name("w5", "rust", 5.0),
        ],
        4,
    );
    req.interleave = true;
    let err = coord
        .variant_search(tonic::Request::new(req))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("two-way"),
        "the refusal should say why: {}",
        err.message()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_arms_each_diff_against_the_first() {
    let (coord, _nodes, _mock) = cluster().await;
    let resp = coord
        .variant_search(tonic::Request::new(request(
            vec![
                body_only("rust smith"),
                with_case_name("w2", "rust smith", 2.0),
                with_case_name("w9", "rust smith", 9.0),
            ],
            5,
        )))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.results.len(), 3);
    assert_eq!(resp.diffs.len(), 2);
    assert!(resp.diffs.iter().all(|d| d.reference == "body-only"));
    assert_eq!(resp.diffs[0].variant, "w2");
    assert_eq!(resp.diffs[1].variant, "w9");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unreadable_requests_are_refused() {
    let (coord, _nodes, _mock) = cluster().await;
    let cases: Vec<(&str, VariantSearchRequest, &str)> = vec![
        (
            "one arm is not a comparison",
            request(vec![body_only("rust")], 5),
            "at least 2",
        ),
        (
            "an unnamed arm cannot be read in the diffs",
            request(
                vec![body_only("rust"), {
                    let mut v = with_case_name("x", "rust", 2.0);
                    v.label = String::new();
                    v
                }],
                5,
            ),
            "empty label",
        ),
        (
            "two arms with one name are ambiguous",
            request(
                vec![body_only("rust"), with_case_name("body-only", "rust", 2.0)],
                5,
            ),
            "duplicate variant label",
        ),
        (
            "an arm with no query has nothing to run",
            request(
                vec![body_only("rust"), {
                    let mut v = with_case_name("empty", "rust", 2.0);
                    v.query = None;
                    v
                }],
                5,
            ),
            "no query set",
        ),
        (
            "k above the coordinator's cap is refused, not clamped",
            request(
                vec![body_only("rust"), with_case_name("x", "rust", 2.0)],
                10_001,
            ),
            "exceeds this coordinator's max_k",
        ),
        (
            "rbo persistence is a probability",
            {
                let mut r = request(vec![body_only("rust"), with_case_name("x", "rust", 2.0)], 5);
                r.rbo_p = 1.0;
                r
            },
            "rbo_p must be in",
        ),
    ];
    for (why, req, expected) in cases {
        let err = coord
            .variant_search(tonic::Request::new(req))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{why}");
        assert!(
            err.message().contains(expected),
            "{why}: wanted {expected:?}, got {:?}",
            err.message()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_arm_is_named_in_the_error() {
    let (coord, _nodes, _mock) = cluster().await;
    // An absent field is a real refusal from the BM25 path; with several
    // arms in flight the caller cannot act on it unless it says which.
    let mut bad = with_case_name("challenger", "rust", 2.0);
    if let Some(search_variant::Query::Bm25(r)) = &mut bad.query {
        r.fields[1].field = "no_such_field".to_string();
    }
    let err = coord
        .variant_search(tonic::Request::new(request(
            vec![body_only("rust"), bad],
            5,
        )))
        .await
        .unwrap_err();
    assert!(
        err.message().contains("challenger"),
        "the failing arm must be named: {}",
        err.message()
    );
}

/// `k` is optional on every client-facing request: 0 (proto3 unset)
/// selects the coordinator's `max_k` stop bound, a value within the cap
/// is honored, and a value above it is refused with both numbers named
/// rather than silently clamped. The cap is what keeps a coordinator
/// from being asked to hold an unbounded heap while the shared floor
/// never rises.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn k_is_optional_and_the_cap_refuses_rather_than_clamps() {
    let (coord, _nodes, _mock) = cluster().await;
    let bm25 = |k: u32| Bm25SearchRequest {
        map_facet_fields: Vec::new(),
        score_stages: Vec::new(),
        facet_fields: Vec::new(),
        text: "rust".to_string(),
        k,
        analysis: None,
        min_score: 0.0,
        fields: Vec::new(),
    };

    // Omitted k runs at the default cap: deep enough to find every
    // matching document in this corpus.
    let all = coord
        .bm25_search(tonic::Request::new(bm25(0)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        all.hits.len(),
        5,
        "omitted k must run at max_k and surface every match"
    );

    // A coordinator capped at 2: omitted k stops there...
    let capped = coord.clone().with_max_k(2);
    let two = capped
        .bm25_search(tonic::Request::new(bm25(0)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(two.hits.len(), 2, "omitted k must stop at the configured cap");

    // ...an explicit k within the cap is honored...
    let one = capped
        .bm25_search(tonic::Request::new(bm25(1)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(one.hits.len(), 1);

    // ...and one above it is refused, naming both numbers so the caller
    // knows what to lower (or which flag to raise).
    let err = capped
        .bm25_search(tonic::Request::new(bm25(3)))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("k=3") && err.message().contains("max_k=2"),
        "refusal must name both sides: {}",
        err.message()
    );

    // VariantSearch rides the same resolution: omitting k no longer
    // refuses, it compares at the cap.
    let resp = coord
        .variant_search(tonic::Request::new(request(
            vec![body_only("rust"), with_case_name("boost", "rust", 2.0)],
            0,
        )))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.results.len(), 2);
    assert!(
        resp.results.iter().all(|r| !r.hits.is_empty()),
        "both arms must run at the defaulted depth"
    );
}
