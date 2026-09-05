//! Segment pruning from per-segment summaries (`docs/segment-pruning.md`).
//!
//! A persisted segmented shard whose rows arrive in `year` order under a
//! small seal bound, so every sealed segment covers one year. A filter
//! on `year` then rules whole segments out from their summaries, and
//! the counts on each route say how many. The answer never moves: every
//! route is compared bitwise against the same shard served with
//! `--segment-pruning=false`.

mod common;

use std::path::PathBuf;

use common::{fit_calibration, start_empty_node, start_opened_node, unit_vectors};
use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::{segments_root, Layout, NodeConfig};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    selection_query, stream_search_request, stream_search_response, AddDocumentsRequest,
    AddVectorsRequest, AggregateOp, AggregateRequest, AggregateShardRequest, Aggregation,
    Bm25SearchRequest, BooleanQuery, BrowseShardRequest, CompiledAggregation,
    CompositeSearchStrategy, DenseQuery, FacetValue, FilterQuery, FlushRequest, IntegerValue,
    LexicalQuery, QueryHit, QueryRequest, QueryResponse, SearchQuery, SelectionOperator,
    SelectionQuery, SetCalibrationRequest, StartStreamSearch, StreamSearchRequest, ValueExpr,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

const DIM: usize = 16;
const BIT_WIDTH: usize = 4;
/// Rows per year, and the seal bound: one sealed segment per year.
const PER_YEAR: usize = 4;
const YEARS: [i64; 3] = [2000, 2001, 2002];
/// The three full years plus the one-row 2003 tail, sealed by the flush
/// after ingest.
const SEALED: u32 = 4;

fn tempdir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("segprune-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn config(index_path: PathBuf, segment_pruning: bool) -> NodeConfig {
    NodeConfig {
        index_path: Some(index_path),
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        facet_fields: vec!["court".to_string()],
        integer_fields: vec!["year".to_string(), "pages".to_string()],
        layout: Layout::Segments,
        seal_tail_docs: PER_YEAR as u32,
        segment_pruning,
        wal: false,
        ..Default::default()
    }
}

/// The corpus: `PER_YEAR` rows per year, in year order, plus one row in
/// the tail (year 2003) so every year seals. "zebra" occurs only in the
/// 2002 rows; every row says "search".
fn rows() -> Vec<(i64, String, &'static str)> {
    let mut out = Vec::new();
    for (yi, year) in YEARS.iter().enumerate() {
        for j in 0..PER_YEAR {
            let i = yi * PER_YEAR + j;
            let extra = if *year == 2002 { " zebra" } else { "" };
            let court = if i.is_multiple_of(2) { "ca9" } else { "scotus" };
            out.push((*year, format!("opinion {i} about search{extra}"), court));
        }
    }
    out.push((2003, "opinion 12 about search tail".to_string(), "ca9"));
    out
}

fn corpus() -> Vec<f32> {
    unit_vectors(rows().len(), DIM, 0x5E6_7A11)
}

type Served = (
    String,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
);

/// A fresh shard on `dir`.
async fn start(dir: &std::path::Path, segment_pruning: bool) -> Served {
    start_empty_node(config(dir.join("prune.tv"), segment_pruning)).await
}

/// The shard on `dir` reopened from disk, the recovery path.
async fn reopen(dir: &std::path::Path, segment_pruning: bool) -> Served {
    start_opened_node(config(dir.join("prune.tv"), segment_pruning)).await
}

/// Ingest the corpus in blocks of `PER_YEAR` documents followed by their
/// vectors, so the tail seals with both counts agreeing at every bound.
async fn ingest(addr: &str) {
    let all = rows();
    let vectors = corpus();
    let sample = &vectors[..vectors.len().min(8 * DIM)];
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
    for (block, chunk) in all.chunks(PER_YEAR).enumerate() {
        let (tx, rx) = mpsc::channel(8);
        for (year, text, court) in chunk {
            tx.send(AddDocumentsRequest {
                text: text.clone(),
                analysis: Some(body_spec()),
                facets: vec![FacetValue {
                    field: "court".into(),
                    value: (*court).to_string(),
                }],
                integers: vec![IntegerValue {
                    field: "year".into(),
                    value: *year,
                }],
                ..Default::default()
            })
            .await
            .unwrap();
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
        let start = block * PER_YEAR * DIM;
        let end = (start + chunk.len() * DIM).min(vectors.len());
        let (tx, rx) = mpsc::channel(2);
        tx.send(AddVectorsRequest {
            vectors: vectors[start..end].to_vec(),
            dim: DIM as u32,
        })
        .await
        .unwrap();
        drop(tx);
        client.add_vectors(ReceiverStream::new(rx)).await.unwrap();
    }
    // Seal the tail too, so the on-disk set is the whole corpus and a
    // reopened shard answers the same questions.
    client.flush(FlushRequest {}).await.unwrap();
}

fn coordinator(addr: &str) -> CoordinatorServiceImpl {
    CoordinatorServiceImpl::new(vec![addr.to_string()]).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    )
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

fn dense(id: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(pipestream_search::pb::search_query::Query::Dense(
                DenseQuery {
                    vector: corpus()[..DIM].to_vec(),
                    ..Default::default()
                },
            )),
        })),
    }
}

fn and(clauses: Vec<SelectionQuery>) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
            operator: SelectionOperator::And as i32,
            clauses,
            scoring: None,
        })),
    }
}

fn boolean(must: Vec<SelectionQuery>, must_not: Vec<SelectionQuery>) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Boolean(BooleanQuery {
            must,
            should: Vec::new(),
            must_not,
            minimum_should_match: 0,
            aggregate: None,
        })),
    }
}

fn request(selection: SelectionQuery) -> QueryRequest {
    QueryRequest {
        request_id: "prune".into(),
        k: 20,
        selection: Some(selection),
        profile: true,
        ..Default::default()
    }
}

async fn query(c: &CoordinatorServiceImpl, selection: SelectionQuery) -> QueryResponse {
    SearchService::query(c, Request::new(request(selection)))
        .await
        .unwrap()
        .into_inner()
}

fn ids(hits: &[QueryHit]) -> Vec<(u64, f32)> {
    hits.iter().map(|h| (h.doc_id, h.score)).collect()
}

fn counts(response: &QueryResponse) -> (u32, u32) {
    let p = response.profile.as_ref().expect("profile requested");
    (p.segments_total, p.segments_skipped)
}

/// Every shape the profile counts for: the segments consulted and the
/// segments a `year` predicate must rule out on the four sealed ones
/// (2000, 2001, 2002, and the one-row 2003). A boolean root counts each
/// membership resolution, so a filter clause and a lexical clause
/// together consult the four segments twice.
fn cases() -> Vec<(&'static str, SelectionQuery, (u32, u32))> {
    let n = SEALED;
    vec![
        (
            "dense >= 2002",
            and(vec![cel("f", "year >= 2002"), dense("v")]),
            (n, 2),
        ),
        (
            "dense >= 2001",
            and(vec![cel("f", "year >= 2001"), dense("v")]),
            (n, 1),
        ),
        (
            "dense > 2003",
            and(vec![cel("f", "year > 2003"), dense("v")]),
            (n, 4),
        ),
        (
            "dense < 1990",
            and(vec![cel("f", "year < 1990"), dense("v")]),
            (n, 4),
        ),
        (
            "dense == 2001",
            and(vec![cel("f", "year == 2001"), dense("v")]),
            (n, 3),
        ),
        (
            "lexical >= 2002",
            and(vec![cel("f", "year >= 2002"), lexical("l", "search")]),
            (n, 2),
        ),
        (
            "lexical < 2001",
            and(vec![cel("f", "year < 2001"), lexical("l", "search")]),
            (n, 3),
        ),
        ("browse >= 2002", cel("f", "year >= 2002"), (n, 2)),
        ("browse < 1990", cel("f", "year < 1990"), (n, 4)),
        // A column no row carries: present == 0 rules every sealed
        // segment out.
        (
            "dense pages",
            and(vec![cel("f", "pages >= 0"), dense("v")]),
            (n, 4),
        ),
        // OR with a facet leaf and NOT are never pruned: the summary
        // cannot bound them.
        (
            "dense or facet",
            and(vec![
                cel("f", r#"year >= 2002 || court == "ca9""#),
                dense("v"),
            ]),
            (n, 0),
        ),
        (
            "dense not",
            and(vec![cel("f", "!(year >= 2002)"), dense("v")]),
            (n, 0),
        ),
        (
            "lexical not",
            and(vec![cel("f", "!(year >= 2001)"), lexical("l", "search")]),
            (n, 0),
        ),
        // The boolean planner: the filter bitmap prunes, the lexical
        // bitmap skips the segments without the term. "search" is in
        // every segment, "zebra" only in 2002.
        (
            "boolean filter",
            boolean(
                vec![cel("f", "year >= 2002"), lexical("l", "search")],
                vec![],
            ),
            (2 * n, 2),
        ),
        (
            "boolean zebra",
            boolean(vec![lexical("l", "zebra"), dense("v")], vec![]),
            (n, 3),
        ),
        (
            "boolean zebra+filter",
            boolean(
                vec![cel("f", "year >= 2002"), lexical("l", "zebra")],
                vec![],
            ),
            (2 * n, 5),
        ),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_year_filter_prunes_the_segments_it_cannot_match_on_every_route() {
    let dir = tempdir("routes");
    let (addr, handle) = start(&dir, true).await;
    ingest(&addr).await;
    let set =
        pipestream_search::segments::OpenedSegmentSet::open(segments_root(&dir.join("prune.tv")))
            .unwrap();
    assert_eq!(
        set.len() as u32,
        SEALED,
        "one sealed segment per year, plus the tail"
    );
    for (i, year) in YEARS.iter().enumerate() {
        let summary = set
            .metadata(i)
            .summary
            .as_ref()
            .expect("a seal writes a summary");
        let years = summary
            .int_columns
            .iter()
            .find(|c| c.name == "year")
            .unwrap();
        assert_eq!(
            (years.min, years.max, years.present),
            (*year, *year, PER_YEAR as u64)
        );
        let pages = summary
            .int_columns
            .iter()
            .find(|c| c.name == "pages")
            .unwrap();
        assert_eq!(pages.present, 0);
        assert!(
            pages.min > pages.max,
            "an absent column has the empty range"
        );
    }
    let c = coordinator(&addr);
    let mut with_pruning = Vec::new();
    for (name, selection, expected) in cases() {
        let response = query(&c, selection).await;
        assert_eq!(
            counts(&response),
            expected,
            "{name}: {:?}",
            response.profile
        );
        with_pruning.push((name, ids(&response.hits)));
    }
    // Results that a naive rule would lose are present.
    let not_2002 = query(&c, and(vec![cel("f", "!(year >= 2002)"), dense("v")])).await;
    assert_eq!(
        not_2002.hits.len(),
        2 * PER_YEAR,
        "years 2000 and 2001 pass the NOT"
    );
    let or_facet = query(
        &c,
        and(vec![
            cel("f", r#"year >= 2002 || court == "ca9""#),
            dense("v"),
        ]),
    )
    .await;
    assert!(
        or_facet.hits.iter().any(|h| h.doc_id < PER_YEAR as u64),
        "a 2000 row with court ca9 passes the OR"
    );
    let pages = query(&c, and(vec![cel("f", "pages >= 0"), dense("v")])).await;
    assert!(pages.hits.is_empty(), "no row carries pages");
    handle.abort();

    // The same shard reopened with pruning off: identical hits, zero
    // skipped, the same totals.
    let (addr, handle) = reopen(&dir, false).await;
    let c = coordinator(&addr);
    for ((name, selection, (total, _)), (_, expected)) in cases().into_iter().zip(&with_pruning) {
        let response = query(&c, selection).await;
        assert_eq!(
            counts(&response),
            (total, 0),
            "{name}: pruning off skips none"
        );
        assert_eq!(
            &ids(&response.hits),
            expected,
            "{name}: hits differ with pruning off"
        );
    }
    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_segment_without_a_summary_is_never_pruned() {
    let dir = tempdir("stripped");
    let (addr, handle) = start(&dir, true).await;
    ingest(&addr).await;
    handle.abort();
    // Strip the summary from the middle segment in the set manifest and
    // its own file, as a segment sealed before summaries existed reads.
    let root = segments_root(&dir.join("prune.tv"));
    let manifest_path = root.join("segments.json");
    let mut manifest: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest_path).unwrap()).unwrap();
    let segments = manifest["segments"].as_array_mut().unwrap();
    assert_eq!(segments.len() as u32, SEALED);
    let stripped_id = segments[1]["segment_id"].as_str().unwrap().to_string();
    segments[1].as_object_mut().unwrap().remove("summary");
    std::fs::write(
        &manifest_path,
        serde_json::to_string_pretty(&manifest).unwrap(),
    )
    .unwrap();
    let meta_path = root.join(&stripped_id).join("segment.json");
    if meta_path.exists() {
        let mut meta: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&meta_path).unwrap()).unwrap();
        meta.as_object_mut().unwrap().remove("summary");
        std::fs::write(&meta_path, serde_json::to_string_pretty(&meta).unwrap()).unwrap();
    }

    let (addr, handle) = reopen(&dir, true).await;
    let c = coordinator(&addr);
    // year >= 2002 rules out 2000 and 2001; the 2001 segment has no
    // summary now, so only 2000 is skipped.
    let response = query(&c, and(vec![cel("f", "year >= 2002"), dense("v")])).await;
    assert_eq!(counts(&response), (SEALED, 1));
    assert_eq!(response.hits.len(), PER_YEAR + 1);
    let response = query(&c, and(vec![cel("f", "year < 1990"), dense("v")])).await;
    assert_eq!(counts(&response), (SEALED, 3));
    assert!(response.hits.is_empty());
    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn facets_aggregation_browse_and_the_stream_report_the_counts() {
    let dir = tempdir("shard-routes");
    let (addr, handle) = start(&dir, true).await;
    ingest(&addr).await;
    let c = coordinator(&addr);

    // Facets over a filtered match set on the public BM25 route.
    let faceted = SearchService::bm25_search(
        &c,
        Request::new(Bm25SearchRequest {
            text: "search".into(),
            k: 20,
            analysis: Some(body_spec()),
            facet_fields: vec!["court".into()],
            filter: "year >= 2002".into(),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        (faceted.segments_total, faceted.segments_skipped),
        (SEALED, 2)
    );
    assert_eq!(faceted.hits.len(), PER_YEAR + 1);
    let counted: u64 = faceted.facets[0].counts.iter().map(|c| c.count).sum();
    assert_eq!(counted, (PER_YEAR + 1) as u64);

    // Aggregation over a pooled query: the profile carries the leaf's
    // counts; the shard's own fold reports its own.
    let pooled = SearchService::query(
        &c,
        Request::new(QueryRequest {
            aggregate: Some(AggregateRequest {
                aggregations: vec![Aggregation {
                    name: "n".into(),
                    expression: "1".into(),
                    op: AggregateOp::Count as i32,
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..request(and(vec![cel("f", "year >= 2002"), dense("v")]))
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(counts(&pooled), (SEALED, 2));
    assert_eq!(
        pooled.aggregate.as_ref().unwrap().matched,
        (PER_YEAR + 1) as u64
    );

    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    let filter = pipestream_search::cel::compile_filter("year >= 2002").unwrap();
    let folded = client
        .aggregate_shard(AggregateShardRequest {
            filter: filter.clone(),
            aggregations: vec![CompiledAggregation {
                expr: Some(ValueExpr {
                    expr: Some(pipestream_search::pb::value_expr::Expr::IntLiteral(1)),
                }),
                op: AggregateOp::Count as i32,
                name: "n".into(),
                max_distinct: 0,
            }],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (folded.segments_total, folded.segments_skipped),
        (SEALED, 2)
    );
    assert_eq!(folded.matched, (PER_YEAR + 1) as u64);

    let browsed = client
        .browse_shard(BrowseShardRequest {
            k: 20,
            first_page: true,
            filter: filter.clone(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (browsed.segments_total, browsed.segments_skipped),
        (SEALED, 2)
    );
    assert_eq!(browsed.doc_ids.len(), PER_YEAR + 1);

    // The streaming node scan: the counts ride the terminal summary and
    // the certificate still holds over the filtered corpus.
    let (tx, rx) = mpsc::channel(2);
    tx.send(StreamSearchRequest {
        payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
            request_id: "prune-stream".to_string(),
            vector: corpus()[..DIM].to_vec(),
            filter,
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    drop(tx);
    let mut inbound = client
        .stream_search(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    let mut emitted = 0usize;
    let summary = loop {
        let message = inbound.message().await.unwrap().expect("terminal summary");
        match message.payload {
            Some(stream_search_response::Payload::Summary(summary)) => break summary,
            Some(stream_search_response::Payload::Batch(batch)) => {
                emitted += batch.hits.len();
            }
            _ => {}
        }
    };
    assert!(summary.completed, "{summary:?}");
    assert_eq!(
        (summary.segments_total, summary.segments_skipped),
        (SEALED, 2)
    );
    assert!(emitted > 0, "the survivors stream out");
    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
