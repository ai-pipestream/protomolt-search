//! Partitioned layout and segment pruning, measured on one local shard
//! (`docs/benchmarks/partition-pruning-2026-09.md`).
//!
//! One segment-layout node plus an in-process coordinator on loopback.
//! N rows with random unit vectors, a short body over a small vocabulary
//! (one common term in about 30% of the rows, one rare term in about
//! 0.5%), and a `year` integer column uniform over 1980..=2019, arriving
//! in shuffled year order under a seal bound of `--bound` rows. The same
//! cases run on the bucket layout, then after `CompactShard` with
//! `partition_column = "year"`, each with segment pruning on and off.
//! Hits are compared bitwise between pruning on and off, and by stable
//! identity across the compaction (which renumbers positional ids).
//!
//! `cargo run --release --example partition_bench -- [--rows=N]
//! [--dim=D] [--bound=B] [--queries=Q] [--dir=PATH] [--out=PATH]`

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::harness::{
    fit_calibration, start_empty_node, start_opened_node, unit_vectors,
};
use pipestream_search::node::{segments_root, Layout, NodeConfig};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    selection_query, AddDocumentsRequest, AddVectorsRequest, AggregateOp, AggregateRequest,
    Aggregation, BooleanQuery, CompactShardRequest, CompositeSearchStrategy, DenseQuery,
    FilterQuery, FlushRequest, GetDocumentsRequest, IntegerValue, LexicalQuery, QueryHit,
    QueryRequest, QueryResponse, QuerySort, SearchQuery, SelectionOperator, SelectionQuery,
    SetCalibrationRequest,
};
use pipestream_search::segments::SegmentCatalog;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

const BIT_WIDTH: usize = 4;
const K: u32 = 10;
const YEAR_LO: i64 = 1980;
const YEAR_HI: i64 = 2019;
/// Rows per AddDocuments / AddVectors call during ingest.
const INGEST_BLOCK: usize = 25_000;

fn opt(args: &[String], name: &str) -> Option<String> {
    let prefix = format!("--{name}=");
    args.iter()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
}

struct Params {
    rows: usize,
    dim: usize,
    bound: u32,
    queries: usize,
    dir: PathBuf,
    out: PathBuf,
}

/// A small deterministic generator (SplitMix64).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn chance(&mut self, per_million: u64) -> bool {
        self.below(1_000_000) < per_million
    }
}

/// The vocabulary the bodies draw from, plus the marker terms.
const COMMON: &str = "commonterm";
const RARE: &str = "rareterm";

fn vocabulary() -> Vec<String> {
    let syllables = [
        "ka", "lo", "mi", "ru", "ne", "ta", "vo", "si", "pe", "du", "ga", "ho", "ji", "wu", "ze",
        "ba",
    ];
    let mut words = Vec::new();
    for a in syllables {
        for b in syllables {
            words.push(format!("{a}{b}x"));
        }
    }
    words
}

/// One row: its stable identity token, body text, and year.
fn row(seq: usize, rng: &mut Rng, vocab: &[String]) -> (String, i64) {
    let words = 4 + rng.below(9) as usize;
    let mut text = format!("d{seq:07}");
    for _ in 0..words {
        text.push(' ');
        text.push_str(&vocab[rng.below(vocab.len() as u64) as usize]);
    }
    if rng.chance(300_000) {
        let times = 1 + rng.below(3);
        for _ in 0..times {
            text.push(' ');
            text.push_str(COMMON);
        }
    }
    if rng.chance(5_000) {
        text.push(' ');
        text.push_str(RARE);
    }
    let year = YEAR_LO + rng.below((YEAR_HI - YEAR_LO + 1) as u64) as i64;
    (text, year)
}

fn config(index_path: PathBuf, bound: u32, segment_pruning: bool) -> NodeConfig {
    NodeConfig {
        index_path: Some(index_path),
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        integer_fields: vec!["year".to_string()],
        layout: Layout::Segments,
        seal_tail_docs: bound,
        segment_pruning,
        wal: true,
        wal_buckets: 16,
        ..Default::default()
    }
}

async fn ingest(addr: &str, p: &Params, vectors: &[f32]) {
    let vocab = vocabulary();
    let mut rng = Rng(0x5EED_0001);
    let sample = &vectors[..vectors.len().min(4096 * p.dim)];
    let (shift, scale) = fit_calibration(p.dim, BIT_WIDTH, sample);
    let mut client = NodeServiceClient::connect(addr.to_string())
        .await
        .unwrap()
        .max_encoding_message_size(usize::MAX)
        .max_decoding_message_size(usize::MAX);
    client
        .set_calibration(SetCalibrationRequest {
            dim: p.dim as u32,
            bit_width: BIT_WIDTH as u32,
            shift,
            scale,
        })
        .await
        .unwrap();
    let mut seq = 0usize;
    while seq < p.rows {
        let end = (seq + INGEST_BLOCK).min(p.rows);
        let (tx, rx) = mpsc::channel(64);
        let sender = tokio::spawn({
            let mut rows = Vec::with_capacity(end - seq);
            for s in seq..end {
                rows.push(row(s, &mut rng, &vocab));
            }
            async move {
                for (text, year) in rows {
                    tx.send(AddDocumentsRequest {
                        text,
                        analysis: Some(body_spec()),
                        integers: vec![IntegerValue {
                            field: "year".into(),
                            value: year,
                        }],
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                }
            }
        });
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
        sender.await.unwrap();
        let (tx, rx) = mpsc::channel(4);
        let block = vectors[seq * p.dim..end * p.dim].to_vec();
        let dim = p.dim as u32;
        let sender = tokio::spawn(async move {
            for chunk in block.chunks(2_000 * dim as usize) {
                tx.send(AddVectorsRequest {
                    vectors: chunk.to_vec(),
                    dim,
                })
                .await
                .unwrap();
            }
        });
        client.add_vectors(ReceiverStream::new(rx)).await.unwrap();
        sender.await.unwrap();
        seq = end;
    }
    client.flush(FlushRequest {}).await.unwrap();
}

fn coordinator(addr: &str) -> CoordinatorServiceImpl {
    CoordinatorServiceImpl::new(vec![addr.to_string()]).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    )
}

fn cel(cel: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Filter(FilterQuery {
            id: "f".to_string(),
            predicate: Some(pipestream_search::pb::filter_query::Predicate::Cel(
                cel.to_string(),
            )),
        })),
    }
}

fn lexical(text: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: "l".to_string(),
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

fn dense(vector: Vec<f32>) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: "v".to_string(),
            query: Some(pipestream_search::pb::search_query::Query::Dense(
                DenseQuery {
                    vector,
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

fn boolean(must: Vec<SelectionQuery>) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Boolean(BooleanQuery {
            must,
            should: Vec::new(),
            must_not: Vec::new(),
            minimum_should_match: 0,
            aggregate: None,
        })),
    }
}

/// One measured case: its label and the request for query index `i`.
struct Case {
    label: &'static str,
    build: Box<dyn Fn(usize) -> QueryRequest>,
}

fn request(selection: SelectionQuery) -> QueryRequest {
    QueryRequest {
        request_id: "bench".into(),
        k: K,
        selection: Some(selection),
        profile: true,
        ..Default::default()
    }
}

fn cases(queries: Vec<Vec<f32>>) -> Vec<Case> {
    let q = std::sync::Arc::new(queries);
    let dense_q = {
        let q = q.clone();
        move |i: usize| dense(q[i % q.len()].clone())
    };
    let filters: [(&'static str, &'static str); 3] = [
        ("year >= 2018 (5%)", "year >= 2018"),
        ("year >= 2010 (25%)", "year >= 2010"),
        ("year >= 2000 (50%)", "year >= 2000"),
    ];
    let mut out: Vec<Case> = Vec::new();
    let dq = dense_q.clone();
    out.push(Case {
        label: "dense k=10, no filter",
        build: Box::new(move |i| request(dq(i))),
    });
    for (label, expr) in filters {
        let dq = dense_q.clone();
        out.push(Case {
            label: Box::leak(format!("dense k=10, {label}").into_boxed_str()),
            build: Box::new(move |i| request(and(vec![cel(expr), dq(i)]))),
        });
    }
    for (label, expr) in filters {
        out.push(Case {
            label: Box::leak(format!("BM25 common term, {label}").into_boxed_str()),
            build: Box::new(move |_| request(and(vec![cel(expr), lexical(COMMON)]))),
        });
    }
    let dq = dense_q.clone();
    out.push(Case {
        label: "boolean AND(rare term, dense)",
        build: Box::new(move |i| request(boolean(vec![lexical(RARE), dq(i)]))),
    });
    let dq = dense_q.clone();
    out.push(Case {
        label: "boolean AND(common term, dense)",
        build: Box::new(move |i| request(boolean(vec![lexical(COMMON), dq(i)]))),
    });
    out.push(Case {
        label: "boolean MUST(common term, year >= 2010)",
        build: Box::new(move |_| request(boolean(vec![lexical(COMMON), cel("year >= 2010")]))),
    });
    let dq = dense_q.clone();
    out.push(Case {
        label: "boolean MUST(dense, year >= 2010)",
        build: Box::new(move |i| request(boolean(vec![dq(i), cel("year >= 2010")]))),
    });
    out.push(Case {
        label: "browse year >= 2018, sorted by year",
        build: Box::new(|_| QueryRequest {
            sort: vec![QuerySort {
                column: "year".into(),
                descending: false,
            }],
            ..request(cel("year >= 2018"))
        }),
    });
    out.push(Case {
        label: "aggregation count, year >= 2018",
        build: Box::new(|_| QueryRequest {
            aggregate: Some(AggregateRequest {
                aggregations: vec![Aggregation {
                    name: "n".into(),
                    expression: "1".into(),
                    op: AggregateOp::Count as i32,
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..request(cel("year >= 2018"))
        }),
    });
    out
}

/// What one case answered: the hits (id, score bits, sort value), the
/// aggregate count, and the segment counts from the profile.
#[derive(Clone, Debug, PartialEq)]
struct Answer {
    hits: Vec<(u64, u32, i64)>,
    count: Option<i64>,
    total: u32,
    skipped: u32,
}

fn answer(r: &QueryResponse) -> Answer {
    let hits = r
        .hits
        .iter()
        .map(|h: &QueryHit| {
            let sort = h
                .sort_values
                .first()
                .and_then(|v| match &v.value {
                    Some(pipestream_search::pb::sort_value::Value::Integer(i)) => Some(*i),
                    _ => None,
                })
                .unwrap_or(0);
            (h.doc_id, h.score.to_bits(), sort)
        })
        .collect();
    let count = r.aggregate.as_ref().and_then(|a| {
        a.results.first().and_then(|x| match x.value {
            Some(pipestream_search::pb::aggregate_result::Value::IntValue(v)) => Some(v),
            _ => None,
        })
    });
    let p = r.profile.as_ref().expect("profile requested");
    Answer {
        hits,
        count,
        total: p.segments_total,
        skipped: p.segments_skipped,
    }
}

#[derive(Clone)]
struct Measured {
    label: &'static str,
    p50_ms: f64,
    p90_ms: f64,
    total: u32,
    skipped: u32,
    answers: Vec<Answer>,
}

async fn measure(c: &CoordinatorServiceImpl, cases: &[Case], queries: usize) -> Vec<Measured> {
    let mut out = Vec::new();
    for case in cases {
        // Two warm-up runs per case, untimed.
        for i in 0..2 {
            let _ = SearchService::query(c, Request::new((case.build)(i)))
                .await
                .unwrap_or_else(|e| panic!("{}: {e}", case.label));
        }
        let mut walls = Vec::with_capacity(queries);
        let mut answers = Vec::with_capacity(queries);
        for i in 0..queries {
            let req = (case.build)(i);
            let t = Instant::now();
            let r = SearchService::query(c, Request::new(req))
                .await
                .unwrap_or_else(|e| panic!("{}: {e}", case.label))
                .into_inner();
            walls.push(t.elapsed());
            answers.push(answer(&r));
        }
        walls.sort();
        let pct = |p: f64| -> f64 {
            let idx = ((walls.len() as f64 - 1.0) * p).round() as usize;
            walls[idx].as_secs_f64() * 1000.0
        };
        let total = answers[0].total;
        let skipped = answers[0].skipped;
        out.push(Measured {
            label: case.label,
            p50_ms: pct(0.5),
            p90_ms: pct(0.9),
            total,
            skipped,
            answers,
        });
    }
    out
}

fn table(title: &str, on: &[Measured], off: &[Measured]) -> String {
    let mut s = String::new();
    s.push_str(&format!("### {title}\n\n"));
    s.push_str(
        "| case | segments skipped / total | p50 on (ms) | p90 on (ms) | p50 off (ms) | p90 off (ms) |\n",
    );
    s.push_str("|---|---:|---:|---:|---:|---:|\n");
    for (a, b) in on.iter().zip(off) {
        s.push_str(&format!(
            "| {} | {} / {} | {:.1} | {:.1} | {:.1} | {:.1} |\n",
            a.label, a.skipped, a.total, a.p50_ms, a.p90_ms, b.p50_ms, b.p90_ms
        ));
    }
    s
}

/// Stable identity of a hit: the `d0000123` token at the front of its
/// text, read from the node.
async fn identities(addr: &str, ids: &[u64]) -> Vec<String> {
    if ids.is_empty() {
        return Vec::new();
    }
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let docs = client
        .get_documents(GetDocumentsRequest {
            doc_ids: ids.to_vec(),
        })
        .await
        .unwrap()
        .into_inner()
        .documents;
    ids.iter()
        .map(|id| {
            let d = docs
                .iter()
                .find(|d| d.doc_id == *id)
                .unwrap_or_else(|| panic!("document {id} not returned"));
            d.text.split(' ').next().unwrap().to_string()
        })
        .collect()
}

/// Pruning on and off must agree bitwise: same ids, same score bits,
/// same order, same counts.
fn assert_bitwise(layout: &str, on: &[Measured], off: &[Measured]) {
    for (a, b) in on.iter().zip(off) {
        for (i, (x, y)) in a.answers.iter().zip(&b.answers).enumerate() {
            assert!(
                x.hits == y.hits && x.count == y.count,
                "{layout}: {} query {i}: pruning on and off disagree\n on:  {:?}\n off: {:?}",
                a.label,
                x,
                y
            );
        }
    }
}

/// Across the compaction, positional ids are renumbered, so hits compare
/// by stable identity. Scores must match bit for bit as a sorted list;
/// the identity set must match for hits strictly above the k-th score
/// (a tie at the boundary is broken by positional id, which the
/// compaction changed); browse rows compare by sort value; the count
/// must match.
async fn assert_stable(
    before_addr: Option<&str>,
    before: &[(Measured, Vec<Vec<String>>)],
    after_addr: &str,
    after: &[Measured],
) -> usize {
    let _ = before_addr;
    let mut boundary_ties = 0usize;
    for ((a, a_ids), b) in before.iter().zip(after) {
        for (i, (x, y)) in a.answers.iter().zip(&b.answers).enumerate() {
            assert_eq!(x.count, y.count, "{}: query {i}: count moved", a.label);
            let xs: Vec<u32> = x.hits.iter().map(|h| h.1).collect();
            let ys: Vec<u32> = y.hits.iter().map(|h| h.1).collect();
            assert_eq!(xs, ys, "{}: query {i}: score bits moved", a.label);
            let xv: Vec<i64> = x.hits.iter().map(|h| h.2).collect();
            let yv: Vec<i64> = y.hits.iter().map(|h| h.2).collect();
            assert_eq!(xv, yv, "{}: query {i}: sort values moved", a.label);
            let y_ids: Vec<u64> = y.hits.iter().map(|h| h.0).collect();
            let y_names = identities(after_addr, &y_ids).await;
            let x_names = &a_ids[i];
            if x_names == &y_names {
                continue;
            }
            // Only hits at the boundary score (or, for a browse, the
            // boundary sort value) may differ; everything above it must
            // be the same set.
            let last_score = xs.last().copied();
            let last_sort = xv.last().copied();
            let strict = |hits: &Vec<(u64, u32, i64)>, names: &Vec<String>| -> BTreeSet<String> {
                hits.iter()
                    .zip(names)
                    .filter(|(h, _)| {
                        if a.label.starts_with("browse") {
                            Some(h.2) != last_sort
                        } else {
                            Some(h.1) != last_score
                        }
                    })
                    .map(|(_, n)| n.clone())
                    .collect()
            };
            assert_eq!(
                strict(&x.hits, x_names),
                strict(&y.hits, &y_names),
                "{}: query {i}: hit identities moved above the tie boundary",
                a.label
            );
            boundary_ties += 1;
        }
    }
    boundary_ties
}

/// One segment's id, rows, and partition range, plus the set's key.
type Ranges = (Vec<(String, u64, Option<(i64, i64)>)>, Option<String>);

fn manifest_ranges(index_path: &Path) -> Ranges {
    let manifest = SegmentCatalog::read_manifest(&segments_root(index_path))
        .unwrap()
        .expect("a catalog manifest");
    let segments = manifest
        .segments
        .iter()
        .map(|s| {
            let range = s
                .summary
                .as_ref()
                .and_then(|x| x.partition.as_ref())
                .map(|p| (p.lo, p.hi));
            (s.segment_id.clone(), s.rows, range)
        })
        .collect();
    (segments, manifest.partition_key)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let p = Params {
        rows: opt(&args, "rows").map_or(2_000_000, |v| v.parse().unwrap()),
        dim: opt(&args, "dim").map_or(128, |v| v.parse().unwrap()),
        bound: opt(&args, "bound").map_or(125_000, |v| v.parse().unwrap()),
        queries: opt(&args, "queries").map_or(20, |v| v.parse().unwrap()),
        dir: opt(&args, "dir").map_or_else(
            || std::env::temp_dir().join(format!("partition-bench-{}", std::process::id())),
            PathBuf::from,
        ),
        out: opt(&args, "out").map_or_else(
            || PathBuf::from("docs/benchmarks/partition-pruning-2026-09.md"),
            PathBuf::from,
        ),
    };
    let _ = std::fs::remove_dir_all(&p.dir);
    std::fs::create_dir_all(&p.dir).unwrap();
    let index_path = p.dir.join("bench.tv");
    let cmdline = args.join(" ");
    eprintln!(
        "rows={} dim={} bound={} queries={} dir={}",
        p.rows,
        p.dim,
        p.bound,
        p.queries,
        p.dir.display()
    );

    let vectors = unit_vectors(p.rows, p.dim, 0xB35E_0001);
    let queries: Vec<Vec<f32>> = unit_vectors(p.queries, p.dim, 0xBEE5)
        .chunks(p.dim)
        .map(<[f32]>::to_vec)
        .collect();

    // Bucket layout, pruning on.
    let (addr, handle) = start_empty_node(config(index_path.clone(), p.bound, true)).await;
    let t = Instant::now();
    ingest(&addr, &p, &vectors).await;
    let ingest_wall = t.elapsed();
    eprintln!("ingest: {:.1} s", ingest_wall.as_secs_f64());
    let (before_segments, key) = manifest_ranges(&index_path);
    eprintln!(
        "bucket layout: {} sealed segments, partition key {:?}",
        before_segments.len(),
        key
    );
    let cases = cases(queries);
    let coord = coordinator(&addr);
    let bucket_on = measure(&coord, &cases, p.queries).await;
    let mut bucket_ids = Vec::new();
    for m in &bucket_on {
        let mut per_query = Vec::new();
        for a in &m.answers {
            let ids: Vec<u64> = a.hits.iter().map(|h| h.0).collect();
            per_query.push(identities(&addr, &ids).await);
        }
        bucket_ids.push(per_query);
    }
    handle.abort();
    let _ = handle.await;

    // Bucket layout, pruning off.
    let (addr, handle) = start_opened_node(config(index_path.clone(), p.bound, false)).await;
    let coord = coordinator(&addr);
    let bucket_off = measure(&coord, &cases, p.queries).await;
    assert_bitwise("bucket layout", &bucket_on, &bucket_off);
    handle.abort();
    let _ = handle.await;

    // Compaction to the partitioned layout, pruning on.
    let (addr, handle) = start_opened_node(config(index_path.clone(), p.bound, true)).await;
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    let mut req = Request::new(CompactShardRequest {
        partition_column: "year".into(),
        tail_bound: p.bound,
        ..Default::default()
    });
    req.set_timeout(Duration::from_secs(3600));
    let t = Instant::now();
    let compacted = client
        .compact_shard(req)
        .await
        .unwrap_or_else(|e| panic!("CompactShard: {e}"))
        .into_inner();
    let compact_wall = t.elapsed();
    eprintln!(
        "compaction: {:.1} s, rows {} -> {}, layout {}, partition column {:?}",
        compact_wall.as_secs_f64(),
        compacted.rows_before,
        compacted.rows_after,
        compacted.layout,
        compacted.partition_column
    );
    assert_eq!(compacted.partition_column, "year");
    let (after_segments, key) = manifest_ranges(&index_path);
    assert_eq!(key.as_deref(), Some("year"), "partition key recorded");
    let mut last_hi: Option<i64> = None;
    let mut keyed = 0usize;
    let mut unkeyed = 0usize;
    for (id, rows, range) in &after_segments {
        assert!(
            *rows <= u64::from(p.bound),
            "segment {id} over the bound: {rows}"
        );
        match range {
            Some((lo, hi)) => {
                assert!(lo <= hi, "segment {id}: inverted range {lo}..={hi}");
                if let Some(prev) = last_hi {
                    assert!(
                        *lo > prev,
                        "segment {id}: range {lo}..={hi} overlaps {prev}"
                    );
                }
                last_hi = Some(*hi);
                keyed += 1;
            }
            None => unkeyed += 1,
        }
    }
    eprintln!(
        "partitioned layout: {} keyed segments ascending and disjoint, {} unkeyed",
        keyed, unkeyed
    );
    let coord = coordinator(&addr);
    let part_on = measure(&coord, &cases, p.queries).await;
    let before: Vec<(Measured, Vec<Vec<String>>)> = bucket_on.into_iter().zip(bucket_ids).collect();
    let ties = assert_stable(None, &before, &addr, &part_on).await;
    // The boolean cases once more with the survivors sent in max_k
    // pieces, the batch before `signal_batch` existed.
    coord
        .knobs()
        .set("signal_batch", &coord.max_k().to_string())
        .unwrap();
    let part_old_batch = measure(&coord, &cases, p.queries).await;
    assert_bitwise("partitioned layout, max_k batch", &part_on, &part_old_batch);
    handle.abort();
    let _ = handle.await;

    // Partitioned layout, pruning off.
    let (addr, handle) = start_opened_node(config(index_path.clone(), p.bound, false)).await;
    let coord = coordinator(&addr);
    let part_off = measure(&coord, &cases, p.queries).await;
    assert_bitwise("partitioned layout", &part_on, &part_off);
    handle.abort();
    let _ = handle.await;

    let bucket_on: Vec<Measured> = before.into_iter().map(|(m, _)| m).collect();
    let coord_batch = pipestream_search::coordinator::DEFAULT_SIGNAL_BATCH;
    let coord_max_k = pipestream_search::coordinator::DEFAULT_MAX_K;
    let mut report = String::new();
    report.push_str("# Partitioned layout and segment pruning: one local shard\n\n");
    report.push_str(&format!(
        "Machine: this workstation (32 cores, 121 GB), one segment-layout node and an \
         in-process coordinator on loopback, native analysis. Rows: {}. Vector dimension: {} \
         at 4 bits. Seal bound: {} rows. Queries per case: {} (p50 and p90 of the coordinator \
         wall, after two warm-ups). Command: `{}`.\n\n",
        p.rows, p.dim, p.bound, p.queries, cmdline
    ));
    report.push_str(&format!(
        "Ingest: {:.1} s. Compaction to the partitioned layout: {:.1} s ({} sealed segments \
         before, {} after: {} keyed and ascending by `year`, {} unkeyed).\n\n",
        ingest_wall.as_secs_f64(),
        compact_wall.as_secs_f64(),
        before_segments.len(),
        after_segments.len(),
        keyed,
        unkeyed
    ));
    report.push_str(&table(
        "Bucket layout (rows in arrival order)",
        &bucket_on,
        &bucket_off,
    ));
    report.push('\n');
    report.push_str(&table(
        "Partitioned layout (rows ordered by `year`)",
        &part_on,
        &part_off,
    ));
    report.push('\n');
    let boolean_new: Vec<Measured> = part_on
        .iter()
        .filter(|m| m.label.starts_with("boolean"))
        .cloned()
        .collect();
    let boolean_old: Vec<Measured> = part_old_batch
        .into_iter()
        .filter(|m| m.label.starts_with("boolean"))
        .collect();
    report.push_str(&table(
        &format!(
            "Partitioned layout, boolean cases: signal_batch = {} (on) against \
             the max_k batch of {} (off)",
            coord_batch, coord_max_k
        ),
        &boolean_new,
        &boolean_old,
    ));
    report.push('\n');
    report.push_str(&format!(
        "Equality: with pruning on and off, the hits, score bits, order, and counts are \
         identical on both layouts. Across the compaction, which renumbers positional ids, \
         the sorted score bits and the counts are identical on every query, and the hit \
         identities are the same set above the tie boundary; {} of {} query answers had a \
         tie at the k-th score whose members the id order picks differently.\n\n",
        ties,
        part_on.iter().map(|m| m.answers.len()).sum::<usize>()
    ));
    report.push_str(
        "What the numbers show: on the bucket layout every segment holds every year, so a \
         `year` predicate rules no segment out and pruning changes no time. On the \
         partitioned layout the same predicate rules out the segments whose range sits \
         below it, and the filtered dense, lexical, browse, and aggregation cases skip \
         them without opening them; the unfiltered dense scan reads what it always \
         read, since the vector kernel still visits every row of a segment it opens. The \
         keyword-gated boolean cases score their survivors per candidate on both clauses \
         (a cursor walk for the term, one masked scan for the vector), so they do not \
         skip segments either; the third table is the survivors sent in max_k pieces \
         against one call per shard, on the linear candidate scorer.\n",
    );
    println!("{report}");
    if let Some(parent) = p.out.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(&p.out, &report).unwrap();
    eprintln!("wrote {}", p.out.display());
    let _ = std::fs::remove_dir_all(&p.dir);
}
