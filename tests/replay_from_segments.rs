//! A re-placement split that takes each document's analyzed fields from
//! the source's sealed segments (`docs/replay-from-segments.md`) serves
//! the same answers as the split that re-analyzes through the analyzer,
//! bit for bit; a spill cut by the year column serves the same answers
//! as the hash cut and comes out partitioned; and the refusals are by
//! name.

mod common;

use std::path::{Path, PathBuf};

use common::{fit_calibration, start_empty_node, unit_vectors};
use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::{Layout, NodeConfig};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    selection_query, AddDocumentsRequest, AddVectorsRequest, AggregateOp, AggregateRequest,
    Aggregation, DeleteDocumentsRequest, DenseQuery, DenseScoreMode, DocumentField, FacetValue,
    FilterQuery, FlushRequest, IntegerValue, LexicalQuery, NumericValue, PhraseMatch, QueryRequest,
    QueryResponse, SearchQuery, SelectionQuery, SetCalibrationRequest,
};
use pipestream_search::placement::{
    PinnedLeaf, Placement, PlacementNodeConfig, PlacementTreeConfig,
};
use pipestream_search::reshard;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

const DIM: usize = 16;
const BIT_WIDTH: usize = 4;
const N: usize = 360;
/// Rows per sealed segment of the source: several segments per bucket.
const SEAL: usize = 48;
const BLOCK: usize = 24;

fn tempdir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("transplant-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn node(name: &str, cel: Option<&str>) -> PlacementNodeConfig {
    PlacementNodeConfig {
        name: name.to_string(),
        cel: cel.map(str::to_string),
        shards: 1,
        nodes: Vec::new(),
        children: Vec::new(),
    }
}

/// The source's tree: one default leaf.
fn old_tree() -> PlacementTreeConfig {
    PlacementTreeConfig {
        column: "placement".into(),
        level_bits: 4,
        nodes: vec![node("archive", None)],
    }
}

/// The bands: recent (2015+), mid (2000-2014), old (the default).
fn band_tree() -> PlacementTreeConfig {
    PlacementTreeConfig {
        column: "placement".into(),
        level_bits: 4,
        nodes: vec![
            node("recent", Some("year >= 2015")),
            node("mid", Some("year >= 2000")),
            node("old", None),
        ],
    }
}

fn year(i: usize) -> i64 {
    1985 + (i % 40) as i64
}

fn court(i: usize) -> &'static str {
    ["scotus", "ca9", "ca2"][i % 3]
}

fn text(i: usize) -> String {
    let mut t = format!("opinion {i} about search in the court");
    if i % 2 == 1 {
        t.push_str(" zebra crossing");
    }
    if i.is_multiple_of(5) {
        t.push_str(" qualified immunity");
    }
    t
}

fn corpus() -> Vec<f32> {
    unit_vectors(N, DIM, 0x7A11_5EED)
}

fn config(index_path: PathBuf) -> NodeConfig {
    NodeConfig {
        index_path: Some(index_path),
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        layout: Layout::Segments,
        seal_tail_docs: SEAL as u32,
        wal: true,
        wal_buckets: 4,
        bm25_fields: vec!["body".into(), "case_name".into()],
        facet_fields: vec!["court".into()],
        integer_fields: vec!["year".into(), "decided".into(), "placement".into()],
        numeric_fields: vec!["pages".into()],
        position_fields: vec!["body".into()],
        sentence_fields: vec!["body".into()],
        ..Default::default()
    }
}

fn calibration() -> SetCalibrationRequest {
    let vectors = corpus();
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &vectors[..64 * DIM]);
    SetCalibrationRequest {
        dim: DIM as u32,
        bit_width: BIT_WIDTH as u32,
        shift,
        scale,
    }
}

fn document(i: usize, archive: i64, with_code: bool) -> AddDocumentsRequest {
    let mut integers = vec![
        IntegerValue {
            field: "year".into(),
            value: year(i),
        },
        // Unique per document: the key the year-cut comparison uses,
        // since a cut by year assigns slots (and so ids) in year order.
        IntegerValue {
            field: "decided".into(),
            value: i as i64,
        },
    ];
    if with_code && !i.is_multiple_of(9) {
        integers.push(IntegerValue {
            field: "placement".into(),
            value: archive,
        });
    }
    AddDocumentsRequest {
        text: text(i),
        analysis: Some(body_spec()),
        fields: vec![DocumentField {
            field: "case_name".into(),
            text: format!("Case {} v. State {}", i % 7, i),
            analysis: Some(body_spec()),
        }],
        integers,
        facets: vec![FacetValue {
            field: "court".into(),
            value: court(i).into(),
        }],
        numerics: vec![NumericValue {
            field: "pages".into(),
            value: (i % 97) as f64 + 0.5,
        }],
        position_fields: vec!["body".into()],
        sentence_fields: vec!["body".into()],
        ..Default::default()
    }
}

/// A segment-layout source with the placement column, several sealed
/// segments per WAL bucket, a few deleted rows, flushed; returns the
/// index path.
async fn source_shard(dir: &Path, flush: bool) -> (PathBuf, String) {
    source_shard_at(dir, "archive.tv", 0, flush).await
}

/// A source shard whose slot offset is `slot_offset`: its log ids are
/// global (offset plus local row) while its catalog labels are local.
async fn source_shard_at(
    dir: &Path,
    name: &str,
    slot_offset: u64,
    flush: bool,
) -> (PathBuf, String) {
    let index_path = dir.join(name);
    let (addr, _handle) = start_empty_node(NodeConfig {
        slot_offset,
        ..config(index_path.clone())
    })
    .await;
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    client.set_calibration(calibration()).await.unwrap();
    let archive = Placement::validate(&old_tree())
        .unwrap()
        .leaf_by_name("archive")
        .unwrap()
        .code;
    let vectors = corpus();
    for block in 0..N.div_ceil(BLOCK) {
        let start = block * BLOCK;
        let end = (start + BLOCK).min(N);
        let (tx, rx) = mpsc::channel(BLOCK);
        for i in start..end {
            tx.send(document(i, archive, true)).await.unwrap();
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
    client
        .delete_documents(DeleteDocumentsRequest {
            doc_ids: [3, 77, 200, 359]
                .iter()
                .map(|local| slot_offset + local)
                .collect(),
            expected_wal_generation: None,
        })
        .await
        .unwrap();
    if flush {
        client.flush(FlushRequest {}).await.unwrap();
    }
    (index_path, addr)
}

#[allow(clippy::type_complexity)]
fn analyzer() -> impl FnMut(
    &[(
        &str,
        Option<&pipestream_search::pb::AnalysisSpec>,
        pipestream_search::analyzer::SessionLayers,
    )],
) -> Result<Vec<pipestream_search::postings::AnalyzedDoc>, String> {
    let handle = tokio::runtime::Handle::current();
    move |docs| {
        tokio::task::block_in_place(|| {
            handle.block_on(async {
                pipestream_search::analyzer::analyze_batch_streams(NATIVE_ANALYSIS_BACKEND, docs, 1)
                    .await
                    .map_err(|e| e.to_string())
            })
        })
    }
}

fn split(
    gen: &Path,
    out: &Path,
    source: reshard::TreeRowSource,
    cut: reshard::SpillCut,
) -> Result<reshard::TreeReshardOutput, String> {
    reshard::split_placement_tree_logs(
        std::slice::from_ref(&gen.to_path_buf()),
        &band_tree(),
        out,
        &[0, 1_000, 2_000],
        None,
        reshard::TreeSplitOptions {
            source,
            cut,
            ..Default::default()
        },
        &mut analyzer(),
    )
}

struct Served {
    coordinator: CoordinatorServiceImpl,
    handles: Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
}

async fn serve(out: &reshard::TreeReshardOutput) -> Served {
    let tree = band_tree();
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for (image, child) in out.images.children.iter().zip(&out.children) {
        let (addr, handle) = pipestream_search::harness::start_opened_node(NodeConfig {
            slot_offset: image.slot_offset,
            placement_column: Some("placement".into()),
            placement_leaf: Some(child.code),
            placement_tree: Some(std::sync::Arc::new(
                PinnedLeaf::pin(&tree, "placement", child.code).unwrap(),
            )),
            ..config(image.vector_path.clone())
        })
        .await;
        addrs.push(addr);
        handles.push(handle);
    }
    let codes: Vec<Option<i64>> = out.children.iter().map(|c| Some(c.code)).collect();
    let coordinator = CoordinatorServiceImpl::new(addrs)
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_topology_generation(out.images.generation)
        .with_shard_pruning(true)
        .with_hot_topology_placed(vec![Some((0, u64::MAX)); 3], Some((tree, codes)))
        .unwrap();
    Served {
        coordinator,
        handles,
    }
}

impl Served {
    fn stop(self) {
        for handle in self.handles {
            handle.abort();
        }
    }
}

fn search(id: &str, query: pipestream_search::pb::search_query::Query) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(query),
        })),
    }
}

fn lexical(text: &str, phrase: Option<u32>) -> SelectionQuery {
    search(
        "l",
        pipestream_search::pb::search_query::Query::Lexical(LexicalQuery {
            text: text.to_string(),
            analysis: Some(body_spec()),
            phrase: phrase.map(|slop| PhraseMatch { slop }),
            ..Default::default()
        }),
    )
}

fn dense(q: usize, mode: DenseScoreMode) -> SelectionQuery {
    search(
        "v",
        pipestream_search::pb::search_query::Query::Dense(DenseQuery {
            vector: corpus()[q * DIM..(q + 1) * DIM].to_vec(),
            score_mode: mode as i32,
            ..Default::default()
        }),
    )
}

fn filtered(cel: &str, inner: SelectionQuery) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Composite(
            pipestream_search::pb::CompositeSearchStrategy {
                operator: pipestream_search::pb::SelectionOperator::And as i32,
                clauses: vec![
                    SelectionQuery {
                        node: Some(selection_query::Node::Filter(FilterQuery {
                            id: "f".into(),
                            predicate: Some(pipestream_search::pb::filter_query::Predicate::Cel(
                                cel.to_string(),
                            )),
                        })),
                    },
                    inner,
                ],
                scoring: None,
            },
        )),
    }
}

fn aggregate() -> AggregateRequest {
    AggregateRequest {
        aggregations: vec![
            Aggregation {
                name: "pages".into(),
                expression: "pages".into(),
                op: AggregateOp::Sum as i32,
                max_distinct: 0,
            },
            Aggregation {
                name: "n".into(),
                expression: "pages".into(),
                op: AggregateOp::Count as i32,
                max_distinct: 0,
            },
        ],
        group_by: "court".into(),
        max_groups: 16,
        ..Default::default()
    }
}

/// The battery every served split answers; the answers must agree bit
/// for bit across the splits.
fn battery() -> Vec<(&'static str, QueryRequest)> {
    let request = |selection: SelectionQuery| QueryRequest {
        request_id: "transplant".into(),
        // Every match: a top-k cut among equal scores would fall on ids.
        k: N as u32,
        selection: Some(selection),
        explain: true,
        projections: vec![pipestream_search::pb::NamedProjection {
            name: "decided".into(),
            expression: "decided".into(),
        }],
        ..Default::default()
    };
    let mut out = vec![
        ("lexical", request(lexical("zebra crossing", None))),
        ("phrase", {
            // Projections are not certified under a phrase constraint;
            // the phrase hits compare by score alone.
            let mut phrase = request(lexical("qualified immunity", Some(0)));
            phrase.projections.clear();
            phrase
        }),
        ("dense", request(dense(7, DenseScoreMode::Native))),
        ("dense fp32", request(dense(7, DenseScoreMode::Fp32Rerank))),
        (
            "filtered lexical",
            request(filtered(
                "year >= 2000 && year < 2015",
                lexical("search", None),
            )),
        ),
        (
            "filtered dense",
            request(filtered(
                "court == \"ca9\"",
                dense(11, DenseScoreMode::Native),
            )),
        ),
    ];
    let mut with_aggregate = request(lexical("court", None));
    with_aggregate.aggregate = Some(aggregate());
    out.push(("aggregate", with_aggregate));
    let faceted = request(filtered("year < 2000", lexical("opinion", None)));
    out.push(("facet slice", faceted));
    out
}

/// A hit without its position in the slot space: the document's key,
/// its score and signals, and its explanation.
fn positionless(
    hit: &pipestream_search::pb::QueryHit,
) -> (i64, u32, pipestream_search::pb::QueryHit) {
    // Without a projection (the phrase request) the key is 0 and equal
    // scores compare by their stripped hits alone.
    let key = match hit.projected.first().and_then(|v| v.value.as_ref()) {
        Some(pipestream_search::pb::projected_value::Value::IntValue(key)) => *key,
        None => 0,
        other => panic!("the battery projects the integer key, got {other:?}"),
    };
    let mut stripped = hit.clone();
    stripped.doc_id = 0;
    stripped.rank = 0;
    (key, hit.score.to_bits(), stripped)
}

/// The responses equal up to the slot order: the same documents (by
/// key) with the same scores, signals and explanations, and the same
/// facets and folds; ids and ranks differ when a child's segments are
/// cut another way, since ids are positional.
fn same_documents(a: &[(&str, QueryResponse)], b: &[(&str, QueryResponse)], what: &str) {
    for ((name, x), (_, y)) in a.iter().zip(b) {
        assert!(!x.hits.is_empty(), "{what}: {name} answered nothing");
        let mut hx: Vec<_> = x.hits.iter().map(positionless).collect();
        let mut hy: Vec<_> = y.hits.iter().map(positionless).collect();
        hx.sort_by(|p, q| q.1.cmp(&p.1).then(p.0.cmp(&q.0)));
        hy.sort_by(|p, q| q.1.cmp(&p.1).then(p.0.cmp(&q.0)));
        assert_eq!(hx.len(), hy.len(), "{what}: {name} hit counts differ");
        for (p, q) in hx.iter().zip(&hy) {
            assert_eq!(
                p.0,
                q.0,
                "{what}: {name}: a different document at score {}",
                f32::from_bits(p.1)
            );
            assert_eq!(p, q, "{what}: {name}: document {} differs", p.0);
        }
        let mut rx = x.clone();
        let mut ry = y.clone();
        rx.hits.clear();
        ry.hits.clear();
        rx.next_cursor.clear();
        ry.next_cursor.clear();
        assert_eq!(rx, ry, "{what}: {name} differs outside the hits");
    }
}

async fn answers(c: &CoordinatorServiceImpl) -> Vec<(&'static str, QueryResponse)> {
    let mut out = Vec::new();
    for (name, request) in battery() {
        let response = SearchService::query(c, Request::new(request))
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"))
            .into_inner();
        out.push((name, response));
    }
    out
}

fn flatten(e: &pipestream_search::pb::Explanation, depth: usize, out: &mut Vec<String>) {
    out.push(format!(
        "{}{} = {}",
        " ".repeat(depth),
        e.description,
        e.value
    ));
    for d in &e.details {
        flatten(d, depth + 1, out);
    }
}

/// The responses equal, the cursor aside: a cursor binds the read
/// versions of the fleet that minted it (`docs/query-read-versions.md`),
/// which two fleets serving the same rows never share.
fn same_answers(a: &[(&str, QueryResponse)], b: &[(&str, QueryResponse)], what: &str) {
    for ((name, x), (_, y)) in a.iter().zip(b) {
        assert!(!x.hits.is_empty(), "{what}: {name} answered nothing");
        let mut x = x.clone();
        let mut y = y.clone();
        x.next_cursor.clear();
        y.next_cursor.clear();
        let (x, y) = (&x, &y);
        if x != y {
            for (hx, hy) in x.hits.iter().zip(&y.hits) {
                if hx != hy {
                    let mut fx = Vec::new();
                    let mut fy = Vec::new();
                    if let Some(e) = &hx.explain {
                        flatten(e, 0, &mut fx);
                    }
                    if let Some(e) = &hy.explain {
                        flatten(e, 0, &mut fy);
                    }
                    for (lx, ly) in fx.iter().zip(&fy) {
                        if lx != ly {
                            panic!("{what}: {name} doc {} differs:\n  {lx}\n  {ly}", hx.doc_id);
                        }
                    }
                    let mut sx = hx.clone();
                    let mut sy = hy.clone();
                    sx.explain = None;
                    sy.explain = None;
                    panic!(
                        "{what}: {name} doc {} differs outside explain:\n  {sx:?}\n  {sy:?}",
                        hx.doc_id
                    );
                }
            }
            let mut rx = x.clone();
            let mut ry = y.clone();
            rx.hits.clear();
            ry.hits.clear();
            assert_eq!(rx, ry, "{what}: {name} differs outside the hits");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transplanted_split_serves_the_re_analyzed_splits_answers_bit_for_bit() {
    let dir = tempdir("equal");
    let (index_path, _addr) = source_shard(&dir, true).await;
    let gen = reshard::resolve_gen(&pipestream_search::wal::wal_dir(&index_path)).unwrap();

    let by_logs = split(
        &gen,
        &dir.join("logs"),
        reshard::TreeRowSource::Logs,
        reshard::SpillCut::Hash,
    )
    .unwrap();
    let by_segments = split(
        &gen,
        &dir.join("segments"),
        reshard::TreeRowSource::Segments,
        reshard::SpillCut::Hash,
    )
    .unwrap();
    let by_year = split(
        &gen,
        &dir.join("years"),
        reshard::TreeRowSource::Segments,
        reshard::SpillCut::Column {
            column: "year".into(),
            rows_per_cut: 30,
        },
    )
    .unwrap();

    // The same rows went to the same children, and the transplant
    // analyzed nothing.
    assert_eq!(by_segments.placed, by_logs.placed);
    assert_eq!(by_year.placed, by_logs.placed);
    assert_eq!(by_segments.moved, by_logs.moved);
    assert_eq!(by_logs.transplanted_rows, 0);
    assert_eq!(by_segments.transplanted_rows, (N - 4) as u64);
    assert_eq!(by_year.transplanted_rows, (N - 4) as u64);
    assert_eq!(by_logs.peak_transpose_bytes, 0);
    assert!(by_segments.peak_transpose_bytes > 0);
    // The bound: one segment's fields, not the corpus.
    let source = pipestream_search::segments::OpenedSegmentSet::open(
        pipestream_search::node::segments_root(&index_path),
    )
    .unwrap();
    assert!(source.len() > 2, "the source sealed several segments");
    let largest = (0..source.len())
        .map(|i| source.metadata(i).rows)
        .max()
        .unwrap();
    assert!(
        by_segments.peak_transpose_bytes < largest * 2_000,
        "transpose peak {} bytes for a {largest}-row segment",
        by_segments.peak_transpose_bytes
    );
    assert!(
        !dir.join("segments").join("spill").exists(),
        "spill removed"
    );

    // The year cut: each child's segments carry disjoint ascending year
    // ranges as their partition, the catalog names the key, and no
    // segment holds more than the bound.
    assert_eq!(by_year.spill_bucket_count.count_ones(), 1);
    let band_bounds = [(2015, i64::MAX), (2000, 2014), (i64::MIN, 1999)];
    for (index, image) in by_year.images.children.iter().enumerate() {
        let root = pipestream_search::node::segments_root(&image.vector_path);
        let set = pipestream_search::segments::OpenedSegmentSet::open(&root).unwrap();
        assert_eq!(
            set.manifest().partition_key.as_deref(),
            Some("year"),
            "child {index}"
        );
        assert!(set.len() > 1, "child {index} has one cut only");
        let mut last_hi = i64::MIN;
        for i in 0..set.len() {
            let meta = set.metadata(i);
            assert!(
                meta.rows <= 30,
                "child {index} segment {i} holds {} rows",
                meta.rows
            );
            let partition = meta
                .summary
                .as_ref()
                .and_then(|s| s.partition.as_ref())
                .unwrap_or_else(|| panic!("child {index} segment {i} has no partition"));
            assert_eq!(partition.column, "year");
            assert!(
                partition.lo > last_hi || (partition.lo == last_hi && i > 0 && meta.rows <= 30),
                "child {index} segment {i}: {}..{} after {last_hi}",
                partition.lo,
                partition.hi
            );
            assert!(partition.lo <= partition.hi);
            let (lo, hi) = band_bounds[index];
            assert!(
                partition.lo >= lo && partition.hi <= hi,
                "child {index} segment {i}"
            );
            last_hi = partition.hi;
        }
    }
    for image in &by_segments.images.children {
        let root = pipestream_search::node::segments_root(&image.vector_path);
        let set = pipestream_search::segments::OpenedSegmentSet::open(&root).unwrap();
        assert_eq!(set.manifest().partition_key, None);
        for i in 0..set.len() {
            assert!(set
                .metadata(i)
                .summary
                .as_ref()
                .unwrap()
                .partition
                .is_none());
        }
    }

    // Served, the three answer the battery the same, bit for bit.
    let logs = serve(&by_logs).await;
    let segments = serve(&by_segments).await;
    let years = serve(&by_year).await;
    let reference = answers(&logs.coordinator).await;
    same_answers(
        &reference,
        &answers(&segments.coordinator).await,
        "segments vs logs",
    );
    same_documents(
        &reference,
        &answers(&years.coordinator).await,
        "year cut vs logs",
    );
    // Positions and sentences came across: the phrase found rows, the
    // explain names spans.
    let phrase = &reference
        .iter()
        .find(|(name, _)| *name == "phrase")
        .unwrap()
        .1;
    let with_phrase = (0..N)
        .filter(|i| i.is_multiple_of(5) && ![3, 77, 200, 359].contains(i))
        .count();
    assert_eq!(phrase.hits.len(), with_phrase);
    let (_, dense_fp32) = reference
        .iter()
        .find(|(name, _)| *name == "dense fp32")
        .unwrap();
    assert!(!dense_fp32.hits.is_empty());
    logs.stop();
    segments.stop();
    years.stop();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_transplant_refuses_by_name() {
    let dir = tempdir("refuse");
    let (index_path, _addr) = source_shard(&dir, true).await;
    let gen = reshard::resolve_gen(&pipestream_search::wal::wal_dir(&index_path)).unwrap();
    // The column cut's refusals: the placement column, a zero bound, a
    // column that is not an integer column, a single image.
    let err = split(
        &gen,
        &dir.join("placement-cut"),
        reshard::TreeRowSource::Segments,
        reshard::SpillCut::Column {
            column: "placement".into(),
            rows_per_cut: 10,
        },
    )
    .unwrap_err();
    assert!(err.contains("is the placement column"), "{err}");
    let err = split(
        &gen,
        &dir.join("zero"),
        reshard::TreeRowSource::Segments,
        reshard::SpillCut::Column {
            column: "year".into(),
            rows_per_cut: 0,
        },
    )
    .unwrap_err();
    assert!(err.contains("positive row bound"), "{err}");
    let err = split(
        &gen,
        &dir.join("not-a-column"),
        reshard::TreeRowSource::Segments,
        reshard::SpillCut::Column {
            column: "pages".into(),
            rows_per_cut: 10,
        },
    )
    .unwrap_err();
    assert!(err.contains("not an integer column"), "{err}");
    let err = reshard::split_placement_tree_logs(
        std::slice::from_ref(&gen),
        &band_tree(),
        &dir.join("single"),
        &[0, 1_000, 2_000],
        None,
        reshard::TreeSplitOptions {
            layout: reshard::TreeChildLayout::SingleImage {
                max_child_rows: N as u64,
            },
            cut: reshard::SpillCut::Column {
                column: "year".into(),
                rows_per_cut: 10,
            },
            ..Default::default()
        },
        &mut analyzer(),
    )
    .unwrap_err();
    assert!(err.contains("needs the segmented layout"), "{err}");
    // A leaf with several shards needs routing keys the segments do not carry.
    let mut two = band_tree();
    two.nodes[0].shards = 2;
    let err = reshard::split_placement_tree_logs(
        std::slice::from_ref(&gen),
        &two,
        &dir.join("two"),
        &[0, 1_000, 2_000, 3_000],
        None,
        reshard::TreeSplitOptions {
            source: reshard::TreeRowSource::Segments,
            ..Default::default()
        },
        &mut analyzer(),
    )
    .unwrap_err();
    assert!(
        err.contains("sealed segments carry no routing keys"),
        "{err}"
    );

    // A single-image source has no catalog to read.
    let single_dir = dir.join("single-source");
    std::fs::create_dir_all(&single_dir).unwrap();
    let single_path = single_dir.join("flat.tv");
    let (single_addr, _handle) = start_empty_node(NodeConfig {
        layout: Layout::SingleImage,
        seal_tail_docs: 0,
        ..config(single_path.clone())
    })
    .await;
    let mut single = NodeServiceClient::connect(single_addr).await.unwrap();
    single.set_calibration(calibration()).await.unwrap();
    let archive = Placement::validate(&old_tree())
        .unwrap()
        .leaf_by_name("archive")
        .unwrap()
        .code;
    let (tx, rx) = mpsc::channel(8);
    for i in 0..8 {
        tx.send(document(i, archive, true)).await.unwrap();
    }
    drop(tx);
    single.add_documents(ReceiverStream::new(rx)).await.unwrap();
    let (tx, rx) = mpsc::channel(1);
    tx.send(AddVectorsRequest {
        vectors: corpus()[..8 * DIM].to_vec(),
        dim: DIM as u32,
    })
    .await
    .unwrap();
    drop(tx);
    single.add_vectors(ReceiverStream::new(rx)).await.unwrap();
    single.flush(FlushRequest {}).await.unwrap();
    let single_gen = reshard::resolve_gen(&pipestream_search::wal::wal_dir(&single_path)).unwrap();
    let err = split(
        &single_gen,
        &dir.join("from-single"),
        reshard::TreeRowSource::Segments,
        reshard::SpillCut::Hash,
    )
    .unwrap_err();
    assert!(err.contains("no segment catalog"), "{err}");

    // An unsealed tail: a row in the log past the catalog (the node is
    // gone; the record is appended to its log as a crashed ingest would
    // have left it).
    let manifest = pipestream_search::wal::read_manifest(&gen).unwrap();
    let mut writer = pipestream_search::wal::WalWriter::resume(&gen, manifest).unwrap();
    let archive = Placement::validate(&old_tree())
        .unwrap()
        .leaf_by_name("archive")
        .unwrap()
        .code;
    writer
        .append(pipestream_search::pb::wal::wal_record::Op::AddDocuments(
            pipestream_search::pb::wal::LoggedAddDocuments {
                source_references: Vec::new(),
                first_id: N as u64,
                documents: vec![document(N, archive, true)],
                stable_routing_keys: Vec::new(),
            },
        ))
        .unwrap();
    writer.flush().unwrap();
    let err = split(
        &gen,
        &dir.join("tail"),
        reshard::TreeRowSource::Segments,
        reshard::SpillCut::Hash,
    )
    .unwrap_err();
    assert!(
        err.contains("unsealed tail") && err.contains("flush"),
        "{err}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A source at a slot offset: its log ids are global and its catalog
/// labels local, so the tail check, the log's deletes, and the spill's
/// source ids all convert. Two sources, one at offset 0 and one at
/// 10,000, split from the logs and from the segments place the same
/// rows, the transplant counts both sources' live rows, and the
/// deleted rows of the offset source are absent from its children.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_source_at_a_slot_offset_transplants_the_same_rows() {
    let dir = tempdir("offset");
    let (a_path, _a) = source_shard_at(&dir, "a.tv", 0, true).await;
    let (b_path, _b) = source_shard_at(&dir, "b.tv", 10_000, true).await;
    let gens = vec![
        reshard::resolve_gen(&pipestream_search::wal::wal_dir(&a_path)).unwrap(),
        reshard::resolve_gen(&pipestream_search::wal::wal_dir(&b_path)).unwrap(),
    ];
    let run = |out: &str, source: reshard::TreeRowSource| {
        reshard::split_placement_tree_logs(
            &gens,
            &band_tree(),
            &dir.join(out),
            &[0, 1_000, 2_000],
            None,
            reshard::TreeSplitOptions {
                source,
                cut: reshard::SpillCut::Hash,
                ..Default::default()
            },
            &mut analyzer(),
        )
    };
    let by_logs = run("logs", reshard::TreeRowSource::Logs).unwrap();
    let by_segments = run("segments", reshard::TreeRowSource::Segments).unwrap();
    assert_eq!(by_segments.placed, by_logs.placed);
    assert_eq!(by_segments.moved, by_logs.moved);
    assert_eq!(by_segments.transplanted_rows, 2 * (N - 4) as u64);
    assert_eq!(
        by_logs.placed.iter().sum::<u64>(),
        2 * (N - 4) as u64,
        "both sources' live rows were placed"
    );
    // The same answers from the children, whichever source they came from.
    let logs = serve(&by_logs).await;
    let segments = serve(&by_segments).await;
    same_answers(
        &answers(&logs.coordinator).await,
        &answers(&segments.coordinator).await,
        "sources at slot offsets",
    );
    logs.stop();
    segments.stop();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Serve one child of a split alone, pinned to its leaf, under a root
/// over that one shard.
async fn serve_child(out: &reshard::TreeReshardOutput, index: usize) -> Served {
    let tree = band_tree();
    let image = &out.images.children[index];
    let child = &out.children[index];
    let (addr, handle) = pipestream_search::harness::start_opened_node(NodeConfig {
        slot_offset: image.slot_offset,
        placement_column: Some("placement".into()),
        placement_leaf: Some(child.code),
        placement_tree: Some(std::sync::Arc::new(
            PinnedLeaf::pin(&tree, "placement", child.code).unwrap(),
        )),
        ..config(image.vector_path.clone())
    })
    .await;
    // A placed root wants a shard for every leaf of the tree; one child
    // served alone sits under a plain root, and the node itself keeps
    // the leaf's pin.
    let coordinator = CoordinatorServiceImpl::new(vec![addr]).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    );
    Served {
        coordinator,
        handles: vec![handle],
    }
}

/// One child rebuilt alone: the routing pass covers every child and the
/// counts are the full split's, but only the named child's catalog is
/// written, and served alone it answers as the full split's child does.
/// A single-image split and an index past the tree are refused by name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_child_is_built_alone_and_equals_the_full_splits() {
    let dir = tempdir("onlychild");
    let (index_path, _addr) = source_shard(&dir, true).await;
    let gen = reshard::resolve_gen(&pipestream_search::wal::wal_dir(&index_path)).unwrap();
    let cut = || reshard::SpillCut::Column {
        column: "year".into(),
        rows_per_cut: 30,
    };
    let full = split(
        &gen,
        &dir.join("full"),
        reshard::TreeRowSource::Segments,
        cut(),
    )
    .unwrap();
    let one = reshard::split_placement_tree_logs(
        std::slice::from_ref(&gen),
        &band_tree(),
        &dir.join("one"),
        &[0, 1_000, 2_000],
        None,
        reshard::TreeSplitOptions {
            source: reshard::TreeRowSource::Segments,
            cut: cut(),
            only_child: Some(1),
            ..Default::default()
        },
        &mut analyzer(),
    )
    .unwrap();
    assert_eq!(
        one.placed, full.placed,
        "the routing pass covers every child"
    );
    assert_eq!(one.segments[1], full.segments[1]);
    for (index, image) in one.images.children.iter().enumerate() {
        let root = pipestream_search::node::segments_root(&image.vector_path);
        assert_eq!(root.exists(), index == 1, "child {index}'s catalog");
    }
    let full_child = serve_child(&full, 1).await;
    let one_child = serve_child(&one, 1).await;
    // The battery is written for the fleet; on one leaf some of its
    // queries have no rows to answer with (a facet value of another
    // leaf), and those must be empty on both sides. The rest compare as
    // the fleet's do.
    let full_answers = answers(&full_child.coordinator).await;
    let one_answers = answers(&one_child.coordinator).await;
    let mut compared = Vec::new();
    let mut compared_one = Vec::new();
    for ((name, x), (_, y)) in full_answers.iter().zip(&one_answers) {
        if x.hits.is_empty() || y.hits.is_empty() {
            assert!(
                x.hits.is_empty() && y.hits.is_empty(),
                "the child built alone: {name} answers on one side only"
            );
            continue;
        }
        compared.push((*name, x.clone()));
        compared_one.push((*name, y.clone()));
    }
    assert!(
        compared.len() >= 4,
        "the leaf answers enough of the battery to compare ({} shapes)",
        compared.len()
    );
    same_answers(&compared, &compared_one, "the child built alone");
    full_child.stop();
    one_child.stop();

    let past = reshard::split_placement_tree_logs(
        std::slice::from_ref(&gen),
        &band_tree(),
        &dir.join("past"),
        &[0, 1_000, 2_000],
        None,
        reshard::TreeSplitOptions {
            source: reshard::TreeRowSource::Segments,
            cut: cut(),
            only_child: Some(9),
            ..Default::default()
        },
        &mut analyzer(),
    )
    .unwrap_err();
    assert!(past.contains("names no child"), "{past}");
    let single = reshard::split_placement_tree_logs(
        std::slice::from_ref(&gen),
        &band_tree(),
        &dir.join("single"),
        &[0, 1_000, 2_000],
        None,
        reshard::TreeSplitOptions {
            layout: reshard::TreeChildLayout::SingleImage {
                max_child_rows: 1_000,
            },
            only_child: Some(1),
            ..Default::default()
        },
        &mut analyzer(),
    )
    .unwrap_err();
    assert!(single.contains("single-image"), "{single}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// A spill plan past the open-files limit is refused before the pass,
/// naming the files it would hold and the limit; the process's own
/// limit is readable and the raise leaves it at or above where it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_spill_plan_past_the_open_files_limit_is_refused_by_name() {
    let dir = tempdir("fdlimit");
    let (index_path, _addr) = source_shard(&dir, true).await;
    let gen = reshard::resolve_gen(&pipestream_search::wal::wal_dir(&index_path)).unwrap();
    let error = reshard::split_placement_tree_logs(
        std::slice::from_ref(&gen),
        &band_tree(),
        &dir.join("small"),
        &[0, 1_000, 2_000],
        None,
        reshard::TreeSplitOptions {
            source: reshard::TreeRowSource::Segments,
            cut: reshard::SpillCut::Column {
                column: "year".into(),
                rows_per_cut: 30,
            },
            open_files_limit: Some(16),
            ..Default::default()
        },
        &mut analyzer(),
    )
    .unwrap_err();
    assert!(error.contains("open-files limit of 16"), "{error}");
    assert!(
        error.contains("spill logs and analysis sidecars"),
        "{error}"
    );
    let before = reshard::open_files_soft_limit().expect("Linux exposes the limit");
    let raised = reshard::raise_open_files_limit().expect("the net build can raise it");
    assert!(raised >= before, "{raised} >= {before}");
    assert_eq!(reshard::open_files_soft_limit(), Some(raised));
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sources_with_different_analyzers_are_refused() {
    let dir = tempdir("mixed");
    let (a_path, _a) = source_shard(&dir.join("a"), true).await;
    // A second source analyzed under another spec: the same corpus with
    // a different stemmer, so the field fingerprints differ.
    let b_dir = dir.join("b");
    std::fs::create_dir_all(&b_dir).unwrap();
    let b_path = b_dir.join("archive.tv");
    let (b_addr, _b) = start_empty_node(config(b_path.clone())).await;
    let mut client = NodeServiceClient::connect(b_addr).await.unwrap();
    client.set_calibration(calibration()).await.unwrap();
    let archive = Placement::validate(&old_tree())
        .unwrap()
        .leaf_by_name("archive")
        .unwrap()
        .code;
    let mut other = body_spec();
    other
        .char_filters
        .retain(|&filter| filter != pipestream_search::analyzer::CHAR_FILTER_ACCENT_FOLD);
    let (tx, rx) = mpsc::channel(8);
    for i in 0..8 {
        let mut doc = document(i, archive, true);
        doc.analysis = Some(other.clone());
        doc.fields[0].analysis = Some(other.clone());
        tx.send(doc).await.unwrap();
    }
    drop(tx);
    client.add_documents(ReceiverStream::new(rx)).await.unwrap();
    let (tx, rx) = mpsc::channel(1);
    tx.send(AddVectorsRequest {
        vectors: corpus()[..8 * DIM].to_vec(),
        dim: DIM as u32,
    })
    .await
    .unwrap();
    drop(tx);
    client.add_vectors(ReceiverStream::new(rx)).await.unwrap();
    client.flush(FlushRequest {}).await.unwrap();
    let gens = vec![
        reshard::resolve_gen(&pipestream_search::wal::wal_dir(&a_path)).unwrap(),
        reshard::resolve_gen(&pipestream_search::wal::wal_dir(&b_path)).unwrap(),
    ];
    let err = reshard::split_placement_tree_logs(
        &gens,
        &band_tree(),
        &dir.join("mixed-out"),
        &[0, 1_000, 2_000],
        None,
        reshard::TreeSplitOptions {
            source: reshard::TreeRowSource::Segments,
            ..Default::default()
        },
        &mut analyzer(),
    )
    .unwrap_err();
    assert!(
        err.contains("analysis fingerprints") && err.contains("differ"),
        "{err}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
