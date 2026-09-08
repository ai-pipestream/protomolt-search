//! The partitioned compaction of a catalog WITHOUT a WAL
//! (docs/replay-from-segments.md, "Partitioned compaction of a catalog
//! without a log"): the children of a re-placement split have no log, so
//! `CompactShard` on such a catalog transplants the sealed segments
//! through `FieldTranspose` — never the analyzer — keyed by the
//! partition column, catches the tail up by sealing it, and cuts over
//! under the same shadow/marker/closing-flush contract as the log
//! replay (docs/mutations.md).
//!
//! Pinned here:
//!
//! - Concurrent writes during the build are caught up by the tail seal
//!   and appear exactly once after the cutover; tombstones from before
//!   the cutoff never reappear.
//! - The outputs keep the shard's declared column tables, each row's
//!   stored original protobuf source and public identity, and the
//!   derived-column declaration (the kind-15 entry) on every segment.
//! - Serial and bounded-parallel builds are byte-identical.
//! - A leftover of an interrupted build is refused by name and never
//!   adopted; the cutover's crash windows roll back at open.
//! - The refusals: a catalog that does not qualify (no sealed segments,
//!   no generation binding, the single-image layout) keeps the old
//!   by-name refusal, and the build knobs are the from-segments path's
//!   alone.

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pipestream_search::analyzer::body_spec;
use pipestream_search::live_docs::LiveDocs;
use pipestream_search::mapping::derive_plan;
use pipestream_search::node::{segments_root, Layout, NodeConfig};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    AddDocumentsRequest, AddVectorsRequest, Bm25SearchRequest, BrowseShardRequest, BrowseSort,
    CompactShardRequest, CompactShardResponse, DeleteDocumentsRequest, DocLineage, FacetValue,
    FlushRequest, GetDocumentsRequest, HealthRequest, IntegerValue, NumericValue,
    ResolveParentsRequest, SearchRequest, SetCalibrationRequest,
};
use pipestream_search::postings::{Bm25Index, Bm25Reader, StoredBinding};
use pipestream_search::reshard::{self, PartitionSpec, SegmentCompactionOptions};
use pipestream_search::segments::{SegmentCatalog, SegmentSetManifest};
use prost::Message;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::Request;

use common::mock::start_mock_analysis;
use common::{fit_calibration, start_empty_node, start_opened_node, unit_vectors, BIT_WIDTH, DIM};

const SEED: u64 = 0x5E61_7A11;
/// Rows per sealed segment of the fixture: several segments per run.
const SEAL: usize = 48;
/// Rows per partition of the compaction under test.
const BOUND: u32 = 96;
const GROUPS: [&str; 3] = ["red", "green", "blue"];
const EXTRAS: [&str; 4] = ["one", "two", "three", "four"];

fn tempdir(tag: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("walless_compaction_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The generation binding a qualifying catalog carries: published onto
/// the empty catalog before the node first opens it, exactly where the
/// re-placement split's children get theirs (the segments then carry it
/// from the bound tail).
fn binding() -> StoredBinding {
    let plan = derive_plan(
        include_bytes!("fixtures/vector-binding/descriptor.bin"),
        "vector_binding.Named",
    )
    .unwrap();
    StoredBinding {
        plan_fingerprint: plan.fingerprint,
        body_path: "body".into(),
        vector_binding: plan.vector_binding.unwrap().encode_to_vec(),
        ..Default::default()
    }
}

/// Create the catalog root with the generation binding published, so the
/// node serving it is the WAL-less child shape the split produces.
fn bind_catalog(index_path: &Path) {
    let catalog = SegmentCatalog::open(segments_root(index_path)).unwrap();
    catalog.publish_binding(&binding()).unwrap();
}

fn config(index_path: PathBuf, analysis: &str) -> NodeConfig {
    NodeConfig {
        index_path: Some(index_path),
        analysis_addr: Some(analysis.to_string()),
        wal: false,
        layout: Layout::Segments,
        seal_tail_docs: SEAL as u32,
        facet_fields: vec!["grp".into()],
        // `other` is declared and never carried: the outputs must still
        // declare it.
        integer_fields: vec!["num".into(), "other".into()],
        numeric_fields: vec!["score".into()],
        position_fields: vec!["body".into()],
        sentence_fields: vec!["body".into()],
        ..Default::default()
    }
}

/// One fixture row: unique text (so hits map across renumbering), the
/// partition column `num` (runs of three share a key), a facet, a
/// numeric, lineage, and the stored original source plus public
/// identity.
#[derive(Clone, Debug)]
struct Row {
    text: String,
    num: i64,
    grp: &'static str,
    score: f64,
    vector: Vec<f32>,
}

fn row(i: usize) -> Row {
    Row {
        text: format!("row{i} common {} {}", GROUPS[i % 3], EXTRAS[i % 4]),
        num: (i / 3) as i64,
        grp: GROUPS[i % 3],
        score: (i % 97) as f64 + 0.5,
        vector: unit_vectors(1, DIM, SEED.wrapping_add(i as u64)),
    }
}

fn number_of(text: &str) -> usize {
    text.split_whitespace()
        .next()
        .unwrap()
        .strip_prefix("row")
        .unwrap()
        .parse()
        .unwrap()
}

fn identity_of(i: usize) -> pipestream_search::pb::DocumentIdentity {
    pipestream_search::pb::DocumentIdentity {
        document_key: format!("parent/{}", i / 4).into_bytes(),
        version: (i % 2) as u64 + 1,
        chunk_ordinal: Some(((i / 2) % 2) as u32),
    }
}

fn doc_of(i: usize) -> AddDocumentsRequest {
    let row = row(i);
    AddDocumentsRequest {
        text: row.text.clone(),
        analysis: Some(body_spec()),
        original_source: Some(common::protobuf_source(
            &format!("parent/{}", i / 4),
            &format!("version/{}", i % 2),
        )),
        source_chunk_ordinal: Some(((i / 2) % 2) as u32),
        identity: Some(identity_of(i)),
        lineage: Some(DocLineage {
            parent_id: i as u64,
            ..Default::default()
        }),
        facets: vec![FacetValue {
            field: "grp".into(),
            value: row.grp.into(),
        }],
        integers: vec![IntegerValue {
            field: "num".into(),
            value: row.num,
        }],
        numerics: vec![NumericValue {
            field: "score".into(),
            value: row.score,
        }],
        position_fields: vec!["body".into()],
        sentence_fields: vec!["body".into()],
        ..Default::default()
    }
}

async fn client(addr: &str) -> NodeServiceClient<Channel> {
    NodeServiceClient::connect(addr.to_string()).await.unwrap()
}

async fn calibrate(addr: &str) {
    let sample = unit_vectors(64, DIM, SEED);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &sample);
    client(addr)
        .await
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: BIT_WIDTH as u32,
            shift,
            scale,
        })
        .await
        .unwrap();
}

/// Stream `rows` in blocks (documents, then vectors, as the legacy
/// two-call ingest does), then flush.
async fn seed(addr: &str, rows: usize) {
    calibrate(addr).await;
    let mut client = client(addr).await;
    for block in 0..rows.div_ceil(SEAL) {
        let start = block * SEAL;
        let end = (start + SEAL).min(rows);
        let (tx, rx) = mpsc::channel(SEAL);
        for i in start..end {
            tx.send(doc_of(i)).await.unwrap();
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
        let mut vectors = Vec::with_capacity((end - start) * DIM);
        for i in start..end {
            vectors.extend_from_slice(&row(i).vector);
        }
        let (tx, rx) = mpsc::channel(2);
        tx.send(AddVectorsRequest {
            vectors,
            dim: DIM as u32,
        })
        .await
        .unwrap();
        drop(tx);
        client.add_vectors(ReceiverStream::new(rx)).await.unwrap();
    }
    client.flush(FlushRequest {}).await.unwrap();
}

/// A bound, WAL-less, segmented catalog of `rows` rows, served.
async fn serve_fixture(
    dir: &Path,
    name: &str,
    analysis: &str,
    rows: usize,
) -> (
    PathBuf,
    String,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let index_path = dir.join(name);
    bind_catalog(&index_path);
    let (addr, handle) = start_opened_node(config(index_path.clone(), analysis)).await;
    seed(&addr, rows).await;
    (index_path, addr, handle)
}

async fn compact(
    addr: &str,
    request: CompactShardRequest,
) -> Result<CompactShardResponse, tonic::Status> {
    let mut request = Request::new(request);
    request.set_timeout(Duration::from_secs(600));
    client(addr)
        .await
        .compact_shard(request)
        .await
        .map(|r| r.into_inner())
}

fn partition_request() -> CompactShardRequest {
    CompactShardRequest {
        partition_column: "num".into(),
        tail_bound: BOUND,
        ..Default::default()
    }
}

fn read_manifest(index_path: &Path) -> SegmentSetManifest {
    SegmentCatalog::read_manifest(&segments_root(index_path))
        .unwrap()
        .unwrap()
}

/// Every file of the catalog (the manifest and each segment) plus the
/// shard-wide overlay, relative path to bytes: the byte-identity
/// comparison of two runs.
fn catalog_bytes(index_path: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut out = BTreeMap::new();
    let root = segments_root(index_path);
    let walk = |dir: &Path, prefix: &str, out: &mut BTreeMap<String, Vec<u8>>| {
        let mut stack = vec![(dir.to_path_buf(), prefix.to_string())];
        while let Some((dir, prefix)) = stack.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let entry = entry.unwrap();
                let name = format!("{prefix}/{}", entry.file_name().to_string_lossy());
                if entry.file_type().unwrap().is_dir() {
                    stack.push((entry.path(), name));
                } else {
                    out.insert(name, std::fs::read(entry.path()).unwrap());
                }
            }
        }
    };
    walk(&root, "segments", &mut out);
    let live = pipestream_search::node::live_docs_sidecar_path(index_path);
    if live.exists() {
        out.insert("live".to_string(), std::fs::read(live).unwrap());
    }
    out
}

/// The manifest's compaction outputs (`cmp-*`): each within the bound,
/// the keyed ones ascending and disjoint by `num` with the range equal
/// to the segment's own summary, unkeyed ones last.
fn check_partitions(manifest: &SegmentSetManifest, prefix: &str) {
    assert_eq!(manifest.partition_key.as_deref(), Some("num"));
    let outputs: Vec<_> = manifest
        .segments
        .iter()
        .filter(|s| s.segment_id.starts_with(prefix))
        .collect();
    assert!(!outputs.is_empty(), "no outputs with prefix {prefix}");
    let mut previous_hi: Option<i64> = None;
    for segment in outputs {
        assert!(
            segment.rows <= u64::from(BOUND),
            "segment {} holds {} rows over the bound {BOUND}",
            segment.segment_id,
            segment.rows
        );
        let summary = segment.summary.as_ref().expect("segment summary");
        let range = summary
            .partition
            .as_ref()
            .expect("a keyed compaction output records its range");
        assert_eq!(range.column, "num");
        let column = summary
            .int_columns
            .iter()
            .find(|c| c.name == "num")
            .expect("num summary");
        assert_eq!((range.lo, range.hi), (column.min, column.max));
        assert_eq!(column.present, segment.rows, "every output row is keyed");
        if let Some(hi) = previous_hi {
            assert!(
                hi < range.lo,
                "segment {} range {}..={} overlaps the previous hi {hi}",
                segment.segment_id,
                range.lo,
                range.hi
            );
        }
        previous_hi = Some(range.hi);
    }
}

/// Every live row's text, in label order.
async fn all_texts(addr: &str) -> Vec<String> {
    let mut node = client(addr).await;
    let browse = node
        .browse_shard(BrowseShardRequest {
            k: 5_000,
            first_page: true,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let fetched = node
        .get_documents(GetDocumentsRequest {
            doc_ids: browse.doc_ids,
        })
        .await
        .unwrap()
        .into_inner();
    fetched.documents.into_iter().map(|d| d.text).collect()
}

/// What a reader can observe, compared between two shards by stable
/// text (scores are exact bits).
#[derive(Debug, PartialEq)]
struct Reads {
    lexical: Vec<Vec<(String, u32)>>,
    facets: Vec<Vec<(String, u64)>>,
    dense: Vec<Vec<(String, u32)>>,
    nums: Vec<i64>,
    parents: Vec<(String, u64)>,
}

const PROBES: [&str; 3] = ["revised", "common", "blue three"];

async fn observe(addr: &str, analysis: &str) -> Reads {
    let coordinator =
        pipestream_search::coordinator::CoordinatorServiceImpl::new(vec![addr.to_string()])
            .with_bm25(Some(analysis.to_string()), Default::default());
    let mut node = client(addr).await;
    let mut lexical = Vec::new();
    let mut facets = Vec::new();
    for probe in PROBES {
        let response = SearchService::bm25_search(
            &coordinator,
            Request::new(Bm25SearchRequest {
                text: probe.into(),
                k: 5_000,
                facet_fields: vec!["grp".into()],
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let ids: Vec<u64> = response.hits.iter().map(|h| h.doc_id).collect();
        let scores: Vec<u32> = response.hits.iter().map(|h| h.score.to_bits()).collect();
        let texts = texts_of(&mut node, &ids).await;
        let mut hits: Vec<(String, u32)> = ids
            .iter()
            .zip(scores)
            .map(|(id, bits)| (texts[id].clone(), bits))
            .collect();
        hits.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        lexical.push(hits);
        let mut counts: Vec<(String, u64)> = response
            .facets
            .iter()
            .flat_map(|f| f.counts.iter().map(|c| (c.value.clone(), c.count)))
            .collect();
        counts.sort();
        facets.push(counts);
    }
    let mut dense = Vec::new();
    for q in 0..3 {
        let query = unit_vectors(1, DIM, 0x9E37_0000 + q);
        let response = SearchService::search(
            &coordinator,
            Request::new(SearchRequest {
                vector: query,
                k: 20,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let ids: Vec<u64> = response.hits.iter().map(|h| h.vector_id).collect();
        let scores: Vec<u32> = response.hits.iter().map(|h| h.score.to_bits()).collect();
        let texts = texts_of(&mut node, &ids).await;
        let mut hits: Vec<(String, u32)> = ids
            .iter()
            .zip(scores)
            .map(|(id, bits)| (texts[id].clone(), bits))
            .collect();
        hits.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        dense.push(hits);
    }
    let browse = node
        .browse_shard(BrowseShardRequest {
            k: 5_000,
            first_page: true,
            sort: vec![BrowseSort {
                map: None,
                column: "num".into(),
                descending: false,
            }],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let texts = texts_of(&mut node, &browse.doc_ids).await;
    let nums: Vec<i64> = browse
        .doc_ids
        .iter()
        .map(|id| row(number_of(&texts[id])).num)
        .collect();
    assert!(
        nums.windows(2).all(|pair| pair[0] <= pair[1]),
        "browse by num is ascending"
    );
    let parents = node
        .resolve_parents(ResolveParentsRequest {
            doc_ids: browse.doc_ids.clone(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let mut parents: Vec<(String, u64)> = parents
        .parents
        .iter()
        .map(|p| (texts[&p.doc_id].clone(), p.parent_id))
        .collect();
    parents.sort();
    Reads {
        lexical,
        facets,
        dense,
        nums,
        parents,
    }
}

/// Fetch each id, checking its public identity against the fixture's.
async fn texts_of(node: &mut NodeServiceClient<Channel>, ids: &[u64]) -> BTreeMap<u64, String> {
    if ids.is_empty() {
        return BTreeMap::new();
    }
    node.get_documents(GetDocumentsRequest {
        doc_ids: ids.to_vec(),
    })
    .await
    .unwrap()
    .into_inner()
    .documents
    .into_iter()
    .map(|d| {
        let i = number_of(&d.text);
        assert_eq!(d.identity, Some(identity_of(i)), "identity of {i}");
        let lineage = d.lineage.expect("lineage");
        assert_eq!(lineage.parent_id, i as u64, "lineage of {i}");
        (d.doc_id, d.text)
    })
    .collect()
}

/// The stored original protobuf source and identity of every row of
/// every `cmp-*` segment, checked against the fixture's.
fn check_sources_and_tables(index_path: &Path) {
    let manifest = read_manifest(index_path);
    let root = segments_root(index_path);
    for segment in &manifest.segments {
        if !segment.segment_id.starts_with("cmp-") {
            continue;
        }
        let bm25 = Bm25Reader::open(
            &SegmentCatalog::segment_dir(&root, &segment.segment_id).join(&segment.bm25.file),
        )
        .unwrap();
        // The declared column tables are the shard's, `other` included.
        let integer_names: Vec<&str> = (0..bm25.integer_count())
            .map(|i| bm25.integer_name(i))
            .collect();
        assert_eq!(integer_names, ["num", "other"]);
        assert_eq!(bm25.facet_count(), 1);
        assert_eq!(bm25.numeric_count(), 1);
        for row in 0..bm25.next_doc_id() {
            let text = Bm25Index::text(&bm25, row).expect("stored text");
            let i = number_of(&text);
            let (source, ordinal) = bm25.protobuf_source(row).unwrap().expect("source");
            assert_eq!(ordinal, Some(((i / 2) % 2) as u32));
            let expected = common::protobuf_source(
                &format!("parent/{}", i / 4),
                &format!("version/{}", i % 2),
            );
            assert_eq!(
                source.encode_to_vec(),
                expected.encode_to_vec(),
                "source bytes of row {i}"
            );
            assert_eq!(bm25.document_identity(row), Some(identity_of(i)));
        }
    }
}

/// A fresh shard over exactly `rows`, for the reference reads.
async fn reference_shard(
    dir: &Path,
    name: &str,
    analysis: &str,
    rows: &[usize],
) -> (
    String,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let (addr, handle) = start_empty_node(config(dir.join(name), analysis)).await;
    calibrate(&addr).await;
    let mut client = client(&addr).await;
    for block in rows.chunks(SEAL) {
        let (tx, rx) = mpsc::channel(SEAL);
        for &i in block {
            tx.send(doc_of(i)).await.unwrap();
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
        let mut vectors = Vec::with_capacity(block.len() * DIM);
        for &i in block {
            vectors.extend_from_slice(&row(i).vector);
        }
        let (tx, rx) = mpsc::channel(2);
        tx.send(AddVectorsRequest {
            vectors,
            dim: DIM as u32,
        })
        .await
        .unwrap();
        drop(tx);
        client.add_vectors(ReceiverStream::new(rx)).await.unwrap();
    }
    client.flush(FlushRequest {}).await.unwrap();
    (addr, handle)
}

/// The seeded corpus for the online test.
const N: usize = 3_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn walless_catalog_compacts_into_partitions_while_writes_continue() {
    let (analysis, _mock) = start_mock_analysis().await;
    let dir = tempdir("online");
    let (index_path, addr, handle) = serve_fixture(&dir, "shard.tv", &analysis, N).await;

    // Tombstones before the cutoff: every seventh row is gone and must
    // never reappear.
    let doomed: Vec<usize> = (0..N).step_by(7).collect();
    let deleted = client(&addr)
        .await
        .delete_documents(DeleteDocumentsRequest {
            doc_ids: doomed.iter().map(|i| *i as u64).collect(),
            expected_wal_generation: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(deleted.deleted as usize, doomed.len());

    // The concurrent writer appends through the public RPCs for the
    // whole run; every row it lands must appear exactly once. The cap is
    // what makes the catch-up loop converge on any machine: it exits on
    // "a seal found nothing", which needs the writer to stop.
    let stop = Arc::new(AtomicBool::new(false));
    let appended = Arc::new(Mutex::new(Vec::new()));
    let writer = {
        let addr = addr.clone();
        let stop = Arc::clone(&stop);
        let appended = Arc::clone(&appended);
        tokio::spawn(async move {
            let mut client = client(&addr).await;
            let mut i = N;
            while !stop.load(Ordering::Acquire) && i - N < 400 {
                let doc = doc_of(i);
                let text = doc.text.clone();
                client
                    .add_documents(Request::new(tokio_stream::iter([doc])))
                    .await
                    .unwrap();
                let (tx, rx) = mpsc::channel(1);
                tx.send(AddVectorsRequest {
                    vectors: row(i).vector.clone(),
                    dim: DIM as u32,
                })
                .await
                .unwrap();
                drop(tx);
                client.add_vectors(ReceiverStream::new(rx)).await.unwrap();
                appended.lock().unwrap().push(text);
                i += 1;
            }
            i - N
        })
    };

    let response = compact(&addr, partition_request()).await.unwrap();
    stop.store(true, Ordering::SeqCst);
    let appends = writer.await.unwrap();
    let appended: Vec<usize> = (N..N + appends).collect();
    eprintln!("walless compaction: {response:?}");
    assert_eq!(response.layout, "segments");
    assert_eq!(response.partition_column, "num");
    assert_eq!(response.wal_generation, 0, "no log is rewritten");
    assert_eq!(response.rows_before, N as u64);
    assert_eq!(response.tombstones_reclaimed, doomed.len() as u64);
    assert!(
        response.tail_records_applied > 0,
        "the writer's rows were caught up after the cutoff"
    );
    assert_eq!(
        response.rows_after,
        (N - doomed.len()) as u64 + response.tail_records_applied,
    );

    // The layout: partition ranges, then the carried tail segments.
    let manifest = read_manifest(&index_path);
    check_partitions(&manifest, "cmp-");
    let carried: Vec<_> = manifest
        .segments
        .iter()
        .filter(|s| !s.segment_id.starts_with("cmp-"))
        .collect();
    assert!(
        !carried.is_empty(),
        "the writer's rows sealed unordered behind the partitions"
    );
    for segment in &carried {
        assert!(segment.summary.as_ref().unwrap().partition.is_none());
    }

    // Exactly once: the live texts are the seeded live rows plus every
    // appended row, no duplicates, no losses, no resurrected tombstones.
    client(&addr).await.flush(FlushRequest {}).await.unwrap();
    let mut expected: Vec<usize> = (0..N).filter(|i| i % 7 != 0).collect();
    expected.extend_from_slice(&appended);
    let mut texts = all_texts(&addr).await;
    texts.sort();
    let mut expected_texts: Vec<String> = expected.iter().map(|&i| row(i).text).collect();
    expected_texts.sort();
    assert_eq!(texts, expected_texts, "the live set after the cutover");

    // Every read path equals a shard built fresh from the final set.
    let (reference, reference_handle) =
        reference_shard(&dir, "reference.tv", &analysis, &expected).await;
    assert_eq!(
        observe(&addr, &analysis).await,
        observe(&reference, &analysis).await
    );

    // The stored sources, identities, lineage and declared tables.
    check_sources_and_tables(&index_path);

    // A reopen from disk serves the same.
    handle.abort();
    let _ = handle.await;
    let (reopened, reopened_handle) =
        start_opened_node(config(index_path.clone(), &analysis)).await;
    assert_eq!(
        observe(&reopened, &analysis).await,
        observe(&reference, &analysis).await
    );

    // The next partitioned compaction folds the carried tail segments
    // in: one layout, keyed ranges only.
    let second = compact(&reopened, partition_request()).await.unwrap();
    assert_eq!(second.partition_column, "num");
    let manifest = read_manifest(&index_path);
    check_partitions(&manifest, "cmp-");
    assert!(
        manifest
            .segments
            .iter()
            .all(|s| s.segment_id.starts_with("cmp-")),
        "every segment is a partition after the second compaction"
    );
    let mut texts = all_texts(&reopened).await;
    texts.sort();
    assert_eq!(texts, expected_texts);
    assert_eq!(
        observe(&reopened, &analysis).await,
        observe(&reference, &analysis).await
    );
    reference_handle.abort();
    reopened_handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn walless_compaction_refusals_name_the_cause() {
    let (analysis, _mock) = start_mock_analysis().await;
    let dir = tempdir("refusals");

    // A catalog with no WAL and no sealed segments: refused by name.
    let bare_path = dir.join("bare.tv");
    bind_catalog(&bare_path);
    let (bare, bare_handle) = start_opened_node(config(bare_path.clone(), &analysis)).await;
    let status = compact(&bare, partition_request()).await.unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(
        status.message().contains("has no WAL") && status.message().contains("no sealed segments"),
        "{}",
        status.message()
    );

    // Sealed but never bound: refused by name.
    let unbound_path = dir.join("unbound.tv");
    let (unbound, unbound_handle) =
        start_opened_node(config(unbound_path.clone(), &analysis)).await;
    seed(&unbound, 200).await;
    let status = compact(&unbound, partition_request()).await.unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(
        status.message().contains("no generation binding"),
        "{}",
        status.message()
    );

    // A bound catalog qualifies; every other refusal names its cause.
    let (index_path, addr, handle) = serve_fixture(&dir, "shard.tv", &analysis, 200).await;
    for (request, needle) in [
        (CompactShardRequest::default(), "needs partition_column"),
        (
            CompactShardRequest {
                partition_column: "score".into(),
                ..partition_request()
            },
            "is a double column",
        ),
        (
            CompactShardRequest {
                partition_column: "grp".into(),
                ..partition_request()
            },
            "is a facet column",
        ),
        (
            CompactShardRequest {
                partition_column: "missing".into(),
                ..partition_request()
            },
            "not a column of this shard",
        ),
        (
            CompactShardRequest {
                partition_column: "other".into(),
                ..partition_request()
            },
            "no document of this shard carries it",
        ),
    ] {
        let status = compact(&addr, request).await.unwrap_err();
        assert!(
            status.message().contains(needle),
            "refusal {:?} does not mention {needle:?}",
            status.message()
        );
    }

    // The build knobs are the from-segments path's alone.
    let (index_path_wal, addr_wal, handle_wal) = {
        let index_path = dir.join("logged.tv");
        let (addr, handle) = start_empty_node(NodeConfig {
            wal: true,
            wal_buckets: 4,
            ..config(index_path.clone(), &analysis)
        })
        .await;
        seed(&addr, 100).await;
        (index_path, addr, handle)
    };
    let _ = index_path_wal;
    let status = compact(
        &addr_wal,
        CompactShardRequest {
            build_threads: 2,
            ..partition_request()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(
        status
            .message()
            .contains("this shard compacts from its log"),
        "{}",
        status.message()
    );
    handle_wal.abort();

    // A dry run reports and writes nothing. (The "other" refusal above
    // happened past the cutoff seal, leaving the empty work directory
    // for inspection; clear it first.)
    let _ = std::fs::remove_dir_all(pipestream_search::compaction::default_work_dir(&index_path));
    let before = catalog_bytes(&index_path);
    let dry = compact(
        &addr,
        CompactShardRequest {
            dry_run: true,
            ..partition_request()
        },
    )
    .await
    .unwrap();
    assert!(dry.dry_run);
    assert_eq!(dry.rows_before, 200);
    assert_eq!(dry.rows_after, 200);
    assert_eq!(dry.partition_column, "num");
    assert_eq!(
        catalog_bytes(&index_path),
        before,
        "a dry run writes nothing"
    );
    assert!(!pipestream_search::compaction::default_work_dir(&index_path).exists());

    // The build-memory budget refuses by name before any work.
    let status = compact(
        &addr,
        CompactShardRequest {
            build_memory: 1 << 20,
            ..partition_request()
        },
    )
    .await
    .unwrap_err();
    assert!(
        status.message().contains("build-memory budget"),
        "{}",
        status.message()
    );
    // The refusal left the work directory behind for inspection; the
    // operator clears it before the next run.
    std::fs::remove_dir_all(pipestream_search::compaction::default_work_dir(&index_path)).unwrap();

    // The reshard level: a zero build queue and the budget, by name.
    let root = segments_root(&index_path);
    let set = SegmentCatalog::open(&root).unwrap().snapshot().clone();
    let overlay = LiveDocs::default();
    let work = dir.join("reshard-work");
    let err = reshard::compact_segments_partitioned(
        &set,
        &overlay,
        0,
        &work,
        PartitionSpec {
            column: "num",
            bound: BOUND as usize,
        },
        &SegmentCompactionOptions {
            build_queue: Some(0),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(err.contains("build queue 0"), "{err}");
    let err = reshard::compact_segments_partitioned(
        &set,
        &overlay,
        0,
        &work,
        PartitionSpec {
            column: "num",
            bound: BOUND as usize,
        },
        &SegmentCompactionOptions {
            build_threads: 4,
            build_memory: Some(200 * reshard::BUILD_BYTES_PER_ROW),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(err.contains("build threads"), "{err}");
    assert!(err.contains("MiB"), "{err}");
    let _ = std::fs::remove_dir_all(&work);

    // A leftover of an interrupted build is refused by name, and the
    // serving catalog is untouched by it.
    let junk = root.join("segments").join("cmp-000099-0000");
    std::fs::create_dir_all(&junk).unwrap();
    std::fs::write(junk.join("junk"), b"torn").unwrap();
    let status = compact(&addr, partition_request()).await.unwrap_err();
    assert!(
        status.message().contains("staged segment directory"),
        "{}",
        status.message()
    );
    let health = client(&addr)
        .await
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(health.bm25_docs, 200);
    std::fs::remove_dir_all(&junk).unwrap();

    bare_handle.abort();
    unbound_handle.abort();
    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn serial_and_parallel_walless_builds_are_byte_identical() {
    let (analysis, _mock) = start_mock_analysis().await;
    let dir = tempdir("identical");
    let (path_a, addr_a, handle_a) = serve_fixture(&dir, "a.tv", &analysis, 1_000).await;
    let (path_b, addr_b, handle_b) = serve_fixture(&dir, "b.tv", &analysis, 1_000).await;
    // Identical tombstones on both.
    for addr in [&addr_a, &addr_b] {
        client(addr)
            .await
            .delete_documents(DeleteDocumentsRequest {
                doc_ids: (0..1_000u64).step_by(7).collect(),
                expected_wal_generation: None,
            })
            .await
            .unwrap();
    }
    let serial = compact(
        &addr_a,
        CompactShardRequest {
            build_threads: 1,
            ..partition_request()
        },
    )
    .await
    .unwrap();
    let parallel = compact(
        &addr_b,
        CompactShardRequest {
            build_threads: 4,
            build_queue: 2,
            ..partition_request()
        },
    )
    .await
    .unwrap();
    assert_eq!(serial.rows_before, parallel.rows_before);
    assert_eq!(serial.tombstones_reclaimed, parallel.tombstones_reclaimed);
    let a = catalog_bytes(&path_a);
    let b = catalog_bytes(&path_b);
    assert_eq!(
        a.keys().collect::<Vec<_>>(),
        b.keys().collect::<Vec<_>>(),
        "the catalogs name the same files"
    );
    for (name, bytes) in &a {
        assert_eq!(bytes, &b[name], "file {name} differs between the builds");
    }
    handle_a.abort();
    handle_b.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn walless_compaction_preserves_the_derived_declaration() {
    use pipestream_search::derived::Declaration;
    use pipestream_search::pb::{
        DerivedColumn, DerivedColumns, DerivedDisclosure, MaterializeKind,
    };
    let (analysis, _mock) = start_mock_analysis().await;
    let dir = tempdir("derived");
    let spec = DerivedColumns {
        columns: vec![DerivedColumn {
            name: "num_d".into(),
            expression: "num + 1".into(),
            kind: MaterializeKind::I64 as i32,
            disclosure: DerivedDisclosure::Inputs as i32,
        }],
    };
    let declaration = Arc::new(Declaration::compile(&spec).unwrap());
    let index_path = dir.join("shard.tv");
    bind_catalog(&index_path);
    let (addr, handle) = start_opened_node(NodeConfig {
        derived: Some(declaration),
        ..config(index_path.clone(), &analysis)
    })
    .await;
    seed(&addr, 300).await;

    let response = compact(&addr, partition_request()).await.unwrap();
    assert_eq!(response.rows_after, 300);

    // Every output segment carries the declaration entry (kind 15) with
    // the declaration's fingerprint, and the derived values survive.
    let manifest = read_manifest(&index_path);
    let root = segments_root(&index_path);
    let outputs: Vec<_> = manifest
        .segments
        .iter()
        .filter(|s| s.segment_id.starts_with("cmp-"))
        .collect();
    assert!(!outputs.is_empty());
    let fingerprint = Declaration::compile(&spec)
        .unwrap()
        .fingerprint()
        .to_string();
    for segment in outputs {
        let bm25 = Bm25Reader::open(
            &SegmentCatalog::segment_dir(&root, &segment.segment_id).join(&segment.bm25.file),
        )
        .unwrap();
        let derived = bm25.derived().expect("kind-15 declaration entry");
        assert_eq!(derived.fingerprint, fingerprint);
        let di = (0..bm25.integer_count())
            .find(|i| bm25.integer_name(*i) == "num_d")
            .expect("the derived column is declared");
        let ni = (0..bm25.integer_count())
            .find(|i| bm25.integer_name(*i) == "num")
            .expect("the source column is declared");
        for row in 0..bm25.next_doc_id() {
            let text = Bm25Index::text(&bm25, row).unwrap();
            let i = number_of(&text);
            assert_eq!(bm25.integer_value(ni, row), Some((i / 3) as i64));
            assert_eq!(bm25.integer_value(di, row), Some((i / 3) as i64 + 1));
        }
    }
    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn an_interrupted_walless_cutover_rolls_back_at_open() {
    // The on-disk state a crash leaves around the manifest publish of a
    // from-segments cutover: the backup the cutover kept, the marker
    // naming the staged outputs and the replaced inputs, and — in the
    // "after publish" case — the new manifest already in place.
    let dir = std::env::temp_dir().join(format!("walless-rollback-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let index_path = dir.join("shard.tv");
    let root = segments_root(&index_path);
    let catalog = SegmentCatalog::open(&root).unwrap();
    // One real segment to stand for the serving set.
    let work = dir.join("work-segment");
    std::fs::create_dir_all(&work).unwrap();
    let bm25_path = work.join("documents.bm25");
    let live_path = work.join("live.bin");
    let mut store = pipestream_search::postings::Bm25Store::with_fields(&["body"]);
    store.add_document(
        0,
        "document".into(),
        pipestream_search::analyzer::analyze_document_native("document", Some(&body_spec()))
            .unwrap(),
    );
    store.save(&bm25_path).unwrap();
    LiveDocs::default().write(&live_path, 1).unwrap();
    catalog
        .append(pipestream_search::segments::SegmentSource {
            segment_id: "seg-00000001",
            generation: 1,
            base_label: 0,
            backend_kind: "",
            vector_path: None,
            exact_vector_path: None,
            bm25_path: &bm25_path,
            live_docs_path: &live_path,
            partition_column: None,
        })
        .unwrap();
    let manifest = catalog.snapshot().manifest().clone();
    let manifest_bytes = std::fs::read(SegmentCatalog::manifest_path(&root)).unwrap();
    let backup_path = {
        let mut name = SegmentCatalog::manifest_path(&root).as_os_str().to_owned();
        name.push(".pre-compact");
        PathBuf::from(name)
    };
    let marker_path = pipestream_search::compaction::marker_path(&index_path);
    let staged_dir = SegmentCatalog::segment_dir(&root, "cmp-000007-0000");
    let write_marker = || {
        std::fs::write(
            &marker_path,
            serde_json::json!({
                "format": 1,
                "layout": "segments",
                "old_wal_generation": 0,
                "new_wal_generation": 0,
                "walless": true,
                "work_dir": dir.join("work"),
                "previous_snapshot": false,
                "legacy_files": [],
                "staged_segments": ["cmp-000007-0000"],
                "replaced_segments": ["seg-00000001"],
            })
            .to_string(),
        )
        .unwrap();
    };

    // Crash after the marker, before the publish: the manifest on disk
    // is the serving one.
    std::fs::write(&backup_path, &manifest_bytes).unwrap();
    std::fs::create_dir_all(&staged_dir).unwrap();
    std::fs::write(staged_dir.join("junk"), b"torn").unwrap();
    write_marker();
    pipestream_search::node::recover_generation(&index_path);
    assert_eq!(
        std::fs::read(SegmentCatalog::manifest_path(&root)).unwrap(),
        manifest_bytes,
        "the serving manifest is restored"
    );
    assert!(!staged_dir.exists(), "the staged outputs are removed");
    assert!(!marker_path.exists());
    assert!(!backup_path.exists());
    assert_eq!(
        SegmentCatalog::open(&root)
            .unwrap()
            .snapshot()
            .manifest()
            .segments
            .len(),
        1,
        "the catalog serves the pre-compaction set"
    );

    // Crash after the publish: the new manifest (the serving set
    // replaced by the staged outputs) is already in place; the rollback
    // restores the backup over it.
    let mut published = manifest.clone();
    published.epoch += 1;
    let mut replacement = published.segments[0].clone();
    replacement.segment_id = "cmp-000007-0000".to_string();
    published.segments.push(replacement);
    pipestream_search::segments::write_manifest_file(
        &SegmentCatalog::manifest_path(&root),
        &published,
    )
    .unwrap();
    std::fs::write(&backup_path, &manifest_bytes).unwrap();
    std::fs::create_dir_all(&staged_dir).unwrap();
    write_marker();
    pipestream_search::node::recover_generation(&index_path);
    assert_eq!(
        std::fs::read(SegmentCatalog::manifest_path(&root)).unwrap(),
        manifest_bytes,
        "the backup replaces the published manifest"
    );
    assert!(!staged_dir.exists());
    assert!(!marker_path.exists());

    // Torn staging without a marker (a crash during the build) is never
    // adopted: the catalog opens as it was and the leftover simply sits
    // there for the next run to refuse by name.
    std::fs::create_dir_all(&staged_dir).unwrap();
    pipestream_search::node::recover_generation(&index_path);
    assert_eq!(
        SegmentCatalog::open(&root)
            .unwrap()
            .snapshot()
            .manifest()
            .segments
            .len(),
        1
    );
    assert!(
        staged_dir.exists(),
        "recovery adopts nothing without a marker"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
