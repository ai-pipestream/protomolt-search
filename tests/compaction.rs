//! Online compaction (`docs/mutations.md`): a shard keeps taking writes
//! while `CompactShard` rebuilds it dense, tails the log into the
//! rebuild, and cuts over under one brief write lock.
//!
//! Pinned here:
//!
//! - Concurrent writes continue; the tail applies them and the cutover's
//!   write-lock hold stays below 500 ms.
//! - A quiescent second compaction reclaims every tombstone: `Health`
//!   shows `deleted_docs = 0` and live rows equal to the tracked set.
//! - Every read path (lexical, dense, hybrid, facets, sorted browse,
//!   fetch, parents) equals a shard freshly built from the same FINAL
//!   document set, compared by stable text with scores bitwise; a reopen
//!   from disk equals; the rewritten generation replays to the same
//!   shard; a cursor minted before the cutover refuses by name.
//! - Distributed equals monolithic with an untouched second shard.
//! - The refusal table, the snapshot-during-compaction abort, the dry
//!   run, and the rollback of an interrupted cutover.

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    search_query, selection_query, AddDocumentsRequest, AddDocumentsResponse, AddVectorsRequest,
    AddVectorsResponse, Bm25SearchRequest, BrowseShardRequest, BrowseSort,
    CommitReplacementsRequest, CompactShardRequest, CompactShardResponse, DeleteDocumentsRequest,
    DenseQuery, DocLineage, FacetValue, FlushRequest, GetDocumentsRequest, HealthRequest,
    HybridSearchRequest, IntegerValue, QueryRequest, Replacement, ResolveParentsRequest,
    SearchQuery, SearchRequest, SelectionQuery,
};
use pipestream_search::segments::{SegmentCatalog, SegmentMetadata, SegmentSetManifest};
use tonic::transport::Channel;
use tonic::Request;

use common::mock::start_mock_analysis;
use common::{fit_calibration, start_empty_node, start_opened_node, unit_vectors, BIT_WIDTH, DIM};

/// Rows ingested before the first compaction.
const N: usize = 1_200;
const GROUPS: [&str; 3] = ["red", "green", "blue"];
const EXTRAS: [&str; 4] = ["one", "two", "three", "four"];
const SEED: u64 = 0xC0A5_7A11;

fn tempdir(tag: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("protomolt_compaction_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn config(index_path: Option<PathBuf>, analysis: &str) -> NodeConfig {
    NodeConfig {
        index_path,
        analysis_addr: Some(analysis.to_string()),
        wal: true,
        wal_buckets: 8,
        facet_fields: vec!["grp".into()],
        integer_fields: vec!["num".into(), "other".into()],
        numeric_fields: vec!["score".into()],
        ..Default::default()
    }
}

/// One product row: its stable key, text (unique per row, so hits map
/// across renumbering by text), columns, and vector.
#[derive(Clone, Debug, PartialEq)]
struct Row {
    key: String,
    text: String,
    num: i64,
    grp: &'static str,
    vector: Vec<f32>,
}

fn row(i: usize, revised: bool) -> Row {
    let grp = GROUPS[i % GROUPS.len()];
    let vector = unit_vectors(1, DIM, SEED.wrapping_add(i as u64 * 7919 + revised as u64));
    Row {
        key: format!("row/{i}{}", if revised { "/v2" } else { "" }),
        text: if revised {
            format!("row{i} common {grp} revised")
        } else {
            format!("row{i} common {grp} {}", EXTRAS[i % EXTRAS.len()])
        },
        num: i as i64 * 2 + revised as i64,
        grp,
        vector,
    }
}

async fn client(addr: &str) -> NodeServiceClient<Channel> {
    NodeServiceClient::connect(addr.to_string()).await.unwrap()
}

async fn calibrate(addr: &str, shift: &[f32], scale: &[f32]) {
    client(addr)
        .await
        .configure_vector_backend(pipestream_search::harness::embedded_backend_request(
            DIM, BIT_WIDTH, shift, scale,
        ))
        .await
        .unwrap();
}

/// Append one row with its stable key on both legs (the routed
/// replication path: one document, then its vector, keyed), and return
/// its global id with the WAL generation that id belongs to. The two
/// calls can straddle a compaction cutover: the document is then
/// answered with an old-generation id and the vector with the new one,
/// and the row sits at the vector's id in the new generation (the tail
/// applied the document at the shadow's tip, where the vector landed).
fn stable_metadata(key: &[u8]) -> tonic::metadata::MetadataValue<tonic::metadata::Binary> {
    let encoded = pipestream_search::replication::encode_stable_keys(&[key.to_vec()]).unwrap();
    tonic::metadata::MetadataValue::from_bytes(&encoded)
}

async fn append(client: &mut NodeServiceClient<Channel>, row: &Row) -> (u64, u64) {
    let documents = add_document(client, row).await;
    let vectors = add_vector(client, row).await;
    if documents.wal_generation == vectors.wal_generation {
        assert_eq!(
            documents.first_id, vectors.first_id,
            "document and vector legs must land at one id"
        );
    }
    (vectors.first_id, vectors.wal_generation)
}

// Adjacent rows share a parent source. Row zero is deleted before
// compaction, so row one's source must survive without its first chunk.
fn original_source(row: &Row) -> (pipestream_search::pb::ProtobufSource, Option<u32>) {
    let parent = format!("parent/{}", row.num / 4);
    let version = format!("version/{}", row.num % 2);
    (
        common::protobuf_source(&parent, &version),
        Some(((row.num / 2) % 2) as u32),
    )
}

fn logical_identity(row: &Row) -> pipestream_search::pb::DocumentIdentity {
    pipestream_search::pb::DocumentIdentity {
        document_key: format!("parent/{}", row.num / 4).into_bytes(),
        version: (row.num % 2) as u64 + 1,
        chunk_ordinal: original_source(row).1,
    }
}

/// The document half of a legacy two-RPC append, keyed.
async fn add_document(client: &mut NodeServiceClient<Channel>, row: &Row) -> AddDocumentsResponse {
    let doc = AddDocumentsRequest {
        text: row.text.clone(),
        original_source: Some(original_source(row).0),
        source_chunk_ordinal: original_source(row).1,
        identity: Some(logical_identity(row)),
        lineage: Some(DocLineage {
            parent_id: row.num as u64,
            ..Default::default()
        }),
        facets: vec![FacetValue {
            field: "grp".into(),
            value: row.grp.to_string(),
        }],
        integers: vec![IntegerValue {
            field: "num".into(),
            value: row.num,
        }],
        ..Default::default()
    };
    let mut request = Request::new(tokio_stream::iter([doc]));
    request.metadata_mut().insert_bin(
        pipestream_search::replication::STABLE_KEYS_METADATA,
        stable_metadata(row.key.as_bytes()),
    );
    client.add_documents(request).await.unwrap().into_inner()
}

/// The vector half of a legacy two-RPC append, keyed.
async fn add_vector(client: &mut NodeServiceClient<Channel>, row: &Row) -> AddVectorsResponse {
    let mut request = Request::new(tokio_stream::iter([AddVectorsRequest {
        materialized_rows: Vec::new(),
        vectors: row.vector.clone(),
        dim: DIM as u32,
    }]));
    request.metadata_mut().insert_bin(
        pipestream_search::replication::STABLE_KEYS_METADATA,
        stable_metadata(row.key.as_bytes()),
    );
    client.add_vectors(request).await.unwrap().into_inner()
}

/// The product's view of the shard: live rows by key, and the global
/// id each live row currently has.
#[derive(Default)]
struct Tracked {
    live: BTreeMap<String, Row>,
    /// `(global id, WAL generation the id belongs to)`.
    ids: BTreeMap<String, (u64, u64)>,
}

async fn seed_shard(addr: &str, shift: &[f32], scale: &[f32]) -> Tracked {
    calibrate(addr, shift, scale).await;
    let mut client = client(addr).await;
    let mut tracked = Tracked::default();
    for i in 0..N {
        let row = row(i, false);
        let id = append(&mut client, &row).await;
        tracked.ids.insert(row.key.clone(), id);
        tracked.live.insert(row.key.clone(), row);
    }
    // Deletes: every seventh row, claiming the generation the ids came
    // from (0: a fresh shard's first generation is claimable too).
    let doomed: Vec<u64> = (0..N)
        .filter(|i| i % 7 == 0)
        .map(|i| tracked.ids[&row(i, false).key].0)
        .collect();
    let deleted = client
        .delete_documents(DeleteDocumentsRequest {
            doc_ids: doomed,
            expected_wal_generation: Some(0),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(deleted.deleted as usize, N.div_ceil(7));
    for i in (0..N).filter(|i| i % 7 == 0) {
        let key = row(i, false).key;
        tracked.live.remove(&key);
        tracked.ids.remove(&key);
    }
    // Replacements: append the revision, then retire the original.
    for i in [5usize, 11, 23, 100, 611] {
        let old = row(i, false);
        let new = row(i, true);
        let new_id = append(&mut client, &new).await;
        let committed = client
            .commit_replacements(CommitReplacementsRequest {
                replacements: vec![Replacement {
                    old_doc_id: tracked.ids[&old.key].0,
                    new_doc_id: new_id.0,
                }],
                expected_wal_generation: Some(0),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(committed.committed, 1);
        tracked.live.remove(&old.key);
        tracked.ids.remove(&old.key);
        tracked.ids.insert(new.key.clone(), new_id);
        tracked.live.insert(new.key.clone(), new);
    }
    client.flush(FlushRequest {}).await.unwrap();
    tracked
}

/// What the concurrent writer did while compaction ran.
#[derive(Default, Debug)]
struct WriterStats {
    calls: u64,
    max_call: Duration,
    /// Id-addressed mutations the writer withheld or the node refused
    /// because the cutover renumbered the rows they named.
    stale: u64,
}

fn is_stale_generation(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::FailedPrecondition
        && status.message().contains("stale WAL generation")
}

/// Appends the writer makes at most. The stop flag ends it first on an
/// unloaded machine (about 700 appends); the cap is what makes the
/// compaction's tail loop converge on any machine, because a loop that
/// exits on "fewer than tail_bound records arrived during a pass" only
/// terminates against a writer that stops, and this one stops after a
/// count, not after a time.
const WRITER_APPEND_CAP: usize = 1_500;

/// Keep writing through the public RPCs until told to stop or the append
/// cap is reached: appends, with every fifth append followed by a delete
/// of an earlier one and every seventh by a replacement. Tracks the
/// product's view.
async fn writer(
    addr: String,
    tracked: Arc<Mutex<Tracked>>,
    stop: Arc<AtomicBool>,
    start_at: usize,
) -> WriterStats {
    let mut client = client(&addr).await;
    let mut stats = WriterStats::default();
    let mut i = start_at;
    let mut mine: Vec<Row> = Vec::new();
    let timed = |stats: &mut WriterStats, started: Instant| {
        stats.calls += 1;
        stats.max_call = stats.max_call.max(started.elapsed());
    };
    while !stop.load(Ordering::Acquire) && i - start_at < WRITER_APPEND_CAP {
        let fresh = row(i, false);
        let started = Instant::now();
        let id = append(&mut client, &fresh).await;
        timed(&mut stats, started);
        {
            let mut t = tracked.lock().unwrap();
            t.ids.insert(fresh.key.clone(), id);
            t.live.insert(fresh.key.clone(), fresh.clone());
        }
        mine.push(fresh);
        if i.is_multiple_of(5) && mine.len() > 2 {
            let victim = mine.remove(0);
            let (id, generation) = tracked.lock().unwrap().ids[&victim.key];
            let started = Instant::now();
            let response = client
                .delete_documents(DeleteDocumentsRequest {
                    doc_ids: vec![id],
                    expected_wal_generation: Some(generation),
                })
                .await;
            timed(&mut stats, started);
            match response {
                Ok(response) => {
                    assert_eq!(response.into_inner().deleted, 1);
                    let mut t = tracked.lock().unwrap();
                    t.live.remove(&victim.key);
                    t.ids.remove(&victim.key);
                }
                Err(status) if is_stale_generation(&status) => stats.stale += 1,
                Err(status) => panic!("delete during compaction: {status}"),
            }
        }
        if i.is_multiple_of(7) && !mine.is_empty() {
            let old = mine.remove(0);
            let old_index: usize = old.key["row/".len()..].parse().unwrap();
            let new = row(old_index, true);
            let started = Instant::now();
            let (new_id, new_generation) = append(&mut client, &new).await;
            {
                let mut t = tracked.lock().unwrap();
                t.ids.insert(new.key.clone(), (new_id, new_generation));
                t.live.insert(new.key.clone(), new.clone());
            }
            let (old_id, old_generation) = tracked.lock().unwrap().ids[&old.key];
            if old_generation != new_generation {
                // The old id predates the cutover: sending it would name
                // some other row now. The revision stays as an ordinary
                // live row beside the original.
                stats.stale += 1;
                timed(&mut stats, started);
                i += 1;
                continue;
            }
            let response = client
                .commit_replacements(CommitReplacementsRequest {
                    replacements: vec![Replacement {
                        old_doc_id: old_id,
                        new_doc_id: new_id,
                    }],
                    expected_wal_generation: Some(new_generation),
                })
                .await;
            timed(&mut stats, started);
            match response {
                Ok(response) => {
                    assert_eq!(response.into_inner().committed, 1);
                    let mut t = tracked.lock().unwrap();
                    t.live.remove(&old.key);
                    t.ids.remove(&old.key);
                }
                Err(status) if is_stale_generation(&status) => stats.stale += 1,
                Err(status) => panic!("replacement during compaction: {status}"),
            }
        }
        i += 1;
    }
    stats
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

/// Everything a reader can observe on one shard, keyed by stable text
/// so two shards with different positional ids compare directly. Scores
/// are exact bits.
#[derive(Debug, PartialEq)]
struct Reads {
    lexical: Vec<(String, Vec<(String, u32)>)>,
    facets: Vec<(String, Vec<(String, u64)>)>,
    dense: Vec<Vec<(String, u32)>>,
    hybrid: Vec<Vec<(String, u32)>>,
    browse_by_num: Vec<String>,
    parents: Vec<(String, u64)>,
}

const LEXICAL_PROBES: [&str; 5] = ["revised", "four", "blue three", "row17 green", "red one"];

async fn texts_of(
    node: &mut NodeServiceClient<Channel>,
    ids: &[u64],
) -> BTreeMap<u64, (String, Option<DocLineage>)> {
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
        let expected = identity_for_text(&d.text);
        assert_eq!(d.identity, expected);
        (d.doc_id, (d.text, d.lineage))
    })
    .collect()
}

fn identity_for_text(text: &str) -> Option<pipestream_search::pb::DocumentIdentity> {
    // The unkeyed partition fixture also includes legacy rows without source metadata.
    if text.starts_with("bare") {
        return None;
    }
    let number = text
        .split_whitespace()
        .next()
        .unwrap()
        .strip_prefix("row")
        .unwrap()
        .parse()
        .unwrap();
    Some(logical_identity(&row(number, text.contains("revised"))))
}

fn sorted(mut hits: Vec<(String, u32)>) -> Vec<(String, u32)> {
    hits.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    hits
}

/// Observe `addrs` through one coordinator (and each node's own browse
/// and parent routes). `browse_addr` is the node whose sorted browse and
/// parents are read; the coordinator routes cover every shard.
async fn observe(addrs: &[String], analysis: &str, queries: &[Vec<f32>]) -> Reads {
    let coordinator = CoordinatorServiceImpl::new(addrs.to_vec())
        .with_bm25(Some(analysis.to_string()), Default::default());
    let mut nodes = Vec::new();
    for addr in addrs {
        nodes.push(client(addr).await);
    }
    // Which node owns a global id: slot offsets are 0 and 1_000_000.
    async fn resolve(
        nodes: &mut [NodeServiceClient<Channel>],
        ids: Vec<(u64, u32)>,
        identities: Option<&BTreeMap<u64, Option<pipestream_search::pb::DocumentIdentity>>>,
    ) -> Vec<(String, u32)> {
        let mut out = Vec::with_capacity(ids.len());
        for (id, bits) in ids {
            let owner = if id >= 1_000_000 { 1 } else { 0 }.min(nodes.len() - 1);
            let mut texts = texts_of(&mut nodes[owner], &[id]).await;
            let (text, _) = texts
                .remove(&id)
                .expect("hit resolves to a stored document");
            if let Some(identities) = identities {
                assert_eq!(identities[&id], identity_for_text(&text));
            }
            out.push((text, bits));
        }
        sorted(out)
    }
    // Public result identities resolve to the logical source after cutover,
    // reopen and WAL replay, even though compaction has renumbered physical IDs.
    let lexical_selection = SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: "lex".into(),
            query: Some(search_query::Query::Lexical(
                pipestream_search::pb::LexicalQuery {
                    text: "common".into(),
                    ..Default::default()
                },
            )),
        })),
    };
    let browse_selection = SelectionQuery {
        node: Some(selection_query::Node::Filter(
            pipestream_search::pb::FilterQuery {
                id: "rows".into(),
                predicate: Some(pipestream_search::pb::filter_query::Predicate::Cel(
                    "num >= 0".into(),
                )),
            },
        )),
    };
    let boolean_selection = SelectionQuery {
        node: Some(selection_query::Node::Boolean(
            pipestream_search::pb::BooleanQuery {
                must: vec![lexical_selection.clone()],
                ..Default::default()
            },
        )),
    };
    for selection in [lexical_selection, browse_selection, boolean_selection] {
        let response = SearchService::query(
            &coordinator,
            Request::new(QueryRequest {
                k: 5000,
                selection: Some(selection),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(!response.hits.is_empty());
        let identities = response
            .hits
            .iter()
            .map(|h| (h.doc_id, h.identity.clone()))
            .collect();
        resolve(
            &mut nodes,
            response
                .hits
                .iter()
                .map(|h| (h.doc_id, h.score.to_bits()))
                .collect(),
            Some(&identities),
        )
        .await;
    }
    let mut lexical = Vec::new();
    let mut facets = Vec::new();
    for probe in LEXICAL_PROBES {
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
        let hits = response
            .hits
            .iter()
            .map(|h| (h.doc_id, h.score.to_bits()))
            .collect();
        let identities = response
            .hits
            .iter()
            .map(|hit| (hit.doc_id, hit.identity.clone()))
            .collect();
        lexical.push((
            probe.to_string(),
            resolve(&mut nodes, hits, Some(&identities)).await,
        ));
        let mut counts: Vec<(String, u64)> = response
            .facets
            .iter()
            .flat_map(|f| f.counts.iter().map(|c| (c.value.clone(), c.count)))
            .collect();
        counts.sort();
        facets.push((probe.to_string(), counts));
    }
    let mut dense = Vec::new();
    let mut hybrid = Vec::new();
    for query in queries {
        let response = SearchService::search(
            &coordinator,
            Request::new(SearchRequest {
                k: 20,
                vector: query.clone(),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let hits = response
            .hits
            .iter()
            .map(|h| (h.vector_id, h.score.to_bits()))
            .collect();
        let identities = response
            .hits
            .iter()
            .map(|hit| (hit.vector_id, hit.identity.clone()))
            .collect();
        let classic = resolve(&mut nodes, hits, Some(&identities)).await;
        let streamed = SearchService::search(
            &coordinator.clone().with_stream_search(true),
            Request::new(SearchRequest {
                k: 20,
                vector: query.clone(),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let hits = streamed
            .hits
            .iter()
            .map(|hit| (hit.vector_id, hit.score.to_bits()))
            .collect();
        let identities = streamed
            .hits
            .iter()
            .map(|hit| (hit.vector_id, hit.identity.clone()))
            .collect();
        assert_eq!(resolve(&mut nodes, hits, Some(&identities)).await, classic);
        dense.push(classic);
        let response = SearchService::hybrid_search(
            &coordinator,
            Request::new(HybridSearchRequest {
                text: "common revised".into(),
                vector: query.clone(),
                k: 20,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let hits = response
            .hits
            .iter()
            .map(|h| (h.doc_id, h.fused_score.to_bits()))
            .collect();
        hybrid.push(resolve(&mut nodes, hits, None).await);
    }
    // Sorted browse and parents on the first node only (the routes are
    // per shard).
    let browse = nodes[0]
        .browse_shard(BrowseShardRequest {
            k: 100_000,
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
    let texts = texts_of(&mut nodes[0], &browse.doc_ids).await;
    let browse_by_num: Vec<String> = browse
        .doc_ids
        .iter()
        .map(|id| texts[id].0.clone())
        .collect();
    let parents = nodes[0]
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
        .map(|p| (texts[&p.doc_id].0.clone(), p.parent_id))
        .collect();
    parents.sort();
    Reads {
        lexical,
        facets,
        dense,
        hybrid,
        browse_by_num,
        parents,
    }
}

/// A fresh shard over exactly `rows`, for the reference reads.
async fn reference_shard(
    dir: &Path,
    name: &str,
    analysis: &str,
    shift: &[f32],
    scale: &[f32],
    rows: &[Row],
) -> (
    String,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let (addr, handle) = start_empty_node(config(Some(dir.join(name)), analysis)).await;
    calibrate(&addr, shift, scale).await;
    let mut client = client(&addr).await;
    for row in rows {
        append(&mut client, row).await;
    }
    client.flush(FlushRequest {}).await.unwrap();
    (addr, handle)
}

use pipestream_search::compaction::{default_work_dir, marker_path};
use pipestream_search::node::segments_root;
use pipestream_search::postings::{Bm25Index, Bm25Reader};
use pipestream_search::reshard::{split_logs_with, SplitRequest};
use pipestream_search::segments::OpenedSegmentSet;

fn wal_gen(index_path: &Path, generation: u64) -> PathBuf {
    pipestream_search::wal::gen_dir(&pipestream_search::wal::wal_dir(index_path), generation)
}

async fn shard_health(addr: &str) -> pipestream_search::pb::HealthResponse {
    client(addr)
        .await
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner()
}

fn dense_cursor_query(probe: Vec<f32>, cursor: String) -> QueryRequest {
    let dense = search_query::Query::Dense(DenseQuery {
        vector: probe,
        ..Default::default()
    });
    let node = selection_query::Node::Search(SearchQuery {
        id: "dense".into(),
        query: Some(dense),
    });
    QueryRequest {
        k: 1,
        selection: Some(SelectionQuery { node: Some(node) }),
        cursor,
        ..Default::default()
    }
}

fn assert_first_compaction_postconditions(
    index_path: &Path,
    first: &CompactShardResponse,
    stats: &WriterStats,
    elapsed: Duration,
) {
    assert!(stats.calls > 0, "the concurrent writer completed no calls");
    assert!(stats.max_call < elapsed / 2, "{stats:?} vs {elapsed:?}");
    assert!(!first.dry_run && first.wal_generation == 1);
    assert_eq!(first.layout, "segments");
    assert!(first.rows_before >= (N + 5) as u64);
    assert!(first.tombstones_reclaimed >= (N.div_ceil(7) + 5) as u64);
    assert!(
        first.tail_records_applied > 0 && first.locked_tail_records == 0,
        "{first:?}"
    );
    assert!(first.write_lock_ms < 500, "lock {} ms", first.write_lock_ms);
    assert!(!default_work_dir(index_path).exists() && !marker_path(index_path).exists());
    assert!(wal_gen(index_path, 0).exists() && wal_gen(index_path, 1).exists());
    let root = segments_root(index_path);
    let manifest = SegmentCatalog::read_manifest(&root).unwrap().unwrap();
    assert!(manifest
        .segments
        .iter()
        .all(|s| { s.segment_id.starts_with("cmp-000001-") || s.segment_id.starts_with("seg-") }));
    for entry in std::fs::read_dir(root.join("segments")).unwrap() {
        let name = entry.unwrap().file_name().to_string_lossy().into_owned();
        assert!(manifest.segments.iter().any(|s| s.segment_id == name));
    }
}

async fn assert_stale_gen_and_cursor_refusals(
    addr: &str,
    analysis: &str,
    stale_id: u64,
    cursor_probe: Vec<f32>,
    cursor: String,
) {
    let del = DeleteDocumentsRequest {
        doc_ids: vec![stale_id],
        expected_wal_generation: Some(0),
    };
    let err = client(addr).await.delete_documents(del).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("stale WAL generation"), "{err}");

    let coord = CoordinatorServiceImpl::new(vec![addr.to_string()])
        .with_cursor_signing_key([0x43; 32])
        .with_bm25(Some(analysis.to_string()), Default::default());
    let req = Request::new(dense_cursor_query(cursor_probe, cursor));
    let err = SearchService::query(&coord, req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message().contains("cursor data context changed"),
        "{err}"
    );
}

fn verify_compacted_reader_sources(reader: &Bm25Reader, final_rows: &[Row]) {
    type KeyedRows = BTreeMap<Vec<u8>, Vec<(u32, u64, Option<u32>)>>;
    let mut keyed = KeyedRows::new();
    for local in 0..reader.next_doc_id() {
        let text = reader.text(local).unwrap();
        let row = final_rows.iter().find(|r| r.text == text).unwrap();
        assert_eq!(
            reader.protobuf_source(local).unwrap(),
            Some(original_source(row))
        );
        let id = logical_identity(row);
        assert_eq!(reader.document_identity(local), Some(id.clone()));
        keyed
            .entry(id.document_key)
            .or_default()
            .push((local, id.version, id.chunk_ordinal));
    }
    for (key, expected) in keyed {
        let mut actual = Vec::new();
        assert!(reader.visit_document_rows(&key, &mut |r, v, o| {
            actual.push((r, v, o));
            true
        }));
        actual.sort_unstable();
        assert_eq!(actual, expected);
    }
}

fn verify_catalog_bm25_sources(index_path: &Path, final_rows: &[Row]) {
    let set = OpenedSegmentSet::open(segments_root(index_path)).unwrap();
    for part in 0..set.manifest().segments.len() {
        verify_compacted_reader_sources(set.bm25(part), final_rows);
    }
}

struct ReplayCheck<'a> {
    dir: &'a Path,
    index_path: &'a Path,
    analysis: &'a str,
    cal: (&'a [f32], &'a [f32]),
    addr: &'a str,
    final_rows: &'a [Row],
    queries: &'a [Vec<f32>],
}

async fn verify_compacted_wal_replay(
    check: &ReplayCheck<'_>,
    expected: &Reads,
) -> tokio::task::JoinHandle<Result<(), tonic::transport::Error>> {
    let rt = tokio::runtime::Handle::current();
    let addr = check.analysis.to_string();
    let mut replay = move |docs: &[(
        &str,
        Option<&pipestream_search::pb::AnalysisSpec>,
        pipestream_search::analyzer::SessionLayers,
    )]| {
        tokio::task::block_in_place(|| {
            rt.block_on(pipestream_search::analyzer::analyze_batch_streams(
                &addr, docs, 1,
            ))
            .map_err(|e| e.to_string())
        })
    };
    let (gen2, out_dir, fields) = (
        wal_gen(check.index_path, 2),
        check.dir.join("replayed"),
        ["body".to_string()],
    );
    let req = SplitRequest::new(std::slice::from_ref(&gen2), 1, &out_dir)
        .slot_stride(1_000_000)
        .bm25_fields(Some(&fields));
    let replayed = split_logs_with(&req, &mut replay).unwrap();
    let child = &replayed.children[0];
    let live = check.final_rows.len() as u64;
    assert_eq!((child.num_vectors, child.num_documents), (live, live));
    verify_catalog_bm25_sources(&child.vector_path, check.final_rows);
    let cfg = config(Some(child.vector_path.clone()), check.analysis);
    let (r_addr, r_handle) = start_opened_node(cfg).await;
    let got = observe(std::slice::from_ref(&r_addr), check.analysis, check.queries).await;
    assert_eq!(&got, expected, "rewritten WAL replays differently");
    r_handle
}

async fn verify_distributed_matches_monolithic(check: &ReplayCheck<'_>, reopened: &str) {
    let other_rows: Vec<Row> = (5_000..5_300).map(|i| row(i, false)).collect();
    let other_cfg = config(Some(check.dir.join("other.vector")), check.analysis);
    let (other, other_h) = start_empty_node(NodeConfig {
        slot_offset: 1_000_000,
        ..other_cfg
    })
    .await;
    calibrate(&other, check.cal.0, check.cal.1).await;
    {
        let mut c = client(&other).await;
        for r in &other_rows {
            append(&mut c, r).await;
        }
        c.flush(FlushRequest {}).await.unwrap();
    }
    let mut union = check.final_rows.to_vec();
    union.extend(other_rows);
    let (mono, mono_h) = reference_shard(
        check.dir,
        "monolithic.vector",
        check.analysis,
        check.cal.0,
        check.cal.1,
        &union,
    )
    .await;
    let shards = [reopened.to_string(), other];
    let dist = observe(&shards, check.analysis, check.queries).await;
    let exp = observe(std::slice::from_ref(&mono), check.analysis, check.queries).await;
    assert_eq!(
        (&dist.lexical, &dist.facets, &dist.dense, &dist.hybrid),
        (&exp.lexical, &exp.facets, &exp.dense, &exp.hybrid)
    );
    other_h.abort();
    mono_h.abort();
}

async fn verify_reopened_and_replayed_equivalence(
    ctx: ReplayCheck<'_>,
    mut handle: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let live_rows = ctx.final_rows.len() as u64;
    verify_catalog_bm25_sources(ctx.index_path, ctx.final_rows);
    let (ref_addr, ref_h) = reference_shard(
        ctx.dir,
        "reference.vector",
        ctx.analysis,
        ctx.cal.0,
        ctx.cal.1,
        ctx.final_rows,
    )
    .await;
    let expected = observe(std::slice::from_ref(&ref_addr), ctx.analysis, ctx.queries).await;
    assert_eq!(expected.browse_by_num.len(), ctx.final_rows.len());
    let live_obs = observe(&[ctx.addr.to_string()], ctx.analysis, ctx.queries).await;
    assert_eq!(live_obs, expected);

    handle.abort();
    let _ = (&mut handle).await;
    let cfg = config(Some(ctx.index_path.to_path_buf()), ctx.analysis);
    let (reopened, reopened_h) = start_opened_node(cfg).await;
    let h = shard_health(&reopened).await;
    assert_eq!(
        (h.live_docs, h.deleted_docs, h.wal_generation),
        (live_rows, 0, 2)
    );
    assert_eq!(
        observe(std::slice::from_ref(&reopened), ctx.analysis, ctx.queries).await,
        expected
    );

    let replayed_h = verify_compacted_wal_replay(&ctx, &expected).await;
    verify_distributed_matches_monolithic(&ctx, &reopened).await;
    ref_h.abort();
    reopened_h.abort();
    replayed_h.abort();
}

async fn run_online_compaction() {
    let (analysis, _mock) = start_mock_analysis().await;
    let dir = tempdir("segments");
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &unit_vectors(256, DIM, SEED));
    let index_path = dir.join("shard.vector");
    let (addr, handle) = start_empty_node(config(Some(index_path.clone()), &analysis)).await;
    let tracked = Arc::new(Mutex::new(seed_shard(&addr, &shift, &scale).await));
    let queries: Vec<Vec<f32>> = (0..3)
        .map(|q| unit_vectors(1, DIM, 0x5E61_0000 + q))
        .collect();

    let coord = CoordinatorServiceImpl::new(vec![addr.clone()])
        .with_cursor_signing_key([0x43; 32])
        .with_bm25(Some(analysis.clone()), Default::default());
    let probe = row(700, false).vector.clone();
    let req = Request::new(dense_cursor_query(probe.clone(), String::new()));
    let page1 = SearchService::query(&coord, req)
        .await
        .unwrap()
        .into_inner();
    assert!(!page1.next_cursor.is_empty());

    let before = shard_health(&addr).await;
    assert!(before.deleted_docs > 0 && before.wal_generation == 0);

    let stop = Arc::new(AtomicBool::new(false));
    let w_task = tokio::spawn(writer(addr.clone(), tracked.clone(), stop.clone(), N + 100));
    let started = Instant::now();
    let first = compact(&addr, Default::default()).await.unwrap();
    let elapsed = started.elapsed();
    stop.store(true, Ordering::Release);
    let stats = w_task.await.unwrap();
    assert_first_compaction_postconditions(&index_path, &first, &stats, elapsed);

    let stale_id = tracked.lock().unwrap().ids.values().next().unwrap().0;
    assert_stale_gen_and_cursor_refusals(&addr, &analysis, stale_id, probe, page1.next_cursor)
        .await;

    let second = compact(&addr, Default::default()).await.unwrap();
    assert_eq!((second.wal_generation, second.tail_records_applied), (2, 0));
    let final_rows: Vec<Row> = tracked.lock().unwrap().live.values().cloned().collect();
    let live = final_rows.len() as u64;
    assert!(second.rows_before >= first.rows_after && second.rows_after == live);
    let h = shard_health(&addr).await;
    assert_eq!(
        (h.deleted_docs, h.live_docs, h.wal_generation),
        (0, live, 2)
    );
    assert_eq!(
        (h.document_slots, h.num_vectors, h.wal_clocked),
        (live, live, true)
    );

    let ctx = ReplayCheck {
        dir: &dir,
        index_path: &index_path,
        analysis: &analysis,
        cal: (&shift, &scale),
        addr: &addr,
        final_rows: &final_rows,
        queries: &queries,
    };
    verify_reopened_and_replayed_equivalence(ctx, handle).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn segmented_shard_compacts_online() {
    run_online_compaction().await;
}

async fn expect_refusal(
    result: Result<CompactShardResponse, tonic::Status>,
    code: tonic::Code,
    needle: &str,
) {
    let status = result.expect_err(&format!("expected a refusal mentioning {needle:?}"));
    assert_eq!(status.code(), code, "{}", status.message());
    assert!(
        status.message().contains(needle),
        "refusal {:?} does not mention {needle:?}",
        status.message()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refusals_name_the_cause_and_a_dry_run_writes_nothing() {
    let (analysis, _mock) = start_mock_analysis().await;
    let dir = tempdir("refusals");
    let sample = unit_vectors(64, DIM, SEED);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &sample);

    // An in-memory shard has no log.
    let (memory, memory_handle) = start_empty_node(NodeConfig {
        wal: false,
        ..config(None, &analysis)
    })
    .await;
    expect_refusal(
        compact(&memory, CompactShardRequest::default()).await,
        tonic::Code::FailedPrecondition,
        "needs a persisted shard",
    )
    .await;
    memory_handle.abort();

    // A persisted shard without a WAL.
    let (unlogged, unlogged_handle) = start_empty_node(NodeConfig {
        wal: false,
        ..config(Some(dir.join("unlogged.vector")), &analysis)
    })
    .await;
    expect_refusal(
        compact(&unlogged, CompactShardRequest::default()).await,
        tonic::Code::FailedPrecondition,
        "has no WAL",
    )
    .await;
    unlogged_handle.abort();

    // A shard with documents in its tail.
    let building_path = dir.join("building.vector");
    let (building, building_handle) =
        start_empty_node(config(Some(building_path.clone()), &analysis)).await;
    calibrate(&building, &shift, &scale).await;
    {
        let mut client = client(&building).await;
        append(&mut client, &row(1, false)).await;
    }
    client(&building)
        .await
        .flush(FlushRequest {})
        .await
        .unwrap();

    // A work directory that is not empty.
    let busy = dir.join("busy-work");
    std::fs::create_dir_all(&busy).unwrap();
    std::fs::write(busy.join("leftover"), b"x").unwrap();
    expect_refusal(
        compact(
            &building,
            CompactShardRequest {
                work_dir: busy.display().to_string(),
                ..Default::default()
            },
        )
        .await,
        tonic::Code::FailedPrecondition,
        "is not empty",
    )
    .await;

    // A dry run reports and writes nothing.
    {
        let mut client = client(&building).await;
        let (id, _) = append(&mut client, &row(2, false)).await;
        client
            .delete_documents(DeleteDocumentsRequest {
                doc_ids: vec![id],
                expected_wal_generation: None,
            })
            .await
            .unwrap();
        client.flush(FlushRequest {}).await.unwrap();
    }
    let dry = compact(
        &building,
        CompactShardRequest {
            dry_run: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(dry.dry_run);
    assert_eq!((dry.rows_before, dry.tombstones_reclaimed), (2, 1));
    assert_eq!(dry.wal_generation, 1);
    assert_eq!(dry.layout, "segments");
    assert!(!pipestream_search::compaction::default_work_dir(&building_path).exists());
    assert!(!wal_gen(&building_path, 1).exists());
    assert_eq!(
        client(&building)
            .await
            .health(HealthRequest {})
            .await
            .unwrap()
            .into_inner()
            .wal_generation,
        0
    );
    building_handle.abort();

    let _ = std::fs::remove_dir_all(&dir);
}

/// A shard large enough that its build outlasts a concurrent RPC.
async fn slow_shard(
    dir: &Path,
    name: &str,
    analysis: &str,
) -> (
    String,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    PathBuf,
) {
    let sample = unit_vectors(64, DIM, SEED);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &sample);
    let index_path = dir.join(name);
    let (addr, handle) = start_empty_node(config(Some(index_path.clone()), analysis)).await;
    calibrate(&addr, &shift, &scale).await;
    let mut client = client(&addr).await;
    for i in 0..4_000 {
        append(&mut client, &row(i, false)).await;
    }
    client
        .delete_documents(DeleteDocumentsRequest {
            doc_ids: vec![1, 2, 3],
            expected_wal_generation: None,
        })
        .await
        .unwrap();
    client.flush(FlushRequest {}).await.unwrap();
    (addr, handle, index_path)
}

/// The batched form of the legacy append: one AddDocuments stream for
/// `rows`, then one AddVectors stream for the same rows, unkeyed (ids
/// and slots align by order within the block).
async fn add_block(client: &mut NodeServiceClient<Channel>, rows: &[Row]) {
    let docs: Vec<AddDocumentsRequest> = rows
        .iter()
        .map(|row| AddDocumentsRequest {
            text: row.text.clone(),
            lineage: Some(DocLineage {
                parent_id: row.num as u64,
                ..Default::default()
            }),
            facets: vec![FacetValue {
                field: "grp".into(),
                value: row.grp.to_string(),
            }],
            integers: vec![IntegerValue {
                field: "num".into(),
                value: row.num,
            }],
            ..Default::default()
        })
        .collect();
    let added = client
        .add_documents(tokio_stream::iter(docs))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(added.added as usize, rows.len());
    let vectors: Vec<f32> = rows.iter().flat_map(|row| row.vector.clone()).collect();
    let added = client
        .add_vectors(tokio_stream::iter([AddVectorsRequest {
            materialized_rows: Vec::new(),
            vectors,
            dim: DIM as u32,
        }]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(added.added as usize, rows.len());
}

/// The rebuild driver's order on the segment layout: the backend is
/// configured first, then documents and vectors arrive in blocks. The
/// seal bound lands mid-block, so the tail must wait for the block's
/// vectors before sealing; every sealed segment then carries its
/// vectors and FP32 rows, and the flush succeeds. Every document first,
/// then every vector, seals document-only segments the vectors can never
/// join, which the first vector batch refuses by name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocks_seal_with_their_vectors_and_documents_first_is_refused_by_name() {
    let (analysis, _mock) = start_mock_analysis().await;
    let dir = tempdir("blocks");
    let sample = unit_vectors(64, DIM, SEED);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &sample);
    let rows: Vec<Row> = (0..320).map(|i| row(i, false)).collect();

    // Blocks of 64 with a seal bound of 100: the bound falls inside the
    // second block (128 documents against 64 vectors).
    let index_path = dir.join("blocks.vector");
    let (addr, handle) = start_empty_node(NodeConfig {
        seal_tail_docs: 100,
        ..config(Some(index_path.clone()), &analysis)
    })
    .await;
    calibrate(&addr, &shift, &scale).await;
    let mut blocks = client(&addr).await;
    for block in rows.chunks(64) {
        add_block(&mut blocks, block).await;
    }
    let health = blocks.health(HealthRequest {}).await.unwrap().into_inner();
    assert_eq!(health.num_vectors, 320);
    assert_eq!(health.bm25_docs, 320);
    let root = pipestream_search::node::segments_root(&index_path);
    let set = pipestream_search::segments::OpenedSegmentSet::open(&root).unwrap();
    assert!(
        set.len() >= 2,
        "the bound sealed segments during ingest: {}",
        set.len()
    );
    let mut sealed_rows = 0usize;
    for i in 0..set.len() {
        let image = set
            .vector(i)
            .unwrap_or_else(|| panic!("segment {i} sealed without its vectors"));
        assert_eq!(
            image.len(),
            set.metadata(i).rows as usize,
            "segment {i} rows carry vectors"
        );
        sealed_rows += set.metadata(i).rows as usize;
    }
    assert!(
        sealed_rows >= 200 && sealed_rows.is_multiple_of(64),
        "sealed at block edges: {sealed_rows}"
    );
    let flushed = blocks.flush(FlushRequest {}).await.unwrap().into_inner();
    assert_eq!(flushed.num_vectors, 320);
    assert_eq!(flushed.num_documents, 320);
    let exact = pipestream_search::exact_vectors::ExactVectorStore::open(
        &pipestream_search::node::exact_vector_sidecar_path(&index_path),
    );
    if let Ok(exact) = exact {
        assert_eq!(exact.len(), 320);
        exact.verify_payload().unwrap();
    }
    handle.abort();

    // Documents first, every one of them: the tail waits for their
    // vectors rather than sealing without them, whatever the bound. A
    // flush seals what the tail has, documents only, and the vectors
    // that arrive after that have no segment to join.
    let other_path = dir.join("docs-first.vector");
    let (addr, handle) = start_empty_node(NodeConfig {
        seal_tail_docs: 100,
        ..config(Some(other_path.clone()), &analysis)
    })
    .await;
    calibrate(&addr, &shift, &scale).await;
    let mut docs_first = client(&addr).await;
    for r in &rows[..250] {
        add_document(&mut docs_first, r).await;
    }
    let root = pipestream_search::node::segments_root(&other_path);
    assert!(
        pipestream_search::segments::OpenedSegmentSet::open(&root)
            .map_or(true, |set| set.is_empty()),
        "no document-only segment sealed on its own"
    );
    // The flush seals the document-only segment; its rows have no FP32
    // rows, and the exact view over the catalog says so.
    let flushed = docs_first
        .flush(FlushRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(flushed.num_documents, 250);
    let vectors: Vec<f32> = rows[..250].iter().flat_map(|r| r.vector.clone()).collect();
    let status = docs_first
        .add_vectors(tokio_stream::iter([AddVectorsRequest {
            materialized_rows: Vec::new(),
            vectors,
            dim: DIM as u32,
        }]))
        .await
        .expect_err("vectors after document-only seals");
    assert_eq!(
        status.code(),
        tonic::Code::FailedPrecondition,
        "{}",
        status.message()
    );
    assert!(
        status
            .message()
            .contains("sealed in segments without vectors"),
        "{}",
        status.message()
    );
    handle.abort();
}

/// A node that stopped without a flush (a refused shutdown flush, a
/// crash) never finalized its whole-shard FP32 sidecar, but the sealed
/// rows' FP32 files live in the segments: the next open shares them without
/// writing a whole-shard sidecar, and appends continue.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_reopened_without_a_flush_shares_its_exact_segment_images() {
    let (analysis, _mock) = start_mock_analysis().await;
    let dir = tempdir("reopen");
    let sample = unit_vectors(64, DIM, SEED);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &sample);
    let rows: Vec<Row> = (0..256).map(|i| row(i, false)).collect();
    let index_path = dir.join("reopen.vector");
    let (addr, handle) = start_empty_node(NodeConfig {
        seal_tail_docs: 64,
        ..config(Some(index_path.clone()), &analysis)
    })
    .await;
    calibrate(&addr, &shift, &scale).await;
    let mut first = client(&addr).await;
    for block in rows.chunks(64) {
        add_block(&mut first, block).await;
    }
    // Every block sealed (the bound equals the block); no flush ran.
    handle.abort();
    let exact_path = pipestream_search::node::exact_vector_sidecar_path(&index_path);
    assert!(!exact_path.exists(), "no flush, no finalized sidecar");

    let (addr, handle) = start_opened_node(config(Some(index_path.clone()), &analysis)).await;
    assert!(
        !exact_path.exists(),
        "reopening must not copy the full FP32 corpus"
    );
    let set = pipestream_search::segments::OpenedSegmentSet::open(
        pipestream_search::node::segments_root(&index_path),
    )
    .unwrap();
    let exact =
        pipestream_search::exact_vectors::ExactVectorStore::from_segments(&set, DIM).unwrap();
    assert_eq!(exact.len(), 256);
    exact.verify_payload().unwrap();
    assert_eq!(exact.row_values(5, 6).unwrap(), rows[5].vector);
    assert_eq!(exact.row_values(200, 201).unwrap(), rows[200].vector);

    let mut second = client(&addr).await;
    let more: Vec<Row> = (256..320).map(|i| row(i, false)).collect();
    add_block(&mut second, &more).await;
    let health = second.health(HealthRequest {}).await.unwrap().into_inner();
    assert_eq!(health.num_vectors, 320);
    assert_eq!(health.bm25_docs, 320);
    let flushed = second.flush(FlushRequest {}).await.unwrap().into_inner();
    assert_eq!(flushed.num_vectors, 320);
    assert!(
        !exact_path.exists(),
        "segment flush persists no redundant sidecar"
    );
    let set = pipestream_search::segments::OpenedSegmentSet::open(
        pipestream_search::node::segments_root(&index_path),
    )
    .unwrap();
    let exact =
        pipestream_search::exact_vectors::ExactVectorStore::from_segments(&set, DIM).unwrap();
    assert_eq!(exact.len(), 320);
    exact.verify_payload().unwrap();
    handle.abort();
}

/// A legacy two-RPC append caught between its calls holds the cut: the
/// compaction waits at the row boundary and, when the row never completes,
/// refuses naming the counts; once the vector lands, the same call runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mid_row_append_refuses_the_cut_until_the_row_completes() {
    let (analysis, _mock) = start_mock_analysis().await;
    let dir = tempdir("mid_row");
    let sample = unit_vectors(64, DIM, SEED);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &sample);
    let index_path = dir.join("shard.vector");
    let (addr, handle) = start_empty_node(config(Some(index_path.clone()), &analysis)).await;
    calibrate(&addr, &shift, &scale).await;
    let mut client = client(&addr).await;
    for i in 0..2 {
        append(&mut client, &row(i, false)).await;
    }
    let half = row(2, false);
    add_document(&mut client, &half).await;

    expect_refusal(
        compact(&addr, CompactShardRequest::default()).await,
        tonic::Code::FailedPrecondition,
        "the tail has 3 documents and 2 vectors",
    )
    .await;

    add_vector(&mut client, &half).await;
    let done = compact(&addr, CompactShardRequest::default())
        .await
        .unwrap();
    assert!(!done.dry_run);
    assert_eq!(done.rows_before, 3);
    assert_eq!(done.rows_after, 3);
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_compaction_and_a_snapshot_install_during_one_refuse_by_name() {
    let (analysis, _mock) = start_mock_analysis().await;
    let dir = tempdir("concurrent");
    let (addr, handle, index_path) = slow_shard(&dir, "shard.vector", &analysis).await;

    // Two at once: the second refuses while the first runs.
    let first = tokio::spawn({
        let addr = addr.clone();
        async move { compact(&addr, CompactShardRequest::default()).await }
    });
    tokio::time::sleep(Duration::from_millis(40)).await;
    expect_refusal(
        compact(&addr, CompactShardRequest::default()).await,
        tonic::Code::FailedPrecondition,
        "already running",
    )
    .await;
    let first = first.await.unwrap().unwrap();
    assert_eq!(first.tombstones_reclaimed, 3);
    assert_eq!(first.wal_generation, 1);

    // A snapshot installed under a running compaction rotates the WAL;
    // the compaction aborts by name and the shard serves the snapshot.
    let repository = dir.join("repository");
    client(&addr)
        .await
        .export_snapshot(pipestream_search::pb::ExportSnapshotRequest {
            directory: repository.display().to_string(),
        })
        .await
        .unwrap();
    let running = tokio::spawn({
        let addr = addr.clone();
        async move { compact(&addr, CompactShardRequest::default()).await }
    });
    tokio::time::sleep(Duration::from_millis(40)).await;
    client(&addr)
        .await
        .install_snapshot_from(pipestream_search::pb::InstallSnapshotFromRequest {
            source: Some(
                pipestream_search::pb::install_snapshot_from_request::Source::Directory(
                    repository.display().to_string(),
                ),
            ),
            ..Default::default()
        })
        .await
        .unwrap();
    let aborted = running.await.unwrap();
    expect_refusal(aborted, tonic::Code::Aborted, "snapshot was installed").await;
    let health = client(&addr)
        .await
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(health.wal_generation, 2);
    assert_eq!(health.live_docs, 3_997);
    assert!(!pipestream_search::compaction::marker_path(&index_path).exists());
    // The snapshot's generation is partial history: compaction refuses
    // it by name rather than dropping the image.
    expect_refusal(
        compact(&addr, CompactShardRequest::default()).await,
        tonic::Code::FailedPrecondition,
        "preexisting",
    )
    .await;
    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// The partitioned layout (docs/immutable-segments.md "Partitioned layout")
// and the column summaries a seal records ("Segment summaries").

fn partition_request(column: &str, bound: u32) -> CompactShardRequest {
    CompactShardRequest {
        partition_column: column.into(),
        tail_bound: bound,
        ..Default::default()
    }
}

fn read_manifest(index_path: &Path) -> SegmentSetManifest {
    let root = pipestream_search::node::segments_root(index_path);
    SegmentCatalog::read_manifest(&root).unwrap().unwrap()
}

fn int_summary(segment: &SegmentMetadata, column: &str) -> (i64, i64, u64) {
    let summary = segment
        .summary
        .as_ref()
        .unwrap_or_else(|| panic!("segment {} has no summary", segment.segment_id));
    let c = summary
        .int_columns
        .iter()
        .find(|c| c.name == column)
        .unwrap_or_else(|| panic!("segment {} has no {column} summary", segment.segment_id));
    (c.min, c.max, c.present)
}

/// The compaction outputs whose id starts with `prefix`, in base order:
/// each within `bound` rows, the keyed ones ascending and disjoint by
/// `num` with a partition range equal to the segment's own `num` range,
/// the unkeyed ones after them. Returns `(keyed, unkeyed)` segments.
fn check_partitions(
    manifest: &SegmentSetManifest,
    prefix: &str,
    bound: u64,
) -> (Vec<SegmentMetadata>, Vec<SegmentMetadata>) {
    assert_eq!(manifest.partition_key.as_deref(), Some("num"));
    let outputs: Vec<&SegmentMetadata> = manifest
        .segments
        .iter()
        .filter(|s| s.segment_id.starts_with(prefix))
        .collect();
    assert!(!outputs.is_empty(), "no outputs with prefix {prefix}");
    let mut keyed = Vec::new();
    let mut unkeyed = Vec::new();
    let mut previous_hi: Option<i64> = None;
    for segment in outputs {
        assert!(
            segment.rows <= bound,
            "segment {} holds {} rows over the bound {bound}",
            segment.segment_id,
            segment.rows
        );
        let (min, max, present) = int_summary(segment, "num");
        let partition = segment.summary.as_ref().unwrap().partition.as_ref();
        match partition {
            Some(range) => {
                assert!(unkeyed.is_empty(), "a keyed segment after an unkeyed one");
                assert_eq!(range.column, "num");
                assert_eq!((range.lo, range.hi), (min, max));
                assert_eq!(
                    present, segment.rows,
                    "a keyed segment holds keyed rows only"
                );
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
                keyed.push(segment.clone());
            }
            None => {
                assert_eq!(present, 0, "an unkeyed segment holds no keyed row");
                assert!(min > max, "an absent column has an inverted range");
                unkeyed.push(segment.clone());
            }
        }
    }
    (keyed, unkeyed)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_segmented_shard_compacts_into_num_partitions() {
    let (analysis, _mock) = start_mock_analysis().await;
    let dir = tempdir("partition");
    let sample = unit_vectors(256, DIM, SEED);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &sample);
    let index_path = dir.join("shard.vector");
    let (addr, mut handle) = start_empty_node(config(Some(index_path.clone()), &analysis)).await;
    let tracked = seed_shard(&addr, &shift, &scale).await;
    let queries: Vec<Vec<f32>> = (0..3)
        .map(|q| unit_vectors(1, DIM, 0x5E61_0000 + q))
        .collect();
    let live_rows = tracked.live.len() as u64;

    let bound = 64u32;
    let first = compact(&addr, partition_request("num", bound))
        .await
        .unwrap();
    eprintln!("partitioned compaction #1: {first:?}");
    assert_eq!(first.partition_column, "num");
    assert_eq!(first.layout, "segments");
    assert_eq!(first.rows_after, live_rows);
    let manifest = read_manifest(&index_path);
    let (keyed, unkeyed) = check_partitions(&manifest, "cmp-000001-", u64::from(bound));
    assert!(unkeyed.is_empty(), "every seeded row carries num");
    assert!(keyed.len() >= (live_rows / u64::from(bound)) as usize);
    assert_eq!(keyed.iter().map(|s| s.rows).sum::<u64>(), live_rows);
    // Each segment's rows are in ascending num order: the browse by id
    // over one segment's range returns ascending nums.
    {
        let mut node = client(&addr).await;
        let segment = &keyed[keyed.len() / 2];
        let ids: Vec<u64> = (segment.base_label..segment.base_label + segment.rows).collect();
        let fetched = node
            .get_documents(GetDocumentsRequest { doc_ids: ids })
            .await
            .unwrap()
            .into_inner();
        let nums: Vec<i64> = fetched
            .documents
            .iter()
            .map(|d| {
                let text = &d.text;
                let n: usize = text
                    .trim_start_matches("row")
                    .split(' ')
                    .next()
                    .unwrap()
                    .parse()
                    .unwrap();
                n as i64 * 2 + i64::from(text.ends_with("revised"))
            })
            .collect();
        let mut sorted = nums.clone();
        sorted.sort_unstable();
        assert_eq!(nums, sorted, "rows inside a segment are in key order");
        let (min, max, _) = int_summary(segment, "num");
        assert_eq!((nums[0], *nums.last().unwrap()), (min, max));
    }

    // Every read path equals a shard built fresh from the same rows.
    let final_rows: Vec<Row> = tracked.live.values().cloned().collect();
    let (reference, reference_handle) = reference_shard(
        &dir,
        "reference.vector",
        &analysis,
        &shift,
        &scale,
        &final_rows,
    )
    .await;
    let expected = observe(std::slice::from_ref(&reference), &analysis, &queries).await;
    let got = observe(std::slice::from_ref(&addr), &analysis, &queries).await;
    assert_eq!(
        got, expected,
        "partitioned shard differs from the reference"
    );

    // Reopen from disk: the key and the ranges survive, the reads equal.
    handle.abort();
    let _ = (&mut handle).await;
    let (reopened, reopened_handle) =
        start_opened_node(config(Some(index_path.clone()), &analysis)).await;
    let manifest = read_manifest(&index_path);
    check_partitions(&manifest, "cmp-000001-", u64::from(bound));
    let got = observe(std::slice::from_ref(&reopened), &analysis, &queries).await;
    assert_eq!(got, expected, "reopened partitioned shard differs");

    // A later seal appends an unordered tail segment and leaves the key.
    let extra = row(N + 7, false);
    let mut node = client(&reopened).await;
    append(&mut node, &extra).await;
    node.flush(FlushRequest {}).await.unwrap();
    let manifest = read_manifest(&index_path);
    assert_eq!(manifest.partition_key.as_deref(), Some("num"));
    let tail: Vec<&SegmentMetadata> = manifest
        .segments
        .iter()
        .filter(|s| !s.segment_id.starts_with("cmp-000001-"))
        .collect();
    assert_eq!(tail.len(), 1, "one sealed tail segment");
    assert_eq!(tail[0].rows, 1);
    assert!(tail[0].summary.as_ref().unwrap().partition.is_none());
    assert_eq!(int_summary(tail[0], "num"), (extra.num, extra.num, 1));

    // The next partitioned compaction folds the tail in.
    let second = compact(&reopened, partition_request("num", bound))
        .await
        .unwrap();
    assert_eq!(second.partition_column, "num");
    assert_eq!(second.rows_after, live_rows + 1);
    let manifest = read_manifest(&index_path);
    let (keyed, unkeyed) = check_partitions(&manifest, "cmp-000002-", u64::from(bound));
    assert!(unkeyed.is_empty());
    assert_eq!(
        keyed.len(),
        manifest.segments.len(),
        "no other segment remains"
    );
    assert_eq!(keyed.iter().map(|s| s.rows).sum::<u64>(), live_rows + 1);

    // And the bucket layout is back on request: no key, no ranges.
    let third = compact(&reopened, CompactShardRequest::default())
        .await
        .unwrap();
    assert_eq!(third.partition_column, "");
    let manifest = read_manifest(&index_path);
    assert_eq!(manifest.partition_key, None);
    assert!(manifest
        .segments
        .iter()
        .all(|s| s.summary.as_ref().unwrap().partition.is_none()));
    let got = observe(std::slice::from_ref(&reopened), &analysis, &queries).await;
    let mut node = client(&reference).await;
    append(&mut node, &extra).await;
    node.flush(FlushRequest {}).await.unwrap();
    let expected = observe(std::slice::from_ref(&reference), &analysis, &queries).await;
    assert_eq!(
        got, expected,
        "the bucket layout differs from the reference"
    );

    reopened_handle.abort();
    reference_handle.abort();
}

/// A document with no `num`: it has a vector and a facet, and no
/// integer value at all.
async fn append_without_num(client: &mut NodeServiceClient<Channel>, i: usize) -> Row {
    let mut bare = row(i, false);
    bare.key = format!("bare/{i}");
    bare.text = format!("bare{i} common {}", bare.grp);
    let doc = AddDocumentsRequest {
        text: bare.text.clone(),
        facets: vec![FacetValue {
            field: "grp".into(),
            value: bare.grp.to_string(),
        }],
        ..Default::default()
    };
    let mut request = Request::new(tokio_stream::iter([doc]));
    request.metadata_mut().insert_bin(
        pipestream_search::replication::STABLE_KEYS_METADATA,
        stable_metadata(bare.key.as_bytes()),
    );
    client.add_documents(request).await.unwrap();
    add_vector(client, &bare).await;
    bare
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rows_without_the_partition_column_seal_apart() {
    let (analysis, _mock) = start_mock_analysis().await;
    let dir = tempdir("partition-unkeyed");
    let sample = unit_vectors(64, DIM, SEED);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &sample);
    let index_path = dir.join("shard.vector");
    let (addr, _handle) = start_empty_node(config(Some(index_path.clone()), &analysis)).await;
    calibrate(&addr, &shift, &scale).await;
    let mut node = client(&addr).await;
    // Keyed and unkeyed rows interleaved, so the unkeyed ones are spread
    // across the WAL buckets.
    let keyed_rows = 150usize;
    let unkeyed_rows = 20usize;
    for i in 0..keyed_rows + unkeyed_rows {
        if i % 8 == 3 && i / 8 < unkeyed_rows {
            append_without_num(&mut node, 10_000 + i).await;
        } else {
            append(&mut node, &row(i, false)).await;
        }
    }
    node.flush(FlushRequest {}).await.unwrap();
    let total = (keyed_rows + unkeyed_rows) as u64;
    let unkeyed_count = (0..keyed_rows + unkeyed_rows)
        .filter(|i| i % 8 == 3 && i / 8 < unkeyed_rows)
        .count() as u64;
    let queries: Vec<Vec<f32>> = (0..2)
        .map(|q| unit_vectors(1, DIM, 0x7A11_0000 + q))
        .collect();
    let before = observe(std::slice::from_ref(&addr), &analysis, &queries).await;

    let bound = 40u32;
    let response = compact(&addr, partition_request("num", bound))
        .await
        .unwrap();
    assert_eq!(response.rows_after, total);
    let manifest = read_manifest(&index_path);
    let (keyed, unkeyed) = check_partitions(&manifest, "cmp-000001-", u64::from(bound));
    assert_eq!(unkeyed.len(), 1, "one unkeyed segment under the bound");
    assert_eq!(unkeyed[0].rows, unkeyed_count);
    assert_eq!(
        keyed.iter().map(|s| s.rows).sum::<u64>(),
        total - unkeyed_count
    );
    // The unkeyed segment holds exactly the bare rows.
    let ids: Vec<u64> = (unkeyed[0].base_label..unkeyed[0].base_label + unkeyed[0].rows).collect();
    let fetched = node
        .get_documents(GetDocumentsRequest { doc_ids: ids })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched.documents.len() as u64, unkeyed_count);
    assert!(fetched.documents.iter().all(|d| d.text.starts_with("bare")));
    let after = observe(std::slice::from_ref(&addr), &analysis, &queries).await;
    assert_eq!(after, before, "the partitioned shard reads differently");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partitioned_compaction_rejects_the_wrong_column() {
    let (analysis, _mock) = start_mock_analysis().await;
    let dir = tempdir("partition-refusals");
    let sample = unit_vectors(64, DIM, SEED);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &sample);

    let (segmented, _segmented_handle) =
        start_empty_node(config(Some(dir.join("segmented.vector")), &analysis)).await;
    calibrate(&segmented, &shift, &scale).await;
    let mut node = client(&segmented).await;
    for i in 0..40 {
        append(&mut node, &row(i, false)).await;
    }
    node.flush(FlushRequest {}).await.unwrap();
    expect_refusal(
        compact(&segmented, partition_request("grp", 16)).await,
        tonic::Code::InvalidArgument,
        "a facet column",
    )
    .await;
    expect_refusal(
        compact(&segmented, partition_request("score", 16)).await,
        tonic::Code::InvalidArgument,
        "a double column",
    )
    .await;
    expect_refusal(
        compact(&segmented, partition_request("nope", 16)).await,
        tonic::Code::InvalidArgument,
        "not a column of this shard",
    )
    .await;
    // An integer column of the table no document carries.
    expect_refusal(
        compact(&segmented, partition_request("other", 16)).await,
        tonic::Code::FailedPrecondition,
        "no document of this shard carries it",
    )
    .await;
    // The refusals wrote nothing: the seeded segment is the only one.
    let manifest = read_manifest(&dir.join("segmented.vector"));
    assert_eq!(manifest.segments.len(), 1);
    assert_eq!(manifest.partition_key, None);
    // A dry run echoes the column and writes nothing.
    let dry = compact(
        &segmented,
        CompactShardRequest {
            dry_run: true,
            ..partition_request("num", 16)
        },
    )
    .await
    .unwrap();
    assert!(dry.dry_run);
    assert_eq!(dry.partition_column, "num");
    assert_eq!(
        read_manifest(&dir.join("segmented.vector")).segments.len(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_plain_seal_records_column_summaries() {
    let (analysis, _mock) = start_mock_analysis().await;
    let dir = tempdir("summaries");
    let sample = unit_vectors(64, DIM, SEED);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &sample);
    let index_path = dir.join("shard.vector");
    let (addr, _handle) = start_empty_node(NodeConfig {
        seal_tail_docs: 100,
        ..config(Some(index_path.clone()), &analysis)
    })
    .await;
    calibrate(&addr, &shift, &scale).await;
    let mut node = client(&addr).await;
    let rows = 250usize;
    for i in 0..rows {
        // Shuffled nums: (i * 37) mod 251 is a permutation of 0..251.
        let mut r = row(i, false);
        r.num = (i as i64 * 37) % 251;
        r.key = format!("shuffled/{i}");
        append(&mut node, &r).await;
    }
    node.flush(FlushRequest {}).await.unwrap();
    let manifest = read_manifest(&index_path);
    assert!(
        manifest.segments.len() >= 3,
        "{} segments",
        manifest.segments.len()
    );
    assert_eq!(manifest.partition_key, None);
    let mut present_total = 0;
    for segment in &manifest.segments {
        let summary = segment.summary.as_ref().expect("a seal writes a summary");
        assert!(summary.partition.is_none());
        let (min, max, present) = int_summary(segment, "num");
        assert_eq!(present, segment.rows);
        assert!(min <= max);
        assert!((0..251).contains(&min) && (0..251).contains(&max));
        present_total += present;
        // The range is the segment's own rows, not the shard's.
        let ids: Vec<u64> = (segment.base_label..segment.base_label + segment.rows).collect();
        let fetched = node
            .get_documents(GetDocumentsRequest { doc_ids: ids })
            .await
            .unwrap()
            .into_inner();
        let nums: Vec<i64> = fetched
            .documents
            .iter()
            .map(|d| {
                let n: i64 = d
                    .text
                    .trim_start_matches("row")
                    .split(' ')
                    .next()
                    .unwrap()
                    .parse()
                    .unwrap();
                (n * 37) % 251
            })
            .collect();
        assert_eq!(nums.iter().min().copied(), Some(min));
        assert_eq!(nums.iter().max().copied(), Some(max));
        let (omin, omax, opresent) = int_summary(segment, "other");
        assert_eq!(opresent, 0);
        assert!(omin > omax, "an absent int column has an inverted range");
        let score = summary
            .numeric_columns
            .iter()
            .find(|c| c.name == "score")
            .expect("the double column is summarized");
        assert_eq!(score.present, 0);
        assert!(
            score.min > score.max,
            "an absent double column has an inverted range"
        );
    }
    assert_eq!(present_total, rows as u64);
}
