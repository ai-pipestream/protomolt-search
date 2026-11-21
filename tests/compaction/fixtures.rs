//! Shared shard seeding, concurrent mutation writer, and compaction RPC helpers.

use super::*;

/// Rows ingested before the first compaction.
pub(crate) const N: usize = 1_200;
pub(crate) const GROUPS: [&str; 3] = ["red", "green", "blue"];
pub(crate) const EXTRAS: [&str; 4] = ["one", "two", "three", "four"];
pub(crate) const SEED: u64 = 0xC0A5_7A11;

pub(crate) fn config(index_path: Option<PathBuf>, analysis: &str) -> NodeConfig {
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
pub(crate) struct Row {
    pub(crate) key: String,
    pub(crate) text: String,
    pub(crate) num: i64,
    pub(crate) grp: &'static str,
    pub(crate) vector: Vec<f32>,
}

pub(crate) fn row(i: usize, revised: bool) -> Row {
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

pub(crate) async fn client(addr: &str) -> NodeServiceClient<Channel> {
    NodeServiceClient::connect(addr.to_string()).await.unwrap()
}

pub(crate) async fn calibrate(addr: &str, shift: &[f32], scale: &[f32]) {
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
pub(crate) async fn append(client: &mut NodeServiceClient<Channel>, row: &Row) -> (u64, u64) {
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
pub(crate) fn original_source(row: &Row) -> (pipestream_search::pb::ProtobufSource, Option<u32>) {
    let parent = format!("parent/{}", row.num / 4);
    let version = format!("version/{}", row.num % 2);
    (
        common::protobuf_source(&parent, &version),
        Some(((row.num / 2) % 2) as u32),
    )
}

pub(crate) fn logical_identity(row: &Row) -> pipestream_search::pb::DocumentIdentity {
    pipestream_search::pb::DocumentIdentity {
        document_key: format!("parent/{}", row.num / 4).into_bytes(),
        version: (row.num % 2) as u64 + 1,
        chunk_ordinal: original_source(row).1,
    }
}

/// The document half of a legacy two-RPC append, keyed.
pub(crate) async fn add_document(
    client: &mut NodeServiceClient<Channel>,
    row: &Row,
) -> AddDocumentsResponse {
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

pub(crate) fn stable_metadata(
    key: &[u8],
) -> tonic::metadata::MetadataValue<tonic::metadata::Binary> {
    let encoded = pipestream_search::replication::encode_stable_keys(&[key.to_vec()]).unwrap();
    tonic::metadata::MetadataValue::from_bytes(&encoded)
}

/// The vector half of a legacy two-RPC append, keyed.
pub(crate) async fn add_vector(
    client: &mut NodeServiceClient<Channel>,
    row: &Row,
) -> AddVectorsResponse {
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
pub(crate) struct Tracked {
    pub(crate) live: BTreeMap<String, Row>,
    /// `(global id, WAL generation the id belongs to)`.
    pub(crate) ids: BTreeMap<String, (u64, u64)>,
}

pub(crate) async fn seed_shard(addr: &str, shift: &[f32], scale: &[f32]) -> Tracked {
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
pub(crate) struct WriterStats {
    pub(crate) calls: u64,
    pub(crate) max_call: Duration,
    /// Id-addressed mutations the writer withheld or the node refused
    /// because the cutover renumbered the rows they named.
    pub(crate) stale: u64,
}

pub(crate) fn is_stale_generation(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::FailedPrecondition
        && status.message().contains("stale WAL generation")
}

/// Appends the writer makes at most. The stop flag ends it first on an
/// unloaded machine (about 700 appends); the cap is what makes the
/// compaction's tail loop converge on any machine, because a loop that
/// exits on "fewer than tail_bound records arrived during a pass" only
/// terminates against a writer that stops, and this one stops after a
/// count, not after a time.
pub(crate) const WRITER_APPEND_CAP: usize = 1_500;

/// Keep writing through the public RPCs until told to stop or the append
/// cap is reached: appends, with every fifth append followed by a delete
/// of an earlier one and every seventh by a replacement. Tracks the
/// product's view.
pub(crate) async fn writer(
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

pub(crate) async fn compact(
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

pub(crate) async fn expect_refusal(
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

pub(crate) fn read_manifest(index_path: &Path) -> SegmentSetManifest {
    let root = pipestream_search::node::segments_root(index_path);
    SegmentCatalog::read_manifest(&root).unwrap().unwrap()
}

pub(crate) fn int_summary(segment: &SegmentMetadata, column: &str) -> (i64, i64, u64) {
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

pub(crate) fn wal_gen(index_path: &Path, generation: u64) -> PathBuf {
    pipestream_search::wal::gen_dir(&pipestream_search::wal::wal_dir(index_path), generation)
}
