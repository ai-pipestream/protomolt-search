//! Online compaction parity on segmented shards under live writes.

use super::*;

async fn shard_health(addr: &str) -> pipestream_search::pb::HealthResponse {
    client(addr)
        .await
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner()
}

fn dense_cursor_query(probe: Vec<f32>, cursor: String) -> QueryRequest {
    let mode = pipestream_search::pb::DenseExecutionMode::Exhaustive as i32;
    QueryRequest {
        k: 1,
        selection: Some(common::dense_selection("dense", probe, mode, 0)),
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
    assert!(first.tail_records_applied > 0, "{first:?}");
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
