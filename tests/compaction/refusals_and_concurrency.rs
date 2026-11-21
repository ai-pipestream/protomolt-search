//! Compaction refusal table, dry-run, mid-row cut refusal, and snapshot conflict.

use super::*;

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
