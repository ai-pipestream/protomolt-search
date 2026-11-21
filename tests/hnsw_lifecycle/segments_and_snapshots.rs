//! Mapped HNSW segment serving, delete/replacement retirement, and snapshot recovery.

use super::*;

/// Ingest, seal, delete, reopen mapped: two sealed hnsw segments plus a
/// tail answer explicit ANN and the streaming scan with exact scores over
/// live rows only. Sealed, the catalog answers identically after a reopen
/// through the map and through the heap (the images are the graphs; an
/// unsealed tail is volatile without a log, as on every provider).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sealed_segments_serve_mapped_and_exclude_deleted_rows() {
    let dir = tempdir("sealed");
    let path = dir.join("shard.hnsw");
    let (addr, handle) = start_empty_node(config(&path, true, false)).await;
    let mut live: BTreeMap<u64, Row> = BTreeMap::new();
    for (batch, range) in [0..300, 300..600, 600..700].into_iter().enumerate() {
        let batch_rows = rows(range.clone());
        let first = ingest_rows(&addr, &batch_rows).await;
        assert_eq!(first, range.start as u64);
        for (i, row) in batch_rows.into_iter().enumerate() {
            live.insert(first + i as u64, row);
        }
        if batch < 2 {
            flush(&addr).await;
        }
    }
    // Deletes land in both sealed segments and in the tail.
    let doomed: Vec<u64> = (0..700u64).filter(|id| id % 9 == 4).collect();
    let deleted = client(&addr)
        .await
        .delete_documents(DeleteDocumentsRequest {
            doc_ids: doomed.clone(),
            expected_wal_generation: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(deleted.deleted as usize, doomed.len());
    for id in &doomed {
        live.remove(id);
    }
    let live_tail_rows = (600..700u64).filter(|id| live.contains_key(id)).count() as u64;
    assert!(live_tail_rows > 0);
    let probes = queries(6, 0x51A1_0000);
    let c = coordinator(&addr);
    for (q, query) in probes.iter().enumerate() {
        let (hits, tail_enum) = ann_with_tail_stats(&c, query, K).await;
        assert_eq!(
            tail_enum, live_tail_rows,
            "live tail rows (600..700) must be enumerated until sealed"
        );
        check(&hits, &live, query, 8, &format!("live query {q}"));
        let stream = streamed(&c, &format!("live-{q}"), query, K).await;
        check(&stream, &live, query, 8, &format!("live stream {q}"));
    }
    // Seal the 100-row tail too: because 100 < MIN_GRAPH_ROWS (256), segment 2
    // persists as a compact unlinked PMHNSW01 image (`entry = NO_NODE`) while
    // segments 0 and 1 (300 rows each) serve HNSW graphs, and reopen serves
    // those images as written.
    flush(&addr).await;
    let mut before = Vec::new();
    for (q, query) in probes.iter().enumerate() {
        let (unary, tail_enum) = ann_with_tail_stats(&c, query, K).await;
        assert_eq!(
            tail_enum, live_tail_rows,
            "tiny sealed segment (< MIN_GRAPH_ROWS) enumerates its live rows"
        );
        check(&unary, &live, query, 8, &format!("sealed query {q}"));
        let stream = streamed(&c, &format!("sealed-{q}"), query, K).await;
        check(&stream, &live, query, 8, &format!("sealed stream {q}"));
        before.push((unary, stream));
    }
    handle.abort();

    let root = segments_root(&path);
    let mapped = OpenedSegmentSet::open_with(&root, VectorLoad::Mapped).unwrap();
    assert_eq!(mapped.len(), 3);
    for i in 0..3 {
        let vector = mapped.vector(i).unwrap();
        assert!(vector.is_mapped());
        assert_eq!(vector.descriptor().backend_kind, HNSW);
        assert_eq!(mapped.metadata(i).backend_kind, HNSW);
        assert_eq!(vector.is_graph_linked(), i < 2, "segment {i} linked state");
    }
    let heap = OpenedSegmentSet::open_with(&root, VectorLoad::Heap).unwrap();
    assert!(!heap.vector(0).unwrap().is_mapped());
    assert!(heap.vector(0).unwrap().is_graph_linked());
    assert!(heap.vector(1).unwrap().is_graph_linked());
    assert!(!heap.vector(2).unwrap().is_graph_linked());

    // Reopened either way, every answer is the one given before.
    for mmap in [true, false] {
        let (addr, handle) = start_opened_node(config(&path, mmap, false)).await;
        let c = coordinator(&addr);
        for (q, query) in probes.iter().enumerate() {
            let unary = ann(&c, query, K).await;
            let stream = streamed(&c, &format!("reopen-{mmap}-{q}"), query, K).await;
            let bits = |hits: &[(u64, f32)]| -> Vec<(u64, u32)> {
                hits.iter().map(|(id, s)| (*id, s.to_bits())).collect()
            };
            check(&unary, &live, query, 8, &format!("reopen {mmap} query {q}"));
            assert_eq!(bits(&unary), bits(&before[q].0), "mmap {mmap} query {q}");
            assert_eq!(bits(&stream), bits(&before[q].1), "mmap {mmap} stream {q}");
        }
        handle.abort();
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// A document replacement retires the old row from every dense answer and
/// serves the revision, in the tail and after the seal that follows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replacements_retire_the_old_row_and_serve_the_revision() {
    let dir = tempdir("replace");
    let path = dir.join("shard.hnsw");
    let (addr, handle) = start_empty_node(config(&path, true, false)).await;
    let base = rows(0..240);
    assert_eq!(ingest_rows(&addr, &base).await, 0);
    flush(&addr).await;
    let mut live: BTreeMap<u64, Row> = base
        .iter()
        .cloned()
        .enumerate()
        .map(|(i, r)| (i as u64, r))
        .collect();

    let replaced = [7usize, 100, 239];
    let revisions: Vec<Row> = replaced.iter().map(|&i| row(i, true)).collect();
    let first_new = ingest_rows(&addr, &revisions).await;
    assert_eq!(first_new, 240);
    let committed = client(&addr)
        .await
        .commit_replacements(CommitReplacementsRequest {
            replacements: replaced
                .iter()
                .enumerate()
                .map(|(n, &old)| Replacement {
                    old_doc_id: old as u64,
                    new_doc_id: first_new + n as u64,
                })
                .collect(),
            expected_wal_generation: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(committed.committed as usize, replaced.len());
    for (n, &old) in replaced.iter().enumerate() {
        live.remove(&(old as u64));
        live.insert(first_new + n as u64, revisions[n].clone());
    }

    // The old row's own vector no longer finds the old row; the revision's
    // finds the revision first.
    let c = coordinator(&addr);
    for (n, &old) in replaced.iter().enumerate() {
        let hits = ann(&c, &base[old].vector, K).await;
        check(
            &hits,
            &live,
            &base[old].vector,
            8,
            &format!("old row {old}"),
        );
        assert!(hits.iter().all(|(id, _)| *id != old as u64));
        let hits = ann(&c, &revisions[n].vector, K).await;
        assert_eq!(hits[0].0, first_new + n as u64, "revision {n} leads");
        let stream = streamed(&c, &format!("rev-{n}"), &revisions[n].vector, K).await;
        assert_eq!(stream[0].0, first_new + n as u64);
        check(
            &stream,
            &live,
            &revisions[n].vector,
            8,
            &format!("revision stream {n}"),
        );
    }
    // Sealed and reopened, the same holds.
    flush(&addr).await;
    handle.abort();
    let (addr, handle) = start_opened_node(config(&path, true, false)).await;
    assert_eq!(
        OpenedSegmentSet::open(segments_root(&path)).unwrap().len(),
        2
    );
    let c = coordinator(&addr);
    for (n, &old) in replaced.iter().enumerate() {
        let hits = ann(&c, &base[old].vector, K).await;
        check(
            &hits,
            &live,
            &base[old].vector,
            8,
            &format!("sealed old row {old}"),
        );
        assert!(hits.iter().all(|(id, _)| *id != old as u64));
        let hits = ann(&c, &revisions[n].vector, K).await;
        assert_eq!(hits[0].0, first_new + n as u64);
    }
    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// The node's own shard search (the coordinator's unary leg).
async fn search_shard(
    client: &mut NodeServiceClient<Channel>,
    vector: Vec<f32>,
    k: u32,
) -> Vec<ScoredHit> {
    let (tx, rx) = mpsc::channel(8);
    tx.send(SearchShardRequest {
        payload: Some(search_shard_request::Payload::Start(StartShardSearch {
            request_id: "lifecycle".into(),
            k,
            vector,
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    drop(tx);
    let mut stream = client
        .search_shard(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    let mut done = None;
    while let Some(msg) = stream.message().await.unwrap() {
        if let Some(search_shard_response::Payload::Done(d)) = msg.payload {
            done = Some(d);
        }
    }
    done.expect("stream must end with SearchShardDone").hits
}

/// Each probe's shard top-k as `(global id, score bits)`.
async fn observe_shard(addr: &str, probes: &[Vec<f32>]) -> Vec<Vec<(u64, u32)>> {
    let mut client = client(addr).await;
    let mut out = Vec::new();
    for q in probes {
        let hits = search_shard(&mut client, q.clone(), K as u32).await;
        out.push(
            hits.iter()
                .map(|h| (h.vector_id, h.score.to_bits()))
                .collect::<Vec<_>>(),
        );
    }
    out
}

/// An hnsw catalog exported from one shard installs on another through a
/// repository, serves the source's own top-k with its exact sidecar, and is
/// what the target reopens.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_snapshot_of_the_graph_installs_and_survives_a_restart() {
    let dir = tempdir("snapshot");
    let n = 500;
    let at = |name: &str| NodeConfig {
        slot_offset: 100,
        ..config(&dir.join(name), true, false)
    };
    let (source, source_handle) = start_empty_node(at("source.hnsw")).await;
    configure(&source).await;
    ingest_rows(&source, &rows(0..n)).await;
    flush(&source).await;
    let repository = dir.join("repository");
    client(&source)
        .await
        .export_snapshot(ExportSnapshotRequest {
            directory: repository.display().to_string(),
        })
        .await
        .unwrap();
    let probes = queries(4, 0x5A4B_1000);
    let expected = observe_shard(&source, &probes).await;
    source_handle.abort();

    let (addr, handle) = start_empty_node(at("shard.hnsw")).await;
    let report = client(&addr)
        .await
        .install_snapshot_from(InstallSnapshotFromRequest {
            source: Some(install_snapshot_from_request::Source::Directory(
                repository.display().to_string(),
            )),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(report.num_vectors, n as u64);
    assert_eq!(report.num_documents, n as u64);
    let installed = observe_shard(&addr, &probes).await;
    assert_eq!(installed, expected);
    let health = health(&addr).await;
    assert_eq!(health.vector_backend, HNSW);
    assert_eq!(health.exact_vector_rows, n as u64);
    handle.abort();

    let (addr, handle) = start_opened_node(at("shard.hnsw")).await;
    assert_eq!(observe_shard(&addr, &probes).await, installed);
    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
