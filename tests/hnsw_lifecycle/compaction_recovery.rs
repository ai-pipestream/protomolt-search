//! Online HNSW graph rebuild during compaction and interrupted publication rollback.

use super::*;

/// Hits as `(text, score bits)`, so answers compare across the renumbering
/// a compaction performs.
async fn dense_by_text(addr: &str, probes: &[Vec<f32>]) -> Vec<Vec<(String, u32)>> {
    let c = coordinator(addr);
    let mut client = client(addr).await;
    let mut out = Vec::new();
    for query in probes {
        let hits = ann(&c, query, K).await;
        let documents = client
            .get_documents(GetDocumentsRequest {
                doc_ids: hits.iter().map(|h| h.0).collect(),
            })
            .await
            .unwrap()
            .into_inner()
            .documents;
        let texts: BTreeMap<u64, String> =
            documents.into_iter().map(|d| (d.doc_id, d.text)).collect();
        out.push(
            hits.iter()
                .map(|(id, score)| (texts[id].clone(), score.to_bits()))
                .collect(),
        );
    }
    out
}

/// A by-text answer holds live rows only, scored exactly, in score order,
/// and reaches `min_recall` of the exact top-K.
fn check_texts(
    hits: &[(String, u32)],
    live: &BTreeMap<u64, Row>,
    query: &[f32],
    min_recall: usize,
    what: &str,
) {
    let by_text: BTreeMap<&str, (u64, &Row)> = live
        .iter()
        .map(|(id, row)| (row.text.as_str(), (*id, row)))
        .collect();
    let mut ids = Vec::with_capacity(hits.len());
    for (text, bits) in hits {
        let (id, row) = by_text
            .get(text.as_str())
            .unwrap_or_else(|| panic!("{what}: {text:?} is not a live row"));
        assert_eq!(
            *bits,
            dot(&row.vector, query).to_bits(),
            "{what}: {text} score"
        );
        ids.push(*id);
    }
    let scores: Vec<f32> = hits.iter().map(|(_, bits)| f32::from_bits(*bits)).collect();
    assert!(scores.windows(2).all(|w| w[0] >= w[1]), "{what}: order");
    let truth: Vec<u64> = truth(live, query, K)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    let found = ids.iter().filter(|id| truth.contains(id)).count();
    assert!(
        found >= min_recall,
        "{what}: {found} of {K} exact neighbours"
    );
}

/// Compaction of a segmented hnsw shard rebuilds the graph over the live
/// rows (tombstones gone; the rebuilt graph answers exactly scored live
/// rows at the recall the graph gives, not the old graph's misses), and a
/// publication interrupted between the write of its outputs and the
/// publish of its manifest rolls back at open: the staged segment is
/// removed, the marker is gone, and the old generation serves the same
/// answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_rebuilds_the_graph_and_an_interrupted_publication_rolls_back() {
    let dir = tempdir("compact");
    let path = dir.join("shard.hnsw");
    let compact_cfg = || NodeConfig {
        wal_buckets: 2,
        ..config(&path, true, true)
    };
    let (addr, handle) = start_empty_node(compact_cfg()).await;
    configure(&addr).await;
    let mut live: BTreeMap<u64, Row> = BTreeMap::new();
    for range in [0..450, 450..900] {
        let batch = rows(range.clone());
        let first = ingest_rows(&addr, &batch).await;
        for (i, row) in batch.into_iter().enumerate() {
            live.insert(first + i as u64, row);
        }
        flush(&addr).await;
    }
    let doomed: Vec<u64> = (0..900u64).filter(|id| id % 5 == 1).collect();
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
    flush(&addr).await;
    assert!(health(&addr).await.deleted_docs > 0);

    let probes = queries(5, 0xC0A7_0000);
    let c = coordinator(&addr);
    for (q, query) in probes.iter().enumerate() {
        check(
            &ann(&c, query, K).await,
            &live,
            query,
            8,
            &format!("pre-compaction {q}"),
        );
    }

    let mut request = Request::new(CompactShardRequest::default());
    request.set_timeout(Duration::from_secs(600));
    let compacted = client(&addr)
        .await
        .compact_shard(request)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(compacted.rows_before, 900);
    assert_eq!(compacted.rows_after as usize, live.len());
    assert_eq!(compacted.tombstones_reclaimed as usize, doomed.len());
    let after_health = health(&addr).await;
    assert_eq!(after_health.deleted_docs, 0);
    assert_eq!(after_health.live_docs as usize, live.len());
    let (_, post_tail_enum) = ann_with_tail_stats(&c, &probes[0], K).await;
    assert_eq!(
        post_tail_enum, 0,
        "compacted segments >= MIN_GRAPH_ROWS must be linked"
    );
    let after = dense_by_text(&addr, &probes).await;
    for (q, query) in probes.iter().enumerate() {
        check_texts(&after[q], &live, query, 8, &format!("post-compaction {q}"));
    }
    handle.abort();

    // The compacted catalog reopened from disk serves the same answers from
    // mapped hnsw images.
    let root = segments_root(&path);
    let reopened = OpenedSegmentSet::open_with(&root, VectorLoad::Mapped).unwrap();
    assert!(!reopened.is_empty());
    for i in 0..reopened.len() {
        if let Some(vector) = reopened.vector(i) {
            assert!(vector.is_mapped());
            assert!(vector.is_graph_linked());
            assert_eq!(vector.descriptor().backend_kind, HNSW);
        }
    }
    let published_ids: Vec<String> = reopened
        .manifest()
        .segments
        .iter()
        .map(|s| s.segment_id.clone())
        .collect();
    drop(reopened);
    let (addr, handle) = start_opened_node(compact_cfg()).await;
    assert_eq!(dense_by_text(&addr, &probes).await, after);
    handle.abort();

    // A crash between writing a later compaction's outputs and publishing
    // its manifest: the manifest backup and the marker are on disk, a
    // staged segment directory sits beside the published ones, and the
    // manifest itself is untouched. Opening rolls the attempt back.
    let manifest_path = SegmentCatalog::manifest_path(&root);
    let mut backup = manifest_path.as_os_str().to_owned();
    backup.push(".pre-compact");
    std::fs::copy(&manifest_path, PathBuf::from(&backup)).unwrap();
    let staged = SegmentCatalog::segment_dir(&root, "cmp-interrupted");
    std::fs::create_dir_all(&staged).unwrap();
    std::fs::write(staged.join("vector.index"), b"half written").unwrap();
    let marker = pipestream_search::compaction::marker_path(&path);
    std::fs::write(
        &marker,
        serde_json::json!({
            "format": 1,
            "layout": "segments",
            "old_wal_generation": 0,
            "new_wal_generation": 0,
            "walless": true,
            "work_dir": dir.join("work"),
            "previous_snapshot": false,
            "legacy_files": [],
            "staged_segments": ["cmp-interrupted"],
            "replaced_segments": published_ids
        })
        .to_string(),
    )
    .unwrap();

    let (addr, handle) = start_opened_node(compact_cfg()).await;
    assert!(!marker.exists(), "the marker is consumed by the rollback");
    assert!(!staged.exists(), "the staged segment is removed");
    assert!(!PathBuf::from(&backup).exists());
    let serving = OpenedSegmentSet::open(&root).unwrap();
    let serving_ids: Vec<String> = serving
        .manifest()
        .segments
        .iter()
        .map(|s| s.segment_id.clone())
        .collect();
    assert_eq!(serving_ids, published_ids, "the old generation serves");
    assert_eq!(dense_by_text(&addr, &probes).await, after);
    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
