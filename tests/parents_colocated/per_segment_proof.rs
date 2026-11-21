//! The proof is per segment and survives compaction, reopen, snapshots.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_proof_is_per_segment_and_survives_compaction_reopen_and_snapshots() {
    let dir = tempdir("lifecycle");
    let path = dir.join("shard.tv");
    let (addr, handle) = common::start_empty_node(config(&path)).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    ingest_corpus(&mut client, true).await;
    client
        .delete_documents(DeleteDocumentsRequest {
            doc_ids: vec![0],
            expected_wal_generation: None,
        })
        .await
        .unwrap();
    client
        .compact_shard(CompactShardRequest::default())
        .await
        .unwrap();
    assert!(catalog(&path).parents_colocated(), "compaction keeps it");

    // One raw chunk of a parent seals into a segment of its own that
    // cannot claim the proof; the segments sealed before keep theirs.
    let sealed_before = catalog(&path).len();
    append(
        &mut client,
        "raw chunk court".into(),
        Some(parent_id(0)),
        None,
    )
    .await;
    client.flush(FlushRequest {}).await.unwrap();
    let set = catalog(&path);
    let bits: Vec<bool> = (0..set.len())
        .map(|i| set.metadata(i).parents_colocated)
        .collect();
    assert_eq!(bits.len(), sealed_before + 1);
    assert!(bits[..sealed_before].iter().all(|&bit| bit));
    assert!(
        !bits[sealed_before],
        "the raw chunk's segment cannot claim it"
    );
    assert!(!set.parents_colocated());
    handle.abort();

    let reopened = NodeServiceImpl::open(config(&path), None).unwrap();
    assert!(!reopened.parents_colocated(), "the bits survive a reopen");
    let repository = dir.join("repository");
    reopened.export_snapshot_blocking(&repository).unwrap();
    let target_path = dir.join("target.tv");
    let target = NodeServiceImpl::open(config(&target_path), None).unwrap();
    let (manifest, _) = pipestream_search::snapshot_repository::read_manifest(&repository).unwrap();
    target
        .install_staged_repository(&repository, &manifest)
        .unwrap();
    let restored = catalog(&target_path);
    assert_eq!(
        (0..restored.len())
            .map(|i| restored.metadata(i).parents_colocated)
            .collect::<Vec<_>>(),
        bits,
        "a snapshot restore keeps every segment's proof"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
