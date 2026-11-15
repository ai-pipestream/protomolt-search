//! A failed closing Flush keeps snapshot installation behind compaction recovery.

use super::*;
use crate::pb::node_service_server::NodeService;
use crate::pb::{install_snapshot_from_request::Source, InstallSnapshotFromRequest};
use std::path::{Path, PathBuf};

fn pending_cutover(fixture: &Fixture) -> PathBuf {
    fixture.append(0);
    fixture.append(1);
    fixture.node.flush_index().unwrap();
    let PreflightKind::Wal(pre) = fixture
        .node
        .preflight_at_row_boundary(&CompactShardRequest::default())
        .unwrap()
    else {
        panic!("WAL preflight")
    };
    std::fs::create_dir_all(&pre.work_dir).unwrap();
    let obstruction = crate::node::live_docs_sidecar_path(&pre.index_path);
    let handle = tokio::runtime::Handle::current();
    let mut analyze = fixture.node.analyzer(&handle);
    let mut obstructed = false;
    let mut flush_shadow = |shadow: &mut Shadow| {
        NodeServiceImpl::flush_compaction_shadow(shadow)?;
        if !obstructed {
            std::fs::remove_file(&obstruction).unwrap();
            std::fs::create_dir(&obstruction).unwrap();
            obstructed = true;
        }
        Ok(())
    };
    let error = fixture
        .node
        .build_and_cut_over_with_shadow_flush(&pre, 256, &mut *analyze, &mut flush_shadow)
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Internal);
    assert!(error.message().contains(&obstruction.display().to_string()));
    let guard = fixture.node.state.read().unwrap();
    assert!(
        guard.pending_compaction.is_some(),
        "the cutover reached serving state"
    );
    assert_eq!(guard.wal.as_ref().unwrap().generation(), 1);
    assert!(marker_path(&pre.index_path).is_file());
    obstruction
}

fn recovery_files(index_path: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let root = crate::node::segments_root(index_path);
    [
        SegmentCatalog::manifest_path(&root),
        super::super::manifest_backup_path(&root),
        marker_path(index_path),
    ]
    .into_iter()
    .map(|path| {
        let bytes = std::fs::read(&path).unwrap();
        (path, bytes)
    })
    .collect()
}

fn serving_identity(node: &NodeServiceImpl) -> (u64, u64, u64, Vec<u8>) {
    let guard = node.state.read().unwrap();
    assert!(guard.pending_compaction.is_some());
    let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
        panic!("segments")
    };
    (
        guard.stats_epoch,
        guard.wal.as_ref().unwrap().generation(),
        guard.index.as_ref().unwrap().len() as u64,
        serde_json::to_vec(shard.snapshot().manifest()).unwrap(),
    )
}

fn run_pending_snapshot() {
    let donor = Fixture::new();
    donor.append(0);
    donor.append(1);
    donor.node.flush_index().unwrap();
    let repository = donor.directory.join("repository");
    donor.node.export_snapshot_blocking(&repository).unwrap();
    let recipient = Fixture::new();
    let obstruction = pending_cutover(&recipient);
    std::fs::remove_dir(&obstruction).unwrap();
    let index_path = recipient.config.index_path.as_ref().unwrap();
    let files = recovery_files(index_path);
    let serving = serving_identity(&recipient.node);
    let texts = Fixture::texts(&recipient.node);
    let error = tokio::runtime::Handle::current()
        .block_on(recipient.node.install_snapshot_from(tonic::Request::new(
            InstallSnapshotFromRequest {
                source: Some(Source::Directory(repository.display().to_string())),
                ..Default::default()
            },
        )))
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("compaction cutover is pending"));
    assert_eq!(recovery_files(index_path), files);
    assert_eq!(serving_identity(&recipient.node), serving);
    assert_eq!(Fixture::texts(&recipient.node), texts);
    recipient.node.flush_index().unwrap();
    assert!(recipient
        .node
        .state
        .read()
        .unwrap()
        .pending_compaction
        .is_none());
    assert!(!marker_path(index_path).exists());
    assert!(!super::super::manifest_backup_path(&crate::node::segments_root(index_path)).exists());
    let reopened = NodeServiceImpl::open(recipient.config.clone(), None, false).unwrap();
    assert_eq!(Fixture::texts(&reopened), texts);
    assert_eq!(
        reopened
            .state
            .read()
            .unwrap()
            .wal
            .as_ref()
            .unwrap()
            .generation(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_compaction_refuses_segment_snapshot_until_closing_flush() {
    tokio::task::spawn_blocking(run_pending_snapshot)
        .await
        .unwrap();
}
