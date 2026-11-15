use super::maintenance::{catalog, next, prepare, simulate_manifest_commit};
use super::*;

#[tokio::test]
async fn checkpoint_keeps_maintenance_anchor_and_unpublished_source_backlog() {
    let fixture = Fixture::new();
    fixture.publish_through_node(KEY, 1, Some(2)).await;
    fixture.source.enable_index_maintenance().unwrap();
    let current = catalog(&fixture);
    let before = current.snapshot();
    let after = next(&before, false);
    let intent = prepare(&fixture, &before, &after);
    let error = fixture.source.capture_checkpoint(1 << 20).err().unwrap();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("pending"));
    let current = simulate_manifest_commit(&after);
    assert_eq!(
        fixture
            .source
            .recover_index_maintenance(INDEX, &current)
            .unwrap(),
        MaintenanceRecovery::Committed(intent.clone())
    );
    let (_, _candidate) = fixture.stage(KEY, 2, Some(0)).await;
    let checkpoint = fixture.source.capture_checkpoint(1 << 20).unwrap();
    let metadata = checkpoint.metadata();
    assert_eq!(metadata.header.as_ref().unwrap().accepted_sequence, 2);
    assert_eq!(metadata.indexes.len(), 1);
    let state = &metadata.indexes[0];
    assert_eq!(state.committed_sequence, 1);
    assert_eq!(state.index_key, INDEX);
    assert_eq!(
        state.committed_manifest_sha256,
        intent.after_manifest_sha256
    );
    assert_eq!(
        state.maintenance_tip.as_ref().unwrap().intent_id,
        intent.intent_id
    );
    let output = fixture.root.join("checkpoint.redb");
    let info = checkpoint.write_to(&output, &checkpoint_limits()).unwrap();
    let restored = DocumentCatalog::open(&output, "books").unwrap();
    assert_eq!(
        restored
            .capture_checkpoint(1 << 20)
            .unwrap()
            .metadata()
            .indexes,
        info.indexes
    );
    assert_eq!(
        restored
            .index_maintenance_decision(INDEX, intent.after_epoch)
            .unwrap(),
        Some(intent)
    );
    assert_eq!(
        restored
            .current_index_publication_decision(INDEX, &current)
            .unwrap(),
        fixture
            .source
            .current_index_publication_decision(INDEX, &current)
            .unwrap()
    );
    assert_eq!(
        restored.get(KEY, Some(2)).unwrap(),
        fixture.source.get(KEY, Some(2)).unwrap()
    );
}
