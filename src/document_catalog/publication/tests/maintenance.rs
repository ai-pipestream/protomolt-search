use super::*;
use crate::node::segments_root;
use crate::pb::storage::{MaintenanceDecisionKey, MaintenanceIntent};

pub(super) fn catalog(fixture: &Fixture) -> SegmentCatalog {
    SegmentCatalog::open(segments_root(
        fixture.node.config.index_path.as_ref().unwrap(),
    ))
    .unwrap()
}
pub(super) fn next(before: &OpenedSegmentSet, reclaim: bool) -> Arc<OpenedSegmentSet> {
    let mut manifest = before.published_manifest();
    manifest.epoch += 1;
    if reclaim {
        manifest.segments.clear();
    }
    SegmentCatalog::open_staged(before.root(), manifest, Default::default())
        .unwrap()
        .snapshot()
}
pub(super) fn prepare(
    fixture: &Fixture,
    before: &OpenedSegmentSet,
    after: &OpenedSegmentSet,
) -> MaintenanceIntent {
    fixture
        .source
        .prepare_index_maintenance(INDEX, before, after, &fixture.root.join("proof"), 1)
        .unwrap()
}
pub(super) fn simulate_manifest_commit(set: &OpenedSegmentSet) -> SegmentCatalog {
    // Crash-window fixture only: runtime publication remains gated until the
    // production owner can join its activation fence to this journal.
    std::fs::write(
        set.root().join("segments.json"),
        serde_json::to_vec(set.manifest()).unwrap(),
    )
    .unwrap();
    SegmentCatalog::open(set.root()).unwrap()
}
async fn reopen_node(fixture: &mut Fixture) -> DocumentProjectionActivation {
    fixture.node = NodeServiceImpl::open(fixture.node.config.clone(), None, false).unwrap();
    let node = fixture.node.clone();
    let source = fixture.source.clone();
    tokio::task::spawn_blocking(move || node.recover_document_projection_blocking(&source, INDEX))
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

/// Every operation record of the source, exactly as stored.
fn operation_records(fixture: &Fixture) -> Vec<(Vec<u8>, Vec<u8>)> {
    let tx = fixture.source.database.begin_read().unwrap();
    let records = tx
        .open_table(OPERATIONS)
        .unwrap()
        .iter()
        .unwrap()
        .map(|r| {
            let (key, value) = r.unwrap();
            (key.value().to_vec(), value.value().to_vec())
        })
        .collect::<Vec<_>>();
    records
}

/// Prepare one maintenance round on top of `current`, commit its manifest
/// and recover it across a source reopen. The round's intent chains from
/// `previous`, is idempotent to prepare and to recover, and leaves
/// `decision_before` the current source decision. Returns the committed
/// catalog and the cursor the next round chains from.
fn commit_maintenance_round(
    fixture: &mut Fixture,
    current: &SegmentCatalog,
    previous: &Option<storage::MaintenanceCursor>,
    decision_before: &ProjectionIntent,
) -> (SegmentCatalog, storage::MaintenanceCursor) {
    let before = current.snapshot();
    let after = next(&before, false);
    let intent = prepare(fixture, &before, &after);
    assert_eq!(&intent.previous, previous);
    assert_eq!(prepare(fixture, &before, &after), intent);
    let current = simulate_manifest_commit(&after);
    fixture.reopen_source();
    assert_eq!(
        fixture
            .source
            .recover_index_maintenance(INDEX, &current)
            .unwrap(),
        MaintenanceRecovery::Committed(intent.clone())
    );
    assert_eq!(
        fixture
            .source
            .index_maintenance_decision(INDEX, intent.after_epoch)
            .unwrap(),
        Some(intent.clone())
    );
    assert_eq!(
        fixture
            .source
            .recover_index_maintenance(INDEX, &current)
            .unwrap(),
        MaintenanceRecovery::Idle
    );
    assert_eq!(
        fixture
            .source
            .current_index_publication_decision(INDEX, &current)
            .unwrap(),
        Some(decision_before.clone())
    );
    let cursor = storage::MaintenanceCursor {
        after_epoch: intent.after_epoch,
        intent_id: intent.intent_id,
    };
    (current, cursor)
}

#[tokio::test]
async fn repeated_maintenance_and_source_writes_preserve_source_history_and_identity() {
    let mut fixture = Fixture::new();
    let activation = fixture.publish_through_node(KEY, 1, Some(2)).await;
    let source_before = fixture.source.get(KEY, Some(1)).unwrap();
    let receipt_bytes = operation_records(&fixture);

    let decision_before = fixture
        .source
        .index_publication_decision(INDEX, 1)
        .unwrap()
        .unwrap();
    fixture.source.enable_index_maintenance().unwrap();
    fixture.source.enable_index_maintenance().unwrap();
    let mut current = catalog(&fixture);
    let mut previous = None;
    for _ in 0..2 {
        let (committed, cursor) =
            commit_maintenance_round(&mut fixture, &current, &previous, &decision_before);
        current = committed;
        previous = Some(cursor);
    }
    assert_eq!(fixture.source.get(KEY, Some(1)).unwrap(), source_before);
    assert_eq!(
        operation_records(&fixture),
        receipt_bytes,
        "maintenance must preserve exact retry receipts"
    );
    let recovered = reopen_node(&mut fixture).await;
    assert_eq!(recovered.catalog_epoch, current.snapshot().epoch());
    assert_eq!(recovered.accepted_sequence, activation.accepted_sequence);
    assert_eq!(recovered.intent_id, activation.intent_id);
    assert_eq!(
        current
            .snapshot()
            .bm25(0)
            .document_identity(0)
            .unwrap()
            .document_key,
        KEY
    );
    let deletion = fixture.publish_through_node(KEY, 2, None).await;
    current = catalog(&fixture);
    let before = current.snapshot();
    let after = next(&before, true);
    let intent = prepare(&fixture, &before, &after);
    assert_eq!(intent.previous, previous);
    assert_eq!(intent.accepted_sequence, 2);
    assert_eq!(intent.preservation.as_ref().unwrap().live_rows, 0);
    current = simulate_manifest_commit(&after);
    assert_eq!(
        fixture
            .source
            .recover_index_maintenance(INDEX, &current)
            .unwrap(),
        MaintenanceRecovery::Committed(intent)
    );
    let recovered = reopen_node(&mut fixture).await;
    assert_eq!(recovered.catalog_epoch, current.snapshot().epoch());
    assert_eq!(recovered.accepted_sequence, deletion.accepted_sequence);
    assert!(current.snapshot().is_empty());
    let updated = fixture.publish_through_node(KEY, 3, Some(1)).await;
    assert_eq!(updated.accepted_sequence, 3);
    assert_eq!(
        fixture.source.index_publication_decision(INDEX, 1).unwrap(),
        Some(decision_before)
    );
    assert_eq!(fixture.source.get(KEY, Some(1)).unwrap(), source_before);
    let current = catalog(&fixture);
    let identity = current.snapshot().bm25(0).document_identity(0).unwrap();
    assert_eq!(identity.document_key, KEY);
    assert_eq!(identity.version, 3);
}

#[tokio::test]
async fn pending_maintenance_blocks_other_publications_and_survives_an_unexpected_manifest() {
    let mut fixture = Fixture::new();
    fixture.publish_through_node(KEY, 1, Some(1)).await;
    fixture.source.enable_index_maintenance().unwrap();
    let current = catalog(&fixture);
    let before = current.snapshot();
    let after = next(&before, false);
    let intent = prepare(&fixture, &before, &after);
    let mut other = after.published_manifest();
    other.segments[0].generation += 1;
    let other = SegmentCatalog::open_staged(before.root(), other, Default::default())
        .unwrap()
        .snapshot();
    assert!(fixture
        .source
        .prepare_index_maintenance(INDEX, &before, &other, &fixture.root.join("other-proof"), 1)
        .unwrap_err()
        .message()
        .contains("pending maintenance"));
    assert!(fixture
        .source
        .recover_index_publication(INDEX, &current)
        .unwrap_err()
        .message()
        .contains("maintenance"));
    assert!(fixture
        .source
        .current_index_publication_decision(INDEX, &current)
        .is_err());
    let (_, candidate) = fixture.stage(KEY, 2, Some(1)).await;
    let node = fixture.node.clone();
    let source = fixture.source.clone();
    let error = tokio::task::spawn_blocking(move || {
        node.publish_document_projection_blocking(&source, INDEX, &candidate)
    })
    .await
    .unwrap()
    .unwrap_err();
    assert!(error.message().contains("maintenance"), "{error}");
    assert_eq!(catalog(&fixture).snapshot().manifest(), before.manifest());
    let unexpected = next(&after, false);
    let foreign = simulate_manifest_commit(&unexpected);
    fixture.reopen_source();
    assert!(fixture
        .source
        .recover_index_maintenance(INDEX, &foreign)
        .unwrap_err()
        .message()
        .contains("neither"));
    assert!(fixture
        .source
        .index_maintenance_decision(INDEX, intent.after_epoch)
        .unwrap()
        .is_none());
    let restored = simulate_manifest_commit(&before);
    assert_eq!(
        fixture
            .source
            .recover_index_maintenance(INDEX, &restored)
            .unwrap(),
        MaintenanceRecovery::Aborted(intent.clone())
    );
    assert!(fixture
        .source
        .index_maintenance_decision(INDEX, intent.after_epoch)
        .unwrap()
        .is_none());
    assert_eq!(prepare(&fixture, &before, &after), intent);
}

#[tokio::test]
async fn migration_refuses_pending_sources_without_changing_the_old_header() {
    let fixture = Fixture::new();
    let (_, candidate) = fixture.stage(KEY, 1, Some(1)).await;
    fixture.bind(&candidate);
    assert!(fixture.publish(&candidate, None, true).is_err());
    assert!(fixture
        .source
        .enable_index_maintenance()
        .unwrap_err()
        .message()
        .contains("pending"));
    let read = fixture.source.database.begin_read().unwrap();
    let meta = read.open_table(META).unwrap();
    let old: ProjectionJournalHeader = decode(meta.get(JOURNAL).unwrap().unwrap().value()).unwrap();
    assert_eq!(old.format_version, 1);
    assert!(!read
        .list_tables()
        .unwrap()
        .any(|t| t.name() == MAINTENANCE.name()));
    drop(meta);
    drop(read);
    fixture
        .source
        .recover_index_publication(INDEX, &fixture.target)
        .unwrap();
    fixture.source.enable_index_maintenance().unwrap();
    assert!(fixture.source.validate_projection_journal().unwrap());
}

#[tokio::test]
async fn maintenance_rejects_unproven_rows_and_forged_pruning_before_journaling() {
    let fixture = Fixture::new();
    fixture.publish_through_node(KEY, 1, Some(2)).await;
    fixture.source.enable_index_maintenance().unwrap();
    let current = catalog(&fixture);
    let before = current.snapshot();
    let lost = next(&before, true);
    assert!(fixture
        .source
        .prepare_index_maintenance(INDEX, &before, &lost, &fixture.root.join("lost-proof"), 1)
        .is_err());
    let mut manifest = before.published_manifest();
    manifest.epoch += 1;
    manifest.segments[0].summary.as_mut().unwrap().uint_columns[0].present = 0;
    let bad = SegmentCatalog::open_staged(before.root(), manifest, Default::default())
        .unwrap()
        .snapshot();
    assert!(fixture
        .source
        .prepare_index_maintenance(INDEX, &before, &bad, &fixture.root.join("bad-proof"), 1)
        .unwrap_err()
        .message()
        .contains("pruning summary"));
    assert_eq!(
        fixture
            .source
            .recover_index_maintenance(INDEX, &current)
            .unwrap(),
        MaintenanceRecovery::Idle
    );
}

#[tokio::test]
async fn corrupted_maintenance_record_and_header_downgrade_refuse_recovery() {
    let fixture = Fixture::new();
    fixture.publish_through_node(KEY, 1, Some(1)).await;
    fixture.source.enable_index_maintenance().unwrap();
    let current = catalog(&fixture);
    let before = current.snapshot();
    let after = next(&before, false);
    let intent = prepare(&fixture, &before, &after);
    let current = simulate_manifest_commit(&after);
    fixture
        .source
        .recover_index_maintenance(INDEX, &current)
        .unwrap();
    let encoded = MaintenanceDecisionKey {
        index_key: INDEX.to_vec(),
        after_epoch: intent.after_epoch,
    }
    .encode_to_vec();
    let tx = fixture.source.database.begin_write().unwrap();
    {
        let mut table = tx.open_table(MAINTENANCE).unwrap();
        let mut changed = intent.clone();
        changed.source_intent_id[0] ^= 1;
        table
            .insert(encoded.as_slice(), changed.encode_to_vec().as_slice())
            .unwrap();
    }
    tx.commit().unwrap();
    assert_eq!(
        fixture
            .source
            .recover_index_maintenance(INDEX, &current)
            .unwrap_err()
            .code(),
        tonic::Code::DataLoss
    );
    let tx = fixture.source.database.begin_write().unwrap();
    {
        let mut table = tx.open_table(MAINTENANCE).unwrap();
        table
            .insert(encoded.as_slice(), intent.encode_to_vec().as_slice())
            .unwrap();
        let mut meta = tx.open_table(META).unwrap();
        let mut header: ProjectionJournalHeader =
            decode(meta.get(JOURNAL).unwrap().unwrap().value()).unwrap();
        header.format_version = 1;
        meta.insert(JOURNAL, header.encode_to_vec().as_slice())
            .unwrap();
    }
    tx.commit().unwrap();
    assert!(fixture.source.validate_projection_journal().is_err());
}

#[tokio::test]
async fn maintenance_links_are_checked_even_when_record_checksums_are_recomputed() {
    let fixture = Fixture::new();
    fixture.publish_through_node(KEY, 1, Some(1)).await;
    fixture.source.enable_index_maintenance().unwrap();
    let current = catalog(&fixture);
    let before = current.snapshot();
    let after = next(&before, false);
    let intent = prepare(&fixture, &before, &after);
    let current = simulate_manifest_commit(&after);
    fixture
        .source
        .recover_index_maintenance(INDEX, &current)
        .unwrap();
    let encoded = MaintenanceDecisionKey {
        index_key: INDEX.to_vec(),
        after_epoch: intent.after_epoch,
    }
    .encode_to_vec();
    for change in 0..2 {
        let mut changed = intent.clone();
        match change {
            0 => changed.source_intent_id[0] ^= 1,
            1 => changed.before_manifest_sha256[0] ^= 1,
            _ => unreachable!(),
        }
        changed.intent_id.clear();
        changed.intent_id = turbo_sha256::sha256(&changed.encode_to_vec()).0.to_vec();
        let tx = fixture.source.database.begin_write().unwrap();
        {
            let mut table = tx.open_table(MAINTENANCE).unwrap();
            table
                .insert(encoded.as_slice(), changed.encode_to_vec().as_slice())
                .unwrap();
            let mut states = tx.open_table(STATES).unwrap();
            let mut state: ProjectionJournalState =
                decode(states.get(INDEX).unwrap().unwrap().value()).unwrap();
            state.maintenance_tip.as_mut().unwrap().intent_id = changed.intent_id;
            states
                .insert(INDEX, state.encode_to_vec().as_slice())
                .unwrap();
        }
        tx.commit().unwrap();
        let error = fixture
            .source
            .recover_index_maintenance(INDEX, &current)
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::DataLoss);
        assert!(
            error.message().contains(if change == 0 {
                "source anchor"
            } else {
                "compose"
            }),
            "{error}"
        );
    }
}
