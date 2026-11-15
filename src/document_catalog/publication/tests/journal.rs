use super::*;

#[tokio::test]
async fn decisions_survive_reopen_and_keep_acceptance_receipts_unchanged() {
    let mut fixture = Fixture::new();
    let (receipt, candidate) = fixture.stage(KEY, 1, Some(2)).await;
    fixture.bind(&candidate);
    let intent = fixture.publish(&candidate, None, false).unwrap();
    assert_eq!(
        fixture.source.index_publication_decision(INDEX, 1).unwrap(),
        None
    );
    assert_eq!(
        fixture
            .source
            .recover_index_publication(INDEX, &fixture.target)
            .unwrap(),
        ProjectionRecovery::Committed(intent.clone())
    );
    assert_eq!(
        fixture
            .source
            .recover_index_publication(INDEX, &fixture.target)
            .unwrap(),
        ProjectionRecovery::Idle
    );
    assert!(!receipt.searchable);
    let (second, replacement) = fixture.stage(KEY, 2, Some(1)).await;
    let next = fixture.publish(&replacement, None, false).unwrap();
    assert_eq!(
        fixture
            .target
            .snapshot()
            .document_retirements(KEY, 100)
            .unwrap()
            .iter()
            .map(|r| r.rows.len())
            .sum::<usize>(),
        1
    );
    assert_eq!(
        fixture
            .source
            .recover_index_publication(INDEX, &fixture.target)
            .unwrap(),
        ProjectionRecovery::Committed(next.clone())
    );
    assert!(!second.searchable);
    let (retried, _) = fixture.stage(KEY, 1, Some(2)).await;
    assert_eq!(
        retried,
        DocumentWriteReceipt {
            replayed: true,
            ..receipt
        }
    );
    assert_eq!(
        fixture.source.index_publication_decision(INDEX, 1).unwrap(),
        Some(intent)
    );
    fixture.close_source();
    let reopened = DocumentCatalog::open(&fixture.root.join("source.redb"), "books").unwrap();
    assert_eq!(
        reopened.index_publication_decision(INDEX, 2).unwrap(),
        Some(next)
    );
}

#[tokio::test]
async fn interrupted_prepare_aborts_and_exact_retry_can_publish() {
    let fixture = Fixture::new();
    let (_, candidate) = fixture.stage(KEY, 1, Some(1)).await;
    fixture.bind(&candidate);
    assert!(fixture
        .publish(&candidate, None, true)
        .unwrap_err()
        .contains("interrupted after intent"));
    assert!(matches!(
        fixture
            .source
            .recover_index_publication(INDEX, &fixture.target)
            .unwrap(),
        ProjectionRecovery::Aborted(_)
    ));
    let intent = fixture.publish(&candidate, None, false).unwrap();
    assert_eq!(
        fixture
            .source
            .recover_index_publication(INDEX, &fixture.target)
            .unwrap(),
        ProjectionRecovery::Committed(intent)
    );
}

#[tokio::test]
async fn failed_manifest_sync_requires_reopen_before_deciding() {
    let fixture = Fixture::new();
    let (_, candidate) = fixture.stage(KEY, 1, Some(1)).await;
    fixture.bind(&candidate);
    crate::segments::FAIL_SET_SYNC.with(|fail| fail.set(true));
    assert!(fixture.publish(&candidate, None, false).is_err());
    assert!(fixture
        .source
        .recover_index_publication(INDEX, &fixture.target)
        .unwrap_err()
        .message()
        .contains("uncertain"));
    let reopened = SegmentCatalog::open(fixture.root.join("target")).unwrap();
    assert!(matches!(
        fixture
            .source
            .recover_index_publication(INDEX, &reopened)
            .unwrap(),
        ProjectionRecovery::Committed(_)
    ));
}

#[tokio::test]
async fn out_of_order_sources_and_incomplete_retirements_refuse() {
    let fixture = Fixture::new();
    let (_, first) = fixture.stage(KEY, 1, Some(2)).await;
    let (_, second) = fixture.stage(KEY, 2, Some(1)).await;
    fixture.bind(&first);
    assert!(fixture
        .publish(&second, None, false)
        .unwrap_err()
        .contains("next accepted sequence"));
    fixture.publish(&first, None, false).unwrap();
    fixture
        .source
        .recover_index_publication(INDEX, &fixture.target)
        .unwrap();
    let before = fixture.target.snapshot();
    assert!(fixture
        .publish(&second, Some(vec![]), false)
        .unwrap_err()
        .contains("exact staged document replacement"));
    assert_eq!(fixture.target.snapshot().manifest(), before.manifest());
    fixture.publish(&second, None, false).unwrap();
}

#[tokio::test]
async fn unrelated_retirements_are_not_certified() {
    let fixture = Fixture::new();
    let (_, first) = fixture.stage(KEY, 1, Some(1)).await;
    fixture.bind(&first);
    fixture.publish(&first, None, false).unwrap();
    fixture
        .source
        .recover_index_publication(INDEX, &fixture.target)
        .unwrap();
    let (_, other) = fixture.stage(b"other", 1, Some(1)).await;
    let retire = fixture
        .target
        .snapshot()
        .document_retirements(KEY, 100)
        .unwrap();
    assert!(fixture
        .publish(&other, Some(retire), false)
        .unwrap_err()
        .contains("exact staged document replacement"));
    fixture.publish(&other, None, false).unwrap();
}

#[tokio::test]
async fn a_third_manifest_preserves_pending_intent_and_refuses() {
    let fixture = Fixture::new();
    let (_, first) = fixture.stage(KEY, 1, Some(1)).await;
    fixture.bind(&first);
    assert!(fixture.publish(&first, None, true).is_err());
    fixture
        .target
        .publish_partition_key(Some("unrelated".into()))
        .unwrap();
    for _ in 0..2 {
        assert!(fixture
            .source
            .recover_index_publication(INDEX, &fixture.target)
            .unwrap_err()
            .message()
            .contains("neither projection manifest"));
    }
    assert!(fixture
        .publish(&first, None, false)
        .unwrap_err()
        .contains("existing projection intent"));
}

#[tokio::test]
async fn empty_source_and_deletion_each_advance_the_publication_sequence() {
    let fixture = Fixture::new();
    let (_, initial) = fixture.stage(KEY, 1, Some(1)).await;
    fixture.bind(&initial);
    fixture.publish(&initial, None, false).unwrap();
    fixture
        .source
        .recover_index_publication(INDEX, &fixture.target)
        .unwrap();
    let (_, empty) = fixture.stage(KEY, 2, Some(0)).await;
    let intent = fixture.publish(&empty, None, false).unwrap();
    assert_eq!(intent.rows, 0);
    assert!(!intent.source.as_ref().unwrap().deleted);
    fixture
        .source
        .recover_index_publication(INDEX, &fixture.target)
        .unwrap();
    let (_, deletion) = fixture.stage(KEY, 3, None).await;
    // No physical rows remain. The source transition still needs a distinct
    // manifest epoch; prepare the validated empty change without adding rows.
    let before = fixture.target.snapshot();
    let mut manifest = before.published_manifest();
    manifest.epoch += 1;
    let shadow =
        SegmentCatalog::open_staged(before.root(), manifest.clone(), Default::default()).unwrap();
    let intent = fixture
        .source
        .prepare_index_publication(INDEX, &deletion, &before, &shadow.snapshot())
        .unwrap();
    assert!(intent.source.as_ref().unwrap().deleted);
    crate::segments::write_manifest_file(&SegmentCatalog::manifest_path(before.root()), &manifest)
        .unwrap();
    let reopened = SegmentCatalog::open(before.root()).unwrap();
    assert_eq!(
        fixture
            .source
            .recover_index_publication(INDEX, &reopened)
            .unwrap(),
        ProjectionRecovery::Committed(intent)
    );
}

#[test]
fn journal_header_cannot_disappear_or_change_history() {
    let fixture = Fixture::new();
    let tx = fixture.source.database.begin_write().unwrap();
    {
        tx.open_table(STATES).unwrap();
    }
    tx.commit().unwrap();
    assert_eq!(
        fixture
            .source
            .validate_projection_journal()
            .unwrap_err()
            .code(),
        tonic::Code::DataLoss
    );
    assert!(journal_header(
        Some(
            &ProjectionJournalHeader {
                format_version: 1,
                history_id: vec![2; 16]
            }
            .encode_to_vec()
        ),
        &[STATES.name().into(), DECISIONS.name().into()],
        &[1; 16]
    )
    .is_err());
}

#[tokio::test]
async fn an_idle_journal_still_checks_its_committed_manifest() {
    let fixture = Fixture::new();
    let (_, first) = fixture.stage(KEY, 1, Some(1)).await;
    fixture.bind(&first);
    fixture.publish(&first, None, false).unwrap();
    fixture
        .source
        .recover_index_publication(INDEX, &fixture.target)
        .unwrap();
    fixture
        .target
        .publish_partition_key(Some("unrelated".into()))
        .unwrap();
    assert!(fixture
        .source
        .recover_index_publication(INDEX, &fixture.target)
        .unwrap_err()
        .message()
        .contains("committed projection manifest"));
}

#[tokio::test]
async fn committed_cursor_must_match_its_immutable_decision() {
    let fixture = Fixture::new();
    let (_, first) = fixture.stage(KEY, 1, Some(1)).await;
    fixture.bind(&first);
    fixture.publish(&first, None, false).unwrap();
    fixture
        .source
        .recover_index_publication(INDEX, &fixture.target)
        .unwrap();
    let tx = fixture.source.database.begin_write().unwrap();
    {
        let mut table = tx.open_table(STATES).unwrap();
        let mut state: ProjectionJournalState =
            decode(table.get(INDEX).unwrap().unwrap().value()).unwrap();
        state.committed_manifest_sha256 = vec![7; 32];
        table
            .insert(INDEX, state.encode_to_vec().as_slice())
            .unwrap();
    }
    tx.commit().unwrap();
    assert_eq!(
        fixture
            .source
            .recover_index_publication(INDEX, &fixture.target)
            .unwrap_err()
            .code(),
        tonic::Code::DataLoss
    );
}

#[tokio::test]
async fn identical_source_bytes_from_another_history_cannot_be_adopted() {
    let fixture = Fixture::new();
    let foreign = Fixture::new();
    let (_, local) = fixture.stage(KEY, 1, Some(1)).await;
    let (_, candidate) = foreign.stage(KEY, 1, Some(1)).await;
    assert_eq!(local.source(), candidate.source());
    fixture.bind(&local);
    assert!(fixture
        .publish(&candidate, None, false)
        .unwrap_err()
        .contains("another catalog history"));
    assert!(!fixture.source.validate_projection_journal().unwrap());
    fixture.publish(&local, None, false).unwrap();
}

#[tokio::test]
async fn reopening_refuses_missing_journal_tables() {
    let mut fixture = Fixture::new();
    let (_, first) = fixture.stage(KEY, 1, Some(1)).await;
    fixture.bind(&first);
    fixture.publish(&first, None, false).unwrap();
    let tx = fixture.source.database.begin_write().unwrap();
    assert!(tx.delete_table(DECISIONS).unwrap());
    tx.commit().unwrap();
    fixture.close_source();
    let error = DocumentCatalog::open(&fixture.root.join("source.redb"), "books")
        .err()
        .expect("missing journal table must refuse reopening");
    assert_eq!(error.code(), tonic::Code::DataLoss);
}

#[tokio::test]
async fn node_recovery_joins_both_crash_windows_after_reopening_source_and_index() {
    use crate::pb::node_service_server::NodeService;
    for after_activation in [false, true] {
        let mut fixture = Fixture::new();
        let (receipt, candidate) = fixture.stage(KEY, 1, Some(1)).await;
        let before_incarnation = candidate.info().target_stats_incarnation.clone();
        let node = fixture.node.clone();
        let source = fixture.source.clone();
        let error = tokio::task::spawn_blocking(move || {
            if after_activation {
                crate::node::FAIL_PROJECTION_DECISION.with(|fail| fail.set(true));
            } else {
                crate::segments::FAIL_SET_SYNC.with(|fail| fail.set(true));
            }
            node.publish_document_projection_blocking(&source, INDEX, &candidate)
        })
        .await
        .unwrap()
        .unwrap_err();
        assert!(
            error.message().contains(if after_activation {
                "before source decision"
            } else {
                "after manifest rename"
            }),
            "{error}"
        );
        assert!(fixture.node.ingest_fence().is_some());
        assert!(fixture
            .source
            .index_publication_decision(INDEX, 1)
            .unwrap()
            .is_none());
        let health = fixture
            .node
            .health(tonic::Request::new(HealthRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(health.num_vectors, u64::from(after_activation));
        fixture.reopen_source();
        fixture.node = NodeServiceImpl::open(fixture.node.config.clone(), None, false).unwrap();
        let source = fixture.source.clone();
        let node = fixture.node.clone();
        let recovered = tokio::task::spawn_blocking(move || {
            node.recover_document_projection_blocking(&source, INDEX)
        })
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert_eq!(recovered.history_id, receipt.history_id);
        assert_eq!(recovered.accepted_sequence, receipt.accepted_sequence);
        assert_ne!(recovered.stats_incarnation, before_incarnation);
        assert_eq!(
            fixture
                .node
                .health(tonic::Request::new(HealthRequest {}))
                .await
                .unwrap()
                .into_inner()
                .num_vectors,
            1
        );
        assert_eq!(
            fixture
                .source
                .index_publication_decision(INDEX, 1)
                .unwrap()
                .unwrap()
                .intent_id,
            recovered.intent_id
        );
    }
}
