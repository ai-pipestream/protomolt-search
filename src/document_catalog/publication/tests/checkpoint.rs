use super::*;

#[tokio::test]
async fn checkpoint_refuses_pending_source_decisions_without_resolving_them() {
    let fixture = Fixture::new();
    let (_, candidate) = fixture.stage(KEY, 1, Some(2)).await;
    fixture.bind(&candidate);
    assert!(fixture.publish(&candidate, None, true).is_err());
    let error = fixture.source.capture_checkpoint(1 << 20).err().unwrap();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("pending"));
    let tx = fixture.source.database.begin_read().unwrap();
    let state: ProjectionJournalState = decode(
        tx.open_table(STATES)
            .unwrap()
            .get(INDEX)
            .unwrap()
            .unwrap()
            .value(),
    )
    .unwrap();
    assert!(state.pending.is_some());
    assert_eq!(state.committed_sequence, 0);
}

#[tokio::test]
async fn checkpoint_includes_every_index_in_one_source_history() {
    let mut fixture = Fixture::new();
    let (_, candidate) = fixture.stage(KEY, 1, Some(2)).await;
    fixture.bind(&candidate);
    fixture.publish(&candidate, None, false).unwrap();
    fixture
        .source
        .recover_index_publication(INDEX, &fixture.target)
        .unwrap();
    fixture.target = SegmentCatalog::open(fixture.root.join("second-target")).unwrap();
    fixture.bind(&candidate);
    fixture
        .publish_for(b"second-index", &candidate, None, false)
        .unwrap();
    fixture
        .source
        .recover_index_publication(b"second-index", &fixture.target)
        .unwrap();
    let checkpoint = fixture.source.capture_checkpoint(1 << 20).unwrap();
    assert_eq!(
        checkpoint
            .metadata()
            .indexes
            .iter()
            .map(|s| s.index_key.as_slice())
            .collect::<Vec<_>>(),
        [INDEX, b"second-index"]
    );
    assert!(checkpoint
        .metadata()
        .indexes
        .iter()
        .all(|s| s.committed_sequence == 1));
    let output = fixture.root.join("both.redb");
    let info = checkpoint.write_to(&output, &checkpoint_limits()).unwrap();
    let copy = DocumentCatalog::open(&output, "books").unwrap();
    assert_eq!(
        copy.capture_checkpoint(1 << 20).unwrap().metadata().indexes,
        info.indexes
    );
}

#[tokio::test]
async fn checkpoint_refuses_a_journal_tip_detached_from_accepted_history() {
    let fixture = Fixture::new();
    let (_, candidate) = fixture.stage(KEY, 1, Some(2)).await;
    fixture.bind(&candidate);
    fixture.publish(&candidate, None, false).unwrap();
    fixture
        .source
        .recover_index_publication(INDEX, &fixture.target)
        .unwrap();
    let tx = fixture.source.database.begin_write().unwrap();
    let key = DocumentVersionKey {
        document_key: KEY.to_vec(),
        version: 1,
    }
    .encode_to_vec();
    tx.open_table(VERSIONS)
        .unwrap()
        .remove(key.as_slice())
        .unwrap();
    tx.commit().unwrap();
    assert_eq!(
        fixture
            .source
            .capture_checkpoint(1 << 20)
            .err()
            .expect("a detached journal anchor must refuse capture")
            .code(),
        tonic::Code::DataLoss
    );
}
