use super::*;

#[test]
fn row_replacement_publishes_both_legs_and_reuses_immutable_images() {
    let input = publication_fixture("replace-old", &["old", "kept"]);
    let output = publication_fixture("replace-new", &["new", "newer"]);
    let root = input.root.join("catalog");
    let catalog = SegmentCatalog::open(&root).unwrap();
    let before = catalog.append(source(&input, "old", 0)).unwrap();
    let after = catalog
        .commit_rows(
            before.epoch(),
            &[retire("old", &[0])],
            vec![source(&output, "new", 2)],
        )
        .unwrap();
    assert_eq!(ids(&before), vec![0, 1]);
    assert_eq!(ids(&after), vec![1, 2, 3]);
    assert_eq!(after.epoch(), before.epoch() + 1);
    assert!(std::ptr::eq(before.bm25(0), after.bm25(0)));
    assert!(std::ptr::eq(
        before.vector(0).unwrap(),
        after.vector(0).unwrap()
    ));
    assert!(!before.live_docs(0).is_deleted(0));
    assert!(after.live_docs(0).is_deleted(0));
    assert_ne!(
        before.metadata(0).live_docs.file,
        after.metadata(0).live_docs.file
    );
    let mut vector_ids: Vec<_> = after
        .search_vector(&[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 20)
        .unwrap()
        .into_iter()
        .map(|hit| hit.doc_id)
        .collect();
    vector_ids.sort_unstable();
    assert_eq!(vector_ids, ids(&after));
    assert_eq!(
        after
            .global_bm25_stats(&["document".into()])
            .unwrap()
            .doc_count,
        3
    );
    assert_eq!(
        ids(&SegmentCatalog::open(&root).unwrap().snapshot()),
        ids(&after)
    );
}

#[test]
fn consumer_preparation_precedes_publication_and_can_refuse_it() {
    let input = publication_fixture("prepare-consumer", &["old", "kept"]);
    let output = publication_fixture("prepare-output", &["new", "newer"]);
    let root = input.root.join("catalog");
    let catalog = SegmentCatalog::open(&root).unwrap();
    let before = catalog.append(source(&input, "old", 0)).unwrap();
    let bytes = std::fs::read(root.join(SET_FILE)).unwrap();
    let refused = catalog.commit_rows_prepared(
        before.epoch(),
        &[retire("old", &[0])],
        vec![source(&output, "new", 2)],
        |candidate| {
            assert_eq!(ids(candidate), [1, 2, 3]);
            assert_eq!(ids(&catalog.snapshot()), [0, 1]);
            assert_eq!(std::fs::read(root.join(SET_FILE)).unwrap(), bytes);
            Err::<(), _>("consumer cannot activate this view".into())
        },
    );
    assert!(refused.unwrap_err().contains("cannot activate"));
    assert_eq!(std::fs::read(root.join(SET_FILE)).unwrap(), bytes);
    assert!(!SegmentCatalog::segment_dir(&root, "new").exists());
    let (published, prepared) = catalog
        .commit_rows_prepared(
            before.epoch(),
            &[retire("old", &[0])],
            vec![source(&output, "new", 2)],
            |candidate| {
                assert_eq!(std::fs::read(root.join(SET_FILE)).unwrap(), bytes);
                Ok(Arc::clone(candidate))
            },
        )
        .unwrap();
    assert!(Arc::ptr_eq(&published, &prepared));
    assert_eq!(ids(&published), [1, 2, 3]);
    assert_eq!(ids(&before), [0, 1]);
}

#[test]
fn zero_row_replacement_retires_the_complete_old_set() {
    let input = publication_fixture("retire-all", &["one", "two"]);
    let root = input.root.join("catalog");
    let catalog = SegmentCatalog::open(&root).unwrap();
    let before = catalog.append(source(&input, "old", 0)).unwrap();
    let after = catalog
        .commit_rows(before.epoch(), &[retire("old", &[0, 1])], vec![])
        .unwrap();
    assert!(ids(&after).is_empty());
    assert!(after
        .search_vector(&[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 20)
        .unwrap()
        .is_empty());
    assert_eq!(after.metadata(0).live_rows, 0);
    assert_eq!(ids(&before), vec![0, 1]);
    assert!(ids(&SegmentCatalog::open(&root).unwrap().snapshot()).is_empty());
}

#[test]
fn row_update_refusals_leave_manifest_and_old_bitmaps_unchanged() {
    let input = publication_fixture("retire-refuse", &["one", "two"]);
    let root = input.root.join("catalog");
    let catalog = SegmentCatalog::open(&root).unwrap();
    let before = catalog.append(source(&input, "old", 0)).unwrap();
    let directory = SegmentCatalog::segment_dir(&root, "old");
    let count = std::fs::read_dir(&directory).unwrap().count();
    let manifest = std::fs::read(root.join(SET_FILE)).unwrap();
    for (epoch, retirements) in [
        (0, vec![retire("old", &[0])]),
        (1, vec![retire("missing", &[0])]),
        (1, vec![retire("old", &[2])]),
        (1, vec![retire("old", &[0, 0])]),
        (1, vec![retire("old", &[])]),
    ] {
        assert!(catalog.commit_rows(epoch, &retirements, vec![]).is_err());
    }
    let mut invalid_counts = before.published_manifest();
    invalid_counts.epoch += 1;
    invalid_counts.segments[0].live_rows = 0;
    assert!(catalog
        .publish(invalid_counts)
        .unwrap_err()
        .contains("row counts"));
    let missing = input.root.join("missing.bm25");
    let mut invalid = source(&input, "new", 2);
    invalid.bm25_path = &missing;
    assert!(catalog
        .commit_rows(1, &[retire("old", &[0])], vec![invalid])
        .is_err());
    assert_eq!(std::fs::read(root.join(SET_FILE)).unwrap(), manifest);
    assert_eq!(std::fs::read_dir(&directory).unwrap().count(), count);
    assert_eq!(catalog.snapshot().epoch(), 1);
    assert_eq!(ids(&before), ids(&catalog.snapshot()));
}

#[test]
fn uncertain_row_update_retains_every_artifact_for_recovery() {
    let input = publication_fixture("retire-uncertain", &["one", "two"]);
    let root = input.root.join("catalog");
    let catalog = SegmentCatalog::open(&root).unwrap();
    let before = catalog.append(source(&input, "old", 0)).unwrap();
    FAIL_SET_SYNC.with(|fail| fail.set(true));
    assert!(catalog
        .commit_rows(1, &[retire("old", &[0, 1])], vec![source(&input, "new", 2)])
        .unwrap_err()
        .contains("after manifest rename"));
    assert_eq!(ids(&catalog.snapshot()), vec![0, 1]);
    assert_eq!(ids(&before), vec![0, 1]);
    assert!(catalog
        .commit_rows(1, &[retire("old", &[0])], vec![])
        .unwrap_err()
        .contains("uncertain"));
    let reopened = SegmentCatalog::open(&root).unwrap();
    assert_eq!(reopened.snapshot().epoch(), 2);
    assert_eq!(ids(&reopened.snapshot()), vec![2, 3]);
    assert!(directory_bitmap_exists(
        &root,
        &reopened.snapshot().metadata(0).live_docs.file
    ));
}

fn directory_bitmap_exists(root: &Path, name: &str) -> bool {
    SegmentCatalog::segment_dir(root, "old").join(name).exists()
}

#[test]
fn exact_key_retirement_spans_segments_and_honors_snapshot_and_budget() {
    let first = publication_fixture("key-retire-first", &["one", "two"]);
    let second = publication_fixture("key-retire-second", &["three", "four"]);
    let original = crate::pb::ProtobufSource {
        descriptor_set: include_bytes!("../../../tests/fixtures/protobuf-semantics/descriptor.bin")
            .to_vec(),
        message_type: "semantics.Doc".into(),
        payload: vec![8, 1],
    };
    let key = vec![0, 255, 0];
    for (fixture, version) in [(&first, 1), (&second, 2)] {
        let mut store = Bm25Store::load(&fixture.bm25).unwrap();
        for row in 0..2 {
            let identity = crate::pb::DocumentIdentity {
                document_key: key.clone(),
                version,
                chunk_ordinal: Some(row),
            };
            store
                .source_archive_mut()
                .attach_source_with_identity(row, &original, Some(row), Some(&identity))
                .unwrap();
        }
        store.save(&fixture.bm25).unwrap();
    }
    let root = first.root.join("catalog");
    let catalog = SegmentCatalog::open(&root).unwrap();
    catalog.append(source(&first, "first", 0)).unwrap();
    let before = catalog.append(source(&second, "second", 2)).unwrap();
    assert!(before
        .document_retirements(&key, 3)
        .unwrap_err()
        .contains("row budget"));
    assert!(before.document_retirements(&key, 0).is_err());
    assert!(before
        .document_retirements(&[0, 255], 4)
        .unwrap()
        .is_empty());
    let all = before.document_retirements(&key, 4).unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].rows, [0, 1]);
    assert_eq!(all[1].rows, [0, 1]);
    let partial = catalog
        .commit_rows(before.epoch(), &[retire("first", &[0])], vec![])
        .unwrap();
    let remaining = partial.document_retirements(&key, 3).unwrap();
    assert_eq!(remaining[0].rows, [1]);
    assert_eq!(remaining[1].rows, [0, 1]);
    assert_eq!(
        before.document_retirements(&key, 4).unwrap()[0].rows,
        [0, 1]
    );
    let gone = catalog
        .commit_rows(partial.epoch(), &remaining, vec![])
        .unwrap();
    assert!(gone.document_retirements(&key, 1).unwrap().is_empty());
    assert!(ids(&gone).is_empty());
    assert!(SegmentCatalog::open(&root)
        .unwrap()
        .snapshot()
        .document_retirements(&key, 1)
        .unwrap()
        .is_empty());
    assert_eq!(gone.bm25(0).document_identity(1).unwrap().document_key, key);
}

#[test]
fn concurrent_row_updates_have_one_epoch_winner() {
    let input = publication_fixture("retire-race", &["one", "two"]);
    let catalog = SegmentCatalog::open(input.root.join("catalog")).unwrap();
    catalog.append(source(&input, "old", 0)).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let handles: Vec<_> = [0, 1]
        .into_iter()
        .map(|row| {
            let catalog = catalog.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                catalog
                    .commit_rows(1, &[retire("old", &[row])], vec![])
                    .is_ok()
            })
        })
        .collect();
    assert_eq!(
        handles
            .into_iter()
            .map(|handle| usize::from(handle.join().unwrap()))
            .sum::<usize>(),
        1
    );
    assert_eq!(catalog.snapshot().epoch(), 2);
    assert_eq!(ids(&catalog.snapshot()).len(), 1);
}
