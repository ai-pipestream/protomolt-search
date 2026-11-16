use super::*;

#[test]
fn compaction_publication_preserves_previously_pinned_epochs() {
    let fixture = fixture(
        "pinned-epoch",
        &[
            1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.8, 0.6, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        ],
        &["one", "two"],
    );
    let catalog = SegmentCatalog::open(fixture.root.join("catalog")).unwrap();
    let before = catalog.snapshot();
    let after = catalog.commit_current(7).unwrap();
    assert_eq!(before.epoch(), 0, "a held snapshot is immutable");
    assert_eq!(after.epoch(), 7);
    assert!(!Arc::ptr_eq(&before, &after));
    assert_eq!(catalog.snapshot().epoch(), 7);
    assert_eq!(
        SegmentCatalog::open(fixture.root.join("catalog"))
            .unwrap()
            .snapshot()
            .epoch(),
        7
    );
}

#[test]
fn failed_compaction_publication_does_not_consume_the_requested_epoch() {
    let fixture = fixture(
        "failed-epoch",
        &[
            1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.8, 0.6, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        ],
        &["one", "two"],
    );
    let root = fixture.root.join("catalog");
    let catalog = SegmentCatalog::open(&root).unwrap();
    catalog.commit_current(1).unwrap();
    let before = catalog.snapshot();
    let path = SegmentCatalog::manifest_path(&root);
    let backup = root.join("saved.json");
    std::fs::rename(&path, &backup).unwrap();
    std::fs::create_dir(&path).unwrap(); // Force manifest rename to fail.
    assert!(catalog.commit_current(2).is_err());
    assert_eq!(before.epoch(), 1);
    assert_eq!(catalog.snapshot().epoch(), 1);
    std::fs::remove_dir(&path).unwrap();
    std::fs::rename(&backup, &path).unwrap();
    assert_eq!(catalog.commit_current(2).unwrap().epoch(), 2);
}

#[test]
fn uncertain_append_keeps_published_artifacts_and_retries_without_duplicate_rows() {
    let data = publication_fixture("uncertain-append", &["one", "two"]);
    let root = data.root.join("catalog");
    let catalog = SegmentCatalog::open(&root).unwrap();
    let before = catalog.snapshot();
    FAIL_SET_SYNC.with(|fail| fail.set(true));
    assert!(catalog
        .append(source(&data, "s0", 0))
        .unwrap_err()
        .contains("after manifest rename"));
    assert_eq!(before.epoch(), 0);
    assert_eq!(catalog.snapshot().epoch(), 0);
    // The disk manifest already names s0: cleanup must retain it.
    assert!(SegmentCatalog::segment_dir(&root, "s0").exists());
    assert_eq!(
        SegmentCatalog::open(&root)
            .unwrap()
            .snapshot()
            .manifest()
            .segments
            .len(),
        1
    );
    // A different write cannot overwrite the disk manifest from a stale view.
    assert!(catalog
        .append(source(&data, "s1", 2))
        .unwrap_err()
        .contains("uncertain"));
    assert!(catalog
        .publish_partition_key(Some("year".into()))
        .unwrap_err()
        .contains("uncertain"));
    assert_eq!(
        SegmentCatalog::read_manifest(&root)
            .unwrap()
            .unwrap()
            .segments
            .len(),
        1
    );
    assert!(!SegmentCatalog::segment_dir(&root, "s1").exists());
    let recovered = catalog.append(source(&data, "s0", 0)).unwrap();
    assert_eq!(recovered.epoch(), 1);
    assert_eq!(recovered.manifest().segments.len(), 1);
    assert_eq!(recovered.manifest().segments[0].rows, 2);
    assert_eq!(catalog.append(source(&data, "s0", 0)).unwrap().epoch(), 1);
    assert_eq!(before.epoch(), 0);
}

#[test]
fn segment_retry_refuses_changed_content_generation_range_and_damaged_artifacts() {
    let data = publication_fixture("retry-source", &["one", "two"]);
    let changed = publication_fixture("retry-changed", &["other", "two"]);
    let root = data.root.join("catalog");
    let catalog = SegmentCatalog::open(&root).unwrap();
    catalog.append(source(&data, "s0", 0)).unwrap();
    assert!(catalog.append(source(&changed, "s0", 0)).is_err());
    assert!(catalog
        .append(source_generation(&data, "s0", 0, 2))
        .is_err());
    assert!(catalog.append(source(&data, "s0", 2)).is_err());
    assert_eq!(catalog.snapshot().epoch(), 1);
    let vector = SegmentCatalog::segment_dir(&root, "s0").join("vector.index");
    let original = std::fs::read(&vector).unwrap();
    let mut damaged = original.clone();
    let last = damaged.len() - 1;
    damaged[last] ^= 1;
    std::fs::write(&vector, &damaged).unwrap();
    assert!(catalog.append(source(&data, "s0", 0)).is_err());
    std::fs::write(&vector, original).unwrap();
    assert_eq!(catalog.append(source(&data, "s0", 0)).unwrap().epoch(), 1);
}

#[test]
fn uncertain_compaction_keeps_outputs_needed_to_reopen_the_published_set() {
    let data = publication_fixture("uncertain-compact", &["one", "two"]);
    let root = data.root.join("catalog");
    let catalog = SegmentCatalog::open(&root).unwrap();
    let before = catalog.append(source(&data, "input", 0)).unwrap();
    FAIL_SET_SYNC.with(|fail| fail.set(true));
    assert!(catalog
        .replace_for_compaction(&["input".into()], source_generation(&data, "output", 0, 2))
        .is_err());
    assert_eq!(before.epoch(), 1);
    assert_eq!(
        catalog.snapshot().manifest().segments[0].segment_id,
        "input"
    );
    let reopened = SegmentCatalog::open(&root).unwrap();
    assert_eq!(reopened.snapshot().epoch(), 2);
    assert_eq!(
        reopened.snapshot().manifest().segments[0].segment_id,
        "output"
    );
    assert!(SegmentCatalog::segment_dir(&root, "input").exists());
    assert!(SegmentCatalog::segment_dir(&root, "output").exists());
}

#[test]
fn staging_refuses_an_existing_private_directory_without_touching_its_files() {
    let data = publication_fixture("stage-collision", &["one", "two"]);
    let root = data.root.join("catalog");
    let catalog = SegmentCatalog::open(&root).unwrap();
    let private = root
        .join("segments")
        .join(format!(".tmp-output-{}-1", std::process::id()));
    std::fs::create_dir(&private).unwrap();
    let marker = private.join("in-progress");
    std::fs::write(&marker, b"another builder's work").unwrap();

    let result = stage_segment(&root, source(&data, "output", 0));
    assert!(
        result.is_err(),
        "another builder's directory must be refused"
    );
    assert_eq!(std::fs::read(&marker).unwrap(), b"another builder's work");
    assert!(!SegmentCatalog::segment_dir(&root, "output").exists());
    assert_eq!(catalog.snapshot().epoch(), 0);

    // Only its owner may remove the private directory. A later retry can
    // then stage normally, without publishing a manifest on its own.
    std::fs::remove_dir_all(&private).unwrap();
    let staged = stage_segment(&root, source(&data, "output", 0)).unwrap();
    assert_eq!(staged.rows, 2);
    assert_eq!(catalog.snapshot().epoch(), 0);
}

#[test]
fn cleanup_retains_artifacts_when_it_cannot_establish_the_committed_manifest() {
    let data = publication_fixture("uncertain-cleanup", &["one", "two"]);
    let root = data.root.join("catalog");
    let catalog = SegmentCatalog::open(&root).unwrap();
    let metadata = stage_segment(&root, source(&data, "orphan", 0)).unwrap();
    std::fs::write(SegmentCatalog::manifest_path(&root), b"broken JSON").unwrap();
    cleanup_staged(&root, std::slice::from_ref(&metadata));
    assert!(SegmentCatalog::segment_dir(&root, "orphan").exists());
    assert!(catalog.append(source(&data, "orphan", 0)).is_err());
    std::fs::remove_file(SegmentCatalog::manifest_path(&root)).unwrap();
    cleanup_staged(&root, &[metadata]);
    assert!(!SegmentCatalog::segment_dir(&root, "orphan").exists());
}

#[test]
fn append_publishes_aligned_segments_and_queries_one_global_order() {
    let a = fixture(
        "a",
        &[
            1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.8, 0.6, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        ],
        &["alpha", "shared"],
    );
    let b = fixture(
        "b",
        &[
            1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.8, 0.6, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        ],
        &["shared", "beta"],
    );
    let catalog = SegmentCatalog::open(a.root.join("catalog")).unwrap();
    let old = catalog.snapshot();
    let after_first = catalog.append(source(&a, "s0", 0)).unwrap();
    let current = catalog.append(source(&b, "s1", 2)).unwrap();
    assert!(old.manifest().segments.is_empty());
    assert_eq!(current.manifest().epoch, 2);
    assert!(
        current.shares_segment(0, &after_first, 0),
        "a publish keeps the segments it already had open"
    );
    let vector = current
        .search_vector(&[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 3)
        .unwrap();
    assert_eq!(
        vector.iter().map(|hit| hit.doc_id).collect::<Vec<_>>(),
        vec![0, 2, 1]
    );
    let lexical = current
        .search_bm25(&["shared".into()], Bm25Params::default(), 10)
        .unwrap();
    assert_eq!(
        lexical.iter().map(|hit| hit.doc_id).collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[test]
fn overlap_and_corruption_fail_before_publication() {
    let values = [
        1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
    ];
    let a = fixture("overlap-a", &values, &["a", "b"]);
    let b = fixture("overlap-b", &values, &["c", "d"]);
    let catalog = SegmentCatalog::open(a.root.join("catalog")).unwrap();
    catalog.append(source(&a, "s0", 0)).unwrap();
    let error = catalog.append(source(&b, "s1", 1)).unwrap_err();
    assert!(error.contains("overlaps"), "{error}");
    assert_eq!(catalog.snapshot().manifest().segments.len(), 1);
    let vector_path = a.root.join("catalog/segments/s0/vector.index");
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(vector_path)
        .unwrap();
    file.write_all(b"corrupt").unwrap();
    let error = OpenedSegmentSet::open(a.root.join("catalog")).unwrap_err();
    assert!(error.contains("integrity mismatch"), "{error}");
}

#[test]
fn tombstones_are_removed_from_global_stats() {
    let values = [
        1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
    ];
    let fixture = fixture("live-stats", &values, &["gone", "live"]);
    let mut live = LiveDocs::default();
    assert!(live.delete(0));
    live.write(&fixture.live_docs, 2).unwrap();
    let catalog = SegmentCatalog::open(fixture.root.join("catalog")).unwrap();
    let snapshot = catalog.append(source(&fixture, "s0", 0)).unwrap();
    let stats = snapshot
        .global_bm25_stats(&["gone".into(), "live".into()])
        .unwrap();
    assert_eq!(stats.doc_count, 1);
    assert_eq!(stats.total_doc_length, 2);
    assert_eq!(stats.dfs, vec![0, 1]);
    assert!(snapshot
        .search_bm25(&["gone".into()], Bm25Params::default(), 10)
        .unwrap()
        .is_empty());
}

#[test]
fn compaction_can_publish_multiple_dense_outputs_atomically() {
    let values = [
        1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
    ];
    let input_a = fixture("compact-input-a", &values, &["a", "b"]);
    let input_b = fixture("compact-input-b", &values, &["c", "d"]);
    let output_a = fixture("compact-output-a", &values, &["a", "b"]);
    let output_b = fixture("compact-output-b", &values, &["c", "d"]);
    let catalog = SegmentCatalog::open(input_a.root.join("catalog")).unwrap();
    catalog.append(source(&input_a, "in-a", 0)).unwrap();
    catalog.append(source(&input_b, "in-b", 2)).unwrap();
    let before = catalog.snapshot();
    let after = catalog
        .replace_many_for_compaction(
            &["in-a".into(), "in-b".into()],
            vec![
                source_generation(&output_a, "out-a", 100, 2),
                source_generation(&output_b, "out-b", 102, 2),
            ],
            None,
        )
        .unwrap();
    assert_eq!(before.manifest().segments.len(), 2);
    assert_eq!(after.manifest().epoch, 3);
    assert_eq!(
        after
            .manifest()
            .segments
            .iter()
            .map(|segment| segment.segment_id.as_str())
            .collect::<Vec<_>>(),
        vec!["out-a", "out-b"]
    );
}
