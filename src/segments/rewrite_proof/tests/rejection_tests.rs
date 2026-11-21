use super::*;

#[test]
fn rewrite_proof_rejects_changed_source_identity_and_every_stored_value_family() {
    let fixture = Fixture::new();
    let before = fixture.set("before", &[1, 2], &[], "", 2);
    for change in [
        "key",
        "version",
        "ordinal",
        "text",
        "lineage",
        "source",
        "missing_identity",
        "term",
        "tf",
        "span",
        "length",
        "position",
        "sentence",
        "facet",
        "double",
        "signed",
        "unsigned",
        "presence",
        "geo",
        "map_facet",
        "map_double",
        "map_signed",
        "map_unsigned",
        "vector",
        "analyzer",
        "schema",
    ] {
        let after = fixture.set(change, &[1, 2], &[], change, 1);
        let scratch = fixture.0.join("proof");
        assert!(
            before.verify_source_rewrite(&after, &scratch, 1).is_err(),
            "accepted changed {change}"
        );
        assert!(!scratch.exists());
    }
}

#[test]
fn rewrite_proof_refuses_duplicate_missing_or_unowned_rows_and_preserves_existing_scratch() {
    let fixture = Fixture::new();
    let before = fixture.set("before", &[1, 2], &[], "", 2);
    let duplicate = fixture.set("duplicate", &[1, 1], &[], "", 1);
    let missing = fixture.set("missing", &[1], &[], "", 1);
    let scratch = fixture.0.join("scratch");
    assert!(before
        .verify_source_rewrite(&duplicate, &scratch, 1)
        .unwrap_err()
        .contains("duplicate"));
    assert!(before
        .verify_source_rewrite(&missing, &scratch, 1)
        .unwrap_err()
        .contains("count"));
    assert!(before.verify_source_rewrite(&before, &scratch, 0).is_err());
    let mut manifest = before.published_manifest();
    manifest.source_owner = None;
    let unowned =
        OpenedSegmentSet::open_manifest(before.root().to_path_buf(), manifest, Default::default())
            .unwrap();
    assert!(before.verify_source_rewrite(&unowned, &scratch, 1).is_err());
    std::fs::create_dir(&scratch).unwrap();
    std::fs::write(scratch.join("keep"), b"user data").unwrap();
    assert!(before.verify_source_rewrite(&before, &scratch, 1).is_err());
    assert_eq!(std::fs::read(scratch.join("keep")).unwrap(), b"user data");
}

#[test]
fn rewrite_proof_batches_cross_posting_skip_boundaries() {
    let fixture = Fixture::new();
    let ids: Vec<u32> = (0..280).collect();
    let deleted: Vec<usize> = (0..280).step_by(5).collect();
    let survivors: Vec<u32> = ids.iter().copied().filter(|id| id % 5 != 0).rev().collect();
    let before = fixture.set("before", &ids, &deleted, "", 280);
    let after = fixture.set("after", &survivors, &[], "", 97);
    let mut expected = None;
    for batch in [13, 128, 1024] {
        let proof = before
            .verify_source_rewrite(&after, &fixture.0.join("scratch"), batch)
            .unwrap();
        assert_eq!(proof.live_rows, survivors.len() as u64);
        if let Some(expected) = &expected {
            assert_eq!(expected, &proof);
        } else {
            expected = Some(proof);
        }
    }
}
