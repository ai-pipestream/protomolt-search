use super::backup::{check_files, compact};
use super::*;
use crate::segments::OpenedSegmentSet;

/// Every retained row of `set`, encoded, after checking that each segment
/// retains vectors and each row holds both the 4D and 8D tiers.
fn retained_rows(set: &OpenedSegmentSet) -> Vec<(usize, u32, Vec<u8>)> {
    let mut expected = Vec::new();
    for segment in 0..set.len() {
        assert!(set.metadata(segment).multi_res.is_some());
        for row in 0..set.metadata(segment).rows as u32 {
            let retained = set.retained_segment(segment).retained(row).unwrap();
            assert!(retained.output.tier(4).is_some());
            assert!(retained.output.tier(8).is_some());
            expected.push((segment, row, crate::multi_res::encode(retained).unwrap()));
        }
    }
    expected
}

/// Retention must also survive source-owned maintenance, whose staged
/// manifest is assembled separately from ordinary shard compaction.
async fn assert_retention_survives_maintenance(f: &Fixture, before: &OpenedSegmentSet) {
    compact(f).await;
    let compacted = f
        .node
        .document_backup_catalog(&f.source, INDEX)
        .unwrap()
        .snapshot();
    assert_eq!(
        compacted
            .manifest()
            .segments
            .iter()
            .map(|s| s.rows)
            .sum::<u64>(),
        2
    );
    assert!(compacted
        .manifest()
        .segments
        .iter()
        .all(|s| s.multi_res.is_some()));
    before
        .verify_source_rewrite(&compacted, &f.root.join("retained-maintenance-proof"), 2)
        .unwrap();
}

#[tokio::test]
async fn retained_vector_artifacts_survive_source_backup_and_verified_restore() {
    let mut f = Fixture::new();
    // Keep the schema's full 8D vector as the active image, and require a
    // separate 4D tier so this exercises actual retained materialization.
    f.node.config.matryoshka_prefix_dims = vec![4, 8];
    f.publish_through_node(KEY, 1, Some(3)).await;
    f.publish_through_node(KEY, 2, Some(2)).await;
    let catalog = f.node.document_backup_catalog(&f.source, INDEX).unwrap();
    let before = catalog.snapshot();
    let expected = retained_rows(&before);
    assert_eq!(
        expected.len(),
        5,
        "retain historical tombstoned rows until compaction"
    );
    let bundle = f.root.join("retained-backup");
    let manifest = f
        .source
        .capture_backup(&[(INDEX, &catalog)], &backup_limits())
        .unwrap()
        .write_to(&bundle)
        .unwrap();
    check_files(&bundle, &manifest);
    let retained_files: Vec<_> = manifest
        .artifacts
        .iter()
        .filter(|artifact| artifact.file.ends_with("/vectors.multi"))
        .collect();
    assert_eq!(
        retained_files.len(),
        before.len(),
        "backup must inventory each retained artifact"
    );
    assert_retention_survives_maintenance(&f, &before).await;

    let destination = f.root.join("retained-restore");
    let request = restore_request(&manifest);
    let verified = DocumentCatalog::stage_backup_restore(&bundle, &destination, &request).unwrap();
    let restored =
        OpenedSegmentSet::open(destination.join(&manifest.indexes[0].directory)).unwrap();
    assert_eq!(restored.manifest(), before.manifest());
    for (segment, row, bytes) in expected {
        assert_eq!(
            crate::multi_res::encode(restored.retained_segment(segment).retained(row).unwrap())
                .unwrap(),
            bytes,
            "full/prefix/norm/code/sentence bits changed at segment {segment}, row {row}"
        );
    }
    before
        .verify_source_rewrite(&restored, &f.root.join("retained-restore-proof"), 2)
        .unwrap();
    drop(restored);
    drop(verified);
    assert!(!destination.exists());
    // A missing retained artifact is corruption even though every original
    // provider, FP32, BM25 and live-doc file is still present and checksummed.
    std::fs::remove_file(bundle.join(&retained_files[0].file)).unwrap();
    assert!(DocumentCatalog::stage_backup_restore(&bundle, &destination, &request).is_err());
    assert!(!destination.exists());
}
