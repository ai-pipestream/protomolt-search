use super::*;

#[test]
fn retained_reader_gate_matches_for_cold_and_bitmap_only_reused_segments() {
    let input = publication_fixture("retained-bitmap-gate", &["one", "two"]);
    let root = input.root.join("catalog");
    let catalog = SegmentCatalog::open(&root).unwrap();
    let before = catalog.append(source(&input, "old", 0)).unwrap();
    let directory = SegmentCatalog::segment_dir(&root, "old");
    for required in [false, true] {
        let mut live = LiveDocs::default();
        live.delete(0);
        if required { live = live.with_retained_vectors(); }
        let path = directory.join("changed-live.bin");
        live.write(&path, 2).unwrap();
        let (bytes, sha256) = digest_file(&path).unwrap();
        let mut manifest = before.published_manifest();
        manifest.epoch += 1;
        manifest.segments[0].live_rows = 1;
        manifest.segments[0].live_docs = SegmentArtifact { file: "changed-live.bin".into(), bytes, sha256 };
        let cold = OpenedSegmentSet::open_manifest(root.clone(), manifest.clone(), Default::default());
        let warm = OpenedSegmentSet::open_manifest_reusing(root.clone(), manifest, Default::default(), Some(&before));
        if required {
            for result in [cold, warm] {
                assert!(result.err().expect("undeclared retention accepted").contains("requires a retained vector artifact"));
            }
        } else {
            assert!(cold.unwrap().live_docs(0).is_deleted(0));
            assert!(warm.unwrap().live_docs(0).is_deleted(0));
        }
    }
}
