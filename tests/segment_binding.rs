use pipestream_search::{
    mapping::derive_plan,
    postings::{AnalyzedDoc, Bm25Store, StoredBinding},
    segments::{OpenedSegmentSet, SegmentBinding, SegmentCatalog, SegmentSource},
};
use prost::Message;
use std::path::{Path, PathBuf};

fn root(tag: &str) -> PathBuf {
    let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("segment-binding-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn binding() -> StoredBinding {
    let plan = derive_plan(
        include_bytes!("fixtures/vector-binding/descriptor.bin"),
        "vector_binding.Named",
    )
    .unwrap();
    StoredBinding {
        plan_fingerprint: plan.fingerprint,
        body_path: "body".into(),
        vector_binding: plan.vector_binding.unwrap().encode_to_vec(),
        ..Default::default()
    }
}

#[test]
fn empty_catalog_binding_is_atomic_idempotent_and_validated_on_open() {
    let root = root("empty");
    let catalog = SegmentCatalog::open(&root).unwrap();
    assert_eq!(catalog.snapshot().manifest().format, 1);
    let binding = binding();
    let pinned = catalog.publish_binding(&binding).unwrap();
    assert!(pinned.is_empty());
    assert_eq!(pinned.manifest().format, 2);
    assert_eq!(pinned.binding(), Some(&binding));
    assert_eq!(
        catalog.publish_binding(&binding).unwrap().epoch(),
        pinned.epoch()
    );
    let mut different = binding.clone();
    different.materialize_sha = "0".repeat(64);
    assert!(catalog.publish_binding(&different).is_err());
    assert_eq!(
        OpenedSegmentSet::open(&root).unwrap().binding(),
        Some(&binding)
    );
    let canonical = pinned.published_manifest();
    for case in 0..7 {
        let mut bad = canonical.clone();
        match case {
            0 => bad.format = 1,
            1 => bad.format = 3,
            2 => bad.binding = None,
            3 => bad.binding.as_mut().unwrap().sha256 = "0".repeat(64),
            _ => {
                let record = bad.binding.as_mut().unwrap();
                match case {
                    4 => record.protobuf.extend([80, 1]),       // unknown field
                    5 => record.protobuf.extend([10, 1, b'x']), // duplicate plan
                    _ => record.protobuf.clear(),
                }
                record.sha256 = pipestream_search::sha256::hex_digest(&record.protobuf);
            }
        }
        pipestream_search::segments::write_manifest_file(&root.join("segments.json"), &bad)
            .unwrap();
        assert!(OpenedSegmentSet::open(&root).is_err(), "case {case}");
    }
    pipestream_search::segments::write_manifest_file(&root.join("segments.json"), &canonical)
        .unwrap();
    assert_eq!(
        OpenedSegmentSet::open(&root).unwrap().binding(),
        Some(&binding)
    );
    std::fs::remove_dir_all(root).unwrap();
}

fn append(
    catalog: &SegmentCatalog,
    work: &Path,
    id: &str,
    base: u64,
    binding: Option<StoredBinding>,
    deleted: bool,
) -> Result<(), String> {
    let bm25 = work.join(format!("{id}.bm25"));
    let live = work.join(format!("{id}.live"));
    let mut store = Bm25Store::new();
    store.set_binding(binding);
    store.add_document(
        0,
        "word".into(),
        AnalyzedDoc::body(vec![("word".into(), 1, vec![(0, 4)])], 1),
    );
    store.save(&bm25).unwrap();
    let mut bitmap = pipestream_search::live_docs::LiveDocs::default();
    if deleted {
        bitmap.delete(0);
    }
    bitmap.write(&live, 1).unwrap();
    catalog
        .append(SegmentSource {
            segment_id: id,
            generation: 1,
            base_label: base,
            backend_kind: "",
            vector_path: None,
            exact_vector_path: None,
            bm25_path: &bm25,
            live_docs_path: &live,
            partition_column: None,
        })
        .map(|_| ())
}

#[test]
fn mixed_bindings_cannot_be_published_and_last_segment_removal_keeps_identity() {
    let root = root("mixed");
    for declared in [false, true] {
        let directory = root.join(format!("catalog-{declared}"));
        let catalog = SegmentCatalog::open(&directory).unwrap();
        let expected = binding();
        if declared {
            catalog.publish_binding(&expected).unwrap();
        }
        // A legacy catalog can derive its binding from its first bound image.
        append(&catalog, &root, "first", 0, Some(expected.clone()), true).unwrap();
        let previous = catalog.snapshot().published_manifest();
        assert!(append(&catalog, &root, "unbound", 1, None, false).is_err());
        let mut other = expected.clone();
        other.materialize_sha = "1".repeat(64);
        assert!(append(&catalog, &root, "different", 1, Some(other), false).is_err());
        assert_eq!(catalog.snapshot().published_manifest(), previous);
        let empty = catalog
            .replace_many_for_compaction(&["first".into()], vec![], None)
            .unwrap();
        assert!(empty.is_empty());
        assert_eq!(empty.binding(), Some(&expected));
        assert_eq!(empty.manifest().format, 2);
        assert_eq!(
            OpenedSegmentSet::open(&directory).unwrap().binding(),
            Some(&expected)
        );
    }
    let raw = SegmentCatalog::open(root.join("raw")).unwrap();
    append(&raw, &root, "raw-first", 0, None, false).unwrap();
    let mut bound_tail = Bm25Store::new();
    bound_tail.set_binding(Some(binding()));
    assert!(
        pipestream_search::segmented::SegmentedShard::open_catalog(raw.clone(), bound_tail)
            .is_err()
    );
    assert!(append(&raw, &root, "claimed", 1, Some(binding()), false).is_err());
    assert!(raw.publish_binding(&binding()).is_err());
    assert!(raw
        .replace_many_for_compaction(&["raw-first".into()], vec![], None)
        .is_err());
    assert_eq!(raw.snapshot().len(), 1);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn catalog_record_uses_the_same_protobuf_binding_as_wal() {
    let expected = binding();
    let record = SegmentBinding::encode(&expected).unwrap();
    let wire =
        pipestream_search::pb::wal::LoggedBinding::decode(record.protobuf.as_slice()).unwrap();
    assert_eq!(wire.vector_binding, expected.vector_binding);
    assert_eq!(wire.plan_fingerprint, expected.plan_fingerprint);
    assert_eq!(record.decode().unwrap(), expected);
}
