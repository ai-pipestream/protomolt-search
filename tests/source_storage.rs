use pipestream_search::pb::ProtobufSource;
use pipestream_search::postings::{AnalyzedDoc, Bm25Reader, Bm25Store, SpillBuilder};

fn source() -> ProtobufSource {
    ProtobufSource {
        descriptor_set: include_bytes!("fixtures/protobuf-semantics/descriptor.bin").to_vec(),
        message_type: "semantics.Doc".into(),
        payload: vec![8, 0x81, 0, 0xa0, 6, 99],
    }
}

fn document() -> AnalyzedDoc {
    AnalyzedDoc::body(vec![("word".into(), 1, vec![(0, 4)])], 1)
}

#[test]
fn source_bytes_survive_heap_spill_and_mapped_storage() {
    let directory = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("source-storage-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let mut heap = Bm25Store::new();
    let mut spill = SpillBuilder::create(&directory.join("spill")).unwrap();
    let source = source();
    let heap_source = heap.source_archive_mut().insert(&source).unwrap();
    let spill_source = spill.source_archive_mut().insert(&source).unwrap();
    for row in 0..3 {
        heap.add_document(row, "word".into(), document());
        spill
            .add_document_with_lineage(row, "word".into(), document(), None)
            .unwrap();
        if row != 1 {
            heap.source_archive_mut()
                .attach(row, heap_source, Some(row))
                .unwrap();
            spill
                .source_archive_mut()
                .attach(row, spill_source, Some(row))
                .unwrap();
        }
    }
    let heap_path = directory.join("heap.bm25");
    let spill_path = directory.join("spill.bm25");
    heap.save(&heap_path).unwrap();
    spill.finish(&spill_path).unwrap();
    for path in [&heap_path, &spill_path] {
        let reader = Bm25Reader::open(path).unwrap();
        reader.verify_integrity().unwrap();
        let loaded = Bm25Store::load(path).unwrap();
        for row in 0..3 {
            let expected = (row != 1).then(|| (source.clone(), Some(row)));
            assert_eq!(reader.protobuf_source(row).unwrap(), expected);
            assert_eq!(loaded.protobuf_source(row).unwrap(), expected);
        }
        assert!(loaded.save_v5(&directory.join("old.bm25")).is_err());
        let mut bytes = Vec::new();
        assert!(loaded.write_v4_for_bench(&mut bytes).is_err());
        let rewritten = directory.join("rewritten.bm25");
        loaded.save(&rewritten).unwrap();
        assert_eq!(
            std::fs::read(path).unwrap(),
            std::fs::read(rewritten).unwrap()
        );
    }
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn zero_row_store_retains_originals() {
    let directory = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("source-empty-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let mut heap = Bm25Store::new();
    heap.source_archive_mut().insert(&source()).unwrap();
    let path = directory.join("empty.bm25");
    heap.save(&path).unwrap();
    Bm25Reader::open(&path).unwrap().verify_integrity().unwrap();
    let mut loaded = Bm25Store::load(&path).unwrap();
    assert!(!loaded.source_archive_mut().is_empty());
    std::fs::remove_dir_all(directory).unwrap();
}
