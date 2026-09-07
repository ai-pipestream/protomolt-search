use super::*;
use crate::pb::{DocumentIdentity, ProtobufSource};
use crate::segments::{SegmentCatalog, SegmentGenerationDeclaration, SegmentSourceOwner};

#[tokio::test]
async fn mixed_vectors_typed_values_and_analysis_survive_bounded_rebuilding() {
    let directory = Directory::create(&std::env::temp_dir()).unwrap();
    let root = directory.0.join("catalog");
    let catalog = SegmentCatalog::open(&root).unwrap();
    let config = NodeConfig {
        collection: "books".into(),
        wal: false,
        bm25_fields: vec!["body".into(), "empty".into()],
        position_fields: vec!["body".into()],
        sentence_fields: vec!["body".into()],
        facet_fields: vec!["facet".into(), "empty_facet".into()],
        numeric_fields: vec!["double".into()],
        integer_fields: vec!["signed".into()],
        unsigned_integer_fields: vec!["unsigned".into()],
        geo_fields: vec!["point".into()],
        map_facet_fields: vec!["labels".into()],
        map_numeric_fields: vec!["doubles".into()],
        map_integer_fields: vec!["signed_map".into()],
        map_unsigned_integer_fields: vec!["unsigned_map".into()],
        ..Default::default()
    };
    let sample: Vec<f32> = (0..64).map(|i| (i as f32 * 0.3).sin()).collect();
    let backend = VectorIndex::fit_backend_config(&config.vector_backend, 8, 4, &sample).unwrap();
    let mut base = 0;
    for (segment, (count, has_vectors)) in [(3, true), (2, false), (2, true), (2, true)]
        .into_iter()
        .enumerate()
    {
        let path = directory.0.join(format!("source-{segment}"));
        std::fs::create_dir(&path).unwrap();
        let mut store = heap_store(&config).unwrap();
        store.set_analysis_fingerprint(0, 11).unwrap();
        store.set_analysis_fingerprint(1, 22).unwrap();
        let mut vectors = Vec::new();
        let mut live = LiveDocs::default();
        for row in 0..count {
            let id = base + row;
            store.add_document(
                row,
                "red red".into(),
                AnalyzedDoc {
                    fields: vec![
                        AnalyzedField {
                            terms: vec![("red".into(), 2, vec![(0, 3), (4, 7)])],
                            length: 2,
                            positions: Some(vec![vec![0, 1]]),
                            sentences: Some(vec![(0, 7)]),
                        },
                        AnalyzedField::default(),
                    ],
                    ..Default::default()
                },
            );
            let identity = DocumentIdentity {
                document_key: format!("key-{id}").into_bytes(),
                version: 7,
                chunk_ordinal: Some(0),
            };
            let source = ProtobufSource {
                descriptor_set: include_bytes!(
                    "../../../../tests/fixtures/unsigned-mapping/descriptor.bin"
                )
                .to_vec(),
                message_type: "unsigned_mapping.Parent".into(),
                payload: [
                    vec![9],
                    (id as u64).to_le_bytes().to_vec(),
                    vec![0xa0, 6, 0x81, 0],
                ]
                .concat(),
            };
            store
                .source_archive_mut()
                .attach_source_with_identity(row, &source, Some(0), Some(&identity))
                .unwrap();
            store.set_facet(0, row, "");
            store.set_numeric(0, row, -0.0);
            store.set_integer(0, row, i64::MIN);
            if id != 4 {
                store.set_unsigned_integer(0, row, if id == 3 { 0 } else { u64::MAX });
            }
            store.set_geo(0, row, -0.0, 2.0);
            store.set_map_facet(0, row, "", "");
            store.set_map_numeric(0, row, "", -0.0);
            store.set_map_integer(0, row, "", i64::MIN).unwrap();
            store
                .set_map_unsigned_integer(0, row, "", u64::MAX)
                .unwrap();
            vectors.extend((0..8).map(|d| (id as f32 + d as f32) / 8.0));
            if id == 1 {
                live.delete(row as usize);
            }
        }
        store.save(&path.join("documents.bm25")).unwrap();
        live.write(&path.join("live-docs.bin"), count as u64)
            .unwrap();
        if has_vectors {
            let mut provider = VectorIndex::from_backend_config(8, &backend).unwrap();
            provider.add(&vectors, 8).unwrap();
            provider.prepare().unwrap();
            provider.write(&path.join("vector.index")).unwrap();
            ExactVectorStore::from_values(8, vectors)
                .unwrap()
                .write(&path.join("vectors.f32"))
                .unwrap();
        }
        catalog
            .append(SegmentSource {
                segment_id: &format!("input-{segment}"),
                generation: 1,
                base_label: base as u64,
                backend_kind: &config.vector_backend,
                vector_path: has_vectors.then_some(path.join("vector.index")).as_deref(),
                exact_vector_path: has_vectors.then_some(path.join("vectors.f32")).as_deref(),
                bm25_path: &path.join("documents.bm25"),
                live_docs_path: &path.join("live-docs.bin"),
                partition_column: None,
            })
            .unwrap();
        base += count;
    }
    let mut manifest = catalog.snapshot().published_manifest();
    manifest.format = 4;
    manifest.source_owner = Some(
        SegmentSourceOwner::encode(&crate::pb::storage::SourceIndexOwner {
            format_version: 1,
            history_id: vec![1; 16],
            index_key: b"index".to_vec(),
            collection: "books".into(),
        })
        .unwrap(),
    );
    manifest.generation_declaration = Some(
        SegmentGenerationDeclaration::encode(
            &generation::from_reader(catalog.snapshot().bm25(0), Some((8, backend))).unwrap(),
        )
        .unwrap(),
    );
    std::fs::write(
        root.join("segments.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    let before = Arc::new(OpenedSegmentSet::open(root).unwrap());
    let node = NodeServiceImpl::new(None, config);
    let request = CompactDocumentIndexRequest {
        index_key: b"index".to_vec(),
        batch_rows: 4,
        batch_bytes: 256 * 1024,
        max_staged_bytes: 1024 * 1024,
        proof_batch_rows: 2,
    };
    let mut builder = Builder {
        node: &node,
        before: &before,
        directory: Directory::create(&directory.0).unwrap(),
        outputs: vec![],
        pending: vec![],
        bytes: 0,
        written: 0,
        next_row: 0,
        request: &request,
    };
    builder.build().unwrap();
    assert_eq!(builder.outputs.len(), 3);
    assert_eq!(
        builder.outputs.iter().map(|o| o.vector).collect::<Vec<_>>(),
        vec![true, false, true]
    );
    assert_eq!(builder.next_row, 8);
    let paths: Vec<_> = builder
        .outputs
        .iter()
        .map(|o| {
            [
                o.directory.join("vector.index"),
                o.directory.join("vectors.f32"),
                o.directory.join("documents.bm25"),
                o.directory.join("live-docs.bin"),
            ]
        })
        .collect();
    let staged = catalog
        .stage_maintenance(
            before.clone(),
            builder
                .outputs
                .iter()
                .zip(&paths)
                .map(|(o, p)| SegmentSource {
                    segment_id: &o.id,
                    generation: before.epoch() + 1,
                    base_label: o.base,
                    backend_kind: &node.config.vector_backend,
                    vector_path: o.vector.then_some(p[0].as_path()),
                    exact_vector_path: o.vector.then_some(p[1].as_path()),
                    bm25_path: &p[2],
                    live_docs_path: &p[3],
                    partition_column: None,
                })
                .collect(),
        )
        .unwrap();
    let proof = before
        .verify_source_rewrite(staged.snapshot(), &directory.0.join("proof"), 2)
        .unwrap();
    assert_eq!(proof.live_rows, 8);
    let after = staged.snapshot();
    assert_eq!(after.bm25(1).unsigned_integer_value(0, 0), Some(0));
    assert_eq!(after.bm25(1).unsigned_integer_value(0, 1), None);
    assert_eq!(
        after.bm25(0).numeric_value(0, 0).unwrap().to_bits(),
        (-0.0f64).to_bits()
    );
    assert_eq!(
        after.bm25(2).document_identity(3).unwrap().document_key,
        b"key-8"
    );
    assert!(after.exact_vectors(1).is_none());
}
