//! SpillBuilder output is byte-identical to the heap store's.

use super::format_v6_tests::two_field_corpus;
use super::store_tests::synthetic_corpus;
use super::*;

#[test]
fn spill_builder_output_is_byte_identical_to_store() {
    let base = std::env::temp_dir().join(format!("spill-eq-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let mut store = Bm25Store::new();
    // Tiny buffer forces many runs, so the merge path is exercised
    // with terms split across runs.
    let mut builder = SpillBuilder::create(&base.join("build"))
        .unwrap()
        .with_buffer_bytes(256);
    for (id, text, doc, lineage) in synthetic_corpus() {
        store.add_document_with_lineage(id, text.clone(), doc.clone(), lineage);
        builder
            .add_document_with_lineage(id, text, doc, lineage)
            .unwrap();
    }
    assert_eq!(store.doc_count(), builder.doc_count());
    assert_eq!(store.total_doc_length(), builder.total_doc_length());
    assert_eq!(store.next_doc_id(), builder.next_doc_id());

    let store_path = base.join("store.bm25");
    let spill_path = base.join("spill.bm25");
    store.save(&store_path).unwrap();
    builder.finish(&spill_path).unwrap();

    let a = std::fs::read(&store_path).unwrap();
    let b = std::fs::read(&spill_path).unwrap();
    assert_eq!(a.len(), b.len(), "file sizes differ");
    assert!(a == b, "spill output is not byte-identical to the store");
    // Spill scratch is cleaned up on success.
    assert!(!base.join("build").exists());

    // And the reader serves it.
    let reader = Bm25Reader::open(&spill_path).unwrap();
    assert_eq!(Bm25Index::doc_count(&reader), store.doc_count());
    assert_eq!(reader.df("court"), store.df("court"));
    let mut seen = Vec::new();
    reader.for_each_posting("vector", &mut |doc_id, tf, offsets| {
        seen.push((doc_id, tf, offsets.to_vec()));
    });
    let expected: Vec<_> = store
        .postings("vector")
        .unwrap()
        .iter()
        .map(|p| (p.doc_id, p.tf, p.offsets.clone()))
        .collect();
    assert_eq!(seen, expected);

    let _ = std::fs::remove_dir_all(&base);
}

/// The dual-writer contract on a MULTI-FIELD corpus: the spill
/// builder's v6 file is byte-identical to the store writer's, with
/// a tiny buffer forcing per-field multi-run merges.
#[test]
fn spill_builder_v6_multi_field_byte_identical() {
    let base = std::env::temp_dir().join(format!("spill-eq-v6mf-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let mut store = Bm25Store::with_fields(&["body", "case_name"]);
    let mut builder = SpillBuilder::create_with_fields(&base.join("build"), &["body", "case_name"])
        .unwrap()
        .with_buffer_bytes(128);
    for (id, text, doc, lineage) in two_field_corpus() {
        store.add_document_with_lineage(id, text.clone(), doc.clone(), lineage);
        builder
            .add_document_with_lineage(id, text, doc, lineage)
            .unwrap();
    }
    assert_eq!(store.doc_count(), builder.doc_count());
    assert_eq!(store.total_doc_length(), builder.total_doc_length());
    assert_eq!(store.next_doc_id(), builder.next_doc_id());

    let store_path = base.join("store.bm25");
    let spill_path = base.join("spill.bm25");
    store.save(&store_path).unwrap();
    builder.finish(&spill_path).unwrap();
    let a = std::fs::read(&store_path).unwrap();
    let b = std::fs::read(&spill_path).unwrap();
    assert_eq!(a.len(), b.len(), "file sizes differ");
    assert!(
        a == b,
        "multi-field spill output is not byte-identical to the store"
    );
    assert!(!base.join("build").exists());

    // And the reader serves both fields.
    let reader = Bm25Reader::open(&spill_path).unwrap();
    assert_eq!(reader.field_count(), 2);
    assert_eq!(reader.field(1).df("smith"), store.field(1).df("smith"));
    assert_eq!(reader.field(0).df("t1"), store.field(0).df("t1"));
    let _ = std::fs::remove_dir_all(&base);
}

fn full_schema_derived() -> StoredDerived {
    StoredDerived::of(
        &crate::derived::Declaration::compile(&crate::pb::DerivedColumns {
            columns: vec![crate::pb::DerivedColumn {
                name: "year".into(),
                expression: "calendar.year(decided)".into(),
                kind: crate::pb::MaterializeKind::I64 as i32,
                disclosure: crate::pb::DerivedDisclosure::Inputs as i32,
            }],
        })
        .unwrap(),
    )
}

fn full_schema_doc(idx: usize) -> (u32, String, AnalyzedDoc, Option<DocLineage>) {
    let doc_id = [1u32, 4, 6, 9][idx];
    let text = format!("rust vector court sentence {idx}. second span {idx}.");
    let s1_end = text.find('.').unwrap() as u32 + 1;
    let text_len = text.len() as u32;
    let body = AnalyzedField {
        terms: vec![
            ("court".into(), 1, vec![(12, 17)]),
            ("rust".into(), 2, vec![(0, 4), (s1_end + 1, s1_end + 7)]),
            ("vector".into(), 1, vec![(5, 11)]),
        ],
        length: 4,
        positions: Some(vec![vec![2], vec![0, 4], vec![1]]),
        sentences: Some(vec![(0, s1_end), (s1_end + 1, text_len)]),
        pos_lengths: Some(vec![vec![1], vec![2, 1], vec![3]]),
        splade: vec![
            ("rust".into(), 400 + idx as u16 * 10),
            ("semantic".into(), 150 + idx as u16 * 5),
        ],
        splade_scale_bits: 0.05f32.to_bits(),
    };
    body.check_sentences().unwrap();
    body.check_positions().unwrap();
    let title = AnalyzedField {
        terms: vec![(format!("heading_{}", idx % 2), 1, vec![(0, 7)])],
        length: 1,
        positions: Some(vec![vec![0]]),
        sentences: None,
        pos_lengths: None,
        splade: Vec::new(),
        splade_scale_bits: 0,
    };
    let doc = AnalyzedDoc {
        fields: vec![body, title],
        ..AnalyzedDoc::default()
    };
    let lineage = idx.is_multiple_of(2).then_some(DocLineage {
        parent_id: 500 + idx as u64,
        group_id: 900 + idx as u64,
        span_start: idx as u32 * 20,
        span_end: idx as u32 * 20 + text_len,
    });
    (doc_id, text, doc, lineage)
}

fn populate_store_and_spill(store: &mut Bm25Store, spill: &mut SpillBuilder) {
    let binding = StoredBinding {
        plan_fingerprint: "fp-plan-137".into(),
        body_path: "doc.body".into(),
        materialize_sha: "sha256-materialize-137".into(),
        analysis_sha: String::new(),
        analysis_contract: Vec::new(),
        vector_binding: Vec::new(),
        index_contract: Vec::new(),
    };
    let derived = full_schema_derived();
    store.set_binding(Some(binding.clone()));
    spill.set_binding(Some(binding));
    store.set_derived(Some(derived.clone()));
    spill.set_derived(Some(derived));
    store.set_analysis_fingerprint(0, 0x000A_11CE_0001).unwrap();
    spill.set_analysis_fingerprint(0, 0x000A_11CE_0001).unwrap();
    store.set_analysis_fingerprint(1, 0x000A_11CE_0002).unwrap();
    spill.set_analysis_fingerprint(1, 0x000A_11CE_0002).unwrap();

    for idx in 0..4 {
        let (doc_id, text, doc, lineage) = full_schema_doc(idx);
        store.add_document_with_lineage(doc_id, text.clone(), doc.clone(), lineage);
        spill
            .add_document_with_lineage(doc_id, text, doc, lineage)
            .unwrap();

        let src = crate::pb::ProtobufSource {
            descriptor_set: b"descriptor-bytes-v1".to_vec(),
            message_type: "test.Document".into(),
            payload: format!("payload-{doc_id}").into_bytes(),
        };
        let s1 = store.source_archive_mut().insert(&src).unwrap();
        store
            .source_archive_mut()
            .attach(doc_id, s1, Some(idx as u32))
            .unwrap();
        let s2 = spill.source_archive_mut().insert(&src).unwrap();
        spill
            .source_archive_mut()
            .attach(doc_id, s2, Some(idx as u32))
            .unwrap();

        let court = if idx.is_multiple_of(2) {
            "scotus"
        } else {
            "ca9"
        };
        store.set_facet(0, doc_id, court);
        spill.set_facet(0, doc_id, court);
        store.set_numeric(0, doc_id, (idx as f64 - 1.5) * 3.25);
        spill.set_numeric(0, doc_id, (idx as f64 - 1.5) * 3.25);
        let signed = if idx == 0 { i64::MIN } else { idx as i64 * -17 };
        store.set_integer(0, doc_id, signed);
        spill.set_integer(0, doc_id, signed);
        let unsigned = if idx == 3 {
            u64::MAX
        } else {
            idx as u64 * 1024
        };
        store.set_unsigned_integer(0, doc_id, unsigned);
        spill.set_unsigned_integer(0, doc_id, unsigned);
        let (lat, lon) = (37.5 + idx as f64 * 0.25, -122.0 + idx as f64 * 0.5);
        store.set_geo(0, doc_id, lat, lon);
        spill.set_geo(0, doc_id, lat, lon);
        store.set_map_facet(0, doc_id, "tier", if idx < 2 { "gold" } else { "silver" });
        spill.set_map_facet(0, doc_id, "tier", if idx < 2 { "gold" } else { "silver" });
        store.set_map_numeric(0, doc_id, "p99", 10.0 + idx as f64);
        spill.set_map_numeric(0, doc_id, "p99", 10.0 + idx as f64);
        store.set_map_integer(0, doc_id, "delta", signed).unwrap();
        spill.set_map_integer(0, doc_id, "delta", signed).unwrap();
        store
            .set_map_unsigned_integer(0, doc_id, "bytes", unsigned)
            .unwrap();
        spill
            .set_map_unsigned_integer(0, doc_id, "bytes", unsigned)
            .unwrap();
    }
}

#[test]
fn store_and_spill_are_byte_identical_across_all_columns_and_v8_sections() {
    let dir = std::env::temp_dir().join(format!("tvbm25_137_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let kinds = [
        (ColumnKind::Facet, "court"),
        (ColumnKind::Numeric, "score"),
        (ColumnKind::Integer, "decided"),
        (ColumnKind::UnsignedInteger, "epoch_ns"),
        (ColumnKind::Geo, "origin"),
        (ColumnKind::MapFacet, "labels"),
        (ColumnKind::MapNumeric, "metrics"),
        (ColumnKind::MapInteger, "counters"),
        (ColumnKind::MapUnsignedInteger, "quotas"),
    ];
    let mut store = Bm25Store::with_text_fields(&["body", "title"])
        .with_positions(&["body", "title"])
        .with_sentences(&["body"]);
    let mut spill =
        SpillBuilder::create_with_text_fields(&dir.join("spill_work"), &["body", "title"])
            .unwrap()
            .with_buffer_bytes(64)
            .with_position_fields(&["body", "title"])
            .with_sentence_fields(&["body"]);
    for (kind, name) in kinds {
        store = store.with_columns(kind, &[name]);
        spill = spill.with_columns(kind, &[name]);
        assert_eq!(store.column_count(kind), 1);
        assert_eq!(spill.column_count(kind), 1);
        assert_eq!(store.column_name(kind, 0), name);
        assert_eq!(spill.column_name(kind, 0), name);
        assert_eq!(store.column_index(kind, name), Some(0));
        assert_eq!(spill.column_index(kind, name), Some(0));
    }

    populate_store_and_spill(&mut store, &mut spill);
    let store_path = dir.join("from_store.bm25");
    let spill_path = dir.join("from_spill.bm25");
    store.save(&store_path).unwrap();
    spill.finish(&spill_path).unwrap();

    let store_bytes = std::fs::read(&store_path).unwrap();
    let spill_bytes = std::fs::read(&spill_path).unwrap();
    assert_eq!(
        store_bytes, spill_bytes,
        "store and spill .bm25 bytes diverged"
    );

    let reader = Bm25Reader::open(&spill_path).unwrap();
    reader.verify_integrity().unwrap();
    assert_eq!(reader.integer_value(0, 1), Some(i64::MIN));
    assert_eq!(reader.unsigned_integer_value(0, 9), Some(u64::MAX));
    let delta_ord = reader.map_integer_key_ord(0, "delta").unwrap();
    let bytes_ord = reader.map_unsigned_integer_key_ord(0, "bytes").unwrap();
    assert_eq!(reader.map_integer_value(0, delta_ord, 1), Some(i64::MIN));
    assert_eq!(
        reader.map_unsigned_integer_value(0, bytes_ord, 9),
        Some(u64::MAX)
    );
    assert_eq!(reader.posting_pos_lengths("rust", 1), Some(vec![2, 1]));
    assert!(!reader.field(1).has_pos_lengths());
    assert!(reader.has_splade());
    assert!(!reader.field(1).has_splade());
    assert!(reader.protobuf_source(1).unwrap().is_some());
    assert_eq!(reader.binding(), store.binding());
    assert_eq!(reader.derived(), Some(&full_schema_derived()));

    let reload_path = dir.join("reloaded.bm25");
    Bm25Store::load(&spill_path)
        .unwrap()
        .save(&reload_path)
        .unwrap();
    assert_eq!(std::fs::read(&reload_path).unwrap(), store_bytes);
    let _ = std::fs::remove_dir_all(&dir);
}
