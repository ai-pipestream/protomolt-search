use super::*;
use pipestream_search::postings::Bm25Index as _;

/// The tail store both indexes build on: body text with the court facet.
pub(super) fn union_tail() -> Bm25Store {
    Bm25Store::with_text_fields(&["body"])
        .with_columns(pipestream_search::postings::ColumnKind::Facet, &["court"])
        .with_positions(&["body"])
        .with_sentences(&["body"])
}

/// The public identity row `row` carries, when it carries one.
pub(super) fn row_identity(row: u32) -> Option<pipestream_search::pb::DocumentIdentity> {
    (row % 3 != 1).then(|| pipestream_search::pb::DocumentIdentity {
        document_key: [vec![0, 255], row.to_le_bytes().to_vec()].concat(),
        version: u64::MAX - u64::from(row),
        chunk_ordinal: row.is_multiple_of(2).then_some(0),
    })
}

/// Attach row `row`'s protobuf source and identity to `store`.
pub(super) fn attach_identity(store: &mut Bm25Store, local: u32, row: u32, text: &str) {
    if let Some(identity) = row_identity(row) {
        store
            .source_archive_mut()
            .attach_source_with_identity(
                local,
                &pipestream_search::pb::ProtobufSource {
                    descriptor_set: b"opaque producer descriptor".to_vec(),
                    message_type: "test.Doc".into(),
                    payload: text.as_bytes().to_vec(),
                },
                identity.chunk_ordinal,
                Some(&identity),
            )
            .unwrap();
    }
}

/// Append one batch to both indexes under the same global row ids.
pub(super) fn append_batch(
    union: &mut SegmentedShard,
    whole: &mut Bm25Store,
    batch: &[(&str, &str)],
    next: &mut u32,
) {
    for (text, court) in batch {
        let doc =
            pipestream_search::analyzer::analyze_document_native(text, Some(&body_spec())).unwrap();
        let local = *next - union.tail_base();
        union
            .add_document(*next, text.to_string(), doc.clone(), None)
            .unwrap();
        union.tail_mut().set_facet(0, local, court);
        attach_identity(union.tail_mut(), local, *next, text);
        union.sync_tail();
        whole.add_document(*next, text.to_string(), doc);
        whole.set_facet(0, *next, court);
        attach_identity(whole, *next, *next, text);
        *next += 1;
    }
}

/// Freeze the tail, serve through it, publish it as the next segment,
/// and record the snapshot/lifetime probes the caller asserts on.
pub(super) fn seal_batch(
    union: &mut SegmentedShard,
    dir: &std::path::Path,
    mapped_sets: &mut Vec<std::sync::Weak<pipestream_search::segments::OpenedSegmentSet>>,
    frozen_stores: &mut Vec<std::sync::Weak<Bm25Store>>,
    whole: &Bm25Store,
    next: u32,
) {
    let frozen = union
        .freeze_tail(union_tail(), union.tail().next_doc_id())
        .unwrap();
    frozen_stores.push(std::sync::Arc::downgrade(&frozen));
    let (base, rows, _) = union.frozen().unwrap();
    assert_eq!(u64::from(base) + u64::from(rows), u64::from(next));
    assert_eq!(union.df("court"), whole.df("court"), "frozen part served");
    assert_eq!(
        union
            .facet_ord(0, next - 1)
            .map(|o| union.facet_value(0, o).to_string()),
        whole
            .facet_ord(0, next - 1)
            .map(|o| whole.facet_value(0, o).to_string())
    );
    let stage = dir.join(format!("stage-{}", union.sealed_parts()));
    std::fs::create_dir_all(&stage).unwrap();
    frozen.save(&stage.join("documents.bm25")).unwrap();
    pipestream_search::live_docs::LiveDocs::default()
        .write(&stage.join("live-docs.bin"), u64::from(rows))
        .unwrap();
    let published = union
        .catalog()
        .append(pipestream_search::segments::SegmentSource {
            multi_res_path: None,
            segment_id: &format!("seg-{}", union.sealed_parts()),
            generation: union.sealed_parts() as u64 + 1,
            base_label: u64::from(base),
            backend_kind: "",
            vector_path: None,
            exact_vector_path: None,
            bm25_path: &stage.join("documents.bm25"),
            live_docs_path: &stage.join("live-docs.bin"),
            partition_column: None,
            parents_colocated: false,
        })
        .unwrap();
    union.republish(published).unwrap();
    assert!(union.frozen().is_none());
    mapped_sets.push(std::sync::Arc::downgrade(union.snapshot()));
}

/// Term-by-term equality of the union and whole indexes.
pub(super) fn assert_equal_terms(union: &SegmentedShard, whole: &Bm25Store, next: u32) {
    assert_eq!(union.next_doc_id(), whole.next_doc_id());
    assert_eq!(Bm25Index::doc_count(union), Bm25Index::doc_count(whole));
    assert_eq!(union.total_doc_length(), whole.total_doc_length());
    for term in ["court", "appeal", "courier", "nothing", "absent"] {
        assert_eq!(union.df(term), whole.df(term), "{term}");
        let mut a = Vec::new();
        union.for_each_posting(term, &mut |doc, tf, offsets| {
            a.push((doc, tf, offsets.to_vec()))
        });
        let mut b = Vec::new();
        whole.for_each_posting(term, &mut |doc, tf, offsets| {
            b.push((doc, tf, offsets.to_vec()))
        });
        assert_eq!(a, b, "{term}");
        for doc in 0..next {
            assert_eq!(union.doc_length(doc), whole.doc_length(doc));
            assert_eq!(
                union.posting_positions(term, doc),
                whole.posting_positions(term, doc)
            );
            assert_eq!(union.text(doc), Bm25Index::text(whole, doc));
            assert_eq!(union.doc_sentences(doc), whole.doc_sentences(doc));
        }
    }
}

/// Dictionary and prefix-expansion equality of both indexes.
pub(super) fn assert_equal_dictionary(union: &SegmentedShard, whole: &Bm25Store, next: u32) {
    // Dictionaries: one global ordinal space, value lookups agree.
    let dict: Vec<String> = union.facet_dictionary(0).to_vec();
    assert_eq!(dict[..union.facet_value_count(0).min(5)].len(), 5);
    for doc in 0..next {
        let a = union
            .facet_ord(0, doc)
            .map(|o| union.facet_value(0, o).to_string());
        let b = whole
            .facet_ord(0, doc)
            .map(|o| whole.facet_value(0, o).to_string());
        assert_eq!(a, b, "doc {doc}");
    }
    for court in ["scotus", "ca9", "dcc", "ca5", "nysd", "zzz"] {
        assert_eq!(
            union
                .facet_value_ord_of(0, court)
                .map(|o| union.facet_value(0, o)),
            whole
                .facet_value_ord_of(0, court)
                .map(|o| whole.facet_value(0, o)),
        );
    }
    assert_eq!(
        union.expand_prefix("cour", 64).unwrap(),
        whole.expand_prefix("cour", 64).unwrap()
    );
    assert_eq!(
        union.expand_prefix("cour", 1),
        whole.expand_prefix("cour", 1)
    );
}
