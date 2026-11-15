//! Token positions through both writers and both readers.

use super::format::{v6v7_section_starts, MAGIC_V7};
use super::*;
use std::path::PathBuf;

/// A two-field corpus where the body keeps token positions and the
/// second field does not. Ordinals are
/// deliberately NOT derivable from the spans: doc 2 has a gap
/// ordinal (a dropped token) between adjacent-looking spans.
fn positioned_corpus() -> Vec<(u32, String, AnalyzedDoc)> {
    let doc = |body: &[(&str, &[u32])], name: &[(&str, u32)]| -> AnalyzedDoc {
        let terms: DocTerms = body
            .iter()
            .map(|(t, ords)| {
                (
                    t.to_string(),
                    ords.len() as u32,
                    ords.iter().map(|&o| (o * 5, o * 5 + 4)).collect(),
                )
            })
            .collect();
        let positions: DocPositions = body.iter().map(|(_, ords)| ords.to_vec()).collect();
        let length = terms.iter().map(|(_, tf, _)| *tf).sum();
        let mut fields = vec![AnalyzedField {
            sentences: None,
            terms,
            length,
            positions: Some(positions),
            ..Default::default()
        }];
        fields.push(AnalyzedField {
            sentences: None,
            terms: name
                .iter()
                .map(|(t, tf)| (t.to_string(), *tf, Vec::new()))
                .collect(),
            length: name.iter().map(|(_, tf)| *tf).sum(),
            positions: None,
            ..Default::default()
        });
        AnalyzedDoc {
            cased: None,
            fields,
            quality: None,
            geography: None,
            entities: Vec::new(),
            nlp: None,
        }
    };
    vec![
        (
            0,
            "new york city".into(),
            doc(
                &[("new", &[0]), ("york", &[1]), ("citi", &[2])],
                &[("smith", 1)],
            ),
        ),
        (
            1,
            "york new york".into(),
            doc(&[("york", &[0, 2]), ("new", &[1])], &[("jones", 1)]),
        ),
        (
            2,
            "new \u{00AD} york".into(),
            doc(&[("new", &[0]), ("york", &[2])], &[]),
        ),
        // A gap slot (vector-only id 3), then a positional document
        // past it, so the base table spans a sparse slot.
        (
            4,
            "city new york new york".into(),
            doc(
                &[("citi", &[0]), ("new", &[1, 3]), ("york", &[2, 4])],
                &[("smith", 2)],
            ),
        ),
    ]
}

/// Build the positioned corpus through both writers under `base`,
/// asserting the two files are byte-identical; returns the heap store
/// and the two file paths.
fn write_positioned(base: &std::path::Path) -> (Bm25Store, PathBuf, PathBuf) {
    let mut store = Bm25Store::with_fields(&["body", "name"]).with_positions(&["body"]);
    let mut builder = SpillBuilder::create_with_fields(&base.join("build"), &["body", "name"])
        .unwrap()
        .with_position_fields(&["body"])
        .with_buffer_bytes(64);
    for (id, text, doc) in positioned_corpus() {
        store.add_document(id, text.clone(), doc.clone());
        builder
            .add_document_with_lineage(id, text, doc, None)
            .unwrap();
    }
    assert!(store.field_has_positions(0));
    assert!(!store.field_has_positions(1));
    assert_eq!(store.positional_fields(), vec!["body"]);
    let store_path = base.join("store.bm25");
    let spill_path = base.join("spill.bm25");
    store.save(&store_path).unwrap();
    builder.finish(&spill_path).unwrap();
    let a = std::fs::read(&store_path).unwrap();
    let b = std::fs::read(&spill_path).unwrap();
    assert!(
        a == b,
        "positional spill output is not byte-identical to the store"
    );
    (store, store_path, spill_path)
}

/// Every reader answers the store's ordinals bitwise, parallel to offsets.
fn assert_ordinals_match(store: &Bm25Store, reader: &Bm25Reader, reloaded: &Bm25Store) {
    for (term, doc_id) in [
        ("new", 0),
        ("york", 0),
        ("citi", 0),
        ("york", 1),
        ("new", 1),
        ("new", 2),
        ("york", 2),
        ("new", 4),
        ("york", 4),
        ("citi", 4),
    ] {
        let want = store.field(0).posting_positions(term, doc_id);
        assert!(
            want.as_ref().is_some_and(|p| !p.is_empty()),
            "{term}@{doc_id}"
        );
        assert_eq!(
            reader.field(0).posting_positions(term, doc_id),
            want,
            "reader {term}@{doc_id}"
        );
        assert_eq!(
            reloaded.field(0).posting_positions(term, doc_id),
            want,
            "reload {term}@{doc_id}"
        );
        assert_eq!(
            reader.field(0).posting_offsets(term, doc_id).len(),
            want.unwrap().len()
        );
    }
}

/// Positions round-trip through both writers, the mmap reader, and
/// the heap reload, bitwise, and the file is byte-identical between
/// writers — the dual-writer contract extended to kind 7.
#[test]
fn positions_round_trip_through_both_writers_and_both_readers() {
    let base = std::env::temp_dir().join(format!("positions-rt-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let (store, store_path, spill_path) = write_positioned(&base);
    let reader = Bm25Reader::open(&spill_path).unwrap();
    let reloaded = Bm25Store::load(&store_path).unwrap();
    assert!(reader.field_has_positions(0) && !reader.field_has_positions(1));
    assert!(reloaded.field_has_positions(0) && !reloaded.field_has_positions(1));
    assert!(reader.field(0).has_positions() && !reader.field(1).has_positions());
    assert_ordinals_match(&store, &reader, &reloaded);
    assert_eq!(
        reader.field(0).posting_positions("new", 4),
        Some(vec![1, 3])
    );
    assert_eq!(
        reader.field(0).posting_positions("york", 2),
        Some(vec![2]),
        "the dropped token keeps its ordinal"
    );
    // Absent posting is None; absent term is None; a present posting
    // of a positionless field is Some(empty).
    assert_eq!(
        reader.field(0).posting_positions("new", 3),
        None,
        "gap slot"
    );
    assert_eq!(reader.field(0).posting_positions("absent", 0), None);
    assert_eq!(
        reader.field(1).posting_positions("smith", 0),
        Some(Vec::new())
    );
    assert_eq!(reader.field(1).posting_positions("smith", 1), None);
    assert!(reader
        .integrity_section_names()
        .iter()
        .any(|n| n == "column:positions:body:vals"));
    // Reloaded store re-saves to the identical bytes.
    let again = base.join("again.bm25");
    reloaded.save(&again).unwrap();
    assert!(std::fs::read(&again).unwrap() == std::fs::read(&store_path).unwrap());
    let _ = std::fs::remove_dir_all(&base);
}

/// A file written without positions (the entire pre-kind-7 fleet)
/// opens, serves its old queries, and simply reports no positions —
/// nothing is converted or invented on open.
#[test]
fn files_without_positions_open_and_report_none() {
    let base = std::env::temp_dir().join(format!("positions-old-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let mut store = Bm25Store::with_fields(&["body", "name"]);
    for (id, text, mut doc) in positioned_corpus() {
        // The analysis carried positions, but the field never asked.
        doc.fields[0].positions = None;
        store.add_document(id, text, doc);
    }
    let path = base.join("old.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    assert!(!reader.field_has_positions(0));
    assert!(!reader.field(0).has_positions());
    assert_eq!(
        reader.field(0).posting_positions("new", 0),
        Some(Vec::new())
    );
    assert_eq!(reader.field(0).df("new"), 4);
    assert!(!reader
        .integrity_section_names()
        .iter()
        .any(|n| n.starts_with("column:positions:")));
    let _ = std::fs::remove_dir_all(&base);
}

/// The open-time validator refuses a positions section whose base
/// table lies, and a kind-7 entry naming a field the table lacks.
#[test]
fn positions_validation_refuses_a_lying_base_table_and_an_orphan_entry() {
    let base = std::env::temp_dir().join(format!("positions-val-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let mut store = Bm25Store::with_fields(&["body"]).with_positions(&["body"]);
    for (id, text, mut doc) in positioned_corpus() {
        doc.fields.truncate(1);
        store.add_document(id, text, doc);
    }
    // Write the raw v7 payload (no integrity envelope) so the doctored
    // bytes reach the structural validator rather than the CRC check.
    let mut bytes = Vec::new();
    store.write_v6_to(&mut bytes).unwrap();
    assert_eq!(&bytes[..8], MAGIC_V7);
    let good = base.join("good.bm25");
    std::fs::write(&good, &bytes).unwrap();
    Bm25Reader::open(&good).unwrap();

    // Locate the kind-7 section through the header walk and break
    // its base table's monotonicity.
    let starts = v6v7_section_starts(&bytes, true).unwrap();
    let (_, off) = starts
        .iter()
        .find(|(name, _)| name == "column:positions:body:vals")
        .expect("the positions section is named")
        .clone();
    let mut broken = bytes.clone();
    let second_base = off as usize + 4 + 4;
    broken[second_base..second_base + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    let bad = base.join("bad.bm25");
    std::fs::write(&bad, &broken).unwrap();
    let error = Bm25Reader::open(&bad).err().expect("refused").to_string();
    assert!(error.contains("base table"), "{error}");

    // Rename the entry's field to one the table lacks.
    let name_at = bytes
        .windows(b"positions:body".len())
        .position(|w| w == b"positions:body")
        .expect("entry name in header");
    let mut orphan = bytes.clone();
    orphan[name_at + "positions:".len()..name_at + "positions:".len() + 4].copy_from_slice(b"nope");
    let orphan_path = base.join("orphan.bm25");
    std::fs::write(&orphan_path, &orphan).unwrap();
    let error = Bm25Reader::open(&orphan_path)
        .err()
        .expect("refused")
        .to_string();
    assert!(error.contains("field table lacks"), "{error}");
    let _ = std::fs::remove_dir_all(&base);
}
