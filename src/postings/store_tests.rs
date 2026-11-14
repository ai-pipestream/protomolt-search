use std::io::{self, Write};

use super::format::{v6v7_section_starts, MAGIC_V6, MAGIC_V7, MAGIC_V8};
use super::format_tests::{test_dir, two_field_corpus};
use super::*;

/// A deterministic synthetic corpus: gappy ids, mixed lineages,
/// offsets on some terms, shared and unique terms across docs.
pub(super) fn synthetic_corpus() -> Vec<(u32, String, AnalyzedDoc, Option<DocLineage>)> {
    let vocab = ["court", "plaintiff", "rust", "search", "vector", "quant"];
    let mut docs = Vec::new();
    let mut id = 0u32;
    for i in 0..200u32 {
        // Every 7th id is a gap (vector-side slot).
        id += if i % 7 == 0 { 2 } else { 1 };
        let mut terms: DocTerms = Vec::new();
        let mut length = 0;
        for (vi, term) in vocab.iter().enumerate() {
            if (i as usize + vi).is_multiple_of(vi + 2) {
                let tf = 1 + (i % 3);
                let offsets = if vi % 2 == 0 {
                    (0..tf).map(|o| (o * 10, o * 10 + 4)).collect()
                } else {
                    Vec::new()
                };
                terms.push((term.to_string(), tf, offsets));
                length += tf;
            }
        }
        let lineage = (i % 3 == 0).then_some(DocLineage {
            parent_id: u64::from(i) * 17,
            group_id: u64::from(i) * 31,
            span_start: i,
            span_end: i + 100,
        });
        docs.push((
            id,
            format!("document {i} body text"),
            AnalyzedDoc::body(terms, length),
            lineage,
        ));
    }
    docs
}

/// A small store for the exhaustive v8 damage sweeps: big enough
/// to have every section populated, small enough that flipping
/// every byte stays cheap.
fn v8_store(columns: bool) -> Bm25Store {
    let mut store = if columns {
        Bm25Store::new()
            .with_facets(&["court"])
            .with_integers(&["cited"])
            .with_geos(&["place"])
    } else {
        Bm25Store::new()
    };
    for (id, text, doc, lineage) in synthetic_corpus().into_iter().take(20) {
        store.add_document_with_lineage(id, text, doc, lineage);
        if columns {
            store.set_facet(0, id, if id % 2 == 0 { "ca9" } else { "scotus" });
            store.set_integer(0, id, i64::from(id) * 3);
            store.set_geo(0, id, 40.0 + f64::from(id) * 0.1, -74.0);
        }
    }
    store
}

/// Autocomplete over the dictionary (`docs/suggest.md`): the heap
/// store's map walk and the reader's binary search return the same
/// `(term, posting df)` table in byte order, the df is the posting
/// list's length, and past the bound both refuse with the count.
#[test]
fn suggest_prefix_reports_posting_df_on_both_stores() {
    let dir = std::env::temp_dir().join(format!("suggest-df-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let docs: [&[&str]; 4] = [
        &["court", "courtesy", "couple"],
        &["court", "courier"],
        &["court", "courtesy", "cove"],
        &["search"],
    ];
    let mut store = Bm25Store::new();
    for (id, terms) in docs.iter().enumerate() {
        let terms: DocTerms = terms
            .iter()
            .map(|t| (t.to_string(), 1, Vec::new()))
            .collect();
        let length = terms.len() as u32;
        store.add_document(id as u32, String::new(), AnalyzedDoc::body(terms, length));
    }
    let path = dir.join("suggest.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let want = |prefix: &str| -> Vec<(String, u32)> {
        let mut df: std::collections::BTreeMap<String, u32> = Default::default();
        for terms in docs {
            for t in terms.iter().filter(|t| t.starts_with(prefix)) {
                *df.entry(t.to_string()).or_insert(0) += 1;
            }
        }
        df.into_iter().collect()
    };
    for prefix in ["cou", "cour", "court", "c", "s", "z"] {
        let expected = want(prefix);
        let bound = expected.len().max(1);
        assert_eq!(
            store.suggest_prefix(prefix, bound),
            Ok(expected.clone()),
            "{prefix}"
        );
        assert_eq!(
            reader.suggest_prefix(prefix, bound),
            Ok(expected.clone()),
            "{prefix}"
        );
        for (term, df) in &expected {
            assert_eq!(reader.df(term), *df, "{term}: df is the posting df");
        }
        if !expected.is_empty() {
            assert_eq!(store.suggest_prefix(prefix, bound - 1), Err(expected.len()));
            assert_eq!(
                reader.suggest_prefix(prefix, bound - 1),
                Err(expected.len())
            );
        }
    }
    assert_eq!(
        reader.suggest_prefix("cou", 10),
        Ok(vec![
            ("couple".to_string(), 1),
            ("courier".to_string(), 1),
            ("court".to_string(), 3),
            ("courtesy".to_string(), 2),
        ])
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn v8_roundtrip_serves_and_deep_verifies() {
    for columns in [false, true] {
        let dir = std::env::temp_dir().join(format!("v8-rt-{columns}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("x.bm25");
        let store = v8_store(columns);
        store.save(&path).unwrap();
        assert_eq!(&std::fs::read(&path).unwrap()[..8], MAGIC_V8);

        let reader = Bm25Reader::open(&path).unwrap();
        assert!(reader.has_integrity());
        assert_eq!(Bm25Index::doc_count(&reader), store.doc_count());
        let (sections, bytes) = reader.verify_integrity().unwrap();
        let table = reader.integrity.as_ref().unwrap();
        assert_eq!(sections, table.entries.len());
        assert_eq!(
            bytes, table.payload_len,
            "entries must cover the whole payload"
        );
        assert_eq!(table.base_v7, columns);
        // The store loader takes the same file back.
        Bm25Store::load(&path).unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }
}

#[test]
fn v8_bit_flip_anywhere_is_caught_by_open_or_deep_verify() {
    let dir = std::env::temp_dir().join(format!("v8-flip-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("x.bm25");
    v8_store(true).save(&path).unwrap();
    let intact = std::fs::read(&path).unwrap();

    let victim = dir.join("flipped.bm25");
    for i in 0..intact.len() {
        let mut bytes = intact.clone();
        bytes[i] ^= 0x01;
        std::fs::write(&victim, &bytes).unwrap();
        match Bm25Reader::open(&victim) {
            Err(_) => {}
            Ok(reader) => {
                let err = reader.verify_integrity().expect_err(&format!(
                    "flip at byte {i}: open accepted it and deep verify found nothing"
                ));
                assert!(
                    err.to_string().contains("CRC mismatch"),
                    "flip at byte {i}: unexpected deep-verify error: {err}"
                );
            }
        }
    }
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn v8_truncation_anywhere_refuses_open() {
    let dir = std::env::temp_dir().join(format!("v8-cut-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("x.bm25");
    v8_store(false).save(&path).unwrap();
    let intact = std::fs::read(&path).unwrap();

    let victim = dir.join("cut.bm25");
    for len in 0..intact.len() {
        std::fs::write(&victim, &intact[..len]).unwrap();
        assert!(
            Bm25Reader::open(&victim).is_err(),
            "a v8 file cut to {len} of {} bytes must refuse to open, \
             never demote to an integrity-less file",
            intact.len()
        );
    }
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn v8_deep_verify_names_the_rotted_section() {
    let dir = std::env::temp_dir().join(format!("v8-name-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("x.bm25");
    v8_store(false).save(&path).unwrap();

    let reader = Bm25Reader::open(&path).unwrap();
    let e = reader
        .integrity
        .as_ref()
        .unwrap()
        .entries
        .iter()
        .find(|e| e.name == "field:body:postings")
        .expect("body postings section exists")
        .clone();
    drop(reader);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[(e.off + e.len / 2) as usize] ^= 0x01;
    std::fs::write(&path, &bytes).unwrap();

    // Postings are not eagerly verified, so open succeeds; the
    // deep verify must fail NAMING the section.
    let reader = Bm25Reader::open(&path).unwrap();
    let err = reader.verify_integrity().unwrap_err();
    assert!(
        err.to_string().contains("field:body:postings"),
        "error must name the rotted section: {err}"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn pre_v8_files_open_but_report_nothing_to_verify() {
    let dir = std::env::temp_dir().join(format!("v8-old-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("x.bm25");
    // A raw v6 payload, no integrity pass: what every pre-v8 build
    // on disk looks like.
    let mut bytes = Vec::new();
    v8_store(false).write_v6_to(&mut bytes).unwrap();
    assert_eq!(&bytes[..8], MAGIC_V6);
    std::fs::write(&path, &bytes).unwrap();

    let reader = Bm25Reader::open(&path).unwrap();
    assert!(!reader.has_integrity());
    let err = reader.verify_integrity().unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Unsupported);
    assert!(err.to_string().contains("predates v8"), "{err}");
    std::fs::remove_dir_all(&dir).ok();
}

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

/// A two-field corpus where the body keeps token positions and the
/// second field does not (`docs/phrase-proximity.md`). Ordinals are
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

/// Positions round-trip through both writers, the mmap reader, and
/// the heap reload, bitwise, and the file is byte-identical between
/// writers — the dual-writer contract extended to kind 7.
#[test]
fn positions_round_trip_through_both_writers_and_both_readers() {
    let base = std::env::temp_dir().join(format!("positions-rt-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

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

    // Every reader answers the store's ordinals bitwise.
    let reader = Bm25Reader::open(&spill_path).unwrap();
    let reloaded = Bm25Store::load(&store_path).unwrap();
    assert!(reader.field_has_positions(0) && !reader.field_has_positions(1));
    assert!(reloaded.field_has_positions(0) && !reloaded.field_has_positions(1));
    assert!(reader.field(0).has_positions() && !reader.field(1).has_positions());
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
        // Positions are parallel to the offsets, in the same order.
        assert_eq!(
            reader.field(0).posting_offsets(term, doc_id).len(),
            want.unwrap().len()
        );
    }
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
    // The section is named in the integrity table like every other.
    assert!(reader
        .integrity_section_names()
        .iter()
        .any(|n| n == "column:positions:body:vals"));

    // Reloaded store re-saves to the identical bytes: positions
    // survive the heap round trip exactly.
    let again = base.join("again.bm25");
    reloaded.save(&again).unwrap();
    assert!(std::fs::read(&again).unwrap() == a);
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

/// Same byte-identity contract on the legacy v4 path (bench/migration
/// writer): the store and the spill builder must agree there too.
#[test]
fn v4_writers_are_byte_identical() {
    let base = std::env::temp_dir().join(format!("spill-eq-v4-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let mut store = Bm25Store::new();
    let mut builder = SpillBuilder::create_v4_for_bench(&base.join("build"))
        .unwrap()
        .with_buffer_bytes(256);
    for (id, text, doc, lineage) in synthetic_corpus() {
        store.add_document_with_lineage(id, text.clone(), doc.clone(), lineage);
        builder
            .add_document_with_lineage(id, text, doc, lineage)
            .unwrap();
    }
    let store_path = base.join("store.bm25");
    let spill_path = base.join("spill.bm25");
    {
        let mut w = io::BufWriter::new(std::fs::File::create(&store_path).unwrap());
        store.write_v4_for_bench(&mut w).unwrap();
        w.flush().unwrap();
    }
    builder.finish(&spill_path).unwrap();
    let a = std::fs::read(&store_path).unwrap();
    let b = std::fs::read(&spill_path).unwrap();
    assert!(a == b, "v4 spill output is not byte-identical to the store");
    let _ = std::fs::remove_dir_all(&base);
}

fn doc_a() -> AnalyzedDoc {
    AnalyzedDoc::body(
        vec![
            ("rust".to_string(), 2, vec![(0, 4), (10, 14)]),
            ("search".to_string(), 1, vec![(5, 11)]),
        ],
        3,
    )
}

fn doc_b() -> AnalyzedDoc {
    AnalyzedDoc::body(
        vec![
            ("rust".to_string(), 1, vec![(0, 4)]),
            ("vector".to_string(), 2, vec![(5, 11), (12, 18)]),
        ],
        3,
    )
}

#[test]
fn build_and_query_postings() {
    let mut store = Bm25Store::new();
    store.add_document(0, "rust search rust".to_string(), doc_a());
    store.add_document(1, "rust vector vector".to_string(), doc_b());

    assert_eq!(store.doc_count(), 2);
    assert_eq!(store.total_doc_length(), 6);
    assert_eq!(store.doc_length(1), 3);
    let postings = store.postings("rust").unwrap();
    assert_eq!(postings.len(), 2);
    assert_eq!(
        postings[0],
        Posting {
            doc_id: 0,
            tf: 2,
            offsets: vec![(0, 4), (10, 14)],
            positions: Vec::new(),
            pos_lengths: Vec::new(),
        }
    );
    assert_eq!(store.postings("vector").unwrap()[0].tf, 2);
    assert!(store.postings("missing").is_none());
    assert_eq!(store.text(1), Some("rust vector vector"));
}

#[test]
fn sparse_slots_for_shared_id_space() {
    let mut store = Bm25Store::new();
    // Ids 0..2 were consumed by the vector side; first document lands
    // at id 2, leaving slots 0..1 sparse.
    store.add_document(
        2,
        "hello world".to_string(),
        AnalyzedDoc::body(vec![("hello".to_string(), 1, vec![(0, 5)])], 2),
    );
    assert_eq!(store.next_doc_id(), 3);
    assert_eq!(store.doc_count(), 1);
    assert_eq!(store.doc_length(0), 0);
    assert_eq!(store.doc_length(2), 2);
    assert!(store.text(0).is_none());
    assert_eq!(store.text(2), Some("hello world"));
}

#[test]
fn round_trip_through_disk() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target/tmp")
        .join(format!("tvbm25_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("shard.tv.bm25");

    let mut store = Bm25Store::new();
    store.add_document(0, "rust search rust".to_string(), doc_a());
    store.add_document(2, "rust vector vector".to_string(), doc_b());
    store.save(&path).unwrap();

    let loaded = Bm25Store::load(&path).unwrap();
    assert_eq!(loaded.fields[0].doc_lengths, store.fields[0].doc_lengths);
    assert_eq!(loaded.fields[0].total_length, store.fields[0].total_length);
    assert_eq!(loaded.texts, store.texts);
    assert_eq!(loaded.fields[0].postings, store.fields[0].postings);

    let _ = std::fs::remove_dir_all(&dir);
}

/// Regression test for the reader's lineage lookup: the on-disk
/// lineage section is variable stride (1 B absent, 25 B present),
/// so indexing must walk variable-width entries across gap slots
/// and missing lineages rather than assuming a fixed 25 B stride.
#[test]
fn reader_lineage_with_gaps_and_missing_entries() {
    let dir = test_dir("lineage");
    let path = dir.join("shard.bm25");
    let mut store = Bm25Store::new();
    let lineage = |i: u32| DocLineage {
        parent_id: 1000 + u64::from(i),
        group_id: 2000 + u64::from(i),
        span_start: i * 7,
        span_end: i * 7 + 90,
    };
    // Gap slots at 0..1, then a mix of present and missing lineages.
    store.add_document_with_lineage(
        2,
        "a".to_string(),
        AnalyzedDoc::body(vec![("rust".into(), 1, vec![(0, 4)])], 1),
        Some(lineage(2)),
    );
    store.add_document_with_lineage(
        3,
        "b".to_string(),
        AnalyzedDoc::body(vec![("rust".into(), 2, vec![(0, 4), (6, 10)])], 2),
        None,
    );
    store.add_document_with_lineage(
        7,
        "c".to_string(),
        AnalyzedDoc::body(vec![("search".into(), 1, vec![(0, 6)])], 1),
        Some(lineage(7)),
    );
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    for slot in 0..store.next_doc_id() {
        assert_eq!(reader.lineage(slot), store.lineage(slot), "lineage({slot})");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// The v3/v4 reader's posting_offsets walks with early exit over the
/// doc-id-ordered postings (the trait default full-scans — O(df) per
/// survivor). Correctness across first/middle/last/absent targets;
/// the win itself is the bench's v4 k=1000 column.
#[test]
fn v4_posting_offsets_early_exit_correctness() {
    let dir = test_dir("v4early");
    let path = dir.join("shard.bm25");
    let mut store = Bm25Store::new();
    let n = 5000u32;
    for i in 0..n {
        store.add_document(
            i * 2, // gaps: even ids only
            format!("doc {i}"),
            AnalyzedDoc::body(vec![("court".to_string(), 1, vec![(i, i + 3)])], 5),
        );
    }
    {
        let mut w = io::BufWriter::new(std::fs::File::create(&path).unwrap());
        store.write_v4_for_bench(&mut w).unwrap();
        w.flush().unwrap();
    }
    let reader = Bm25Reader::open(&path).unwrap();
    assert!(!reader.v5_runs, "expected the v4 path");
    // First posting of the list (early exit after one record).
    assert_eq!(reader.posting_offsets("court", 0), vec![(0, 3)]);
    // Middle and last.
    assert_eq!(reader.posting_offsets("court", 4998), vec![(2499, 2502)]);
    assert_eq!(reader.posting_offsets("court", 9998), vec![(4999, 5002)]);
    // Absent: a gap id, beyond the last, and an unknown term.
    assert!(reader.posting_offsets("court", 9997).is_empty());
    assert!(reader.posting_offsets("court", 2_000_000).is_empty());
    assert!(reader.posting_offsets("nope", 0).is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn load_rejects_garbage() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target/tmp")
        .join(format!("tvbm25_bad_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("bad.bm25");
    std::fs::write(&path, b"not a postings file").unwrap();
    assert!(Bm25Store::load(&path).is_err());
    let _ = std::fs::remove_dir_all(&dir);
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
