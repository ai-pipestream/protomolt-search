use super::*;

use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// A deterministic synthetic corpus: gappy ids, mixed lineages,
/// offsets on some terms, shared and unique terms across docs.
fn synthetic_corpus() -> Vec<(u32, String, AnalyzedDoc, Option<DocLineage>)> {
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
        }];
        fields.push(AnalyzedField {
            sentences: None,
            terms: name
                .iter()
                .map(|(t, tf)| (t.to_string(), *tf, Vec::new()))
                .collect(),
            length: name.iter().map(|(_, tf)| *tf).sum(),
            positions: None,
        });
        AnalyzedDoc {
            cased: None,
            fields,
            quality: None,
            geography: None,
            entities: Vec::new(),
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
/// and the reader used to index it at a fixed 25B stride — correct
/// only for dense all-present lineages, garbage after any gap slot
/// or missing lineage (which is all the old tests had, so it
/// passed). Gap slots + missing lineages here.
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

// --- v5 (TVBM2505) tests -------------------------------------------

/// Hand-rolled deterministic RNG (LCG, same style as
/// `harness::unit_vectors`) — no proptest dependency.
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_add(0x9E3779B97F4A7C15))
    }
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn test_dir(name: &str) -> PathBuf {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target/tmp")
        .join(format!("tvbm25_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A random corpus: gappy ids, a small vocabulary so terms repeat
/// across docs, mixed tf, some SCORING_ONLY postings (no offsets).
fn random_corpus(rng: &mut Lcg, n_docs: u64, vocab: &[String]) -> Vec<(u32, String, AnalyzedDoc)> {
    let mut docs = Vec::new();
    let mut id = 0u32;
    for _ in 0..n_docs {
        id += 1 + rng.below(3) as u32;
        let mut terms: DocTerms = Vec::new();
        let mut length = 0;
        for _ in 0..1 + rng.below(6) {
            let term = vocab[rng.below(vocab.len() as u64) as usize].clone();
            if terms.iter().any(|(t, _, _)| *t == term) {
                continue;
            }
            let tf = 1 + rng.below(4) as u32;
            let offsets: Vec<(u32, u32)> = if rng.below(3) == 0 {
                Vec::new()
            } else {
                (0..tf)
                    .map(|o| {
                        let start = o * 10 + rng.below(5) as u32;
                        (start, start + 1 + rng.below(4) as u32)
                    })
                    .collect()
            };
            length += tf;
            terms.push((term, tf, offsets));
        }
        docs.push((id, format!("doc {id}"), AnalyzedDoc::body(terms, length)));
    }
    docs
}

fn build_store(corpus: &[(u32, String, AnalyzedDoc)]) -> Bm25Store {
    let mut store = Bm25Store::new();
    for (id, text, doc) in corpus {
        store.add_document(*id, text.clone(), doc.clone());
    }
    store
}

fn vocab(rng: &mut Lcg, n: u64) -> Vec<String> {
    (0..n.max(1))
        .map(|i| format!("t{}", i + rng.below(1)))
        .collect()
}

/// v5 round trip: the reader serves exactly what the heap store
/// holds, through every trait method the scorer uses.
#[test]
fn v5_round_trip_matches_heap_store() {
    let dir = test_dir("v5rt");
    let path = dir.join("shard.bm25");
    let corpus = synthetic_corpus();
    let mut store = Bm25Store::new();
    for (id, text, doc, lineage) in &corpus {
        store.add_document_with_lineage(*id, text.clone(), doc.clone(), *lineage);
    }
    store.save_v5(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();

    assert_eq!(Bm25Index::doc_count(&reader), store.doc_count());
    assert_eq!(reader.total_doc_length(), store.total_doc_length());
    assert_eq!(reader.next_doc_id(), store.next_doc_id());
    for term in [
        "court",
        "plaintiff",
        "rust",
        "search",
        "vector",
        "quant",
        "missing",
    ] {
        assert_eq!(reader.df(term), store.df(term), "df({term})");
        // for_each_posting: identical (doc, tf, offsets) stream.
        let mut got = Vec::new();
        reader.for_each_posting(term, &mut |d, tf, o| got.push((d, tf, o.to_vec())));
        let want = store
            .postings(term)
            .map(|ps| {
                ps.iter()
                    .map(|p| (p.doc_id, p.tf, p.offsets.clone()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        assert_eq!(got, want, "for_each_posting({term})");
        // for_each_doc_tf: identical (doc, tf) stream.
        let mut got_tf = Vec::new();
        reader.for_each_doc_tf(term, &mut |d, tf| got_tf.push((d, tf)));
        let want_tf: Vec<(u32, u32)> = want.iter().map(|(d, tf, _)| (*d, *tf)).collect();
        assert_eq!(got_tf, want_tf, "for_each_doc_tf({term})");
        // posting_offsets: exact per (term, doc).
        for (d, _, offs) in &want {
            assert_eq!(
                &reader.posting_offsets(term, *d),
                offs,
                "posting_offsets({term}, {d})"
            );
        }
        assert!(reader.posting_offsets(term, u32::MAX).is_empty());
    }
    // text / lineage / doc_length (lineage exercises the lazily
    // built offset index: this corpus has gap slots and missing
    // lineages).
    for (id, text, _, lineage) in &corpus {
        assert_eq!(
            reader.text(*id).as_deref(),
            Some(text.as_str()),
            "text({id})"
        );
        assert_eq!(reader.lineage(*id), *lineage, "lineage({id})");
    }

    // Scoring surface: top_k and score_candidates identical to heap.
    let terms = vec![
        "court".to_string(),
        "rust".to_string(),
        "missing".to_string(),
    ];
    let stats = crate::bm25::CorpusStats {
        doc_count: store.doc_count(),
        total_doc_length: store.total_doc_length(),
        dfs: terms.iter().map(|t| store.df(t)).collect(),
    };
    let params = crate::bm25::Bm25Params::default();
    let heap_hits = crate::bm25::top_k(&store, &terms, &stats, params, 20);
    let disk_hits = crate::bm25::top_k(&reader, &terms, &stats, params, 20);
    assert_eq!(heap_hits, disk_hits, "top_k heap vs v5 reader");
    let oracle = crate::bm25::top_k_exhaustive(&store, &terms, &stats, params, 20);
    assert_eq!(heap_hits, oracle, "top_k vs exhaustive oracle");
    let ids: Vec<u32> = (0..store.next_doc_id()).filter(|i| i % 3 == 0).collect();
    let heap_c = crate::bm25::score_candidates(&store, &terms, &stats, params, &ids);
    let disk_c = crate::bm25::score_candidates(&reader, &terms, &stats, params, &ids);
    assert_eq!(heap_c, disk_c, "score_candidates heap vs v5 reader");

    // And the heap reload path (shard append) parses v5.
    let loaded = Bm25Store::load(&path).unwrap();
    assert_eq!(loaded.fields[0].postings, store.fields[0].postings);
    assert_eq!(loaded.fields[0].doc_lengths, store.fields[0].doc_lengths);
    assert_eq!(loaded.texts, store.texts);

    let _ = std::fs::remove_dir_all(&dir);
}

/// Bitwise cross-format property test: the SAME corpus written to v4
/// and v5 must answer top_k with identical `(doc_id, score.to_bits(),
/// term_offsets)` sequences — over random corpora, random k, random
/// term counts, random k1/b — and identical to the exhaustive oracle.
#[test]
fn v5_v4_top_k_bitwise_identical() {
    let dir = test_dir("v5x");
    let mut rng = Lcg::new(0xB10C);
    let k1s = [0.0, 0.6, 1.2, 2.0];
    let bs = [0.0, 0.4, 0.75, 1.0];
    for round in 0..24 {
        let n_voc = 3 + rng.below(20);
        let voc = vocab(&mut rng, n_voc);
        let n_docs = 30 + rng.below(170);
        let corpus = random_corpus(&mut rng, n_docs, &voc);
        let store = build_store(&corpus);
        let v5_path = dir.join(format!("r{round}.v5.bm25"));
        let v4_path = dir.join(format!("r{round}.v4.bm25"));
        store.save_v5(&v5_path).unwrap();
        {
            let mut w = io::BufWriter::new(std::fs::File::create(&v4_path).unwrap());
            store.write_v4_for_bench(&mut w).unwrap();
            w.flush().unwrap();
        }
        let v5r = Bm25Reader::open(&v5_path).unwrap();
        let v4r = Bm25Reader::open(&v4_path).unwrap();
        for _ in 0..6 {
            let n_terms = 1 + rng.below(4) as usize;
            let terms: Vec<String> = voc
                .iter()
                .filter(|_| rng.below(2) == 0)
                .take(n_terms.max(1))
                .cloned()
                .collect();
            let terms = if terms.is_empty() {
                vec![voc[0].clone()]
            } else {
                terms
            };
            let stats = crate::bm25::CorpusStats {
                doc_count: store.doc_count(),
                total_doc_length: store.total_doc_length(),
                dfs: terms.iter().map(|t| store.df(t)).collect(),
            };
            let params = crate::bm25::Bm25Params {
                k1: k1s[rng.below(k1s.len() as u64) as usize],
                b: bs[rng.below(bs.len() as u64) as usize],
            };
            let k = 1 + rng.below(30) as usize;
            let sig = |hits: &[crate::bm25::ScoredDoc]| {
                hits.iter()
                    .map(|h| (h.doc_id, h.score.to_bits(), h.term_offsets.clone()))
                    .collect::<Vec<_>>()
            };
            let heap = sig(&crate::bm25::top_k(&store, &terms, &stats, params, k));
            let oracle = sig(&crate::bm25::top_k_exhaustive(
                &store, &terms, &stats, params, k,
            ));
            let v5 = sig(&crate::bm25::top_k(&v5r, &terms, &stats, params, k));
            let v4 = sig(&crate::bm25::top_k(&v4r, &terms, &stats, params, k));
            assert_eq!(heap, oracle, "round {round}: top_k diverged from oracle");
            assert_eq!(heap, v5, "round {round}: v5 reader diverged");
            assert_eq!(heap, v4, "round {round}: v4 reader diverged");
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn pareto_frontier_prunes_dominated() {
    let f = pareto_frontier(&[(5, 10), (3, 10), (5, 12), (1, 1)]);
    assert_eq!(f, vec![(1, 1), (5, 10)]);
    // Empty block, single pair, duplicates.
    assert!(pareto_frontier(&[]).is_empty());
    assert_eq!(pareto_frontier(&[(7, 7), (7, 7)]), vec![(7, 7)]);
}

#[test]
fn pareto_frontier_collapses_never_drops() {
    // A 20-point staircase is entirely on the frontier and forces
    // collapsing down to the cap.
    let pairs: Vec<(u32, u32)> = (1..=20u32).map(|i| (i, i * 10)).collect();
    let f = pareto_frontier(&pairs);
    assert!(f.len() <= MAX_FRONTIER, "frontier not capped: {f:?}");
    // Bound property: every original point is weakly dominated by
    // some stored pair (tf' >= tf, dl' <= dl) — collapsing only ever
    // raises the bound, it never drops coverage.
    for &(tf, dl) in &pairs {
        assert!(
            f.iter().any(|&(t, d)| t >= tf && d <= dl),
            "({tf},{dl}) not bounded by collapsed frontier {f:?}"
        );
    }
    // The staircase stays strictly increasing in both coordinates.
    for w in f.windows(2) {
        assert!(w[1].0 > w[0].0 && w[1].1 > w[0].1, "not a staircase: {f:?}");
    }
}

/// Frontier safety gate: for every level-0 block of every term, the
/// stored (possibly collapsed) frontier's max over tf_norm is >= the
/// true block max, at several avgdl values and k1/b combos.
#[test]
fn v5_frontier_never_underbounds() {
    use crate::bm25::{tf_norm, Bm25Params};
    let dir = test_dir("v5frontier");

    fn check(store: &Bm25Store, reader: &Bm25Reader, terms: &[String]) -> bool {
        let params_list = [
            Bm25Params { k1: 0.0, b: 0.0 },
            Bm25Params { k1: 1.2, b: 0.75 },
            Bm25Params { k1: 2.0, b: 1.0 },
        ];
        let avgdl = store.total_doc_length() as f64 / store.doc_count() as f64;
        let mut saw_collapsed = false;
        for term in terms {
            let Some((_, skip_off, _, df)) = reader.directory_lookup_v5(&reader.fields[0], term)
            else {
                continue;
            };
            let records = reader.v5_l0_records(skip_off, df);
            let postings = store.postings(term).unwrap();
            assert_eq!(records.len(), postings.len().div_ceil(BLOCK));
            for (block_i, (_, pairs)) in records.iter().enumerate() {
                assert!(!pairs.is_empty() && pairs.len() <= MAX_FRONTIER);
                saw_collapsed |= pairs.len() == MAX_FRONTIER;
                let block = &postings[block_i * BLOCK..((block_i + 1) * BLOCK).min(postings.len())];
                // Block frontier covers every posting (tf' >= tf, dl' <= dl).
                for p in block {
                    let dl = store.doc_length(p.doc_id);
                    assert!(
                        pairs.iter().any(|&(t, d)| t >= p.tf && d <= dl),
                        "{term} block {block_i}: posting ({},{dl}) not bounded by {pairs:?}",
                        p.tf
                    );
                }
                for params in params_list {
                    for a in [avgdl, 1.0, 50.0, 1000.0] {
                        let true_max = block
                            .iter()
                            .map(|p| tf_norm(params, p.tf, store.doc_length(p.doc_id), a))
                            .fold(f64::NEG_INFINITY, f64::max);
                        let stored_max = pairs
                            .iter()
                            .map(|&(t, d)| tf_norm(params, t, d, a))
                            .fold(f64::NEG_INFINITY, f64::max);
                        assert!(
                            stored_max >= true_max,
                            "{term} block {block_i} underbounds at avgdl {a}, {params:?}: \
                                 stored {stored_max} < true {true_max}"
                        );
                    }
                }
            }
        }
        saw_collapsed
    }

    // Small vocab + many docs: every term spans several 128-posting
    // blocks.
    let mut rng = Lcg::new(0xFE07);
    let voc = vocab(&mut rng, 4);
    let corpus = random_corpus(&mut rng, 900, &voc);
    let store = build_store(&corpus);
    let path = dir.join("random.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    check(&store, &reader, &voc);

    // A (tf, dl) staircase forces frontier collapse: within a block,
    // tf and dl both strictly increase, so every posting is on the
    // frontier and the cap must collapse, never drop.
    let mut stair = Bm25Store::new();
    for i in 0..300u32 {
        stair.add_document(
            i,
            format!("doc {i}"),
            AnalyzedDoc::body(
                vec![("stair".to_string(), 1 + i % 150, vec![(i, i + 2)])],
                100 + i,
            ),
        );
    }
    let path = dir.join("stair.bm25");
    stair.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    assert!(
        check(&stair, &reader, &["stair".to_string()]),
        "collapse path never exercised"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Laziness proof: corrupt the occurrence run of a COPY of a v5 file;
/// the for_each_doc_tf-based scored path (top_k) must return identical
/// scores/doc ids, while posting_offsets returns the garbage — the
/// scored path never touched the occurrence bytes.
#[test]
fn v5_scored_path_never_reads_occurrence_run() {
    let dir = test_dir("v5lazy");
    let path = dir.join("shard.bm25");
    // One term, several docs, offsets on every posting.
    let mut store = Bm25Store::new();
    for i in 0..50u32 {
        let tf = 1 + i % 3;
        store.add_document(
            i,
            format!("doc {i}"),
            AnalyzedDoc::body(
                vec![(
                    "court".to_string(),
                    tf,
                    (0..tf).map(|o| (o * 10 + i, o * 10 + i + 4)).collect(),
                )],
                tf,
            ),
        );
    }
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let (_, skip_off, occ_off, _) = reader
        .directory_lookup_v5(&reader.fields[0], "court")
        .unwrap();

    let corrupted = dir.join("corrupted.bm25");
    let mut bytes = std::fs::read(&path).unwrap();
    for b in &mut bytes[occ_off as usize..skip_off as usize] {
        *b ^= 0xFF;
    }
    std::fs::write(&corrupted, &bytes).unwrap();
    drop(reader);
    let good = Bm25Reader::open(&path).unwrap();
    let bad = Bm25Reader::open(&corrupted).unwrap();

    let terms = vec!["court".to_string()];
    let stats = crate::bm25::CorpusStats {
        doc_count: store.doc_count(),
        total_doc_length: store.total_doc_length(),
        dfs: vec![store.df("court")],
    };
    let params = crate::bm25::Bm25Params::default();
    let sig = |hits: &[crate::bm25::ScoredDoc]| -> Vec<(u32, u64)> {
        hits.iter().map(|h| (h.doc_id, h.score.to_bits())).collect()
    };
    let a = crate::bm25::top_k(&good, &terms, &stats, params, 10);
    let b = crate::bm25::top_k(&bad, &terms, &stats, params, 10);
    assert_eq!(sig(&a), sig(&b), "scored path touched occurrence bytes");
    // The survivors' offsets DO come from the occurrence run, so they
    // reflect the corruption — proving the corruption took and that
    // offsets really are fetched separately.
    assert_ne!(
        a.iter().map(|h| &h.term_offsets).collect::<Vec<_>>(),
        b.iter().map(|h| &h.term_offsets).collect::<Vec<_>>(),
        "corruption did not reach the occurrence run"
    );
    // posting_offsets decodes the corrupted run: garbage, same shape.
    let good_offs = good.posting_offsets("court", 7);
    let bad_offs = bad.posting_offsets("court", 7);
    assert_eq!(good_offs.len(), bad_offs.len());
    assert_ne!(good_offs, bad_offs);

    let _ = std::fs::remove_dir_all(&dir);
}

// --- v6 (TVBM2506) tests -------------------------------------------

fn synthetic_store() -> Bm25Store {
    let mut store = Bm25Store::new();
    for (id, text, doc, lineage) in synthetic_corpus() {
        store.add_document_with_lineage(id, text, doc, lineage);
    }
    store
}

/// The increment-1 parity contract (`docs/multi-field.md` build
/// order step 1): a single-field v6 file carries the SAME section
/// bytes as the v5 file of the same corpus. doc_lengths, texts,
/// lineages, and postings are byte-identical; text_index entries
/// and directory run offsets differ only by their documented
/// rebasing (v6 stores them section-relative).
#[test]
fn v6_single_field_sections_match_v5() {
    let store = synthetic_store();
    let mut v5 = Vec::new();
    store.write_to(&mut v5).unwrap();
    let mut v6 = Vec::new();
    store.write_v6_to(&mut v6).unwrap();

    let u32le = |b: &[u8], o: usize| u32::from_le_bytes(b[o..o + 4].try_into().unwrap());
    let u64le = |b: &[u8], o: usize| u64::from_le_bytes(b[o..o + 8].try_into().unwrap());

    // v5 geometry from its fixed header.
    let v5_texts_off = u64le(&v5, 16) as usize;
    let v5_lineages_off = u64le(&v5, 24) as usize;
    let v5_postings_off = u64le(&v5, 32) as usize;
    let v5_directory_off = u64le(&v5, 40) as usize;
    let n_slots = u32le(&v5, 48) as usize;
    let v5_text_index_off = v5_lineages_off - 12 * n_slots;

    // v6 geometry from its section table.
    assert_eq!(&v6[..8], b"TVBM2506");
    assert_eq!(u32le(&v6, 8), 1, "n_fields");
    assert_eq!(u32le(&v6, 12) as usize, n_slots, "n_slots");
    let t_off = u64le(&v6, 16) as usize;
    let ti_off = u64le(&v6, 24) as usize;
    let l_off = u64le(&v6, 32) as usize;
    let name_len = u16::from_le_bytes(v6[40..42].try_into().unwrap()) as usize;
    assert_eq!(&v6[42..42 + name_len], b"body");
    let fe = 42 + name_len;
    assert_eq!(u64le(&v6, fe), 0, "analysis fingerprint placeholder");
    assert_eq!(u64le(&v6, fe + 8), u64le(&v5, 8), "total_length");
    let dl_off = u64le(&v6, fe + 16) as usize;
    let p_off = u64le(&v6, fe + 24) as usize;
    let d_off = u64le(&v6, fe + 32) as usize;

    // Byte-identical sections.
    assert_eq!(
        &v6[dl_off..dl_off + 4 * n_slots],
        &v5[52..v5_texts_off],
        "doc_lengths section"
    );
    assert_eq!(
        &v6[t_off..ti_off],
        &v5[v5_texts_off..v5_text_index_off],
        "texts section"
    );
    assert_eq!(
        &v6[l_off..dl_off],
        &v5[v5_lineages_off..v5_postings_off],
        "lineages section"
    );
    assert_eq!(
        &v6[p_off..d_off],
        &v5[v5_postings_off..v5_directory_off],
        "postings section"
    );

    // text_index: identical entries after the documented rebase.
    for slot in 0..n_slots {
        let a = v5_text_index_off + 12 * slot;
        let b = ti_off + 12 * slot;
        assert_eq!(
            u32le(&v5, a + 8),
            u32le(&v6, b + 8),
            "text len, slot {slot}"
        );
        if u32le(&v5, a + 8) != u32::MAX {
            assert_eq!(
                u64le(&v5, a) - v5_texts_off as u64,
                u64le(&v6, b),
                "text offset, slot {slot}"
            );
        } else {
            assert_eq!(u64le(&v6, b), 0, "absent marker, slot {slot}");
        }
    }

    // directory: identical entries after the documented rebase,
    // identical term blob, both files fully tiled.
    let n_terms = u32le(&v5, v5_directory_off) as usize;
    assert_eq!(u32le(&v6, d_off) as usize, n_terms, "n_terms");
    for i in 0..n_terms {
        let a = v5_directory_off + 4 + 34 * i;
        let b = d_off + 4 + 34 * i;
        for run in 0..3 {
            assert_eq!(
                u64le(&v5, a + 8 * run) - v5_postings_off as u64,
                u64le(&v6, b + 8 * run),
                "run offset {run}, term {i}"
            );
        }
        assert_eq!(
            &v5[a + 24..a + 34],
            &v6[b + 24..b + 34],
            "df/blob/len, term {i}"
        );
    }
    let blob_a = v5_directory_off + 4 + 34 * n_terms;
    let blob_b = d_off + 4 + 34 * n_terms;
    assert_eq!(v5.len() - blob_a, v6.len() - blob_b, "blob sizes");
    assert_eq!(&v5[blob_a..], &v6[blob_b..], "term blob");
}

/// v6 round trip: the reader serves exactly what the heap store
/// holds through every trait method, the block-max surface is
/// present, and `Bm25Store::load` (the shard append path)
/// reproduces the store exactly.
#[test]
fn v6_round_trip_matches_heap_store() {
    let dir = test_dir("v6rt");
    let path = dir.join("shard.bm25");
    let store = synthetic_store();
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();

    assert_eq!(Bm25Index::doc_count(&reader), store.doc_count());
    assert_eq!(reader.total_doc_length(), store.total_doc_length());
    assert_eq!(reader.next_doc_id(), store.next_doc_id());
    for term in [
        "court",
        "plaintiff",
        "rust",
        "search",
        "vector",
        "quant",
        "missing",
    ] {
        assert_eq!(reader.df(term), store.df(term), "df({term})");
        let mut got = Vec::new();
        reader.for_each_posting(term, &mut |d, tf, o| got.push((d, tf, o.to_vec())));
        let want = store
            .postings(term)
            .map(|ps| {
                ps.iter()
                    .map(|p| (p.doc_id, p.tf, p.offsets.clone()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        assert_eq!(got, want, "for_each_posting({term})");
        let mut got_tf = Vec::new();
        reader.for_each_doc_tf(term, &mut |d, tf| got_tf.push((d, tf)));
        let want_tf: Vec<(u32, u32)> = want.iter().map(|(d, tf, _)| (*d, *tf)).collect();
        assert_eq!(got_tf, want_tf, "for_each_doc_tf({term})");
        for (d, _, offs) in &want {
            assert_eq!(
                &reader.posting_offsets(term, *d),
                offs,
                "posting_offsets({term}, {d})"
            );
        }
        assert_eq!(
            reader.has_impacts(term),
            store.df(term) > 0,
            "has_impacts({term})"
        );
    }
    for slot in 0..store.next_doc_id() {
        assert_eq!(
            reader.doc_length(slot),
            store.doc_length(slot),
            "doc_length({slot})"
        );
        assert_eq!(
            Bm25Index::text(&reader, slot),
            store.text(slot).map(str::to_string),
            "text({slot})"
        );
        assert_eq!(
            Bm25Index::lineage(&reader, slot),
            store.lineage(slot),
            "lineage({slot})"
        );
    }
    // Heap reload (the shard append path) parses v6.
    let loaded = Bm25Store::load(&path).unwrap();
    assert_eq!(loaded.fields[0].name, "body");
    assert_eq!(loaded.fields[0].postings, store.fields[0].postings);
    assert_eq!(loaded.fields[0].doc_lengths, store.fields[0].doc_lengths);
    assert_eq!(loaded.fields[0].total_length, store.fields[0].total_length);
    assert_eq!(loaded.texts, store.texts);
    assert_eq!(loaded.lineages, store.lineages);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A deterministic two-field corpus ("body", "case_name"): gappy
/// ids, terms shared across fields with different postings, docs
/// without a case name, a doc with an empty body but an indexed
/// case name, and one slot with stored text but no postings at all.
fn two_field_corpus() -> Vec<(u32, String, AnalyzedDoc, Option<DocLineage>)> {
    let mut docs = Vec::new();
    for i in 0..40u32 {
        let id = i * 3 + (i % 2);
        let mut body: DocTerms = vec![
            ("court".to_string(), 1 + i % 4, vec![(i, i + 5)]),
            (format!("t{}", i % 6), 1, vec![(i + 7, i + 9)]),
        ];
        if i % 8 == 0 {
            body.push(("smith".to_string(), 2, vec![(0, 5), (9, 14)]));
        }
        if i % 10 == 7 {
            body.clear();
        }
        let body_len: u32 = body.iter().map(|t| t.1).sum();
        let mut fields = vec![AnalyzedField {
            sentences: None,
            terms: body,
            length: body_len,
            positions: None,
        }];
        if i % 3 != 2 {
            // "smith" and "court" appear in BOTH fields, with
            // different postings; "n*" terms only here.
            fields.push(AnalyzedField {
                sentences: None,
                terms: vec![
                    ("smith".to_string(), 1, vec![(0, 5)]),
                    ("court".to_string(), 1, Vec::new()),
                    (format!("n{}", i % 4), 1, Vec::new()),
                ],
                length: 3,
                positions: None,
            });
        }
        let lineage = (i % 4 == 0).then_some(DocLineage {
            parent_id: u64::from(i) * 11,
            group_id: u64::from(i) * 13,
            span_start: i,
            span_end: i + 40,
        });
        docs.push((
            id,
            format!("case {i} body"),
            AnalyzedDoc {
                cased: None,
                fields,
                quality: None,
                geography: None,
                entities: Vec::new(),
            },
            lineage,
        ));
    }
    docs
}

fn two_field_store() -> Bm25Store {
    let mut store = Bm25Store::with_fields(&["body", "case_name"]);
    for (id, text, doc, lineage) in two_field_corpus() {
        store.add_document_with_lineage(id, text, doc, lineage);
    }
    store
}

/// Increment 2 (`docs/multi-field.md` build order step 2): a
/// two-field store round-trips through the v6 file and the reader
/// serves BOTH fields via [`Bm25Reader::field`], each view
/// answering exactly like the heap store's view of the same field,
/// with shared texts, lineages, and document count.
#[test]
fn multi_field_v6_round_trip() {
    let dir = test_dir("v6multi");
    let path = dir.join("shard.bm25");
    let store = two_field_store();
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();

    assert_eq!(reader.field_count(), 2);
    assert_eq!(reader.field_name(0), "body");
    assert_eq!(reader.field_name(1), "case_name");
    assert_eq!(Bm25Index::doc_count(&reader), store.doc_count());

    let terms = ["court", "smith", "t1", "n1", "missing"];
    for f in 0..2 {
        let rv = reader.field(f);
        let sv = store.field(f);
        assert_eq!(rv.doc_count(), sv.doc_count(), "field {f} doc_count");
        assert_eq!(
            rv.total_doc_length(),
            sv.total_doc_length(),
            "field {f} total_doc_length"
        );
        for term in terms {
            assert_eq!(rv.df(term), sv.df(term), "field {f} df({term})");
            let mut got = Vec::new();
            rv.for_each_posting(term, &mut |d, tf, o| got.push((d, tf, o.to_vec())));
            let mut want = Vec::new();
            sv.for_each_posting(term, &mut |d, tf, o| want.push((d, tf, o.to_vec())));
            assert_eq!(got, want, "field {f} postings({term})");
            assert_eq!(
                rv.has_impacts(term),
                sv.df(term) > 0,
                "field {f} has_impacts({term})"
            );
            for (d, _, offs) in &want {
                assert_eq!(
                    &rv.posting_offsets(term, *d),
                    offs,
                    "field {f} posting_offsets({term}, {d})"
                );
            }
        }
        for slot in 0..store.next_doc_id() {
            assert_eq!(
                rv.doc_length(slot),
                sv.doc_length(slot),
                "field {f} dl({slot})"
            );
        }
    }
    // The two fields are genuinely different indexes over the
    // shared slot space.
    assert_ne!(reader.field(0).df("smith"), reader.field(1).df("smith"));
    assert_eq!(reader.field(0).df("n1"), 0);
    assert_ne!(reader.field(1).df("n1"), 0);
    // Shared text/lineage through any view.
    for slot in (0..store.next_doc_id()).step_by(7) {
        assert_eq!(
            reader.field(1).text(slot),
            store.text(slot).map(str::to_string),
            "text({slot})"
        );
        assert_eq!(
            reader.field(1).lineage(slot),
            store.lineage(slot),
            "lineage({slot})"
        );
    }
    // Heap reload (the shard append path) reproduces both fields.
    let loaded = Bm25Store::load(&path).unwrap();
    assert_eq!(loaded.field_count(), 2);
    for f in 0..2 {
        assert_eq!(loaded.fields[f].name, store.fields[f].name);
        assert_eq!(loaded.fields[f].postings, store.fields[f].postings);
        assert_eq!(loaded.fields[f].doc_lengths, store.fields[f].doc_lengths);
        assert_eq!(loaded.fields[f].total_length, store.fields[f].total_length);
    }
    assert_eq!(loaded.texts, store.texts);
    assert_eq!(loaded.lineages, store.lineages);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Fused multi-field scoring through the v6 READER's field views
/// is bit-identical to scoring the heap store's views — the mmap
/// read path changes nothing about the scores.
#[test]
fn fused_scoring_v6_reader_matches_store() {
    use crate::bm25::{top_k_fused_exhaustive, Bm25Params, CorpusStats, FieldQuery};
    let dir = test_dir("v6fused");
    let path = dir.join("shard.bm25");
    let store = two_field_store();
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();

    let body_terms = vec!["court".to_string(), "smith".to_string(), "t3".to_string()];
    let name_terms = vec!["smith".to_string(), "n2".to_string()];
    let stats_for = |f: usize, terms: &[String]| CorpusStats {
        doc_count: store.doc_count(),
        total_doc_length: store.field(f).total_doc_length(),
        dfs: terms.iter().map(|t| store.field(f).df(t)).collect(),
    };
    let body_stats = stats_for(0, &body_terms);
    let name_stats = stats_for(1, &name_terms);
    let queries = |v0: &dyn Bm25Index, v1: &dyn Bm25Index| {
        top_k_fused_exhaustive(
            &[
                FieldQuery {
                    index: v0,
                    terms: &body_terms,
                    stats: body_stats.clone(),
                    params: Bm25Params::default(),
                    weight: 1.0,
                },
                FieldQuery {
                    index: v1,
                    terms: &name_terms,
                    stats: name_stats.clone(),
                    params: Bm25Params { k1: 0.6, b: 0.2 },
                    weight: 2.5,
                },
            ],
            15,
        )
    };
    let want = queries(&store.field(0), &store.field(1));
    let got = queries(&reader.field(0), &reader.field(1));
    assert!(!want.is_empty());
    assert_eq!(want.len(), got.len());
    for (w, g) in want.iter().zip(&got) {
        assert_eq!(w.doc_id, g.doc_id);
        assert_eq!(w.score.to_bits(), g.score.to_bits(), "doc {}", w.doc_id);
        assert_eq!(w.term_offsets, g.term_offsets, "doc {}", w.doc_id);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// The transcoder's contract: transcoding the v4 file of a corpus
/// produces byte-for-byte the v5 file the direct writer emits, on
/// a gappy/lineaged corpus and on one whose hot term spans
/// multiple level-1 skip groups (df > 4096).
#[test]
fn transcode_v4_matches_direct_v5_bytes() {
    let dir = test_dir("transcode");
    let hot_store = {
        let mut store = Bm25Store::new();
        for i in 0..4300u32 {
            let tf = 1 + i % 3;
            let mut terms: DocTerms = vec![("hot".to_string(), tf, vec![(i, i + 2)])];
            if i % 5 == 0 {
                terms.push((format!("t{}", i % 7), 1, Vec::new()));
            }
            let length = terms.iter().map(|t| t.1).sum();
            store.add_document(i * 2, format!("doc {i}"), AnalyzedDoc::body(terms, length));
        }
        store
    };
    for (tag, store) in [("synthetic", synthetic_store()), ("hot", hot_store)] {
        let v4_path = dir.join(format!("{tag}.v4.bm25"));
        {
            let mut w = io::BufWriter::new(std::fs::File::create(&v4_path).unwrap());
            store.write_v4_for_bench(&mut w).unwrap();
            w.flush().unwrap();
        }
        let v5_path = dir.join(format!("{tag}.v5.bm25"));
        let stats = transcode_to_v5(&v4_path, &v5_path).unwrap();
        assert_eq!(stats.n_slots, store.next_doc_id(), "{tag}: n_slots");
        let mut direct = Vec::new();
        store.write_to(&mut direct).unwrap();
        let transcoded = std::fs::read(&v5_path).unwrap();
        assert_eq!(transcoded.len(), direct.len(), "{tag}: sizes differ");
        assert!(
            transcoded == direct,
            "{tag}: transcoded v5 is not byte-identical to the direct writer"
        );
        // And it serves as v5, block-max surface included.
        let reader = Bm25Reader::open(&v5_path).unwrap();
        let probe = if tag == "hot" { "hot" } else { "court" };
        assert!(reader.has_impacts(probe), "{tag}: no impacts on {probe}");
        // Transcoding a v5 file is refused.
        assert!(transcode_to_v5(&v5_path, &dir.join("again.bm25")).is_err());
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// v6 validation rejects malformed headers and section geometry
/// with errors, never panics.
#[test]
fn v6_open_rejects_malformed_files() {
    let dir = test_dir("v6bad");
    let path = dir.join("shard.bm25");
    let store = synthetic_store();
    store.save(&path).unwrap();
    let good = std::fs::read(&path).unwrap();
    assert!(Bm25Reader::open(&path).is_ok());

    let bad_path = dir.join("bad.bm25");
    let expect_err = |mutate: &dyn Fn(&mut Vec<u8>), what: &str| {
        let mut bytes = good.clone();
        mutate(&mut bytes);
        std::fs::write(&bad_path, &bytes).unwrap();
        assert!(Bm25Reader::open(&bad_path).is_err(), "{what} was accepted");
    };
    expect_err(
        &|b| b[8..12].copy_from_slice(&0u32.to_le_bytes()),
        "zero fields",
    );
    expect_err(
        &|b| {
            let v = u64::from_le_bytes(b[16..24].try_into().unwrap()) + 1;
            b[16..24].copy_from_slice(&v.to_le_bytes());
        },
        "texts section not at the header end",
    );
    expect_err(
        &|b| {
            let v = u64::from_le_bytes(b[32..40].try_into().unwrap()) + 1;
            b[32..40].copy_from_slice(&v.to_le_bytes());
        },
        "lineages offset off the text index end",
    );
    expect_err(
        &|b| {
            // Field total_length no longer matches the doc lengths.
            let name_len = u16::from_le_bytes(b[40..42].try_into().unwrap()) as usize;
            let fe = 42 + name_len + 8;
            let v = u64::from_le_bytes(b[fe..fe + 8].try_into().unwrap()) + 1;
            b[fe..fe + 8].copy_from_slice(&v.to_le_bytes());
        },
        "field total_length mismatch",
    );
    expect_err(
        &|b| {
            let n = b.len();
            b.truncate(n - 3);
        },
        "truncated term blob",
    );
    expect_err(&|b| b.truncate(30), "truncated header");
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(test)]
mod malformed_tests {
    use super::*;

    /// Build a store whose v5 file exercises every validated structure:
    /// one term with >4096 postings (2 level-1 groups), other terms,
    /// gap slots, lineages, offsets.
    fn malformed_corpus_store() -> Bm25Store {
        let mut store = Bm25Store::new();
        for i in 0..4300u32 {
            let id = i * 2; // gap slots
            let lineage = (i % 3 == 0).then_some(DocLineage {
                parent_id: u64::from(i),
                group_id: u64::from(i) * 7,
                span_start: i,
                span_end: i + 10,
            });
            let mut terms: DocTerms = vec![
                ("hot".to_string(), 1 + i % 3, vec![(i, i + 2)]),
                (format!("t{}", i % 7), 1, vec![(0, 1)]),
            ];
            if i % 11 == 0 {
                terms.push(("cold".to_string(), 2, vec![(3, 9)]));
            }
            let length = terms.iter().map(|t| t.1).sum();
            store.add_document_with_lineage(
                id,
                format!("d{i}"),
                AnalyzedDoc::body(terms, length),
                lineage,
            );
        }
        store
    }

    fn write_v4(store: &Bm25Store, path: &Path) {
        let mut w = io::BufWriter::new(std::fs::File::create(path).unwrap());
        store.write_v4_for_bench(&mut w).unwrap();
        w.flush().unwrap();
    }

    /// A small store (~few KB on disk) for the every-byte truncation
    /// sweep: gaps, lineages, one multi-block term, offsets.
    fn small_corpus_store() -> Bm25Store {
        let mut store = Bm25Store::new();
        for i in 0..140u32 {
            let id = i * 2;
            let lineage = (i % 3 == 0).then_some(DocLineage {
                parent_id: u64::from(i),
                group_id: u64::from(i) * 7,
                span_start: i,
                span_end: i + 10,
            });
            let terms: DocTerms = vec![
                ("hot".to_string(), 1 + i % 3, vec![(i, i + 2)]),
                (format!("t{}", i % 3), 1, vec![(0, 1)]),
            ];
            let length = terms.iter().map(|t| t.1).sum();
            store.add_document_with_lineage(
                id,
                format!("d{i}"),
                AnalyzedDoc::body(terms, length),
                lineage,
            );
        }
        store
    }

    /// Truncation at EVERY byte length must error, never panic — on a
    /// small file so the sweep stays fast. v6, v5, and v4.
    #[test]
    fn truncated_open_errors_never_panics() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target/tmp")
            .join(format!("truncate_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = small_corpus_store();
        let v6_path = dir.join("small.v6.bm25");
        let v5_path = dir.join("small.v5.bm25");
        let v4_path = dir.join("small.v4.bm25");
        store.save(&v6_path).unwrap();
        store.save_v5(&v5_path).unwrap();
        write_v4(&store, &v4_path);
        for (tag, src) in [("v6", &v6_path), ("v5", &v5_path), ("v4", &v4_path)] {
            let bytes = std::fs::read(src).unwrap();
            Bm25Reader::open(src).unwrap();
            let trunc = dir.join(format!("trunc.{tag}"));
            for len in 0..bytes.len() {
                std::fs::write(&trunc, &bytes[..len]).unwrap();
                assert!(
                    Bm25Reader::open(&trunc).is_err(),
                    "{tag}: truncation to {len} bytes opened successfully"
                );
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupted_open_errors_never_panics() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target/tmp")
            .join(format!("malformed_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = malformed_corpus_store();
        let v6_path = dir.join("corpus.v6.bm25");
        let v5_path = dir.join("corpus.v5.bm25");
        let v4_path = dir.join("corpus.v4.bm25");
        store.save(&v6_path).unwrap();
        store.save_v5(&v5_path).unwrap();
        write_v4(&store, &v4_path);

        for (tag, src) in [("v6", &v6_path), ("v5", &v5_path), ("v4", &v4_path)] {
            let bytes = std::fs::read(src).unwrap();
            // Sanity: the intact file opens.
            Bm25Reader::open(src).unwrap();

            // Random single-byte flips in the header, directory, and
            // skip-run regions: error or valid open, never a panic.
            let mut s = 0xC0FFEEu64;
            let mut next = move || {
                s = s
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (s >> 33) as usize
            };
            // v6: flips anywhere — the explicit section table makes
            // the whole file validated structure. v5/v4: header,
            // directory, and (v5) skip-run regions from the fixed
            // header: magic(8) total_length(8) texts(8) lineages(8)
            // postings(8) directory(8) n_slots(4).
            let regions: Vec<(usize, usize)> = if tag == "v6" {
                vec![(0, bytes.len())]
            } else {
                let directory_off = u64::from_le_bytes(bytes[40..48].try_into().unwrap()) as usize;
                let n_terms =
                    u32::from_le_bytes(bytes[directory_off..directory_off + 4].try_into().unwrap())
                        as usize;
                let mut regions: Vec<(usize, usize)> = vec![(0, 52), (directory_off, bytes.len())];
                if tag == "v5" {
                    for i in 0..n_terms {
                        let e = directory_off + 4 + 34 * i;
                        let skip_off =
                            u64::from_le_bytes(bytes[e + 8..e + 16].try_into().unwrap()) as usize;
                        let end = if i + 1 < n_terms {
                            u64::from_le_bytes(bytes[e + 34..e + 42].try_into().unwrap()) as usize
                        } else {
                            directory_off
                        };
                        regions.push((skip_off, end));
                    }
                }
                regions
            };
            // v3/v4 flips stay in the header and directory: postings
            // record payloads are data (like occurrence bytes and
            // frontier pairs), not validated structure.
            let flipped = dir.join(format!("flip.{tag}"));
            let (mut errors, mut opens) = (0usize, 0usize);
            for _ in 0..2000 {
                let &(lo, hi) = &regions[next() % regions.len()];
                let pos = lo + next() % (hi - lo).max(1);
                let mut bad = bytes.clone();
                bad[pos] ^= 1 + (next() % 255) as u8;
                std::fs::write(&flipped, &bad).unwrap();
                match Bm25Reader::open(&flipped) {
                    Ok(reader) => {
                        opens += 1;
                        // A valid open must stay usable on the read
                        // paths the structure guards (no panics).
                        let _ = reader.df("hot");
                        reader.for_each_doc_tf("hot", &mut |_, _| {});
                        let _ = reader.posting_offsets("hot", 0);
                    }
                    Err(_) => errors += 1,
                }
            }
            assert!(errors > 0, "{tag}: no flip was ever rejected");
            eprintln!("{tag}: flips: {errors} rejected, {opens} opened-valid");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
