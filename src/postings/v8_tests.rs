//! v8 integrity envelope: round trips, damage sweeps, and deep verify.

use std::io;

use super::format::{MAGIC_V6, MAGIC_V8};
use super::store_tests::synthetic_corpus;
use super::*;

/// A small store for the exhaustive v8 damage sweeps: big enough
/// to have every section populated, small enough that flipping
/// every byte stays cheap.
fn v8_store(columns: bool) -> Bm25Store {
    let mut store = if columns {
        Bm25Store::new()
            .with_columns(crate::postings::ColumnKind::Facet, &["court"])
            .with_columns(crate::postings::ColumnKind::Integer, &["cited"])
            .with_columns(crate::postings::ColumnKind::Geo, &["place"])
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

/// Autocomplete over the dictionary: the heap
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
