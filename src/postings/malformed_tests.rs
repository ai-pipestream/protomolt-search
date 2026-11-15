use std::io;
use std::path::Path;

use super::format_tests::test_dir;
use super::*;

/// Build a store that exercises every validated structure: one term with
/// `> 4096` postings (2 level-1 groups), secondary terms, gap slots, lineages,
/// and occurrence offsets.
fn malformed_corpus_store() -> Bm25Store {
    let mut store = Bm25Store::new();
    for i in 0..4300u32 {
        let id = i * 2;
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

/// Small store (~few KB on disk) for the every-byte truncation sweep:
/// gaps, lineages, one multi-block term, and offsets.
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

fn write_raw_payload(store: &Bm25Store, path: &Path) {
    let mut raw = Vec::new();
    store.write_v6_to(&mut raw).unwrap();
    std::fs::write(path, raw).unwrap();
}

/// Truncation at EVERY byte length must error, never panic — across both
/// raw payload (`TVBM2506`) and integrity-wrapped (`TVBM2508`) files.
#[test]
fn truncated_open_errors_never_panics() {
    let dir = test_dir("truncate");
    let store = small_corpus_store();
    let raw_path = dir.join("small.raw.bm25");
    let v8_path = dir.join("small.v8.bm25");
    write_raw_payload(&store, &raw_path);
    store.save(&v8_path).unwrap();
    for (tag, src) in [("raw", &raw_path), ("v8", &v8_path)] {
        let bytes = std::fs::read(src).unwrap();
        Bm25Reader::open(src).unwrap();
        let trunc = dir.join(format!("trunc.{tag}"));
        for len in 0..bytes.len() {
            std::fs::write(&trunc, &bytes[..len]).unwrap();
            assert!(
                Bm25Reader::open(&trunc).is_err(),
                "{tag}: truncation to {len} bytes opened successfully"
            );
            assert!(
                Bm25Store::load(&trunc).is_err(),
                "{tag}: truncation to {len} bytes loaded successfully"
            );
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn corrupted_open_errors_never_panics() {
    let dir = test_dir("malformed");
    let store = malformed_corpus_store();
    let raw_path = dir.join("corpus.raw.bm25");
    let v8_path = dir.join("corpus.v8.bm25");
    write_raw_payload(&store, &raw_path);
    store.save(&v8_path).unwrap();

    for (tag, src) in [("raw", &raw_path), ("v8", &v8_path)] {
        let bytes = std::fs::read(src).unwrap();
        Bm25Reader::open(src).unwrap();
        let mut s = 0xC0FFEEu64;
        let mut next = move || {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (s >> 33) as usize
        };
        let flipped = dir.join(format!("flip.{tag}"));
        let mut errors = 0usize;
        for _ in 0..2000 {
            let pos = next() % bytes.len();
            let mut bad = bytes.clone();
            bad[pos] ^= 1 + (next() % 255) as u8;
            std::fs::write(&flipped, &bad).unwrap();
            match Bm25Reader::open(&flipped) {
                Ok(reader) => {
                    let _ = reader.df("hot");
                    reader.for_each_doc_tf("hot", &mut |_, _| {});
                    let _ = reader.posting_offsets("hot", 0);
                }
                Err(_) => errors += 1,
            }
        }
        assert!(errors > 0, "{tag}: no flip was ever rejected");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn legacy_magics_and_kind15_derived_fail_closed() {
    let dir = test_dir("legacy_refusal");
    let bad_path = dir.join("bad.bm25");
    for magic in [
        &b"TVBM2501"[..],
        &b"TVBM2502"[..],
        &b"TVBM2503"[..],
        &b"TVBM2504"[..],
        &b"TVBM2505"[..],
        &b"TVBM2500"[..],
        &b"TVBM2599"[..],
        &b"TVBM25"[..],
    ] {
        let mut buf = magic.to_vec();
        buf.resize(128, 0);
        std::fs::write(&bad_path, &buf).unwrap();
        assert_eq!(
            Bm25Reader::open(&bad_path).err().unwrap().kind(),
            io::ErrorKind::InvalidData,
        );
        assert_eq!(
            Bm25Store::load(&bad_path).err().unwrap().kind(),
            io::ErrorKind::InvalidData,
        );
    }

    let mut store = small_corpus_store();
    let decl = crate::derived::Declaration::compile(&crate::pb::DerivedColumns {
        columns: vec![crate::pb::DerivedColumn {
            name: "year".into(),
            expression: "calendar.year(decided)".into(),
            kind: crate::pb::MaterializeKind::I64 as i32,
            disclosure: crate::pb::DerivedDisclosure::Inputs as i32,
        }],
    })
    .unwrap();
    store.set_derived(Some(StoredDerived::of(&decl)));
    let mut raw = Vec::new();
    store.write_v6_to(&mut raw).unwrap();
    let needle = b"derived-columns";
    let pos = raw.windows(needle.len()).position(|w| w == needle).unwrap();
    assert_eq!(raw[pos + needle.len()], 17);
    raw[pos + needle.len()] = 15;
    std::fs::write(&bad_path, &raw).unwrap();
    assert_eq!(
        Bm25Reader::open(&bad_path).err().unwrap().kind(),
        io::ErrorKind::InvalidData,
    );
    assert_eq!(
        Bm25Store::load(&bad_path).err().unwrap().kind(),
        io::ErrorKind::InvalidData,
    );
    let _ = std::fs::remove_dir_all(&dir);
}
