//! v6 multi-field layout: round trips, fused scoring, malformed files.

use super::format_tests::test_dir;
use super::store_tests::synthetic_corpus;
use super::*;

fn synthetic_store() -> Bm25Store {
    let mut store = Bm25Store::new();
    for (id, text, doc, lineage) in synthetic_corpus() {
        store.add_document_with_lineage(id, text, doc, lineage);
    }
    store
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
pub(super) fn two_field_corpus() -> Vec<(u32, String, AnalyzedDoc, Option<DocLineage>)> {
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
            ..Default::default()
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
                ..Default::default()
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
                nlp: None,
            },
            lineage,
        ));
    }
    docs
}

fn two_field_store() -> Bm25Store {
    let mut store = Bm25Store::with_text_fields(&["body", "case_name"]);
    for (id, text, doc, lineage) in two_field_corpus() {
        store.add_document_with_lineage(id, text, doc, lineage);
    }
    store
}

/// Every field view of `reader` answers exactly like the store's view.
fn assert_field_views_match(store: &Bm25Store, reader: &Bm25Reader) {
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
}

/// Increment 2: a
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

    assert_field_views_match(&store, &reader);
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
            None,
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
