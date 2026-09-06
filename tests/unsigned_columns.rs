mod common;

use pipestream_search::postings::{
    AnalyzedDoc, Bm25Reader, Bm25Store, SpillBuilder, StoredBinding,
};

fn directory(name: &str) -> std::path::PathBuf {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("unsigned_columns_{name}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
fn analyzed(positional: bool) -> AnalyzedDoc {
    let mut doc = AnalyzedDoc::body(vec![("word".into(), 1, vec![(0, 4)])], 1);
    if positional {
        doc.fields[0].positions = Some(vec![vec![0]]);
        doc.fields[0].sentences = Some(vec![(0, 4)]);
    }
    doc
}

#[test]
fn full_unsigned_domain_survives_both_writers_readers_and_other_column_kinds() {
    let dir = directory("roundtrip");
    let domain = [
        Some(0),
        None,
        Some(1),
        Some((1u64 << 53) + 1),
        Some(i64::MAX as u64),
        Some(1u64 << 63),
        Some(u64::MAX),
    ];
    let source = common::protobuf_source("word", "original");
    for mixed in [false, true] {
        for slots in [0, 1, 7, 8, 9, 65] {
            let tag = format!("{mixed}-{slots}");
            let expected: Vec<_> = (0..slots).map(|row| domain[row % domain.len()]).collect();
            let mut heap = Bm25Store::new().with_unsigned_integers(&["unsigned", "missing"]);
            let mut spill = SpillBuilder::create(&dir.join(format!("{tag}.build")))
                .unwrap()
                .with_unsigned_integer_fields(&["unsigned", "missing"])
                .with_buffer_bytes(32);
            if mixed {
                heap = heap
                    .with_facets(&["facet"])
                    .with_numerics(&["float"])
                    .with_map_facets(&["map_facet"])
                    .with_map_numerics(&["map_float"])
                    .with_integers(&["signed"])
                    .with_geos(&["geo"])
                    .with_positions(&["body"])
                    .with_sentences(&["body"]);
                spill = spill
                    .with_facet_fields(&["facet"])
                    .with_numeric_fields(&["float"])
                    .with_map_facet_fields(&["map_facet"])
                    .with_map_numeric_fields(&["map_float"])
                    .with_integer_fields(&["signed"])
                    .with_geo_fields(&["geo"])
                    .with_position_fields(&["body"])
                    .with_sentence_fields(&["body"]);
                let binding = StoredBinding {
                    plan_fingerprint: "test-plan".into(),
                    body_path: "body".into(),
                    materialize_sha: String::new(),
                    analysis_sha: String::new(),
                    analysis_contract: Vec::new(),
                };
                heap.set_binding(Some(binding.clone()));
                spill.set_binding(Some(binding));
            }
            let source_ids = mixed.then(|| {
                (
                    heap.source_archive_mut().insert(&source).unwrap(),
                    spill.source_archive_mut().insert(&source).unwrap(),
                )
            });
            for (row, value) in expected.iter().enumerate() {
                let row = row as u32;
                heap.add_document(row, "word".into(), analyzed(mixed));
                spill
                    .add_document_with_lineage(row, "word".into(), analyzed(mixed), None)
                    .unwrap();
                if let Some(value) = value {
                    heap.set_unsigned_integer(0, row, *value);
                    spill.set_unsigned_integer(0, row, *value);
                }
                if let Some((heap_source, spill_source)) = source_ids {
                    heap.source_archive_mut()
                        .attach(row, heap_source, Some(row))
                        .unwrap();
                    spill
                        .source_archive_mut()
                        .attach(row, spill_source, Some(row))
                        .unwrap();
                    macro_rules! set_other_kinds {
                        ($store:ident) => {
                            $store.set_integer(0, row, i64::MIN);
                            $store.set_facet(0, row, "label");
                            $store.set_numeric(0, row, 1.5);
                            $store.set_map_facet(0, row, "key", "text");
                            $store.set_map_numeric(0, row, "key", 2.5);
                            $store.set_geo(0, row, 40.0, -70.0);
                        };
                    }
                    set_other_kinds!(heap);
                    set_other_kinds!(spill);
                }
            }
            let path = dir.join(format!("heap-{tag}.bm25"));
            let spilled = dir.join(format!("spill-{tag}.bm25"));
            heap.save(&path).unwrap();
            spill.finish(&spilled).unwrap();
            let bytes = std::fs::read(&path).unwrap();
            assert_eq!(bytes, std::fs::read(&spilled).unwrap(), "{tag}");
            let loaded = Bm25Store::load(&path).unwrap();
            let mapped = Bm25Reader::open(&path).unwrap();
            mapped.verify_integrity().unwrap();
            assert_eq!(mapped.unsigned_integer_count(), 2);
            assert_eq!(mapped.unsigned_integer_name(0), "unsigned");
            assert_eq!(mapped.unsigned_integer_index("unsigned"), Some(0));
            assert_eq!(mapped.integer_index("unsigned"), None);
            let range = (
                expected.iter().flatten().min().copied().unwrap_or(u64::MAX),
                expected.iter().flatten().max().copied().unwrap_or(0),
            );
            assert_eq!(mapped.unsigned_integer_min_max(0), range);
            assert_eq!(loaded.unsigned_integer_min_max(0), range);
            assert_eq!(mapped.unsigned_integer_min_max(1), (u64::MAX, 0));
            for (row, expected) in expected.iter().enumerate() {
                let row = row as u32;
                assert_eq!(mapped.unsigned_integer_value(0, row), *expected);
                assert_eq!(loaded.unsigned_integer_value(0, row), *expected);
                assert_eq!(heap.unsigned_integer_value(0, row), *expected);
                assert_eq!(mapped.unsigned_integer_value(1, row), None);
                if mixed {
                    assert_eq!(mapped.integer_value(0, row), Some(i64::MIN));
                    assert_eq!(
                        mapped.protobuf_source(row).unwrap(),
                        Some((source.clone(), Some(row)))
                    );
                }
            }
            assert_eq!(mapped.unsigned_integer_value(0, slots as u32), None);
            let rewritten = dir.join(format!("rewrite-{tag}.bm25"));
            loaded.save(&rewritten).unwrap();
            assert_eq!(bytes, std::fs::read(rewritten).unwrap());
            assert!(loaded.save_v5(&dir.join("legacy.bm25")).is_err());
            assert!(loaded.write_v4_for_bench(&mut Vec::new()).is_err());
        }
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn signed_and_unsigned_column_names_cannot_alias() {
    let dir = directory("names");
    let heap = Bm25Store::new()
        .with_integers(&["value"])
        .with_unsigned_integers(&["value"]);
    assert!(heap.save(&dir.join("duplicate.bm25")).is_err());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn unsigned_presence_metadata_and_truncation_are_checked() {
    let dir = directory("corruption");
    let mut store = Bm25Store::new().with_unsigned_integers(&["value"]);
    for row in 0..9 {
        store.add_document(row, "word".into(), analyzed(false));
        if row != 1 {
            store.set_unsigned_integer(0, row, u64::MAX - u64::from(row));
        }
    }
    let mut bytes = Vec::new();
    store.write_v6_to(&mut bytes).unwrap();
    let field_len = u16::from_le_bytes(bytes[40..42].try_into().unwrap()) as usize;
    let column = 40 + 2 + field_len + 40 + 4;
    let name_len = u16::from_le_bytes(bytes[column..column + 2].try_into().unwrap()) as usize;
    let kind = column + 2 + name_len;
    assert_eq!(bytes[kind], 11);
    let base = kind + 1;
    let values = u64::from_le_bytes(bytes[base + 16..base + 24].try_into().unwrap()) as usize;
    let bitmap = values + 9 * 8;
    let mut bad = Vec::new();
    let mut padding = bytes.clone();
    padding[bitmap + 1] |= 0x80;
    bad.push(padding);
    let mut absent = bytes.clone();
    absent[values + 8] = 1;
    bad.push(absent);
    let mut bounds = bytes.clone();
    bounds[base..base + 8].copy_from_slice(&0u64.to_le_bytes());
    bad.push(bounds);
    let mut unknown = bytes.clone();
    unknown[kind] = 255;
    bad.push(unknown);
    for offset in [0, 1, values as u64 + 1, u64::MAX - 8, u64::MAX] {
        let mut bad_offset = bytes.clone();
        bad_offset[base + 16..base + 24].copy_from_slice(&offset.to_le_bytes());
        bad.push(bad_offset);
    }
    for cut in values..bytes.len() {
        bad.push(bytes[..cut].to_vec());
    }
    for bytes in bad {
        let path = dir.join("bad.bm25");
        std::fs::write(&path, bytes).unwrap();
        assert!(Bm25Store::load(&path).is_err());
        assert!(Bm25Reader::open(&path).is_err());
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn unsigned_values_survive_sealing_publication_and_reopen() {
    use pipestream_search::live_docs::LiveDocs;
    use pipestream_search::segmented::SegmentedShard;
    use pipestream_search::segments::SegmentSource;

    fn tail() -> Bm25Store {
        Bm25Store::new().with_unsigned_integers(&["unsigned", "missing"])
    }
    fn append(shard: &mut SegmentedShard, values: &[Option<u64>]) {
        for value in values {
            let local = shard.tail().next_doc_id();
            shard
                .tail_mut()
                .add_document(local, "word".into(), analyzed(false));
            if let Some(value) = value {
                shard.tail_mut().set_unsigned_integer(0, local, *value);
            }
        }
        shard.sync_tail();
    }
    fn verify(shard: &SegmentedShard, expected: &[Option<u64>]) {
        assert_eq!(shard.unsigned_integer_count(), 2);
        assert_eq!(shard.unsigned_integer_name(0), "unsigned");
        assert_eq!(shard.unsigned_integer_index("unsigned"), Some(0));
        assert_eq!(shard.integer_index("unsigned"), None);
        for (row, value) in expected.iter().enumerate() {
            assert_eq!(shard.unsigned_integer_value(0, row as u32), *value);
            assert_eq!(shard.unsigned_integer_value(1, row as u32), None);
        }
        assert_eq!(shard.unsigned_integer_value(0, expected.len() as u32), None);
        let min = expected.iter().flatten().copied().min().unwrap_or(u64::MAX);
        let max = expected.iter().flatten().copied().max().unwrap_or(0);
        assert_eq!(shard.unsigned_integer_min_max(0), (min, max));
        assert_eq!(shard.unsigned_integer_min_max(1), (u64::MAX, 0));
    }
    let dir = directory("segments");
    let root = dir.join("catalog");
    let mut shard = SegmentedShard::open(&root, tail()).unwrap();
    let mut expected = Vec::new();
    verify(&shard, &expected);
    for (index, batch) in [
        [Some(u64::MAX), None, Some((1u64 << 63) + 1)],
        [Some((1u64 << 53) + 1), Some(0), None],
        [Some(1), Some(u64::MAX - 1), None],
    ]
    .into_iter()
    .enumerate()
    {
        append(&mut shard, &batch);
        expected.extend(batch);
        verify(&shard, &expected);
        // Refusal leaves the existing schema and values untouched.
        for wrong in [
            Bm25Store::new(),
            Bm25Store::new().with_unsigned_integers(&["missing", "unsigned"]),
            Bm25Store::new().with_integers(&["unsigned", "missing"]),
            tail().with_facets(&["extra"]),
        ] {
            let rows = shard.tail().next_doc_id();
            assert!(shard
                .freeze_tail(wrong, rows)
                .err()
                .unwrap()
                .contains("tables"));
            assert!(shard.frozen().is_none());
            verify(&shard, &expected);
        }
        let rows = shard.tail().next_doc_id();
        let frozen = shard.freeze_tail(tail(), rows).unwrap();
        let base = shard.frozen().unwrap().0;
        verify(&shard, &expected);
        // Ingest continues while the frozen rows await publication.
        let probe = [None, Some(u64::MAX - index as u64)];
        append(&mut shard, &probe);
        expected.extend(probe);
        verify(&shard, &expected);
        let bm25 = dir.join(format!("{index}.bm25"));
        let live = dir.join(format!("{index}.live"));
        frozen.save(&bm25).unwrap();
        LiveDocs::default().write(&live, u64::from(rows)).unwrap();
        let published = shard
            .catalog()
            .append(SegmentSource {
                segment_id: &format!("seg-{index}"),
                generation: index as u64 + 1,
                base_label: u64::from(base),
                backend_kind: "",
                vector_path: None,
                exact_vector_path: None,
                bm25_path: &bm25,
                live_docs_path: &live,
                partition_column: None,
            })
            .unwrap();
        let summary = published.metadata(index).summary.as_ref().unwrap();
        let stored = &expected[base as usize..(base + rows) as usize];
        assert_eq!(summary.uint_columns.len(), 2);
        let range = &summary.uint_columns[0];
        assert_eq!(range.name, "unsigned");
        assert_eq!(
            range.min,
            stored.iter().flatten().copied().min().unwrap_or(u64::MAX)
        );
        assert_eq!(
            range.max,
            stored.iter().flatten().copied().max().unwrap_or(0)
        );
        assert_eq!(range.present, stored.iter().flatten().count() as u64);
        assert_eq!(summary.uint_columns[1].present, 0);
        assert_eq!(summary.uint_columns[1].min, u64::MAX);
        assert_eq!(summary.uint_columns[1].max, 0);
        let encoded = serde_json::to_vec(summary).unwrap();
        let decoded: pipestream_search::segments::SegmentSummary =
            serde_json::from_slice(&encoded).unwrap();
        assert_eq!(&decoded, summary);
        shard.republish(published).unwrap();
        assert!(shard.frozen().is_none());
        assert_eq!(shard.sealed_parts(), index + 1);
        verify(&shard, &expected);
    }
    let sealed_rows = shard.tail_base() as usize;
    drop(shard);
    let reopened = SegmentedShard::open(&root, tail()).unwrap();
    verify(&reopened, &expected[..sealed_rows]);
    for wrong in [
        Bm25Store::new(),
        Bm25Store::new().with_unsigned_integers(&["unsigned"]),
        Bm25Store::new().with_unsigned_integers(&["missing", "unsigned"]),
        Bm25Store::new().with_integers(&["unsigned", "missing"]),
    ] {
        assert!(SegmentedShard::open(&root, wrong)
            .err()
            .unwrap()
            .contains("table"));
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn older_segment_summaries_have_no_unsigned_pruning_information() {
    let old = r#"{"int_columns":[{"name":"signed","min":-1,"max":5,"present":2}],"numeric_columns":[],"partition":null}"#;
    let summary: pipestream_search::segments::SegmentSummary = serde_json::from_str(old).unwrap();
    assert!(summary.uint_columns.is_empty());
    assert_eq!(summary.int_columns[0].min, -1);
    assert!(!serde_json::to_string(&summary)
        .unwrap()
        .contains("uint_columns"));
}
