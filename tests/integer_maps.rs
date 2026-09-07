//! Exact integer maps retain their types, presence and bounds across both writers/readers.
mod common;
use pipestream_search::postings::{AnalyzedDoc, Bm25Reader, Bm25Store, SpillBuilder};

fn directory(tag: &str) -> std::path::PathBuf {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("integer-maps-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn document() -> AnalyzedDoc {
    AnalyzedDoc::body(vec![("map".into(), 1, vec![(0, 3)])], 1)
}

#[test]
fn integer_map_writers_and_readers_preserve_exact_values() {
    let dir = directory("roundtrip");
    let mut heap = Bm25Store::with_fields(&["body"])
        .with_map_numerics(&["float"])
        .with_integers(&["signed"])
        .with_unsigned_integers(&["unsigned"])
        .with_map_integers(&["signed_map", "empty_signed"])
        .with_map_unsigned_integers(&["unsigned_map", "empty_unsigned"]);
    let mut spill = SpillBuilder::create_with_fields(&dir.as_path().join("build"), &["body"])
        .unwrap()
        .with_map_numeric_fields(&["float"])
        .with_integer_fields(&["signed"])
        .with_unsigned_integer_fields(&["unsigned"])
        .with_map_integer_fields(&["signed_map", "empty_signed"])
        .with_map_unsigned_integer_fields(&["unsigned_map", "empty_unsigned"])
        .with_buffer_bytes(1);
    for row in 0..5 {
        heap.add_document(row, "map".into(), document());
        spill
            .add_document_with_lineage(row, "map".into(), document(), None)
            .unwrap();
    }
    let long_key = "é".repeat(40_000);
    let signed = [
        (0, "z", i64::MIN),
        (0, "", i64::MAX),
        (1, "", 0),
        (3, "z", (1i64 << 53) + 1),
        (3, &long_key, -1),
    ];
    let unsigned = [
        (0, "z", u64::MAX),
        (0, "", (1u64 << 53) + 1),
        (1, "", 0),
        (3, "z", 1),
        (3, &long_key, 1 << 63),
    ];
    for (row, key, value) in signed {
        heap.set_map_integer(0, row, key, value).unwrap();
        spill.set_map_integer(0, row, key, value).unwrap();
    }
    for (row, key, value) in unsigned {
        heap.set_map_unsigned_integer(0, row, key, value).unwrap();
        spill.set_map_unsigned_integer(0, row, key, value).unwrap();
    }
    assert!(heap.set_map_integer(0, 0, "z", 7).is_err());
    assert!(heap.set_map_integer(0, 0, "", i64::MIN).is_err());
    assert!(spill.set_map_integer(0, 0, "", i64::MIN).is_err());
    assert!(spill.set_map_unsigned_integer(0, 0, "z", 7).is_err());
    assert!(heap.set_map_integer(0, u32::MAX, "bad", 7).is_err());
    assert!(spill
        .set_map_unsigned_integer(0, u32::MAX, "bad", 7)
        .is_err());
    assert!(heap.set_map_integer(99, 0, "bad", 7).is_err());
    for row in [0, 3] {
        heap.set_map_numeric(0, row, "", 2.5);
        spill.set_map_numeric(0, row, "", 2.5);
        heap.set_integer(0, row, i64::MIN);
        spill.set_integer(0, row, i64::MIN);
        heap.set_unsigned_integer(0, row, u64::MAX);
        spill.set_unsigned_integer(0, row, u64::MAX);
    }
    let source = common::protobuf_source("map", "original");
    for archive in [heap.source_archive_mut(), spill.source_archive_mut()] {
        archive
            .attach_source_with_identity(3, &source, Some(0), None)
            .unwrap();
    }
    let heap_path = dir.as_path().join("heap");
    let spill_path = dir.as_path().join("spill");
    heap.save(&heap_path).unwrap();
    spill.finish(&spill_path).unwrap();
    assert_eq!(
        std::fs::read(&heap_path).unwrap(),
        std::fs::read(&spill_path).unwrap()
    );
    for path in [&heap_path, &spill_path] {
        let read = Bm25Reader::open(path).unwrap();
        let loaded = Bm25Store::load(path).unwrap();
        assert_eq!(
            read.protobuf_source(3).unwrap(),
            Some((source.clone(), Some(0)))
        );
        read.verify_integrity().unwrap();
        assert_eq!(read.map_integer_count(), 2);
        assert_eq!(loaded.map_unsigned_integer_count(), 2);
        assert_eq!(read.map_unsigned_integer_name(0), "unsigned_map");
        assert_eq!(read.map_integer_index("signed_map"), Some(0));
        assert!(read.map_integer_keys(1).is_empty());
        assert!(read.map_unsigned_integer_keys(1).is_empty());
        assert_eq!(
            read.map_integer_keys(0),
            &["".to_string(), "z".to_string(), long_key.clone()]
        );
        for (row, key, value) in signed {
            let ord = read.map_integer_key_ord(0, key).unwrap();
            assert_eq!(read.map_integer_value(0, ord, row), Some(value));
            assert_eq!(
                loaded.map_integer_value(0, loaded.map_integer_key_ord(0, key).unwrap(), row),
                Some(value)
            );
            assert_eq!(read.map_integer_value(0, ord, 2), None);
            assert_eq!(read.map_integer_value(0, ord, 4), None);
            assert_eq!(read.map_integer_value(0, ord, u32::MAX), None);
        }
        for (row, key, value) in unsigned {
            let ord = read.map_unsigned_integer_key_ord(0, key).unwrap();
            assert_eq!(read.map_unsigned_integer_value(0, ord, row), Some(value));
            assert_eq!(
                loaded.map_unsigned_integer_value(
                    0,
                    loaded.map_unsigned_integer_key_ord(0, key).unwrap(),
                    row
                ),
                Some(value)
            );
            assert_eq!(read.map_unsigned_integer_value(0, ord, 2), None);
        }
        assert_eq!(read.map_integer_key_min_max(0, 0), Some((0, i64::MAX)));
        assert_eq!(
            read.map_integer_key_min_max(0, 1),
            Some((i64::MIN, (1i64 << 53) + 1))
        );
        assert_eq!(
            read.map_unsigned_integer_key_min_max(0, 1),
            Some((1, u64::MAX))
        );
        assert_eq!(read.map_unsigned_integer_key_min_max(1, 0), None);
        assert_eq!(read.map_integer_value(0, u32::MAX, 0), None);
        let copied = dir.as_path().join("copied");
        loaded.save(&copied).unwrap();
        assert_eq!(std::fs::read(path).unwrap(), std::fs::read(copied).unwrap());
    }
    let mut legacy = Vec::new();
    assert_eq!(
        heap.write_v4_for_bench(&mut legacy).unwrap_err().kind(),
        std::io::ErrorKind::Unsupported
    );
    assert!(legacy.is_empty());
    assert_eq!(
        heap.save_v5(&dir.as_path().join("v5")).unwrap_err().kind(),
        std::io::ErrorKind::Unsupported
    );
}

#[test]
fn integer_map_only_and_empty_files_have_valid_section_boundaries() {
    let dir = directory("empty");
    for rows in [0, 1] {
        for unsigned in [false, true] {
            let mut heap = Bm25Store::new();
            heap = if unsigned {
                heap.with_map_unsigned_integers(&["m"])
            } else {
                heap.with_map_integers(&["m"])
            };
            if rows == 1 {
                heap.add_document(0, "map".into(), document());
            }
            let path = dir.as_path().join(format!("{rows}-{unsigned}"));
            heap.save(&path).unwrap();
            let read = Bm25Reader::open(&path).unwrap();
            let loaded = Bm25Store::load(&path).unwrap();
            if unsigned {
                assert_eq!(read.map_unsigned_integer_count(), 1);
                assert!(loaded.map_unsigned_integer_keys(0).is_empty());
            } else {
                assert_eq!(read.map_integer_count(), 1);
                assert!(loaded.map_integer_keys(0).is_empty());
            }
        }
    }
}

#[test]
fn integer_maps_translate_keys_across_sealed_frozen_and_mutable_parts() {
    use pipestream_search::{segmented::SegmentedShard, segments::SegmentSource};
    let dir = directory("segments");
    let root = dir.join("catalog");
    let tail = || {
        Bm25Store::new()
            .with_map_integers(&["signed"])
            .with_map_unsigned_integers(&["unsigned"])
    };
    let mut shard = SegmentedShard::open(&root, tail()).unwrap();
    let rows = [
        ("z", i64::MIN, u64::MAX),
        ("", 0, 0),
        ("a", (1i64 << 53) + 1, (1u64 << 53) + 1),
        ("z", i64::MAX, 1 << 63),
        ("", -1, 42),
        ("b", 3, 7),
    ];
    let check = |shard: &SegmentedShard, count: usize| {
        for (row, &(key, signed, unsigned)) in rows[..count].iter().enumerate() {
            let sk = shard.map_integer_key_ord(0, key).unwrap();
            let uk = shard.map_unsigned_integer_key_ord(0, key).unwrap();
            assert_eq!(shard.map_integer_value(0, sk, row as u32), Some(signed));
            assert_eq!(
                shard.map_unsigned_integer_value(0, uk, row as u32),
                Some(unsigned)
            );
            let values: Vec<_> = rows[..count]
                .iter()
                .filter(|entry| entry.0 == key)
                .collect();
            assert_eq!(
                shard.map_integer_key_min_max(0, sk),
                Some((
                    values.iter().map(|e| e.1).min().unwrap(),
                    values.iter().map(|e| e.1).max().unwrap()
                ))
            );
            assert_eq!(
                shard.map_unsigned_integer_key_min_max(0, uk),
                Some((
                    values.iter().map(|e| e.2).min().unwrap(),
                    values.iter().map(|e| e.2).max().unwrap()
                ))
            );
            for other in 0..count {
                if rows[other].0 != key {
                    assert_eq!(shard.map_integer_value(0, sk, other as u32), None);
                    assert_eq!(shard.map_unsigned_integer_value(0, uk, other as u32), None);
                }
            }
            assert_eq!(shard.map_integer_value(0, sk, u32::MAX), None);
        }
        assert_eq!(shard.map_unsigned_integer_key_min_max(0, u32::MAX), None);
    };
    let add_row = |shard: &mut SegmentedShard, row: usize| {
        let local = row as u32 - shard.tail_base();
        shard
            .add_document(row as u32, "map".into(), document(), None)
            .unwrap();
        shard
            .tail_mut()
            .set_map_integer(0, local, rows[row].0, rows[row].1)
            .unwrap();
        shard
            .tail_mut()
            .set_map_unsigned_integer(0, local, rows[row].0, rows[row].2)
            .unwrap();
        shard.sync_tail();
    };
    for batch in 0..3 {
        for row in batch * 2..batch * 2 + 2 {
            if shard.next_doc_id() <= row as u32 {
                add_row(&mut shard, row);
            }
            check(&shard, row + 1);
        }
        assert!(shard.freeze_tail(Bm25Store::new(), 2).is_err());
        let frozen = shard.freeze_tail(tail(), 2).unwrap();
        check(&shard, (batch + 1) * 2);
        if batch == 0 {
            // The fresh tail introduces a key absent from the frozen dictionary.
            add_row(&mut shard, 2);
            check(&shard, 3);
        }
        let (base, count, _) = shard.frozen().unwrap();
        let image = dir.join(format!("{batch}.bm25"));
        let live = dir.join(format!("{batch}.live"));
        frozen.save(&image).unwrap();
        pipestream_search::live_docs::LiveDocs::default()
            .write(&live, count as u64)
            .unwrap();
        let published = shard
            .catalog()
            .append(SegmentSource {
                segment_id: &format!("seg-{batch}"),
                generation: batch as u64 + 1,
                base_label: base as u64,
                backend_kind: "",
                vector_path: None,
                exact_vector_path: None,
                bm25_path: &image,
                live_docs_path: &live,
                partition_column: None,
            })
            .unwrap();
        shard.republish(published).unwrap();
        check(&shard, shard.next_doc_id() as usize);
    }
    drop(shard);
    assert!(SegmentedShard::open(&root, Bm25Store::new()).is_err());
    let reopened = SegmentedShard::open(&root, tail()).unwrap();
    check(&reopened, rows.len());
    std::fs::remove_dir_all(dir).unwrap();
}
