use super::*;

#[test]
fn exact_key_lookup_survives_sealing_and_builder_reload() {
    let archive = lookup_fixture();
    let mut bytes = Vec::new();
    archive.write(&mut bytes, 9).unwrap();
    let reader = SourceArchiveReader::open(&bytes, 9).unwrap();
    let mut loaded = SourceArchive::read(&bytes, 9).unwrap();
    for key in [vec![0, 255], vec![0, 255, 0], vec![0], vec![255], vec![]] {
        let mut expected = Vec::new();
        for row in 0..9 {
            if let Some(identity) = archive
                .identity(row)
                .filter(|identity| identity.document_key == key)
            {
                expected.push((row, identity.version, identity.chunk_ordinal));
            }
        }
        for variant in 0..3 {
            let mut actual = Vec::new();
            let mut visit = |row, version, ordinal| {
                actual.push((row, version, ordinal));
                true
            };
            assert!(match variant {
                0 => archive.visit_document_rows(&key, &mut visit),
                1 => reader.visit_document_rows(&key, &mut visit),
                _ => loaded.visit_document_rows(&key, &mut visit),
            });
            actual.sort_unstable();
            assert_eq!(actual, expected);
        }
    }
    let mut seen = 0;
    assert!(!reader.visit_document_rows(&[0, 255], &mut |_, _, _| {
        seen += 1;
        false
    }));
    assert_eq!(seen, 1);
    seen = 0;
    assert!(!archive.visit_document_rows(&[0, 255], &mut |_, _, _| {
        seen += 1;
        false
    }));
    assert_eq!(seen, 1);
    let identity = DocumentIdentity {
        document_key: vec![0, 255],
        version: 2,
        chunk_ordinal: Some(2),
    };
    loaded
        .attach_source_with_identity(9, &source(), Some(2), Some(&identity))
        .unwrap();
    let mut found = Vec::new();
    loaded.visit_document_rows(&[0, 255], &mut |row, _, _| {
        found.push(row);
        true
    });
    found.sort_unstable();
    assert_eq!(found, [0, 1, 7, 9]);
    let mut upgraded = Vec::new();
    loaded.write(&mut upgraded, 10).unwrap();
    assert_eq!(
        SourceArchiveReader::open(&upgraded, 10)
            .unwrap()
            .index
            .format_version,
        3
    );
}

#[test]
fn malformed_reverse_lookup_is_refused_even_with_valid_checksum() {
    let mut bytes = Vec::new();
    lookup_fixture().write(&mut bytes, 9).unwrap();
    for corruption in 0..10 {
        let corrupt = rewrite_archive(&bytes, |index| {
            match corruption {
                0 => index.identity_lookup = None,
                1 => index
                    .identity_lookup
                    .as_mut()
                    .unwrap()
                    .identities
                    .swap(0, 1),
                2 => index.identity_lookup.as_mut().unwrap().identities[0] = 0,
                3 => index.identity_lookup.as_mut().unwrap().offsets[0] = 1,
                4 => index.identity_lookup.as_mut().unwrap().offsets[1] = u32::MAX,
                5 => index.identity_lookup.as_mut().unwrap().rows[0] = 8, // unbound
                6 => index.identity_lookup.as_mut().unwrap().rows[0] = 99,
                7 => {
                    let lookup = index.identity_lookup.as_mut().unwrap();
                    lookup.rows[2] = lookup.rows[1];
                }
                8 => {
                    let lookup = index.identity_lookup.as_mut().unwrap();
                    lookup.rows.pop();
                    *lookup.offsets.last_mut().unwrap() -= 1;
                }
                9 => index.format_version = 2, // never accept a disguised new contract
                _ => unreachable!(),
            }
        });
        assert!(
            SourceArchiveReader::open(&corrupt, 9).is_err(),
            "corruption {corruption}"
        );
    }
}

#[test]
fn logical_identity_is_interned_and_survives_archive_reload() {
    let mut archive = SourceArchive::default();
    let key = vec![0xfe; 16 * 1024];
    let source = source();
    for (row, ordinal) in [(0, 0), (2, 1)] {
        let identity = DocumentIdentity {
            document_key: key.clone(),
            version: 17,
            chunk_ordinal: Some(ordinal),
        };
        archive
            .attach_source_with_identity(row, &source, Some(ordinal), Some(&identity))
            .unwrap();
    }
    let mut bytes = Vec::new();
    archive.write(&mut bytes, 3).unwrap();
    let reader = SourceArchiveReader::open(&bytes, 3).unwrap();
    assert_eq!(reader.index.format_version, 3);
    assert_eq!(reader.index.identities.len(), 1);
    assert_eq!(reader.index.sources.len(), 1);
    assert_eq!(
        bytes
            .windows(key.len())
            .filter(|window| *window == key)
            .count(),
        1
    );
    let restored = SourceArchive::read(&bytes, 3).unwrap();
    for row in [0, 1, 2] {
        assert_eq!(reader.identity(row), archive.identity(row));
        assert_eq!(restored.identity(row), archive.identity(row));
    }
    assert!(reader.identity(1).is_none());
    let mut rewritten = Vec::new();
    restored.write(&mut rewritten, 3).unwrap();
    assert_eq!(rewritten, bytes);
}

#[test]
fn conflicting_source_version_and_chunk_metadata_are_refused_before_row_attachment() {
    let mut archive = SourceArchive::default();
    let mut identity = DocumentIdentity {
        document_key: b"key".to_vec(),
        version: 1,
        chunk_ordinal: Some(0),
    };
    archive
        .attach_source_with_identity(0, &source(), Some(0), Some(&identity))
        .unwrap();
    identity.chunk_ordinal = Some(1);
    assert!(archive
        .attach_source_with_identity(1, &source(), Some(0), Some(&identity))
        .is_err());
    let mut different = source();
    different.payload.push(0);
    assert!(archive
        .attach_source_with_identity(1, &different, Some(1), Some(&identity))
        .is_err());
    assert!(archive.row(1).unwrap().is_none());
    let mut bytes = Vec::new();
    archive.write(&mut bytes, 1).unwrap();
    let reader = SourceArchiveReader::open(&bytes, 1).unwrap();
    // Any format label other than the current one is refused.
    let mut index = (*reader.index).clone();
    index.format_version = 1;
    let encoded = index.encode_to_vec();
    bytes[16..48].copy_from_slice(&turbo_sha256::sha256(&encoded).0);
    bytes[48..48 + encoded.len()].copy_from_slice(&encoded);
    assert!(SourceArchiveReader::open(&bytes, 1).is_err());
}

#[test]
fn shared_sources_preserve_exact_bytes_and_sparse_chunk_references() {
    let source = source();
    let mut archive = SourceArchive::default();
    let id = archive.insert(&source).unwrap();
    assert_eq!(archive.insert(&source).unwrap(), id);
    archive.attach(0, id, Some(0)).unwrap();
    archive.attach(2, id, Some(1)).unwrap();
    assert!(archive.attach(0, id, None).is_err());
    assert!(archive.attach(3, 99, None).is_err());
    let mut bytes = Vec::new();
    archive.write(&mut bytes, 4).unwrap();
    assert_eq!(bytes.len() as u64, archive.encoded_len(4).unwrap());
    let reader = SourceArchiveReader::open(&bytes, 4).unwrap();
    assert_eq!(reader.index.sources.len(), 1);
    assert_eq!(reader.index.descriptors.len(), 1);
    for (row, ordinal) in [(0, 0), (2, 1)] {
        assert_eq!(
            reader.row(&bytes, row).unwrap(),
            Some((source.clone(), Some(ordinal)))
        );
    }
    assert!(reader.row(&bytes, 1).unwrap().is_none());
    assert!(reader.row(&bytes, 3).unwrap().is_none());
    assert!(reader.row(&bytes, 4).is_err());
    reader.verify(&bytes).unwrap();
    let restored = SourceArchive::read(&bytes, 4).unwrap();
    let mut rewritten = Vec::new();
    restored.write(&mut rewritten, 4).unwrap();
    assert_eq!(rewritten, bytes);
}

#[test]
fn originals_without_rows_and_reused_descriptors_survive() {
    let mut source = source();
    let mut archive = SourceArchive::default();
    let first = archive.insert(&source).unwrap();
    source.payload.clear();
    let second = archive.insert(&source).unwrap();
    let mut bytes = Vec::new();
    archive.write(&mut bytes, 0).unwrap();
    let reader = SourceArchiveReader::open(&bytes, 0).unwrap();
    assert_eq!(reader.index.sources.len(), 2);
    assert_eq!(reader.index.descriptors.len(), 1);
    assert_ne!(
        reader.source(&bytes, first).unwrap().payload,
        source.payload
    );
    assert_eq!(reader.source(&bytes, second).unwrap(), source);
}

#[test]
fn every_truncation_and_byte_corruption_is_detected() {
    let mut archive = SourceArchive::default();
    let id = archive.insert(&source()).unwrap();
    archive.attach(0, id, None).unwrap();
    let mut bytes = Vec::new();
    archive.write(&mut bytes, 1).unwrap();
    for at in 0..bytes.len() {
        let truncated = &bytes[..at];
        assert!(
            SourceArchiveReader::open(truncated, 1)
                .and_then(|r| r.verify(truncated))
                .is_err(),
            "truncate {at}"
        );
        let mut corrupted = bytes.clone();
        corrupted[at] ^= 1;
        assert!(
            SourceArchiveReader::open(&corrupted, 1)
                .and_then(|r| r.verify(&corrupted))
                .is_err(),
            "flip {at}"
        );
    }
    assert!(SourceArchiveReader::open(&bytes, 2).is_err());
    bytes.push(0);
    assert!(SourceArchiveReader::open(&bytes, 1).is_err());
}
