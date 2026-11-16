use super::pages::*;
use super::*;

#[test]
fn composed_identity_views_keep_range_boundaries_and_reject_aliases() {
    let mut archive = SourceArchive::default();
    let identity = DocumentIdentity {
        document_key: b"source".to_vec(),
        version: 1,
        chunk_ordinal: None,
    };
    for row in 0..2 {
        archive
            .attach_source_with_identity(row, &source(), None, Some(&identity))
            .unwrap();
    }
    let source = archive.identity_snapshot();
    let snapshot = IdentitySnapshot::from_parts([
        (4, 1, source.clone()),
        (7, 2, source.clone()),
        (u32::MAX, 1, source.clone()),
    ])
    .unwrap();
    for row in [4, 7, 8, u32::MAX] {
        assert_eq!(snapshot.identity(row), Some(identity.clone()));
    }
    for row in [0, 3, 5, 6, 9, u32::MAX - 1] {
        assert!(snapshot.identity(row).is_none());
    }
    let adjacent =
        IdentitySnapshot::from_parts([(0, 1, source.clone()), (1, 1, IdentitySnapshot::default())])
            .unwrap();
    assert!(adjacent.identity(0).is_some());
    assert!(adjacent.identity(1).is_none());
    for ranges in [
        vec![(4, 2), (5, 1)],
        vec![(4, 1), (3, 1)],
        vec![(u32::MAX, 2)],
    ] {
        assert!(
            IdentitySnapshot::from_parts(ranges.into_iter().map(|(b, n)| (b, n, source.clone())))
                .is_err()
        );
    }
    assert!(IdentitySnapshot::from_parts([])
        .unwrap()
        .identity(0)
        .is_none());
    assert!(IdentitySnapshot::from_parts([(0, 0, source)])
        .unwrap()
        .identity(0)
        .is_none());
}

#[test]
fn identity_snapshots_survive_sparse_appends_and_archive_replacement() {
    let mut archive = SourceArchive::default();
    let source = source();
    let first = DocumentIdentity {
        document_key: vec![0, 255, 1],
        version: u64::MAX,
        chunk_ordinal: Some(0),
    };
    archive
        .attach_source_with_identity(2, &source, Some(0), Some(&first))
        .unwrap();
    let before = archive.identity_snapshot();
    let original_blobs: Vec<_> = archive
        .descriptors
        .values()
        .chain(archive.sources.iter().map(|(_, blob)| blob))
        .map(|blob| match blob {
            ArchiveBlob::Memory(bytes) => Arc::downgrade(bytes),
            _ => unreachable!(),
        })
        .collect();
    let second = DocumentIdentity {
        document_key: b"second".to_vec(),
        version: 1,
        chunk_ordinal: None,
    };
    archive
        .attach_source_with_identity(0, &source, None, Some(&second))
        .unwrap();
    archive
        .attach_source_with_identity(3, &source, None, Some(&second))
        .unwrap();
    let after = archive.identity_snapshot();
    let mut bytes = Vec::new();
    archive.write(&mut bytes, 4).unwrap();
    let reader = SourceArchiveReader::open(&bytes, 4).unwrap();
    let from_reader = reader.identity_snapshot();
    assert!(Arc::ptr_eq(
        &reader.index,
        match &from_reader.data {
            IdentitySnapshotData::Encoded(index) => index,
            _ => unreachable!(),
        }
    ));
    drop(reader);
    drop(bytes);
    drop(archive);
    assert!(original_blobs.iter().all(|weak| weak.upgrade().is_none()));
    assert_eq!(before.identity(2), Some(first.clone()));
    for row in [0, 1, 3, u32::MAX] {
        assert!(before.identity(row).is_none());
    }
    for snapshot in [after, from_reader] {
        assert_eq!(snapshot.identity(0), Some(second.clone()));
        assert_eq!(snapshot.identity(2), Some(first.clone()));
        assert_eq!(snapshot.identity(3), Some(second.clone()));
        assert!(snapshot.identity(1).is_none());
        assert!(snapshot.identity(u32::MAX).is_none());
    }
}

#[test]
fn identity_snapshot_updates_copy_only_touched_pages_and_share_key_bytes() {
    let mut archive = SourceArchive::default();
    let source = source();
    let count = IDENTITY_PAGE_ROWS + 2;
    for row in 1..count {
        let identity = DocumentIdentity {
            document_key: row.to_le_bytes().to_vec(),
            version: 1,
            chunk_ordinal: None,
        };
        archive
            .attach_source_with_identity(row as u32, &source, None, Some(&identity))
            .unwrap();
    }
    let snapshot = archive.identity_snapshot();
    let IdentitySnapshotData::Building { rows, identities } = &snapshot.data else {
        unreachable!()
    };
    assert!(Arc::ptr_eq(&rows.pages, &archive.rows.pages));
    assert!(Arc::ptr_eq(&identities.pages, &archive.identities.pages));
    let another = DocumentIdentity {
        document_key: vec![255; 16 * 1024],
        version: 2,
        chunk_ordinal: None,
    };
    archive
        .attach_source_with_identity(count as u32, &source, None, Some(&another))
        .unwrap();
    assert!(Arc::ptr_eq(&rows.pages[0], &archive.rows.pages[0]));
    assert!(!Arc::ptr_eq(&rows.pages[1], &archive.rows.pages[1]));
    assert!(Arc::ptr_eq(
        &identities.pages[0],
        &archive.identities.pages[0]
    ));
    assert!(!Arc::ptr_eq(
        &identities.pages[1],
        &archive.identities.pages[1]
    ));
    assert!(Arc::ptr_eq(
        &identities[IDENTITY_PAGE_ROWS].document_key,
        &archive.identities[IDENTITY_PAGE_ROWS].document_key
    ));
    archive
        .attach_source_with_identity(0, &source, None, Some(&another))
        .unwrap();
    assert!(!Arc::ptr_eq(&rows.pages[0], &archive.rows.pages[0]));
    assert!(snapshot.identity(0).is_none());
    assert!(snapshot.identity(count as u32).is_none());
    assert_eq!(archive.identity(0), Some(another));
    for row in 1..count {
        assert_eq!(snapshot.identity(row as u32), archive.identity(row as u32));
    }
}

fn source() -> ProtobufSource {
    ProtobufSource {
        descriptor_set: include_bytes!("../../../tests/fixtures/protobuf-semantics/descriptor.bin")
            .to_vec(),
        message_type: "semantics.Doc".into(),
        // Noncanonical varint and an unknown field, retained without decode.
        payload: vec![8, 0x81, 0, 0xa0, 6, 99],
    }
}

fn rewrite_archive(bytes: &[u8], mutate: impl FnOnce(&mut SourceArchiveIndex)) -> Vec<u8> {
    let old_len = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
    let mut index = SourceArchiveIndex::decode(&bytes[48..48 + old_len]).unwrap();
    mutate(&mut index);
    let index = index.encode_to_vec();
    let mut output = Vec::new();
    output.extend_from_slice(MAGIC);
    output.extend_from_slice(&(index.len() as u64).to_le_bytes());
    output.extend_from_slice(&turbo_sha256::sha256(&index).0);
    output.extend_from_slice(&index);
    output.extend_from_slice(&bytes[48 + old_len..]);
    output
}

fn lookup_fixture() -> SourceArchive {
    let mut archive = SourceArchive::default();
    // Deliberately interleave keys, versions and row positions. Exact binary
    // keys must not alias by UTF-8 conversion, prefixes or source hashes.
    for (row, key, version, ordinal) in [
        (7, vec![0, 255], 2, Some(1)),
        (3, vec![0, 255, 0], 1, None),
        (1, vec![0, 255], u64::MAX, None),
        (0, vec![0, 255], 2, Some(0)),
        (5, vec![0, 254], 2, None),
    ] {
        let identity = DocumentIdentity {
            document_key: key,
            version,
            chunk_ordinal: ordinal,
        };
        archive
            .attach_source_with_identity(row, &source(), ordinal, Some(&identity))
            .unwrap();
    }
    archive
}

mod archive;
