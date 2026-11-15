use super::*;

/// Flip a bit in the first descriptor or source blob, or remove the first
/// source blob.
fn damage_blob(tx: &redb::WriteTransaction, source: &DocumentCatalog, damage: &str) {
    let definition = if damage == "descriptor" {
        DESCRIPTORS
    } else {
        SOURCES
    };
    let (key, mut value) = records(&source.database.begin_read().unwrap(), definition).remove(0);
    let mut table = tx.open_table(definition).unwrap();
    if damage == "source_missing" {
        table.remove(key.as_slice()).unwrap();
    } else {
        value[0] ^= 1;
        table.insert(key.as_slice(), value.as_slice()).unwrap();
    }
}

/// Point the document's head at its first version.
fn damage_head(tx: &redb::WriteTransaction) {
    let key = DocumentVersionKey {
        document_key: request(1).document_key,
        version: 1,
    }
    .encode_to_vec();
    let versions = tx.open_table(VERSIONS).unwrap();
    let old = versions.get(key.as_slice()).unwrap().unwrap();
    tx.open_table(HEADS)
        .unwrap()
        .insert(request(1).document_key.as_slice(), old.value())
        .unwrap();
}

/// Remove the third operation's receipt, or rewrite it as a duplicate of
/// the first, as searchable, or without its history identity.
fn damage_receipt(tx: &redb::WriteTransaction, damage: &str) {
    let key = 3u64.to_be_bytes();
    let mut table = tx.open_table(OPERATIONS).unwrap();
    if damage == "receipt_missing" {
        table.remove(key.as_slice()).unwrap();
    } else {
        let bytes = table.get(key.as_slice()).unwrap().unwrap().value().to_vec();
        let mut operation: DocumentOperation = decode(&bytes).unwrap();
        match damage {
            "receipt_duplicate" => {
                let first: DocumentOperation = decode(
                    table
                        .get(1u64.to_be_bytes().as_slice())
                        .unwrap()
                        .unwrap()
                        .value(),
                )
                .unwrap();
                operation.receipt = first.receipt;
            }
            "receipt_flags" => operation.receipt.as_mut().unwrap().searchable = true,
            "receipt_history" => operation.receipt.as_mut().unwrap().history_id.clear(),
            _ => unreachable!(),
        }
        table
            .insert(key.as_slice(), operation.encode_to_vec().as_slice())
            .unwrap();
    }
}

/// Move the first change-order entry past the accepted sequence.
fn damage_sequence(tx: &redb::WriteTransaction) {
    let mut table = tx.open_table(CHANGES).unwrap();
    let key = table.remove(1).unwrap().unwrap().value().to_vec();
    table.insert(4, key.as_slice()).unwrap();
}

/// Renumber the third version as a fourth with no third before it.
fn damage_predecessor(tx: &redb::WriteTransaction) {
    let old = DocumentVersionKey {
        document_key: request(1).document_key,
        version: 3,
    }
    .encode_to_vec();
    let mut versions = tx.open_table(VERSIONS).unwrap();
    let bytes = versions
        .remove(old.as_slice())
        .unwrap()
        .unwrap()
        .value()
        .to_vec();
    let mut version: DocumentVersion = decode(&bytes).unwrap();
    version.version = 4;
    let key = DocumentVersionKey {
        document_key: version.document_key.clone(),
        version: 4,
    }
    .encode_to_vec();
    versions
        .insert(key.as_slice(), version.encode_to_vec().as_slice())
        .unwrap();
    tx.open_table(CHANGES)
        .unwrap()
        .insert(3, key.as_slice())
        .unwrap();
    tx.open_table(HEADS)
        .unwrap()
        .insert(
            version.document_key.as_slice(),
            version.encode_to_vec().as_slice(),
        )
        .unwrap();
}

/// Apply one named corruption to the source in a single transaction.
fn corrupt(source: &DocumentCatalog, damage: &str) {
    let tx = source.database.begin_write().unwrap();
    match damage {
        "descriptor" | "source" | "source_missing" => damage_blob(&tx, source, damage),
        "head" => damage_head(&tx),
        "receipt_duplicate" | "receipt_missing" | "receipt_flags" | "receipt_history" => {
            damage_receipt(&tx, damage)
        }
        "sequence" => damage_sequence(&tx),
        "predecessor" => damage_predecessor(&tx),
        _ => unreachable!(),
    }
    tx.commit().unwrap();
}

#[test]
fn source_history_audit_rejects_corruption_beyond_the_latest_head() {
    for damage in [
        "descriptor",
        "source",
        "source_missing",
        "head",
        "receipt_duplicate",
        "receipt_missing",
        "receipt_flags",
        "receipt_history",
        "sequence",
        "predecessor",
    ] {
        let dir = Directory::new();
        let source = create_source(&dir);
        for n in 1..=3 {
            source.accept(&request(n)).unwrap();
        }
        corrupt(&source, damage);
        let scratch = dir.0.join("audit");
        if damage == "receipt_missing" {
            let error = source.capture_checkpoint(1 << 20).err().unwrap();
            assert_eq!(error.code(), Code::DataLoss, "{damage}: {error}");
            assert!(
                error.message().contains("operation table counts"),
                "{error}"
            );
            assert!(!scratch.exists(), "{damage}");
            continue;
        }
        let checkpoint = source.capture_checkpoint(1 << 20).unwrap();
        let error = checkpoint
            .verify_source_history(&scratch, 1 << 20, 32 << 20)
            .unwrap_err();
        assert_eq!(error.code(), Code::DataLoss, "{damage}: {error}");
        assert!(!scratch.exists(), "{damage}");
    }
}

#[test]
fn source_history_audit_preserves_legacy_retry_bytes_and_checks_budgets() {
    let dir = Directory::new();
    let source = create_source(&dir);
    source.accept(&request(1)).unwrap();
    let tx = source.database.begin_write().unwrap();
    {
        let mut table = tx.open_table(OPERATIONS).unwrap();
        let bytes = table
            .get(1u64.to_be_bytes().as_slice())
            .unwrap()
            .unwrap()
            .value()
            .to_vec();
        let mut operation: DocumentOperation = decode(&bytes).unwrap();
        operation.receipt.as_mut().unwrap().history_id.clear();
        let mut bytes = operation.encode_to_vec();
        bytes.extend_from_slice(&[0xa0, 6, 0x81, 0]);
        table
            .insert(1u64.to_be_bytes().as_slice(), bytes.as_slice())
            .unwrap();
        let mut meta = tx.open_table(META).unwrap();
        let mut header: DocumentCatalogHeader =
            decode(meta.get("header").unwrap().unwrap().value()).unwrap();
        header.legacy_receipts_through_sequence = 1;
        meta.insert("header", header.encode_to_vec().as_slice())
            .unwrap();
    }
    tx.commit().unwrap();
    let checkpoint = source.capture_checkpoint(1 << 20).unwrap();
    let before = records(&checkpoint.read, OPERATIONS);
    let scratch = dir.0.join("audit");
    checkpoint
        .verify_source_history(&scratch, 1 << 20, 32 << 20)
        .unwrap();
    assert!(!scratch.exists());
    assert_eq!(records(&checkpoint.read, OPERATIONS), before);
    for (record_bytes, scratch_bytes) in [(1, 32 << 20), (1 << 20, 1)] {
        assert_eq!(
            checkpoint
                .verify_source_history(&scratch, record_bytes, scratch_bytes)
                .unwrap_err()
                .code(),
            Code::ResourceExhausted
        );
        assert!(!scratch.exists());
    }
    std::fs::write(&scratch, b"existing scratch belongs to caller").unwrap();
    assert!(checkpoint
        .verify_source_history(&scratch, 1 << 20, 32 << 20)
        .is_err());
    assert_eq!(
        std::fs::read(&scratch).unwrap(),
        b"existing scratch belongs to caller"
    );
}

#[test]
fn source_history_audit_detects_duplicate_receipts_across_scratch_batches() {
    let dir = Directory::new();
    let source = create_source(&dir);
    for n in 1..=1030 {
        source.accept(&request(n)).unwrap();
    }
    let checkpoint = source.capture_checkpoint(1 << 20).unwrap();
    checkpoint
        .verify_source_history(&dir.0.join("valid"), 1 << 20, 32 << 20)
        .unwrap();
    let tx = source.database.begin_write().unwrap();
    {
        let mut table = tx.open_table(OPERATIONS).unwrap();
        let bytes = table
            .get(1u64.to_be_bytes().as_slice())
            .unwrap()
            .unwrap()
            .value()
            .to_vec();
        table
            .insert(1030u64.to_be_bytes().as_slice(), bytes.as_slice())
            .unwrap();
    }
    tx.commit().unwrap();
    // The held view remains coherent; a new capture sees the damaged receipt.
    checkpoint
        .verify_source_history(&dir.0.join("held"), 1 << 20, 32 << 20)
        .unwrap();
    let fresh = source.capture_checkpoint(1 << 20).unwrap();
    let error = fresh
        .verify_source_history(&dir.0.join("bad"), 1 << 20, 32 << 20)
        .unwrap_err();
    assert!(error.message().contains("multiple operations"), "{error}");
    for name in ["valid", "held", "bad"] {
        assert!(!dir.0.join(name).exists());
    }
}
