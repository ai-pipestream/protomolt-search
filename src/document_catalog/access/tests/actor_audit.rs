use super::*;

#[test]
fn source_history_audit_refuses_a_noncanonical_actor_operation_key() {
    let (path, catalog) = two_actor_operations("actor-key-canonical");
    {
        let tx = catalog.inner.database.begin_write().unwrap();
        {
            let mut operations = tx.open_table(actors::OPERATIONS).unwrap();
            let (mut noncanonical, value) = {
                let (key, value) = operations.iter().unwrap().next().unwrap().unwrap();
                (key.value().to_vec(), value.value().to_vec())
            };
            operations.remove(noncanonical.as_slice()).unwrap();
            // Unknown field 99, varint 1: same decoded ActorOperationKey meaning,
            // but not its unique canonical encoding.
            noncanonical.extend_from_slice(&[0x98, 0x06, 0x01]);
            operations
                .insert(noncanonical.as_slice(), value.as_slice())
                .unwrap();
        }
        tx.commit().unwrap();
    }

    let checkpoint = catalog.inner.capture_checkpoint(1 << 20).unwrap();
    let error = checkpoint
        .verify_source_history(
            &path.parent().unwrap().join("noncanonical-audit.redb"),
            1 << 20,
            64 << 20,
        )
        .unwrap_err();
    assert_eq!(error.code(), Code::DataLoss);
    assert!(error.message().contains("actor operation key"), "{error}");
    drop(checkpoint);
    drop(catalog);
    std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
}

#[test]
fn source_history_audit_refuses_two_actor_keys_claiming_one_sequence() {
    let (path, catalog) = two_actor_operations("actor-sequence-bijection");
    {
        let tx = catalog.inner.database.begin_write().unwrap();
        {
            let mut operations = tx.open_table(actors::OPERATIONS).unwrap();
            let rows: Vec<_> = operations
                .iter()
                .unwrap()
                .map(|entry| {
                    let (key, value) = entry.unwrap();
                    (key.value().to_vec(), value.value().to_vec())
                })
                .collect();
            assert_eq!(rows.len(), 2);
            operations
                .insert(rows[1].0.as_slice(), rows[0].1.as_slice())
                .unwrap();
        }
        tx.commit().unwrap();
    }

    let checkpoint = catalog.inner.capture_checkpoint(1 << 20).unwrap();
    let error = checkpoint
        .verify_source_history(
            &path.parent().unwrap().join("duplicate-sequence-audit.redb"),
            1 << 20,
            64 << 20,
        )
        .unwrap_err();
    assert_eq!(error.code(), Code::DataLoss);
    assert!(
        error
            .message()
            .contains("multiple operations claim one accepted sequence"),
        "{error}"
    );
    drop(checkpoint);
    drop(catalog);
    std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
}
