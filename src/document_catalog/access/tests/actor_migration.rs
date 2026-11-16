use super::*;

#[test]
fn actor_migration_checkpoint_preserves_both_namespaces_and_audits_each_phase() {
    let path = temp_path("actor-checkpoint");
    let root = path.parent().unwrap().to_path_buf();
    let first = actor_write(b"legacy-one", b"source-one", 1);
    let second = actor_write(b"legacy-two", b"source-two", 2);
    let (first_receipt, second_receipt) = {
        let catalog = DocumentCatalog::create(&path, "books").unwrap();
        (
            catalog.accept(&first).unwrap(),
            catalog.accept(&second).unwrap(),
        )
    };
    let first_raw = append_unknown_to_legacy_operation(&path, &first.operation_id);
    make_legacy_controlled(&path);

    let (admin, ingest) = permits();
    let catalog = AccessControlledCatalog::open(&path, &binding(), &admin).unwrap();
    catalog
        .assign_legacy_actor(&admin, &actor_assignment(&first_receipt, &first, "writer"))
        .unwrap();
    assert_eq!(
        raw_binary_rows_from(&catalog.inner.database, "actor_operations")[0].1,
        first_raw
    );

    let partial_copy = audited_copy(&catalog, &root, "partial");
    let partial = AccessControlledCatalog::open(&partial_copy, &binding(), &admin).unwrap();
    for request in [
        &first,
        &actor_write(b"new-during-migration", b"source-three", 3),
    ] {
        let error = partial.accept(&ingest, request).unwrap_err();
        assert_eq!(error.code(), Code::FailedPrecondition);
        assert!(error.message().contains("attribution"), "{error}");
    }
    drop(partial);
    let original_raw = raw_binary_rows_from(&catalog.inner.database, "operations");
    assert_eq!(original_raw, raw_binary_rows(&partial_copy, "operations"));
    let original_actor_raw = raw_binary_rows_from(&catalog.inner.database, "actor_operations");
    assert_eq!(
        original_actor_raw,
        raw_binary_rows(&partial_copy, "actor_operations")
    );

    catalog
        .assign_legacy_actor(
            &admin,
            &actor_assignment(&second_receipt, &second, "writer"),
        )
        .unwrap();
    let third = actor_write(b"actor-three", b"source-three", 3);
    let third_receipt = catalog.accept(&ingest, &third).unwrap();
    assert_eq!(third_receipt.accepted_sequence, 3);

    let complete_copy = audited_copy(&catalog, &root, "complete");
    drop(catalog);
    let complete = AccessControlledCatalog::open(&complete_copy, &binding(), &admin).unwrap();
    assert_replay(&complete, &ingest, &first, &first_receipt);
    assert_replay(&complete, &ingest, &second, &second_receipt);
    assert_replay(&complete, &ingest, &third, &third_receipt);
    drop(complete);
    assert_eq!(
        raw_binary_rows(&path, "operations"),
        raw_binary_rows(&complete_copy, "operations")
    );
    assert_eq!(
        raw_binary_rows(&path, "actor_operations"),
        raw_binary_rows(&complete_copy, "actor_operations")
    );

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn actor_namespace_refuses_new_acceptance_during_incomplete_attribution() {
    let (path, catalog) = two_actor_operations("actor-migration-watermark");
    let tx = catalog.inner.database.begin_write().unwrap();
    {
        let mut actor = tx.open_table(actors::OPERATIONS).unwrap();
        let (key, value) = {
            let (key, value) = actor.iter().unwrap().next().unwrap().unwrap();
            (key.value().to_vec(), value.value().to_vec())
        };
        let decoded: crate::pb::storage::ActorOperationKey = decode(&key).unwrap();
        let operation: DocumentOperation = decode(&value).unwrap();
        assert_eq!(operation.receipt.unwrap().accepted_sequence, 1);
        actor.remove(key.as_slice()).unwrap();
        tx.open_table(OPERATIONS)
            .unwrap()
            .insert(decoded.operation_id.as_slice(), value.as_slice())
            .unwrap();
        let mut meta = tx.open_table(META).unwrap();
        let mut header: DocumentCatalogHeader =
            decode(meta.get("header").unwrap().unwrap().value()).unwrap();
        // Table counts still agree: one unassigned legacy record, one actor
        // record, two accepted versions. The impossible state is accepting
        // sequence two before attribution of sequence one finished.
        header.actor_namespace = Some(actors::namespace(1));
        meta.insert("header", header.encode_to_vec().as_slice())
            .unwrap();
    }
    tx.commit().unwrap();
    let error = catalog
        .inner
        .capture_checkpoint(1 << 20)
        .err()
        .expect("impossible migration must refuse");
    assert_eq!(error.code(), Code::DataLoss);
    assert!(error.message().contains("actor namespace"), "{error}");
    drop(catalog);
    std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
}
