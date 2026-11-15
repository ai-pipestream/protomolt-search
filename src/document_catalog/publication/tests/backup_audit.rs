use super::backup::compact;
use super::*;
use crate::pb::storage::{MaintenanceCursor, MaintenanceIntent};
use tonic::Code;

#[tokio::test]
async fn backup_rejects_a_hole_in_older_accepted_history() {
    let f = Fixture::new();
    let _ = f.stage(KEY, 1, Some(0)).await;
    let _ = f.stage(KEY, 2, Some(0)).await;
    let key = DocumentVersionKey {
        document_key: KEY.to_vec(),
        version: 1,
    }
    .encode_to_vec();
    let tx = f.source.database.begin_write().unwrap();
    tx.open_table(VERSIONS)
        .unwrap()
        .remove(key.as_slice())
        .unwrap();
    tx.commit().unwrap();
    // The latest head still exists and there are no index tips to expose the hole.
    assert!(f.source.get(KEY, None).unwrap().is_some());
    let output = f.root.join("incomplete-history");
    let result = f
        .source
        .capture_backup(&[], &backup_limits())
        .unwrap()
        .write_to(&output);
    assert_eq!(result.unwrap_err().code(), Code::DataLoss);
    assert!(!output.exists());
}

#[tokio::test]
async fn backup_rejects_missing_older_publication_decisions() {
    let f = Fixture::new();
    for version in 1..=3 {
        f.publish_through_node(KEY, version, Some(2)).await;
    }
    let key = decision_key(INDEX, 1);
    let tx = f.source.database.begin_write().unwrap();
    tx.open_table(DECISIONS)
        .unwrap()
        .remove(key.as_slice())
        .unwrap();
    tx.commit().unwrap();
    let catalog = f.node.document_backup_catalog(&f.source, INDEX).unwrap();
    let output = f.root.join("missing-decision");
    let captured = f
        .source
        .capture_backup(&[(INDEX, &catalog)], &backup_limits())
        .unwrap();
    assert_eq!(
        captured.write_to(&output).unwrap_err().code(),
        Code::DataLoss
    );
    assert!(!output.exists());
}

#[tokio::test]
async fn backup_checks_historical_source_links_even_with_new_record_hashes() {
    for damage in ["before", "after", "source", "orphan"] {
        let f = Fixture::new();
        for version in 1..=3 {
            f.publish_through_node(KEY, version, Some(2)).await;
        }
        let tx = f.source.database.begin_write().unwrap();
        {
            let mut table = tx.open_table(DECISIONS).unwrap();
            let key = decision_key(INDEX, 2);
            let mut intent: ProjectionIntent =
                decode(table.get(key.as_slice()).unwrap().unwrap().value()).unwrap();
            match damage {
                "before" => intent.before_manifest_sha256 = vec![44; 32],
                "after" => intent.after_manifest_sha256 = vec![45; 32],
                "source" => intent.source = Some(f.source.get(KEY, Some(1)).unwrap().unwrap().0),
                "orphan" => intent.index_key = b"unregistered-index".to_vec(),
                _ => unreachable!(),
            }
            intent.intent_id = intent_hash(&intent);
            let key = decision_key(&intent.index_key, 2);
            table
                .insert(key.as_slice(), intent.encode_to_vec().as_slice())
                .unwrap();
        }
        tx.commit().unwrap();
        let catalog = f.node.document_backup_catalog(&f.source, INDEX).unwrap();
        let captured = f
            .source
            .capture_backup(&[(INDEX, &catalog)], &backup_limits())
            .unwrap();
        let output = f.root.join("broken-source-chain");
        let error = captured.write_to(&output).unwrap_err();
        assert_eq!(error.code(), Code::DataLoss, "{damage}: {error}");
        assert!(
            error.message().contains("journal audit"),
            "{damage}: {error}"
        );
        assert!(!output.exists());
    }
}

/// Damage the oldest maintenance decision in the journal: remove it,
/// append unknown bytes to it, or rewrite one of its links under a
/// recomputed intent id.
fn damage_first_maintenance(f: &Fixture, damage: &str) {
    let tx = f.source.database.begin_write().unwrap();
    {
        let mut table = tx.open_table(MAINTENANCE).unwrap();
        assert_eq!(table.len().unwrap(), 4);
        let first: MaintenanceIntent = table
            .iter()
            .unwrap()
            .map(|entry| decode::<MaintenanceIntent>(entry.unwrap().1.value()).unwrap())
            .min_by_key(|intent| intent.after_epoch)
            .unwrap();
        let key = maintenance_key(INDEX, first.after_epoch);
        if damage == "missing" {
            table.remove(key.as_slice()).unwrap();
        } else if damage == "encoding" {
            let mut bytes = first.encode_to_vec();
            bytes.extend_from_slice(&[0xa0, 6, 1]);
            table.insert(key.as_slice(), bytes.as_slice()).unwrap();
        } else {
            let mut intent = first;
            match damage {
                "previous" => {
                    intent.previous = Some(MaintenanceCursor {
                        after_epoch: intent.before_epoch,
                        intent_id: vec![46; 32],
                    })
                }
                "anchor" => intent.source_intent_id = vec![47; 32],
                "hash" => intent.before_manifest_sha256 = vec![48; 32],
                "orphan" => {
                    intent.owner.as_mut().unwrap().index_key = b"unregistered-index".to_vec();
                    intent.preservation.as_mut().unwrap().owner = intent.owner.clone();
                }
                _ => unreachable!(),
            }
            intent.intent_id.clear();
            intent.intent_id = sha256::digest(&intent.encode_to_vec()).to_vec();
            let key = maintenance_key(
                &intent.owner.as_ref().unwrap().index_key,
                intent.after_epoch,
            );
            table
                .insert(key.as_slice(), intent.encode_to_vec().as_slice())
                .unwrap();
        }
    }
    tx.commit().unwrap();
}

#[tokio::test]
async fn backup_walks_maintenance_older_than_the_current_tip_and_its_predecessor() {
    for damage in [
        "missing", "previous", "anchor", "hash", "encoding", "orphan",
    ] {
        let f = Fixture::new();
        f.publish_through_node(KEY, 1, Some(2)).await;
        compact(&f).await;
        f.publish_through_node(KEY, 2, Some(3)).await;
        for _ in 0..3 {
            compact(&f).await;
        }
        f.publish_through_node(KEY, 3, Some(1)).await;
        let _ = f.stage(KEY, 4, Some(0)).await;
        // A coherent interleaved chain and unpublished backlog must pass first.
        let catalog = f.node.document_backup_catalog(&f.source, INDEX).unwrap();
        let clean = f
            .source
            .capture_backup(&[(INDEX, &catalog)], &backup_limits())
            .unwrap()
            .write_to(&f.root.join("clean-chain"))
            .unwrap();
        assert_eq!(
            clean
                .source
                .as_ref()
                .unwrap()
                .header
                .as_ref()
                .unwrap()
                .accepted_sequence,
            4
        );
        assert_eq!(
            clean.source.as_ref().unwrap().indexes[0].committed_sequence,
            3
        );
        damage_first_maintenance(&f, damage);
        // The tip validator sees only the most recent maintenance pair; the
        // damaged first decision sits before both and before later source writes.
        let captured = f
            .source
            .capture_backup(&[(INDEX, &catalog)], &backup_limits())
            .unwrap();
        let output = f.root.join("broken-maintenance-chain");
        let error = captured.write_to(&output).unwrap_err();
        assert_eq!(error.code(), Code::DataLoss, "{damage}: {error}");
        assert!(!output.exists());
    }
}
