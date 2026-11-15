use super::retirement::{request, seal_request};
use super::*;
use crate::pb::storage::{SourceRestoreRequest, SourceRetirementIntent};
use tonic::Code;

/// A copy of a retiring source carries the intent and refuses a new write
/// by name.
fn assert_copy_is_retiring(bundle: &Path, intent: &SourceRetirementIntent) {
    let copied = DocumentCatalog::open(&bundle.join("sources.redb"), "books").unwrap();
    assert_eq!(copied.retirement_intent().unwrap(), Some(intent.clone()));
    let error = copied.accept(&fresh_delete(b"after-copy")).unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("retiring"), "{error}");
}

/// After the retirement's seal, a new backup and its verified restore both
/// carry the retired header: format 6, the intent, the seal, and the
/// accepted and committed sequences the retiring bundle had.
fn assert_retired_bundle_restores(
    f: &Fixture,
    catalog: &SegmentCatalog,
    intent: SourceRetirementIntent,
) {
    let seal = f.source.seal_history(&seal_request(&intent)).unwrap();
    let sealed_bundle = f.root.join("retired-bundle");
    let sealed_manifest = f
        .source
        .capture_backup(&[(INDEX, catalog)], &backup_limits())
        .unwrap()
        .write_to(&sealed_bundle)
        .unwrap();
    let sealed_header = sealed_manifest
        .source
        .as_ref()
        .unwrap()
        .header
        .as_ref()
        .unwrap();
    assert_eq!(sealed_header.format_version, 6);
    assert_eq!(sealed_header.retirement_intent, Some(intent.clone()));
    assert_eq!(sealed_header.history_seal, Some(seal.clone()));
    assert_eq!(sealed_header.accepted_sequence, 2);
    assert_eq!(
        sealed_manifest.source.as_ref().unwrap().indexes[0].committed_sequence,
        1
    );
    let sealed_destination = f.root.join("retired-restore");
    let sealed_verified = DocumentCatalog::stage_backup_restore(
        &sealed_bundle,
        &sealed_destination,
        &SourceRestoreRequest {
            expected_manifest_sha256: sealed_manifest.manifest_sha256.clone(),
            collection: "books".into(),
            history_id: intent.history_id.clone(),
            limits: Some(backup_limits()),
        },
    )
    .unwrap();
    let staged_header = sealed_verified
        .manifest()
        .source
        .as_ref()
        .unwrap()
        .header
        .as_ref()
        .unwrap();
    assert_eq!(staged_header.format_version, 6);
    assert_eq!(staged_header.retirement_intent, Some(intent));
    assert_eq!(staged_header.history_seal, Some(seal));
    assert_eq!(staged_header.accepted_sequence, 2);
    assert_eq!(
        sealed_verified.manifest().source.as_ref().unwrap().indexes[0].committed_sequence,
        1
    );
}

#[tokio::test]
async fn retiring_backup_and_restore_preserve_intent_and_backlog() {
    let f = Fixture::new();
    f.publish_through_node(KEY, 1, Some(2)).await;
    let _ = f.stage(KEY, 2, Some(0)).await;
    let catalog = f.node.document_backup_catalog(&f.source, INDEX).unwrap();
    let intent = f
        .source
        .begin_retirement(&request(&f, b"transfer-backup"))
        .unwrap();
    let bundle = f.root.join("retiring-bundle");
    let manifest = f
        .source
        .capture_backup(&[(INDEX, &catalog)], &backup_limits())
        .unwrap()
        .write_to(&bundle)
        .unwrap();
    let header = manifest.source.as_ref().unwrap().header.as_ref().unwrap();
    assert_eq!(header.accepted_sequence, 2);
    assert_eq!(header.retirement_intent, Some(intent.clone()));
    assert_eq!(
        manifest.source.as_ref().unwrap().indexes[0].committed_sequence,
        1
    );
    let destination = f.root.join("retiring-restore");
    let verified = DocumentCatalog::stage_backup_restore(
        &bundle,
        &destination,
        &SourceRestoreRequest {
            expected_manifest_sha256: manifest.manifest_sha256.clone(),
            collection: "books".into(),
            history_id: intent.history_id.clone(),
            limits: Some(backup_limits()),
        },
    )
    .unwrap();
    assert_eq!(
        verified
            .manifest()
            .source
            .as_ref()
            .unwrap()
            .header
            .as_ref()
            .unwrap()
            .retirement_intent,
        Some(intent.clone())
    );
    assert_copy_is_retiring(&bundle, &intent);
    assert_retired_bundle_restores(&f, &catalog, intent);
}
