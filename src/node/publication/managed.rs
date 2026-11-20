//! Managed publication: an activated source's accepted history projected
//! onto its target indexes in sequence order. Every target passes each
//! sequence once: an ACCEPTED version is staged and published, a FENCED
//! one is skipped without staging rows, and an UNCONFIRMED one stalls
//! every target at its sequence until its outcome is settled.
use super::*;
use crate::document_catalog::ActiveManagedCatalog;
use crate::pb::storage::{DocumentVersion, WriteOutcome};
use crate::pb::{
    DocumentPublicationState, DocumentPublicationStatus, DocumentPublicationTarget,
    StageDocumentProjectionRequest,
};
use crate::source_authority::SourceAdmission;

/// One target index of a managed source: the node that serves it, its
/// logical index key, and its staging contract. The contract's projection
/// names no version; the publisher fills in history, key and version.
#[derive(Clone)]
pub struct ManagedTarget {
    pub node: Arc<NodeServiceImpl>,
    pub index_key: Vec<u8>,
    pub stage: StageDocumentProjectionRequest,
}

/// The next version a publication step projects: the earliest sequence
/// any target has not committed, read with its settled outcome.
pub struct NextPublication {
    source: Arc<DocumentCatalog>,
    version: DocumentVersion,
}

/// The next version to project onto `targets`, under current Admin; `None`
/// when every target has committed the whole accepted history.
pub fn next_publication_blocking(
    catalog: &ActiveManagedCatalog,
    admission: &SourceAdmission<'_>,
    targets: &[ManagedTarget],
) -> Result<Option<NextPublication>, Status> {
    let source = catalog.publication_source(admission)?;
    let next = next_sequence(&source, targets)?;
    Ok(catalog
        .settled_version(admission, next)?
        .map(|version| NextPublication { source, version }))
}

fn next_sequence(source: &DocumentCatalog, targets: &[ManagedTarget]) -> Result<u64, Status> {
    let mut committed = u64::MAX;
    for target in targets {
        committed = committed.min(source.committed_index_sequence(&target.index_key)?);
    }
    if targets.is_empty() {
        return Err(Status::invalid_argument(
            "managed publication requires at least one target",
        ));
    }
    Ok(committed + 1)
}

/// Project `next` onto every target that has not committed its sequence,
/// and report where the version stands afterwards. An unconfirmed version
/// touches no target.
pub async fn publish_next(
    next: NextPublication,
    targets: &[ManagedTarget],
) -> Result<DocumentPublicationStatus, Status> {
    let NextPublication { source, version } = next;
    let outcome = WriteOutcome::try_from(version.outcome)
        .map_err(|_| Status::data_loss("accepted version carries an unknown write outcome"))?;
    if outcome != WriteOutcome::Unconfirmed {
        for target in targets {
            if source.committed_index_sequence(&target.index_key)? >= version.accepted_sequence {
                continue;
            }
            project(&source, &version, outcome, target).await?;
        }
    }
    status_of(&source, &version, targets)
}

/// Reconcile a target's staged intent before another publication step.
async fn recover_staged(
    source: &Arc<DocumentCatalog>,
    version: &DocumentVersion,
    target: &ManagedTarget,
) -> Result<(), Status> {
    let status = source.index_publication_target(&target.index_key, version.accepted_sequence)?;
    if status.state != crate::pb::DocumentPublicationTargetState::Staged as i32 {
        return Ok(());
    }
    let node = Arc::clone(&target.node);
    let catalog = Arc::clone(source);
    let key = target.index_key.clone();
    tokio::task::spawn_blocking(move || node.recover_document_projection_blocking(&catalog, &key))
        .await
        .map_err(|_| Status::internal("managed recovery worker failed"))??;
    Ok(())
}

/// Publish or skip `version` on one target.
async fn project(
    source: &Arc<DocumentCatalog>,
    version: &DocumentVersion,
    outcome: WriteOutcome,
    target: &ManagedTarget,
) -> Result<(), Status> {
    recover_staged(source, version, target).await?;
    if source.committed_index_sequence(&target.index_key)? >= version.accepted_sequence {
        return Ok(());
    }
    let node = Arc::clone(&target.node);
    let catalog = Arc::clone(source);
    let key = target.index_key.clone();
    let staged = match outcome {
        WriteOutcome::Fenced => None,
        _ => Some(
            target
                .node
                .stage_document_projection(Arc::clone(source), staging(source, version, target)?)
                .await?,
        ),
    };
    let version = version.clone();
    tokio::task::spawn_blocking(move || match staged {
        Some(candidate) => node.publish_document_projection_blocking(&catalog, &key, &candidate),
        None => node.skip_fenced_projection_blocking(&catalog, &key, &version),
    })
    .await
    .map_err(|_| Status::internal("managed publication worker failed"))??;
    Ok(())
}

/// The target's staging contract for `version`. A deletion declares no
/// mapping, so its mapping fields are cleared.
fn staging(
    source: &DocumentCatalog,
    version: &DocumentVersion,
    target: &ManagedTarget,
) -> Result<StageDocumentProjectionRequest, Status> {
    let mut request = target.stage.clone();
    let projection = request.projection.get_or_insert_with(Default::default);
    projection.history_id = source.index_owner(&target.index_key)?.history_id;
    projection.document_key = version.document_key.clone();
    projection.version = version.version;
    if version.deleted {
        projection.expected_plan_fingerprint.clear();
        projection.body_path.clear();
        projection.index_definition = None;
        request.field_analysis.clear();
        request.materialize = None;
    }
    Ok(request)
}

/// Where the version at `sequence` stands across `targets`, under current
/// Admin or Ingest; `None` past the accepted history.
pub fn publication_status_blocking(
    catalog: &ActiveManagedCatalog,
    admission: &SourceAdmission<'_>,
    sequence: u64,
    targets: &[ManagedTarget],
) -> Result<Option<DocumentPublicationStatus>, Status> {
    let source = catalog.publication_status_source(admission)?;
    catalog
        .settled_publication_status(admission, sequence)?
        .map(|version| status_of(&source, &version, targets))
        .transpose()
}

fn status_of(
    source: &DocumentCatalog,
    version: &DocumentVersion,
    targets: &[ManagedTarget],
) -> Result<DocumentPublicationStatus, Status> {
    let state = match WriteOutcome::try_from(version.outcome) {
        Ok(WriteOutcome::Accepted) => DocumentPublicationState::AcceptedFinal,
        Ok(WriteOutcome::Unconfirmed) => DocumentPublicationState::Unresolved,
        Ok(WriteOutcome::Fenced) => DocumentPublicationState::Fenced,
        Err(_) => return Err(Status::data_loss("unknown write outcome")),
    };
    let Some(first) = targets.first() else {
        return Err(Status::invalid_argument(
            "publication status requires at least one target",
        ));
    };
    let history_id = source.index_owner(&first.index_key)?.history_id;
    let mut observed = Vec::with_capacity(targets.len());
    for target in targets {
        observed.push(target_status(source, version, target)?);
    }
    Ok(DocumentPublicationStatus {
        history_id,
        document_key: version.document_key.clone(),
        version: version.version,
        accepted_sequence: version.accepted_sequence,
        state: state as i32,
        fenced_at_revision: version.fenced_at_revision,
        targets: observed,
    })
}

/// One target's journal state, bound to its node's current read version.
fn target_status(
    source: &DocumentCatalog,
    version: &DocumentVersion,
    target: &ManagedTarget,
) -> Result<DocumentPublicationTarget, Status> {
    let mut status =
        source.index_publication_target(&target.index_key, version.accepted_sequence)?;
    let guard = read_shard(&target.node.state);
    status.stats_epoch = guard.stats_epoch;
    status.stats_incarnation = guard.stats_incarnation.bytes()?.to_vec();
    Ok(status)
}
