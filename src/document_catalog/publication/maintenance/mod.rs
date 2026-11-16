//! Immutable physical maintenance decisions joined to accepted source history.
use super::*;
use crate::pb::storage::{MaintenanceCursor, MaintenanceDecisionKey, MaintenanceIntent};

mod decision;
mod prepare;

pub(super) const MAINTENANCE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("index_maintenance_decisions");

#[derive(Debug, Clone, PartialEq)]
pub enum MaintenanceRecovery {
    Committed(MaintenanceIntent),
    Aborted(MaintenanceIntent),
    Idle,
}

pub(super) fn maintenance_key(key: &[u8], epoch: u64) -> Vec<u8> {
    MaintenanceDecisionKey {
        index_key: key.to_vec(),
        after_epoch: epoch,
    }
    .encode_to_vec()
}
fn maintenance_hash(intent: &MaintenanceIntent) -> Vec<u8> {
    let mut value = intent.clone();
    value.intent_id.clear();
    turbo_sha256::sha256(&value.encode_to_vec()).0.to_vec()
}
pub(super) fn cursor(intent: &MaintenanceIntent) -> MaintenanceCursor {
    MaintenanceCursor {
        after_epoch: intent.after_epoch,
        intent_id: intent.intent_id.clone(),
    }
}
pub(super) fn validate_maintenance(
    intent: &MaintenanceIntent,
    key: &[u8],
    history: &[u8],
    collection: &str,
) -> Result<(), Status> {
    let fail = || {
        Status::data_loss("invalid maintenance identity, preservation proof or manifest transition")
    };
    let owner = intent.owner.as_ref().ok_or_else(fail)?;
    let proof = intent.preservation.as_ref().ok_or_else(fail)?;
    if intent.format_version != 1
        || owner.format_version != 1
        || owner.index_key != key
        || owner.history_id != history
        || owner.collection != collection
        || intent.accepted_sequence == 0
        || intent.source_intent_id.len() != 32
        || intent.before_manifest_sha256.len() != 32
        || intent.after_manifest_sha256.len() != 32
        || intent.before_manifest_sha256 == intent.after_manifest_sha256
        || intent.before_epoch.checked_add(1) != Some(intent.after_epoch)
        || intent.previous.as_ref().is_some_and(|p| {
            p.after_epoch == 0 || p.after_epoch > intent.before_epoch || p.intent_id.len() != 32
        })
        // Format 1 certified stored content and exact FP32 rows; format 2
        // adds the provider's own encoded rows.
        // Format 3 additionally covers lattice lengths and SPLADE scales/impacts.
        // Older journals remain readable with their original coverage.
        || !matches!(proof.format_version, 1..=3)
        || proof.owner.as_ref() != Some(owner)
        || proof.schema_sha256.len() != 32
        || proof.identity_content_sha256.len() != 32
        || intent.intent_id != maintenance_hash(intent)
    {
        return Err(fail());
    }
    Ok(())
}
fn record<T: Message + Default>(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    key: &[u8],
) -> Result<Option<T>, Status> {
    table
        .get(key)
        .map_err(storage)?
        .map(|v| decode(v.value()))
        .transpose()
}
fn maintenance_record(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    state: &ProjectionJournalState,
    reference: &MaintenanceCursor,
    collection: &str,
) -> Result<MaintenanceIntent, Status> {
    let key = maintenance_key(&state.index_key, reference.after_epoch);
    let bytes = table
        .get(key.as_slice())
        .map_err(storage)?
        .ok_or_else(|| Status::data_loss("maintenance committed decision missing"))?;
    let intent: MaintenanceIntent = decode(bytes.value())?;
    validate_maintenance(&intent, &state.index_key, &state.history_id, collection)?;
    if cursor(&intent) != *reference || intent.encode_to_vec() != bytes.value() {
        return Err(Status::data_loss(
            "maintenance cursor differs from its canonical immutable decision",
        ));
    }
    Ok(intent)
}

/// Validate the active physical tip against immutable source and maintenance
/// records. The most recent source decision never changes during maintenance.
pub(super) fn validate_tip(
    state: &ProjectionJournalState,
    decisions: &impl ReadableTable<&'static [u8], &'static [u8]>,
    maintenance: Option<&impl ReadableTable<&'static [u8], &'static [u8]>>,
    collection: &str,
) -> Result<Option<ProjectionIntent>, Status> {
    let source: Option<ProjectionIntent> = record(
        decisions,
        &decision_key(&state.index_key, state.committed_sequence.max(1)),
    )?;
    if state.committed_sequence == 0 {
        if source.is_some()
            || state.maintenance_tip.is_some()
            || state.pending_maintenance.is_some()
        {
            return Err(Status::data_loss(
                "projection journal lost its committed cursor",
            ));
        }
        return Ok(None);
    }
    let source =
        source.ok_or_else(|| Status::data_loss("projection committed decision missing"))?;
    validate_intent(&source, &state.index_key, &state.history_id)?;
    if source.source.as_ref().map(|s| s.accepted_sequence) != Some(state.committed_sequence) {
        return Err(Status::data_loss(
            "projection cursor differs from its committed decision",
        ));
    }
    let mut expected_hash = &source.after_manifest_sha256;
    let mut expected_epoch = source.after_epoch;
    let latest = if let Some(reference) = &state.maintenance_tip {
        let table = maintenance
            .ok_or_else(|| Status::data_loss("maintenance cursor has no decision table"))?;
        Some(committed_maintenance(
            state, decisions, table, reference, &source, collection,
        )?)
    } else {
        None
    };
    if let Some(latest) = &latest {
        if latest.accepted_sequence == state.committed_sequence {
            expected_hash = &latest.after_manifest_sha256;
            expected_epoch = latest.after_epoch;
        }
    }
    if &state.committed_manifest_sha256 != expected_hash {
        return Err(Status::data_loss(
            "projection cursor differs from its committed physical decision",
        ));
    }
    if state
        .pending
        .as_ref()
        .is_some_and(|p| p.before_epoch != expected_epoch)
    {
        return Err(Status::data_loss(
            "pending source does not follow the committed catalog epoch",
        ));
    }
    if let Some(pending) = &state.pending_maintenance {
        if maintenance.is_none() {
            return Err(Status::data_loss(
                "pending maintenance has no decision table",
            ));
        }
        validate_pending_maintenance(state, pending, &source, expected_epoch, collection)?;
    }
    Ok(Some(source))
}

/// The committed maintenance decision the state's tip names, checked
/// against its source anchor, its predecessor and the current source
/// decision.
fn committed_maintenance(
    state: &ProjectionJournalState,
    decisions: &impl ReadableTable<&'static [u8], &'static [u8]>,
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    reference: &MaintenanceCursor,
    source: &ProjectionIntent,
    collection: &str,
) -> Result<MaintenanceIntent, Status> {
    let latest = maintenance_record(table, state, reference, collection)?;
    let anchor: ProjectionIntent = record(
        decisions,
        &decision_key(&state.index_key, latest.accepted_sequence),
    )?
    .ok_or_else(|| Status::data_loss("maintenance source anchor is missing"))?;
    validate_intent(&anchor, &state.index_key, &state.history_id)?;
    if anchor.source.as_ref().map(|s| s.accepted_sequence) != Some(latest.accepted_sequence)
        || latest.source_intent_id != anchor.intent_id
        || latest.accepted_sequence > state.committed_sequence
    {
        return Err(Status::data_loss(
            "maintenance differs from its accepted source anchor",
        ));
    }
    let previous = latest
        .previous
        .as_ref()
        .map(|p| maintenance_record(table, state, p, collection))
        .transpose()?;
    if previous
        .as_ref()
        .is_some_and(|p| p.accepted_sequence > latest.accepted_sequence)
    {
        return Err(Status::data_loss("maintenance source order regressed"));
    }
    let (before_hash, before_epoch) = match &previous {
        Some(p) if p.accepted_sequence == latest.accepted_sequence => {
            (&p.after_manifest_sha256, p.after_epoch)
        }
        Some(p) => {
            if anchor.before_epoch < p.after_epoch {
                return Err(Status::data_loss(
                    "source anchor precedes previous maintenance",
                ));
            }
            (&anchor.after_manifest_sha256, anchor.after_epoch)
        }
        None => (&anchor.after_manifest_sha256, anchor.after_epoch),
    };
    if &latest.before_manifest_sha256 != before_hash || latest.before_epoch != before_epoch {
        return Err(Status::data_loss(
            "maintenance does not compose with its preceding physical decision",
        ));
    }
    if latest.accepted_sequence < state.committed_sequence
        && source.before_epoch < latest.after_epoch
    {
        return Err(Status::data_loss(
            "current source decision precedes maintenance",
        ));
    }
    Ok(latest)
}

/// A pending maintenance intent must extend the committed source and
/// physical tips exactly.
fn validate_pending_maintenance(
    state: &ProjectionJournalState,
    pending: &MaintenanceIntent,
    source: &ProjectionIntent,
    expected_epoch: u64,
    collection: &str,
) -> Result<(), Status> {
    validate_maintenance(pending, &state.index_key, &state.history_id, collection)?;
    if pending.accepted_sequence != state.committed_sequence
        || pending.source_intent_id != source.intent_id
        || pending.before_manifest_sha256 != state.committed_manifest_sha256
        || pending.before_epoch != expected_epoch
        || pending.previous != state.maintenance_tip
    {
        return Err(Status::data_loss(
            "pending maintenance differs from the committed source and physical tips",
        ));
    }
    Ok(())
}

/// Constructed only by the local row/metadata comparison. It is deliberately
/// not a protobuf credential that another caller can manufacture or deserialize.
pub(crate) struct VerifiedMaintenance<'a> {
    owner: crate::pb::storage::SourceIndexOwner,
    before: &'a OpenedSegmentSet,
    after: &'a OpenedSegmentSet,
    preservation: crate::pb::storage::SourceRewriteCertificate,
}
