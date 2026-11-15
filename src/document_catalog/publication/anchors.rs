//! The journal anchors a source checkpoint carries: every index state,
//! each joined to the accepted version its tip names.
use super::*;

/// Validate all physical journal anchors in the caller's pinned source read.
/// Do not begin a second transaction: accepted backlog and every index must
/// describe the same point in source history.
pub(in crate::document_catalog) fn checkpoint_states(
    tx: &redb::ReadTransaction,
    header: &DocumentCatalogHeader,
    metadata_limit: usize,
) -> Result<(Vec<ProjectionJournalState>, Vec<checkpoint::BinaryTable>), Status> {
    let meta = tx.open_table(META).map_err(storage)?;
    let names: Vec<_> = tx
        .list_tables()
        .map_err(storage)?
        .map(|t| t.name().to_string())
        .collect();
    let journal = meta.get(JOURNAL).map_err(storage)?;
    if !journal_header(
        journal.as_ref().map(|v| v.value()),
        &names,
        &header.history_id,
    )? {
        return Ok((Vec::new(), Vec::new()));
    }
    let states = tx.open_table(STATES).map_err(storage)?;
    let decisions = tx.open_table(DECISIONS).map_err(storage)?;
    let maintenance = maintenance_table!(tx);
    let mut tables = vec![STATES, DECISIONS];
    if maintenance.is_some() {
        tables.push(MAINTENANCE);
    }
    let mut indexes = Vec::new();
    let mut used = header.encoded_len();
    for row in states.iter().map_err(storage)? {
        let (key, value) = row.map_err(storage)?;
        used = used
            .checked_add(value.value().len())
            .and_then(|v| v.checked_add(64))
            .ok_or_else(|| Status::resource_exhausted("checkpoint metadata size overflow"))?;
        if used > metadata_limit {
            return Err(Status::resource_exhausted(
                "checkpoint metadata budget exceeded",
            ));
        }
        let state: ProjectionJournalState = decode(value.value())?;
        index_key(key.value()).map_err(|e| Status::data_loss(e.message().to_string()))?;
        validate_state(&state, key.value(), &header.history_id)?;
        if state.pending.is_some() || state.pending_maintenance.is_some() {
            return Err(Status::failed_precondition(
                "resolve pending source/index decisions before checkpoint capture",
            ));
        }
        if state.committed_sequence > header.accepted_sequence {
            return Err(Status::data_loss(
                "checkpoint index is ahead of accepted source history",
            ));
        }
        if let Some(intent) =
            validate_tip(&state, &decisions, maintenance.as_ref(), &header.collection)?
        {
            verify_anchor(tx, &intent, metadata_limit)?;
        }
        indexes.push(state);
    }
    Ok((indexes, tables))
}

/// A journal tip's source must be the accepted version at its sequence.
fn verify_anchor(
    tx: &redb::ReadTransaction,
    intent: &ProjectionIntent,
    metadata_limit: usize,
) -> Result<(), Status> {
    let source = intent.source.as_ref().expect("validated source intent");
    let key = DocumentVersionKey {
        document_key: source.document_key.clone(),
        version: source.version,
    }
    .encode_to_vec();
    let versions = tx.open_table(VERSIONS).map_err(storage)?;
    let version = versions
        .get(key.as_slice())
        .map_err(storage)?
        .ok_or_else(|| {
            Status::data_loss("checkpoint journal anchor has no accepted source version")
        })?;
    if version.value().len() > metadata_limit {
        return Err(Status::resource_exhausted(
            "checkpoint source anchor exceeds metadata budget",
        ));
    }
    let accepted: DocumentVersion = decode(version.value())?;
    let changes = tx.open_table(CHANGES).map_err(storage)?;
    if accepted != *source
        || changes
            .get(source.accepted_sequence)
            .map_err(storage)?
            .is_none_or(|value| value.value() != key)
    {
        return Err(Status::data_loss(
            "checkpoint journal anchor differs from accepted source history",
        ));
    }
    Ok(())
}
