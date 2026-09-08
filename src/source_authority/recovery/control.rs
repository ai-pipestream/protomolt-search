//! Audit of the format-2 control tables: every import decision, workflow,
//! chunk reference and applied row is recomputed against the headers. Nothing
//! is repaired; a disagreement refuses the open.
use super::super::import::{self, *};
use super::*;
use crate::pb::storage::control_import_command::Action as ImportAction;
use std::collections::BTreeMap;

fn decision_ok(
    decision: &ControlImportDecision,
    header: &SourceAuthorityHeader,
    policy: &AccessPolicy,
) -> Result<(), Status> {
    if decision.format_version != 1
        || decision.control_revision == 0
        || decision.control_revision > header.control_revision
        || decision.policy_revision == 0
        || decision.policy_revision > policy.revision
        || ![0, 3, 5, 6, 8, 9].contains(&decision.code)
    {
        return Err(corrupt(
            "import decision format, status or revision is invalid",
        ));
    }
    if decision.code != 0 && (decision.receipt.is_some() || decision.message.is_empty()) {
        return Err(corrupt(
            "rejected import decision carries a receipt or no reason",
        ));
    }
    Ok(())
}

/// Returns the accepted import decision count and the bytes held by every
/// control table (keys and values), for the header reconciliation.
pub(super) fn validate(
    tx: &redb::ReadTransaction,
    header: &SourceAuthorityHeader,
    control: &ControlStoreHeader,
    policy: &AccessPolicy,
    regular: &impl ReadableTable<&'static [u8], &'static [u8]>,
) -> Result<(u64, usize), Status> {
    let identity = header
        .identity
        .as_ref()
        .ok_or_else(|| missing("identity"))?;
    let limits = header.limits.as_ref().ok_or_else(|| missing("limits"))?;
    let mut payload = 0usize;
    let mut accepted = 0u64;

    // Import operations: the retained retry history, one record per command.
    let operations = tx.open_table(IMPORT_OPERATIONS).map_err(corrupt)?;
    let mut ops: BTreeMap<Vec<u8>, ControlImportOperation> = BTreeMap::new();
    for entry in operations.iter().map_err(storage)? {
        let (k, v) = entry.map_err(storage)?;
        let key: SourceAuthorityOperationKey = contract::decode(k.value())?;
        let value: ControlImportOperation = contract::decode(v.value())?;
        if key.format_version != 1 || value.format_version != 1 {
            return Err(corrupt("unsupported import operation format"));
        }
        contract::principal(&key.principal).map_err(corrupt)?;
        let command = value
            .command
            .as_ref()
            .ok_or_else(|| missing("stored import command"))?;
        import::validate_stored_command(command, identity, limits).map_err(corrupt)?;
        if key.key != command.key
            || key.command_id != command.command_id
            || value.request_sha256 != import::command_digest(command)
            || regular.get(k.value()).map_err(storage)?.is_some()
        {
            return Err(corrupt("import operation key, digest or namespace differs"));
        }
        let decision = value
            .decision
            .as_ref()
            .ok_or_else(|| missing("import decision"))?;
        decision_ok(decision, header, policy)?;
        if decision.code == 0 {
            if command.expected_control_revision.checked_add(1) != Some(decision.control_revision)
                || command.expected_policy_revision != decision.policy_revision
                || !decision.message.is_empty()
                || decision.receipt.is_some()
                    != matches!(command.action, Some(ImportAction::Commit(_)))
            {
                return Err(corrupt("accepted import decision differs from its command"));
            }
            accepted += 1;
        }
        payload += k.value().len() + v.value().len();
        ops.insert(k.value().to_vec(), value);
    }
    if operations.len().map_err(storage)? != control.import_decision_count {
        return Err(corrupt(
            "import decision count differs from the control header",
        ));
    }

    // Workflows: bound to actor and resource; STAGING rows hold the reservation.
    let imports = tx.open_table(IMPORTS).map_err(corrupt)?;
    let mut staging_per_resource: BTreeMap<Vec<u8>, ControlImportWorkflow> = BTreeMap::new();
    let mut committed_per_resource: BTreeMap<Vec<u8>, ControlImportReceipt> = BTreeMap::new();
    let mut reserved = (0u64, 0u64);
    let mut workflow_count = 0u64;
    for entry in imports.iter().map_err(storage)? {
        let (k, v) = entry.map_err(storage)?;
        let key: SourceAuthorityWorkflowKey = contract::decode(k.value())?;
        let row: ControlImportWorkflow = contract::decode(v.value())?;
        workflow_count += 1;
        payload += k.value().len() + v.value().len();
        let resource = key
            .key
            .as_ref()
            .ok_or_else(|| missing("workflow resource"))?;
        if row.format_version != 1
            || row.key.as_ref() != Some(resource)
            || row.workflow_id != key.workflow_id
        {
            return Err(corrupt("import workflow row differs from its key"));
        }
        contract::principal(&row.principal).map_err(corrupt)?;
        let begin_key = row
            .begin
            .as_ref()
            .ok_or_else(|| missing("workflow begin"))?;
        let begin = ops
            .get(&begin_key.encode_to_vec())
            .ok_or_else(|| corrupt("workflow begin decision is missing"))?;
        let begin_command = begin.command.as_ref().expect("validated command");
        let declared = row
            .declared
            .as_ref()
            .ok_or_else(|| missing("workflow declaration"))?;
        match (
            &begin_command.action,
            begin.decision.as_ref().map(|d| d.code),
        ) {
            (Some(ImportAction::Begin(b)), Some(0))
                if b == declared
                    && begin_key.principal == row.principal
                    && begin_command.workflow_id == row.workflow_id
                    && begin.decision.as_ref().map(|d| d.control_revision)
                        == Some(row.begun_control_revision) => {}
            _ => {
                return Err(corrupt(
                    "workflow begin decision differs from the workflow row",
                ))
            }
        }
        match ControlImportPhase::try_from(row.phase) {
            Ok(ControlImportPhase::Staging) => {
                if row.terminal.is_some() {
                    return Err(corrupt("staging workflow carries a terminal command"));
                }
                if staging_per_resource
                    .insert(resource.encode_to_vec(), row.clone())
                    .is_some()
                {
                    return Err(corrupt("two import workflows are pending for one resource"));
                }
                reserved.0 += row.reserved_bytes;
                reserved.1 += row.reserved_decisions;
            }
            Ok(phase @ (ControlImportPhase::Committed | ControlImportPhase::Aborted)) => {
                if row.reserved_bytes != 0 || row.reserved_decisions != 0 {
                    return Err(corrupt("terminal workflow still holds a reservation"));
                }
                let terminal_key = row
                    .terminal
                    .as_ref()
                    .ok_or_else(|| missing("terminal command"))?;
                let terminal = ops
                    .get(&terminal_key.encode_to_vec())
                    .ok_or_else(|| corrupt("terminal import decision is missing"))?;
                let action = terminal.command.as_ref().and_then(|c| c.action.as_ref());
                let decision = terminal.decision.as_ref().expect("validated decision");
                let matches = match (phase, action) {
                    (ControlImportPhase::Committed, Some(ImportAction::Commit(_))) => {
                        decision.receipt.is_some()
                    }
                    (ControlImportPhase::Aborted, Some(ImportAction::Abort(_))) => true,
                    _ => false,
                };
                if !matches || decision.code != 0 || terminal_key.principal != row.principal {
                    return Err(corrupt("terminal workflow decision differs from its phase"));
                }
                if phase == ControlImportPhase::Committed {
                    let receipt = decision.receipt.clone().expect("checked receipt");
                    if committed_per_resource
                        .insert(resource.encode_to_vec(), receipt)
                        .is_some()
                    {
                        return Err(corrupt("two committed imports for one resource"));
                    }
                }
            }
            _ => return Err(corrupt("unsupported import workflow phase")),
        }
    }
    if workflow_count != control.import_workflow_count
        || reserved != (control.reserved_bytes, control.reserved_decisions)
    {
        return Err(corrupt(
            "workflow count or reservation differs from the control header",
        ));
    }

    // Chunk references: each names a retained chunk command of a pending workflow.
    let chunks = tx.open_table(CHUNKS).map_err(corrupt)?;
    let mut staged_per_resource: BTreeMap<Vec<u8>, u32> = BTreeMap::new();
    let mut chunk_count = 0u64;
    for entry in chunks.iter().map_err(storage)? {
        let (k, v) = entry.map_err(storage)?;
        chunk_count += 1;
        payload += k.value().len() + v.value().len();
        let key: ControlImportChunkKey = contract::decode(k.value())?;
        let reference: ControlImportChunkRef = contract::decode(v.value())?;
        let resource = key.key.as_ref().ok_or_else(|| missing("chunk resource"))?;
        let workflow = staging_per_resource
            .get(&resource.encode_to_vec())
            .ok_or_else(|| corrupt("chunk reference without a pending workflow"))?;
        let command_key = reference
            .command
            .as_ref()
            .ok_or_else(|| missing("chunk command"))?;
        let op = ops
            .get(&command_key.encode_to_vec())
            .ok_or_else(|| corrupt("chunk reference names a missing command"))?;
        let command = op.command.as_ref().expect("validated command");
        let chunk = match command.action.as_ref() {
            Some(ImportAction::Chunk(chunk)) => chunk,
            _ => return Err(corrupt("chunk reference names a non-chunk command")),
        };
        if reference.format_version != 1
            || chunk.ordinal != key.ordinal
            || command.workflow_id != workflow.workflow_id
            || command_key.principal != workflow.principal
            || chunk.bytes.len() as u32 != reference.length
            || import::chunk_digest(&chunk.bytes) != reference.sha256
            || chunk.sha256 != reference.sha256
            || op.decision.as_ref().map(|d| d.code) != Some(0)
            || key.ordinal >= workflow.declared.as_ref().expect("validated").chunk_count
        {
            return Err(corrupt("chunk reference differs from its retained command"));
        }
        *staged_per_resource
            .entry(resource.encode_to_vec())
            .or_default() += 1;
    }
    for (resource, workflow) in &staging_per_resource {
        if staged_per_resource.get(resource).copied().unwrap_or(0) != workflow.staged_chunks {
            return Err(corrupt("staged chunk count differs from the workflow row"));
        }
    }
    if chunk_count != control.staged_chunk_count {
        return Err(corrupt(
            "staged chunk count differs from the control header",
        ));
    }

    // Applied state: one control row per committed resource, its rows exact.
    let control_table = tx.open_table(CONTROL).map_err(corrupt)?;
    let mut expected_rows: BTreeMap<Vec<u8>, ControlImportReceipt> = BTreeMap::new();
    let mut control_count = 0u64;
    let topologies = tx.open_table(TOPOLOGIES).map_err(corrupt)?;
    for entry in control_table.iter().map_err(storage)? {
        let (k, v) = entry.map_err(storage)?;
        control_count += 1;
        payload += k.value().len() + v.value().len();
        let key: LogicalSourceOwner = contract::decode(k.value())?;
        let state: ControlCollectionState = contract::decode(v.value())?;
        let receipt = state
            .import
            .as_ref()
            .ok_or_else(|| missing("applied import receipt"))?;
        if state.format_version != 1
            || state.key.as_ref() != Some(&key)
            || state.control_revision == 0
            || state.control_revision > header.control_revision
            || state.history_rollback_available
            || committed_per_resource.get(k.value()) != Some(receipt)
            || receipt.control_revision != state.control_revision
            || receipt.imported_generation != state.topology_generation
            || receipt.history != state.history_generations.len() as u64
        {
            return Err(corrupt(
                "applied control state differs from its committed import",
            ));
        }
        let mut generations = vec![state.topology_generation];
        generations.extend(&state.history_generations);
        for (index, generation) in generations.iter().enumerate() {
            let topology_key = ControlTopologyKey {
                key: Some(key.clone()),
                generation: *generation,
            }
            .encode_to_vec();
            let row: ControlTopology = contract::decode(
                topologies
                    .get(topology_key.as_slice())
                    .map_err(storage)?
                    .ok_or_else(|| corrupt("applied topology row is missing"))?
                    .value(),
            )?;
            let current = index == 0;
            if row.format_version != 1
                || row.generation != *generation
                || row.current != current
                || (current && row.routes.len() as u64 != receipt.routes)
                || (!current
                    && (row.codes_available
                        || !row.codes.is_empty()
                        || row.history_index as usize != index - 1))
                || (row.codes_available != !row.codes.is_empty())
                || (row.codes_available && row.codes.len() != row.routes.len())
            {
                return Err(corrupt("applied topology row differs from its state"));
            }
        }
        expected_rows.insert(k.value().to_vec(), receipt.clone());
    }
    if control_count != control.control_count
        || control_count != committed_per_resource.len() as u64
    {
        return Err(corrupt(
            "applied control count differs from committed imports",
        ));
    }
    let mut topology_rows = 0u64;
    for entry in topologies.iter().map_err(storage)? {
        let (k, v) = entry.map_err(storage)?;
        topology_rows += 1;
        payload += k.value().len() + v.value().len();
    }
    let expected_topologies: u64 = expected_rows.values().map(|r| r.history + 1).sum();
    if topology_rows != control.topology_count || topology_rows != expected_topologies {
        return Err(corrupt("topology row count differs from applied imports"));
    }

    // Per-resource fact counts must equal the receipts, and be bounded rows.
    let mut counts: BTreeMap<Vec<u8>, [u64; 4]> = BTreeMap::new();
    let mut totals = [0u64; 4];
    for (slot, definition) in [(0usize, NODES), (1, REPLICAS), (2, ACTIONS), (3, COMPLETED)] {
        let table = tx.open_table(definition).map_err(corrupt)?;
        for entry in table.iter().map_err(storage)? {
            let (k, v) = entry.map_err(storage)?;
            payload += k.value().len() + v.value().len();
            totals[slot] += 1;
            let resource = match slot {
                0 => contract::decode::<ControlNodeKey>(k.value())?.key,
                1 => contract::decode::<ControlReplicaKey>(k.value())?.key,
                _ => contract::decode::<ControlActionKey>(k.value())?.key,
            }
            .ok_or_else(|| missing("fact resource"))?
            .encode_to_vec();
            match slot {
                0 => {
                    let row: ControlNode = contract::decode(v.value())?;
                    if row.format_version != 1
                        || row.node.is_none()
                        || row.provenance != ControlFactProvenance::LegacyImport as i32
                    {
                        return Err(corrupt("applied node row is invalid"));
                    }
                }
                1 => {
                    let row: ControlReplica = contract::decode(v.value())?;
                    if row.format_version != 1
                        || row.replica.is_none()
                        || row.provenance != ControlFactProvenance::LegacyImport as i32
                    {
                        return Err(corrupt("applied replica row is invalid"));
                    }
                }
                2 => {
                    let row: ControlAction = contract::decode(v.value())?;
                    if row.format_version != 1
                        || row.action.is_none()
                        || row.state != ControlActionState::ImportedUnreconciled as i32
                    {
                        return Err(corrupt("applied action row is invalid or reconciled"));
                    }
                }
                _ => {
                    let row: ControlCompletedAction = contract::decode(v.value())?;
                    if row.format_version != 1
                        || row.provenance != ControlFactProvenance::LegacyImport as i32
                    {
                        return Err(corrupt("applied completed-action row is invalid"));
                    }
                }
            }
            counts.entry(resource).or_default()[slot] += 1;
        }
    }
    for (resource, receipt) in &expected_rows {
        let have = counts.get(resource).copied().unwrap_or_default();
        if have
            != [
                receipt.nodes,
                receipt.replicas,
                receipt.actions,
                receipt.completed_actions,
            ]
        {
            return Err(corrupt(
                "applied fact counts differ from the import receipt",
            ));
        }
    }
    if counts.len() > expected_rows.len() {
        return Err(corrupt("applied facts exist without a control row"));
    }
    if totals
        != [
            control.node_count,
            control.replica_count,
            control.action_count,
            control.completed_count,
        ]
    {
        return Err(corrupt(
            "applied fact counts differ from the control header",
        ));
    }
    Ok((accepted, payload))
}
