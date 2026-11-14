//! Audit of the control tables: every install decision and installed row is
//! recomputed against the headers. Nothing is repaired; a disagreement
//! refuses the open.
use super::super::control::{self, *};
use super::*;
use std::collections::BTreeMap;

fn decision_ok(
    decision: &ControlInstallDecision,
    header: &SourceAuthorityHeader,
    policy: &AccessPolicy,
) -> Result<(), Status> {
    if decision.format_version != 1
        || decision.control_revision == 0
        || decision.control_revision > header.control_revision
        || decision.policy_revision == 0
        || decision.policy_revision > policy.revision
        || ![0, 6, 8, 9].contains(&decision.code)
    {
        return Err(corrupt(
            "install decision format, status or revision is invalid",
        ));
    }
    if (decision.code == 0) != decision.receipt.is_some()
        || (decision.code != 0 && decision.message.is_empty())
    {
        return Err(corrupt(
            "install decision receipt or reason differs from its status",
        ));
    }
    Ok(())
}

/// Returns the accepted install count and the bytes held by every control
/// table (keys and values), for the header reconciliation.
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

    // Install operations: the retained retry history, one record per command.
    let operations = tx.open_table(INSTALL_OPERATIONS).map_err(corrupt)?;
    let mut installed: BTreeMap<Vec<u8>, ControlInstallReceipt> = BTreeMap::new();
    for entry in operations.iter().map_err(storage)? {
        let (k, v) = entry.map_err(storage)?;
        let key: SourceAuthorityOperationKey = contract::decode(k.value())?;
        let value: ControlInstallOperation = contract::decode(v.value())?;
        if key.format_version != 1 || value.format_version != 1 {
            return Err(corrupt("unsupported install operation format"));
        }
        contract::principal(&key.principal).map_err(corrupt)?;
        let command = value
            .command
            .as_ref()
            .ok_or_else(|| missing("stored install command"))?;
        control::validate_stored_command(command, identity, limits).map_err(corrupt)?;
        if key.key != command.key
            || key.command_id != command.command_id
            || value.request_sha256 != control::command_digest(command)
            || regular.get(k.value()).map_err(storage)?.is_some()
        {
            return Err(corrupt(
                "install operation key, digest or namespace differs",
            ));
        }
        let decision = value
            .decision
            .as_ref()
            .ok_or_else(|| missing("install decision"))?;
        decision_ok(decision, header, policy)?;
        if let Some(receipt) = decision.receipt.as_ref() {
            if command.expected_control_revision.checked_add(1) != Some(decision.control_revision)
                || command.expected_policy_revision != decision.policy_revision
                || !decision.message.is_empty()
                || receipt.control_revision != decision.control_revision
            {
                return Err(corrupt(
                    "accepted install decision differs from its command",
                ));
            }
            control::verify_installed(tx, command, receipt)?;
            let resource = command.key.as_ref().expect("validated key").encode_to_vec();
            if installed.insert(resource, *receipt).is_some() {
                return Err(corrupt("two accepted installs for one resource"));
            }
        }
        payload += k.value().len() + v.value().len();
    }
    if operations.len().map_err(storage)? != control.install_decision_count {
        return Err(corrupt(
            "install decision count differs from the control header",
        ));
    }

    // Installed state: one control row per accepted install, its rows exact.
    let control_table = tx.open_table(CONTROL).map_err(corrupt)?;
    let mut control_count = 0u64;
    let topologies = tx.open_table(TOPOLOGIES).map_err(corrupt)?;
    for entry in control_table.iter().map_err(storage)? {
        let (k, v) = entry.map_err(storage)?;
        control_count += 1;
        payload += k.value().len() + v.value().len();
        let key: LogicalSourceOwner = contract::decode(k.value())?;
        let state: ControlCollectionState = contract::decode(v.value())?;
        let receipt = state
            .install
            .as_ref()
            .ok_or_else(|| missing("install receipt"))?;
        if state.format_version != 1
            || state.key.as_ref() != Some(&key)
            || state.control_revision == 0
            || state.control_revision > header.control_revision
            || installed.get(k.value()) != Some(receipt)
            || receipt.control_revision != state.control_revision
            || receipt.generation != state.topology_generation
            || receipt.history != state.history_generations.len() as u64
        {
            return Err(corrupt(
                "installed control state differs from its accepted install",
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
                    .ok_or_else(|| corrupt("installed topology row is missing"))?
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
                return Err(corrupt("installed topology row differs from its state"));
            }
        }
    }
    if control_count != control.control_count || control_count != installed.len() as u64 {
        return Err(corrupt(
            "installed control count differs from accepted installs",
        ));
    }
    let mut topology_rows = 0u64;
    for entry in topologies.iter().map_err(storage)? {
        let (k, v) = entry.map_err(storage)?;
        topology_rows += 1;
        payload += k.value().len() + v.value().len();
    }
    let expected_topologies: u64 = installed.values().map(|r| r.history + 1).sum();
    if topology_rows != control.topology_count || topology_rows != expected_topologies {
        return Err(corrupt("topology row count differs from accepted installs"));
    }

    // Per-resource fact counts must equal the receipts.
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
            let valid = match slot {
                0 => {
                    let row: ControlNode = contract::decode(v.value())?;
                    row.format_version == 1 && row.node.is_some()
                }
                1 => {
                    let row: ControlReplica = contract::decode(v.value())?;
                    row.format_version == 1 && row.replica.is_some()
                }
                2 => {
                    let row: ControlAction = contract::decode(v.value())?;
                    row.format_version == 1
                        && row.action.is_some()
                        && row.state == ControlActionState::Unreconciled as i32
                }
                _ => contract::decode::<ControlCompletedAction>(v.value())?.format_version == 1,
            };
            if !valid {
                return Err(corrupt("installed fact row is invalid"));
            }
            counts.entry(resource).or_default()[slot] += 1;
        }
    }
    for (resource, receipt) in &installed {
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
                "installed fact counts differ from the install receipt",
            ));
        }
    }
    if counts.len() > installed.len() {
        return Err(corrupt("installed facts exist without a control row"));
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
            "installed fact counts differ from the control header",
        ));
    }
    Ok((installed.len() as u64, payload))
}
