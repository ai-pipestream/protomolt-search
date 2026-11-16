//! The rows an install writes, and the recovery check that the installed
//! rows are exactly those of the accepted command.
use super::*;

/// Rows an install writes, each already bounded.
pub(super) struct Rows {
    pub(super) receipt: ControlInstallReceipt,
    control: (Vec<u8>, Vec<u8>),
    topologies: Vec<(Vec<u8>, Vec<u8>)>,
    nodes: Vec<(Vec<u8>, Vec<u8>)>,
    replicas: Vec<(Vec<u8>, Vec<u8>)>,
    actions: Vec<(Vec<u8>, Vec<u8>)>,
    completed: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Rows {
    pub(super) fn iter(&self) -> impl Iterator<Item = &(Vec<u8>, Vec<u8>)> {
        std::iter::once(&self.control)
            .chain(&self.topologies)
            .chain(&self.nodes)
            .chain(&self.replicas)
            .chain(&self.actions)
            .chain(&self.completed)
    }
}

pub(super) fn write_rows(
    tx: &redb::WriteTransaction,
    rows: &Rows,
) -> Result<ControlStoreHeader, Status> {
    let mut delta = ControlStoreHeader::default();
    tx.open_table(CONTROL)
        .map_err(storage)?
        .insert(rows.control.0.as_slice(), rows.control.1.as_slice())
        .map_err(storage)?;
    delta.control_count = 1;
    for (definition, rows, count) in [
        (TOPOLOGIES, &rows.topologies, &mut delta.topology_count),
        (NODES, &rows.nodes, &mut delta.node_count),
        (REPLICAS, &rows.replicas, &mut delta.replica_count),
        (ACTIONS, &rows.actions, &mut delta.action_count),
        (COMPLETED, &rows.completed, &mut delta.completed_count),
    ] {
        let mut table = tx.open_table(definition).map_err(storage)?;
        for (k, v) in rows {
            if table
                .insert(k.as_slice(), v.as_slice())
                .map_err(storage)?
                .is_some()
            {
                return Err(corrupt("installed control row already exists"));
            }
            *count += 1;
        }
    }
    Ok(delta)
}

/// Recovery reconstructs one bounded canonical row at a time from the retained
/// accepted command. No corpus-sized expected-row map or new stored digest is
/// needed; the existing audit separately checks table counts and payload bytes.
pub(in crate::source_authority) fn verify_installed(
    tx: &redb::ReadTransaction,
    command: &ControlInstallCommand,
    receipt: &ControlInstallReceipt,
) -> Result<(), Status> {
    let definitions = [CONTROL, TOPOLOGIES, NODES, REPLICAS, ACTIONS, COMPLETED];
    let tables = definitions
        .into_iter()
        .map(|definition| tx.open_table(definition).map_err(corrupt))
        .collect::<Result<Vec<_>, _>>()?;
    let expected = visit_rows(
        command
            .key
            .as_ref()
            .ok_or_else(|| missing("install command key"))?,
        command
            .state
            .as_ref()
            .ok_or_else(|| missing("install command state"))?,
        receipt.control_revision,
        |table, (key, value)| {
            let stored = tables[table]
                .get(key.as_slice())
                .map_err(storage)?
                .ok_or_else(|| corrupt("accepted install row is missing or its key changed"))?;
            if stored.value() != value.as_slice() {
                return Err(corrupt(
                    "installed control row differs from its accepted install command",
                ));
            }
            Ok(())
        },
    )
    .map_err(corrupt)?;
    if expected != *receipt {
        return Err(corrupt("install receipt differs from its accepted command"));
    }
    Ok(())
}

pub(super) fn rows(
    key: &LogicalSourceOwner,
    state: &ControlInstallState,
    revision: u64,
) -> Result<Rows, Status> {
    let mut groups: [RawRows; 6] = Default::default();
    let receipt = visit_rows(key, state, revision, |group, row| {
        groups[group].push(row);
        Ok(())
    })?;
    let [mut control, topologies, nodes, replicas, actions, completed] = groups;
    Ok(Rows {
        receipt,
        control: control.pop().expect("one canonical control row"),
        topologies,
        nodes,
        replicas,
        actions,
        completed,
    })
}

/// Lay out a validated state as bounded rows. Pure over its inputs.
fn visit_rows(
    key: &LogicalSourceOwner,
    state: &ControlInstallState,
    revision: u64,
    mut visit: impl FnMut(usize, (Vec<u8>, Vec<u8>)) -> Result<(), Status>,
) -> Result<ControlInstallReceipt, Status> {
    let current = state.topology.as_ref().expect("validated topology");
    let configuration = state
        .configuration
        .as_ref()
        .expect("validated configuration");
    visit_topologies(key, state, &mut visit)?;
    visit_nodes(key, state, &mut visit)?;
    visit_replicas_and_actions(key, state, &mut visit)?;
    let receipt = ControlInstallReceipt {
        format_version: 1,
        control_revision: revision,
        generation: current.generation,
        routes: current.routes.len() as u64,
        history: state.history.len() as u64,
        nodes: state.nodes.len() as u64,
        replicas: state.replicas.len() as u64,
        actions: state.actions.len() as u64,
        completed_actions: state.completed_actions.len() as u64,
    };
    let control_value = ControlCollectionState {
        format_version: 1,
        key: Some(key.clone()),
        install: Some(receipt),
        control_revision: revision,
        next_token: state.next_token,
        next_action: state.next_action,
        topology_generation: current.generation,
        history_generations: state.history.iter().map(|t| t.generation).collect(),
        configuration: Some(configuration.clone()),
    }
    .encode_to_vec();
    bounded_record(&control_value, "installed control state")?;
    visit(0, (key.encode_to_vec(), control_value))?;
    Ok(receipt)
}

/// The current topology with its placement codes, then each history table.
fn visit_topologies(
    key: &LogicalSourceOwner,
    state: &ControlInstallState,
    visit: &mut impl FnMut(usize, (Vec<u8>, Vec<u8>)) -> Result<(), Status>,
) -> Result<(), Status> {
    let current = state.topology.as_ref().expect("validated topology");
    let configuration = state
        .configuration
        .as_ref()
        .expect("validated configuration");
    let codes = match configuration.placement.as_ref() {
        Some(control_configuration::Placement::Tree(_)) => configuration.route_codes.clone(),
        _ => Vec::new(),
    };
    visit(
        1,
        topology_row(
            key,
            ControlTopology {
                format_version: 1,
                generation: current.generation,
                routes: current.routes.clone(),
                codes_available: !codes.is_empty(),
                codes,
                current: true,
                history_index: 0,
            },
        )?,
    )?;
    for (index, table) in state.history.iter().enumerate() {
        visit(
            1,
            topology_row(
                key,
                ControlTopology {
                    format_version: 1,
                    generation: table.generation,
                    routes: table.routes.clone(),
                    codes_available: false,
                    codes: Vec::new(),
                    current: false,
                    history_index: index as u32,
                },
            )?,
        )?;
    }
    Ok(())
}

fn visit_nodes(
    key: &LogicalSourceOwner,
    state: &ControlInstallState,
    visit: &mut impl FnMut(usize, (Vec<u8>, Vec<u8>)) -> Result<(), Status>,
) -> Result<(), Status> {
    for node in &state.nodes {
        let k = ControlNodeKey {
            key: Some(key.clone()),
            node_id: node.node_id.clone(),
        }
        .encode_to_vec();
        // The node's own residency declaration, copied exactly.
        let residency = match node
            .capacity
            .as_ref()
            .and_then(|c| crate::pb::NodeResidency::try_from(c.residency).ok())
            .expect("validated residency")
        {
            crate::pb::NodeResidency::Server => SourceResidency::Server,
            crate::pb::NodeResidency::Device => SourceResidency::DeviceLocal,
            crate::pb::NodeResidency::Unspecified => SourceResidency::Unspecified,
        };
        let v = ControlNode {
            format_version: 1,
            node: Some(node.clone()),
            residency: residency as i32,
        }
        .encode_to_vec();
        bounded_record(&v, "installed node")?;
        visit(2, (k, v))?;
    }
    Ok(())
}

/// Replica rows, pending actions in their command order, then the
/// completed action ids.
fn visit_replicas_and_actions(
    key: &LogicalSourceOwner,
    state: &ControlInstallState,
    visit: &mut impl FnMut(usize, (Vec<u8>, Vec<u8>)) -> Result<(), Status>,
) -> Result<(), Status> {
    for replica in &state.replicas {
        let k = ControlReplicaKey {
            key: Some(key.clone()),
            replica_key: replica_key(&replica.shard_id, &replica.node_id),
        }
        .encode_to_vec();
        let v = ControlReplica {
            format_version: 1,
            replica: Some(replica.clone()),
        }
        .encode_to_vec();
        bounded_record(&v, "installed replica")?;
        visit(3, (k, v))?;
    }
    for (order, action) in state.actions.iter().enumerate() {
        let k = ControlActionKey {
            key: Some(key.clone()),
            action_id: action.action_id,
        }
        .encode_to_vec();
        let v = ControlAction {
            format_version: 1,
            action: Some(action.clone()),
            order: order as u32,
            state: ControlActionState::Unreconciled as i32,
        }
        .encode_to_vec();
        bounded_record(&v, "installed action")?;
        visit(4, (k, v))?;
    }
    for id in &state.completed_actions {
        visit(
            5,
            (
                ControlActionKey {
                    key: Some(key.clone()),
                    action_id: *id,
                }
                .encode_to_vec(),
                ControlCompletedAction { format_version: 1 }.encode_to_vec(),
            ),
        )?;
    }
    Ok(())
}

fn topology_row(
    key: &LogicalSourceOwner,
    row: ControlTopology,
) -> Result<(Vec<u8>, Vec<u8>), Status> {
    let k = ControlTopologyKey {
        key: Some(key.clone()),
        generation: row.generation,
    }
    .encode_to_vec();
    let v = row.encode_to_vec();
    bounded_record(&v, "installed topology")?;
    Ok((k, v))
}
