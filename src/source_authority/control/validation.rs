//! Validation of a control install command and the state it carries.
use super::*;

pub(super) fn validate_command(
    command: &ControlInstallCommand,
    identity: &SourceAuthorityIdentity,
    limits: &SourceAuthorityLimits,
) -> Result<(), Status> {
    if command.encoded_len() > limits.max_command_bytes as usize {
        return Err(Status::resource_exhausted(
            "control install command exceeds max_command_bytes",
        ));
    }
    if command.format_version != 1 {
        return Err(invalid("requires command format 1"));
    }
    if command.authority.as_ref() != Some(identity) {
        return Err(Status::failed_precondition(
            "control install authority group or incarnation differs",
        ));
    }
    let key = command
        .key
        .as_ref()
        .ok_or_else(|| invalid("resource key is required"))?;
    contract::key(key, false)?;
    if !key.owner_id.is_empty() {
        return Err(invalid("the command names a collection, not an owner"));
    }
    contract::operation_id(&command.command_id)?;
    validate_state(
        command
            .state
            .as_ref()
            .ok_or_else(|| invalid("state is required"))?,
    )
}

pub(in crate::source_authority) fn validate_stored_command(
    command: &ControlInstallCommand,
    identity: &SourceAuthorityIdentity,
    limits: &SourceAuthorityLimits,
) -> Result<(), Status> {
    validate_command(command, identity, limits)
}

/// The state is pure data on the command, so every check here is a direct
/// refusal: nothing is recorded for a command that could never apply.
fn validate_state(state: &ControlInstallState) -> Result<(), Status> {
    if state.next_token == 0 || state.next_action == 0 {
        return Err(invalid("allocators must be nonzero"));
    }
    let current = state
        .topology
        .as_ref()
        .ok_or_else(|| invalid("topology is required"))?;
    let mut generations = BTreeSet::new();
    for table in std::iter::once(current).chain(&state.history) {
        if !generations.insert(table.generation) {
            return Err(invalid("a topology generation repeats"));
        }
        if table.routes.iter().any(|r| r.hash_lo > r.hash_hi) {
            return Err(invalid("topology route has a reversed hash range"));
        }
    }
    validate_nodes(state)?;
    validate_replicas(state)?;
    validate_actions(state)?;
    validate_configuration(
        state
            .configuration
            .as_ref()
            .ok_or_else(|| invalid("configuration is required"))?,
        current.routes.len(),
    )
}

fn validate_nodes(state: &ControlInstallState) -> Result<(), Status> {
    use crate::pb::{ClusterNodeState, NodeResidency};
    let mut previous: Option<&str> = None;
    let mut lease_tokens = BTreeSet::new();
    for node in &state.nodes {
        if node.node_id.is_empty() || node.node_id.contains('\0') {
            return Err(invalid("node id is empty or contains NUL"));
        }
        if previous.is_some_and(|p| p >= node.node_id.as_str()) {
            return Err(invalid("nodes must be unique and in node_id order"));
        }
        previous = Some(&node.node_id);
        if !matches!(
            ClusterNodeState::try_from(node.state),
            Ok(ClusterNodeState::Active | ClusterNodeState::Draining | ClusterNodeState::Expired)
        ) {
            return Err(invalid("node state is unknown or unspecified"));
        }
        if node.lease_token == 0
            || node.lease_token >= state.next_token
            || !lease_tokens.insert(node.lease_token)
        {
            return Err(invalid(
                "node lease token is zero, reused or reaches next_token",
            ));
        }
        let capacity = node
            .capacity
            .as_ref()
            .ok_or_else(|| invalid("node capacity is required"))?;
        if NodeResidency::try_from(capacity.residency).is_err() {
            return Err(invalid("node residency is unknown"));
        }
    }
    Ok(())
}

fn validate_replicas(state: &ControlInstallState) -> Result<(), Status> {
    use crate::pb::ShardReplicaRole;
    let mut previous: Option<String> = None;
    for replica in &state.replicas {
        if replica.shard_id.contains('\0') || replica.node_id.contains('\0') {
            return Err(invalid("replica shard or node id contains NUL"));
        }
        let key = replica_key(&replica.shard_id, &replica.node_id);
        if previous.as_ref().is_some_and(|p| *p >= key) {
            return Err(invalid(
                "replicas must be unique and in (shard_id, node_id) order",
            ));
        }
        previous = Some(key);
        if !matches!(
            ShardReplicaRole::try_from(replica.role),
            Ok(ShardReplicaRole::Primary | ShardReplicaRole::Replica)
        ) {
            return Err(invalid("replica role is unknown or unspecified"));
        }
        if replica.hash_lo > replica.hash_hi || replica.tombstones > replica.rows {
            return Err(invalid("replica range or tombstone count is invalid"));
        }
    }
    Ok(())
}

fn validate_actions(state: &ControlInstallState) -> Result<(), Status> {
    use crate::pb::PlacementActionKind;
    let mut ids = BTreeSet::new();
    let mut prior = None;
    for &id in &state.completed_actions {
        if id == 0 || id >= state.next_action || prior.is_some_and(|p| p >= id) {
            return Err(invalid(
                "completed actions must be ascending unique nonzero ids below next_action",
            ));
        }
        prior = Some(id);
        ids.insert(id);
    }
    for action in &state.actions {
        if action.action_id == 0
            || action.action_id >= state.next_action
            || !ids.insert(action.action_id)
        {
            return Err(invalid(
                "pending action id is zero, reused or reaches next_action",
            ));
        }
        if !matches!(
            PlacementActionKind::try_from(action.kind),
            Ok(PlacementActionKind::CopyReplica
                | PlacementActionKind::DropReplica
                | PlacementActionKind::PromoteReplica
                | PlacementActionKind::SplitShard
                | PlacementActionKind::MergeShards
                | PlacementActionKind::CompactShard)
        ) {
            return Err(invalid("action kind is unknown or unspecified"));
        }
        if action.hash_lo > action.hash_hi {
            return Err(invalid("action has a reversed hash range"));
        }
    }
    Ok(())
}

fn bad(message: &str) -> Status {
    invalid(&format!("configuration: {message}"))
}

fn validate_configuration(
    configuration: &ControlConfiguration,
    routes: usize,
) -> Result<(), Status> {
    if configuration.format_version != 1 {
        return Err(bad("requires format 1"));
    }
    validate_placement(configuration, routes)?;
    validate_derived(configuration)?;
    validate_provider(configuration)?;
    let policy = configuration
        .policy
        .as_ref()
        .ok_or_else(|| bad("planner policy is required"))?;
    if policy.format_version != 1 || policy.planner_version == 0 || policy.reconcile.is_none() {
        return Err(bad(
            "planner policy requires format 1, a nonzero planner version and a reconcile policy",
        ));
    }
    Ok(())
}

fn validate_placement(configuration: &ControlConfiguration, routes: usize) -> Result<(), Status> {
    use control_configuration::Placement;
    match configuration.placement.as_ref() {
        Some(Placement::Tree(tree)) => {
            let placement = crate::placement::Placement::validate(
                &crate::placement::PlacementTreeConfig::from_proto(tree),
            )
            .map_err(|error| bad(&format!("placement tree: {error}")))?;
            if configuration.route_codes.len() != routes {
                return Err(bad(&format!(
                    "{} route codes for {routes} current routes",
                    configuration.route_codes.len()
                )));
            }
            for (index, code) in configuration.route_codes.iter().enumerate() {
                if !code.has_placement {
                    return Err(bad(&format!(
                        "route {index} has no placement code under a tree"
                    )));
                }
                let signed = i64::try_from(code.placement)
                    .map_err(|_| bad(&format!("route {index} placement code exceeds i64")))?;
                if placement.leaf_by_code(signed).is_none() {
                    return Err(bad(&format!("route {index} placement code names no leaf")));
                }
            }
            Ok(())
        }
        Some(Placement::NoPlacement(true)) => {
            if !configuration.route_codes.is_empty() {
                return Err(bad("route codes are present without a tree"));
            }
            Ok(())
        }
        _ => Err(bad(
            "placement must be an explicit tree or no_placement = true",
        )),
    }
}

fn validate_derived(configuration: &ControlConfiguration) -> Result<(), Status> {
    use control_configuration::Derived;
    match configuration.derived.as_ref() {
        Some(Derived::Declaration(declaration)) => {
            crate::derived::Declaration::compile(declaration)
                .map_err(|error| bad(&format!("derived declaration: {error}")))?;
            if crate::derived::fingerprint_of(declaration) != configuration.derived_fingerprint {
                return Err(bad("derived fingerprint differs from the declaration"));
            }
            Ok(())
        }
        Some(Derived::NoDerived(true)) => {
            if !configuration.derived_fingerprint.is_empty() {
                return Err(bad("derived fingerprint present without a declaration"));
            }
            Ok(())
        }
        _ => Err(bad(
            "derived must be an explicit declaration or no_derived = true",
        )),
    }
}

fn validate_provider(configuration: &ControlConfiguration) -> Result<(), Status> {
    use control_configuration::Provider;
    match configuration.provider.as_ref() {
        Some(Provider::Geometry(geometry)) => {
            if geometry.format_version != 1
                || geometry.backend_kind.is_empty()
                || geometry.config_format.is_empty()
                || geometry.dimension == 0
                || geometry.dimension % 8 != 0
                || !(2..=4).contains(&geometry.bits_per_component)
                || geometry.row_bytes_formula_version != 1
                || geometry.scoring_fingerprint.is_empty()
            {
                return Err(bad("provider geometry requires format 1, backend kind and config format, a dimension that is a positive multiple of 8, 2..4 bits, row-bytes formula 1 and a scoring fingerprint"));
            }
            Ok(())
        }
        Some(Provider::NoProvider(true)) => Ok(()),
        _ => Err(bad(
            "provider must be explicit geometry or no_provider = true",
        )),
    }
}
