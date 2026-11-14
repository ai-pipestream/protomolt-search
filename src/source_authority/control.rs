//! Collection control state (docs/control-state.md): one bounded install
//! command writes a collection's topology, nodes, replicas and actions as
//! rows, and the planner, the map feed and observers read those rows.
//!
//! The install is an actor-scoped, retryable command in one durable
//! transaction, in the same command_id namespace as every other command.
//! Nothing here activates an owner, publishes a map or reconciles an action.
use super::*;
use std::collections::BTreeSet;

pub(super) const CONTROL_HEADER: &str = "control";
pub(super) const STORE_FORMAT: u32 = 3;
pub(super) const INSTALL_OPERATIONS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("source_authority_control_install_operations");
pub(super) const CONTROL: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("source_authority_control");
pub(super) const TOPOLOGIES: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("source_authority_control_topologies");
pub(super) const NODES: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("source_authority_control_nodes");
pub(super) const REPLICAS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("source_authority_control_replicas");
pub(super) const ACTIONS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("source_authority_control_actions");
pub(super) const COMPLETED: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("source_authority_control_completed");

const COMMAND_DOMAIN: &[u8] = b"protomolt.control-install.command.v1\0";
const SNAPSHOT_DOMAIN: &[u8] = b"protomolt.control-snapshot.v1\0";

pub(super) fn hash(domain: &[u8], bytes: &[u8]) -> Vec<u8> {
    let mut hasher = crate::sha256::Sha256::new();
    hasher.update(domain);
    hasher.update(bytes);
    hasher.finalize().to_vec()
}

pub(super) fn command_digest(command: &ControlInstallCommand) -> Vec<u8> {
    hash(COMMAND_DOMAIN, &command.encode_to_vec())
}

pub(super) fn resource_prefix(key: &LogicalSourceOwner) -> Vec<u8> {
    // Field 1 of every keyed record is the resource, so its encoding is the
    // byte prefix shared by every row of that resource.
    let mut prefix = Vec::new();
    prost::encoding::message::encode(1, key, &mut prefix);
    prefix
}

pub(super) fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < u8::MAX {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

/// The replica row key: shard id, NUL, node id.
pub(super) fn replica_key(shard_id: &str, node_id: &str) -> String {
    format!("{shard_id}\0{node_id}")
}

/// Raw key/value pairs read from one table range.
type RawRows = Vec<(Vec<u8>, Vec<u8>)>;

fn reject(code: Code, message: impl Into<String>) -> Status {
    Status::new(code, message.into())
}

fn invalid(message: &str) -> Status {
    Status::invalid_argument(format!("control install: {message}"))
}

fn validate_command(
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

pub(super) fn validate_stored_command(
    command: &ControlInstallCommand,
    identity: &SourceAuthorityIdentity,
    limits: &SourceAuthorityLimits,
) -> Result<(), Status> {
    validate_command(command, identity, limits)
}

/// The state is pure data on the command, so every check here is a direct
/// refusal: nothing is recorded for a command that could never apply.
fn validate_state(state: &ControlInstallState) -> Result<(), Status> {
    use crate::pb::{ClusterNodeState, NodeResidency, PlacementActionKind, ShardReplicaRole};
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
    validate_configuration(
        state
            .configuration
            .as_ref()
            .ok_or_else(|| invalid("configuration is required"))?,
        current.routes.len(),
    )
}

fn validate_configuration(
    configuration: &ControlConfiguration,
    routes: usize,
) -> Result<(), Status> {
    use control_configuration::{Derived, Placement, Provider};
    let bad = |message: &str| invalid(&format!("configuration: {message}"));
    if configuration.format_version != 1 {
        return Err(bad("requires format 1"));
    }
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
        }
        Some(Placement::NoPlacement(true)) => {
            if !configuration.route_codes.is_empty() {
                return Err(bad("route codes are present without a tree"));
            }
        }
        _ => {
            return Err(bad(
                "placement must be an explicit tree or no_placement = true",
            ))
        }
    }
    match configuration.derived.as_ref() {
        Some(Derived::Declaration(declaration)) => {
            crate::derived::Declaration::compile(declaration)
                .map_err(|error| bad(&format!("derived declaration: {error}")))?;
            if crate::derived::fingerprint_of(declaration) != configuration.derived_fingerprint {
                return Err(bad("derived fingerprint differs from the declaration"));
            }
        }
        Some(Derived::NoDerived(true)) => {
            if !configuration.derived_fingerprint.is_empty() {
                return Err(bad("derived fingerprint present without a declaration"));
            }
        }
        _ => {
            return Err(bad(
                "derived must be an explicit declaration or no_derived = true",
            ))
        }
    }
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
        }
        Some(Provider::NoProvider(true)) => {}
        _ => {
            return Err(bad(
                "provider must be explicit geometry or no_provider = true",
            ))
        }
    }
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

pub(super) struct Header {
    pub(super) source: SourceAuthorityHeader,
    pub(super) control: ControlStoreHeader,
    pub(super) limits: SourceAuthorityLimits,
}

pub(super) fn read_headers(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
    identity: &SourceAuthorityIdentity,
) -> Result<(Header, AccessPolicy), Status> {
    let source: SourceAuthorityHeader = contract::decode(
        meta.get("header")
            .map_err(storage)?
            .ok_or_else(|| missing("header"))?
            .value(),
    )?;
    recovery::header(&source, identity)?;
    let control = control_header(meta)?;
    let policy: AccessPolicy = contract::decode(
        meta.get("policy")
            .map_err(storage)?
            .ok_or_else(|| missing("policy"))?
            .value(),
    )?;
    contract::policy(&policy).map_err(corrupt)?;
    let limits = source.limits.ok_or_else(|| missing("limits"))?;
    Ok((
        Header {
            source,
            control,
            limits,
        },
        policy,
    ))
}

fn control_header(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<ControlStoreHeader, Status> {
    let control: ControlStoreHeader = contract::decode(
        meta.get(CONTROL_HEADER)
            .map_err(storage)?
            .ok_or_else(|| missing("control header"))?
            .value(),
    )?;
    recovery::control_header(&control)?;
    Ok(control)
}

/// Decisions retained outside the regular decision table (installs and
/// capacity configures); every command's capacity check counts them.
pub(super) fn retained_decisions(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<u64, Status> {
    control_header(meta)?
        .install_decision_count
        .checked_add(capacity::header(meta)?.configure_decision_count)
        .ok_or_else(|| corrupt("retained decision count overflow"))
}

fn bounded_record(bytes: &[u8], what: &str) -> Result<(), Status> {
    if bytes.len() > contract::MAX_RECORD_BYTES {
        return Err(reject(
            Code::ResourceExhausted,
            format!("{what} exceeds the 2MiB record bound"),
        ));
    }
    Ok(())
}

impl SourceAuthorityStore {
    /// Install a collection's control state on the direct path.
    pub fn install_control(
        &self,
        principal: &str,
        command: &ControlInstallCommand,
    ) -> Result<ControlInstallDecision, Status> {
        self.direct()?;
        self.replay_control_install(principal, command)
    }

    /// The committed-log path of `install_control`: no direct-path check.
    pub(crate) fn replay_control_install(
        &self,
        principal: &str,
        command: &ControlInstallCommand,
    ) -> Result<ControlInstallDecision, Status> {
        let _exclusive = self.exclusive()?;
        self.guarded(|| {
            let decision = self.install_locked(principal, command)?;
            let tx = self.inner.database().begin_read().map_err(storage)?;
            self.read_policy(
                &tx,
                principal,
                command.key.as_ref().ok_or_else(|| missing("command key"))?,
            )?;
            Ok(decision)
        })
    }

    fn install_locked(
        &self,
        principal: &str,
        command: &ControlInstallCommand,
    ) -> Result<ControlInstallDecision, Status> {
        let mut tx = self.inner.database().begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate).map_err(storage)?;
        let decision;
        let mut retried = false;
        'apply: {
            let mut meta = tx.open_table(META).map_err(storage)?;
            let (mut header, policy) = read_headers(&meta, &self.inner.identity)?;
            validate_command(command, &self.inner.identity, &header.limits)?;
            let key = command.key.as_ref().expect("validated key");
            let state = command.state.as_ref().expect("validated state");
            contract::authorize(&policy, principal, key)?;
            let operation_key = contract::operation_key(principal, key, &command.command_id);
            let operation_bytes = operation_key.encode_to_vec();
            let request_sha256 = command_digest(command);
            if tx
                .open_table(DECISIONS)
                .map_err(storage)?
                .get(operation_bytes.as_slice())
                .map_err(storage)?
                .is_some()
                || tx
                    .open_table(capacity::CAPACITY_OPERATIONS)
                    .map_err(storage)?
                    .get(operation_bytes.as_slice())
                    .map_err(storage)?
                    .is_some()
            {
                return Err(Status::failed_precondition(
                    "command_id was already used by another source authority command",
                ));
            }
            let mut operations = tx.open_table(INSTALL_OPERATIONS).map_err(storage)?;
            if let Some(saved) = operations
                .get(operation_bytes.as_slice())
                .map_err(storage)?
            {
                let operation: ControlInstallOperation = contract::decode(saved.value())?;
                if operation.request_sha256 != request_sha256
                    || operation.command.as_ref() != Some(command)
                {
                    return Err(Status::failed_precondition(
                        "control install command_id was already used with different content",
                    ));
                }
                decision = operation
                    .decision
                    .ok_or_else(|| missing("retry decision"))?;
                retried = true;
                // Under Raft the entry is consumed: the applied position
                // advances for an exact retry too, with no other change.
                self.write_pending_applied(&mut meta)?;
                break 'apply;
            }
            if header
                .source
                .decision_count
                .saturating_add(retained_decisions(&meta)?)
                >= header.limits.max_decisions
            {
                return Err(Status::resource_exhausted(
                    "source authority decision capacity is full; retry history is never evicted",
                ));
            }
            let revision = header
                .source
                .control_revision
                .checked_add(1)
                .ok_or_else(|| {
                    Status::resource_exhausted("source authority control revision exhausted")
                })?;
            let mut result = ControlInstallDecision {
                format_version: 1,
                control_revision: header.source.control_revision,
                policy_revision: policy.revision,
                ..Default::default()
            };
            let mut added_bytes = 0usize;
            let transition = (|| -> Result<ControlStoreHeader, Status> {
                if command.expected_control_revision != header.source.control_revision {
                    return Err(reject(
                        Code::FailedPrecondition,
                        "source authority control revision differs",
                    ));
                }
                if command.expected_policy_revision != policy.revision {
                    return Err(reject(
                        Code::FailedPrecondition,
                        "source authority policy revision differs",
                    ));
                }
                if tx
                    .open_table(CONTROL)
                    .map_err(storage)?
                    .get(key.encode_to_vec().as_slice())
                    .map_err(storage)?
                    .is_some()
                {
                    return Err(reject(
                        Code::AlreadyExists,
                        "resource already holds installed control state",
                    ));
                }
                let rows = rows(key, state, revision)?;
                let new_bytes: usize = rows.iter().map(|(k, v)| k.len() + v.len()).sum();
                let bytes_left = header
                    .limits
                    .max_payload_bytes
                    .saturating_sub(header.source.payload_bytes);
                if new_bytes as u64 > bytes_left {
                    return Err(reject(
                        Code::ResourceExhausted,
                        format!(
                            "installed control state needs {new_bytes} bytes; {bytes_left} remain"
                        ),
                    ));
                }
                let delta = write_rows(&tx, &rows)?;
                added_bytes += new_bytes;
                result.receipt = Some(rows.receipt);
                result.control_revision = revision;
                Ok(delta)
            })();
            let delta = match transition {
                Ok(delta) => Some(delta),
                Err(error)
                    if matches!(
                        error.code(),
                        Code::FailedPrecondition | Code::AlreadyExists | Code::ResourceExhausted
                    ) =>
                {
                    // A recorded rejection: durable, deterministic, unchanged state.
                    result.code = error.code() as u32;
                    result.message = error.message().to_string();
                    None
                }
                Err(error) => return Err(error),
            };
            let operation = ControlInstallOperation {
                format_version: 1,
                request_sha256,
                command: Some(command.clone()),
                decision: Some(result.clone()),
            };
            let encoded = operation.encode_to_vec();
            if encoded.len() > contract::MAX_RECORD_BYTES {
                return Err(Status::resource_exhausted(
                    "control install decision record exceeds 2MiB",
                ));
            }
            added_bytes += operation_bytes.len() + encoded.len();
            header.source.payload_bytes =
                payload_change(header.source.payload_bytes, 0, added_bytes)?;
            if header.source.payload_bytes > header.limits.max_payload_bytes {
                return Err(Status::resource_exhausted(
                    "source authority payload capacity is full; retry history is never evicted",
                ));
            }
            let c = &mut header.control;
            c.install_decision_count = c
                .install_decision_count
                .checked_add(1)
                .ok_or_else(|| corrupt("install decision counter overflow"))?;
            if let Some(delta) = delta {
                header.source.control_revision = revision;
                c.control_count += delta.control_count;
                c.topology_count += delta.topology_count;
                c.node_count += delta.node_count;
                c.replica_count += delta.replica_count;
                c.action_count += delta.action_count;
                c.completed_count += delta.completed_count;
            }
            operations
                .insert(operation_bytes.as_slice(), encoded.as_slice())
                .map_err(storage)?;
            meta.insert("header", header.source.encode_to_vec().as_slice())
                .map_err(storage)?;
            self.write_pending_applied(&mut meta)?;
            meta.insert(CONTROL_HEADER, header.control.encode_to_vec().as_slice())
                .map_err(storage)?;
            decision = result;
        }
        if retried {
            self.finish_retry(tx)?;
            return Ok(decision);
        }
        #[cfg(any(test, feature = "fault-injection"))]
        self.inject(false)?;
        tx.commit().map_err(storage)?;
        self.publish_applied(decision.control_revision);
        #[cfg(any(test, feature = "fault-injection"))]
        self.inject(true)?;
        Ok(decision)
    }

    /// A stored install decision under this actor's key.
    pub fn control_install_decision(
        &self,
        principal: &str,
        key: &LogicalSourceOwner,
        command_id: &[u8],
    ) -> Result<ControlInstallDecision, Status> {
        self.guarded(|| {
            contract::operation_id(command_id)?;
            let tx = self.inner.database().begin_read().map_err(storage)?;
            self.read_policy(&tx, principal, key)?;
            let operations = tx.open_table(INSTALL_OPERATIONS).map_err(storage)?;
            let bytes = contract::operation_key(principal, key, command_id).encode_to_vec();
            let operation: ControlInstallOperation = contract::decode(
                operations
                    .get(bytes.as_slice())
                    .map_err(storage)?
                    .ok_or_else(|| Status::not_found("control install command has no decision"))?
                    .value(),
            )?;
            operation.decision.ok_or_else(|| missing("decision"))
        })
    }

    /// The immutable committed view of a resource's control state. Lease
    /// tokens are omitted. Requires current resource Admin.
    pub fn control_snapshot(
        &self,
        principal: &str,
        key: &LogicalSourceOwner,
    ) -> Result<ControlCollectionSnapshot, Status> {
        self.guarded(|| {
            contract::key(key, false)?;
            let tx = self.inner.database().begin_read().map_err(storage)?;
            let policy = self.read_policy(&tx, principal, key)?;
            let meta = tx.open_table(META).map_err(storage)?;
            let (header, _) = read_headers(&meta, &self.inner.identity)?;
            let control = tx.open_table(CONTROL).map_err(storage)?;
            let state: ControlCollectionState = contract::decode(
                control
                    .get(key.encode_to_vec().as_slice())
                    .map_err(storage)?
                    .ok_or_else(|| Status::not_found("resource has no installed control state"))?
                    .value(),
            )?;
            let topologies = tx.open_table(TOPOLOGIES).map_err(storage)?;
            let topology: ControlTopology = contract::decode(
                topologies
                    .get(
                        ControlTopologyKey {
                            key: Some(key.clone()),
                            generation: state.topology_generation,
                        }
                        .encode_to_vec()
                        .as_slice(),
                    )
                    .map_err(storage)?
                    .ok_or_else(|| corrupt("current topology row is missing"))?
                    .value(),
            )?;
            let prefix = resource_prefix(key);
            let end = prefix_end(&prefix).ok_or_else(|| corrupt("resource prefix overflow"))?;
            let range = |definition: TableDefinition<&[u8], &[u8]>| -> Result<RawRows, Status> {
                let mut out = Vec::new();
                for entry in tx
                    .open_table(definition)
                    .map_err(storage)?
                    .range(prefix.as_slice()..end.as_slice())
                    .map_err(storage)?
                {
                    let (k, v) = entry.map_err(storage)?;
                    out.push((k.value().to_vec(), v.value().to_vec()));
                }
                Ok(out)
            };
            let mut nodes = Vec::new();
            for (_, value) in range(NODES)? {
                let row: ControlNode = contract::decode(&value)?;
                let node = row.node.ok_or_else(|| missing("node"))?;
                nodes.push(ControlNodeSnapshot {
                    node_id: node.node_id,
                    addr: node.addr,
                    state: node.state,
                    expires_unix_ms: node.expires_unix_ms,
                    capacity: node.capacity,
                    residency: row.residency,
                });
            }
            let replicas = range(REPLICAS)?
                .iter()
                .map(|(_, v)| contract::decode::<ControlReplica>(v))
                .collect::<Result<Vec<_>, _>>()?;
            let mut actions = range(ACTIONS)?
                .iter()
                .map(|(_, v)| contract::decode::<ControlAction>(v))
                .collect::<Result<Vec<_>, _>>()?;
            actions.sort_by_key(|a| a.order);
            let completed_actions = range(COMPLETED)?
                .iter()
                .map(|(k, _)| contract::decode::<ControlActionKey>(k).map(|k| k.action_id))
                .collect::<Result<Vec<_>, _>>()?;
            let mut snapshot = ControlCollectionSnapshot {
                format_version: 1,
                authority: Some(self.inner.identity.clone()),
                key: Some(key.clone()),
                control_revision: header.source.control_revision,
                policy_revision: policy.revision,
                state: Some(state),
                topology: Some(topology),
                nodes,
                replicas,
                actions,
                completed_actions,
                digest: Vec::new(),
            };
            snapshot.digest = hash(SNAPSHOT_DOMAIN, &snapshot.encode_to_vec());
            Ok(snapshot)
        })
    }
}

/// Rows an install writes, each already bounded.
struct Rows {
    receipt: ControlInstallReceipt,
    control: (Vec<u8>, Vec<u8>),
    topologies: Vec<(Vec<u8>, Vec<u8>)>,
    nodes: Vec<(Vec<u8>, Vec<u8>)>,
    replicas: Vec<(Vec<u8>, Vec<u8>)>,
    actions: Vec<(Vec<u8>, Vec<u8>)>,
    completed: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Rows {
    fn iter(&self) -> impl Iterator<Item = &(Vec<u8>, Vec<u8>)> {
        std::iter::once(&self.control)
            .chain(&self.topologies)
            .chain(&self.nodes)
            .chain(&self.replicas)
            .chain(&self.actions)
            .chain(&self.completed)
    }
}

fn write_rows(tx: &redb::WriteTransaction, rows: &Rows) -> Result<ControlStoreHeader, Status> {
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

/// Lay out a validated state as bounded rows. Pure over its inputs.
fn rows(
    key: &LogicalSourceOwner,
    state: &ControlInstallState,
    revision: u64,
) -> Result<Rows, Status> {
    let current = state.topology.as_ref().expect("validated topology");
    let configuration = state
        .configuration
        .as_ref()
        .expect("validated configuration");
    let codes = match configuration.placement.as_ref() {
        Some(control_configuration::Placement::Tree(_)) => configuration.route_codes.clone(),
        _ => Vec::new(),
    };
    let mut topologies = Vec::with_capacity(1 + state.history.len());
    topologies.push(topology_row(
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
    )?);
    for (index, table) in state.history.iter().enumerate() {
        topologies.push(topology_row(
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
        )?);
    }
    let mut nodes = Vec::with_capacity(state.nodes.len());
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
        nodes.push((k, v));
    }
    let mut replicas = Vec::with_capacity(state.replicas.len());
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
        replicas.push((k, v));
    }
    let mut actions = Vec::with_capacity(state.actions.len());
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
        actions.push((k, v));
    }
    let completed = state
        .completed_actions
        .iter()
        .map(|id| {
            (
                ControlActionKey {
                    key: Some(key.clone()),
                    action_id: *id,
                }
                .encode_to_vec(),
                ControlCompletedAction { format_version: 1 }.encode_to_vec(),
            )
        })
        .collect::<Vec<_>>();
    let receipt = ControlInstallReceipt {
        format_version: 1,
        control_revision: revision,
        generation: current.generation,
        routes: current.routes.len() as u64,
        history: state.history.len() as u64,
        nodes: nodes.len() as u64,
        replicas: replicas.len() as u64,
        actions: actions.len() as u64,
        completed_actions: completed.len() as u64,
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
    Ok(Rows {
        receipt,
        control: (key.encode_to_vec(), control_value),
        topologies,
        nodes,
        replicas,
        actions,
        completed,
    })
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

pub(super) fn create_tables(tx: &redb::WriteTransaction) -> Result<(), Status> {
    for definition in [
        INSTALL_OPERATIONS,
        CONTROL,
        TOPOLOGIES,
        NODES,
        REPLICAS,
        ACTIONS,
        COMPLETED,
    ] {
        tx.open_table(definition).map_err(storage)?;
    }
    let mut meta = tx.open_table(META).map_err(storage)?;
    meta.insert(
        CONTROL_HEADER,
        ControlStoreHeader {
            format_version: STORE_FORMAT,
            ..Default::default()
        }
        .encode_to_vec()
        .as_slice(),
    )
    .map_err(storage)?;
    Ok(())
}
