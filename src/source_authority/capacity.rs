//! Capacity observations as committed state (docs/capacity-observations.md).
//!
//! The planner's frozen snapshot (`capacity_tiers::TierSnapshot`) is a pure
//! function of committed rows: the applied control state of the resource,
//! its capacity configuration, the registered reporter incarnations and the
//! retained observations. This module persists those rows, applies the
//! observation rules through Kimi's `ObservationStore` as the pure
//! transition, and transcribes one consistent read into a
//! `TierSnapshotInput`. It never repairs an input: `TierSnapshot::validated`
//! is the only validator of what it hands over.
use super::import::{hash, prefix_end, read_headers, resource_prefix};
use super::import::{CONTROL, NODES, REPLICAS, TOPOLOGIES};
use super::*;
use crate::capacity_tiers as tiers;
use crate::pb::storage::capacity_transition::Action as TransitionAction;
use crate::pb::storage::legacy_control_import_supplement::{
    Derived as DerivedChoice, Placement, Provider,
};
use std::collections::BTreeMap;

pub(super) const CAPACITY_HEADER: &str = "capacity";
pub(super) const CAPACITY_OPERATIONS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("capacity_configure_operations");
pub(super) const CAPACITY: TableDefinition<&[u8], &[u8]> = TableDefinition::new("capacity_state");
pub(super) const REPORTERS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("capacity_reporters");
pub(super) const OBSERVATIONS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("capacity_observations");
/// Format 3: the format-2 tables plus the four capacity tables.
pub(super) const FORMAT_3_TABLES: usize = 17;

/// Bounds a configuration may declare; they keep the per-command rebuild of
/// the observation store bounded.
pub const MAX_CAPACITY_RECORDS: u64 = 65_536;
pub const MAX_CAPACITY_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_CAPACITY_REPORTERS: u64 = 4096;
const MAX_COHORT_LENGTH_MS: u64 = 1 << 40;

const COMMAND_DOMAIN: &[u8] = b"protomolt.capacity-configure.command.v1\0";
const TREE_DOMAIN: &[u8] = b"protomolt.capacity-planner.placement-tree.v1\0";
const GEOMETRY_DOMAIN: &[u8] = b"protomolt.capacity-planner.provider-geometry.v1\0";

/// What a planning read needs beyond the committed rows: the instant, the
/// caller's bounds and the fragments to plan. Everything else is committed.
#[derive(Debug, Clone)]
pub struct PlanningContext {
    pub planning_instant_unix_ms: u64,
    pub max_moves: u64,
    pub max_observation_age_ms: u64,
    pub clock_skew_bound_ms: u64,
    pub fragment_requests: Vec<tiers::FragmentRequest>,
}

pub(super) fn create_tables(tx: &redb::WriteTransaction) -> Result<(), Status> {
    for definition in [CAPACITY_OPERATIONS, CAPACITY, REPORTERS, OBSERVATIONS] {
        tx.open_table(definition).map_err(storage)?;
    }
    let mut meta = tx.open_table(META).map_err(storage)?;
    meta.insert(
        CAPACITY_HEADER,
        CapacityStoreHeader {
            format_version: 1,
            ..Default::default()
        }
        .encode_to_vec()
        .as_slice(),
    )
    .map_err(storage)?;
    Ok(())
}

pub(super) fn header(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<CapacityStoreHeader, Status> {
    let header: CapacityStoreHeader = contract::decode(
        meta.get(CAPACITY_HEADER)
            .map_err(storage)?
            .ok_or_else(|| missing("capacity header"))?
            .value(),
    )?;
    if header.format_version != 1 {
        return Err(corrupt("unsupported capacity header format"));
    }
    Ok(header)
}

fn command_digest(command: &CapacityConfigureCommand) -> Vec<u8> {
    hash(COMMAND_DOMAIN, &command.encode_to_vec())
}

fn reject(code: Code, message: impl Into<String>) -> Status {
    Status::new(code, message.into())
}

fn refusal(message: String) -> Status {
    // Kimi's refusals name the route, the field and both values; the bound
    // refusals are capacity, everything else is a precondition.
    if message.contains("the bound of") || message.contains("exceed its bound") {
        Status::resource_exhausted(message)
    } else {
        Status::failed_precondition(message)
    }
}

fn hex16(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(out, "{byte:02x}").expect("writing to a String cannot fail");
    }
    out
}

fn parse_hex32(text: &str) -> Option<[u8; 32]> {
    let bytes = text.as_bytes();
    if bytes.len() != 64 {
        return None;
    }
    let nibble = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    };
    let mut out = [0u8; 32];
    for (i, pair) in bytes.chunks(2).enumerate() {
        out[i] = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Some(out)
}

fn fixed<const N: usize>(bytes: &[u8], what: &str) -> Result<[u8; N], Status> {
    bytes
        .try_into()
        .map_err(|_| Status::invalid_argument(format!("{what} must be {N} bytes")))
}

pub(super) fn tier_policy(policy: &CapacityTierPolicy) -> Result<tiers::TierPolicy, Status> {
    if policy.format_version != 1 {
        return Err(Status::invalid_argument(
            "capacity tier policy requires format 1",
        ));
    }
    let mut tiers = Vec::with_capacity(policy.tiers.len());
    for spec in &policy.tiers {
        let residency = match CapacityResidency::try_from(spec.residency) {
            Ok(CapacityResidency::Server) => tiers::NodeResidency::Server,
            Ok(CapacityResidency::Device) => tiers::NodeResidency::Device,
            Ok(CapacityResidency::Unspecified) => tiers::NodeResidency::Unspecified,
            Err(_) => {
                return Err(Status::invalid_argument(format!(
                    "tier {:?} names an unknown residency",
                    spec.name
                )))
            }
        };
        tiers.push(tiers::CapacityTier {
            name: spec.name.clone(),
            residency,
            min_replicas: spec.min_replicas,
            scans_per_byte_nanos_lo: spec.scans_per_byte_nanos_lo,
            scans_per_byte_nanos_hi: spec.scans_per_byte_nanos_hi,
            max_seconds_since_scan: spec.max_seconds_since_scan,
        });
    }
    let policy = tiers::TierPolicy { tiers };
    policy.validate().map_err(Status::invalid_argument)?;
    Ok(policy)
}

pub(super) fn validate_configuration(
    configuration: &CapacityConfiguration,
) -> Result<tiers::TierPolicy, Status> {
    if configuration.format_version != 1 {
        return Err(Status::invalid_argument(
            "capacity configuration requires format 1",
        ));
    }
    if configuration.cohort_length_ms == 0 || configuration.cohort_length_ms > MAX_COHORT_LENGTH_MS
    {
        return Err(Status::invalid_argument(
            "capacity configuration cohort length must be 1..2^40 ms",
        ));
    }
    if configuration.max_total_records == 0
        || configuration.max_total_records > MAX_CAPACITY_RECORDS
        || configuration.max_total_bytes == 0
        || configuration.max_total_bytes > MAX_CAPACITY_BYTES
        || configuration.max_registered_nodes == 0
        || configuration.max_registered_nodes > MAX_CAPACITY_REPORTERS
    {
        return Err(Status::invalid_argument(format!(
            "capacity configuration bounds must be records 1..{MAX_CAPACITY_RECORDS}, bytes 1..{MAX_CAPACITY_BYTES}, reporters 1..{MAX_CAPACITY_REPORTERS}"
        )));
    }
    tier_policy(
        configuration.policy.as_ref().ok_or_else(|| {
            Status::invalid_argument("capacity configuration requires a tier policy")
        })?,
    )
}

fn validate_configure(
    command: &CapacityConfigureCommand,
    identity: &SourceAuthorityIdentity,
    limits: &SourceAuthorityLimits,
) -> Result<(), Status> {
    if command.encoded_len() > limits.max_command_bytes as usize {
        return Err(Status::resource_exhausted(
            "capacity configure command exceeds max_command_bytes",
        ));
    }
    if command.format_version != 1 {
        return Err(Status::invalid_argument(
            "capacity configure command requires format 1",
        ));
    }
    if command.authority.as_ref() != Some(identity) {
        return Err(Status::failed_precondition(
            "capacity command authority group or incarnation differs",
        ));
    }
    let key = command
        .key
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("capacity command resource key is required"))?;
    contract::key(key, false)?;
    if !key.owner_id.is_empty() {
        return Err(Status::invalid_argument(
            "capacity commands name a collection, not an owner",
        ));
    }
    contract::operation_id(&command.command_id)?;
    validate_configuration(
        command.configuration.as_ref().ok_or_else(|| {
            Status::invalid_argument("capacity configure requires a configuration")
        })?,
    )?;
    Ok(())
}

pub(super) fn validate_stored_configure(
    command: &CapacityConfigureCommand,
    identity: &SourceAuthorityIdentity,
    limits: &SourceAuthorityLimits,
) -> Result<(), Status> {
    validate_configure(command, identity, limits)
}

fn validate_transition(
    transition: &CapacityTransition,
    identity: &SourceAuthorityIdentity,
    limits: &SourceAuthorityLimits,
) -> Result<(), Status> {
    if transition.encoded_len() > limits.max_command_bytes as usize {
        return Err(Status::resource_exhausted(
            "capacity transition exceeds max_command_bytes",
        ));
    }
    if transition.format_version != 1 {
        return Err(Status::invalid_argument(
            "capacity transition requires format 1",
        ));
    }
    if transition.authority.as_ref() != Some(identity) {
        return Err(Status::failed_precondition(
            "capacity transition authority group or incarnation differs",
        ));
    }
    let key = transition
        .key
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("capacity transition resource key is required"))?;
    contract::key(key, false)?;
    if !key.owner_id.is_empty() {
        return Err(Status::invalid_argument(
            "capacity transitions name a collection, not an owner",
        ));
    }
    match transition.action.as_ref() {
        None => Err(Status::invalid_argument(
            "capacity transition action is missing or unsupported",
        )),
        Some(TransitionAction::Register(register)) => {
            if register.node_id.is_empty() || register.node_id.len() > 1024 {
                return Err(Status::invalid_argument(
                    "reporter node_id must be 1..1024 bytes",
                ));
            }
            fixed::<16>(&register.process_incarnation, "process_incarnation")?;
            Ok(())
        }
        Some(TransitionAction::Report(report)) => {
            observation_from_proto(report)?;
            Ok(())
        }
        Some(TransitionAction::Expire(_)) => Ok(()),
    }
}

pub(super) fn observation_from_proto(
    value: &CapacityObservation,
) -> Result<tiers::PartitionObservation, Status> {
    if value.format_version != 1 {
        return Err(Status::invalid_argument(
            "capacity observation requires format 1",
        ));
    }
    let partition = value
        .partition
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("capacity observation requires a partition"))?;
    let shard = value
        .shard
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("capacity observation requires a shard"))?;
    if value.node_id.is_empty()
        || value.node_id.len() > 1024
        || partition.column.is_empty()
        || partition.column.len() > 1024
        || value.leaf.len() > 1024
        || shard.shard.is_empty()
        || shard.shard.len() > 1024
    {
        return Err(Status::invalid_argument(
            "capacity observation node, column, leaf and shard names must be bounded and present",
        ));
    }
    Ok(tiers::PartitionObservation {
        partition: tiers::PartitionIdentity {
            workspace: partition.workspace.clone(),
            collection: partition.collection.clone(),
            derived_fingerprint: fixed::<32>(
                &partition.derived_fingerprint,
                "derived_fingerprint",
            )?,
            column: partition.column.clone(),
            bucket: partition.bucket,
        },
        reporter: tiers::ReporterIdentity {
            node_id: value.node_id.clone(),
            process_incarnation: fixed::<16>(&value.process_incarnation, "process_incarnation")?,
        },
        shard: tiers::ShardRef {
            shard: shard.shard.clone(),
            source_generation: shard.source_generation,
            storage_incarnation: fixed::<16>(&shard.storage_incarnation, "storage_incarnation")?,
            ownership_epoch: shard.ownership_epoch,
        },
        topology_generation: value.topology_generation,
        leaf: value.leaf.clone(),
        rows: value.rows,
        resident_bytes: value.resident_bytes,
        scans_observed: value.scans_observed,
        scan_bytes: value.scan_bytes,
        queue_wait_p50_us: value.queue_wait_p50_us,
        queue_wait_p99_us: value.queue_wait_p99_us,
        samples: value.samples,
        last_scanned_unix_ms: value.last_scanned_unix_ms,
        window_start_unix_ms: value.window_start_unix_ms,
        window_end_unix_ms: value.window_end_unix_ms,
    })
}

pub(super) fn observation_key(key: &LogicalSourceOwner, value: &CapacityObservation) -> Vec<u8> {
    let partition = value.partition.clone().unwrap_or_default();
    let shard = value.shard.clone().unwrap_or_default();
    CapacityObservationKey {
        key: Some(key.clone()),
        derived_fingerprint: partition.derived_fingerprint,
        column: partition.column,
        bucket: partition.bucket,
        topology_generation: value.topology_generation,
        leaf: value.leaf.clone(),
        shard: shard.shard,
        source_generation: shard.source_generation,
        ownership_epoch: shard.ownership_epoch,
        node_id: value.node_id.clone(),
        process_incarnation: value.process_incarnation.clone(),
        storage_incarnation: shard.storage_incarnation,
    }
    .encode_to_vec()
}

fn reporter_key(key: &LogicalSourceOwner, node_id: &str) -> Vec<u8> {
    CapacityReporterKey {
        key: Some(key.clone()),
        node_id: node_id.to_string(),
    }
    .encode_to_vec()
}

fn state_key(key: &LogicalSourceOwner) -> Vec<u8> {
    key.encode_to_vec()
}

/// The committed control rows of one resource, read in one transaction.
pub(super) struct Committed {
    pub(super) state: ControlCollectionState,
    pub(super) topology: ControlTopology,
    pub(super) nodes: BTreeMap<String, ControlNode>,
    pub(super) replicas: Vec<ControlReplica>,
}

pub(super) fn load_committed(
    control: &impl ReadableTable<&'static [u8], &'static [u8]>,
    topologies: &impl ReadableTable<&'static [u8], &'static [u8]>,
    nodes: &impl ReadableTable<&'static [u8], &'static [u8]>,
    replicas: &impl ReadableTable<&'static [u8], &'static [u8]>,
    key: &LogicalSourceOwner,
) -> Result<Option<Committed>, Status> {
    let Some(state) = control
        .get(key.encode_to_vec().as_slice())
        .map_err(storage)?
    else {
        return Ok(None);
    };
    let state: ControlCollectionState = contract::decode(state.value())?;
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
    let mut node_rows = BTreeMap::new();
    for entry in nodes
        .range(prefix.as_slice()..end.as_slice())
        .map_err(storage)?
    {
        let (k, v) = entry.map_err(storage)?;
        let node_key: ControlNodeKey = contract::decode(k.value())?;
        let row: ControlNode = contract::decode(v.value())?;
        node_rows.insert(node_key.node_id, row);
    }
    let mut replica_rows = Vec::new();
    for entry in replicas
        .range(prefix.as_slice()..end.as_slice())
        .map_err(storage)?
    {
        let (_, v) = entry.map_err(storage)?;
        replica_rows.push(contract::decode::<ControlReplica>(v.value())?);
    }
    Ok(Some(Committed {
        state,
        topology,
        nodes: node_rows,
        replicas: replica_rows,
    }))
}

/// The committed facts the planner binds to, derived without repair.
pub(super) struct Derived {
    pub(super) view: tiers::CommittedView,
    pub(super) derived_fingerprint: [u8; 32],
    pub(super) placement_tree_digest: [u8; 32],
    pub(super) provider_geometry_digest: [u8; 32],
}

pub(super) fn derive(committed: &Committed, key: &LogicalSourceOwner) -> Result<Derived, Status> {
    let configuration = committed
        .state
        .configuration
        .as_ref()
        .ok_or_else(|| corrupt("applied control state has no configuration"))?;
    let derived_fingerprint = match configuration.derived.as_ref() {
        Some(DerivedChoice::Declaration(_)) => parse_hex32(&configuration.derived_fingerprint)
            .ok_or_else(|| corrupt("committed derived fingerprint is not 64 hex digits"))?,
        Some(DerivedChoice::NoDerived(true)) => [0u8; 32],
        _ => return Err(corrupt("committed derived configuration is not explicit")),
    };
    let (placement, placement_tree_digest) = match configuration.placement.as_ref() {
        Some(Placement::Tree(tree)) => {
            let placement = crate::placement::Placement::validate(
                &crate::placement::PlacementTreeConfig::from_proto(tree),
            )
            .map_err(|error| corrupt(format!("committed placement tree: {error}")))?;
            let digest: [u8; 32] = hash(TREE_DOMAIN, &tree.encode_to_vec())
                .try_into()
                .expect("sha256 is 32 bytes");
            (Some(placement), digest)
        }
        Some(Placement::NoPlacement(true)) => (None, [0u8; 32]),
        _ => return Err(corrupt("committed placement configuration is not explicit")),
    };
    let provider_geometry_digest = match configuration.provider.as_ref() {
        Some(Provider::Geometry(geometry)) => hash(GEOMETRY_DOMAIN, &geometry.encode_to_vec())
            .try_into()
            .expect("sha256 is 32 bytes"),
        Some(Provider::NoProvider(true)) => [0u8; 32],
        _ => return Err(corrupt("committed provider configuration is not explicit")),
    };

    // Shards: every committed replica row names its shard; the primary's
    // generation is the shard's source generation. A shard with no primary
    // or several is an impossible committed state, reported by name.
    let mut shards: BTreeMap<String, tiers::CommittedShard> = BTreeMap::new();
    let mut by_shard: BTreeMap<String, Vec<&LegacyControlReplica>> = BTreeMap::new();
    for row in &committed.replicas {
        let replica = row
            .replica
            .as_ref()
            .ok_or_else(|| corrupt("replica row without a replica"))?;
        by_shard
            .entry(replica.shard_id.clone())
            .or_default()
            .push(replica);
    }
    for (shard_id, replicas) in by_shard {
        let primaries: Vec<&&LegacyControlReplica> = replicas
            .iter()
            .filter(|r| r.role == crate::pb::ShardReplicaRole::Primary as i32)
            .collect();
        let [primary] = primaries.as_slice() else {
            return Err(Status::failed_precondition(format!(
                "planner input: shard {shard_id} has {} primary replicas in committed state; its source generation cannot be named",
                primaries.len()
            )));
        };
        let mut pool: Vec<String> = replicas.iter().map(|r| r.node_id.clone()).collect();
        pool.sort();
        pool.dedup();
        let mut copies: Vec<tiers::CommittedCopy> = replicas
            .iter()
            .map(|r| tiers::CommittedCopy {
                node_id: r.node_id.clone(),
                // Legacy replicas carry no storage incarnation or coverage
                // evidence: partial evidence, never a complete replica.
                storage_incarnation: [0u8; 16],
                failure_domain: committed
                    .nodes
                    .get(&r.node_id)
                    .and_then(|n| n.node.as_ref())
                    .and_then(|n| n.capacity.as_ref())
                    .map(|c| c.failure_domain.clone())
                    .unwrap_or_default(),
                complete: false,
                coverage_digest: [0u8; 32],
            })
            .collect();
        copies.sort_by(|a, b| {
            (&a.node_id, &a.storage_incarnation).cmp(&(&b.node_id, &b.storage_incarnation))
        });
        copies.dedup();
        let mut leaves = BTreeMap::new();
        if let Some(placement) = placement.as_ref() {
            // The current topology's route with the primary's hash range
            // names the leaf through its committed placement code.
            for (index, route) in committed.topology.routes.iter().enumerate() {
                if route.hash_lo != primary.hash_lo || route.hash_hi != primary.hash_hi {
                    continue;
                }
                let Some(code) = committed.topology.codes.get(index) else {
                    continue;
                };
                if !code.has_placement {
                    continue;
                }
                let code = i64::try_from(code.placement)
                    .map_err(|_| corrupt("committed placement code exceeds i64"))?;
                let leaf = placement
                    .leaf_by_code(code)
                    .ok_or_else(|| corrupt("committed placement code names no leaf"))?;
                leaves.insert(
                    leaf.name.clone(),
                    tiers::LeafCoverage {
                        pool: pool.clone(),
                        owner_node: primary.node_id.clone(),
                        copies: copies.clone(),
                    },
                );
            }
        }
        shards.insert(
            shard_id,
            tiers::CommittedShard {
                source_generation: primary.generation,
                // No committed ownership transition has happened since the
                // import; the counter starts here and the map feed publishes it.
                ownership_epoch: 0,
                leaves,
            },
        );
    }
    Ok(Derived {
        view: tiers::CommittedView {
            workspace: key.workspace.clone(),
            collection: key.collection.clone(),
            derived_fingerprint,
            topology_generation: committed.state.topology_generation,
            shards,
        },
        derived_fingerprint,
        placement_tree_digest,
        provider_geometry_digest,
    })
}

fn node_records(
    committed: &Committed,
    planning_instant_unix_ms: u64,
) -> Result<BTreeMap<String, tiers::NodeRecord>, Status> {
    let mut out = BTreeMap::new();
    for (node_id, row) in &committed.nodes {
        let node = row.node.as_ref().ok_or_else(|| missing("node"))?;
        let residency = match SourceResidency::try_from(row.residency) {
            Ok(SourceResidency::Server) => tiers::NodeResidency::Server,
            Ok(SourceResidency::DeviceLocal) => tiers::NodeResidency::Device,
            Ok(SourceResidency::Unspecified) => tiers::NodeResidency::Unspecified,
            Err(_) => return Err(corrupt("committed node residency is unknown")),
        };
        // Eligibility is a transcription of committed facts at the planning
        // instant: an active, server-resident node whose committed lease has
        // not expired. Imported leases are observations; this grants nothing.
        let eligible = node.state == crate::pb::ClusterNodeState::Active as i32
            && residency == tiers::NodeResidency::Server
            && node.expires_unix_ms > planning_instant_unix_ms;
        let capacity = node.capacity.clone().unwrap_or_default();
        out.insert(
            node_id.clone(),
            tiers::NodeRecord {
                residency,
                eligible,
                failure_domain: capacity.failure_domain,
                capacity: tiers::NodeCapacity {
                    total_bytes: capacity.disk_bytes,
                    resident_bytes: capacity.used_disk_bytes,
                },
            },
        );
    }
    Ok(out)
}

/// The retained capacity rows of one resource.
pub(super) struct Rows {
    pub(super) reporters: Vec<(String, [u8; 16])>,
    pub(super) observations: Vec<(Vec<u8>, CapacityObservation)>,
}

pub(super) fn load_rows(
    reporters: &impl ReadableTable<&'static [u8], &'static [u8]>,
    observations: &impl ReadableTable<&'static [u8], &'static [u8]>,
    key: &LogicalSourceOwner,
) -> Result<Rows, Status> {
    let prefix = resource_prefix(key);
    let end = prefix_end(&prefix).ok_or_else(|| corrupt("resource prefix overflow"))?;
    let mut reporter_rows = Vec::new();
    for entry in reporters
        .range(prefix.as_slice()..end.as_slice())
        .map_err(storage)?
    {
        let (k, v) = entry.map_err(storage)?;
        let reporter_key: CapacityReporterKey = contract::decode(k.value())?;
        let reporter: CapacityReporter = contract::decode(v.value())?;
        if reporter.format_version != 1 {
            return Err(corrupt("unsupported capacity reporter format"));
        }
        let incarnation: [u8; 16] = reporter
            .process_incarnation
            .as_slice()
            .try_into()
            .map_err(|_| corrupt("stored reporter incarnation is not 16 bytes"))?;
        reporter_rows.push((reporter_key.node_id, incarnation));
    }
    let mut observation_rows = Vec::new();
    for entry in observations
        .range(prefix.as_slice()..end.as_slice())
        .map_err(storage)?
    {
        let (k, v) = entry.map_err(storage)?;
        let value: CapacityObservation = contract::decode(v.value())?;
        if observation_key(key, &value) != k.value() {
            return Err(corrupt("stored observation key differs from its value"));
        }
        observation_rows.push((k.value().to_vec(), value));
    }
    Ok(Rows {
        reporters: reporter_rows,
        observations: observation_rows,
    })
}

/// Rebuild Kimi's store from the committed rows: every retained row must
/// land again under the same rules, or the rows disagree with the committed
/// view and the open refuses.
pub(super) fn rebuild(
    configuration: &CapacityConfiguration,
    view: tiers::CommittedView,
    rows: &Rows,
) -> Result<tiers::ObservationStore, Status> {
    let mut store = tiers::ObservationStore::new(
        configuration.cohort_length_ms,
        configuration.cohort_phase_unix_ms,
        configuration.max_total_records as usize,
        configuration.max_total_bytes as usize,
        configuration.max_registered_nodes as usize,
        view,
    )
    .map_err(corrupt)?;
    for (node_id, incarnation) in &rows.reporters {
        store
            .register_incarnation(node_id, *incarnation)
            .map_err(|error| corrupt(format!("stored reporter: {error}")))?;
    }
    for (_, value) in &rows.observations {
        let observation = observation_from_proto(value)
            .map_err(|error| corrupt(format!("stored observation: {}", error.message())))?;
        match store.ingest(observation) {
            Ok(tiers::IngestOutcome::Landed) => {}
            Ok(other) => {
                return Err(corrupt(format!(
                    "stored observation did not land on rebuild: {other:?}"
                )))
            }
            Err(error) => {
                return Err(corrupt(format!(
                    "stored observation no longer validates: {error}"
                )))
            }
        }
    }
    Ok(store)
}

impl SourceAuthorityStore {
    /// Commit the planner configuration of one resource: a control command
    /// with a retained decision, current Admin, revision CAS. The cohort
    /// cannot change while observations are retained; bounds cannot drop
    /// below what is held.
    pub fn configure_capacity(
        &self,
        principal: &str,
        command: &CapacityConfigureCommand,
    ) -> Result<CapacityConfigureDecision, Status> {
        let _exclusive = self.exclusive()?;
        self.guarded(|| {
            let decision = self.configure_locked(principal, command)?;
            let tx = self.inner.database.begin_read().map_err(storage)?;
            self.read_policy(
                &tx,
                principal,
                command.key.as_ref().ok_or_else(|| missing("command key"))?,
            )?;
            Ok(decision)
        })
    }

    fn configure_locked(
        &self,
        principal: &str,
        command: &CapacityConfigureCommand,
    ) -> Result<CapacityConfigureDecision, Status> {
        let mut tx = self.inner.database.begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate).map_err(storage)?;
        let decision;
        {
            let mut meta = tx.open_table(META).map_err(storage)?;
            let (mut header, policy) = read_headers(&meta, &self.inner.identity)?;
            let mut capacity_header = header_of(&meta)?;
            validate_configure(command, &self.inner.identity, &header.limits)?;
            let key = command.key.as_ref().expect("validated key");
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
                    .open_table(import::IMPORT_OPERATIONS)
                    .map_err(storage)?
                    .get(operation_bytes.as_slice())
                    .map_err(storage)?
                    .is_some()
            {
                return Err(Status::failed_precondition(
                    "command_id was already used by another source authority command",
                ));
            }
            let mut operations = tx.open_table(CAPACITY_OPERATIONS).map_err(storage)?;
            if let Some(saved) = operations
                .get(operation_bytes.as_slice())
                .map_err(storage)?
            {
                let operation: CapacityConfigureOperation = contract::decode(saved.value())?;
                if operation.request_sha256 != request_sha256
                    || operation.command.as_ref() != Some(command)
                {
                    return Err(Status::failed_precondition(
                        "capacity command_id was already used with different content",
                    ));
                }
                return operation.decision.ok_or_else(|| missing("retry decision"));
            }
            let (reserved_bytes, reserved_decisions, retained) = import::reserved(&meta)?;
            if header
                .source
                .decision_count
                .saturating_add(retained)
                .saturating_add(reserved_decisions)
                >= header.limits.max_decisions
            {
                return Err(Status::resource_exhausted(
                    "source authority decision capacity is full; retry history is never evicted",
                ));
            }
            let mut states = tx.open_table(CAPACITY).map_err(storage)?;
            let state_bytes = state_key(key);
            let existing: Option<CapacityState> = states
                .get(state_bytes.as_slice())
                .map_err(storage)?
                .map(|v| contract::decode(v.value()))
                .transpose()?;
            let revision = header
                .source
                .control_revision
                .checked_add(1)
                .ok_or_else(|| {
                    Status::resource_exhausted("source authority control revision exhausted")
                })?;
            let mut result = CapacityConfigureDecision {
                format_version: 1,
                control_revision: header.source.control_revision,
                policy_revision: policy.revision,
                observation_epoch: existing.as_ref().map_or(0, |s| s.observation_epoch),
                ..Default::default()
            };
            let configuration = command
                .configuration
                .clone()
                .expect("validated configuration");
            let transition = (|| -> Result<CapacityState, Status> {
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
                    .is_none()
                {
                    return Err(reject(
                        Code::FailedPrecondition,
                        "resource has no applied control state to plan over",
                    ));
                }
                let mut next = existing.clone().unwrap_or(CapacityState {
                    format_version: 1,
                    key: Some(key.clone()),
                    ..Default::default()
                });
                if let Some(current) = existing.as_ref() {
                    let held = current
                        .configuration
                        .as_ref()
                        .ok_or_else(|| missing("configuration"))?;
                    if current.observation_count > 0
                        && (held.cohort_length_ms != configuration.cohort_length_ms
                            || held.cohort_phase_unix_ms != configuration.cohort_phase_unix_ms)
                    {
                        return Err(reject(
                            Code::FailedPrecondition,
                            "cohort cannot change while observations are retained; commit an expiry first",
                        ));
                    }
                    if current.observation_count > configuration.max_total_records
                        || current.reporter_count > configuration.max_registered_nodes
                    {
                        return Err(reject(
                            Code::FailedPrecondition,
                            "configuration bounds are below the retained reporters or observations",
                        ));
                    }
                }
                next.configuration = Some(configuration);
                next.configured_control_revision = revision;
                Ok(next)
            })();
            let mut state_change = None;
            match transition {
                Ok(next) => {
                    result.control_revision = revision;
                    state_change = Some(next);
                }
                Err(error) => {
                    result.code = error.code() as u32;
                    result.message = error.message().to_string();
                }
            }
            decision = result;
            let operation = CapacityConfigureOperation {
                format_version: 1,
                request_sha256,
                command: Some(command.clone()),
                decision: Some(decision.clone()),
            };
            let encoded = operation.encode_to_vec();
            if encoded.len() > contract::MAX_RECORD_BYTES {
                return Err(Status::resource_exhausted(
                    "capacity decision record exceeds 2MiB",
                ));
            }
            header.source.payload_bytes = payload_change(
                header.source.payload_bytes,
                0,
                operation_bytes.len() + encoded.len(),
            )?;
            capacity_header.configure_decision_count = capacity_header
                .configure_decision_count
                .checked_add(1)
                .ok_or_else(|| corrupt("capacity decision counter overflow"))?;
            if let Some(next) = state_change {
                let bytes = next.encode_to_vec();
                let old = states
                    .get(state_bytes.as_slice())
                    .map_err(storage)?
                    .map_or(0, |v| v.value().len());
                if old == 0 {
                    capacity_header.state_count = capacity_header
                        .state_count
                        .checked_add(1)
                        .ok_or_else(|| corrupt("capacity state counter overflow"))?;
                    header.source.payload_bytes =
                        payload_change(header.source.payload_bytes, 0, state_bytes.len())?;
                }
                header.source.payload_bytes =
                    payload_change(header.source.payload_bytes, old, bytes.len())?;
                states
                    .insert(state_bytes.as_slice(), bytes.as_slice())
                    .map_err(storage)?;
                header.source.control_revision = revision;
            }
            if header.source.payload_bytes.saturating_add(reserved_bytes)
                > header.limits.max_payload_bytes
            {
                return Err(Status::resource_exhausted(
                    "source authority payload capacity is full; retry history is never evicted",
                ));
            }
            operations
                .insert(operation_bytes.as_slice(), encoded.as_slice())
                .map_err(storage)?;
            meta.insert("header", header.source.encode_to_vec().as_slice())
                .map_err(storage)?;
            meta.insert(CAPACITY_HEADER, capacity_header.encode_to_vec().as_slice())
                .map_err(storage)?;
        }
        #[cfg(test)]
        self.inject(false)?;
        tx.commit().map_err(storage)?;
        self.inner.revisions.send_replace(decision.policy_revision);
        #[cfg(test)]
        self.inject(true)?;
        Ok(decision)
    }

    /// Apply one observation transition under the committed configuration
    /// and view: register a reporter incarnation, land a report, or commit
    /// an expiry. Idempotent by content; refusals are returned, not stored.
    pub fn capacity_transition(
        &self,
        principal: &str,
        transition: &CapacityTransition,
    ) -> Result<CapacityTransitionReceipt, Status> {
        let _exclusive = self.exclusive()?;
        self.guarded(|| self.transition_locked(principal, transition))
    }

    fn transition_locked(
        &self,
        principal: &str,
        transition: &CapacityTransition,
    ) -> Result<CapacityTransitionReceipt, Status> {
        let mut tx = self.inner.database.begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate).map_err(storage)?;
        let receipt;
        let changed;
        {
            let mut meta = tx.open_table(META).map_err(storage)?;
            let (mut header, policy) = read_headers(&meta, &self.inner.identity)?;
            let mut capacity_header = header_of(&meta)?;
            validate_transition(transition, &self.inner.identity, &header.limits)?;
            let key = transition.key.as_ref().expect("validated key");
            contract::authorize(&policy, principal, key)?;
            let mut states = tx.open_table(CAPACITY).map_err(storage)?;
            let state_bytes = state_key(key);
            let mut state: CapacityState = contract::decode(
                states
                    .get(state_bytes.as_slice())
                    .map_err(storage)?
                    .ok_or_else(|| {
                        Status::failed_precondition(
                            "resource has no committed capacity configuration",
                        )
                    })?
                    .value(),
            )?;
            let configuration = state
                .configuration
                .clone()
                .ok_or_else(|| missing("capacity configuration"))?;
            let committed = {
                let control = tx.open_table(CONTROL).map_err(storage)?;
                let topologies = tx.open_table(TOPOLOGIES).map_err(storage)?;
                let nodes = tx.open_table(NODES).map_err(storage)?;
                let replicas = tx.open_table(REPLICAS).map_err(storage)?;
                load_committed(&control, &topologies, &nodes, &replicas, key)?
                    .ok_or_else(|| corrupt("configured resource has no applied control state"))?
            };
            let derived = derive(&committed, key)?;
            let mut reporters = tx.open_table(REPORTERS).map_err(storage)?;
            let mut observations = tx.open_table(OBSERVATIONS).map_err(storage)?;
            let rows = load_rows(&reporters, &observations, key)?;
            let mut store = rebuild(&configuration, derived.view, &rows)?;
            let before = store.epoch();
            let mut removed_bytes = 0usize;
            let mut added_bytes = 0usize;
            let mut dropped = 0u64;
            let mut outcome = CapacityTransitionOutcome::Unspecified;
            match transition.action.as_ref().expect("validated action") {
                TransitionAction::Register(register) => {
                    let incarnation: [u8; 16] = register
                        .process_incarnation
                        .as_slice()
                        .try_into()
                        .expect("validated incarnation");
                    store
                        .register_incarnation(&register.node_id, incarnation)
                        .map_err(refusal)?;
                    let reporter_bytes = reporter_key(key, &register.node_id);
                    let value = CapacityReporter {
                        format_version: 1,
                        process_incarnation: incarnation.to_vec(),
                    }
                    .encode_to_vec();
                    let old = reporters
                        .get(reporter_bytes.as_slice())
                        .map_err(storage)?
                        .map(|v| v.value().to_vec());
                    if old.as_deref() != Some(value.as_slice()) {
                        if old.is_none() {
                            added_bytes += reporter_bytes.len();
                            state.reporter_count += 1;
                            capacity_header.reporter_count = capacity_header
                                .reporter_count
                                .checked_add(1)
                                .ok_or_else(|| corrupt("reporter counter overflow"))?;
                        }
                        removed_bytes += old.map_or(0, |v| v.len());
                        added_bytes += value.len();
                        reporters
                            .insert(reporter_bytes.as_slice(), value.as_slice())
                            .map_err(storage)?;
                        for (row_key, row) in &rows.observations {
                            if row.node_id == register.node_id
                                && row.process_incarnation != incarnation
                            {
                                observations.remove(row_key.as_slice()).map_err(storage)?;
                                removed_bytes += row_key.len() + row.encoded_len();
                                dropped += 1;
                            }
                        }
                    }
                }
                TransitionAction::Report(report) => {
                    let observation = observation_from_proto(report)?;
                    match store.ingest(observation).map_err(refusal)? {
                        tiers::IngestOutcome::Landed => {
                            outcome = CapacityTransitionOutcome::Landed;
                            let row_key = observation_key(key, report);
                            let value = report.encode_to_vec();
                            let old = observations
                                .get(row_key.as_slice())
                                .map_err(storage)?
                                .map(|v| v.value().len());
                            if old.is_none() {
                                added_bytes += row_key.len();
                                state.observation_count += 1;
                                capacity_header.observation_count = capacity_header
                                    .observation_count
                                    .checked_add(1)
                                    .ok_or_else(|| corrupt("observation counter overflow"))?;
                            }
                            removed_bytes += old.unwrap_or(0);
                            added_bytes += value.len();
                            observations
                                .insert(row_key.as_slice(), value.as_slice())
                                .map_err(storage)?;
                        }
                        tiers::IngestOutcome::Unchanged => {
                            outcome = CapacityTransitionOutcome::Unchanged
                        }
                        tiers::IngestOutcome::Superseded => {
                            outcome = CapacityTransitionOutcome::Superseded
                        }
                    }
                }
                TransitionAction::Expire(expire) => {
                    store
                        .commit_expiry(expire.now_unix_ms, expire.max_age_ms)
                        .map_err(refusal)?;
                    for (row_key, row) in &rows.observations {
                        if expire.now_unix_ms.saturating_sub(row.window_end_unix_ms)
                            > expire.max_age_ms
                        {
                            observations.remove(row_key.as_slice()).map_err(storage)?;
                            removed_bytes += row_key.len() + row.encoded_len();
                            dropped += 1;
                        }
                    }
                }
            }
            changed = store.epoch() != before || dropped > 0 || added_bytes > 0;
            if dropped > 0 {
                state.observation_count = state
                    .observation_count
                    .checked_sub(dropped)
                    .ok_or_else(|| corrupt("observation count underflow"))?;
                capacity_header.observation_count = capacity_header
                    .observation_count
                    .checked_sub(dropped)
                    .ok_or_else(|| corrupt("observation counter underflow"))?;
            }
            if changed {
                if store.epoch() != before {
                    state.observation_epoch = state
                        .observation_epoch
                        .checked_add(1)
                        .ok_or_else(|| Status::resource_exhausted("observation epoch exhausted"))?;
                }
                let old_state = states
                    .get(state_bytes.as_slice())
                    .map_err(storage)?
                    .map_or(0, |v| v.value().len());
                let state_value = state.encode_to_vec();
                removed_bytes += old_state;
                added_bytes += state_value.len();
                states
                    .insert(state_bytes.as_slice(), state_value.as_slice())
                    .map_err(storage)?;
                header.source.payload_bytes =
                    payload_change(header.source.payload_bytes, removed_bytes, added_bytes)?;
                if header
                    .source
                    .payload_bytes
                    .saturating_add(header.control.reserved_bytes)
                    > header.limits.max_payload_bytes
                {
                    return Err(Status::resource_exhausted(
                        "source authority payload capacity is full; retry history is never evicted",
                    ));
                }
                meta.insert("header", header.source.encode_to_vec().as_slice())
                    .map_err(storage)?;
                meta.insert(CAPACITY_HEADER, capacity_header.encode_to_vec().as_slice())
                    .map_err(storage)?;
            }
            receipt = CapacityTransitionReceipt {
                format_version: 1,
                outcome: outcome as i32,
                observation_epoch: state.observation_epoch,
                dropped,
                control_revision: header.source.control_revision,
            };
        }
        if !changed {
            tx.abort().map_err(storage)?;
            return Ok(receipt);
        }
        #[cfg(test)]
        self.inject(false)?;
        tx.commit().map_err(storage)?;
        #[cfg(test)]
        self.inject(true)?;
        Ok(receipt)
    }

    pub fn capacity_state(
        &self,
        principal: &str,
        key: &LogicalSourceOwner,
    ) -> Result<CapacityState, Status> {
        self.guarded(|| {
            contract::key(key, false)?;
            let tx = self.inner.database.begin_read().map_err(storage)?;
            self.read_policy(&tx, principal, key)?;
            let states = tx.open_table(CAPACITY).map_err(storage)?;
            contract::decode(
                states
                    .get(state_key(key).as_slice())
                    .map_err(storage)?
                    .ok_or_else(|| Status::not_found("resource has no capacity configuration"))?
                    .value(),
            )
        })
    }

    pub fn capacity_configure_decision(
        &self,
        principal: &str,
        key: &LogicalSourceOwner,
        command_id: &[u8],
    ) -> Result<CapacityConfigureDecision, Status> {
        self.guarded(|| {
            contract::operation_id(command_id)?;
            let tx = self.inner.database.begin_read().map_err(storage)?;
            self.read_policy(&tx, principal, key)?;
            let operations = tx.open_table(CAPACITY_OPERATIONS).map_err(storage)?;
            let bytes = contract::operation_key(principal, key, command_id).encode_to_vec();
            let operation: CapacityConfigureOperation = contract::decode(
                operations
                    .get(bytes.as_slice())
                    .map_err(storage)?
                    .ok_or_else(|| Status::not_found("capacity command has no decision"))?
                    .value(),
            )?;
            operation.decision.ok_or_else(|| missing("decision"))
        })
    }

    /// One consistent read of everything the planner binds to, transcribed
    /// into the planner's input. Nothing is validated or repaired here beyond
    /// the committed rows' own integrity; `TierSnapshot::validated` decides.
    pub fn planner_input(
        &self,
        principal: &str,
        key: &LogicalSourceOwner,
        context: &PlanningContext,
    ) -> Result<tiers::TierSnapshotInput, Status> {
        self.guarded(|| {
            contract::key(key, false)?;
            let tx = self.inner.database.begin_read().map_err(storage)?;
            let policy = self.read_policy(&tx, principal, key)?;
            let meta = tx.open_table(META).map_err(storage)?;
            let (header, _) = read_headers(&meta, &self.inner.identity)?;
            let states = tx.open_table(CAPACITY).map_err(storage)?;
            let state: CapacityState = contract::decode(
                states
                    .get(state_key(key).as_slice())
                    .map_err(storage)?
                    .ok_or_else(|| Status::not_found("resource has no capacity configuration"))?
                    .value(),
            )?;
            let configuration = state
                .configuration
                .as_ref()
                .ok_or_else(|| missing("capacity configuration"))?;
            let tier_policy = validate_configuration(configuration).map_err(corrupt)?;
            let committed = {
                let control = tx.open_table(CONTROL).map_err(storage)?;
                let topologies = tx.open_table(TOPOLOGIES).map_err(storage)?;
                let nodes = tx.open_table(NODES).map_err(storage)?;
                let replicas = tx.open_table(REPLICAS).map_err(storage)?;
                load_committed(&control, &topologies, &nodes, &replicas, key)?
                    .ok_or_else(|| corrupt("configured resource has no applied control state"))?
            };
            let derived = derive(&committed, key)?;
            let rows = {
                let reporters = tx.open_table(REPORTERS).map_err(storage)?;
                let observations = tx.open_table(OBSERVATIONS).map_err(storage)?;
                load_rows(&reporters, &observations, key)?
            };
            let mut observations = Vec::with_capacity(rows.observations.len());
            for (_, value) in &rows.observations {
                observations.push(observation_from_proto(value).map_err(corrupt)?);
            }
            Ok(tiers::TierSnapshotInput {
                planning_instant_unix_ms: context.planning_instant_unix_ms,
                max_moves: context.max_moves,
                max_observation_age_ms: context.max_observation_age_ms,
                clock_skew_bound_ms: context.clock_skew_bound_ms,
                authority_id: hex16(&self.inner.identity.group_id),
                authority_incarnation: fixed::<16>(
                    &self.inner.identity.authority_incarnation,
                    "authority_incarnation",
                )
                .map_err(corrupt)?,
                control_revision: header.source.control_revision,
                policy_revision: policy.revision,
                policy: tier_policy,
                observation_epoch: state.observation_epoch,
                topology_generation: committed.state.topology_generation,
                placement_tree_digest: derived.placement_tree_digest,
                workspace: key.workspace.clone(),
                collection: key.collection.clone(),
                derived_fingerprint: derived.derived_fingerprint,
                provider_geometry_digest: derived.provider_geometry_digest,
                cohort_length_ms: configuration.cohort_length_ms,
                cohort_phase_unix_ms: configuration.cohort_phase_unix_ms,
                nodes: node_records(&committed, context.planning_instant_unix_ms)?,
                observations,
                fragment_requests: context.fragment_requests.clone(),
                committed: derived.view,
                current_incarnations: rows.reporters.iter().cloned().collect(),
            })
        })
    }
}

fn header_of(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<CapacityStoreHeader, Status> {
    header(meta)
}

/// Recovery audit of the capacity tables. Returns the accepted configure
/// decision count and the bytes held by every capacity table.
pub(super) fn validate(
    tx: &redb::ReadTransaction,
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
    source: &SourceAuthorityHeader,
    policy: &AccessPolicy,
    regular: &impl ReadableTable<&'static [u8], &'static [u8]>,
) -> Result<(u64, usize), Status> {
    let capacity_header = header(meta)?;
    let identity = source
        .identity
        .as_ref()
        .ok_or_else(|| missing("identity"))?;
    let limits = source.limits.as_ref().ok_or_else(|| missing("limits"))?;
    let imports = tx.open_table(import::IMPORT_OPERATIONS).map_err(corrupt)?;
    let mut payload = 0usize;
    let mut accepted = 0u64;
    let mut decisions = 0u64;
    let mut latest_accepted: BTreeMap<Vec<u8>, (u64, CapacityConfiguration)> = BTreeMap::new();
    let operations = tx.open_table(CAPACITY_OPERATIONS).map_err(corrupt)?;
    for entry in operations.iter().map_err(storage)? {
        let (k, v) = entry.map_err(storage)?;
        let key: SourceAuthorityOperationKey = contract::decode(k.value())?;
        let value: CapacityConfigureOperation = contract::decode(v.value())?;
        if key.format_version != 1 || value.format_version != 1 {
            return Err(corrupt("unsupported capacity operation format"));
        }
        contract::principal(&key.principal).map_err(corrupt)?;
        let command = value
            .command
            .as_ref()
            .ok_or_else(|| missing("stored capacity command"))?;
        validate_stored_configure(command, identity, limits).map_err(corrupt)?;
        if key.key != command.key
            || key.command_id != command.command_id
            || value.request_sha256 != command_digest(command)
            || regular.get(k.value()).map_err(storage)?.is_some()
            || imports.get(k.value()).map_err(storage)?.is_some()
        {
            return Err(corrupt(
                "capacity operation key, digest or namespace differs",
            ));
        }
        let decision = value
            .decision
            .as_ref()
            .ok_or_else(|| missing("stored capacity decision"))?;
        if decision.format_version != 1
            || decision.control_revision == 0
            || decision.control_revision > source.control_revision
            || decision.policy_revision == 0
            || decision.policy_revision > policy.revision
            || ![0, 3, 5, 8, 9].contains(&decision.code)
            || (decision.code != 0 && decision.message.is_empty())
            || (decision.code == 0
                && (!decision.message.is_empty()
                    || command.expected_control_revision.checked_add(1)
                        != Some(decision.control_revision)))
        {
            return Err(corrupt(
                "capacity decision format, status or revision is invalid",
            ));
        }
        if decision.code == 0 {
            accepted += 1;
            let resource = command.key.as_ref().expect("validated key").encode_to_vec();
            let configuration = command
                .configuration
                .clone()
                .expect("validated configuration");
            match latest_accepted.get(&resource) {
                Some((revision, _)) if *revision > decision.control_revision => {}
                _ => {
                    latest_accepted.insert(resource, (decision.control_revision, configuration));
                }
            }
        }
        decisions += 1;
        payload += k.value().len() + v.value().len();
    }
    if decisions != capacity_header.configure_decision_count {
        return Err(corrupt("capacity decision count differs from its header"));
    }

    let states = tx.open_table(CAPACITY).map_err(corrupt)?;
    let reporters = tx.open_table(REPORTERS).map_err(corrupt)?;
    let observations = tx.open_table(OBSERVATIONS).map_err(corrupt)?;
    let control = tx.open_table(CONTROL).map_err(corrupt)?;
    let topologies = tx.open_table(TOPOLOGIES).map_err(corrupt)?;
    let nodes = tx.open_table(NODES).map_err(corrupt)?;
    let replicas = tx.open_table(REPLICAS).map_err(corrupt)?;
    let mut state_count = 0u64;
    let mut reporter_total = 0u64;
    let mut observation_total = 0u64;
    let mut covered = 0u64;
    for entry in states.iter().map_err(storage)? {
        let (k, v) = entry.map_err(storage)?;
        let key: LogicalSourceOwner = contract::decode(k.value())?;
        let state: CapacityState = contract::decode(v.value())?;
        if state.format_version != 1 || state.key.as_ref() != Some(&key) {
            return Err(corrupt("capacity state format or key differs"));
        }
        let configuration = state
            .configuration
            .as_ref()
            .ok_or_else(|| missing("capacity configuration"))?;
        validate_configuration(configuration).map_err(corrupt)?;
        match latest_accepted.get(k.value()) {
            Some((revision, latest))
                if *revision == state.configured_control_revision && latest == configuration => {}
            _ => {
                return Err(corrupt(
                    "capacity state differs from its latest accepted configure decision",
                ))
            }
        }
        let committed = load_committed(&control, &topologies, &nodes, &replicas, &key)?
            .ok_or_else(|| {
                corrupt("capacity state names a resource with no applied control state")
            })?;
        let derived = derive(&committed, &key)?;
        let rows = load_rows(&reporters, &observations, &key)?;
        if rows.reporters.len() as u64 != state.reporter_count
            || rows.observations.len() as u64 != state.observation_count
        {
            return Err(corrupt("capacity state counts differ from its rows"));
        }
        rebuild(configuration, derived.view, &rows)?;
        reporter_total += state.reporter_count;
        observation_total += state.observation_count;
        state_count += 1;
        covered += 1;
        payload += k.value().len() + v.value().len();
    }
    // Every reporter and observation row belongs to a configured resource.
    let mut reporter_rows = 0u64;
    for entry in reporters.iter().map_err(storage)? {
        let (k, v) = entry.map_err(storage)?;
        let key: CapacityReporterKey = contract::decode(k.value())?;
        let resource = key
            .key
            .ok_or_else(|| missing("reporter resource"))?
            .encode_to_vec();
        if states.get(resource.as_slice()).map_err(storage)?.is_none() {
            return Err(corrupt("reporter row names an unconfigured resource"));
        }
        reporter_rows += 1;
        payload += k.value().len() + v.value().len();
    }
    let mut observation_rows = 0u64;
    for entry in observations.iter().map_err(storage)? {
        let (k, v) = entry.map_err(storage)?;
        let key: CapacityObservationKey = contract::decode(k.value())?;
        let resource = key
            .key
            .ok_or_else(|| missing("observation resource"))?
            .encode_to_vec();
        if states.get(resource.as_slice()).map_err(storage)?.is_none() {
            return Err(corrupt("observation row names an unconfigured resource"));
        }
        observation_rows += 1;
        payload += k.value().len() + v.value().len();
    }
    if state_count != capacity_header.state_count
        || reporter_rows != reporter_total
        || reporter_rows != capacity_header.reporter_count
        || observation_rows != observation_total
        || observation_rows != capacity_header.observation_count
        || covered != state_count
    {
        return Err(corrupt("capacity table counts differ from the headers"));
    }
    Ok((accepted, payload))
}
