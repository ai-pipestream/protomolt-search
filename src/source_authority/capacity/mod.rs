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

mod store;

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
