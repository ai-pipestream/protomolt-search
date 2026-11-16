//! Envelope and content validation of capacity commands, transitions and
//! observations.
use super::*;

fn tier_policy(policy: &CapacityTierPolicy) -> Result<tiers::TierPolicy, Status> {
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

pub(in crate::source_authority) fn validate_configuration(
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

pub(super) fn validate_configure(
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

pub(in crate::source_authority) fn validate_stored_configure(
    command: &CapacityConfigureCommand,
    identity: &SourceAuthorityIdentity,
    limits: &SourceAuthorityLimits,
) -> Result<(), Status> {
    validate_configure(command, identity, limits)
}

pub(super) fn validate_transition(
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

pub(in crate::source_authority) fn observation_from_proto(
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

pub(in crate::source_authority) fn observation_key(
    key: &LogicalSourceOwner,
    value: &CapacityObservation,
) -> Vec<u8> {
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
