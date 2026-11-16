//! Recovery audit of the capacity tables: every retained configure decision
//! and every state, reporter and observation row is recomputed against the
//! headers. Nothing is repaired; a disagreement refuses the open.
use super::*;

type ReadTable = redb::ReadOnlyTable<&'static [u8], &'static [u8]>;

/// The latest accepted configuration of each resource, by its key bytes,
/// with the control revision that accepted it.
type LatestAccepted = BTreeMap<Vec<u8>, (u64, CapacityConfiguration)>;

/// What the audit of the configure operations found.
#[derive(Default)]
struct Operations {
    payload: usize,
    accepted: u64,
    decisions: u64,
    latest_accepted: LatestAccepted,
}

/// The capacity state, reporter and observation tables with the control
/// tables their rows are checked against.
struct Tables {
    states: ReadTable,
    reporters: ReadTable,
    observations: ReadTable,
    control: ReadTable,
    topologies: ReadTable,
    nodes: ReadTable,
    replicas: ReadTable,
}

/// What the audit of the capacity states found.
#[derive(Default)]
struct States {
    payload: usize,
    state_count: u64,
    reporter_total: u64,
    observation_total: u64,
    covered: u64,
}

/// Recovery audit of the capacity tables. Returns the accepted configure
/// decision count and the bytes held by every capacity table.
pub(in crate::source_authority) fn validate(
    tx: &redb::ReadTransaction,
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
    source: &SourceAuthorityHeader,
    policy: &AccessPolicy,
    regular: &impl ReadableTable<&'static [u8], &'static [u8]>,
) -> Result<(u64, usize), Status> {
    let capacity_header = header(meta)?;
    let operations = audit_operations(tx, source, policy, regular)?;
    if operations.decisions != capacity_header.configure_decision_count {
        return Err(corrupt("capacity decision count differs from its header"));
    }
    let tables = Tables {
        states: tx.open_table(CAPACITY).map_err(corrupt)?,
        reporters: tx.open_table(REPORTERS).map_err(corrupt)?,
        observations: tx.open_table(OBSERVATIONS).map_err(corrupt)?,
        control: tx.open_table(CONTROL).map_err(corrupt)?,
        topologies: tx.open_table(TOPOLOGIES).map_err(corrupt)?,
        nodes: tx.open_table(NODES).map_err(corrupt)?,
        replicas: tx.open_table(REPLICAS).map_err(corrupt)?,
    };
    let states = audit_states(&tables, &operations.latest_accepted)?;
    // Every reporter and observation row belongs to a configured resource.
    let (reporter_rows, reporter_payload) = audit_reporter_rows(&tables)?;
    let (observation_rows, observation_payload) = audit_observation_rows(&tables)?;
    if states.state_count != capacity_header.state_count
        || reporter_rows != states.reporter_total
        || reporter_rows != capacity_header.reporter_count
        || observation_rows != states.observation_total
        || observation_rows != capacity_header.observation_count
        || states.covered != states.state_count
    {
        return Err(corrupt("capacity table counts differ from the headers"));
    }
    let payload = operations.payload + states.payload + reporter_payload + observation_payload;
    Ok((operations.accepted, payload))
}

/// Audit every retained configure operation against its command, its
/// namespace and the committed revisions.
fn audit_operations(
    tx: &redb::ReadTransaction,
    source: &SourceAuthorityHeader,
    policy: &AccessPolicy,
    regular: &impl ReadableTable<&'static [u8], &'static [u8]>,
) -> Result<Operations, Status> {
    let identity = source
        .identity
        .as_ref()
        .ok_or_else(|| missing("identity"))?;
    let limits = source.limits.as_ref().ok_or_else(|| missing("limits"))?;
    let installs = tx
        .open_table(control::INSTALL_OPERATIONS)
        .map_err(corrupt)?;
    let mut found = Operations::default();
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
            || installs.get(k.value()).map_err(storage)?.is_some()
        {
            return Err(corrupt(
                "capacity operation key, digest or namespace differs",
            ));
        }
        let decision = value
            .decision
            .as_ref()
            .ok_or_else(|| missing("stored capacity decision"))?;
        audit_decision(decision, command, source, policy)?;
        if decision.code == 0 {
            found.accepted += 1;
            let resource = command.key.as_ref().expect("validated key").encode_to_vec();
            let configuration = command
                .configuration
                .clone()
                .expect("validated configuration");
            match found.latest_accepted.get(&resource) {
                Some((revision, _)) if *revision > decision.control_revision => {}
                _ => {
                    found
                        .latest_accepted
                        .insert(resource, (decision.control_revision, configuration));
                }
            }
        }
        found.decisions += 1;
        found.payload += k.value().len() + v.value().len();
    }
    Ok(found)
}

fn audit_decision(
    decision: &CapacityConfigureDecision,
    command: &CapacityConfigureCommand,
    source: &SourceAuthorityHeader,
    policy: &AccessPolicy,
) -> Result<(), Status> {
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
    Ok(())
}

/// Audit every capacity state against its latest accepted configuration,
/// its applied control state and its own rows.
fn audit_states(tables: &Tables, latest_accepted: &LatestAccepted) -> Result<States, Status> {
    let mut found = States::default();
    for entry in tables.states.iter().map_err(storage)? {
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
        let committed = load_committed(
            &tables.control,
            &tables.topologies,
            &tables.nodes,
            &tables.replicas,
            &key,
        )?
        .ok_or_else(|| corrupt("capacity state names a resource with no applied control state"))?;
        let derived = derive(&committed, &key)?;
        let rows = load_rows(&tables.reporters, &tables.observations, &key)?;
        if rows.reporters.len() as u64 != state.reporter_count
            || rows.observations.len() as u64 != state.observation_count
        {
            return Err(corrupt("capacity state counts differ from its rows"));
        }
        rebuild(configuration, derived.view, &rows)?;
        found.reporter_total += state.reporter_count;
        found.observation_total += state.observation_count;
        found.state_count += 1;
        found.covered += 1;
        found.payload += k.value().len() + v.value().len();
    }
    Ok(found)
}

/// The reporter row count and bytes; a row of an unconfigured resource
/// refuses.
fn audit_reporter_rows(tables: &Tables) -> Result<(u64, usize), Status> {
    let mut rows = 0u64;
    let mut payload = 0usize;
    for entry in tables.reporters.iter().map_err(storage)? {
        let (k, v) = entry.map_err(storage)?;
        let key: CapacityReporterKey = contract::decode(k.value())?;
        let resource = key
            .key
            .ok_or_else(|| missing("reporter resource"))?
            .encode_to_vec();
        if tables
            .states
            .get(resource.as_slice())
            .map_err(storage)?
            .is_none()
        {
            return Err(corrupt("reporter row names an unconfigured resource"));
        }
        rows += 1;
        payload += k.value().len() + v.value().len();
    }
    Ok((rows, payload))
}

/// The observation row count and bytes; a row of an unconfigured resource
/// refuses.
fn audit_observation_rows(tables: &Tables) -> Result<(u64, usize), Status> {
    let mut rows = 0u64;
    let mut payload = 0usize;
    for entry in tables.observations.iter().map_err(storage)? {
        let (k, v) = entry.map_err(storage)?;
        let key: CapacityObservationKey = contract::decode(k.value())?;
        let resource = key
            .key
            .ok_or_else(|| missing("observation resource"))?
            .encode_to_vec();
        if tables
            .states
            .get(resource.as_slice())
            .map_err(storage)?
            .is_none()
        {
            return Err(corrupt("observation row names an unconfigured resource"));
        }
        rows += 1;
        payload += k.value().len() + v.value().len();
    }
    Ok((rows, payload))
}
