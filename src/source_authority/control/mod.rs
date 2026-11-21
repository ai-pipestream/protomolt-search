//! Collection control state: one bounded install command writes a
//! collection's topology, nodes, replicas and actions as rows, and the
//! planner, the map feed and observers read those rows.
//!
//! The install is an actor-scoped, retryable command in one durable
//! transaction, in the same command_id namespace as every other command.
//! Nothing here activates an owner, publishes a map or reconciles an action.
use super::*;
use std::collections::BTreeSet;

mod install;
mod rows;
mod snapshot;
mod validation;

pub(super) use rows::verify_installed;
pub(super) use validation::validate_stored_command;

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
    let mut hasher = turbo_sha256::Sha256::new();
    hasher.update(domain);
    hasher.update(bytes);
    hasher.finish().0.to_vec()
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
