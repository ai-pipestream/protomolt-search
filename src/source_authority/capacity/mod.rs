//! Capacity observations as committed state.
//!
//! The planner's frozen snapshot (`capacity_tiers::TierSnapshot`) is a pure
//! function of committed rows: the applied control state of the resource,
//! its capacity configuration, the registered reporter incarnations and the
//! retained observations. This module persists those rows, applies the
//! observation rules through Kimi's `ObservationStore` as the pure
//! transition, and transcribes one consistent read into a
//! `TierSnapshotInput`. It never repairs an input: `TierSnapshot::validated`
//! is the only validator of what it hands over.
use super::control::{self, hash, prefix_end, read_headers, resource_prefix};
use super::control::{CONTROL, NODES, REPLICAS, TOPOLOGIES};
use super::*;
use crate::capacity_tiers as tiers;
use crate::pb::storage::capacity_transition::Action as TransitionAction;
use crate::pb::storage::control_configuration::{Derived as DerivedChoice, Placement, Provider};
use std::collections::BTreeMap;

mod audit;
mod committed;
mod configure;
mod observe;
mod read;
mod rows;
mod validation;

pub(super) use audit::validate;
pub(super) use committed::{current_topology, load_committed, Committed};
use committed::{derive, node_records};
use rows::{load_rows, rebuild, Rows};
pub(super) use validation::observation_key;
use validation::{observation_from_proto, validate_configuration, validate_stored_configure};

pub(super) const CAPACITY_HEADER: &str = "capacity";
pub(super) const CAPACITY_OPERATIONS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("capacity_configure_operations");
pub(super) const CAPACITY: TableDefinition<&[u8], &[u8]> = TableDefinition::new("capacity_state");
pub(super) const REPORTERS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("capacity_reporters");
pub(super) const OBSERVATIONS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("capacity_observations");
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

fn header_of(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<CapacityStoreHeader, Status> {
    header(meta)
}
