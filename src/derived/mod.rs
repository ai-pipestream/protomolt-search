//! Derived columns declared on the index:
//! CEL value columns computed once at index time from each document's
//! own values, stored as ordinary typed columns, and usable as the
//! physical shard key.
//!
//! The declaration is a protobuf contract (`DerivedColumns`), which the
//! TOML `[[derived]]` table configures field for field. Its fingerprint
//! identifies what an index means: every shard, the coordinator's shard
//! map, the write-ahead log, the store file (kind-15 entry), and the
//! segment catalog carry it, and a mismatch refuses by name. A changed
//! expression is a rebuild through the reshard tool, never a mutation
//! in place.
//!
//! At ingest the node is the only writer of derived values: a request
//! that carries a value under a derived name, or a fingerprint, is
//! refused as forged; the node evaluates the declaration over the
//! document's own values, pushes the results into the ordinary value
//! lists, stamps the fingerprint, and logs that. A logged record
//! replays with its recorded values and never evaluates twice.

#![deny(clippy::result_large_err)]

use crate::pb::{self, AddDocumentsRequest};
use crate::values::{self, IngestEnv, IngestVal, ValueType};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashSet};

crate::domain_error::domain_error! {
    /// Why a document's derived columns were refused, converted to
    /// transport status only at the RPC edge.
    #[derive(Debug, Clone)]
    pub enum DerivedError {
        InvalidArgument => invalid_argument,
        FailedPrecondition => failed_precondition,
    }
    foreign: Timestamp
}

/// Domain separator of the declaration fingerprint. Bumped only when
/// the meaning of an unchanged declaration changes.
const FINGERPRINT_DOMAIN: &[u8] = b"protomolt-search.derived-columns.v1\0";

/// One compiled derived column.
#[derive(Debug, Clone)]
pub struct CompiledColumn {
    /// The column name.
    pub name: String,
    /// The compiled expression.
    pub expr: pb::ValueExpr,
    /// The stored family.
    pub kind: pb::MaterializeKind,
    /// The disclosure rule.
    pub disclosure: pb::DerivedDisclosure,
    /// The column names the expression reads (facets included).
    pub inputs: Vec<String>,
    /// Whether the expression reads the stable routing key.
    pub reads_stable_key: bool,
}

/// A validated declaration.
#[derive(Debug, Clone)]
pub struct Declaration {
    spec: pb::DerivedColumns,
    fingerprint: String,
    columns: Vec<CompiledColumn>,
}

impl PartialEq for Declaration {
    fn eq(&self, other: &Self) -> bool {
        self.fingerprint == other.fingerprint
    }
}
impl Eq for Declaration {}

/// The fingerprint of a declaration: SHA-256 over a domain separator
/// and the canonical protobuf encoding, lowercase hex. Independent of
/// the TOML spelling, so a shard map and a node config that mean the
/// same declaration agree.
pub fn fingerprint_of(spec: &pb::DerivedColumns) -> String {
    let mut hasher = crate::sha256::Sha256::new();
    hasher.update(FINGERPRINT_DOMAIN);
    hasher.update(&spec.encode_to_vec());
    crate::sha256::to_hex(&hasher.finalize())
}

/// Decode a stored canonical declaration.
pub fn decode_canonical(bytes: &[u8]) -> Result<pb::DerivedColumns, String> {
    pb::DerivedColumns::decode(bytes).map_err(|e| format!("derived-column declaration: {e}"))
}

/// The wire kind's name in the TOML form.
fn kind_name(kind: pb::MaterializeKind) -> &'static str {
    match kind {
        pb::MaterializeKind::F64 => "f64",
        pb::MaterializeKind::I64 => "i64",
        pb::MaterializeKind::U64 => "u64",
        pb::MaterializeKind::Unspecified => "unspecified",
    }
}

fn disclosure_name(disclosure: pb::DerivedDisclosure) -> &'static str {
    match disclosure {
        pb::DerivedDisclosure::Inputs => "inputs",
        pb::DerivedDisclosure::Own => "own",
        pb::DerivedDisclosure::Unspecified => "unspecified",
    }
}

mod config;
mod declaration;
mod ingest;
#[cfg(test)]
mod tests;

pub use config::{
    config_from_spec, declaration_toml, load_declaration, spec_from_config, DerivedColumnConfig,
};
pub use ingest::{ingest_env, materialize_into};
