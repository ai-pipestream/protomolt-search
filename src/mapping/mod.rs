//! Descriptor-derived index plans, and the protobuf-native extraction that
//! follows a plan.
//!
//! The input is what a producer already has: a serialized
//! `google.protobuf.FileDescriptorSet` and a fully qualified message
//! type name. The output is a deterministic plan — which fields exist,
//! at what dotted paths, with what resolved kinds, landing on which of
//! this engine's column families — plus a lowercase-hex SHA-256
//! fingerprint that identifies the plan the way an analysis fingerprint
//! identifies an analyzer. Same input, same plan, same fingerprint, on
//! every node and every run.
//!
//! Fields may carry explicit hints as descriptor options, using the
//! `(ai.protomolt.proto.index.hints.v1.index)` extension vendored from
//! protomolt: a proto annotated for protomolt's indexers is understood
//! here without modification. Unhinted integer kinds retain descriptor
//! signedness. Other kinds follow protomolt's rules, with the structural
//! deviation the frozen turbovec-grpc reference also made:
//! an unannotated singular message field is expanded into dotted paths
//! rather than kept as a single OBJECT entry, because this is a flat
//! engine with no native object type. An explicit OBJECT or NESTED hint
//! still keeps the single entry.
//!
//! Everything ambiguous is an error, not a guess. A message with two
//! plausible vector fields, or no resolvable document id, fails
//! derivation with the hint the caller should add. This module never
//! picks one of several candidates silently — that is the failure mode
//! the whole feature exists to refuse.
//!
//! One mechanical note: hint extensions live on
//! `google.protobuf.FieldOptions`, and prost drops extension fields it
//! was not compiled against. The extraction therefore walks the raw
//! descriptor-set bytes once — plain varint/length-delimited wire
//! format, hand-rolled like every parser in this codebase — collecting
//! the extension payloads keyed by structural position (file index,
//! nested-message index path, field index), which is exactly the order
//! the prost-decoded structures iterate in, because both read the same
//! bytes and protobuf preserves repeated-field order.

#![deny(clippy::result_large_err)]

use crate::pb;

mod assemble;
mod derive;
mod extractor;
mod extractor_build;
mod fingerprint;
mod hint_bytes;
mod hint_resolve;
mod hint_validate;
mod land;
mod project;
mod resolve;
mod type_index;
mod walk;

#[path = "../schema_report.rs"]
mod schema_report;

#[path = "../index_definition.rs"]
mod index_definition;

pub use derive::{derive_plan, derive_plan_with_definition, describe_schema};
pub use extractor::{ExtractedDoc, Extractor};
use fingerprint::{collect_enum_values, reachable_messages};
use hint_resolve::{
    is_repeated, plain_hint, projection_scalar_type, shape, wrapper_kind, ResolvedHint, Shape,
};
use hint_validate::validate_hint;
use land::land_for;
use type_index::{MsgEntry, TypeIndex};
use walk::{planned, well_known_leaf};

/// Full name of the field-option extension carrying indexing hints,
/// owned by ProtoMolt and vendored under `proto/ai/protomolt/`.
const HINT_EXTENSION_NAME: &str = "ai.protomolt.proto.index.hints.v1.index";

/// The extension's registered field number on
/// `google.protobuf.FieldOptions`.
const HINT_EXTENSION_NUMBER: u64 = 59_100_471;

/// Nested messages deeper than this stop expanding and are recorded as
/// a single OBJECT entry, which also bounds recursive message types.
const MAX_DEPTH: usize = 8;

/// Version tag mixed into the canonical fingerprint bytes. Bump when a
/// semantic or encoding change is not already distinguished by the canonical
/// plan. Kind and family changes participate directly in the hash. A changed
/// fingerprint requires a new index binding, never a silent rebind.
const FINGERPRINT_VERSION: &str = "pipestream-search.plan.v3";

crate::domain_error::domain_error! {
    /// Why a mapping, plan, or projection was refused, converted to
    /// transport status only at the RPC edge.
    pub enum MappingError {
        InvalidArgument => invalid_argument,
        Internal => internal,
        FailedPrecondition => failed_precondition,
        ResourceExhausted => resource_exhausted,
    }
}

/// One refusal, uniformly shaped: every message begins `plan: `.
fn refuse(msg: impl Into<String>) -> MappingError {
    MappingError::invalid_argument(format!("plan: {}", msg.into()))
}

fn refuse_at(path: &str, msg: impl AsRef<str>) -> MappingError {
    refuse(format!("{}: {}", path, msg.as_ref()))
}
