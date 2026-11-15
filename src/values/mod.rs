//! Compiled value expressions on the serving path: the resolution and
//! evaluation half of the CEL value dialect, the sibling of
//! [`crate::filter`].
//!
//! The coordinator compiles expression text once (`cel::compile_value`)
//! into the [`crate::pb::ValueExpr`] IR. Each shard resolves column
//! names against its OWN dictionaries — a name this shard's tables lack
//! is ABSENT for every document here, exactly the filter rule — and
//! finishes the type check stock CEL's rules demand: int with int,
//! uint with uint, double with double, mixed refused naming `double()`, strings joining
//! no arithmetic. Evaluation is then array reads and register
//! arithmetic per RETURNED hit; nothing is interpreted, and nothing
//! runs per candidate.
//!
//! Absence semantics are the engine's Kleene rule extended to values:
//! an absent input makes the result absent, and the integer operations
//! stock CEL calls ERRORS — overflow, division by zero, `i64::MIN`
//! edge cases — also evaluate to absent rather than failing the query.
//! That deviation is documented and pinned in tests; everywhere stock
//! CEL yields a value, this module yields the same value (the
//! differential oracle in `tests/cel_values.rs` holds it there).

mod contract;
mod eval;
mod functions;
mod ingest;
mod ingest_call;
mod ingest_env;
mod ingest_logic;
mod leaves;
mod lookup;
mod resolve;
mod resolve_call;
mod resolve_compare;
mod types;

use tonic::Status;

pub use contract::{
    merge_projection_types, projected_type, validate_map_fetch_contract, validate_projection_row,
    TYPED_MAP_FETCH_VERSION,
};
pub use eval::eval;
pub use functions::{civil_of_micros, fn_signature, fnv1a64_bytes, FnSignature};
pub use ingest::{eval_ingest, eval_ingest_derived};
pub use ingest_env::{IngestEnv, IngestVal};
pub use leaves::{column_leaves, leaf_known, reads_stable_key, ValueLeaf};
pub use lookup::{ColumnLookup, NumericTypes};
pub use resolve::resolve;
pub use types::{CmpOp, Op, ResolvedValue, Val, ValueType};

/// One refusal, uniformly shaped like the compiler's.
fn refuse(msg: impl Into<String>) -> Status {
    Status::invalid_argument(format!("projection: {}", msg.into()))
}
