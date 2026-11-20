//! The dense execution contract. A request names a selection mode and a
//! score precision; [`resolve`] binds them to one validated
//! [`ResolvedDensePlan`] against the live provider's proven capabilities,
//! and [`stamp`] validates what the searched parts report against that plan
//! before the outcome is published. Contradictory receipts are errors, and
//! FP32 rescoring of a candidate pool never certifies exhaustive coverage.

mod plan;
mod raw;
mod receipt;
#[cfg(test)]
mod tests;
#[cfg(test)]
use plan::{HNSW_BALANCED, HNSW_FAST};

pub(crate) use plan::{
    parse_request, resolve, DenseExecutionError, DenseRequest, ProviderCaps, ResolvedDensePlan,
    Route,
};
pub(crate) use raw::{validate_raw_controls, RawDenseControls};
pub(crate) use receipt::{planned_outcome, stamp, verify_exhaustive_scan, Evidence};
