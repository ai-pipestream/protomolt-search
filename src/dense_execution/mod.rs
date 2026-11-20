//! The dense execution contract. A request names a selection mode and a
//! score precision; [`resolve`] binds them to one validated
//! [`ResolvedDensePlan`] against the live provider's proven capabilities,
//! and [`stamp`] validates what the searched parts report against that plan
//! before the outcome is published. Contradictory receipts are errors, and
//! FP32 rescoring of a candidate pool never certifies exhaustive coverage.

mod plan;
mod qualified;
#[cfg(test)]
mod qualified_tests;
mod raw;
mod receipt;
#[cfg(test)]
mod tests;
pub(crate) use plan::{
    output_precision, parse_request, resolve, DenseExecutionError, DenseRequest, Preset,
    ProviderCaps, ResolvedDensePlan, Route, HNSW_BALANCED, HNSW_FAST,
};
pub(crate) use qualified::qualified;
pub(crate) use raw::{validate_raw_controls, RawDenseControls};
pub(crate) use receipt::{
    planned_outcome, stamp, verify_exhaustive_scan, Evidence, ShardPartEvidence,
};
