//! Compiled aggregation plans and typed result merging.

use super::*;

mod compile;
mod facets;
mod merge;
mod percentile;

pub(crate) use compile::{compile_aggregations, compile_projections, CompiledAggregate};
pub(crate) use facets::merge_facet_counts;
pub(super) use facets::{
    merge_cardinality, merge_column_stats, merge_range_counts, refuse_unknown_filter_leaves,
    refuse_unknown_geo_columns,
};
pub(super) use merge::agg_vt_name;
pub(crate) use merge::AggMerge;
pub(super) use percentile::nearest_percentile_rank;
pub(crate) use percentile::PctMerge;

/// What a faceted BM25 fan-out returns: the global top-k plus the
/// merged counts of each facet kind that was asked for (empty lists for
/// the kinds that were not). Named because three of the fan-out
/// entry points share the shape and a bare triple reads as noise at
/// every call site.
pub(super) type FacetedHits = (
    Vec<Bm25Hit>,
    Vec<crate::pb::FacetFieldCounts>,
    Vec<crate::pb::RangeFacetCounts>,
);

/// [`FacetedHits`] plus merged match-set column stats, exact distinct-value
/// counts, and segment-pruning telemetry.
pub(super) type AggregatedHits = (
    Vec<Bm25Hit>,
    Vec<crate::pb::FacetFieldCounts>,
    Vec<crate::pb::RangeFacetCounts>,
    Vec<crate::pb::ColumnStats>,
    Vec<crate::pb::FacetCardinality>,
    crate::segment_prune::PruneStats,
);

/// The evaluated values for one candidate set (`fetch_values`).
pub struct FetchedValues {
    /// Agreed declaration types, including empty nodes and absent candidate values.
    pub projection_types: Vec<crate::pb::ScalarValueType>,
    /// Explicit identity/absence records when requested, under the same receipt.
    pub identities: HashMap<u64, Option<crate::pb::DocumentIdentity>>,
    /// doc -> projected values, aligned with the request projections.
    pub rows: HashMap<u64, Vec<crate::pb::ProjectedValue>>,
    /// Per stage: doc -> identity-score contribution. A doc absent
    /// from a map has no value for that stage's column.
    pub stage_rows: Vec<HashMap<u64, f64>>,
    /// Versions read, in node order. Empty only when an unscoped request had
    /// neither projections nor stages and performed no fan-out.
    pub epochs: Vec<StatsClaim>,
}
