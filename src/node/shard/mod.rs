//! Per-shard lexical/columnar read surfaces and synchronized shard state.

mod bm25_shard;
mod columns;
mod facets;
mod filters;
mod state;

pub use self::bm25_shard::Bm25Shard;
pub(crate) use self::columns::{
    f64_from_order_bits, f64_order_bits, i64_from_order_bits, i64_order_bits, parse_score_stages,
    ScoreStageSpec,
};
pub(in crate::node) use self::columns::{
    int_min_max_as_f64, projected_value, uint_min_max_as_f64, EmptyColumns, ShardNumericRead,
};
pub(crate) use self::facets::{
    timestamp_to_epoch_micros, validate_geo_filter, validate_geo_filters,
    validate_range_facet_fields,
};
pub(in crate::node) use self::facets::{unknown_range_counts, validate_lat_lon};
pub(in crate::node) use self::filters::{
    filter_known_flags, prune_segments, resolve_shard_filters, ResolvedShardFilters, SegmentPrune,
};
pub(in crate::node) use self::state::{
    active_artifact_rows, live_document_count, live_field_stats, IngestGuard,
};
pub(crate) use self::state::{
    physical_rows, read_shard, write_shard, ParentMap, ShardState, STALE_STATS_EPOCH,
};
