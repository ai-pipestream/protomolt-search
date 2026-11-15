//! Whether one resolved leaf is impossible under a segment's summary.

use super::ColumnNames;
use crate::filter::{Edge, ResolvedLeaf};
use crate::segments::SegmentSummary;

pub(super) fn leaf_impossible(
    leaf: &ResolvedLeaf,
    summary: &SegmentSummary,
    names: &dyn ColumnNames,
) -> bool {
    match leaf {
        ResolvedLeaf::IntRange { column, lo, hi } => {
            let Some(name) = names.integer_name(*column) else {
                return false;
            };
            int_range_impossible(summary, name, *lo, *hi)
        }
        ResolvedLeaf::UintRange { column, lo, hi } => {
            let Some(name) = names.unsigned_integer_name(*column) else {
                return false;
            };
            let Some(range) = summary.uint_columns.iter().find(|range| range.name == name) else {
                return false;
            };
            range.present == 0 || lo > hi || *lo > range.max || *hi < range.min
        }
        ResolvedLeaf::F64Range { column, lo, hi } => {
            let Some(name) = names.numeric_name(*column) else {
                return false;
            };
            f64_range_impossible(summary, name, lo.as_ref(), hi.as_ref())
        }
        // A presence test is total: it is impossible only when every
        // family the name resolves to is known empty here. Facet and
        // geo tables are not summarized, so a name in either is "maybe".
        ResolvedLeaf::Has {
            facet,
            numeric,
            integer,
            unsigned_integer,
            geo,
        } => {
            if facet.is_some() || geo.is_some() {
                return false;
            }
            let int_empty = integer.is_none_or(|ii| {
                names
                    .integer_name(ii)
                    .is_some_and(|name| int_present(summary, name) == Some(0))
            });
            let uint_empty = unsigned_integer.is_none_or(|ii| {
                names.unsigned_integer_name(ii).is_some_and(|name| {
                    summary
                        .uint_columns
                        .iter()
                        .find(|range| range.name == name)
                        .is_some_and(|range| range.present == 0)
                })
            });
            let num_empty = numeric.is_none_or(|ni| {
                names
                    .numeric_name(ni)
                    .is_some_and(|name| numeric_present(summary, name) == Some(0))
            });
            // A `has` that resolved to no family at all is False on
            // every row already; leave that verdict to the evaluator.
            (integer.is_some() || unsigned_integer.is_some() || numeric.is_some())
                && int_empty
                && uint_empty
                && num_empty
        }
        ResolvedLeaf::Facet { .. }
        | ResolvedLeaf::NumberUnknown
        | ResolvedLeaf::MapFacet { .. }
        | ResolvedLeaf::MapNumber { .. }
        | ResolvedLeaf::MapIntRange { .. }
        | ResolvedLeaf::MapUintRange { .. }
        | ResolvedLeaf::MapHasKey(_)
        | ResolvedLeaf::Geo { .. }
        | ResolvedLeaf::FacetOrdRange { .. }
        | ResolvedLeaf::MapFacetOrdRange { .. } => false,
    }
}

fn int_present(summary: &SegmentSummary, name: &str) -> Option<u64> {
    summary
        .int_columns
        .iter()
        .find(|c| c.name == name)
        .map(|c| c.present)
}

fn numeric_present(summary: &SegmentSummary, name: &str) -> Option<u64> {
    summary
        .numeric_columns
        .iter()
        .find(|c| c.name == name)
        .map(|c| c.present)
}

/// An inclusive integer predicate `lo..=hi` against the segment's
/// range for `name`. `lo > hi` is the empty predicate and is impossible
/// everywhere. A partition range on the same column narrows further:
/// every row that carries the column has a value inside it.
fn int_range_impossible(summary: &SegmentSummary, name: &str, lo: i64, hi: i64) -> bool {
    if lo > hi {
        return true;
    }
    let Some(column) = summary.int_columns.iter().find(|c| c.name == name) else {
        // The summary does not know the column: written before the
        // column existed on this shard, or a different table. Maybe.
        return false;
    };
    if column.present == 0 {
        return true;
    }
    if column.max < lo || column.min > hi {
        return true;
    }
    match summary.partition.as_ref() {
        Some(range) if range.column == name => range.hi < lo || range.lo > hi,
        _ => false,
    }
}

/// A double predicate with optional exact edges against the segment's
/// range for `name`. Some value in `[min, max]` passes both edges
/// exactly when `max` passes the lower edge and `min` passes the upper
/// one, because each edge is monotone over the reals.
fn f64_range_impossible(
    summary: &SegmentSummary,
    name: &str,
    lo: Option<&Edge>,
    hi: Option<&Edge>,
) -> bool {
    let Some(column) = summary.numeric_columns.iter().find(|c| c.name == name) else {
        return false;
    };
    if column.present == 0 {
        return true;
    }
    if column.min.is_nan() || column.max.is_nan() || column.min > column.max {
        // Never written by the seal for a populated column; treat as
        // unknown rather than trust it.
        return false;
    }
    let lower_ok = lo.is_none_or(|edge| edge.admits_from_below(column.max));
    let upper_ok = hi.is_none_or(|edge| edge.admits_from_above(column.min));
    !(lower_ok && upper_ok)
}
