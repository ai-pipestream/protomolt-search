//! Facet, cardinality, column-stat and range merges, and unknown-name refusals.

use super::*;

/// Merge per-shard column stats: counts and sums add, mins and maxes fold,
/// `mean` is computed at the coordinator, and an unknown column refuses by name.
pub(in crate::coordinator) fn merge_column_stats(
    requested: &[String],
    shard_stats: &[Vec<crate::pb::ColumnStats>],
) -> Result<Vec<crate::pb::ColumnStats>, Status> {
    crate::column_stats::merge(requested, shard_stats)
}

/// Union per-shard distinct facet values into exact global cardinalities
/// (comparing string values rather than shard-local ordinals).
pub(in crate::coordinator) fn merge_cardinality(
    requested: &[String],
    shard_distinct: &[Vec<crate::pb::FacetDistinct>],
) -> Result<Vec<crate::pb::FacetCardinality>, Status> {
    let mut known = vec![false; requested.len()];
    let mut sets: Vec<std::collections::HashSet<String>> =
        requested.iter().map(|_| Default::default()).collect();
    for shard in shard_distinct {
        if shard.len() != requested.len() {
            return Err(Status::internal(format!(
                "shard answered {} cardinality columns for {} requested",
                shard.len(),
                requested.len()
            )));
        }
        for ((k, set), d) in known.iter_mut().zip(&mut sets).zip(shard) {
            *k |= d.known;
            set.extend(d.values.iter().cloned());
        }
    }
    requested
        .iter()
        .zip(known)
        .zip(sets)
        .map(|((name, known), set)| {
            if !known {
                return Err(Status::invalid_argument(format!(
                    "no shard has facet column {:?} for cardinality: check the spelling, \
                     or the nodes' --facet-fields",
                    name
                )));
            }
            Ok(crate::pb::FacetCardinality {
                field: name.clone(),
                cardinality: set.len() as u64,
            })
        })
        .collect()
}

/// Merge per-shard facet counts into global counts: the plain per-value
/// sum (counts are additive — no node's count depends on another's, so
/// there is no analog of the global-df trap), `known` when at least one
/// shard has the field, sorted count-descending with ties by value
/// ascending. A facet field NO shard knows is refused — the same rule
/// as an unknown scoring field: zeros everywhere would make a typo read
/// as "no results per anything".
pub(crate) fn merge_facet_counts(
    requested: &[String],
    map_requested: &[crate::pb::MapFacetField],
    shard_facets: &[Vec<crate::pb::FacetFieldCounts>],
    require_known: bool,
) -> Result<Vec<crate::pb::FacetFieldCounts>, Status> {
    let targets: Vec<(&str, Option<&str>)> = requested
        .iter()
        .map(|name| (name.as_str(), None))
        .chain(
            map_requested
                .iter()
                .map(|entry| (entry.column.as_str(), Some(entry.key.as_str()))),
        )
        .collect();
    let mut known = vec![false; targets.len()];
    let mut sums: Vec<HashMap<String, u64>> = targets.iter().map(|_| HashMap::new()).collect();
    for share in shard_facets {
        if share.len() != targets.len() {
            return Err(Status::failed_precondition(
                "facet response has wrong field count",
            ));
        }
        for (i, (response, &(field, key))) in share.iter().zip(&targets).enumerate() {
            if response.field != field
                || response.map_key.as_deref() != key
                || (!response.known && !response.counts.is_empty())
            {
                return Err(Status::failed_precondition(format!(
                    "facet response does not match requested column {field:?} and map key {key:?}"
                )));
            }
            known[i] |= response.known;
            for count in &response.counts {
                let total = sums[i].entry(count.value.clone()).or_default();
                *total = total
                    .checked_add(count.count)
                    .ok_or_else(|| Status::out_of_range("facet count overflows u64"))?;
            }
        }
    }
    targets
        .into_iter()
        .zip(known)
        .zip(sums)
        .map(|(((field, key), known), sum)| {
            if require_known && !known {
                let target = match key {
                    Some(key) => format!("{field:?}[{key:?}]"),
                    None => format!("{field:?}"),
                };
                return Err(Status::invalid_argument(format!(
                    "no shard has facet field {target}; check --facet-fields / --map-facet-fields"
                )));
            }
            let mut counts: Vec<_> = sum
                .into_iter()
                .map(|(value, count)| crate::pb::FacetCount { value, count })
                .collect();
            counts.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.value.cmp(&b.value)));
            Ok(crate::pb::FacetFieldCounts {
                field: field.to_owned(),
                map_key: key.map(str::to_owned),
                known,
                counts,
            })
        })
        .collect()
}

/// Merge per-shard range-facet counts positionally by bucket, refusing any
/// column unknown across all shards.
pub(in crate::coordinator) fn merge_range_counts(
    requested: &[crate::pb::RangeFacetField],
    shard_ranges: &[Vec<crate::pb::RangeFacetCounts>],
) -> Result<Vec<crate::pb::RangeFacetCounts>, Status> {
    crate::rangefacet::merge(requested, shard_ranges, true)
}

/// Refuse a geo-filter column that no shard declares; partially known columns
/// remain valid across heterogeneous fleets.
pub(in crate::coordinator) fn refuse_unknown_geo_columns(
    filters: &[crate::pb::GeoFilter],
    known: &[bool],
) -> Result<(), Status> {
    let unknown: Vec<String> = filters
        .iter()
        .zip(known)
        .filter(|(_, k)| !**k)
        .map(|(f, _)| format!("{:?}", f.column))
        .collect();
    if !unknown.is_empty() {
        return Err(Status::invalid_argument(format!(
            "no shard has geo column {}: filtering on an unknown column would remove every \
             document and read as an empty result set. Check the spelling, or the nodes' \
             --geo-fields.",
            unknown.join(", ")
        )));
    }
    Ok(())
}

/// The typo rule for compiled filter trees: a leaf NO shard can
/// resolve is a name spelled wrong (or a literal whose type picked the
/// wrong table), and filtering on it would read as an empty result
/// set. Known flags are positional over
/// [`crate::filter::walk_leaves`] order; the nodes derive theirs from
/// the same walk, so the zip below cannot misattribute a flag.
pub(in crate::coordinator) fn refuse_unknown_filter_leaves(
    filter: Option<&crate::pb::FilterExpr>,
    known: &[bool],
) -> Result<(), Status> {
    let Some(expr) = filter else {
        return Ok(());
    };
    let mut leaves = Vec::new();
    crate::filter::walk_leaves(expr, &mut |l| leaves.push(l));
    let unknown: Vec<String> = leaves
        .iter()
        .zip(known)
        .filter(|(_, k)| !**k)
        .map(|(l, _)| l.describe())
        .collect();
    if !unknown.is_empty() {
        return Err(Status::invalid_argument(format!(
            "no shard can resolve filter {}: filtering on an unknown name would read as \
             an empty result set. Check the spelling and the literal's type (a string \
             literal selects the facet tables, a number the i64/u64/f64 tables), or the \
             nodes' --facet-fields / --numeric-fields / --integer-fields / --unsigned-integer-fields / \
             --map-facet-fields / --map-numeric-fields / --geo-fields.",
            unknown.join(", ")
        )));
    }
    Ok(())
}
