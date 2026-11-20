//! Validation of a published shard map into a coordinator topology.

use super::*;

/// Inclusive hash ranges `(lo, hi, shard)` must tile `0..=u64::MAX`
/// with no gap and no overlap.
fn check_hash_tiling(mut ranges: Vec<(u64, u64, usize)>, scope: &str) -> Result<(), String> {
    ranges.sort_by_key(|range| range.0);
    let mut expected = 0u64;
    for (position, (lo, hi, shard)) in ranges.iter().copied().enumerate() {
        if lo != expected {
            return Err(format!(
                "topology hash space{scope} has a gap or overlap before shard {shard}: expected {expected}, got {lo}"
            ));
        }
        if position + 1 == ranges.len() {
            if hi != u64::MAX {
                return Err(format!(
                    "topology hash space{scope} ends at {hi}, not {}",
                    u64::MAX
                ));
            }
        } else {
            expected = hi.checked_add(1).ok_or_else(|| {
                format!("topology shard {shard} reaches the hash-space end too early")
            })?;
        }
    }
    Ok(())
}

pub(in crate::coordinator) fn build_topology(
    generation: u64,
    routes: Vec<TopologyRoute>,
    tree: Option<&crate::placement::PlacementTreeConfig>,
) -> Result<CoordinatorTopology, String> {
    if routes.is_empty() {
        return Err("topology requires at least one primary shard".to_string());
    }
    let placement = match tree {
        Some(tree) => Some(Arc::new(crate::placement::Placement::validate(tree)?)),
        None => None,
    };
    check_placement_codes(&routes, placement.as_deref())?;
    check_route_addresses(&routes)?;
    check_hash_ranges(&routes, placement.as_deref())?;
    Ok(CoordinatorTopology {
        generation,
        stats_cache: Arc::new(crate::stats_cache::StatsCache::new(routes.len())),
        routes,
        placement,
    })
}

/// Every shard names a leaf of the tree and every leaf has a shard, or the
/// map has no tree and no shard names a code.
fn check_placement_codes(
    routes: &[TopologyRoute],
    placement: Option<&crate::placement::Placement>,
) -> Result<(), String> {
    match placement {
        None => {
            if let Some(shard) = routes.iter().position(|route| route.placement.is_some()) {
                return Err(format!(
                    "topology shard {shard} carries a placement code but the map has no \
                     placement tree"
                ));
            }
        }
        Some(placement) => {
            let mut served = vec![0usize; placement.leaves().len()];
            for (shard, route) in routes.iter().enumerate() {
                let Some(code) = route.placement else {
                    return Err(format!(
                        "topology shard {shard} has no placement code; the map has a placement \
                         tree, so every shard names the leaf it serves"
                    ));
                };
                let Some(index) = placement.leaves().iter().position(|leaf| leaf.code == code)
                else {
                    return Err(format!(
                        "topology shard {shard} names placement code {code}, which is no leaf \
                         of the tree"
                    ));
                };
                served[index] += 1;
            }
            for (leaf, count) in placement.leaves().iter().zip(&served) {
                if *count == 0 {
                    return Err(format!(
                        "placement leaf {:?} (code {}) has no shard",
                        leaf.name, leaf.code
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Addresses are non-empty and unique, and no hash range is inverted.
fn check_route_addresses(routes: &[TopologyRoute]) -> Result<(), String> {
    let mut addresses = std::collections::HashSet::new();
    for (shard, route) in routes.iter().enumerate() {
        if route.addr.is_empty() {
            return Err(format!(
                "topology shard {shard} has an empty primary address"
            ));
        }
        if !addresses.insert(route.addr.as_str()) {
            return Err(format!("duplicate topology endpoint {:?}", route.addr));
        }
        if let Some(replica) = route.replica.as_deref() {
            if replica.is_empty() {
                return Err(format!(
                    "topology shard {shard} has an empty replica address"
                ));
            }
            if !addresses.insert(replica) {
                return Err(format!("duplicate topology endpoint {replica:?}"));
            }
        }
        if let Some((lo, hi)) = route.hash_range {
            if lo > hi {
                return Err(format!(
                    "topology shard {shard} has inverted hash range {lo}..={hi}"
                ));
            }
        }
    }
    Ok(())
}

/// Hash ranges cover every shard or none, and tile the hash space (per
/// placement leaf under a tree).
fn check_hash_ranges(
    routes: &[TopologyRoute],
    placement: Option<&crate::placement::Placement>,
) -> Result<(), String> {
    let ranged = routes
        .iter()
        .filter(|route| route.hash_range.is_some())
        .count();
    if ranged != 0 && ranged != routes.len() {
        return Err("topology must provide hash ranges for every shard or none".to_string());
    }
    if ranged != 0 {
        let ranges = |code: Option<i64>| -> Vec<(u64, u64, usize)> {
            routes
                .iter()
                .enumerate()
                .filter(|(_, route)| code.is_none() || route.placement == code)
                .map(|(shard, route)| {
                    let (lo, hi) = route.hash_range.expect("all routes ranged");
                    (lo, hi, shard)
                })
                .collect()
        };
        match placement {
            None => check_hash_tiling(ranges(None), "")?,
            Some(placement) => {
                for leaf in placement.leaves() {
                    check_hash_tiling(
                        ranges(Some(leaf.code)),
                        &format!(" of placement leaf {:?}", leaf.name),
                    )?;
                }
            }
        }
    }
    Ok(())
}
