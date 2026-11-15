//! The live identity a measured profile binds to, read from `ClusterHealth`.

use crate::pb::{ClusterHealthResponse, HealthResponse};

use super::{LiveIdentity, ProfileRoute};

/// What the primary shards agree on, summed or held in common.
#[derive(Default)]
struct ShardTotals {
    provider: Option<String>,
    fingerprint: Option<String>,
    dimensions: Option<u32>,
    rows: u64,
    exact_rows: u64,
}

/// Read the live identity through `ClusterHealth` and refuse anything a
/// measured profile could not later bind to: an unreachable or empty
/// shard, a provider mismatch, missing exact rows, tombstones.
pub async fn live_identity<R: ProfileRoute>(
    route: &mut R,
    collection: &str,
) -> Result<LiveIdentity, String> {
    let health = route
        .cluster_health(collection.to_string())
        .await
        .map_err(|status| format!("ClusterHealth: {}", status.message()))?;
    if !health.collections.is_empty() {
        let names: Vec<&str> = health.collections.iter().map(|c| c.name.as_str()).collect();
        return Err(format!(
            "the coordinator serves named collections {names:?}; name the one to measure"
        ));
    }
    if !health.provider_mismatch.is_empty() {
        return Err(format!(
            "the fleet does not score in one space: {}",
            health.provider_mismatch
        ));
    }
    let mut totals = ShardTotals::default();
    for target in health.targets.iter().filter(|t| !t.is_replica) {
        let shard = match &target.health {
            Some(shard) if target.reachable => shard,
            _ => {
                return Err(format!(
                    "shard {} at {} is unreachable: {}",
                    target.shard, target.addr, target.error
                ))
            }
        };
        if !target.error.is_empty() {
            return Err(format!(
                "shard {} at {}: {}",
                target.shard, target.addr, target.error
            ));
        }
        check_measurable(shard)?;
        totals.add(shard)?;
    }
    if totals.rows == 0 {
        return Err("the coordinator reports no populated primary shard".into());
    }
    if health.clustered_vector.is_some() {
        return clustered_identity(&health, totals.exact_rows);
    }
    Ok(LiveIdentity {
        provider_backend: totals.provider.unwrap_or_default(),
        scoring_fingerprint: totals.fingerprint.unwrap_or_default(),
        dimensions: totals.dimensions.unwrap_or_default(),
        rows: totals.rows,
        topology_generation: health.topology_generation,
    })
}

/// A shard a profile can measure: populated, every row with its exact
/// FP32 sidecar, and no tombstones.
fn check_measurable(shard: &HealthResponse) -> Result<(), String> {
    if shard.num_vectors == 0 {
        return Err(format!(
            "shard at slot_offset={} has no vectors; a profile measures a populated generation",
            shard.slot_offset
        ));
    }
    if !shard.exact_vectors_available || shard.exact_vector_rows != shard.num_vectors {
        return Err(format!(
            "shard at slot_offset={} owns {} exact FP32 rows for {} vectors; FP32 rerank \
             needs the aligned sidecar for every row",
            shard.slot_offset, shard.exact_vector_rows, shard.num_vectors
        ));
    }
    if shard.deleted_docs != 0 {
        return Err(format!(
            "shard at slot_offset={} has {} tombstoned rows; a profile describes an all-live \
             generation, compact before measuring",
            shard.slot_offset, shard.deleted_docs
        ));
    }
    Ok(())
}

impl ShardTotals {
    /// Fold one shard in: provider, fingerprint, and dimensions must
    /// agree with every earlier shard; rows sum with a check.
    fn add(&mut self, shard: &HealthResponse) -> Result<(), String> {
        for (name, held, seen) in [
            ("vector_backend", &mut self.provider, &shard.vector_backend),
            (
                "scoring_fingerprint",
                &mut self.fingerprint,
                &shard.scoring_fingerprint,
            ),
        ] {
            match held {
                Some(value) if value != seen => {
                    return Err(format!(
                        "shards disagree on {name}: {value:?} vs {seen:?} at slot_offset={}",
                        shard.slot_offset
                    ))
                }
                None => *held = Some(seen.clone()),
                _ => {}
            }
        }
        match self.dimensions {
            Some(dim) if dim != shard.dim => {
                return Err(format!(
                    "shards disagree on dimensions: {dim} vs {} at slot_offset={}",
                    shard.dim, shard.slot_offset
                ))
            }
            None => self.dimensions = Some(shard.dim),
            _ => {}
        }
        self.rows = self
            .rows
            .checked_add(shard.num_vectors)
            .ok_or("row count overflow")?;
        self.exact_rows = self
            .exact_rows
            .checked_add(shard.exact_vector_rows)
            .ok_or("exact row count overflow")?;
        Ok(())
    }
}

/// The clustered provider's identity, which must be servable, carry a
/// scoring identity, and hold exactly the product shards' exact rows.
fn clustered_identity(
    health: &ClusterHealthResponse,
    exact_rows: u64,
) -> Result<LiveIdentity, String> {
    let clustered = health.clustered_vector.as_ref().expect("checked by caller");
    if !clustered.reachable || !clustered.servable {
        return Err(format!(
            "clustered vector provider {} is not servable: {}",
            clustered.backend_kind, clustered.error
        ));
    }
    if clustered.scoring_fingerprint.is_empty() || clustered.dimensions == 0 {
        return Err(format!(
            "clustered vector provider {} reports no scoring identity",
            clustered.backend_kind
        ));
    }
    if clustered.rows != exact_rows {
        return Err(format!(
            "clustered vector provider holds {} rows but product shards own {exact_rows} exact rows",
            clustered.rows
        ));
    }
    Ok(LiveIdentity {
        provider_backend: clustered.backend_kind.clone(),
        scoring_fingerprint: clustered.scoring_fingerprint.clone(),
        dimensions: clustered.dimensions,
        rows: clustered.rows,
        topology_generation: clustered.topology_generation,
    })
}
