//! Dense provider selection, quality checks, and exact rescoring.

use super::*;

mod auto;
mod exact;
mod fleet;
mod provider;
mod quality;
mod resolve;

pub(crate) struct ExactRerankScores {
    pub scores: HashMap<u64, f32>,
    pub rows: u64,
    pub logical_bytes: u64,
    pub pages_touched: u64,
    pub tasks: u32,
}

/// The provider-mismatch line of a cluster health report: empty when
/// every reachable, configured shard scores under one backend kind and
/// scoring fingerprint, else every distinct pair with the shards that
/// serve it.
pub(super) fn provider_mismatch_of(targets: &[ShardHealth]) -> String {
    let mut seen: Vec<((String, String), Vec<String>)> = Vec::new();
    for target in targets {
        let Some(health) = &target.health else {
            continue;
        };
        if health.vector_backend.is_empty() {
            continue;
        }
        let key = (
            health.vector_backend.clone(),
            health.scoring_fingerprint.clone(),
        );
        let who = format!("shard {} ({})", target.shard, target.addr);
        match seen.iter_mut().find(|(k, _)| *k == key) {
            Some((_, shards)) => shards.push(who),
            None => seen.push((key, vec![who])),
        }
    }
    if seen.len() <= 1 {
        return String::new();
    }
    seen.iter()
        .map(|((kind, fingerprint), shards)| format!("{kind}/{fingerprint}: {}", shards.join(", ")))
        .collect::<Vec<_>>()
        .join("; ")
}

/// One provider identity the shard fleet agrees on
/// (`CoordinatorServiceImpl::fleet_vector_identity`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FleetVectorIdentity {
    pub provider: String,
    pub scoring_fingerprint: String,
    pub dimensions: u32,
    pub rows: u64,
}

/// Request-side key for dense execution policy lookup: raw `k`, optional
/// `candidate_depth` (`selection_k`), and compiled filters for live selectivity.
#[derive(Debug, Clone, Copy)]
pub struct DenseRequestKey<'a> {
    pub k: u32,
    pub candidate_depth: u32,
    pub filters: Option<&'a RequestFilters>,
}

/// Hold `actual` as the fleet-wide value on first sight, and refuse with
/// `mismatch` when a later shard reports a different one.
fn hold_uniform<T: PartialEq>(
    held: &mut Option<T>,
    actual: T,
    mismatch: &str,
) -> Result<(), Status> {
    match held {
        Some(value) if *value != actual => Err(Status::failed_precondition(mismatch)),
        Some(_) => Ok(()),
        None => {
            *held = Some(actual);
            Ok(())
        }
    }
}
