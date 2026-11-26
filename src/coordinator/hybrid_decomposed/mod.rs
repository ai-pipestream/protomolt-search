//! Exact weighted hybrid search with conservative vector floors and certified close-out.

use super::hybrid::accept_hybrid_receipt;
use super::hybrid_context::HybridRound;
use super::*;
use crate::hnsw::distance::dot_underflow_slack;

mod bounds;
mod close_out;
mod run;
#[cfg(feature = "net")]
mod scan;

use bounds::LowerBounds;
use run::DecomposedRun;

/// Total-order f64 wrapper for heap keys.
#[derive(PartialEq)]
pub(super) struct F64Ord(pub(super) f64);

impl Eq for F64Ord {}

impl PartialOrd for F64Ord {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for F64Ord {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// Magnitude-relative slack for the decomposed-floor bounds: 2^-40 of
/// the participating magnitudes, about four thousand times the worst
/// accumulated f64 rounding error of the three operations involved and
/// one part in 10^12 of the scores — provably conservative, immeasurably
/// loose.
pub(super) const SLACK_REL: f64 = 1.0 / (1u64 << 40) as f64;

/// The decomposed vector floor for a fused lower bound `s_lb`:
/// mathematically (s_lb - w_b * b_1) / w_v, rounded DOWN past every
/// f64 rounding error and the f32 cast, so that v(d) < result implies
/// w_v * v(d) + w_b * b(d) < s_lb for every doc (b(d) <= b_1 by the
/// exactness of the BM25 leg). `wb_b1` arrives premultiplied.
pub(super) fn decomposed_floor(s_lb: f64, wb_b1: f64, w_v: f64) -> f32 {
    let t = (s_lb - wb_b1) / w_v;
    let mag = t.abs() + (s_lb.abs() + wb_b1.abs()) / w_v;
    let safe = t - mag * SLACK_REL - dot_underflow_slack(1);
    (safe as f32).next_down()
}

type ExactCandidate = (u64, u32, f32, f32);
type VectorCandidate = (u64, u32, f32);
type RankedCandidate = (u64, u32, f32, f32, f64);
type LexicalTask = tokio::task::JoinHandle<Result<(u32, crate::pb::ShardLegsResponse), Status>>;

impl CoordinatorServiceImpl {
    pub(super) async fn fanout_hybrid_decomposed(
        &self,
        round: HybridRound<'_>,
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>, DenseScanExecution), Status> {
        if self.node_addrs.is_empty() {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }
        let mut run = DecomposedRun::new(self, round);
        let legs_started = std::time::Instant::now();
        run.load_lexical(self).await?;
        let initial_floor = run.seed_vectors(self).await?;
        #[cfg(feature = "net")]
        run.scan_vectors(self, initial_floor).await?;
        #[cfg(not(feature = "net"))]
        let _ = initial_floor;
        let exec = self.decomposed_scan_execution(&run).await?;
        let legs_ms = legs_started.elapsed().as_secs_f32() * 1e3;
        let fusion_started = std::time::Instant::now();
        let hits = run.close_out(self).await?;
        let debug = run.debug(legs_ms, fusion_started);
        Ok((hits, debug, exec))
    }

    async fn decomposed_scan_execution(
        &self,
        run: &DecomposedRun<'_>,
    ) -> Result<DenseScanExecution, Status> {
        if let Some(identity) = self.clustered_quality_identity().await? {
            return Ok(clustered_scan_execution(identity.rows));
        }
        let summaries: Vec<StreamSearchSummary> = run
            .summaries
            .iter()
            .map(|s| s.clone().unwrap_or_default())
            .collect();
        Ok(self.evidence_from_summaries(
            &summaries,
            run.mask.as_ref().map(|m| m.skipped.as_slice()),
            0,
            count_shards_finished_early(&summaries),
            count_shards_floor_pruned(&summaries),
        ))
    }
}

#[cfg(all(test, feature = "net"))]
#[path = "../hybrid_decomposed_tests.rs"]
mod tests;
