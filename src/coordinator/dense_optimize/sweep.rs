//! Candidate-depth ladder sweeps across native and pooled FP32 precisions.

use std::time::Instant;

use tonic::Status;

use super::measure::{run, warmup, CellSpec, Run, TruthBaseline};
use super::{CandidateTechnique, Fleet, PrecisionPass, Sweep};
use crate::coordinator::CoordinatorServiceImpl;
use crate::dense_execution::{ProviderCaps, HNSW_BALANCED, HNSW_FAST};
use crate::pb::{
    DenseExecutionMode as Mode, DensePolicyPoint, DensePrecisionRequest, DenseScorePrecision,
    DenseTechnique,
};

pub(super) fn techniques(caps: &ProviderCaps) -> Vec<CandidateTechnique> {
    let walk = |mode, preset: crate::dense_execution::Preset| CandidateTechnique {
        mode,
        technique: DenseTechnique::HnswWalk,
        patience: preset.patience,
    };
    let mut out = Vec::new();
    if caps.walks {
        out.push(walk(Mode::Balanced, HNSW_BALANCED));
        if caps.patience {
            out.push(walk(Mode::Fast, HNSW_FAST));
        }
    }
    if caps.staged {
        out.push(CandidateTechnique {
            mode: Mode::Balanced,
            technique: DenseTechnique::BitplaneStaged,
            patience: None,
        });
    }
    out
}

fn depths(k: u32, limit: u32) -> Vec<u32> {
    (0..4)
        .map(|shift| k.saturating_mul(1 << shift))
        .filter(|depth| *depth <= limit)
        .collect()
}

fn build_point(
    spec: &CellSpec,
    approximate: &Run,
    truth: &TruthBaseline,
    fleet: &Fleet,
) -> Option<DensePolicyPoint> {
    let measured_recall_ppm = approximate.min_recall_ppm(truth, spec.k as usize);
    if measured_recall_ppm == 0 {
        return None;
    }
    let (approximate_p50_us, approximate_p99_us, approximate_qps) = approximate.latency();
    let (exhaustive_p50_us, exhaustive_p99_us, exhaustive_qps) = truth.latency();
    Some(DensePolicyPoint {
        k: spec.k,
        filter_selectivity_ppm_min: 1_000_000,
        filter_selectivity_ppm_max: 1_000_000,
        candidates: spec.depth,
        measured_recall_ppm,
        patience_saturation_ppm: spec.patience.map(|(ppm, _)| ppm),
        patience_hops: spec.patience.map(|(_, hops)| hops),
        mode: spec.mode as i32,
        technique: spec.technique as i32,
        selection_precision: fleet.caps.native as i32,
        output_precision: spec.output_precision as i32,
        filter_strategy: approximate.filter_strategy as i32,
        shape: fleet.shape as i32,
        approximate_p50_us,
        approximate_p99_us,
        exhaustive_p50_us,
        exhaustive_p99_us,
        approximate_qps,
        exhaustive_qps,
        ..Default::default()
    })
}

impl CoordinatorServiceImpl {
    pub(super) async fn sweep(
        &self,
        fleet: &Fleet,
        queries: &[Vec<f32>],
        ks: &[u32],
        deadline: Instant,
    ) -> Result<Sweep, Status> {
        let mut sweep = Sweep {
            points: Vec::new(),
            cells: 0,
            truncated: false,
        };
        let native_pass = PrecisionPass {
            precision_req: DensePrecisionRequest::Auto,
            output_precision: fleet.caps.native,
            pooled_fp32: false,
            deadline,
        };
        self.sweep_precision_pass(fleet, queries, ks, native_pass, &mut sweep)
            .await?;
        if !sweep.truncated && fleet.caps.native == DenseScorePrecision::Quantized {
            let fp32_pass = PrecisionPass {
                precision_req: DensePrecisionRequest::Fp32,
                output_precision: DenseScorePrecision::Fp32,
                pooled_fp32: true,
                deadline,
            };
            self.sweep_precision_pass(fleet, queries, ks, fp32_pass, &mut sweep)
                .await?;
        }
        Ok(sweep)
    }

    async fn warmup_pass(
        &self,
        queries: &[Vec<f32>],
        ks: &[u32],
        pass: PrecisionPass,
        sweep: &mut Sweep,
    ) -> Result<bool, Status> {
        let (Some(first_q), Some(&first_k)) = (queries.first(), ks.first()) else {
            return Ok(false);
        };
        match warmup(
            self,
            first_q,
            first_k,
            pass.precision_req,
            pass.output_precision,
            pass.deadline,
        )
        .await
        {
            Ok(true) => Ok(true),
            Ok(false) => {
                sweep.truncated = true;
                Ok(false)
            }
            Err(status) if pass.pooled_fp32 && status.code() == tonic::Code::FailedPrecondition => {
                Ok(false)
            }
            Err(err) => Err(err),
        }
    }

    async fn sweep_precision_pass(
        &self,
        fleet: &Fleet,
        queries: &[Vec<f32>],
        ks: &[u32],
        pass: PrecisionPass,
        sweep: &mut Sweep,
    ) -> Result<(), Status> {
        if !self.warmup_pass(queries, ks, pass, sweep).await? {
            return Ok(());
        }
        let limit = self
            .knobs
            .max_k()
            .min(u32::try_from(fleet.identity.corpus_rows).unwrap_or(u32::MAX));
        let candidates = techniques(&fleet.caps);
        for &k in ks {
            let truth_spec = CellSpec {
                k,
                depth: k,
                mode: Mode::Exhaustive,
                technique: DenseTechnique::Fp32Scan,
                patience: None,
                precision_req: pass.precision_req,
                output_precision: pass.output_precision,
                pooled_fp32: false,
            };
            let truth_run = match run(self, queries, &truth_spec, pass.deadline).await {
                Ok(Some(run)) => run,
                Ok(None) => {
                    sweep.truncated = true;
                    return Ok(());
                }
                Err(status)
                    if pass.pooled_fp32 && status.code() == tonic::Code::FailedPrecondition =>
                {
                    return Ok(());
                }
                Err(err) => return Err(err),
            };
            if Instant::now() >= pass.deadline {
                sweep.truncated = true;
                return Ok(());
            }
            let truth = TruthBaseline::from_run(&truth_run, k as usize);
            for depth in depths(k, limit) {
                for &cand in &candidates {
                    let spec = CellSpec {
                        k,
                        depth,
                        mode: cand.mode,
                        technique: cand.technique,
                        patience: cand.patience,
                        precision_req: pass.precision_req,
                        output_precision: pass.output_precision,
                        pooled_fp32: pass.pooled_fp32,
                    };
                    let Some(approximate) = run(self, queries, &spec, pass.deadline).await? else {
                        sweep.truncated = true;
                        return Ok(());
                    };
                    if Instant::now() >= pass.deadline {
                        sweep.truncated = true;
                        return Ok(());
                    }
                    sweep.cells += 1;
                    if let Some(pt) = build_point(&spec, &approximate, &truth, fleet) {
                        sweep.points.push(pt);
                    }
                }
            }
        }
        Ok(())
    }
}
