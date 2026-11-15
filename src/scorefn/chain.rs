//! The resolved chain: evaluation in pinned order and the bound lift.

use super::{ColumnRef, NumericRead, Stage, StageOp};

/// A resolved score-function chain; empty means identity.
#[derive(Debug, Clone, Default)]
pub struct ScoreChain {
    /// Stages in the pinned evaluation order (request-list order).
    pub stages: Vec<Stage>,
}

impl ScoreChain {
    /// The document's final score: stages applied in order. Absent
    /// values (or an unresolved column) leave the score unchanged.
    pub fn eval(&self, score: f64, doc_id: u32, columns: &dyn NumericRead) -> f64 {
        let mut s = score;
        for stage in &self.stages {
            // The geo ops read a PAIR, so they branch before the scalar
            // read rather than pretending a point is one f64.
            if let StageOp::MultGeoDecay {
                metric,
                origin_lat,
                origin_lon,
                scale,
            } = stage.op
            {
                let Some(ColumnRef::Geo(gi)) = stage.column else {
                    continue;
                };
                let Some((lat, lon)) = columns.geo_value(gi, doc_id) else {
                    continue;
                };
                s *= (-metric.meters(origin_lat, origin_lon, lat, lon) / scale).exp();
                continue;
            }
            let x = match stage.column {
                Some(ColumnRef::Numeric(ni)) => columns.value(ni, doc_id),
                Some(ColumnRef::Integer(ii)) => columns.int_value(ii, doc_id).map(|v| v as f64),
                Some(ColumnRef::UnsignedInteger(ui)) => {
                    columns.uint_value(ui, doc_id).map(|v| v as f64)
                }
                Some(ColumnRef::MapKey { column, key_ord }) => {
                    columns.map_value(column, key_ord, doc_id)
                }
                Some(ColumnRef::MapIntegerKey { column, key_ord }) => columns
                    .map_int_value(column, key_ord, doc_id)
                    .map(|v| v as f64),
                Some(ColumnRef::MapUnsignedIntegerKey { column, key_ord }) => columns
                    .map_uint_value(column, key_ord, doc_id)
                    .map(|v| v as f64),
                Some(ColumnRef::Geo(_)) | None => None,
            };
            let Some(x) = x else {
                continue;
            };
            s = match stage.op {
                StageOp::MultExpDecay { origin, scale } => {
                    s * (-((x - origin).abs()) / scale).exp()
                }
                StageOp::MultLog { weight } => s * (1.0 + weight * (1.0 + x.max(0.0)).ln()),
                StageOp::AddLinear { weight } => s + weight * x,
                StageOp::MultGeoDecay { .. } => unreachable!("geo ops returned above"),
            };
        }
        s
    }

    /// Lift an upper bound through the chain: for every stage, the
    /// most favorable value in the column's domain — including absence
    /// (identity) — bounds its effect. Monotone, and maps non-negative
    /// bounds to non-negative bounds (see the module docs for why that
    /// matters).
    pub fn bound(&self, mut ub: f64) -> f64 {
        for stage in &self.stages {
            let (_, max) = stage.min_max;
            ub = match stage.op {
                // Factor <= 1 everywhere and absence means 1 exactly.
                StageOp::MultExpDecay { .. } => ub,
                // Same, one dimension up: exp(-d/scale) <= 1 for every
                // non-negative distance, and a document without a point
                // passes through at exactly 1.
                //
                // The geo column's BOUNDING BOX cannot tighten this,
                // and the reason is worth stating because it looks like
                // it should. A tighter lift would be
                // exp(-d_min(origin, bbox) / scale) — but the bound must
                // dominate EVERY document in the block, including the
                // ones with no point, whose factor is exactly 1. So the
                // honest lift is max(1, exp(-d_min/scale)) = 1, for
                // every origin and every box. Tightening needs per-block
                // presence counts (and then per-block boxes), which is
                // the same future optimization MULT_EXP_DECAY has open —
                // not a correctness gap, and NOT something a bbox alone
                // can buy.
                StageOp::MultGeoDecay { .. } => ub,
                StageOp::MultLog { weight } => {
                    let factor = if max.is_nan() {
                        1.0
                    } else {
                        1.0 + weight * (1.0 + max.max(0.0)).ln()
                    };
                    ub * factor.max(1.0)
                }
                StageOp::AddLinear { weight } => {
                    let (min, _) = stage.min_max;
                    let addend = if max.is_nan() {
                        0.0
                    } else {
                        (weight * min).max(weight * max)
                    };
                    ub + addend.max(0.0)
                }
            };
        }
        ub
    }
}
