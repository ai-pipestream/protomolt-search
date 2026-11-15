//! One resolved stage's own contribution and the value it read.

use super::{ColumnRef, NumericRead, Stage, StageOp};

impl Stage {
    /// The stage's own contribution at its IDENTITY score: the factor
    /// for the multiplicative ops, the addend for `ADD_LINEAR`. `None`
    /// exactly when the document has no value (or the shard lacks the
    /// column) — identity, which a stored-value scorer dimension reads
    /// as a missing signal. The float expressions match
    /// [`ScoreChain::eval`] term for term, so a contribution composed
    /// back onto a score reproduces the chain's arithmetic.
    pub fn contribution(&self, doc_id: u32, columns: &dyn NumericRead) -> Option<f64> {
        if let StageOp::MultGeoDecay {
            metric,
            origin_lat,
            origin_lon,
            scale,
        } = self.op
        {
            let Some(ColumnRef::Geo(gi)) = self.column else {
                return None;
            };
            let (lat, lon) = columns.geo_value(gi, doc_id)?;
            return Some((-metric.meters(origin_lat, origin_lon, lat, lon) / scale).exp());
        }
        let x = match self.column {
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
        }?;
        Some(match self.op {
            StageOp::MultExpDecay { origin, scale } => (-((x - origin).abs()) / scale).exp(),
            StageOp::MultLog { weight } => 1.0 + weight * (1.0 + x.max(0.0)).ln(),
            StageOp::AddLinear { weight } => weight * x,
            StageOp::MultGeoDecay { .. } => unreachable!("geo ops returned above"),
        })
    }
}

impl Stage {
    /// The value the stage read for the document: the column value, or
    /// for the geo ops the distance in meters from the origin. `None`
    /// exactly when [`Stage::contribution`] is `None`.
    pub fn input(&self, doc_id: u32, columns: &dyn NumericRead) -> Option<f64> {
        if let StageOp::MultGeoDecay {
            metric,
            origin_lat,
            origin_lon,
            ..
        } = self.op
        {
            let Some(ColumnRef::Geo(gi)) = self.column else {
                return None;
            };
            let (lat, lon) = columns.geo_value(gi, doc_id)?;
            return Some(metric.meters(origin_lat, origin_lon, lat, lon));
        }
        match self.column {
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
        }
    }

    /// Whether the stage adds its contribution (`ADD_LINEAR`) rather
    /// than multiplying by it.
    pub fn is_additive(&self) -> bool {
        matches!(self.op, StageOp::AddLinear { .. })
    }
}
