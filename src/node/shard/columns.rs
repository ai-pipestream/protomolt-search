//! Column lookup traits, score-stage parsing, and numeric projection helpers.

use crate::node::*;

/// Declared families on a node with no lexical store yet. No map keys or
/// dictionary values exist, but an absent row must not erase a declared type.
pub(in crate::node) struct EmptyColumns<'a>(pub(in crate::node) &'a NodeConfig);
impl crate::values::ColumnLookup for EmptyColumns<'_> {
    fn numeric_index(&self, name: &str) -> Option<usize> {
        self.0.numeric_fields.iter().position(|n| n == name)
    }
    fn integer_index(&self, name: &str) -> Option<usize> {
        self.0.integer_fields.iter().position(|n| n == name)
    }
    fn unsigned_integer_index(&self, name: &str) -> Option<usize> {
        self.0
            .unsigned_integer_fields
            .iter()
            .position(|n| n == name)
    }
    fn facet_index(&self, name: &str) -> Option<usize> {
        self.0.facet_fields.iter().position(|n| n == name)
    }
    fn map_numeric_index(&self, name: &str) -> Option<usize> {
        self.0.map_numeric_fields.iter().position(|n| n == name)
    }
    fn map_integer_index(&self, name: &str) -> Option<usize> {
        self.0.map_integer_fields.iter().position(|n| n == name)
    }
    fn map_unsigned_integer_index(&self, name: &str) -> Option<usize> {
        self.0
            .map_unsigned_integer_fields
            .iter()
            .position(|n| n == name)
    }
    fn map_facet_index(&self, name: &str) -> Option<usize> {
        self.0.map_facet_fields.iter().position(|n| n == name)
    }
    fn map_numeric_key_ord(&self, _: usize, _: &str) -> Option<u32> {
        None
    }
    fn map_integer_key_ord(&self, _: usize, _: &str) -> Option<u32> {
        None
    }
    fn map_unsigned_integer_key_ord(&self, _: usize, _: &str) -> Option<u32> {
        None
    }
    fn map_facet_key_ord(&self, _: usize, _: &str) -> Option<u32> {
        None
    }
    fn facet_value_ord_of(&self, _: usize, _: &str) -> Option<u32> {
        None
    }
    fn map_facet_value_ord_of(&self, _: usize, _: &str) -> Option<u32> {
        None
    }
}

/// [`crate::scorefn::NumericRead`] over a searchable shard shape, for
/// score-chain evaluation during scoring.
pub(in crate::node) struct ShardNumericRead<'a>(pub(in crate::node) &'a Bm25Shard);

/// The inverted facet columns of a searchable shard
/// (`crate::filter_bits`). A heap store has no inverted columns and is
/// read forward; a mapped file and a segmented shard answer from theirs.
impl crate::filter_bits::ColumnSets for Bm25Shard {
    fn number_rows(
        &self,
        range: &crate::filter_bits::NumberRange<'_>,
        want: bool,
        domain: &crate::boolean_bits::Bits,
        out: &mut crate::boolean_bits::Bits,
    ) {
        match self {
            Bm25Shard::Building(store) => {
                range.part(store, 0..store.next_doc_id() as usize, want, domain, out)
            }
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(reader) => range.part(reader, 0..domain.len(), want, domain, out),
            Bm25Shard::Segmented(shard) => shard.number_rows(range, want, domain, out),
        }
    }

    fn facet_rows(
        &self,
        fi: usize,
        accept: &dyn Fn(u32) -> bool,
        out: &mut crate::boolean_bits::Bits,
    ) {
        match self {
            Bm25Shard::Building(store) => {
                for slot in 0..(store.next_doc_id() as usize).min(out.len()) {
                    if store.facet_ord(fi, slot as u32).is_some_and(accept) {
                        out.set(slot);
                    }
                }
            }
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(reader) => {
                let inverted = reader.facet_rows(fi);
                for ord in (0..inverted.values() as u32).filter(|&ord| accept(ord)) {
                    inverted.union_into(ord, 0, out);
                }
            }
            Bm25Shard::Segmented(shard) => shard.facet_rows(fi, accept, out),
        }
    }

    fn facet_present(&self, fi: usize, out: &mut crate::boolean_bits::Bits) {
        match self {
            Bm25Shard::Building(store) => {
                for slot in 0..(store.next_doc_id() as usize).min(out.len()) {
                    if store.facet_ord(fi, slot as u32).is_some() {
                        out.set(slot);
                    }
                }
            }
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(reader) => reader.facet_rows(fi).present_into(0, out),
            Bm25Shard::Segmented(shard) => shard.facet_present(fi, out),
        }
    }
}

/// Value-expression column lookup implementation consumed by `values::resolve`.
impl crate::values::ColumnLookup for Bm25Shard {
    fn map_integer_index(&self, name: &str) -> Option<usize> {
        Bm25Shard::map_integer_index(self, name)
    }
    fn map_integer_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        Bm25Shard::map_integer_key_ord(self, ci, key)
    }
    fn map_unsigned_integer_index(&self, name: &str) -> Option<usize> {
        Bm25Shard::map_unsigned_integer_index(self, name)
    }
    fn map_unsigned_integer_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        Bm25Shard::map_unsigned_integer_key_ord(self, ci, key)
    }
    fn numeric_index(&self, name: &str) -> Option<usize> {
        Bm25Shard::numeric_index(self, name)
    }
    fn integer_index(&self, name: &str) -> Option<usize> {
        Bm25Shard::integer_index(self, name)
    }
    fn unsigned_integer_index(&self, name: &str) -> Option<usize> {
        Bm25Shard::unsigned_integer_index(self, name)
    }
    fn facet_index(&self, name: &str) -> Option<usize> {
        Bm25Shard::facet_index(self, name)
    }
    fn map_numeric_index(&self, name: &str) -> Option<usize> {
        Bm25Shard::map_numeric_index(self, name)
    }
    fn map_numeric_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        Bm25Shard::map_numeric_key_ord(self, ci, key)
    }
    fn map_facet_index(&self, name: &str) -> Option<usize> {
        Bm25Shard::map_facet_index(self, name)
    }
    fn map_facet_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        Bm25Shard::map_facet_key_ord(self, ci, key)
    }
    fn facet_value_ord_of(&self, ci: usize, value: &str) -> Option<u32> {
        Bm25Shard::facet_value_ord_of(self, ci, value)
    }
    fn map_facet_value_ord_of(&self, ci: usize, value: &str) -> Option<u32> {
        Bm25Shard::map_facet_value_ord_of(self, ci, value)
    }
}

impl crate::scorefn::NumericRead for ShardNumericRead<'_> {
    fn value(&self, ni: usize, doc_id: u32) -> Option<f64> {
        self.0.numeric_value(ni, doc_id)
    }
    fn map_value(&self, column: usize, key_ord: u32, doc_id: u32) -> Option<f64> {
        self.0.map_numeric_value(column, key_ord, doc_id)
    }
    fn map_int_value(&self, column: usize, key_ord: u32, doc_id: u32) -> Option<i64> {
        self.0.map_integer_value(column, key_ord, doc_id)
    }
    fn map_uint_value(&self, column: usize, key_ord: u32, doc_id: u32) -> Option<u64> {
        self.0.map_unsigned_integer_value(column, key_ord, doc_id)
    }
    fn int_value(&self, ii: usize, doc_id: u32) -> Option<i64> {
        self.0.integer_value(ii, doc_id)
    }
    fn uint_value(&self, ii: usize, doc_id: u32) -> Option<u64> {
        self.0.unsigned_integer_value(ii, doc_id)
    }
    fn geo_value(&self, gi: usize, doc_id: u32) -> Option<(f64, f64)> {
        self.0.geo_value(gi, doc_id)
    }
    fn facet_ord(&self, fi: usize, doc_id: u32) -> Option<u32> {
        self.0.facet_ord(fi, doc_id)
    }
    fn map_facet_value_ord(&self, ci: usize, key_ord: u32, doc_id: u32) -> Option<u32> {
        self.0.map_facet_value_ord(ci, key_ord, doc_id)
    }
}

/// Render one evaluated projection value onto the wire. Absence is the
/// unset oneof; string ordinals render against the dictionary the
/// shard owns.
pub(in crate::node) fn projected_value(
    val: Option<crate::values::Val>,
    store: &Bm25Shard,
) -> crate::pb::ProjectedValue {
    use crate::pb::projected_value::Value as W;
    let value = match val {
        None => None,
        Some(crate::values::Val::Int(v)) => Some(W::IntValue(v)),
        Some(crate::values::Val::Uint(v)) => Some(W::UintValue(v)),
        Some(crate::values::Val::Double(v)) => Some(W::DoubleValue(v)),
        Some(crate::values::Val::FacetOrd { column, ord }) => {
            Some(W::StringValue(store.facet_value(column, ord).to_string()))
        }
        Some(crate::values::Val::MapFacetOrd { column, ord }) => Some(W::StringValue(
            store.map_facet_value(column, ord).to_string(),
        )),
        Some(crate::values::Val::Bool(v)) => Some(W::BoolValue(v)),
    };
    crate::pb::ProjectedValue { value }
}

/// Order-preserving `u64` key for a signed `i64` value via offset binary.
pub(crate) fn i64_order_bits(x: i64) -> u64 {
    (x as u64) ^ (1u64 << 63)
}

/// Inverse of [`i64_order_bits`].
pub(crate) fn i64_from_order_bits(bits: u64) -> i64 {
    (bits ^ (1u64 << 63)) as i64
}

/// Inverse of [`f64_order_bits`].
pub(crate) fn f64_from_order_bits(bits: u64) -> f64 {
    if bits >> 63 == 1 {
        f64::from_bits(bits & !(1u64 << 63))
    } else {
        f64::from_bits(!bits)
    }
}

/// Order-preserving u64 key for a (finite) f64 value: the sign-flip
/// trick — negatives flip every bit, positives set the sign bit — so
/// unsigned comparison matches numeric order. NaN never reaches this
/// (NaN is the f64 column's absence sentinel and absent values are
/// excluded before keying).
pub(crate) fn f64_order_bits(x: f64) -> u64 {
    let bits = x.to_bits();
    if bits >> 63 == 1 {
        !bits
    } else {
        bits | (1u64 << 63)
    }
}

/// An i64 column's bound metadata on the score-chain's f64 scale. The
/// empty range (min > max, the i64 column's stand-in for NaN) becomes
/// the (NaN, NaN) the bound rules already read as "no metadata"; a real
/// range converts through the same monotone cast the values do, so the
/// bound still dominates every converted value.
pub(in crate::node) fn int_min_max_as_f64((min, max): (i64, i64)) -> (f64, f64) {
    if min > max {
        (f64::NAN, f64::NAN)
    } else {
        (min as f64, max as f64)
    }
}

/// Unsigned extrema on the score scale. Check emptiness before converting:
/// u64::MAX rounds to 2^64 but remains a present value.
pub(in crate::node) fn uint_min_max_as_f64((min, max): (u64, u64)) -> (f64, f64) {
    if min > max {
        (f64::NAN, f64::NAN)
    } else {
        (min as f64, max as f64)
    }
}

/// A validated score input. Keep the typed map selector distinct from the
/// legacy f64-only map operation throughout resolution.
pub(crate) struct ScoreStageSpec {
    pub(in crate::node) op: crate::scorefn::StageOp,
    pub(in crate::node) column: String,
    pub(in crate::node) key: Option<String>,
    pub(in crate::node) typed_map: bool,
}

/// The validated operation of wire stage `i`.
fn parse_stage_op(
    i: usize,
    stage: &crate::pb::ScoreStage,
) -> Result<crate::scorefn::StageOp, Status> {
    use crate::scorefn::StageOp;
    Ok(match crate::pb::ScoreOp::try_from(stage.operation_code()) {
        Ok(crate::pb::ScoreOp::MultExpDecay) => {
            if !(stage.scale.is_finite() && stage.scale > 0.0) || !stage.origin.is_finite() {
                return Err(Status::invalid_argument(format!(
                    "score stage {i}: MULT_EXP_DECAY needs finite origin and scale > 0"
                )));
            }
            StageOp::MultExpDecay {
                origin: stage.origin,
                scale: stage.scale,
            }
        }
        Ok(crate::pb::ScoreOp::MultLog) => {
            if !stage.weight.is_finite() || stage.weight < 0.0 {
                return Err(Status::invalid_argument(format!(
                    "score stage {i}: MULT_LOG needs a finite weight >= 0 (a negative \
                     weight could turn the factor negative, breaking monotonicity)"
                )));
            }
            StageOp::MultLog {
                weight: stage.weight,
            }
        }
        Ok(crate::pb::ScoreOp::AddLinear) => {
            if !stage.weight.is_finite() {
                return Err(Status::invalid_argument(format!(
                    "score stage {i}: ADD_LINEAR needs a finite weight"
                )));
            }
            StageOp::AddLinear {
                weight: stage.weight,
            }
        }
        Ok(
            op @ (crate::pb::ScoreOp::MultGeoDecayHaversine
            | crate::pb::ScoreOp::MultGeoDecayManhattan),
        ) => {
            if !(stage.scale.is_finite() && stage.scale > 0.0) {
                return Err(Status::invalid_argument(format!(
                    "score stage {i}: MULT_GEO_DECAY needs a finite scale > 0 (meters)"
                )));
            }
            validate_lat_lon(
                &format!("score stage {i}"),
                stage.origin_lat,
                stage.origin_lon,
            )?;
            if stage.map_key().is_some() {
                return Err(Status::invalid_argument(format!(
                    "score stage {i}: MULT_GEO_DECAY reads a geo-point column, which \
                     has no keys; use the plain `op` selector with `key` empty"
                )));
            }
            StageOp::MultGeoDecay {
                metric: if op == crate::pb::ScoreOp::MultGeoDecayHaversine {
                    crate::geo::GeoMetric::Haversine
                } else {
                    crate::geo::GeoMetric::Manhattan
                },
                origin_lat: stage.origin_lat,
                origin_lon: stage.origin_lon,
                scale: stage.scale,
            }
        }
        Ok(crate::pb::ScoreOp::Unspecified) | Err(_) => {
            return Err(Status::invalid_argument(format!(
                "score stage {i}: unknown op {}",
                stage.operation_code()
            )));
        }
    })
}

/// Parse and validate wire score stages into shard-independent stage specs,
/// refusing unknown ops, empty column names, or out-of-range parameters.
pub(crate) fn parse_score_stages(
    stages: &[crate::pb::ScoreStage],
) -> Result<Vec<ScoreStageSpec>, Status> {
    stages
        .iter()
        .enumerate()
        .map(|(i, stage)| {
            if stage.column.is_empty() {
                return Err(Status::invalid_argument(format!(
                    "score stage {i}: a stage names the numeric column it reads"
                )));
            }
            if matches!(
                stage.operation,
                Some(crate::pb::score_stage::Operation::MapOp(_) | crate::pb::score_stage::Operation::TypedMapOp(_))
            ) && !stage.key.is_empty()
            {
                return Err(Status::invalid_argument(format!(
                    "score stage {i}: an explicit map operation carries its own key; leave the legacy key empty"
                )));
            }
            let op = parse_stage_op(i, stage)?;
            Ok(ScoreStageSpec {
                op,
                column: stage.column.clone(),
                key: stage.map_key().map(str::to_owned),
                typed_map: matches!(stage.operation, Some(crate::pb::score_stage::Operation::TypedMapOp(_))),
            })
        })
        .collect()
}
