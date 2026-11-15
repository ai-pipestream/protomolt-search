//! Score-function chains over numeric columns.
//!
//! The final score of a document is a chain of stages applied in
//! request-list order to its BM25 score, on the node, BEFORE the floor
//! test and heap insertion. Every stage satisfies one contract that
//! makes chaining sound under block-max pruning:
//!
//! - `eval` is monotone non-decreasing in the incoming score, and
//! - `bound` lifts an upper bound through the stage using the column's
//!   min/max metadata, valid over the whole domain INCLUDING absence
//!   (a document without a value passes through unchanged).
//!
//! Under that contract "upper bound in, upper bound out" survives
//! composition: the pruned scorer lifts every bound sum through
//! [`ScoreChain::bound`] before its inert tests and inserts candidates
//! by their [`ScoreChain::eval`] result, and MaxScore, seeded floors,
//! and `kth_best` all keep working on the final-score scale with no
//! new theorems. Lifted bounds stay non-negative (BM25 bounds start at
//! zero or above, multiplicative factors stay at least 1 on the bound
//! side because absence caps them at identity, and additive lifts add
//! nothing negative), which is what keeps the multiplicative lift an
//! upper bound even when a negative additive stage pushes some
//! document's true score below zero.
//!
//! Chain-list order is the pinned evaluation order (IEEE float math is
//! not associative across reorderings), identical on every shard, so
//! distributed results stay bitwise equal to the monolith's.

mod chain;
mod stage;
#[cfg(test)]
mod tests;

pub use chain::ScoreChain;

/// Read surface for a shard's columns during scoring and filtering,
/// implemented by the heap store and the mmap reader (via the node's
/// shard wrapper). Score chains read the numeric half; compiled
/// filters read all of it — one surface serves selection and scoring
/// alike, so the two can never disagree about what a document holds.
pub trait NumericRead {
    /// `doc_id`'s value in numeric column `ni`, `None` when absent.
    fn value(&self, ni: usize, doc_id: u32) -> Option<f64>;
    /// `doc_id`'s value under `key_ord` in map-numeric column
    /// `column`, `None` when absent.
    fn map_value(&self, column: usize, key_ord: u32, doc_id: u32) -> Option<f64>;
    /// Exact signed map value, with absence separate from zero.
    fn map_int_value(&self, column: usize, key_ord: u32, doc_id: u32) -> Option<i64>;
    /// Exact unsigned map value, with absence separate from zero.
    fn map_uint_value(&self, column: usize, key_ord: u32, doc_id: u32) -> Option<u64>;
    /// `doc_id`'s value in i64 column `ii`, `None` when absent. Kept i64
    /// here and cast at the eval site so the storage stays exact and only
    /// the arithmetic is float.
    fn int_value(&self, ii: usize, doc_id: u32) -> Option<i64>;
    /// Exact u64 column value, with absence separate from every numeric value.
    fn uint_value(&self, ii: usize, doc_id: u32) -> Option<u64>;
    /// `doc_id`'s (lat, lon) in geo column `gi`, `None` when absent. Also
    /// the read surface geo FILTERS use — one column read serves selection
    /// and scoring alike.
    fn geo_value(&self, gi: usize, doc_id: u32) -> Option<(f64, f64)>;
    /// `doc_id`'s value ordinal in facet column `fi`, `None` when absent.
    /// Filters compare ordinals, never strings: values resolve against the
    /// shard's dictionary once per request.
    fn facet_ord(&self, fi: usize, doc_id: u32) -> Option<u32>;
    /// `doc_id`'s value ordinal under `key_ord` in map-facet column
    /// `ci`, `None` when absent.
    fn map_facet_value_ord(&self, ci: usize, key_ord: u32, doc_id: u32) -> Option<u32>;
}

/// A stage's resolved column on THIS shard: a plain f64 column, an
/// i64/u64 column, or a map-numeric column entry under a shard-local key
/// ordinal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnRef {
    /// Index into the shard's numeric table.
    Numeric(usize),
    /// Index into the shard's integer table. Values reach the stage
    /// arithmetic through `as f64`, which is monotone non-decreasing,
    /// so a bound computed from the column's min/max converted the
    /// same way is still a bound (rounding cannot reorder the two
    /// sides). Above 2^53 the stage arithmetic loses the exactness the
    /// STORAGE keeps — the point of the kind is that the value comes
    /// back intact, not that ln() suddenly has 64 bits.
    Integer(usize),
    /// Index into the shard's u64 table. Values and extrema use the same
    /// monotone conversion to f64 as signed columns; score arithmetic may
    /// round adjacent large integers while storage remains exact.
    UnsignedInteger(usize),
    /// A map-numeric column and a key ordinal in ITS key dictionary
    /// (ordinals are shard-local, like every dictionary here).
    MapKey {
        /// Index into the shard's map-numeric table.
        column: usize,
        /// Key ordinal within that column's key dictionary.
        key_ord: u32,
    },
    /// Exact signed map input, converted to double at score evaluation.
    MapIntegerKey {
        /// Index into the signed map table.
        column: usize,
        /// Key ordinal within that column's dictionary.
        key_ord: u32,
    },
    /// Exact unsigned map input, converted to double at score evaluation.
    MapUnsignedIntegerKey {
        /// Index into the unsigned map table.
        column: usize,
        /// Key ordinal within that column's dictionary.
        key_ord: u32,
    },
    /// Index into the shard's geo table. Only the
    /// [`StageOp::MultGeoDecay`] ops resolve to this; the scalar ops never
    /// see it, and it never sees them.
    Geo(usize),
}

/// One stage's transform. Every op is monotone non-decreasing in the
/// incoming score; parameters are validated at request parse (finite,
/// `scale > 0`, `MULT_LOG` weight >= 0 — a negative log weight could
/// turn the factor negative, which breaks monotonicity, so it is
/// refused rather than admitted).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StageOp {
    /// `score * exp(-|x - origin| / scale)`: recency decay. Factor in
    /// (0, 1], so the bound lift is identity (absence already caps the
    /// factor at 1).
    MultExpDecay {
        /// The value at which the factor is 1.
        origin: f64,
        /// Decay scale (> 0).
        scale: f64,
    },
    /// `score * (1 + weight * ln(1 + max(x, 0)))`: citation-count
    /// style boost, `weight >= 0`.
    MultLog {
        /// Boost weight (>= 0).
        weight: f64,
    },
    /// `score + weight * x`: additive boost, any finite weight.
    AddLinear {
        /// Addend weight.
        weight: f64,
    },
    /// `score * exp(-distance_meters(origin, point) / scale)` over a
    /// GEO-POINT column. Factor in (0, 1], so the stage is monotone
    /// non-decreasing in the incoming score and its bound lift is
    /// identity — [`StageOp::MultExpDecay`]'s argument, one dimension up.
    MultGeoDecay {
        /// Which distance the decay measures.
        metric: crate::geo::GeoMetric,
        /// Origin latitude in degrees (the point where the factor is 1).
        origin_lat: f64,
        /// Origin longitude in degrees.
        origin_lon: f64,
        /// Decay scale in METERS (> 0).
        scale: f64,
    },
}

/// One resolved stage: the op plus THIS shard's view of its column.
#[derive(Debug, Clone, Copy)]
pub struct Stage {
    /// The transform.
    pub op: StageOp,
    /// The resolved column reference, `None` when the shard lacks the
    /// column (or, for map stages, the key) — every document is then
    /// absent and the stage is identity, which is exact, not degraded
    /// (the coordinator refuses a column NO shard knows).
    pub column: Option<ColumnRef>,
    /// This shard's (min, max) over the read values — the whole column
    /// for a plain stage, the KEY's values for a map stage; NaN when
    /// missing or empty (an integer column's metadata arrives through the
    /// same monotone `as f64` cast its values do, and its empty range
    /// arrives as NaN). Feeds [`ScoreChain::bound`]. A geo stage leaves
    /// it (NaN, NaN): its bound is identity regardless of the column's
    /// extent, for the reason [`ScoreChain::bound`] spells out.
    pub min_max: (f64, f64),
}

impl crate::pb::ScoreStage {
    /// Wire operation code, including the code inside an explicit map input.
    pub(crate) fn operation_code(&self) -> i32 {
        use crate::pb::score_stage::Operation;
        match &self.operation {
            Some(Operation::Op(op)) => *op,
            Some(Operation::MapOp(map) | Operation::TypedMapOp(map)) => map.op,
            None => 0,
        }
    }

    /// A map key has presence independently of its string value. Validation
    /// rejects conflicting legacy keys before this selector is resolved.
    pub(crate) fn map_key(&self) -> Option<&str> {
        use crate::pb::score_stage::Operation;
        match &self.operation {
            Some(Operation::MapOp(map) | Operation::TypedMapOp(map)) => Some(&map.key),
            _ => (!self.key.is_empty()).then_some(self.key.as_str()),
        }
    }
}
